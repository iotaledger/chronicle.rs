// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0
use super::*;
use crate::{
    application::{
        BrokerEvent,
        BrokerHandle,
        ImporterSession,
    },
    archiver::LogFile,
};
use bee_message::{
    address::Ed25519Address,
    output::Output,
    payload::transaction::{
        Essence,
        TransactionPayload,
    },
    prelude::{
        Address,
        Input,
        MilestoneIndex,
        MilestonePayload,
        Payload,
        TransactionId,
    },
};

use chronicle_common::config::PartitionConfig;
use chronicle_storage::access::SyncRecord;
use std::ops::Range;

type ImporterHandle = UnboundedHandle<ImporterEvent>;

use std::{
    collections::btree_map::IntoIter,
    path::PathBuf,
    sync::atomic::Ordering,
};

/// Import all records to all tables
pub struct All;

/// Defines the Importer Mode
pub trait ImportMode: Sized + Send + 'static {
    /// Instruct how to import the milestone data
    fn handle_milestone_data(
        milestone_data: MilestoneData,
        importer_handle: &ImporterHandle,
        importer: &mut Importer<Self>,
    ) -> anyhow::Result<()>;
}
impl ImportMode for All {
    fn handle_milestone_data(
        milestone_data: MilestoneData,
        importer_handle: &ImporterHandle,
        importer: &mut Importer<All>,
    ) -> anyhow::Result<()> {
        let analytic_record = milestone_data.get_analytic_record().map_err(|e| {
            error!("Unable to get analytic record for milestone data. Error: {}", e);
            e
        })?;
        let milestone_index = milestone_data.milestone_index();
        let mut iterator = milestone_data.into_iter();
        importer.insert_some_messages(milestone_index, &mut iterator, importer_handle)?;
        importer
            .in_progress_milestones_data
            .insert(milestone_index, (iterator, analytic_record));
        Ok(())
    }
}

/// Importer events
#[derive(Debug, Clone, Copy)]
pub enum ImporterEvent {
    /// The result of an insert into the database
    CqlResult(Result<u32, u32>),
    /// Indicator to continue processing
    ProcessMore(u32),
    /// Shutdown the importer
    Shutdown,
}

impl ShutdownEvent for ImporterEvent {
    fn shutdown_event() -> Self {
        ImporterEvent::Shutdown
    }
}

/// Importer state
pub struct Importer<T> {
    /// The file path of the importer
    file_path: PathBuf,
    /// The log file
    log_file: Option<LogFile>,
    /// LogFile total_size,
    log_file_size: u64,
    /// from milestone index
    from_ms: u32,
    /// to milestone index
    to_ms: u32,
    /// The default Chronicle keyspace
    default_keyspace: ChronicleKeyspace,
    /// The number of retires per query
    retries: u8,
    /// The chronicle id
    chronicle_id: u8,
    /// The number of parallelism
    parallelism: u8,
    /// The resume flag
    resume: bool,
    /// The range of requested milestones to import
    import_range: std::ops::Range<u32>,
    /// The database sync data
    sync_data: SyncData,
    /// In progress milestones data
    in_progress_milestones_data: HashMap<u32, (IntoIter<MessageId, FullMessage>, AnalyticRecord)>,
    in_progress_milestones_data_bytes_size: HashMap<u32, usize>,
    /// The flag of end of file
    eof: bool,
    /// Import mode marker
    _mode: std::marker::PhantomData<T>,
}
impl<T: ImportMode> Importer<T> {
    pub(crate) fn new(
        default_keyspace: ChronicleKeyspace,
        partition_config: PartitionConfig,
        file_path: PathBuf,
        resume: bool,
        import_range: Range<u32>,
        import_type: T,
        parallelism: u8,
        retries: u8,
    ) -> Self {
        Self {
            file_path,
            log_file: None,
            log_file_size: 0,
            from_ms: 0,
            to_ms: 0,
            default_keyspace,
            partition_config,
            parallelism,
            chronicle_id: 0,
            in_progress_milestones_data: HashMap::new(),
            in_progress_milestones_data_bytes_size: HashMap::new(),
            retries,
            resume,
            import_range,
            sync_data: SyncData::default(),
            eof: false,
            _mode: std::marker::PhantomData::<T>,
        }
    }
}

#[async_trait]
impl<T: ImportMode> Actor<BrokerHandle> for Importer<T> {
    type Data = ();
    type Channel = UnboundedChannel<ImporterEvent>;

    async fn init(&mut self, rt: &mut Rt<Self, BrokerHandle>) -> ActorResult<Self::Data> {
        let path = info!(
            "Importer {:?} is Initializing, with permanode keyspace: {}",
            self.file_path,
            self.default_keyspace.name()
        );
        let log_file = LogFile::try_from(self.file_path.clone()).map_err(|e| {
            error!("Unable to create LogFile. Error: {}", e);
            let event = BrokerEvent::ImporterSession(ImporterSession::PathError {
                path: self.file_path.clone(),
                msg: "Invalid LogFile path".into(),
            });
            rt.supervisor_handle().send(event).ok();
            ActorError::aborted(e)
        })?;
        let from = log_file.from_ms_index();
        let to = log_file.to_ms_index();
        self.log_file_size = log_file.len();
        self.from_ms = from;
        self.to_ms = to;
        let importer_session = ImporterSession::ProgressBar {
            log_file_size: self.log_file_size,
            from_ms: from,
            to_ms: to,
            ms_bytes_size: 0,
            milestone_index: 0,
            skipped: true,
        };
        // fetch sync data from the keyspace
        if self.resume {
            let sync_range = SyncRange { from, to };
            self.sync_data = SyncData::try_fetch(&self.default_keyspace, &sync_range, 10)
                .await
                .map_err(|e| {
                    error!("Unable to fetch SyncData {}", e);
                    let event = BrokerEvent::ImporterSession(ImporterSession::Finish {
                        from_ms: self.from_ms,
                        to_ms: self.to_ms,
                        msg: "failed".into(),
                    });
                    rt.supervisor_handle().send(event).ok();
                    ActorError::aborted(e)
                })?;
        }
        self.log_file.replace(log_file);
        self.init_importing(rt.handle(), rt.supervisor_handle())
            .await
            .map_err(|e| {
                error!("Unable to init importing process. Error: {}", e);
                ActorError::aborted(e)
            })?;
        rt.supervisor_handle()
            .send(BrokerEvent::ImporterSession(importer_session))
            .ok();
        Ok(())
    }

    async fn run(&mut self, rt: &mut Rt<Self, BrokerHandle>, data: Self::Data) -> ActorResult<()> {
        info!("Importer LogFile: {:?} is running", self.file_path);
        // check if it's already EOF and nothing to progress
        if self.in_progress_milestones_data.is_empty() && self.eof {
            warn!("Skipped already imported LogFile: {:?}", self.file_path);
            let event = BrokerEvent::ImporterSession(ImporterSession::Finish {
                from_ms: self.from_ms,
                to_ms: self.to_ms,
                msg: "ok".into(),
            });
            rt.supervisor_handle().send(event).ok();
            return Ok(());
        }
        while let Some(event) = rt.inbox_mut().next().await {
            match event {
                ImporterEvent::CqlResult(result) => {
                    match result {
                        Ok(milestone_index) => {
                            // remove it from in_progress
                            let _ = self
                                .in_progress_milestones_data
                                .remove(&milestone_index)
                                .expect("Expected entry for a milestone data");
                            info!("Imported milestone data for milestone index: {}", milestone_index);
                            let ms_bytes_size = self
                                .in_progress_milestones_data_bytes_size
                                .remove(&milestone_index)
                                .expect("Expected size-entry for a milestone data");
                            let skipped = false;
                            Self::imported(
                                rt.supervisor_handle(),
                                self.from_ms,
                                self.to_ms,
                                self.log_file_size,
                                milestone_index,
                                ms_bytes_size,
                                skipped,
                            );
                            // check if we should process more
                            if !rt.service().is_stopping() {
                                // process one more
                                if let Some(milestone_data) =
                                    self.next_milestone_data(rt.supervisor_handle()).await.map_err(|e| {
                                        error!("Unable to fetch next milestone data. Error: {}", e);
                                        let event = BrokerEvent::ImporterSession(ImporterSession::Finish {
                                            from_ms: self.from_ms,
                                            to_ms: self.to_ms,
                                            msg: "failed".into(),
                                        });
                                        rt.supervisor_handle().send(event).ok();
                                        ActorError::aborted(e)
                                    })?
                                {
                                    T::handle_milestone_data(milestone_data, rt.handle(), self).map_err(|e| {
                                        error!("{}", e);
                                        let event = BrokerEvent::ImporterSession(ImporterSession::Finish {
                                            from_ms: self.from_ms,
                                            to_ms: self.to_ms,
                                            msg: "failed".into(),
                                        });
                                        rt.supervisor_handle().send(event).ok();
                                        ActorError::aborted(e)
                                    })?;
                                } else {
                                    // no more milestone data.
                                    if self.in_progress_milestones_data.is_empty() {
                                        // shut it down
                                        info!("Imported the LogFile: {:?}", self.file_path);
                                        break;
                                    }
                                };
                            }
                        }
                        Err(milestone_index) => {
                            let event = BrokerEvent::ImporterSession(ImporterSession::Finish {
                                from_ms: self.from_ms,
                                to_ms: self.to_ms,
                                msg: "failed".into(),
                            });
                            rt.supervisor_handle().send(event).ok();
                            // an outage in scylla so we abort
                            return Err(ActorError::aborted_msg(format!(
                                "Unable to import milestone {}",
                                milestone_index
                            )));
                        }
                    }
                }
                // note: we receive this variant in All modes.
                ImporterEvent::ProcessMore(milestone_index) => {
                    if rt.service().is_stopping() {
                        continue;
                    }
                    // extract the remaining milestone data iterator
                    let (mut iter, analytic_record) = self
                        .in_progress_milestones_data
                        .remove(&milestone_index)
                        .expect("Expected Entry for milestone data");
                    let is_empty = iter.len() == 0;
                    let importer_handle = rt.handle();
                    let keyspace = self.default_keyspace.clone();
                    if !is_empty {
                        self.insert_some_messages(milestone_index, &mut iter, importer_handle)
                            .map_err(|e| {
                                error!("Unable to insert/import more message ,Error: {}", e);
                                let event = BrokerEvent::ImporterSession(ImporterSession::Finish {
                                    from_ms: self.from_ms,
                                    to_ms: self.to_ms,
                                    msg: "failed".into(),
                                });
                                rt.supervisor_handle().send(event).ok();
                                ActorError::aborted(e)
                            })?;
                    } else {
                        // insert it into analytics and sync table
                        let milestone_index = bee_message::prelude::MilestoneIndex(milestone_index);
                        let synced_by = Some(self.chronicle_id);
                        let logged_by = Some(self.chronicle_id);
                        let synced_record = SyncRecord::new(milestone_index, synced_by, logged_by);
                        let worker = AnalyzeAndSyncWorker::boxed(
                            importer_handle.clone(),
                            keyspace,
                            analytic_record.clone(),
                            synced_record,
                            self.retries.into(),
                        );
                        if let Err(RequestError::Ring(r)) = self
                            .default_keyspace
                            .insert_prepared(&"permanode".to_string(), &analytic_record)
                            .consistency(Consistency::Quorum)
                            .build()
                            .map_err(|e| ActorError::exit(e))?
                            .send_local_with_worker(worker)
                        {
                            let keyspace_name = self.default_keyspace.name();
                            if let Err(worker) = retry_send(&keyspace_name, r, 2) {
                                worker.handle_error(WorkerError::NoRing, None)?;
                            };
                        };
                    }
                    // put it back
                    self.in_progress_milestones_data
                        .insert(milestone_index, (iter, analytic_record));
                    // NOTE: we only delete it once we get Ok CqlResult
                }
                ImporterEvent::Shutdown => {
                    rt.update_status(ServiceStatus::Stopping).await;
                    let event = BrokerEvent::ImporterSession(ImporterSession::Finish {
                        from_ms: self.from_ms,
                        to_ms: self.to_ms,
                        msg: "failed".into(),
                    });
                    rt.supervisor_handle().send(event).ok();
                    return Err(ActorError::aborted_msg("Got shutdown in middle of importing session"));
                }
            }
        }
        let event = BrokerEvent::ImporterSession(ImporterSession::Finish {
            from_ms: self.from_ms,
            to_ms: self.to_ms,
            msg: "ok".into(),
        });
        rt.supervisor_handle().send(event).ok();
        Ok(())
    }
}
impl<T: ImportMode> Importer<T> {
    async fn init_importing(
        &mut self,
        importer_handle: &ImporterHandle,
        supervisor: &BrokerHandle,
    ) -> anyhow::Result<()> {
        for _ in 0..self.parallelism {
            if let Some(milestone_data) = self.next_milestone_data(supervisor).await? {
                T::handle_milestone_data(milestone_data, importer_handle, self)?;
            } else {
                self.eof = true;
                break;
            }
        }
        Ok(())
    }
    pub(crate) async fn next_milestone_data(
        &mut self,
        supervisor: &BrokerHandle,
    ) -> anyhow::Result<Option<MilestoneData>> {
        let log_file = self
            .log_file
            .as_mut()
            .ok_or_else(|| anyhow!("No LogFile in importer state"))?;
        let mut scan_budget: usize = 100;
        loop {
            let pre_len = log_file.len();
            if let Some(milestone_data) = log_file.next().await? {
                let milestone_index = milestone_data.milestone_index();
                let not_in_import_range = !self.import_range.contains(&milestone_index);
                let resume = self.resume && self.sync_data.completed.iter().any(|r| r.contains(&milestone_index));
                if resume || not_in_import_range {
                    warn!(
                        "Skipping imported milestone data for milestone index: {}",
                        milestone_index
                    );
                    let skipped = true;
                    let ms_bytes_size = (pre_len - log_file.len()) as usize;
                    Self::imported(
                        supervisor,
                        log_file.from_ms_index(),
                        log_file.to_ms_index(),
                        self.log_file_size,
                        milestone_index,
                        ms_bytes_size,
                        skipped,
                    );
                    // skip this synced milestone data
                    if scan_budget > 0 {
                        scan_budget -= 1;
                    } else {
                        scan_budget = 100;
                        tokio::task::yield_now().await;
                    }
                } else {
                    let ms_bytes_size = (pre_len - log_file.len()) as usize;
                    self.in_progress_milestones_data_bytes_size
                        .insert(milestone_index, ms_bytes_size);
                    return Ok(Some(milestone_data));
                }
            } else {
                return Ok(None);
            }
        }
    }
    pub(crate) fn imported(
        supervisor: &BrokerHandle,
        from_ms: u32,
        to_ms: u32,
        log_file_size: u64,
        milestone_index: u32,
        ms_bytes_size: usize,
        skipped: bool,
    ) {
        let importer_session = ImporterSession::ProgressBar {
            log_file_size,
            from_ms,
            to_ms,
            ms_bytes_size,
            milestone_index,
            skipped,
        };
        supervisor.send(BrokerEvent::ImporterSession(importer_session)).ok();
    }
    pub(crate) fn insert_some_messages(
        &mut self,
        milestone_index: u32,
        milestone_data: &mut IntoIter<MessageId, FullMessage>,
        importer_handle: &ImporterHandle,
    ) -> anyhow::Result<()> {
        let keyspace = self.default_keyspace.clone();
        let inherent_worker = MilestoneDataWorker::new(
            importer_handle.clone(),
            keyspace,
            milestone_index,
            self.retries as usize,
        );
        for _ in 0..self.parallelism {
            if let Some((message_id, FullMessage(message, metadata))) = milestone_data.next() {
                // Insert the message
                self.insert_message_with_metadata(&inherent_worker, message_id, message, metadata)?;
            } else {
                // break for loop
                break;
            }
        }
        Ok(())
    }
}

impl Importer<Analytics> {
    pub(crate) fn insert_analytic_record(
        &self,
        importer_handle: ImporterHandle,
        analytic_record: &AnalyticRecord,
    ) -> anyhow::Result<()> {
        let keyspace = self.default_keyspace.clone();
        let worker = AnalyzeWorker::boxed(
            importer_handle,
            keyspace,
            analytic_record.clone(),
            self.retries as usize,
        );
        if let Err(RequestError::Ring(r)) = self
            .default_keyspace
            .insert_prepared(&"permanode".to_string(), analytic_record)
            .consistency(Consistency::Quorum)
            .build()?
            .send_local_with_worker(worker)
        {
            let keyspace_name = self.default_keyspace.name();
            if let Err(worker) = retry_send(&keyspace_name, r, 2) {
                worker.handle_error(WorkerError::NoRing, None)?;
            };
        };

        Ok(())
    }
}

/// Scylla worker implementation for the importer
#[derive(Clone, Debug)]
pub struct AtomicImporterWorker<S, K, V>
where
    S: 'static + Insert<K, V> + std::fmt::Debug + Insert<String, SyncRecord>,
    K: 'static + Send,
    V: 'static + Send,
{
    handle: std::sync::Arc<AtomicImporterHandle<S>>,
    keyspace: S,
    key: K,
    value: V,
    retries: usize,
}
/// An atomic importer handle
#[derive(Debug)]
pub struct AtomicImporterHandle<S>
where
    S: 'static + Insert<String, SyncRecord> + std::fmt::Debug,
{
    /// The importer handle
    pub(crate) handle: ImporterHandle,
    /// The keyspace
    pub(crate) keyspace: S,
    /// The milestone index
    pub(crate) milestone_index: u32,
    /// The atomic flag to indicate any error
    pub(crate) any_error: std::sync::atomic::AtomicBool,
    /// The number of retires
    pub(crate) retries: usize,
}

impl<S> AtomicImporterHandle<S>
where
    S: 'static + Insert<String, SyncRecord> + std::fmt::Debug,
{
    /// Create a new atomic importer handle with an importer handle, a keyspace, a milestone index, an atomic error
    /// indicator, and a number of retires
    pub fn new(
        handle: ImporterHandle,
        keyspace: S,
        milestone_index: u32,
        any_error: std::sync::atomic::AtomicBool,
        retries: usize,
    ) -> Self {
        Self {
            handle,
            keyspace,
            milestone_index,
            any_error,
            retries,
        }
    }
}
impl<S: Insert<K, V>, K, V> AtomicImporterWorker<S, K, V>
where
    S: 'static + Insert<K, V> + Insert<String, SyncRecord> + std::fmt::Debug,
    K: 'static + Send + std::fmt::Debug,
    V: 'static + Send + std::fmt::Debug,
{
    /// Create a new atomic importer worker with an atomic importer handle, a keyspace, a key, a value, and a number of
    /// retries
    pub fn new(handle: std::sync::Arc<AtomicImporterHandle<S>>, key: K, value: V) -> Self {
        let keyspace = handle.keyspace.clone();
        let retries = handle.retries;
        Self {
            handle,
            keyspace,
            key,
            value,
            retries,
        }
    }
    /// Create a new boxed atomic importer worker with an atomic importer handle, a keyspace, a key, a value, and a
    /// number of retries
    pub fn boxed(handle: std::sync::Arc<AtomicImporterHandle<S>>, key: K, value: V) -> Box<Self> {
        Box::new(Self::new(handle, key, value))
    }
}

impl<S, K, V> Worker for AtomicImporterWorker<S, K, V>
where
    S: 'static + Insert<K, V> + std::fmt::Debug + Insert<String, SyncRecord> + Insert<String, AnalyticRecord>,
    K: 'static + Send + Clone + Sync + std::fmt::Debug + TokenEncoder,
    V: 'static + Send + Clone + Sync + std::fmt::Debug,
{
    fn handle_response(self: Box<Self>, giveload: Vec<u8>) -> anyhow::Result<()> {
        Decoder::from(giveload.try_into()?).get_void()
    }
    fn handle_error(
        mut self: Box<Self>,
        mut error: WorkerError,
        reporter: Option<&ReporterHandle>,
    ) -> anyhow::Result<()> {
        if let WorkerError::Cql(ref mut cql_error) = error {
            if let (Some(id), Some(reporter)) = (cql_error.take_unprepared_id(), reporter) {
                let keyspace_name = self.keyspace.name();
                let statement = self.keyspace.insert_statement::<K, V>();
                PrepareWorker::new(Some(keyspace_name), id, statement.into())
                    .send_to_reporter(reporter)
                    .ok();
            }
        }
        if self.retries > 0 {
            self.retries -= 1;
            // currently we assume all cql/worker errors are retryable, but we might change this in future
            let req = self
                .keyspace
                .insert_query(&self.key, &self.value)
                .consistency(Consistency::Quorum)
                .build()?;
            let keyspace_name = self.keyspace.name();
            if let Err(RequestError::Ring(r)) = req.send_global_with_worker(self) {
                if let Err(worker) = retry_send(&keyspace_name, r, 2) {
                    worker.handle_error(WorkerError::NoRing, None)?
                };
            };
        } else {
            // no more retries
            self.handle.any_error.store(true, Ordering::Relaxed);
        }
        Ok(())
    }
}

impl<S> Drop for AtomicImporterHandle<S>
where
    S: 'static + Insert<String, SyncRecord> + std::fmt::Debug,
{
    fn drop(&mut self) {
        let any_error = self.any_error.load(Ordering::Relaxed);
        if any_error {
            let _ = self.handle.send(ImporterEvent::CqlResult(Err(self.milestone_index)));
        } else {
            // tell importer to process more
            let _ = self.handle.send(ImporterEvent::ProcessMore(self.milestone_index));
        }
    }
}

/// Analyze allownd Sync worker for the importer
#[derive(Clone, Debug)]
pub struct AnalyzeAndSyncWorker<S>
where
    S: 'static + Insert<String, SyncRecord>,
{
    /// The importer handle
    handle: ImporterHandle,
    /// The keyspace
    keyspace: S,
    /// The `analytics` table row
    analytic_record: AnalyticRecord,
    /// Identify if we analyzed the
    analyzed: bool, // if true we sync
    /// The `sync` table row
    synced_record: SyncRecord,
    /// The number of retries
    retries: usize,
}

impl<S> AnalyzeAndSyncWorker<S>
where
    S: 'static + Insert<String, SyncRecord>,
{
    /// Create a new sync worker with an importer handle, a keyspace, a `sync` table row (`SyncRecord`), and a number of
    /// retries
    pub fn new(
        handle: ImporterHandle,
        keyspace: S,
        analytic_record: AnalyticRecord,
        synced_record: SyncRecord,
        retries: usize,
    ) -> Self {
        Self {
            handle,
            keyspace,
            analytic_record,
            synced_record,
            analyzed: false,
            retries,
        }
    }
    ///  Create a new boxed ync worker with an importer handle, a keyspace, a `sync` table row (`SyncRecord`), and a
    /// number of retries
    pub fn boxed(
        handle: ImporterHandle,
        keyspace: S,
        analytic_record: AnalyticRecord,
        synced_record: SyncRecord,
        retries: usize,
    ) -> Box<Self> {
        Box::new(Self::new(handle, keyspace, analytic_record, synced_record, retries))
    }
}

/// Implement the Scylla `Worker` trait
impl<S> Worker for AnalyzeAndSyncWorker<S>
where
    S: 'static + std::fmt::Debug + Insert<String, SyncRecord> + Insert<String, AnalyticRecord>,
{
    fn handle_response(mut self: Box<Self>, giveload: Vec<u8>) -> anyhow::Result<()> {
        Decoder::from(giveload.try_into()?).get_void()?;
        let milestone_index = *self.synced_record.milestone_index;
        if self.analyzed {
            // tell importer
            self.handle.send(ImporterEvent::CqlResult(Ok(milestone_index))).ok();
        } else {
            // set it to be analyzed, as this response is for analytic record
            self.analyzed = true;
            // insert sync record
            let keyspace_name = self.keyspace.name();
            if let Err(RequestError::Ring(r)) = self
                .keyspace
                .insert_prepared(&"permanode".to_string(), &self.synced_record)
                .consistency(Consistency::Quorum)
                .build()?
                .send_local_with_worker(self)
            {
                if let Err(worker) = retry_send(&keyspace_name, r, 2) {
                    worker.handle_error(WorkerError::NoRing, None)?;
                };
            };
        }
        Ok(())
    }
    fn handle_error(
        mut self: Box<Self>,
        mut error: WorkerError,
        reporter: Option<&ReporterHandle>,
    ) -> anyhow::Result<()> {
        if let WorkerError::Cql(ref mut cql_error) = error {
            if let (Some(id), Some(reporter)) = (cql_error.take_unprepared_id(), reporter) {
                let keyspace_name = self.keyspace.name();
                let statement;
                if self.analyzed {
                    statement = self.keyspace.insert_statement::<String, SyncRecord>();
                } else {
                    statement = self.keyspace.insert_statement::<String, AnalyticRecord>();
                }
                PrepareWorker::new(Some(keyspace_name), id, statement.into())
                    .send_to_reporter(reporter)
                    .ok();
            }
        }
        if self.retries > 0 {
            self.retries -= 1;
            // currently we assume all cql/worker errors are retryable, but we might change this in future
            if self.analyzed {
                // retry inserting an sync record
                let req = self
                    .keyspace
                    .insert_query(&"permanode".to_string(), &self.synced_record)
                    .consistency(Consistency::Quorum)
                    .build()?;
                let keyspace_name = self.keyspace.name();
                if let Err(RequestError::Ring(r)) = req.send_global_with_worker(self) {
                    if let Err(worker) = retry_send(&keyspace_name, r, 2) {
                        worker.handle_error(WorkerError::NoRing, None)?
                    };
                };
            } else {
                // retry inserting an analytic record
                let req = self
                    .keyspace
                    .insert_query(&"permanode".to_string(), &self.analytic_record)
                    .consistency(Consistency::Quorum)
                    .build()?;
                let keyspace_name = self.keyspace.name();
                if let Err(RequestError::Ring(r)) = req.send_global_with_worker(self) {
                    if let Err(worker) = retry_send(&keyspace_name, r, 2) {
                        worker.handle_error(WorkerError::NoRing, None)?
                    };
                };
            }
        } else {
            // no more retries
            // respond with error
            let milestone_index = *self.synced_record.milestone_index;
            let _ = self.handle.send(ImporterEvent::CqlResult(Err(milestone_index)));
        }
        Ok(())
    }
}

/// A milestone data worker
pub struct MilestoneDataWorker<S>
where
    S: 'static + Insert<String, SyncRecord> + std::fmt::Debug,
{
    /// The arced atomic importer handle for a given keyspace
    arc_handle: std::sync::Arc<AtomicImporterHandle<S>>,
}

impl<S> MilestoneDataWorker<S>
where
    S: 'static + Insert<String, SyncRecord> + std::fmt::Debug,
{
    /// Create a new milestone data worker with an importer handle, a keyspace, a milestone index, and a number of
    /// retries
    fn new(importer_handle: ImporterHandle, keyspace: S, milestone_index: u32, retries: usize) -> Self {
        let any_error = std::sync::atomic::AtomicBool::new(false);
        let atomic_handle =
            AtomicImporterHandle::new(importer_handle, keyspace.clone(), milestone_index, any_error, retries);
        let arc_handle = std::sync::Arc::new(atomic_handle);
        Self { arc_handle }
    }
}

/// Implement the `Inherent` trait for the milestone data worker, so we can get the atomic importer worker
/// which contains the atomic importer handle of the milestone data worker
impl<K, V> Inherent<ChronicleKeyspace, K, V> for MilestoneDataWorker<ChronicleKeyspace>
where
    K: 'static + Send + Sync + Clone + TokenEncoder + Debug,
    V: 'static + Send + Sync + Clone + Debug,
    ChronicleKeyspace: Insert<K, V>,
{
    type Output = AtomicImporterWorker<ChronicleKeyspace, K, V>;

    fn inherent_boxed(&self, _keyspace: ChronicleKeyspace, key: K, value: V) -> Box<Self::Output>
    where
        ChronicleKeyspace: 'static + Insert<K, V> + Debug,
        K: 'static + Send + Sync + Clone + Debug + TokenEncoder,
        V: 'static + Send + Sync + Clone + Debug,
    {
        AtomicImporterWorker::boxed(self.arc_handle.clone(), key, value)
    }
}

/// Scylla worker implementation for importer when running in Analytics mode
#[derive(Clone, Debug)]
pub struct AnalyzeWorker<S>
where
    S: 'static + Insert<String, AnalyticRecord>,
{
    /// The importer handle
    handle: ImporterHandle,
    /// The keyspace
    keyspace: S,
    /// The `analytics` table row
    analytic_record: AnalyticRecord,
    /// The number of retries
    retries: usize,
}

impl<S> AnalyzeWorker<S>
where
    S: 'static + Insert<String, AnalyticRecord>,
{
    /// Create a new AnalyzeWorker with an importer handle, a keyspace, a `analytics` table row (`AnalyticRecord`), and
    /// a number of retries
    pub fn new(handle: ImporterHandle, keyspace: S, analytic_record: AnalyticRecord, retries: usize) -> Self {
        Self {
            handle,
            keyspace,
            analytic_record,
            retries,
        }
    }
    /// Create a new boxed AnalyzeWorker with an importer handle, a keyspace, a `analytics` table row
    /// (`AnalyticRecord`), and a number of retries
    pub fn boxed(handle: ImporterHandle, keyspace: S, analytic_record: AnalyticRecord, retries: usize) -> Box<Self> {
        Box::new(Self::new(handle, keyspace, analytic_record, retries))
    }
}

/// Implement the Scylla `Worker` trait
impl<S> Worker for AnalyzeWorker<S>
where
    S: 'static + std::fmt::Debug + Insert<String, AnalyticRecord>,
{
    fn handle_response(self: Box<Self>, giveload: Vec<u8>) -> anyhow::Result<()> {
        Decoder::from(giveload.try_into()?).get_void()?;
        let milestone_index = self.analytic_record.milestone_index();
        self.handle.send(ImporterEvent::CqlResult(Ok(**milestone_index))).ok();
        Ok(())
    }
    fn handle_error(
        mut self: Box<Self>,
        mut error: WorkerError,
        reporter: Option<&ReporterHandle>,
    ) -> anyhow::Result<()> {
        if let WorkerError::Cql(ref mut cql_error) = error {
            if let (Some(id), Some(reporter)) = (cql_error.take_unprepared_id(), reporter) {
                let keyspace_name = self.keyspace.name();
                let statement = self.keyspace.statement();
                PrepareWorker::new(Some(keyspace_name), id, statement.into())
                    .send_to_reporter(reporter)
                    .ok();
            }
        }
        if self.retries > 0 {
            self.retries -= 1;
            // currently we assume all cql/worker errors are retryable, but we might change this in future
            // retry inserting an analytic record
            let req = self
                .keyspace
                .insert_query(&"permanode".to_string(), &self.analytic_record)
                .consistency(Consistency::Quorum)
                .build()?;
            let keyspace_name = self.keyspace.name();
            if let Err(RequestError::Ring(r)) = req.send_global_with_worker(self) {
                if let Err(worker) = retry_send(&keyspace_name, r, 2) {
                    worker.handle_error(WorkerError::NoRing, None)?
                };
            };
        } else {
            // no more retries
            // respond with error
            let milestone_index = self.analytic_record.milestone_index();
            let _ = self.handle.send(ImporterEvent::CqlResult(Err(**milestone_index)));
        }
        Ok(())
    }
}

impl<T: ImportMode> Importer<T> {
    pub(crate) fn get_keyspace(&self) -> ChronicleKeyspace {
        self.default_keyspace.clone()
    }
    fn get_partition_id(&self, milestone_index: MilestoneIndex) -> u16 {
        self.partition_config.partition_id(milestone_index.0)
    }
    pub(crate) fn insert_message_with_metadata<
        I: Inherent<ChronicleKeyspace, Bee<MessageId>, (Message, MessageMetadata)>
            + Inherent<ChronicleKeyspace, Partitioned<Bee<MessageId>>, ParentRecord>
            + Inherent<ChronicleKeyspace, Bee<MilestoneIndex>, (Bee<MessageId>, Box<MilestonePayload>)>
            + Inherent<ChronicleKeyspace, Hint, Partition>
            + Inherent<ChronicleKeyspace, Partitioned<Indexation>, IndexationRecord>
            + Inherent<ChronicleKeyspace, Partitioned<Bee<Ed25519Address>>, AddressRecord>
            + Inherent<ChronicleKeyspace, Bee<TransactionId>, TransactionRecord>,
    >(
        &mut self,
        inherent_worker: &I,
        message_id: MessageId,
        message: Message,
        metadata: MessageMetadata,
    ) -> anyhow::Result<()> {
        let milestone_index = metadata
            .referenced_by_milestone_index
            .expect("Expected referenced milestone index in metadata");
        // Insert parents/children
        self.insert_parents(
            inherent_worker,
            &message_id,
            &message.parents(),
            MilestoneIndex(milestone_index),
            metadata.ledger_inclusion_state.clone(),
        )?;
        // insert payload (if any)
        if let Some(payload) = message.payload() {
            self.insert_payload(
                inherent_worker,
                &message_id,
                &message,
                &payload,
                MilestoneIndex(milestone_index),
                metadata.ledger_inclusion_state.clone(),
                Some(metadata.clone()),
            )?;
        }
        let message_tuple = (message, metadata);
        // store message and metadata
        self.insert(inherent_worker, Bee(message_id), message_tuple)
    }

    fn insert_parents<
        I: Inherent<ChronicleKeyspace, Partitioned<Bee<MessageId>>, ParentRecord>
            + Inherent<ChronicleKeyspace, Hint, Partition>,
    >(
        &self,
        inherent_worker: &I,
        message_id: &MessageId,
        parents: &[MessageId],
        milestone_index: MilestoneIndex,
        inclusion_state: Option<LedgerInclusionState>,
    ) -> anyhow::Result<()> {
        let partition_id = self.get_partition_id(milestone_index);
        for parent_id in parents {
            let partitioned = Partitioned::new(Bee(*parent_id), partition_id);
            let parent_record = ParentRecord::new(milestone_index, *message_id, inclusion_state);
            self.insert(inherent_worker, partitioned, parent_record)?;
            // insert hint record
            let hint = Hint::parent(parent_id.to_string());
            let partition = Partition::new(partition_id, *milestone_index);
            self.insert(inherent_worker, hint, partition)?;
        }
        Ok(())
    }
    fn insert_payload<
        I: Inherent<ChronicleKeyspace, Bee<MilestoneIndex>, (Bee<MessageId>, Box<MilestonePayload>)>
            + Inherent<ChronicleKeyspace, Partitioned<Indexation>, IndexationRecord>
            + Inherent<ChronicleKeyspace, Hint, Partition>
            + Inherent<ChronicleKeyspace, Partitioned<Bee<Ed25519Address>>, AddressRecord>
            + Inherent<ChronicleKeyspace, Bee<TransactionId>, TransactionRecord>,
    >(
        &mut self,
        inherent_worker: &I,
        message_id: &MessageId,
        message: &Message,
        payload: &Payload,
        milestone_index: MilestoneIndex,
        inclusion_state: Option<LedgerInclusionState>,
        metadata: Option<MessageMetadata>,
    ) -> anyhow::Result<()> {
        match payload {
            Payload::Indexation(indexation) => {
                self.insert_index(
                    inherent_worker,
                    message_id,
                    Indexation(hex::encode(indexation.index())),
                    milestone_index,
                    inclusion_state,
                )?;
            }
            Payload::Transaction(transaction) => {
                self.insert_transaction(
                    inherent_worker,
                    message_id,
                    message,
                    transaction,
                    inclusion_state,
                    milestone_index,
                    metadata,
                )?;
            }
            Payload::Milestone(milestone) => {
                let ms_index = *milestone.essence().index();
                let parents_check = message.parents().eq(milestone.essence().parents());
                if metadata.is_some() && parents_check {
                    self.insert(
                        inherent_worker,
                        Bee(MilestoneIndex(ms_index)),
                        (Bee(*message_id), milestone.clone()),
                    )?
                }
            }
            e => {
                // Skip remaining payload types.
                warn!("Skipping unsupported payload variant: {:?}", e);
            }
        }
        Ok(())
    }
    fn insert_index<
        I: Inherent<ChronicleKeyspace, Partitioned<Indexation>, IndexationRecord>
            + Inherent<ChronicleKeyspace, Hint, Partition>,
    >(
        &self,
        inherent_worker: &I,
        message_id: &MessageId,
        index: Indexation,
        milestone_index: MilestoneIndex,
        inclusion_state: Option<LedgerInclusionState>,
    ) -> anyhow::Result<()> {
        let partition_id = self.get_partition_id(milestone_index);
        let partitioned = Partitioned::new(index.clone(), partition_id);
        let index_record = IndexationRecord::new(milestone_index, *message_id, inclusion_state);
        self.insert(inherent_worker, partitioned, index_record)?;
        // insert hint record
        let hint = Hint::index(index.0);
        let partition = Partition::new(partition_id, *milestone_index);
        self.insert(inherent_worker, hint, partition)
    }
    fn insert_transaction<
        I: Inherent<ChronicleKeyspace, Partitioned<Bee<Ed25519Address>>, AddressRecord>
            + Inherent<ChronicleKeyspace, Hint, Partition>
            + Inherent<ChronicleKeyspace, Bee<TransactionId>, TransactionRecord>
            + Inherent<ChronicleKeyspace, Bee<MilestoneIndex>, (Bee<MessageId>, Box<MilestonePayload>)>
            + Inherent<ChronicleKeyspace, Partitioned<Indexation>, IndexationRecord>,
    >(
        &mut self,
        inherent_worker: &I,
        message_id: &MessageId,
        message: &Message,
        transaction: &Box<TransactionPayload>,
        ledger_inclusion_state: Option<LedgerInclusionState>,
        milestone_index: MilestoneIndex,
        metadata: Option<MessageMetadata>,
    ) -> anyhow::Result<()> {
        let transaction_id = transaction.id();
        let unlock_blocks = transaction.unlock_blocks();
        let confirmed_milestone_index;
        if ledger_inclusion_state.is_some() {
            confirmed_milestone_index = Some(milestone_index);
        } else {
            confirmed_milestone_index = None;
        }
        let Essence::Regular(regular) = transaction.essence();
        {
            for (input_index, input) in regular.inputs().iter().enumerate() {
                // insert utxoinput row along with input row
                if let Input::Utxo(utxo_input) = input {
                    let unlock_block = &unlock_blocks[input_index];
                    let input_data = InputData::utxo(utxo_input.clone(), unlock_block.clone());
                    // insert input row
                    self.insert_input(
                        inherent_worker,
                        message_id,
                        &transaction_id,
                        input_index as u16,
                        input_data,
                        ledger_inclusion_state,
                        confirmed_milestone_index,
                    )?;
                    // this is the spent_output which the input is spending from
                    let output_id = utxo_input.output_id();
                    // therefore we insert utxo_input.output_id() -> unlock_block to indicate that this output is_spent;
                    let unlock_data = UnlockData::new(transaction_id, input_index as u16, unlock_block.clone());
                    self.insert_unlock(
                        inherent_worker,
                        &message_id,
                        output_id.transaction_id(),
                        output_id.index(),
                        unlock_data,
                        ledger_inclusion_state,
                        confirmed_milestone_index,
                    )?;
                } else if let Input::Treasury(treasury_input) = input {
                    let input_data = InputData::treasury(treasury_input.clone());
                    // insert input row
                    self.insert_input(
                        inherent_worker,
                        message_id,
                        &transaction_id,
                        input_index as u16,
                        input_data,
                        ledger_inclusion_state,
                        confirmed_milestone_index,
                    )?;
                };
            }
            for (output_index, output) in regular.outputs().iter().enumerate() {
                // insert output row
                self.insert_output(
                    inherent_worker,
                    message_id,
                    &transaction_id,
                    output_index as u16,
                    output.clone(),
                    ledger_inclusion_state,
                    confirmed_milestone_index,
                )?;
                // insert address row
                self.insert_address(
                    inherent_worker,
                    output,
                    &transaction_id,
                    output_index as u16,
                    milestone_index,
                    ledger_inclusion_state,
                )?;
            }
            if let Some(payload) = regular.payload() {
                self.insert_payload(
                    inherent_worker,
                    message_id,
                    message,
                    payload,
                    milestone_index,
                    ledger_inclusion_state,
                    metadata,
                )?;
            }
        };
        Ok(())
    }
    fn insert_input<I: Inherent<ChronicleKeyspace, Bee<TransactionId>, TransactionRecord>>(
        &self,
        inherent_worker: &I,
        message_id: &MessageId,
        transaction_id: &TransactionId,
        index: u16,
        input_data: InputData,
        inclusion_state: Option<LedgerInclusionState>,
        milestone_index: Option<MilestoneIndex>,
    ) -> anyhow::Result<()> {
        // -input variant: (InputTransactionId, InputIndex) -> UTXOInput data column
        let input_id = Bee(*transaction_id);
        let transaction_record =
            TransactionRecord::input(index, *message_id, input_data, inclusion_state, milestone_index);
        self.insert(inherent_worker, input_id, transaction_record)
    }
    fn insert_unlock<I: Inherent<ChronicleKeyspace, Bee<TransactionId>, TransactionRecord>>(
        &self,
        inherent_worker: &I,
        message_id: &MessageId,
        utxo_transaction_id: &TransactionId,
        utxo_index: u16,
        unlock_data: UnlockData,
        inclusion_state: Option<LedgerInclusionState>,
        milestone_index: Option<MilestoneIndex>,
    ) -> anyhow::Result<()> {
        // -unlock variant: (UtxoInputTransactionId, UtxoInputOutputIndex) -> Unlock data column
        let utxo_id = Bee(*utxo_transaction_id);
        let transaction_record =
            TransactionRecord::unlock(utxo_index, *message_id, unlock_data, inclusion_state, milestone_index);
        self.insert(inherent_worker, utxo_id, transaction_record)
    }
    fn insert_output<I: Inherent<ChronicleKeyspace, Bee<TransactionId>, TransactionRecord>>(
        &self,
        inherent_worker: &I,
        message_id: &MessageId,
        transaction_id: &TransactionId,
        index: u16,
        output: Output,
        inclusion_state: Option<LedgerInclusionState>,
        milestone_index: Option<MilestoneIndex>,
    ) -> anyhow::Result<()> {
        // -output variant: (OutputTransactionId, OutputIndex) -> Output data column
        let output_id = Bee(*transaction_id);
        let transaction_record =
            TransactionRecord::output(index, *message_id, output, inclusion_state, milestone_index);
        self.insert(inherent_worker, output_id, transaction_record)
    }
    fn insert_address<
        I: Inherent<ChronicleKeyspace, Partitioned<Bee<Ed25519Address>>, AddressRecord>
            + Inherent<ChronicleKeyspace, Hint, Partition>,
    >(
        &self,
        inherent_worker: &I,
        output: &Output,
        transaction_id: &TransactionId,
        index: u16,
        milestone_index: MilestoneIndex,
        inclusion_state: Option<LedgerInclusionState>,
    ) -> anyhow::Result<()> {
        let partition_id = self.get_partition_id(milestone_index);
        let output_type = output.kind();
        match output {
            Output::SignatureLockedSingle(sls) => {
                let Address::Ed25519(ed_address) = sls.address();
                {
                    let partitioned = Partitioned::new(Bee(*ed_address), partition_id);
                    let address_record = AddressRecord::new(
                        milestone_index,
                        output_type,
                        *transaction_id,
                        index,
                        sls.amount(),
                        inclusion_state,
                    );
                    self.insert(inherent_worker, partitioned, address_record)?;
                    // insert hint record
                    let hint = Hint::address(ed_address.to_string());
                    let partition = Partition::new(partition_id, *milestone_index);
                    self.insert(inherent_worker, hint, partition)?;
                };
            }
            Output::SignatureLockedDustAllowance(slda) => {
                let Address::Ed25519(ed_address) = slda.address();
                {
                    let partitioned = Partitioned::new(Bee(*ed_address), partition_id);
                    let address_record = AddressRecord::new(
                        milestone_index,
                        output_type,
                        *transaction_id,
                        index,
                        slda.amount(),
                        inclusion_state,
                    );
                    self.insert(inherent_worker, partitioned, address_record)?;
                    // insert hint record
                    let hint = Hint::address(ed_address.to_string());
                    let partition = Partition::new(partition_id, *milestone_index);
                    self.insert(inherent_worker, hint, partition)?;
                };
            }
            e => {
                if let Output::Treasury(_) = e {
                } else {
                    error!("Unexpected new output variant {:?}", e);
                }
            }
        }
        Ok(())
    }
    /// The low-level insert function to insert a key/value pair through an inherent worker
    fn insert<I, K, V>(&self, inherent_worker: &I, key: K, value: V) -> anyhow::Result<()>
    where
        I: Inherent<ChronicleKeyspace, K, V>,
        ChronicleKeyspace: 'static + Insert<K, V>,
        K: 'static + Send + Sync + Clone + std::fmt::Debug + TokenEncoder,
        V: 'static + Send + Sync + Clone + std::fmt::Debug,
    {
        let insert_req = self
            .default_keyspace
            .insert(&key, &value)
            .consistency(Consistency::Quorum)
            .build()?;
        let worker = inherent_worker.inherent_boxed(self.get_keyspace(), key, value);
        if let Err(RequestError::Ring(r)) = insert_req.send_local_with_worker(worker) {
            let keyspace_name = self.default_keyspace.name();
            if let Err(worker) = retry_send(&keyspace_name, r, 2) {
                worker.handle_error(WorkerError::NoRing, None)?;
            };
        };
        Ok(())
    }
}
