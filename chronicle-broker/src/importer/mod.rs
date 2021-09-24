// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0
use super::*;
use crate::{
    application::{
        BrokerChild,
        BrokerEvent,
        BrokerHandle,
        ChronicleBrokerScope,
    },
    archiver::LogFile,
};
use bee_message::{
    output::Output,
    payload::transaction::{
        Essence,
        TransactionPayload,
    },
};
use chronicle_common::{
    config::PartitionConfig,
    Synckey,
};
use chronicle_storage::access::SyncRecord;
use scylla_rs::{
    app::worker::handle_insert_unprepared_error,
    prelude::stage::ReporterHandle,
};
use std::{
    collections::hash_map::IntoIter,
    ops::{
        Deref,
        DerefMut,
        Range,
    },
    path::PathBuf,
    sync::atomic::Ordering,
};

mod event_loop;
mod init;
mod terminating;

/// Import all records to all tables
pub struct All;
/// Import analytics records only which are stored in analytics table
pub struct Analytics;

/// Defines the Importer Mode
pub trait ImportMode: Sized + Send + 'static {
    /// Instruct how to import the milestone data
    fn handle_milestone_data(milestone_data: MilestoneData, importer: &mut Importer<Self>) -> anyhow::Result<()>;
}
impl ImportMode for All {
    fn handle_milestone_data(milestone_data: MilestoneData, importer: &mut Importer<All>) -> anyhow::Result<()> {
        let analytic_record = milestone_data.get_analytic_record().map_err(|e| {
            error!("Unable to get analytic record for milestone data. Error: {}", e);
            e
        })?;
        let milestone_index = milestone_data.milestone_index();
        let mut iterator = milestone_data.into_iter();
        importer.insert_some_messages(milestone_index, &mut iterator)?;
        importer
            .in_progress_milestones_data
            .insert(milestone_index, (iterator, analytic_record));
        Ok(())
    }
}

impl ImportMode for Analytics {
    fn handle_milestone_data(milestone_data: MilestoneData, importer: &mut Importer<Analytics>) -> anyhow::Result<()> {
        let analytic_record = milestone_data.get_analytic_record().map_err(|e| {
            error!("Unable to get analytic record for milestone data. Error: {}", e);
            e
        })?;
        let milestone_index = milestone_data.milestone_index();
        let iterator = milestone_data.into_iter();
        importer.insert_analytic_record(&analytic_record)?;
        // note: iterator is not needed to presist analytic record in Analytics mode,
        // however we kept them for simplicty sake.
        importer
            .in_progress_milestones_data
            .insert(milestone_index, (iterator, analytic_record));
        Ok(())
    }
}
// Importer builder
builder!(ImporterBuilder<T> {
    file_path: PathBuf,
    retries_per_query: usize,
    resume: bool,
    import_range: Range<u32>,
    parallelism: u8,
    chronicle_id: u8
});

/// Importer events
pub enum ImporterEvent {
    /// The result of an insert into the database
    CqlResult(Result<u32, u32>),
    /// Indicator to continue processing
    ProcessMore(u32),
    /// Shutdown the importer
    Shutdown,
}

/// ImporterHandle to be passed to children(Inserter) and the supervisor(in order to shutdown)
#[derive(Clone)]
pub struct ImporterHandle {
    pub(crate) tx: tokio::sync::mpsc::UnboundedSender<ImporterEvent>,
}

/// ImporterInbox is used to recv events
pub struct ImporterInbox {
    pub(crate) rx: tokio::sync::mpsc::UnboundedReceiver<ImporterEvent>,
}
impl Deref for ImporterHandle {
    type Target = tokio::sync::mpsc::UnboundedSender<ImporterEvent>;

    fn deref(&self) -> &Self::Target {
        &self.tx
    }
}

impl DerefMut for ImporterHandle {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.tx
    }
}

impl Deref for ImporterInbox {
    type Target = tokio::sync::mpsc::UnboundedReceiver<ImporterEvent>;

    fn deref(&self) -> &Self::Target {
        &self.rx
    }
}

impl DerefMut for ImporterInbox {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.rx
    }
}

impl Shutdown for ImporterHandle {
    fn shutdown(self) -> Option<Self>
    where
        Self: Sized,
    {
        let shutdown_event = ImporterEvent::Shutdown;
        self.send(shutdown_event).ok();
        None
    }
}
/// Importer state
pub struct Importer<T> {
    /// The importer service
    service: Service,
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
    /// The partition configuration
    partition_config: PartitionConfig,
    /// The number of retires per query
    retries_per_query: usize,
    /// The chronicle id
    chronicle_id: u8,
    /// The number of parallelism
    parallelism: u8,
    /// The resume flag
    resume: bool,
    /// The range of requested milestones to import
    import_range: Range<u32>,
    /// The database sync data
    sync_data: SyncData,
    /// In progress milestones data
    in_progress_milestones_data: HashMap<u32, (IntoIter<MessageId, FullMessage>, AnalyticRecord)>,
    in_progress_milestones_data_bytes_size: HashMap<u32, usize>,
    /// The importer handle
    handle: Option<ImporterHandle>,
    /// The importer inbox to receive events
    inbox: ImporterInbox,
    /// The flag of end of file
    eof: bool,
    /// Import mode marker
    _mode: std::marker::PhantomData<T>,
}

impl<H: ChronicleBrokerScope, T: ImportMode> ActorBuilder<BrokerHandle<H>> for ImporterBuilder<T> {}

/// Implementation of builder
impl<T: ImportMode> Builder for ImporterBuilder<T> {
    type State = Importer<T>;
    fn build(self) -> Self::State {
        // Get the first keyspace or default to "permanode"
        // In order to use multiple keyspaces, the user must
        // use filters to determine where records go
        let config = chronicle_common::get_config();
        let default_keyspace = ChronicleKeyspace::new(
            config
                .storage_config
                .keyspaces
                .first()
                .and_then(|keyspace| Some(keyspace.name.clone()))
                .unwrap_or("permanode".to_owned()),
        );
        let partition_config = config.storage_config.partition_config;
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let handle = Some(ImporterHandle { tx });
        let inbox = ImporterInbox { rx };
        let import_range = self.import_range.unwrap_or(Range {
            start: 1,
            end: i32::MAX as u32,
        });
        Self::State {
            service: Service::new(),
            file_path: self.file_path.unwrap(),
            log_file: None,
            log_file_size: 0,
            from_ms: 0,
            to_ms: 0,
            default_keyspace,
            partition_config,
            parallelism: self.parallelism.unwrap_or(10),
            chronicle_id: self.chronicle_id.unwrap(),
            in_progress_milestones_data: HashMap::new(),
            in_progress_milestones_data_bytes_size: HashMap::new(),
            retries_per_query: self.retries_per_query.unwrap_or(10),
            resume: self.resume.unwrap_or(true),
            import_range,
            sync_data: SyncData::default(),
            handle,
            inbox,
            eof: false,
            _mode: std::marker::PhantomData::<T>,
        }
        .set_name()
    }
}

impl<T> Importer<T> {
    pub(crate) fn clone_handle(&self) -> Option<ImporterHandle> {
        self.handle.clone()
    }
}
/// Implement `Name` trait of the Importer
impl<T> Name for Importer<T> {
    fn set_name(mut self) -> Self {
        let name = format!("{}", self.file_path.to_str().unwrap());
        self.service.update_name(name);
        self
    }
    fn get_name(&self) -> String {
        self.service.get_name()
    }
}
impl Importer<Analytics> {
    pub(crate) fn insert_analytic_record(&self, analytic_record: &AnalyticRecord) -> anyhow::Result<()> {
        if let Some(importer_handle) = self.handle.clone() {
            let keyspace = self.get_keyspace();
            let worker = AnalyzeWorker::boxed(
                importer_handle,
                keyspace,
                analytic_record.clone(),
                self.retries_per_query,
            );
            self.default_keyspace
                .insert_prepared(&Synckey, analytic_record)
                .consistency(Consistency::One)
                .build()?
                .send_local(worker);
            Ok(())
        } else {
            bail!("Expected importer handle in order to import/insert analytic record");
        }
    }
}
#[async_trait::async_trait]
impl<H: ChronicleBrokerScope, T: ImportMode> AknShutdown<Importer<T>> for BrokerHandle<H> {
    async fn aknowledge_shutdown(self, mut state: Importer<T>, status: Result<(), Need>) {
        let parallelism = state.parallelism;
        state.service.update_status(ServiceStatus::Stopped);
        let event = BrokerEvent::Children(BrokerChild::Importer(state.service.clone(), status, parallelism));
        let _ = self.send(event);
    }
}

/// Scylla worker implementation for the importer
#[derive(Clone)]
pub struct AtomicImporterWorker<S, K, V>
where
    S: 'static + Insert<K, V> + Insert<Synckey, SyncRecord>,
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
pub struct AtomicImporterHandle<S>
where
    S: 'static + Insert<Synckey, SyncRecord>,
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
    S: 'static + Insert<Synckey, SyncRecord>,
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
    S: 'static + Insert<K, V> + Insert<Synckey, SyncRecord>,
    K: 'static + Send,
    V: 'static + Send,
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
    S: 'static + Insert<K, V> + Insert<Synckey, SyncRecord> + Insert<Synckey, AnalyticRecord>,
    K: 'static + Send + Clone,
    V: 'static + Send + Clone,
{
    fn handle_response(self: Box<Self>, giveload: Vec<u8>) -> anyhow::Result<()> {
        Decoder::from(giveload.try_into()?).get_void()
    }
    fn handle_error(
        mut self: Box<Self>,
        mut error: WorkerError,
        reporter: &Option<ReporterHandle>,
    ) -> anyhow::Result<()> {
        if let WorkerError::Cql(ref mut cql_error) = error {
            if let (Some(id), Some(reporter)) = (cql_error.take_unprepared_id(), reporter) {
                handle_insert_unprepared_error(&self, &self.keyspace, &self.key, &self.value, id, reporter)?;
                return Ok(());
            }
        }
        if self.retries > 0 {
            self.retries -= 1;
            // currently we assume all cql/worker errors are retryable, but we might change this in future
            let req = self
                .keyspace
                .insert_query(&self.key, &self.value)
                .consistency(Consistency::One)
                .build()?;
            tokio::spawn(async { req.send_global(self) });
        } else {
            // no more retries
            self.handle.any_error.store(true, Ordering::Relaxed);
        }
        Ok(())
    }
}

impl<S> Drop for AtomicImporterHandle<S>
where
    S: 'static + Insert<Synckey, SyncRecord>,
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
#[derive(Clone)]
pub struct AnalyzeAndSyncWorker<S>
where
    S: 'static + Insert<Synckey, SyncRecord>,
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
    S: 'static + Insert<Synckey, SyncRecord>,
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
    S: 'static + Insert<Synckey, SyncRecord> + Insert<Synckey, AnalyticRecord>,
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
            let req = self
                .keyspace
                .insert_prepared(&Synckey, &self.synced_record)
                .consistency(Consistency::One)
                .build()?;
            req.send_local(self);
        }
        Ok(())
    }
    fn handle_error(
        mut self: Box<Self>,
        mut error: WorkerError,
        reporter: &Option<ReporterHandle>,
    ) -> anyhow::Result<()> {
        if let WorkerError::Cql(ref mut cql_error) = error {
            if let (Some(id), Some(reporter)) = (cql_error.take_unprepared_id(), reporter) {
                // check if the worker is handling analyzed or synced record error
                if self.analyzed {
                    handle_insert_unprepared_error(&self, &self.keyspace, &Synckey, &self.synced_record, id, reporter)?;
                } else {
                    handle_insert_unprepared_error(
                        &self,
                        &self.keyspace,
                        &Synckey,
                        &self.analytic_record,
                        id,
                        reporter,
                    )?;
                }
                return Ok(());
            }
        }
        if self.retries > 0 {
            self.retries -= 1;
            // currently we assume all cql/worker errors are retryable, but we might change this in future
            if self.analyzed {
                // retry inserting an sync record
                let req = self
                    .keyspace
                    .insert_query(&Synckey, &self.synced_record)
                    .consistency(Consistency::One)
                    .build()?;
                tokio::spawn(async { req.send_global(self) });
            } else {
                // retry inserting an analytic record
                let req = self
                    .keyspace
                    .insert_query(&Synckey, &self.analytic_record)
                    .consistency(Consistency::One)
                    .build()?;
                tokio::spawn(async { req.send_global(self) });
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
    S: 'static + Insert<Synckey, SyncRecord>,
{
    /// The arced atomic importer handle for a given keyspace
    arc_handle: std::sync::Arc<AtomicImporterHandle<S>>,
}

impl<S> MilestoneDataWorker<S>
where
    S: 'static + Insert<Synckey, SyncRecord>,
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

/// The inherent trait to return a boxed worker for a given key/value pair
pub(crate) trait Inherent {
    fn inherent_boxed<K, V>(&self, key: K, value: V) -> Box<dyn Worker>
    where
        ChronicleKeyspace: 'static + Insert<K, V> + Insert<Synckey, SyncRecord>,
        K: 'static + Send + Clone,
        V: 'static + Send + Clone;
}

/// Implement the `Inherent` trait for the milestone data worker, so we can get the atomic importer worker
/// which contains the atomic importer handle of the milestone data worker
impl Inherent for MilestoneDataWorker<ChronicleKeyspace> {
    fn inherent_boxed<K, V>(&self, key: K, value: V) -> Box<dyn Worker>
    where
        ChronicleKeyspace: 'static + Insert<K, V> + Insert<Synckey, SyncRecord>,
        K: 'static + Send + Clone,
        V: 'static + Send + Clone,
    {
        AtomicImporterWorker::boxed(self.arc_handle.clone(), key, value)
    }
}

/// Scylla worker implementation for importer when running in Analytics mode
#[derive(Clone)]
pub struct AnalyzeWorker<S>
where
    S: 'static + Insert<Synckey, AnalyticRecord>,
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
    S: 'static + Insert<Synckey, AnalyticRecord>,
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
    S: 'static + Insert<Synckey, AnalyticRecord>,
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
        reporter: &Option<ReporterHandle>,
    ) -> anyhow::Result<()> {
        if let WorkerError::Cql(ref mut cql_error) = error {
            if let (Some(id), Some(reporter)) = (cql_error.take_unprepared_id(), reporter) {
                handle_insert_unprepared_error(&self, &self.keyspace, &Synckey, &self.analytic_record, id, reporter)?;
                return Ok(());
            }
        }
        if self.retries > 0 {
            self.retries -= 1;
            // currently we assume all cql/worker errors are retryable, but we might change this in future
            // retry inserting an analytic record
            let req = self
                .keyspace
                .insert_query(&Synckey, &self.analytic_record)
                .consistency(Consistency::One)
                .build()?;
            tokio::spawn(async { req.send_global(self) });
        } else {
            // no more retries
            // respond with error
            let milestone_index = self.analytic_record.milestone_index();
            let _ = self.handle.send(ImporterEvent::CqlResult(Err(**milestone_index)));
        }
        Ok(())
    }
}
