// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0
use crate::{
    application::*,
    archiver::LogFile,
    solidifier::{
        FullMessage,
        MilestoneData,
    },
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
use std::{
    collections::hash_map::IntoIter,
    ops::{
        Deref,
        DerefMut,
    },
    sync::atomic::Ordering,
};

mod event_loop;
mod init;
mod terminating;

// Importer builder
builder!(ImporterBuilder {
    file_path: PathBuf,
    retries_per_query: usize,
    resume: bool,
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
pub struct Importer {
    /// The importer service
    service: Service,
    /// The file path of the importer
    file_path: PathBuf,
    /// The log file
    log_file: Option<LogFile>,
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
    /// The database sync data
    sync_data: SyncData,
    /// The map from the milestones to the data in progress
    in_progress_milestones_data: HashMap<u32, IntoIter<MessageId, FullMessage>>,
    /// The importer handle
    handle: Option<ImporterHandle>,
    /// The importer inbox to receive events
    inbox: ImporterInbox,
    /// The flag of end of file
    eof: bool,
}

impl<H: ChronicleBrokerScope> ActorBuilder<BrokerHandle<H>> for ImporterBuilder {}

/// Implementation of builder
impl Builder for ImporterBuilder {
    type State = Importer;
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
        Self::State {
            service: Service::new(),
            file_path: self.file_path.unwrap(),
            log_file: None,
            default_keyspace,
            partition_config,
            parallelism: self.parallelism.unwrap_or(10),
            chronicle_id: self.chronicle_id.unwrap(),
            in_progress_milestones_data: HashMap::new(),
            retries_per_query: self.retries_per_query.unwrap_or(10),
            resume: self.resume.unwrap_or(true),
            sync_data: SyncData::default(),
            handle,
            inbox,
            eof: false,
        }
        .set_name()
    }
}

/// Implement `Name` trait of the Importer
impl Name for Importer {
    fn set_name(mut self) -> Self {
        let name = format!("{}", self.file_path.to_str().unwrap());
        self.service.update_name(name);
        self
    }
    fn get_name(&self) -> String {
        self.service.get_name()
    }
}

#[async_trait::async_trait]
impl<H: ChronicleBrokerScope> AknShutdown<Importer> for BrokerHandle<H> {
    async fn aknowledge_shutdown(self, mut _state: Importer, _status: Result<(), Need>) {
        _state.service.update_status(ServiceStatus::Stopped);
        let event = BrokerEvent::Children(BrokerChild::Importer(_state.service.clone(), _status));
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
    pub fn new(handle: std::sync::Arc<AtomicImporterHandle<S>>, keyspace: S, key: K, value: V, retries: usize) -> Self {
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
    pub fn boxed(
        handle: std::sync::Arc<AtomicImporterHandle<S>>,
        keyspace: S,
        key: K,
        value: V,
        retries: usize,
    ) -> Box<Self> {
        Box::new(Self::new(handle, keyspace, key, value, retries))
    }
}

impl<S, K, V> Worker for AtomicImporterWorker<S, K, V>
where
    S: 'static + Insert<K, V> + Insert<Synckey, SyncRecord>,
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
                scylla::worker::handle_insert_unprepared_error(
                    &self,
                    &self.keyspace,
                    &self.key,
                    &self.value,
                    id,
                    reporter,
                )?;
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
            self.handle.any_error.store(true, Ordering::Acquire);
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

/// A sync worker for the importer
#[derive(Clone)]
pub struct SyncWorker<S>
where
    S: 'static + Insert<Synckey, SyncRecord>,
{
    /// The importer handle
    handle: ImporterHandle,
    /// The keyspace
    keyspace: S,
    /// The `sync` table row
    synced_record: SyncRecord,
    /// The number of retries
    retries: usize,
}

impl<S> SyncWorker<S>
where
    S: 'static + Insert<Synckey, SyncRecord>,
{
    /// Create a new sync worker with an importer handle, a keyspace, a `sync` table row (`SyncRecord`), and a number of
    /// retries
    pub fn new(handle: ImporterHandle, keyspace: S, synced_record: SyncRecord, retries: usize) -> Self {
        Self {
            handle,
            keyspace,
            synced_record,
            retries,
        }
    }
    ///  Create a new boxed ync worker with an importer handle, a keyspace, a `sync` table row (`SyncRecord`), and a
    /// number of retries
    pub fn boxed(handle: ImporterHandle, keyspace: S, synced_record: SyncRecord, retries: usize) -> Box<Self> {
        Box::new(Self::new(handle, keyspace, synced_record, retries))
    }
}

/// Implement the Scylla `Worker` trait
impl<S> Worker for SyncWorker<S>
where
    S: 'static + Insert<Synckey, SyncRecord>,
{
    fn handle_response(self: Box<Self>, giveload: Vec<u8>) -> anyhow::Result<()> {
        Decoder::from(giveload.try_into()?).get_void()?;
        let milestone_index = *self.synced_record.milestone_index;
        self.handle.send(ImporterEvent::CqlResult(Ok(milestone_index))).ok();
        Ok(())
    }
    fn handle_error(
        mut self: Box<Self>,
        mut error: WorkerError,
        reporter: &Option<ReporterHandle>,
    ) -> anyhow::Result<()> {
        if let WorkerError::Cql(ref mut cql_error) = error {
            if let (Some(id), Some(reporter)) = (cql_error.take_unprepared_id(), reporter) {
                scylla::worker::handle_insert_unprepared_error(
                    &self,
                    &self.keyspace,
                    &Synckey,
                    &self.synced_record,
                    id,
                    reporter,
                )?;
                return Ok(());
            }
        }
        if self.retries > 0 {
            self.retries -= 1;
            // currently we assume all cql/worker errors are retryable, but we might change this in future
            let req = self
                .keyspace
                .insert_query(&Synckey, &self.synced_record)
                .consistency(Consistency::One)
                .build()?;
            tokio::spawn(async { req.send_global(self) });
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
    /// The keyspace
    keyspace: S,
    /// The number of retries
    retries: usize,
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
        Self {
            arc_handle,
            retries,
            keyspace,
        }
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

/// Implement the `Inherent` trait for the milestone data worker, so we can get the actomic importer worker
/// which contanis the atomic importer handle of the milestone data worker
impl Inherent for MilestoneDataWorker<ChronicleKeyspace> {
    fn inherent_boxed<K, V>(&self, key: K, value: V) -> Box<dyn Worker>
    where
        ChronicleKeyspace: 'static + Insert<K, V> + Insert<Synckey, SyncRecord>,
        K: 'static + Send + Clone,
        V: 'static + Send + Clone,
    {
        AtomicImporterWorker::boxed(self.arc_handle.clone(), self.keyspace.clone(), key, value, self.retries)
    }
}
