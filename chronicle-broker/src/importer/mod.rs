// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0
use crate::{
    application::*,
    solidifier::FullMessage,
};
use chronicle_common::Synckey;
use std::{
    collections::hash_map::IntoIter,
    sync::atomic::{
        AtomicBool,
        Ordering,
    },
};

use bee_message::{
    output::Output,
    payload::transaction::{
        Essence,
        TransactionPayload,
    },
};

use chronicle_common::config::{
    PartitionConfig,
    StorageConfig,
};

use std::ops::{
    Deref,
    DerefMut,
};

mod event_loop;
mod init;
mod terminating;

// Importer builder
builder!(ImporterBuilder {
    file_path: PathBuf,
    inserter_count: u8,
    retries_per_query: usize,
    parallelism: u8,
    permanode_id: u8,
    partition_config: PartitionConfig,
    storage_config: StorageConfig
});

pub enum ImporterEvent {
    CqlResult(Result<u32, u32>),
    ProcessMore(u32),
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
// Importer state
pub struct Importer {
    service: Service,
    file_path: PathBuf,
    default_keyspace: ChronicleKeyspace,
    partition_config: PartitionConfig,
    retries_per_query: usize,
    permanode_id: u8,
    parallelism: u8,
    in_progress_milestones_data: HashMap<u32, IntoIter<MessageId, FullMessage>>,
    handle: Option<ImporterHandle>,
    inbox: ImporterInbox,
}

impl<H: ChronicleBrokerScope> ActorBuilder<BrokerHandle<H>> for ImporterBuilder {}

/// implementation of builder
impl Builder for ImporterBuilder {
    type State = Importer;
    fn build(self) -> Self::State {
        // Get the first keyspace or default to "permanode"
        // In order to use multiple keyspaces, the user must
        // use filters to determine where records go
        let default_keyspace = ChronicleKeyspace::new(
            self.storage_config
                .as_ref()
                .and_then(|config| {
                    config
                        .keyspaces
                        .first()
                        .and_then(|keyspace| Some(keyspace.name.clone()))
                })
                .unwrap_or("permanode".to_owned()),
        );
        let partition_config = self
            .storage_config
            .as_ref()
            .map(|config| config.partition_config.clone())
            .unwrap_or(PartitionConfig::default());
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let handle = Some(ImporterHandle { tx });
        let inbox = ImporterInbox { rx };
        Self::State {
            service: Service::new(),
            file_path: self.file_path.unwrap(),
            default_keyspace,
            partition_config,
            parallelism: self.parallelism.unwrap_or(10),
            permanode_id: self.permanode_id.unwrap(),
            in_progress_milestones_data: HashMap::new(),
            retries_per_query: self.retries_per_query.unwrap_or(10),
            handle,
            inbox,
        }
        .set_name()
    }
}

/// impl name of the Importer
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
        let event = BrokerEvent::Children(BrokerChild::Importer(_state.service.clone()));
        let _ = self.send(event);
    }
}

// Scylla worker implementation
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

pub struct AtomicImporterHandle<S>
where
    S: 'static + Insert<Synckey, SyncRecord>,
{
    pub(crate) handle: ImporterHandle,
    pub(crate) keyspace: S,
    pub(crate) milestone_index: u32,
    pub(crate) any_error: std::sync::atomic::AtomicBool,
    pub(crate) retries: usize,
}

impl<S> AtomicImporterHandle<S>
where
    S: 'static + Insert<Synckey, SyncRecord>,
{
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
    pub fn new(handle: std::sync::Arc<AtomicImporterHandle<S>>, keyspace: S, key: K, value: V, retries: usize) -> Self {
        Self {
            handle,
            keyspace,
            key,
            value,
            retries,
        }
    }
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
    fn handle_response(self: Box<Self>, giveload: Vec<u8>) {
        let _void = Decoder::from(giveload).get_void();
    }
    fn handle_error(mut self: Box<Self>, mut error: WorkerError, reporter: &Option<ReporterHandle>) {
        if let WorkerError::Cql(ref mut cql_error) = error {
            if let (Some(id), Some(reporter)) = (cql_error.take_unprepared_id(), reporter) {
                scylla::worker::insert::handle_unprepared_error(
                    &self,
                    &self.keyspace,
                    &self.key,
                    &self.value,
                    id,
                    reporter,
                );
                return ();
            }
        }
        if self.retries > 0 {
            self.retries -= 1;
            // currently we assume all cql/worker errors are retryable, but we might change this in future
            let req = self
                .keyspace
                .insert_query(&self.key, &self.value)
                .consistency(Consistency::One)
                .build();
            tokio::spawn(async { req.send_global(self) });
        } else {
            // no more retries
            self.handle.any_error.store(true, Ordering::Acquire);
        }
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

#[derive(Clone)]
pub struct SyncWorker<S>
where
    S: 'static + Insert<Synckey, SyncRecord>,
{
    handle: ImporterHandle,
    keyspace: S,
    synced_record: SyncRecord,
    retries: usize,
}

impl<S> SyncWorker<S>
where
    S: 'static + Insert<Synckey, SyncRecord>,
{
    pub fn new(handle: ImporterHandle, keyspace: S, synced_record: SyncRecord, retries: usize) -> Self {
        Self {
            handle,
            keyspace,
            synced_record,
            retries,
        }
    }
    pub fn boxed(handle: ImporterHandle, keyspace: S, synced_record: SyncRecord, retries: usize) -> Box<Self> {
        Box::new(Self::new(handle, keyspace, synced_record, retries))
    }
}

impl<S> Worker for SyncWorker<S>
where
    S: 'static + Insert<Synckey, SyncRecord>,
{
    fn handle_response(self: Box<Self>, giveload: Vec<u8>) {
        let _void = Decoder::from(giveload).get_void();
        let milestone_index = *self.synced_record.milestone_index;
        let _ = self.handle.send(ImporterEvent::CqlResult(Ok(milestone_index)));
    }
    fn handle_error(mut self: Box<Self>, mut error: WorkerError, reporter: &Option<ReporterHandle>) {
        if let WorkerError::Cql(ref mut cql_error) = error {
            if let (Some(id), Some(reporter)) = (cql_error.take_unprepared_id(), reporter) {
                scylla::worker::insert::handle_unprepared_error(
                    &self,
                    &self.keyspace,
                    &Synckey,
                    &self.synced_record,
                    id,
                    reporter,
                );
                return ();
            }
        }
        if self.retries > 0 {
            self.retries -= 1;
            // currently we assume all cql/worker errors are retryable, but we might change this in future
            let req = self
                .keyspace
                .insert_query(&Synckey, &self.synced_record)
                .consistency(Consistency::One)
                .build();
            tokio::spawn(async { req.send_global(self) });
        } else {
            // no more retries
            // respond with error
            let milestone_index = *self.synced_record.milestone_index;
            let _ = self.handle.send(ImporterEvent::CqlResult(Err(milestone_index)));
        }
    }
}

pub struct MilestoneDataWorker<S>
where
    S: 'static + Insert<Synckey, SyncRecord>,
{
    arc_handle: std::sync::Arc<AtomicImporterHandle<S>>,
    keyspace: S,
    retries: usize,
}

impl<S> MilestoneDataWorker<S>
where
    S: 'static + Insert<Synckey, SyncRecord>,
{
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

trait Inherent {
    fn inherent_boxed<K, V>(&self, key: K, value: V) -> Box<dyn Worker>
    where
        ChronicleKeyspace: 'static + Insert<K, V> + Insert<Synckey, SyncRecord>,
        K: 'static + Send + Clone,
        V: 'static + Send + Clone;
}

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
