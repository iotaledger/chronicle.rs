// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0
use super::{
    archiver::{
        ArchiverEvent,
        ArchiverHandle,
    },
    collector::{
        AskCollector,
        CollectorEvent,
        CollectorHandle,
        MessageIdPartitioner,
    },
    syncer::{
        SyncerEvent,
        SyncerHandle,
    },
    *,
};
use bee_message::prelude::MilestonePayload;

use std::{
    ops::{
        Deref,
        DerefMut,
    },
    sync::atomic::Ordering,
};

mod event_loop;
mod init;
mod terminating;

// Solidifier builder
builder!(SolidifierBuilder {
    chronicle_id: u8,
    keyspace: ChronicleKeyspace,
    partition_id: u8,
    lru_capacity: usize,
    syncer_handle: SyncerHandle,
    archiver_handle: ArchiverHandle,
    handle: SolidifierHandle,
    inbox: SolidifierInbox,
    gap_start: u32,
    retries: u16,
    collector_handles: HashMap<u8, CollectorHandle>,
    collector_count: u8
});

/// A milestone message payload
pub struct MilestoneMessage(MessageId, Box<MilestonePayload>, Message, Option<MessageMetadata>);
impl MilestoneMessage {
    /// Create a new milestone message payload
    pub fn new(
        message_id: MessageId,
        milestone_payload: Box<MilestonePayload>,
        message: Message,
        metadata: Option<MessageMetadata>,
    ) -> Self {
        Self(message_id, milestone_payload, message, metadata)
    }
}

struct InDatabase {
    #[allow(unused)]
    milestone_index: u32,
    analyzed: bool,
    messages_len: usize,
    in_database: HashMap<MessageId, ()>,
}

impl InDatabase {
    fn new(milestone_index: u32) -> Self {
        Self {
            milestone_index,
            analyzed: false,
            messages_len: usize::MAX,
            in_database: HashMap::new(),
        }
    }
    fn set_messages_len(&mut self, message_len: usize) {
        self.messages_len = message_len
    }
    fn add_message_id(&mut self, message_id: MessageId) {
        self.in_database.insert(message_id, ());
    }
    fn set_analyzed(&mut self, analyzed: bool) {
        self.analyzed = analyzed;
    }
    fn check_if_all_in_database(&self) -> bool {
        self.messages_len == self.in_database.len() && self.analyzed
    }
}

impl From<&MilestoneData> for InDatabase {
    fn from(milestone_data: &MilestoneData) -> Self {
        let mut in_database = Self::new(milestone_data.milestone_index());
        in_database.set_messages_len(milestone_data.messages().len());
        in_database
    }
}

/// Solidifier events
pub enum SolidifierEvent {
    /// Milestone fullmessage;
    Milestone(MilestoneMessage),
    /// Pushed or requested messages, that definitely belong to self solidifier
    Message(FullMessage),
    /// Close MessageId that doesn't belong at all to Solidifier of milestone u32
    Close(MessageId, u32),
    /// Solidifiy request from Syncer.
    /// Solidifier should collect milestonedata and pass it to Syncer(not archiver)
    Solidify(Result<u32, u32>),
    /// CqlResult from scylla worker;
    CqlResult(Result<CqlResult, CqlResult>),
    /// Shutdown the solidifier
    Shutdown,
}

/// Cql Results
pub enum CqlResult {
    /// Message was persisted or not
    PersistedMsg(MessageId, u32),
    /// Milestone was synced or not
    SyncedMilestone(u32),
    /// Analyzed MilestoneData or not
    AnalyzedMilestone(u32),
}

/// SolidifierHandle
#[derive(Clone)]
pub struct SolidifierHandle {
    pub(crate) tx: tokio::sync::mpsc::UnboundedSender<SolidifierEvent>,
}
/// SolidifierInbox is used to recv events
pub struct SolidifierInbox {
    pub(crate) rx: tokio::sync::mpsc::UnboundedReceiver<SolidifierEvent>,
}
impl Deref for SolidifierHandle {
    type Target = tokio::sync::mpsc::UnboundedSender<SolidifierEvent>;

    fn deref(&self) -> &Self::Target {
        &self.tx
    }
}

impl DerefMut for SolidifierHandle {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.tx
    }
}

impl Deref for SolidifierInbox {
    type Target = tokio::sync::mpsc::UnboundedReceiver<SolidifierEvent>;

    fn deref(&self) -> &Self::Target {
        &self.rx
    }
}

impl DerefMut for SolidifierInbox {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.rx
    }
}

impl Shutdown for SolidifierHandle {
    fn shutdown(self) -> Option<Self>
    where
        Self: Sized,
    {
        let _ = self.tx.send(SolidifierEvent::Shutdown);
        None
    }
}

/// Solidifier state, each Solidifier solidifiy subset of (milestones_index % solidifier_count == partition_id)
pub struct Solidifier {
    service: Service,
    /// It's the chronicle id.
    chronicle_id: u8,
    keyspace: ChronicleKeyspace,
    partition_id: u8,
    milestones_data: HashMap<u32, MilestoneData>,
    in_database: HashMap<u32, InDatabase>,
    lru_in_database: lru::LruCache<u32, ()>,
    unreachable: lru::LruCache<u32, ()>,
    collector_handles: HashMap<u8, CollectorHandle>,
    collector_count: u8,
    syncer_handle: SyncerHandle,
    archiver_handle: Option<ArchiverHandle>,
    message_id_partitioner: MessageIdPartitioner,
    first: Option<u32>,
    gap_start: u32,
    expected: u32,
    retries: u16,
    handle: SolidifierHandle,
    inbox: SolidifierInbox,
}

impl<H: ChronicleBrokerScope> ActorBuilder<BrokerHandle<H>> for SolidifierBuilder {}

/// implementation of builder
impl Builder for SolidifierBuilder {
    type State = Solidifier;
    fn build(self) -> Self::State {
        let collector_count = self.collector_count.unwrap();
        Self::State {
            service: Service::new(),
            partition_id: self.partition_id.unwrap(),
            keyspace: self.keyspace.unwrap(),
            chronicle_id: self.chronicle_id.unwrap_or(0),
            in_database: HashMap::new(),
            lru_in_database: lru::LruCache::new(100),
            unreachable: lru::LruCache::new(100),
            milestones_data: HashMap::new(),
            collector_handles: self.collector_handles.unwrap(),
            collector_count,
            syncer_handle: self.syncer_handle.unwrap(),
            archiver_handle: self.archiver_handle,
            message_id_partitioner: MessageIdPartitioner::new(collector_count),
            first: None,
            gap_start: self.gap_start.unwrap(),
            expected: 0,
            retries: self.retries.unwrap_or(100),
            handle: self.handle.unwrap(),
            inbox: self.inbox.unwrap(),
        }
        .set_name()
    }
}

/// impl name of the Collector
impl Name for Solidifier {
    fn set_name(mut self) -> Self {
        let name = format!("Solidifier_{}", self.partition_id);
        self.service.update_name(name);
        self
    }
    fn get_name(&self) -> String {
        self.service.get_name()
    }
}

#[async_trait::async_trait]
impl<H: ChronicleBrokerScope> AknShutdown<Solidifier> for BrokerHandle<H> {
    async fn aknowledge_shutdown(self, mut _state: Solidifier, _status: Result<(), Need>) {
        _state.service.update_status(ServiceStatus::Stopped);
        let event = BrokerEvent::Children(BrokerChild::Solidifier(_state.service.clone(), _status));
        let _ = self.send(event);
    }
}

/// Scylla worker implementation
#[derive(Clone)]
pub struct AtomicSolidifierWorker<S, K, V>
where
    S: 'static + Insert<K, V>,
    K: 'static + Send,
    V: 'static + Send,
{
    handle: std::sync::Arc<AtomicSolidifierHandle>,
    keyspace: S,
    key: K,
    value: V,
    retries: usize,
}

/// Atomic solidifier handle
pub struct AtomicSolidifierHandle {
    pub(crate) handle: SolidifierHandle,
    pub(crate) milestone_index: u32,
    pub(crate) message_id: MessageId,
    pub(crate) any_error: std::sync::atomic::AtomicBool,
}
impl AtomicSolidifierHandle {
    /// Create a new Atomic solidifier handle
    pub fn new(
        handle: SolidifierHandle,
        milestone_index: u32,
        message_id: MessageId,
        any_error: std::sync::atomic::AtomicBool,
    ) -> Self {
        Self {
            handle,
            milestone_index,
            message_id,
            any_error,
        }
    }
}
impl<S: Insert<K, V>, K, V> AtomicSolidifierWorker<S, K, V>
where
    S: 'static + Insert<K, V>,
    K: 'static + Send,
    V: 'static + Send,
{
    /// Create a new atomic solidifier worker with a handle and retries
    pub fn new(handle: std::sync::Arc<AtomicSolidifierHandle>, keyspace: S, key: K, value: V, retries: usize) -> Self {
        Self {
            handle,
            keyspace,
            key,
            value,
            retries,
        }
    }
    /// Create a new boxed atomic solidifier worker with a handle and retries
    pub fn boxed(
        handle: std::sync::Arc<AtomicSolidifierHandle>,
        keyspace: S,
        key: K,
        value: V,
        retries: usize,
    ) -> Box<Self> {
        Box::new(Self::new(handle, keyspace, key, value, retries))
    }
}

impl<S, K, V> Worker for AtomicSolidifierWorker<S, K, V>
where
    S: 'static + Insert<K, V>,
    K: 'static + Send + Clone,
    V: 'static + Send + Clone,
{
    fn handle_response(self: Box<Self>, giveload: Vec<u8>) -> anyhow::Result<()> {
        Decoder::try_from(giveload).and_then(|decoder| decoder.get_void())
    }
    fn handle_error(
        mut self: Box<Self>,
        mut error: WorkerError,
        reporter: &Option<ReporterHandle>,
    ) -> anyhow::Result<()> {
        if let WorkerError::Cql(ref mut cql_error) = error {
            if let (Some(id), Some(reporter)) = (cql_error.take_unprepared_id(), reporter) {
                handle_insert_unprepared_error(&self, &self.keyspace, &self.key, &self.value, id, reporter)?;
            }
        }
        if self.retries > 0 {
            self.retries -= 1;
            // currently we assume all cql/worker errors are retryable, but we might change this in future
            match self
                .keyspace
                .insert_query(&self.key, &self.value)
                .consistency(Consistency::One)
                .build()
            {
                Ok(req) => {
                    tokio::spawn(async { req.send_global(self) });
                }
                Err(e) => {
                    error!("{}", e);
                    self.handle.any_error.store(true, Ordering::Relaxed);
                }
            }
        } else {
            // no more retries
            self.handle.any_error.store(true, Ordering::Relaxed);
        }
        Ok(())
    }
}

impl Drop for AtomicSolidifierHandle {
    fn drop(&mut self) {
        let cql_result = CqlResult::PersistedMsg(self.message_id, self.milestone_index);
        let any_error = self.any_error.load(Ordering::Relaxed);
        if any_error {
            self.handle.send(SolidifierEvent::CqlResult(Err(cql_result))).ok();
        } else {
            // respond with void
            self.handle.send(SolidifierEvent::CqlResult(Ok(cql_result))).ok();
        }
    }
}

/// Solidifier worker
#[derive(Clone)]
pub struct SyncedMilestoneWorker<S, K, V>
where
    S: 'static + Insert<K, V>,
    K: 'static + Send,
    V: 'static + Send,
{
    handle: SolidifierHandle,
    milestone_index: u32,
    keyspace: S,
    key: K,
    value: V,
    retries: u16,
}

impl<S: Insert<K, V>, K, V> SyncedMilestoneWorker<S, K, V>
where
    S: 'static + Insert<K, V>,
    K: 'static + Send,
    V: 'static + Send,
{
    /// Create a new solidifier worker with a handle and retries
    pub fn new(handle: SolidifierHandle, milestone_index: u32, keyspace: S, key: K, value: V, retries: u16) -> Self {
        Self {
            handle,
            milestone_index,
            keyspace,
            key,
            value,
            retries,
        }
    }
    /// Create a new boxed solidifier worker with a handle and retries
    pub fn boxed(
        handle: SolidifierHandle,
        milestone_index: u32,
        keyspace: S,
        key: K,
        value: V,
        retries: u16,
    ) -> Box<Self> {
        Box::new(Self::new(handle, milestone_index, keyspace, key, value, retries))
    }
}

impl<S, K, V> Worker for SyncedMilestoneWorker<S, K, V>
where
    S: 'static + Insert<K, V>,
    K: 'static + Send + Clone,
    V: 'static + Send + Clone,
{
    fn handle_response(self: Box<Self>, giveload: Vec<u8>) -> anyhow::Result<()> {
        Decoder::try_from(giveload).and_then(|decoder| decoder.get_void())?;
        let synced_ms = CqlResult::SyncedMilestone(self.milestone_index);
        let _ = self.handle.send(SolidifierEvent::CqlResult(Ok(synced_ms)));
        Ok(())
    }
    fn handle_error(
        mut self: Box<Self>,
        mut error: WorkerError,
        reporter: &Option<ReporterHandle>,
    ) -> anyhow::Result<()> {
        error!(
            "{:?}, left retries: {}, reporter running: {}",
            error,
            self.retries,
            reporter.is_some()
        );
        if let WorkerError::Cql(ref mut cql_error) = error {
            if let (Some(id), Some(reporter)) = (cql_error.take_unprepared_id(), reporter) {
                handle_insert_unprepared_error(&self, &self.keyspace, &self.key, &self.value, id, reporter)?;
            }
        }
        if self.retries > 0 {
            self.retries -= 1;
            // currently we assume all cql/worker errors are retryable, but we might change this in future
            match self
                .keyspace
                .insert_query(&self.key, &self.value)
                .consistency(Consistency::One)
                .build()
            {
                Ok(req) => {
                    tokio::spawn(async { req.send_global(self) });
                }
                Err(e) => {
                    error!("{}", e);
                    let synced_ms = CqlResult::SyncedMilestone(self.milestone_index);
                    let _ = self.handle.send(SolidifierEvent::CqlResult(Err(synced_ms)));
                }
            }
        } else {
            // no more retries
            // respond with error
            let synced_ms = CqlResult::SyncedMilestone(self.milestone_index);
            let _ = self.handle.send(SolidifierEvent::CqlResult(Err(synced_ms)));
        }
        Ok(())
    }
}

/// Solidifier worker
#[derive(Clone)]
pub struct AnalyzedMilestoneWorker<S, K, V>
where
    S: 'static + Insert<K, V>,
    K: 'static + Send,
    V: 'static + Send,
{
    handle: SolidifierHandle,
    milestone_index: u32,
    keyspace: S,
    key: K,
    value: V,
    retries: u16,
}

impl<S: Insert<K, V>, K, V> AnalyzedMilestoneWorker<S, K, V>
where
    S: 'static + Insert<K, V>,
    K: 'static + Send,
    V: 'static + Send,
{
    /// Create a new solidifier worker with a handle and retries
    pub fn new(handle: SolidifierHandle, milestone_index: u32, keyspace: S, key: K, value: V, retries: u16) -> Self {
        Self {
            handle,
            milestone_index,
            keyspace,
            key,
            value,
            retries,
        }
    }
    /// Create a new boxed solidifier worker with a handle and retries
    pub fn boxed(
        handle: SolidifierHandle,
        milestone_index: u32,
        keyspace: S,
        key: K,
        value: V,
        retries: u16,
    ) -> Box<Self> {
        Box::new(Self::new(handle, milestone_index, keyspace, key, value, retries))
    }
}

impl<S, K, V> Worker for AnalyzedMilestoneWorker<S, K, V>
where
    S: 'static + Insert<K, V>,
    K: 'static + Send + Clone,
    V: 'static + Send + Clone,
{
    fn handle_response(self: Box<Self>, giveload: Vec<u8>) -> anyhow::Result<()> {
        Decoder::try_from(giveload).and_then(|decoder| decoder.get_void())?;
        let analyzed_ms = CqlResult::AnalyzedMilestone(self.milestone_index);
        let _ = self.handle.send(SolidifierEvent::CqlResult(Ok(analyzed_ms)));
        Ok(())
    }
    fn handle_error(
        mut self: Box<Self>,
        mut error: WorkerError,
        reporter: &Option<ReporterHandle>,
    ) -> anyhow::Result<()> {
        error!(
            "{:?}, left retries: {}, reporter running: {}",
            error,
            self.retries,
            reporter.is_some()
        );
        if let WorkerError::Cql(ref mut cql_error) = error {
            if let (Some(id), Some(reporter)) = (cql_error.take_unprepared_id(), reporter) {
                handle_insert_unprepared_error(&self, &self.keyspace, &self.key, &self.value, id, reporter)?;
            }
        }
        if self.retries > 0 {
            self.retries -= 1;
            // currently we assume all cql/worker errors are retryable, but we might change this in future
            match self
                .keyspace
                .insert_query(&self.key, &self.value)
                .consistency(Consistency::One)
                .build()
            {
                Ok(req) => {
                    tokio::spawn(async { req.send_global(self) });
                }
                Err(e) => {
                    error!("{}", e);
                    let analyzed_ms = CqlResult::AnalyzedMilestone(self.milestone_index);
                    let _ = self.handle.send(SolidifierEvent::CqlResult(Err(analyzed_ms)));
                }
            }
        } else {
            // no more retries
            // respond with error
            let analyzed_ms = CqlResult::AnalyzedMilestone(self.milestone_index);
            let _ = self.handle.send(SolidifierEvent::CqlResult(Err(analyzed_ms)));
        }
        Ok(())
    }
}
