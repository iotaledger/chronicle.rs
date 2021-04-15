// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use crate::{
    application::*,
    archiver::*,
    collector::*,
    syncer::{
        SyncerEvent,
        SyncerHandle,
    },
};
use serde::{
    Deserialize,
    Serialize,
};
use std::{
    ops::{
        Deref,
        DerefMut,
    },
    sync::atomic::{
        AtomicBool,
        Ordering,
    },
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
    collectors_count: u8
});

pub struct MilestoneMessage(MessageId, Box<MilestonePayload>, Message, Option<MessageMetadata>);
impl MilestoneMessage {
    pub fn new(
        message_id: MessageId,
        milestone_payload: Box<MilestonePayload>,
        message: Message,
        metadata: Option<MessageMetadata>,
    ) -> Self {
        Self(message_id, milestone_payload, message, metadata)
    }
}
#[derive(Debug, Deserialize, Serialize)]
pub struct FullMessage(pub Message, pub MessageMetadata);

impl FullMessage {
    pub fn new(message: Message, metadata: MessageMetadata) -> Self {
        Self(message, metadata)
    }
    pub fn message_id(&self) -> &MessageId {
        &self.1.message_id
    }
    pub fn metadata(&self) -> &MessageMetadata {
        &self.1
    }
    pub fn message(&self) -> &Message {
        &self.0
    }
    pub fn ref_ms(&self) -> u32 {
        self.1.referenced_by_milestone_index.unwrap()
    }
}
#[derive(Deserialize, Serialize)]
pub struct MessageStatus {
    in_messages: bool,
    in_database: bool,
}

pub struct InDatabase {
    milestone_index: u32,
    messages_len: usize,
    in_database: HashMap<MessageId, ()>,
}

impl InDatabase {
    fn new(milestone_index: u32) -> Self {
        Self {
            milestone_index,
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
    fn check_if_all_in_database(&self) -> bool {
        if self.messages_len.eq(&0) && self.in_database.len().eq(&0) {
            panic!("Not supposed to be zero, milestone_index: {}", self.milestone_index);
        } else if self.messages_len > 0 && self.in_database.len().eq(&0) {
            println!("milestone index: {}, len: {} ", self.milestone_index, self.messages_len);
        }
        self.messages_len == self.in_database.len()
    }
}

impl From<&MilestoneData> for InDatabase {
    fn from(milestone_data: &MilestoneData) -> Self {
        let mut in_database = Self::new(milestone_data.milestone_index);
        in_database.set_messages_len(milestone_data.messages().len());
        in_database
    }
}

#[derive(Deserialize, Serialize)]
pub struct MilestoneData {
    milestone_index: u32,
    milestone: Option<Box<MilestonePayload>>,
    messages: HashMap<MessageId, FullMessage>,
    pending: HashMap<MessageId, ()>,
    complete: bool,
    created_by: CreatedBy,
}

impl MilestoneData {
    pub fn created_by(&self) -> &CreatedBy {
        &self.created_by
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[repr(u8)]
pub enum CreatedBy {
    /// Created by the new incoming messages from the network
    Incoming = 0,
    /// Created by the new expected messages from the network
    Expected = 1,
    /// Created by solidifiy/sync request from syncer
    Syncer = 2,
}

impl MilestoneData {
    fn new(milestone_index: u32, created_by: CreatedBy) -> Self {
        Self {
            milestone_index,
            milestone: None,
            messages: HashMap::new(),
            pending: HashMap::new(),
            complete: false,
            created_by,
        }
    }
    pub fn milestone_index(&self) -> u32 {
        self.milestone_index
    }
    fn set_milestone(&mut self, boxed_milestone_payload: Box<MilestonePayload>) {
        self.milestone.replace(boxed_milestone_payload);
    }
    fn milestone_exist(&self) -> bool {
        self.milestone.is_some()
    }
    fn add_full_message(&mut self, full_message: FullMessage) {
        self.messages.insert(*full_message.message_id(), full_message);
    }
    fn remove_from_pending(&mut self, message_id: &MessageId) {
        self.pending.remove(message_id);
    }
    fn messages(&self) -> &HashMap<MessageId, FullMessage> {
        &self.messages
    }
    fn pending(&self) -> &HashMap<MessageId, ()> {
        &self.pending
    }
    fn set_completed(&mut self) {
        self.complete = true;
    }
    fn is_complete(&self) -> bool {
        self.complete
    }
}
pub enum SolidifierEvent {
    // Milestone fullmessage;
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
pub enum CqlResult {
    PersistedMsg(MessageId, u32),
    SyncedMilestone(u32),
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

// Solidifier state, each Solidifier solidifiy subset of (milestones_index % solidifier_count == partition_id)
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
    collectors_count: u8,
    syncer_handle: SyncerHandle,
    archiver_handle: ArchiverHandle,
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
        let collectors_count = self.collectors_count.unwrap();
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
            collectors_count,
            syncer_handle: self.syncer_handle.unwrap(),
            archiver_handle: self.archiver_handle.unwrap(),
            message_id_partitioner: MessageIdPartitioner::new(collectors_count),
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

// Scylla worker implementation
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

pub struct AtomicSolidifierHandle {
    pub(crate) handle: SolidifierHandle,
    pub(crate) milestone_index: u32,
    pub(crate) message_id: MessageId,
    pub(crate) any_error: std::sync::atomic::AtomicBool,
}
impl AtomicSolidifierHandle {
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
    pub fn new(handle: std::sync::Arc<AtomicSolidifierHandle>, keyspace: S, key: K, value: V, retries: usize) -> Self {
        Self {
            handle,
            keyspace,
            key,
            value,
            retries,
        }
    }
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
            self.handle.any_error.store(true, Ordering::Relaxed);
        }
    }
}

impl Drop for AtomicSolidifierHandle {
    fn drop(&mut self) {
        let cql_result = CqlResult::PersistedMsg(self.message_id, self.milestone_index);
        let any_error = self.any_error.load(Ordering::Relaxed);
        if any_error {
            self.handle.send(SolidifierEvent::CqlResult(Err(cql_result)));
        } else {
            // respond with void
            self.handle.send(SolidifierEvent::CqlResult(Ok(cql_result)));
        }
    }
}

#[derive(Clone)]
pub struct SolidifierWorker<S, K, V>
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

impl<S: Insert<K, V>, K, V> SolidifierWorker<S, K, V>
where
    S: 'static + Insert<K, V>,
    K: 'static + Send,
    V: 'static + Send,
{
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

impl<S, K, V> Worker for SolidifierWorker<S, K, V>
where
    S: 'static + Insert<K, V>,
    K: 'static + Send + Clone,
    V: 'static + Send + Clone,
{
    fn handle_response(self: Box<Self>, giveload: Vec<u8>) {
        let _void = Decoder::from(giveload).get_void();
        let synced_ms = CqlResult::SyncedMilestone(self.milestone_index);
        let _ = self.handle.send(SolidifierEvent::CqlResult(Ok(synced_ms)));
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
            // resond with error
            let synced_ms = CqlResult::SyncedMilestone(self.milestone_index);
            let _ = self.handle.send(SolidifierEvent::CqlResult(Err(synced_ms)));
        }
    }
}
