// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use crate::{
    application::*,
    collector::*,
    logger::*,
};
use lru::LruCache;
use permanode_storage::access::*;
use std::ops::{
    Deref,
    DerefMut,
};

mod event_loop;
mod init;
mod terminating;

// Solidifier builder
builder!(SolidifierBuilder {
    partition_id: u8,
    lru_capacity: usize,
    logger_handle: LoggerHandle,
    inbox: SolidifierInbox,
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
#[derive(Debug, serde::Serialize)]
pub struct FullMessage(Message, MessageMetadata);

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

#[derive(Debug, serde::Serialize)]
pub struct MilestoneData {
    milestone_index: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    milestone: Option<Box<MilestonePayload>>,
    messages: HashMap<MessageId, FullMessage>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    pending: HashMap<MessageId, Option<Message>>,
    #[serde(skip_serializing)]
    complete: bool,
}

impl MilestoneData {
    fn new(milestone_index: u32) -> Self {
        Self {
            milestone_index,
            milestone: None,
            messages: HashMap::new(),
            pending: HashMap::new(),
            complete: false,
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
    fn pending(&self) -> &HashMap<MessageId, Option<Message>> {
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
    /// To be determined Message, that might belong to self solidifier
    /// u32 is milestone index which requested the msg, assuming it belongs to it,
    /// but unfortunately the collector doesn't have the MessageMetadata.
    /// collector likely will re request it from the network.
    Tbd(u32, MessageId, Message),
    /// Shutdown the solidifier
    Shutdown,
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
    partition_id: u8,
    milestones_data: HashMap<u32, MilestoneData>,
    collector_handles: HashMap<u8, CollectorHandle>,
    collectors_count: u8,
    logger_handle: LoggerHandle,
    message_id_partitioner: MessageIdPartitioner,
    inbox: SolidifierInbox,
}

impl<H: PermanodeBrokerScope> ActorBuilder<BrokerHandle<H>> for SolidifierBuilder {}

/// implementation of builder
impl Builder for SolidifierBuilder {
    type State = Solidifier;
    fn build(self) -> Self::State {
        let collectors_count = self.collectors_count.unwrap();
        Self::State {
            service: Service::new(),
            partition_id: self.partition_id.unwrap(),
            milestones_data: HashMap::new(),
            collector_handles: self.collector_handles.unwrap(),
            collectors_count,
            logger_handle: self.logger_handle.unwrap(),
            message_id_partitioner: MessageIdPartitioner::new(collectors_count),
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
impl<H: PermanodeBrokerScope> AknShutdown<Solidifier> for BrokerHandle<H> {
    async fn aknowledge_shutdown(self, mut _state: Solidifier, _status: Result<(), Need>) {
        _state.service.update_status(ServiceStatus::Stopped);
        let event = BrokerEvent::Children(BrokerChild::Solidifier(_state.service.clone()));
        let _ = self.send(event);
    }
}
