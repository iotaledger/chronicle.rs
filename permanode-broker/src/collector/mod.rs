// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0
use crate::{
    application::*,
    requester::*,
    solidifier::*,
};
use std::collections::VecDeque;

use bee_message::{
    output::Output,
    payload::transaction::{
        Essence,
        TransactionPayload,
    },
};
use std::collections::BinaryHeap;

use lru::LruCache;
use permanode_storage::StorageConfig;
use std::ops::{
    Deref,
    DerefMut,
};

mod event_loop;
mod init;
mod terminating;
use reqwest::Client;
use url::Url;
// Collector builder
builder!(CollectorBuilder {
    partition_id: u8,
    lru_capacity: usize,
    inbox: CollectorInbox,
    solidifier_handles: HashMap<u8, SolidifierHandle>,
    reqwest_client: Client,
    api_endpoints: VecDeque<Url>,
    collectors_count: u8,
    requester_count: u8,
    handle: CollectorHandle,
    storage_config: StorageConfig
});

pub enum CollectorEvent {
    /// Requested Message and Metadata,
    MessageAndMeta(RequesterId, Option<FullMessage>),
    /// Newly seen message from feed source(s)
    Message(MessageId, Message),
    /// Newly seen MessageMetadataObj from feed source(s)
    MessageReferenced(MessageMetadata),
    /// Ask requests from solidifier(s)
    Ask(Ask),
    /// Shutdown the collector
    Shutdown,
}

pub enum Ask {
    /// Solidifier(s) will use this variant, u8 is solidifier_id
    FullMessage(u8, u32, MessageId),
    MilestoneMessage(u32),
}

/// CollectorHandle to be passed to siblings(feed sources) and the supervisor(in order to shutdown)
#[derive(Clone)]
pub struct CollectorHandle {
    pub(crate) tx: tokio::sync::mpsc::UnboundedSender<CollectorEvent>,
}
pub struct MessageIdPartitioner {
    count: u8,
}
impl MessageIdPartitioner {
    pub fn new(count: u8) -> Self {
        Self { count }
    }
    pub fn partition_id(&self, message_id: &MessageId) -> u8 {
        // partitioning based on first byte of the message_id
        message_id.as_ref()[0] % self.count
    }
}
/// CollectorInbox is used to recv events
pub struct CollectorInbox {
    pub(crate) rx: tokio::sync::mpsc::UnboundedReceiver<CollectorEvent>,
}
impl Deref for CollectorHandle {
    type Target = tokio::sync::mpsc::UnboundedSender<CollectorEvent>;

    fn deref(&self) -> &Self::Target {
        &self.tx
    }
}

impl DerefMut for CollectorHandle {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.tx
    }
}

impl Deref for CollectorInbox {
    type Target = tokio::sync::mpsc::UnboundedReceiver<CollectorEvent>;

    fn deref(&self) -> &Self::Target {
        &self.rx
    }
}

impl DerefMut for CollectorInbox {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.rx
    }
}

impl Shutdown for CollectorHandle {
    fn shutdown(self) -> Option<Self>
    where
        Self: Sized,
    {
        let shutdown_event = CollectorEvent::Shutdown;
        self.send(shutdown_event);
        None
    }
}

// collector state, each collector is basically LRU cache
pub struct Collector {
    service: Service,
    partition_id: u8,
    collectors_count: u8,
    requester_count: u8,
    requester_handles: BinaryHeap<RequesterHandle>,
    est_ms: MilestoneIndex,
    ref_ms: MilestoneIndex,
    lru_msg: LruCache<MessageId, (MilestoneIndex, Message)>,
    lru_msg_ref: LruCache<MessageId, MessageMetadata>,
    handle: Option<CollectorHandle>,
    inbox: CollectorInbox,
    solidifier_handles: HashMap<u8, SolidifierHandle>,
    api_endpoints: VecDeque<Url>,
    reqwest_client: Client,
    default_keyspace: PermanodeKeyspace,
    storage_config: Option<StorageConfig>,
}

impl<H: PermanodeBrokerScope> ActorBuilder<BrokerHandle<H>> for CollectorBuilder {}

/// implementation of builder
impl Builder for CollectorBuilder {
    type State = Collector;
    fn build(self) -> Self::State {
        let lru_cap = self.lru_capacity.unwrap_or(10000);
        // Get the first keyspace or default to "permanode"
        // In order to use multiple keyspaces, the user must
        // use filters to determine where records go
        let default_keyspace = PermanodeKeyspace::new(
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
        Self::State {
            service: Service::new(),
            lru_msg: LruCache::new(lru_cap),
            lru_msg_ref: LruCache::new(lru_cap),
            partition_id: self.partition_id.unwrap(),
            requester_handles: BinaryHeap::new(),
            est_ms: MilestoneIndex(0),
            ref_ms: MilestoneIndex(0),
            solidifier_handles: self.solidifier_handles.expect("Collector expected solidifier handles"),
            collectors_count: self.collectors_count.unwrap(),
            requester_count: self.requester_count.unwrap_or(10),
            handle: self.handle,
            inbox: self.inbox.unwrap(),
            api_endpoints: self.api_endpoints.unwrap(),
            reqwest_client: self.reqwest_client.unwrap(),
            default_keyspace,
            storage_config: self.storage_config,
        }
        .set_name()
    }
}

/// impl name of the Collector
impl Name for Collector {
    fn set_name(mut self) -> Self {
        let name = format!("Collector_{}", self.partition_id);
        self.service.update_name(name);
        self
    }
    fn get_name(&self) -> String {
        self.service.get_name()
    }
}

#[async_trait::async_trait]
impl<H: PermanodeBrokerScope> AknShutdown<Collector> for BrokerHandle<H> {
    async fn aknowledge_shutdown(self, mut _state: Collector, _status: Result<(), Need>) {
        _state.service.update_status(ServiceStatus::Stopped);
        let event = BrokerEvent::Children(BrokerChild::Collector(_state.service.clone()));
        let _ = self.send(event);
    }
}
