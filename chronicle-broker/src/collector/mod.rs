// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0
use super::*;
use crate::{
    application::*,
    requester::*,
    solidifier::*,
};
use anyhow::bail;
use bee_message::{
    output::Output,
    payload::transaction::{
        Essence,
        TransactionPayload,
    },
    prelude::MilestoneIndex,
};
use std::collections::{
    BinaryHeap,
    VecDeque,
};

use chronicle_common::config::{
    PartitionConfig,
    StorageConfig,
};
use lru::LruCache;
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
    collector_count: u8,
    requester_count: u8,
    retries_per_query: usize,
    retries_per_endpoint: usize,
    requesters_channels: Vec<(RequesterSender, RequesterReceiver)>,
    handle: CollectorHandle,
    storage_config: StorageConfig
});

/// Collector events
pub enum CollectorEvent {
    /// Requested Message and Metadata, u32 is the milestoneindex
    MessageAndMeta(RequesterId, u32, Option<MessageId>, Option<FullMessage>),
    /// Newly seen message from feed source(s)
    Message(MessageId, Message),
    /// Newly seen MessageMetadataObj from feed source(s)
    MessageReferenced(MessageMetadata),
    /// Ask requests from solidifier(s)
    Ask(AskCollector),
    /// Shutdown the collector
    Internal(Internal),
}
pub enum Internal {
    Service(Service),
    Shutdown,
}
/// Messages for asking the collector for missing data
pub enum AskCollector {
    /// Solidifier(s) will use this variant, u8 is solidifier_id
    FullMessage(u8, u32, MessageId),
    /// Ask for a milestone with the given index
    MilestoneMessage(u32),
}

/// CollectorHandle to be passed to siblings(feed sources) and the supervisor(in order to shutdown)
#[derive(Clone)]
pub struct CollectorHandle {
    pub(crate) tx: tokio::sync::mpsc::UnboundedSender<CollectorEvent>,
    pub(crate) requesters_senders: Vec<RequesterSender>,
}

impl CollectorHandle {
    pub(crate) fn send_requester_topology(&self, requester_topology: RequesterTopology) {
        self.requesters_senders.iter().for_each(|r| {
            let event = RequesterEvent::Topology(requester_topology.clone());
            r.send(event);
        });
    }
}
pub(crate) struct MessageIdPartitioner {
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
        let shutdown_event = CollectorEvent::Internal(Internal::Shutdown);
        self.send(shutdown_event).ok();
        None
    }
}

/// Collector state, each collector is basically LRU cache
pub struct Collector {
    /// The service of the collector metics
    service: Service,
    /// The partition id
    partition_id: u8,
    /// The collector count
    collector_count: u8,
    /// The requester count
    requester_count: u8,
    /// The binary heap stores the requester handles
    requester_handles: BinaryHeap<RequesterHandle>,
    /// The estimated milestone index
    est_ms: MilestoneIndex,
    /// The referenced milestone index
    ref_ms: MilestoneIndex,
    /// The LRU cache from message id to (milestone index, message) pair
    lru_msg: LruCache<MessageId, (MilestoneIndex, Message)>,
    /// The LRU cache from message id to message metadata
    lru_msg_ref: LruCache<MessageId, MessageMetadata>,
    /// The collector handle
    handle: Option<CollectorHandle>,
    /// The collector inbox to receive collector events
    inbox: CollectorInbox,
    /// The hashmap from a partition id to the corresponding solidifier handle
    solidifier_handles: HashMap<u8, SolidifierHandle>,
    /// All requester channels
    requesters_channels: Vec<(RequesterSender, RequesterReceiver)>,
    /// The number of retries per query
    retries_per_query: usize,
    /// The total number of retries per endpoint
    /// NOTE: used by requester
    retries_per_endpoint: usize,
    /// The hashmap to facilitate the recording the pending requests, which maps from
    /// a message id to the corresponding (milestone index, message) pair
    pending_requests: HashMap<MessageId, (u32, Message)>,
    /// The double ended queue stores the api endpoints
    api_endpoints: VecDeque<Url>,
    /// The http client
    reqwest_client: Client,
    /// The partition configure
    partition_config: PartitionConfig,
    /// The `Chronicle` keyspace
    default_keyspace: ChronicleKeyspace,
}

impl<H: ChronicleBrokerScope> ActorBuilder<BrokerHandle<H>> for CollectorBuilder {}

/// implementation of builder
impl Builder for CollectorBuilder {
    type State = Collector;
    fn build(self) -> Self::State {
        let lru_cap = self.lru_capacity.unwrap_or(10000);
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
        Self::State {
            service: Service::new(),
            lru_msg: LruCache::new(lru_cap),
            lru_msg_ref: LruCache::new(lru_cap),
            partition_id: self.partition_id.unwrap(),
            requester_handles: BinaryHeap::new(),
            est_ms: MilestoneIndex(0),
            ref_ms: MilestoneIndex(0),
            solidifier_handles: self.solidifier_handles.expect("Collector expected solidifier handles"),
            retries_per_query: self.retries_per_query.unwrap_or(100),
            retries_per_endpoint: self.retries_per_endpoint.unwrap_or(5),
            collector_count: self.collector_count.unwrap(),
            requester_count: self.requester_count.unwrap_or(10),
            requesters_channels: self
                .requesters_channels
                .expect("Collector expected requesters channels"),
            handle: self.handle,
            inbox: self.inbox.unwrap(),
            pending_requests: HashMap::new(),
            api_endpoints: self.api_endpoints.unwrap(),
            reqwest_client: self.reqwest_client.unwrap(),
            partition_config,
            default_keyspace,
        }
        .set_name()
    }
}

/// Implement the `Name` trait of the `Collector`
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
impl<H: ChronicleBrokerScope> AknShutdown<Collector> for BrokerHandle<H> {
    async fn aknowledge_shutdown(self, mut _state: Collector, _status: Result<(), Need>) {
        _state.service.update_status(ServiceStatus::Stopped);
        let event = BrokerEvent::Children(BrokerChild::Collector(_state.service.clone()));
        let _ = self.send(event);
    }
}
