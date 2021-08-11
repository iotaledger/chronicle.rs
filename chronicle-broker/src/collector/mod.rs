// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0
use super::{
    requester::{
        Requester,
        RequesterEvent,
    },
    solidifier::{
        AtomicSolidifierHandle,
        AtomicSolidifierWorker,
        MilestoneMessage,
        Solidifier,
        SolidifierEvent,
    },
    *,
};
use anyhow::bail;
use bee_message::{
    address::Address,
    input::Input,
    output::Output,
    parents::Parents,
    payload::{
        transaction::{
            Essence,
            TransactionPayload,
        },
        Payload,
    },
    prelude::{
        MilestoneIndex,
        TransactionId,
    },
};
use chronicle_common::config::{
    PartitionConfig,
    StorageConfig,
};
use lru::LruCache;
use reqwest::Client;
use std::sync::Arc;

/// Collector events
#[supervise(Requester)]
pub enum CollectorEvent {
    /// Requested Message and Metadata, u32 is the milestoneindex
    MessageAndMeta(u32, Option<MessageId>, Option<FullMessage>),
    /// Newly seen message from feed source(s)
    Message(MessageId, Message),
    /// Newly seen MessageMetadataObj from feed source(s)
    MessageReferenced(MessageMetadata),
    /// Ask requests from solidifier(s)
    Ask(AskCollector),
    /// Notify requesters about a topology change
    RequesterTopologyChange,
    /// Shutdown the collector
    Shutdown,
}
/// Messages for asking the collector for missing data
pub enum AskCollector {
    /// Solidifier(s) will use this variant, u8 is solidifier_id
    FullMessage(u8, u32, MessageId, CreatedBy),
    /// Ask for a milestone with the given index
    MilestoneMessage(u32),
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

/// Collector state, each collector is basically LRU cache
pub struct Collector {
    /// The partition id
    partition_id: u8,
    /// The collector count
    collector_count: u8,
    /// The requester count
    requester_count: u8,
    /// The estimated milestone index
    est_ms: MilestoneIndex,
    /// The referenced milestone index
    ref_ms: MilestoneIndex,
    /// The LRU cache from message id to (milestone index, message) pair
    lru_msg: LruCache<MessageId, (MilestoneIndex, Message)>,
    /// The LRU cache from message id to message metadata
    lru_msg_ref: LruCache<MessageId, MessageMetadata>,
    /// The number of retries per query
    retries_per_query: usize,
    /// The total number of retries per endpoint
    /// NOTE: used by requester
    retries_per_endpoint: usize,
    /// The hashmap to facilitate the recording the pending requests, which maps from
    /// a message id to the corresponding (milestone index, message) pair
    pending_requests: HashMap<MessageId, (u32, Message)>,
    /// The http client
    reqwest_client: Client,
    /// The partition configure
    partition_config: PartitionConfig,
    /// The `Chronicle` keyspace
    default_keyspace: ChronicleKeyspace,
}

#[build]
#[derive(Clone)]
pub fn build_collector(
    partition_id: u8,
    lru_capacity: Option<usize>,
    reqwest_client: Client,
    collector_count: u8,
    requester_count: Option<u8>,
    retries_per_query: Option<usize>,
    retries_per_endpoint: Option<usize>,
    storage_config: Option<StorageConfig>,
) -> Collector {
    let lru_cap = lru_capacity.unwrap_or(10000);
    // Get the first keyspace or default to "permanode"
    // In order to use multiple keyspaces, the user must
    // use filters to determine where records go
    let default_keyspace = ChronicleKeyspace::new(
        storage_config
            .as_ref()
            .and_then(|config| {
                config
                    .keyspaces
                    .first()
                    .and_then(|keyspace| Some(keyspace.name.clone()))
            })
            .unwrap_or("permanode".to_owned()),
    );
    let partition_config = storage_config
        .as_ref()
        .map(|config| config.partition_config.clone())
        .unwrap_or(PartitionConfig::default());
    Collector {
        lru_msg: LruCache::new(lru_cap),
        lru_msg_ref: LruCache::new(lru_cap),
        partition_id,
        est_ms: MilestoneIndex(0),
        ref_ms: MilestoneIndex(0),
        retries_per_query: retries_per_query.unwrap_or(100),
        retries_per_endpoint: retries_per_endpoint.unwrap_or(5),
        collector_count,
        requester_count: requester_count.unwrap_or(10),
        pending_requests: HashMap::new(),
        reqwest_client,
        partition_config,
        default_keyspace,
    }
}

#[async_trait]
impl Actor for Collector {
    type Dependencies = (Pool<MapPool<Solidifier, u8>>, Pool<LruPool<Requester>>, Act<Scylla>);
    type Event = CollectorEvent;
    type Channel = UnboundedTokioChannel<Self::Event>;

    async fn init<Reg: RegistryAccess + Send + Sync, Sup: EventDriven>(
        &mut self,
        rt: &mut ActorScopedRuntime<Self, Reg, Sup>,
    ) -> Result<(), ActorError>
    where
        Self: Sized,
        Sup::Event: SupervisorEvent,
        <Sup::Event as SupervisorEvent>::Children: From<PhantomData<Self>>,
    {
        rt.update_status(ServiceStatus::Initializing).await.ok();

        for id in 0..self.requester_count {
            let reqwest_client = self.reqwest_client.clone();
            let requester = super::requester::RequesterBuilder::new()
                .requester_id(id)
                .retries_per_endpoint(self.retries_per_endpoint)
                .reqwest_client(reqwest_client)
                .build();
            rt.spawn_into_pool::<LruPool<_>>(requester).await?;
        }
        Ok(())
    }

    async fn run<Reg: RegistryAccess + Send + Sync, Sup: EventDriven>(
        &mut self,
        rt: &mut ActorScopedRuntime<Self, Reg, Sup>,
        (solidifier_handles, requester_handles, scylla_handle): Self::Dependencies,
    ) -> Result<(), ActorError>
    where
        Self: Sized,
        Sup::Event: SupervisorEvent,
        <Sup::Event as SupervisorEvent>::Children: From<PhantomData<Self>>,
    {
        rt.update_status(ServiceStatus::Running).await.ok();
        while let Some(event) = rt.next_event().await {
            match event {
                CollectorEvent::MessageAndMeta(try_ms_index, message_id, opt_full_msg) => {
                    if let Some(FullMessage(message, metadata)) = opt_full_msg {
                        let message_id = message_id.expect("Expected message_id in requester response");
                        let partition_id = (try_ms_index % (self.collector_count as u32)) as u8;
                        let ref_ms = metadata
                            .referenced_by_milestone_index
                            .as_ref()
                            .expect("Expected referenced_by_milestone_index");
                        // check if the requested message actually belongs to the expected milestone_index
                        if ref_ms.eq(&try_ms_index) {
                            // push full message to solidifier;
                            self.push_fullmsg_to_solidifier(
                                partition_id,
                                message.clone(),
                                metadata.clone(),
                                &solidifier_handles,
                            )
                            .await;
                            // proceed to insert the message and put it in the cache.
                        } else {
                            // close the request
                            self.push_close_to_solidifier(partition_id, message_id, try_ms_index, &solidifier_handles)
                                .await;
                        }
                        // set the ref_ms to be the current requested message ref_ms
                        self.ref_ms.0 = *ref_ms;
                        // check if msg already in lru cache(if so then it's already presisted)
                        let wrong_msg_est_ms;
                        if let Some((est_ms, _)) = self.lru_msg.get_mut(&message_id) {
                            // check if est_ms is not identical to ref_ms
                            if &est_ms.0 != ref_ms {
                                wrong_msg_est_ms = Some(*est_ms);
                                // adjust est_ms to match the actual ref_ms
                                est_ms.0 = *ref_ms;
                            } else {
                                wrong_msg_est_ms = None;
                            }
                        } else {
                            // add it to the cache in order to not presist it again.
                            self.lru_msg.put(message_id, (self.ref_ms, message.clone()));
                            wrong_msg_est_ms = None;
                        }
                        // Cache metadata.
                        self.lru_msg_ref.put(message_id, metadata.clone());
                        if let Some(wrong_est_ms) = wrong_msg_est_ms {
                            self.clean_up_wrong_est_msg(&message_id, &message, wrong_est_ms)?;
                        }
                        self.insert_message_with_metadata(message_id, message, metadata, &solidifier_handles)
                            .await?;
                    } else {
                        error!(
                            "{} , unable to fetch message: {:?}, from network triggered by milestone_index: {}",
                            self.name(),
                            message_id,
                            try_ms_index
                        );
                        // inform solidifier
                        let solidifier_id = (try_ms_index % (self.collector_count as u32)) as u8;
                        solidifier_handles
                            .send(&solidifier_id, SolidifierEvent::Solidify(Err(try_ms_index)))
                            .await;
                    }
                }
                CollectorEvent::Message(message_id, mut message) => {
                    // check if msg already in lru cache(if so then it's already presisted)
                    if self.lru_msg.get(&message_id).is_none() {
                        // store message
                        self.insert_message(&message_id, &mut message, &solidifier_handles)
                            .await?;
                        // add it to the cache in order to not presist it again.
                        self.lru_msg.put(message_id, (self.est_ms, message));
                    }
                }
                CollectorEvent::MessageReferenced(metadata) => {
                    if metadata.referenced_by_milestone_index.is_none() {
                        // metadata is not referenced yet, so we discard it.
                        continue;
                    }
                    let ref_ms = metadata.referenced_by_milestone_index.as_ref().unwrap();
                    let partition_id = (ref_ms % (self.collector_count as u32)) as u8;
                    let message_id = metadata.message_id;
                    // set the ref_ms to be the most recent ref_ms
                    self.ref_ms.0 = *ref_ms;
                    // update the est_ms to be the most recent ref_ms+1
                    let new_ms = self.ref_ms.0 + 1;
                    if self.est_ms.0 < new_ms {
                        self.est_ms.0 = new_ms;
                    }
                    // check if msg already in lru cache (if so then it's already persisted)
                    if self.lru_msg_ref.get(&message_id).is_none() {
                        // add it to the cache in order to not persist it again.
                        self.lru_msg_ref.put(message_id, metadata.clone());
                        // check if msg already exist in the cache, if so we push it to solidifier
                        let cached_msg: Option<Message>;
                        let wrong_msg_est_ms;
                        if let Some((est_ms, message)) = self.lru_msg.get_mut(&message_id) {
                            // check if est_ms is not identical to ref_ms
                            if &est_ms.0 != ref_ms {
                                wrong_msg_est_ms = Some(*est_ms);
                                // adjust est_ms to match the actual ref_ms
                                est_ms.0 = *ref_ms;
                            } else {
                                wrong_msg_est_ms = None;
                            }
                            cached_msg = Some(message.clone());

                            solidifier_handles
                                .send(
                                    &partition_id,
                                    SolidifierEvent::Message(FullMessage::new(message.clone(), metadata.clone())),
                                )
                                .await;

                            // however the message_id might had been requested,
                            if let Some((requested_by_this_ms, _)) = self.pending_requests.remove(&message_id) {
                                // check if we have to close it
                                if !requested_by_this_ms.eq(&*ref_ms) {
                                    // close it
                                    let solidifier_id = (requested_by_this_ms % (self.collector_count as u32)) as u8;
                                    self.push_close_to_solidifier(
                                        solidifier_id,
                                        message_id,
                                        requested_by_this_ms,
                                        &solidifier_handles,
                                    )
                                    .await;
                                }
                            }
                            // request all pending_requests with less than the received milestone index
                            self.process_pending_requests(*ref_ms, &requester_handles).await;
                        } else {
                            // check if it's in the pending_requests
                            if let Some((requested_by_this_ms, message)) = self.pending_requests.remove(&message_id) {
                                // check if we have to close or push full message
                                if requested_by_this_ms.eq(&*ref_ms) {
                                    // push full message
                                    self.push_fullmsg_to_solidifier(
                                        partition_id,
                                        message.clone(),
                                        metadata.clone(),
                                        &solidifier_handles,
                                    )
                                    .await;
                                } else {
                                    // close it
                                    let solidifier_id = (requested_by_this_ms % (self.collector_count as u32)) as u8;
                                    self.push_close_to_solidifier(
                                        solidifier_id,
                                        message_id,
                                        requested_by_this_ms,
                                        &solidifier_handles,
                                    )
                                    .await;
                                }
                                cached_msg = Some(message);
                            } else {
                                cached_msg = None;
                            }
                            wrong_msg_est_ms = None;
                            self.process_pending_requests(*ref_ms, &requester_handles).await;
                        }
                        if let Some(message) = cached_msg {
                            if let Some(wrong_est_ms) = wrong_msg_est_ms {
                                self.clean_up_wrong_est_msg(&message_id, &message, wrong_est_ms)?;
                            }
                            self.insert_message_with_metadata(message_id, message, metadata, &solidifier_handles)
                                .await?;
                        } else {
                            // store it as metadata
                            self.insert_message_metadata(metadata)?;
                        }
                    }
                }
                CollectorEvent::Ask(ask) => {
                    match ask {
                        AskCollector::FullMessage(solidifier_id, try_ms_index, message_id, created_by) => {
                            let mut message_tuple = None;
                            if let Some((_, message)) = self.lru_msg.get(&message_id) {
                                if let Some(metadata) = self.lru_msg_ref.get(&message_id) {
                                    // metadata exist means we already pushed the full message to the solidifier,
                                    // or the message doesn't belong to the solidifier
                                    if !metadata.referenced_by_milestone_index.unwrap().eq(&try_ms_index) {
                                        self.push_close_to_solidifier(
                                            solidifier_id,
                                            message_id,
                                            try_ms_index,
                                            &solidifier_handles,
                                        )
                                        .await;
                                    } else {
                                        solidifier_handles
                                            .send(
                                                &solidifier_id,
                                                SolidifierEvent::Message(FullMessage::new(
                                                    message.clone(),
                                                    metadata.clone(),
                                                )),
                                            )
                                            .await
                                            .ok();

                                        // make sure to insert the message if it's requested from syncer
                                        if created_by == CreatedBy::Syncer {
                                            self.ref_ms.0 = try_ms_index;
                                            message_tuple = Some((message.clone(), metadata.clone()));
                                        }
                                    }
                                } else {
                                    if !(*self.est_ms).eq(&0) {
                                        let highest_ms = *self.est_ms - 1;
                                        if try_ms_index >= highest_ms {
                                            if let Some((pre_ms_index, _)) = self.pending_requests.get_mut(&message_id)
                                            {
                                                // check if other solidifier(other milestone) already requested the
                                                // message_id with diff try_ms_index
                                                let old_ms = *pre_ms_index;
                                                if old_ms < try_ms_index {
                                                    // close try_ms_index, and keep pre_ms_index to be processed
                                                    // eventually
                                                    self.push_close_to_solidifier(
                                                        solidifier_id,
                                                        message_id,
                                                        try_ms_index,
                                                        &solidifier_handles,
                                                    )
                                                    .await;
                                                } else {
                                                    // overwrite pre_ms_index by try_ms_index, which it will be
                                                    // eventually processed;
                                                    *pre_ms_index = try_ms_index;
                                                    // close pre_ms_index(old_ms) as it's greater than what we have atm
                                                    // (try_ms_index).
                                                    let solidifier_id = (old_ms % (self.collector_count as u32)) as u8;
                                                    self.push_close_to_solidifier(
                                                        solidifier_id,
                                                        message_id,
                                                        old_ms,
                                                        &solidifier_handles,
                                                    )
                                                    .await;
                                                }
                                            } else {
                                                // add it to back_pressured requests
                                                self.pending_requests
                                                    .insert(message_id, (try_ms_index, message.clone()));
                                            };
                                        } else {
                                            self.request_full_message(message_id, try_ms_index, &requester_handles)
                                                .await;
                                        }
                                    } else {
                                        self.request_full_message(message_id, try_ms_index, &requester_handles)
                                            .await;
                                    }
                                }
                            } else {
                                self.request_full_message(message_id, try_ms_index, &requester_handles)
                                    .await;
                            }
                            // insert the message if requested by syncer to ensure it gets cql responses for all the
                            // requested messages
                            if let Some((message, metadata)) = message_tuple.take() {
                                self.insert_message_with_metadata(
                                    message_id.clone(),
                                    message,
                                    metadata,
                                    &solidifier_handles,
                                )
                                .await?;
                            }
                        }
                        AskCollector::MilestoneMessage(milestone_index) => {
                            // Request it from network
                            self.request_milestone_message(milestone_index, &requester_handles)
                                .await;
                        }
                    }
                }
                CollectorEvent::RequesterTopologyChange => {
                    for handle in requester_handles.handles().await {
                        handle.send(RequesterEvent::TopologyChange).ok();
                    }
                }
                CollectorEvent::Shutdown => break,
                CollectorEvent::ReportExit(res) => match res {
                    Ok(_) => break,
                    Err(mut e) => match e.error.request().clone() {
                        ActorRequest::Restart => {
                            rt.spawn_into_pool::<LruPool<_>>(e.state).await?;
                        }
                        ActorRequest::Reschedule(dur) => {
                            let handle = rt.handle();
                            e.error = ActorError::RuntimeError(ActorRequest::Restart);
                            let evt = Self::Event::report_err(e);
                            tokio::spawn(async move {
                                tokio::time::sleep(dur).await;
                                handle.send(evt).ok();
                            });
                        }
                        ActorRequest::Finish => error!("{}", e.error),
                        ActorRequest::Panic => panic!("{}", e.error),
                    },
                },
                CollectorEvent::StatusChange(s) => {
                    // TODO
                }
            }
        }
        Ok(())
    }

    fn name(&self) -> std::borrow::Cow<'static, str> {
        format!("Collector ({})", self.partition_id).into()
    }
}

impl Collector {
    /// Process the pending requests for a given milestone index
    async fn process_pending_requests(&mut self, milestone_index: u32, requester_handles: &Pool<LruPool<Requester>>) {
        for (message_id, (ms, msg)) in std::mem::take(&mut self.pending_requests) {
            if ms < milestone_index {
                self.request_full_message(message_id, ms, requester_handles).await;
            }
        }
    }
    /// Request the milestone message of a given milestone index
    async fn request_milestone_message(&mut self, milestone_index: u32, requester_handles: &Pool<LruPool<Requester>>) {
        requester_handles
            .send(RequesterEvent::RequestMilestone(milestone_index))
            .await;
    }
    /// Request the full message (i.e., including both message and metadata) of a given message id and
    /// a milestone index
    async fn request_full_message(
        &mut self,
        message_id: MessageId,
        try_ms_index: u32,
        requester_handles: &Pool<LruPool<Requester>>,
    ) {
        requester_handles
            .send(RequesterEvent::RequestFullMessage(message_id, try_ms_index))
            .await;
    }

    /// Clean up the message and message_id with the wrong estimated milestone index
    fn clean_up_wrong_est_msg(
        &mut self,
        message_id: &MessageId,
        message: &Message,
        wrong_est_ms: MilestoneIndex,
    ) -> anyhow::Result<()> {
        self.delete_parents(message_id, message.parents(), wrong_est_ms)?;
        match message.payload() {
            // delete indexation if any
            Some(Payload::Indexation(indexation)) => {
                let index_key = Indexation(hex::encode(indexation.index()));
                self.delete_indexation(&message_id, index_key, wrong_est_ms)?;
            }
            // delete transactiion partitioned rows if any
            Some(Payload::Transaction(transaction_payload)) => {
                self.delete_transaction_partitioned_rows(message_id, transaction_payload, wrong_est_ms)?;
            }
            _ => {}
        }
        Ok(())
    }
    /// Push the full message (both the message and message metadata) to a given partiion of the solidifier
    async fn push_fullmsg_to_solidifier(
        &self,
        partition_id: u8,
        message: Message,
        metadata: MessageMetadata,
        solidifier_handles: &Pool<MapPool<Solidifier, u8>>,
    ) {
        solidifier_handles
            .send(
                &partition_id,
                SolidifierEvent::Message(FullMessage::new(message, metadata)),
            )
            .await;
    }
    /// Push a `Close` message_id (which doesn't belong to all solidifiers with a given milestone index) to the
    /// solidifier
    async fn push_close_to_solidifier(
        &self,
        partition_id: u8,
        message_id: MessageId,
        try_ms_index: u32,
        solidifier_handles: &Pool<MapPool<Solidifier, u8>>,
    ) {
        solidifier_handles
            .send(&partition_id, SolidifierEvent::Close(message_id, try_ms_index))
            .await;
    }
    /// Get the `Chronicle` keyspace of a message
    #[cfg(feature = "filter")]
    fn get_keyspace_for_message(&self, message: &mut Message) -> ChronicleKeyspace {
        let res = futures::executor::block_on(chronicle_filter::filter_messages(message));
        ChronicleKeyspace::new(res.keyspace.into_owned())
    }
    /// Get the Chronicle keyspace
    fn get_keyspace(&self) -> ChronicleKeyspace {
        self.default_keyspace.clone()
    }
    /// Get the partition id of a given milestone index
    fn get_partition_id(&self, milestone_index: MilestoneIndex) -> u16 {
        self.partition_config.partition_id(milestone_index.0)
    }
    /// Insert the message id and message to the table
    async fn insert_message(
        &mut self,
        message_id: &MessageId,
        message: &mut Message,
        solidifier_handles: &Pool<MapPool<Solidifier, u8>>,
    ) -> anyhow::Result<()> {
        // Check if metadata already exist in the cache
        let ledger_inclusion_state;

        #[cfg(feature = "filter")]
        let keyspace = self.get_keyspace_for_message(message);
        #[cfg(not(feature = "filter"))]
        let keyspace = self.get_keyspace();
        let metadata;
        if let Some(meta) = self.lru_msg_ref.get(message_id) {
            metadata = Some(meta.clone());
            ledger_inclusion_state = meta.ledger_inclusion_state.clone();
            let milestone_index = MilestoneIndex(*meta.referenced_by_milestone_index.as_ref().unwrap());
            let solidifier_id = (*milestone_index % (self.collector_count as u32)) as u8;
            let solidifier_handle = solidifier_handles
                .get(&solidifier_id)
                .await
                .ok_or_else(|| anyhow::anyhow!("No solidifier handle for id {}!", solidifier_id))?;
            let inherent_worker =
                AtomicWorker::new(solidifier_handle, *milestone_index, *message_id, self.retries_per_query);
            let message_tuple = (message.clone(), meta.clone());
            // store message and metadata
            self.insert(&inherent_worker, &keyspace, *message_id, message_tuple)?;
            // Insert parents/children
            self.insert_parents(
                &inherent_worker,
                &message_id,
                &message.parents(),
                milestone_index,
                ledger_inclusion_state.clone(),
            )?;
            // insert payload (if any)
            if let Some(payload) = message.payload() {
                self.insert_payload(
                    &inherent_worker,
                    &message_id,
                    &message,
                    &payload,
                    milestone_index,
                    ledger_inclusion_state,
                    metadata,
                    solidifier_handles,
                )
                .await?;
            }
        } else {
            metadata = None;
            ledger_inclusion_state = None;
            let inherent_worker = SimpleWorker {
                retries: self.retries_per_query,
            };
            // store message only
            self.insert(&inherent_worker, &keyspace, *message_id, message.clone())?;
            // Insert parents/children
            self.insert_parents(
                &inherent_worker,
                &message_id,
                &message.parents(),
                self.est_ms,
                ledger_inclusion_state.clone(),
            )?;
            // insert payload (if any)
            if let Some(payload) = message.payload() {
                self.insert_payload(
                    &inherent_worker,
                    &message_id,
                    &message,
                    &payload,
                    self.est_ms,
                    ledger_inclusion_state,
                    metadata,
                    solidifier_handles,
                )
                .await?;
            }
        };
        Ok(())
    }
    /// Insert the parents' message ids of a given message id to the table
    fn insert_parents<I: Inherent>(
        &self,
        inherent_worker: &I,
        message_id: &MessageId,
        parents: &[MessageId],
        milestone_index: MilestoneIndex,
        inclusion_state: Option<LedgerInclusionState>,
    ) -> anyhow::Result<()> {
        let partition_id = self.get_partition_id(milestone_index);
        for parent_id in parents {
            let partitioned = Partitioned::new(*parent_id, partition_id, milestone_index.0);
            let parent_record = ParentRecord::new(*message_id, inclusion_state);
            self.insert(inherent_worker, &self.get_keyspace(), partitioned, parent_record)?;
            // insert hint record
            let hint = Hint::parent(parent_id.to_string());
            let partition = Partition::new(partition_id, *milestone_index);
            self.insert(inherent_worker, &self.get_keyspace(), hint, partition)?
        }
        Ok(())
    }
    // NOT complete, TODO finish it.
    #[async_recursion::async_recursion]
    async fn insert_payload<I: Inherent + Send + Sync>(
        &mut self,
        inherent_worker: &I,
        message_id: &MessageId,
        message: &Message,
        payload: &Payload,
        milestone_index: MilestoneIndex,
        inclusion_state: Option<LedgerInclusionState>,
        metadata: Option<MessageMetadata>,
        solidifier_handles: &Pool<MapPool<Solidifier, u8>>,
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
                    solidifier_handles,
                )
                .await?
            }
            Payload::Milestone(milestone) => {
                let ms_index = milestone.essence().index();
                let parents_check = message.parents().eq(milestone.essence().parents());
                if metadata.is_some() && parents_check {
                    // push to the right solidifier
                    let solidifier_id = (ms_index.0 % (self.collector_count as u32)) as u8;

                    solidifier_handles
                        .send(
                            &solidifier_id,
                            SolidifierEvent::Milestone(MilestoneMessage::new(
                                *message_id,
                                milestone.clone(),
                                message.clone(),
                                metadata,
                            )),
                        )
                        .await;

                    self.insert(
                        inherent_worker,
                        &self.get_keyspace(),
                        ms_index,
                        (*message_id, milestone.clone()),
                    )?
                }
            }
            // remaining payload types
            e => {
                warn!("Skipping unsupported payload variant: {:?}", e);
            }
        }
        Ok(())
    }
    /// Insert the `Indexation` of a given message id to the table
    fn insert_index<I: Inherent>(
        &self,
        inherent_worker: &I,
        message_id: &MessageId,
        index: Indexation,
        milestone_index: MilestoneIndex,
        inclusion_state: Option<LedgerInclusionState>,
    ) -> anyhow::Result<()> {
        let partition_id = self.get_partition_id(milestone_index);
        let partitioned = Partitioned::new(index.clone(), partition_id, milestone_index.0);
        let index_record = IndexationRecord::new(*message_id, inclusion_state);
        self.insert(inherent_worker, &self.get_keyspace(), partitioned, index_record)?;
        // insert hint record
        let hint = Hint::index(index.0);
        let partition = Partition::new(partition_id, *milestone_index);
        self.insert(inherent_worker, &self.get_keyspace(), hint, partition)
    }
    /// Insert the message metadata to the table
    fn insert_message_metadata(&self, metadata: MessageMetadata) -> anyhow::Result<()> {
        let message_id = metadata.message_id;
        let inherent_worker = SimpleWorker {
            retries: self.retries_per_query,
        };
        // store message and metadata
        self.insert(&inherent_worker, &self.get_keyspace(), message_id, metadata.clone())?;
        // Insert parents/children
        let parents = metadata.parent_message_ids;
        self.insert_parents(
            &inherent_worker,
            &message_id,
            &parents.as_slice(),
            self.ref_ms,
            metadata.ledger_inclusion_state.clone(),
        )
    }
    /// Insert the message with the associated metadata of a given message id to the table
    async fn insert_message_with_metadata(
        &mut self,
        message_id: MessageId,
        message: Message,
        metadata: MessageMetadata,
        solidifier_handles: &Pool<MapPool<Solidifier, u8>>,
    ) -> anyhow::Result<()> {
        #[cfg(feature = "filter")]
        let keyspace = self.get_keyspace_for_message(&mut message);
        #[cfg(not(feature = "filter"))]
        let keyspace = self.get_keyspace();
        let solidifier_id = (self.ref_ms.0 % (self.collector_count as u32)) as u8;
        let solidifier_handle = solidifier_handles
            .get(&solidifier_id)
            .await
            .ok_or_else(|| anyhow::anyhow!("No solidifier for id {}", solidifier_id))?;
        let inherent_worker = AtomicWorker::new(solidifier_handle, *self.ref_ms, message_id, self.retries_per_query);
        // Insert parents/children
        self.insert_parents(
            &inherent_worker,
            &message_id,
            &message.parents(),
            self.ref_ms,
            metadata.ledger_inclusion_state.clone(),
        )?;
        // insert payload (if any)
        if let Some(payload) = message.payload() {
            self.insert_payload(
                &inherent_worker,
                &message_id,
                &message,
                &payload,
                self.ref_ms,
                metadata.ledger_inclusion_state.clone(),
                Some(metadata.clone()),
                solidifier_handles,
            )
            .await?;
        }
        let message_tuple = (message, metadata);
        // store message and metadata
        self.insert(&inherent_worker, &keyspace, message_id, message_tuple)
    }
    /// Insert the transaction to the table
    async fn insert_transaction<I: Inherent + Send + Sync>(
        &mut self,
        inherent_worker: &I,
        message_id: &MessageId,
        message: &Message,
        transaction: &Box<TransactionPayload>,
        ledger_inclusion_state: Option<LedgerInclusionState>,
        milestone_index: MilestoneIndex,
        metadata: Option<MessageMetadata>,
        solidifier_handles: &Pool<MapPool<Solidifier, u8>>,
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
                } else {
                    error!("A new input variant was added to this type!")
                }
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
                    solidifier_handles,
                )
                .await?
            }
        };
        Ok(())
    }
    /// Insert the `InputData` to the table
    fn insert_input<I: Inherent>(
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
        let input_id = (*transaction_id, index);
        let transaction_record = TransactionRecord::input(*message_id, input_data, inclusion_state, milestone_index);
        self.insert(inherent_worker, &self.get_keyspace(), input_id, transaction_record)
    }
    /// Insert the `UnlockData` to the table
    fn insert_unlock<I: Inherent>(
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
        let utxo_id = (*utxo_transaction_id, utxo_index);
        let transaction_record = TransactionRecord::unlock(*message_id, unlock_data, inclusion_state, milestone_index);
        self.insert(inherent_worker, &self.get_keyspace(), utxo_id, transaction_record)
    }
    /// Insert the `Output` to the table
    fn insert_output<I: Inherent>(
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
        let output_id = (*transaction_id, index);
        let transaction_record = TransactionRecord::output(*message_id, output, inclusion_state, milestone_index);
        self.insert(inherent_worker, &self.get_keyspace(), output_id, transaction_record)
    }
    /// Insert the `Address` to the table
    fn insert_address<I: Inherent>(
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
                    let partitioned = Partitioned::new(*ed_address, partition_id, milestone_index.0);
                    let address_record =
                        AddressRecord::new(output_type, *transaction_id, index, sls.amount(), inclusion_state);
                    self.insert(inherent_worker, &self.get_keyspace(), partitioned, address_record)?;
                    // insert hint record
                    let hint = Hint::address(ed_address.to_string());
                    let partition = Partition::new(partition_id, *milestone_index);
                    self.insert(inherent_worker, &self.get_keyspace(), hint, partition)
                }
            }
            Output::SignatureLockedDustAllowance(slda) => {
                let Address::Ed25519(ed_address) = slda.address();
                {
                    let partitioned = Partitioned::new(*ed_address, partition_id, milestone_index.0);
                    let address_record =
                        AddressRecord::new(output_type, *transaction_id, index, slda.amount(), inclusion_state);
                    self.insert(inherent_worker, &self.get_keyspace(), partitioned, address_record)?;
                    // insert hint record
                    let hint = Hint::address(ed_address.to_string());
                    let partition = Partition::new(partition_id, *milestone_index);
                    self.insert(inherent_worker, &self.get_keyspace(), hint, partition)
                }
            }
            e => {
                if let Output::Treasury(_) = e {
                    Ok(())
                } else {
                    bail!("Unexpected new output variant {:?}", e);
                }
            }
        }
    }
    /// The low-level insert function to insert a key/value pair through an inherent worker
    fn insert<I, S, K, V>(&self, inherent_worker: &I, keyspace: &S, key: K, value: V) -> anyhow::Result<()>
    where
        I: Inherent,
        S: 'static + Insert<K, V>,
        K: 'static + Send + Sync + Clone,
        V: 'static + Send + Sync + Clone,
    {
        let insert_req = keyspace.insert(&key, &value).consistency(Consistency::One).build()?;
        let worker = inherent_worker.inherent_boxed(keyspace.clone(), key, value);
        insert_req.send_local(worker);
        Ok(())
    }
    /// Delete the `Parents` of a given message id in the table
    fn delete_parents(
        &self,
        message_id: &MessageId,
        parents: &Parents,
        milestone_index: MilestoneIndex,
    ) -> anyhow::Result<()> {
        let partition_id = self.get_partition_id(milestone_index);
        for parent_id in parents.iter() {
            let parent_pk = ParentPK::new(*parent_id, partition_id, milestone_index, *message_id);
            self.delete(parent_pk)?;
        }
        Ok(())
    }
    /// Delete the `Indexation` of a given message id in the table
    fn delete_indexation(
        &self,
        message_id: &MessageId,
        indexation: Indexation,
        milestone_index: MilestoneIndex,
    ) -> anyhow::Result<()> {
        let partition_id = self.get_partition_id(milestone_index);
        let index_pk = IndexationPK::new(indexation, partition_id, milestone_index, *message_id);
        self.delete(index_pk)
    }
    /// Delete the transaction partitioned rows of a given message id in the table
    fn delete_transaction_partitioned_rows(
        &self,
        message_id: &MessageId,
        transaction: &Box<TransactionPayload>,
        milestone_index: MilestoneIndex,
    ) -> anyhow::Result<()> {
        let transaction_id = transaction.id();
        let Essence::Regular(regular) = transaction.essence();
        {
            if let Some(Payload::Indexation(indexation)) = regular.payload() {
                let index_key = Indexation(hex::encode(indexation.index()));
                self.delete_indexation(&message_id, index_key, milestone_index)?;
            }
            for (output_index, output) in regular.outputs().iter().enumerate() {
                self.delete_address(output, &transaction_id, output_index as u16, milestone_index)?;
            }
        }
        Ok(())
    }
    /// Delete the `Address` with a given `TransactionId` and the corresponding index in the table
    fn delete_address(
        &self,
        output: &Output,
        transaction_id: &TransactionId,
        index: u16,
        milestone_index: MilestoneIndex,
    ) -> anyhow::Result<()> {
        let partition_id = self.get_partition_id(milestone_index);
        let output_type = output.kind();
        match output {
            Output::SignatureLockedSingle(sls) => {
                let Address::Ed25519(ed_address) = sls.address();
                {
                    let address_pk = Ed25519AddressPK::new(
                        *ed_address,
                        partition_id,
                        milestone_index,
                        output_type,
                        *transaction_id,
                        index,
                    );
                    self.delete(address_pk)?;
                }
            }
            Output::SignatureLockedDustAllowance(slda) => {
                let Address::Ed25519(ed_address) = slda.address();
                {
                    let address_pk = Ed25519AddressPK::new(
                        *ed_address,
                        partition_id,
                        milestone_index,
                        output_type,
                        *transaction_id,
                        index,
                    );
                    self.delete(address_pk)?;
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
    /// Delete the key in the `Chronicle` keyspace
    fn delete<K, V>(&self, key: K) -> anyhow::Result<()>
    where
        ChronicleKeyspace: Delete<K, V>,
        K: 'static + Send + Sync + Clone,
        V: 'static + Send + Sync + Clone,
    {
        let delete_req = self
            .default_keyspace
            .delete(&key)
            .consistency(Consistency::One)
            .build()?;
        let worker = DeleteWorker::boxed(self.default_keyspace.clone(), key, self.retries_per_query);
        delete_req.send_local(worker);
        Ok(())
    }
}

/// An atomic solidifier worker
pub struct AtomicWorker {
    /// The arced atomic solidifier handle
    arc_handle: Arc<AtomicSolidifierHandle>,
    /// The number of retires
    retries: usize,
}

impl AtomicWorker {
    /// Create a new atomic solidifier worker with a solidifier handle, an milestone index, a message id, and a number
    /// of retries
    fn new(solidifier_handle: Act<Solidifier>, milestone_index: u32, message_id: MessageId, retries: usize) -> Self {
        let any_error = std::sync::atomic::AtomicBool::new(false);
        let atomic_handle = AtomicSolidifierHandle::new(solidifier_handle, milestone_index, message_id, any_error);
        let arc_handle = std::sync::Arc::new(atomic_handle);
        Self { arc_handle, retries }
    }
}

/// A simple worker with only a field which specifies the number of retires
pub struct SimpleWorker {
    /// The number of retires
    retries: usize,
}

/// The inherent trait to return a boxed worker for a given key/value pair in a keyspace
trait Inherent {
    fn inherent_boxed<S, K, V>(&self, keyspace: S, key: K, value: V) -> Box<dyn Worker>
    where
        S: 'static + Insert<K, V>,
        K: 'static + Send + Sync + Clone,
        V: 'static + Send + Sync + Clone;
}

/// Implement the `Inherent` trait for the simple worker
impl Inherent for SimpleWorker {
    fn inherent_boxed<S, K, V>(&self, keyspace: S, key: K, value: V) -> Box<dyn Worker>
    where
        S: 'static + Insert<K, V>,
        K: 'static + Send + Sync + Clone,
        V: 'static + Send + Sync + Clone,
    {
        InsertWorker::boxed(keyspace, key, value, self.retries)
    }
}

/// Implement the `Inherent` trait for the atomic solidifier worker
impl Inherent for AtomicWorker {
    fn inherent_boxed<S, K, V>(&self, keyspace: S, key: K, value: V) -> Box<dyn Worker>
    where
        S: 'static + Insert<K, V>,
        K: 'static + Send + Sync + Clone,
        V: 'static + Send + Sync + Clone,
    {
        AtomicSolidifierWorker::boxed(self.arc_handle.clone(), keyspace, key, value, self.retries)
    }
}
