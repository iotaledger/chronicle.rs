// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0
use super::{
    filter::FilterBuilder,
    *,
};
use crate::{
    application::*,
    requester::*,
    solidifier::*,
};
use anyhow::bail;
use backstage::core::Channel;
use bee_message::{
    milestone::MilestoneIndex,
    output::OutputId,
    parent::Parents,
    payload::{
        transaction::{
            TransactionEssence,
            TransactionId,
            TransactionPayload,
        },
        Payload,
    },
};
use bee_rest_api::types::responses::MessageMetadataResponse;
use chronicle_common::metrics::CONFIRMATION_TIME_COLLECTOR;
use chronicle_storage::ConflictReason;
use lru::LruCache;
use std::{
    fmt::Debug,
    str::FromStr,
    sync::Arc,
};

pub(crate) type CollectorId = u8;
pub(crate) type CollectorHandle = UnboundedHandle<CollectorEvent>;
pub(crate) type CollectorHandles = HashMap<CollectorId, CollectorHandle>;

/// Collector events
#[derive(Debug)]
pub enum CollectorEvent {
    /// Requested Message and Metadata, u32 is the milestoneindex
    MessageAndMeta(MessageId, Option<MessageRecord>),
    // Requested Milestone
    Milestone(u32, Option<MessageRecord>),
    /// Newly seen message from feed source(s)
    Message(MessageId, Message),
    /// Newly seen MessageMetadataObj from feed source(s)
    MessageReferenced(MessageMetadataResponse),
    /// Ask requests from solidifier(s)
    Ask(AskCollector),
    /// Shutdown the collector
    Shutdown,
}
impl ShutdownEvent for CollectorEvent {
    fn shutdown_event() -> Self {
        Self::Shutdown
    }
}
#[derive(Debug)]
/// Messages for asking the collector for missing data
pub enum AskCollector {
    /// Solidifier(s) will use this variant, u8 is solidifier_id
    FullMessage(u8, u32, MessageId, CreatedBy),
    /// Ask for a milestone with the given index
    MilestoneMessage(u32),
}

#[derive(Clone, Copy)]
pub struct MessageIdPartitioner {
    count: u8,
}

impl MessageIdPartitioner {
    /// Create new partitioner
    pub fn new(count: u8) -> Self {
        Self { count }
    }
    /// Get the partition id
    pub fn partition_id(&self, message_id: &MessageId) -> u8 {
        // partitioning based on first byte of the message_id
        message_id.as_ref()[0] % self.count
    }
    /// Get the partition count
    pub fn partition_count(&self) -> u8 {
        self.count
    }
}
/// Collector state, each collector is basically LRU cache
pub struct Collector<T>
where
    T: FilterBuilder,
{
    /// keysapce
    keyspace: ChronicleKeyspace,
    /// The partition id
    partition_id: u8,
    /// partition_count
    partition_count: u8,
    /// retries
    retries: u8,
    /// The estimated milestone index
    est_ms: MilestoneIndex,
    /// The referenced milestone index
    ref_ms: MilestoneIndex,
    /// The LRU cache from message id to (milestone index, message) pair
    lru_msg: LruCache<MessageId, (Option<std::time::Instant>, MilestoneIndex, Message)>,
    /// The LRU cache from message id to message metadata
    lru_msg_ref: LruCache<MessageId, (MilestoneIndex, Option<LedgerInclusionState>, Option<ConflictReason>)>,
    /// The hashmap to facilitate the recording the pending requests, which maps from
    /// a message id to the corresponding (milestone index, message) pair
    pending_requests: HashMap<MessageId, (u32, Message)>,
    /// selective builder
    selective_builder: T,
    /// uda actor handle
    uda_handle: <<T::Actor as Actor<BrokerHandle>>::Channel as Channel>::Handle,
}

impl<T: FilterBuilder> Collector<T> {
    pub(super) fn new(
        keyspace: ChronicleKeyspace,
        partition_id: u8,
        partition_count: u8,
        retries: u8,
        lru_capacity: usize,
        selective_builder: T,
        uda_handle: <<T::Actor as Actor<BrokerHandle>>::Channel as Channel>::Handle,
    ) -> Self {
        Self {
            keyspace,
            partition_id,
            partition_count,
            retries,
            est_ms: MilestoneIndex(0),
            ref_ms: MilestoneIndex(0),
            lru_msg: LruCache::new(lru_capacity),
            lru_msg_ref: LruCache::new(lru_capacity),
            pending_requests: HashMap::new(),
            selective_builder,
            uda_handle,
        }
    }
}

#[async_trait]
impl<S, T> Actor<S> for Collector<T>
where
    S: SupHandle<Self>,
    T: FilterBuilder,
{
    type Data = (HashMap<u8, SolidifierHandle>, RequesterHandles<T>);
    type Channel = UnboundedChannel<CollectorEvent>;
    async fn init(&mut self, rt: &mut Rt<Self, S>) -> ActorResult<Self::Data> {
        log::info!("{:?} is initializing", rt.service().directory());
        let parent_id = rt
            .parent_id()
            .ok_or_else(|| ActorError::exit_msg("Collector without parent"))?;
        let solidifier_handles = rt.depends_on(parent_id).await?;
        let requester_handles = rt.depends_on(parent_id).await?;
        Ok((solidifier_handles, requester_handles))
    }
    async fn run(
        &mut self,
        rt: &mut Rt<Self, S>,
        (solidifier_handles, mut requester_handles): Self::Data,
    ) -> ActorResult<()> {
        log::info!("{:?} is running", rt.service().directory());
        while let Some(event) = rt.inbox_mut().next().await {
            match event {
                CollectorEvent::MessageAndMeta(message_id, message) => {
                    if let Some(message) = message {
                        let mut selected: Option<Selected> = None;
                        let milestone_index = message.milestone_index().unwrap().0;
                        let partition_id = (milestone_index % (self.partition_count as u32)) as u8;
                        // filter the message
                        selected = self
                            .selective_builder
                            .filter_message(&self.uda_handle, &message)
                            .await?;
                        // push full message to solidifier;
                        self.push_fullmsg_to_solidifier(partition_id, message.clone(), &solidifier_handles, selected);
                        // set the ref_ms to be the current requested message ref_ms
                        self.ref_ms.0 = milestone_index;
                        // check if msg already in lru cache(if so then it's already presisted)
                        let wrong_msg_est_ms;
                        if let Some((_, est_ms, _)) = self.lru_msg.get_mut(&message_id) {
                            // check if est_ms is not identical to ref_ms
                            if est_ms.0 != milestone_index {
                                wrong_msg_est_ms = Some(*est_ms);
                                // adjust est_ms to match the actual ref_ms
                                est_ms.0 = milestone_index;
                            } else {
                                wrong_msg_est_ms = None;
                            }
                        } else {
                            // add it to the cache in order to not presist it again.
                            self.lru_msg
                                .put(message_id, (None, self.ref_ms, message.message.clone()));
                            wrong_msg_est_ms = None;
                        }
                        // Cache metadata.
                        self.lru_msg_ref.put(
                            message_id,
                            (
                                milestone_index.into(),
                                message.inclusion_state().cloned(),
                                message.conflict_reason().cloned(),
                            ),
                        );
                        if let Some(wrong_est_ms) = wrong_msg_est_ms {
                            self.clean_up_wrong_est_msg(&message, wrong_est_ms)?
                        }
                        self.insert_message_with_metadata(message, &solidifier_handles, selected)
                            .await?
                    } else {
                        error!(
                            "{:?}, unable to fetch message: {:?}",
                            rt.service().directory(),
                            message_id
                        );
                    }
                }
                CollectorEvent::Milestone(milestone_index, message) => {
                    if let Some(message) = message {
                        let mut selected: Option<Selected> = None;
                        let partition_id = (milestone_index % (self.partition_count as u32)) as u8;
                        // filter the message
                        selected = self
                            .selective_builder
                            .filter_message(&self.uda_handle, &message)
                            .await?;
                        // push full message to solidifier;
                        self.push_fullmsg_to_solidifier(partition_id, message.clone(), &solidifier_handles, selected);
                        // Cache metadata.
                        self.lru_msg_ref.put(
                            message.message_id,
                            (
                                milestone_index.into(),
                                message.inclusion_state().cloned(),
                                message.conflict_reason().cloned(),
                            ),
                        );
                        // set the ref_ms to be the current requested message ref_ms
                        self.ref_ms.0 = milestone_index;
                        self.insert_message_with_metadata(message, &solidifier_handles, selected)
                            .await?
                    } else {
                        error!(
                            "{:?}, unable to fetch milestone: {:?}",
                            rt.service().directory(),
                            milestone_index
                        );
                        // inform solidifier
                        self.send_err_solidify(milestone_index, &solidifier_handles);
                    }
                }
                CollectorEvent::Message(message_id, message) => {
                    // check if msg already in lru cache(if so then it's already presisted)
                    if let None = self.lru_msg.get(&message_id) {
                        // store message
                        self.insert_message(message_id, message.clone(), &solidifier_handles)
                            .await?;
                        // add it to the cache in order to not presist it again.
                        self.lru_msg
                            .put(message_id, (Some(std::time::Instant::now()), self.est_ms, message));
                    }
                }
                CollectorEvent::MessageReferenced(metadata) => {
                    if metadata.referenced_by_milestone_index.is_none() {
                        // metadata is not referenced yet, so we discard it.
                        continue;
                    }
                    let ref_ms = metadata
                        .referenced_by_milestone_index
                        .expect("Expected referenced_by_milestone_index");
                    let partition_id = (ref_ms % (self.partition_count as u32)) as u8;
                    let message_id = MessageId::from_str(&metadata.message_id).unwrap();
                    // set the ref_ms to be the most recent ref_ms
                    self.ref_ms.0 = ref_ms;
                    // update the est_ms to be the most recent ref_ms+1
                    let new_ms = self.ref_ms.0 + 1;
                    if self.est_ms.0 < new_ms {
                        self.est_ms.0 = new_ms;
                    }
                    // check if msg already in lru cache(if so then it's already presisted)
                    if let None = self.lru_msg_ref.get(&message_id) {
                        // add it to the cache in order to not presist it again.
                        self.lru_msg_ref.put(
                            message_id,
                            (
                                ref_ms.into(),
                                metadata.ledger_inclusion_state.clone().map(Into::into),
                                metadata.conflict_reason.and_then(|c| c.try_into().ok()),
                            ),
                        );
                        // check if msg already exist in the cache, if so we push it to solidifier
                        if let Some((timestamp, est_ms, message)) = self.lru_msg.get_mut(&message_id) {
                            let message = MessageRecord::from((message.clone(), metadata));
                            let mut est_ms = est_ms.0;

                            if let Some(timestamp) = timestamp {
                                let ms = timestamp.elapsed().as_millis() as f64;
                                CONFIRMATION_TIME_COLLECTOR.set(ms);
                            }

                            // check if est_ms is not identical to ref_ms
                            if est_ms != ref_ms {
                                self.clean_up_wrong_est_msg(&message, est_ms.into())
                                    .unwrap_or_else(|e| {
                                        error!("{}", e);
                                    });
                                // adjust est_ms to match the actual ref_ms
                                est_ms = ref_ms;
                            }
                            let selected = self
                                .selective_builder
                                .filter_message(&self.uda_handle, &message)
                                .await?;
                            // push to solidifier
                            if let Some(solidifier_handle) = solidifier_handles.get(&partition_id) {
                                solidifier_handle
                                    .send(SolidifierEvent::Message(message.clone(), selected))
                                    .ok();
                            };
                            self.insert_message_with_metadata(message, &solidifier_handles, selected)
                                .await?;
                            // however the message_id might had been requested,
                            if let Some((requested_by_this_ms, _)) = self.pending_requests.remove(&message_id) {
                                // check if we have to close it
                                if requested_by_this_ms != ref_ms {
                                    // close it
                                    let solidifier_id = (requested_by_this_ms % (self.partition_count as u32)) as u8;
                                    self.push_close_to_solidifier(
                                        solidifier_id,
                                        message_id,
                                        requested_by_this_ms,
                                        &solidifier_handles,
                                    );
                                }
                            }
                            // request all pending_requests with less than the received milestone index
                            self.process_pending_requests(&mut requester_handles, ref_ms);
                        } else {
                            // check if it's in the pending_requests
                            if let Some((requested_by_this_ms, message)) = self.pending_requests.remove(&message_id) {
                                let message = MessageRecord::from((message.clone(), metadata));
                                let mut selected = None;

                                // check if we have to close or push full message
                                if requested_by_this_ms == ref_ms {
                                    selected = self
                                        .selective_builder
                                        .filter_message(&self.uda_handle, &message)
                                        .await?;
                                    // push full message
                                    self.push_fullmsg_to_solidifier(
                                        partition_id,
                                        message.clone(),
                                        &solidifier_handles,
                                        selected,
                                    )
                                } else {
                                    // close it
                                    let solidifier_id = (requested_by_this_ms % (self.partition_count as u32)) as u8;
                                    self.push_close_to_solidifier(
                                        solidifier_id,
                                        message_id,
                                        requested_by_this_ms,
                                        &solidifier_handles,
                                    );
                                }
                                self.insert_message_with_metadata(message, &solidifier_handles, selected)
                                    .await?;
                            }
                            self.process_pending_requests(&mut requester_handles, ref_ms);
                        }
                    }
                }
                CollectorEvent::Ask(ask) => {
                    match ask {
                        AskCollector::FullMessage(solidifier_id, try_ms_index, message_id, created_by) => {
                            if let Some((_, _, message)) = self.lru_msg.get(&message_id) {
                                if let Some((milestone_index, inclusion_state, conflict_reason)) =
                                    self.lru_msg_ref.get(&message_id)
                                {
                                    let mut message = MessageRecord::from(message.clone());
                                    message.milestone_index = Some(*milestone_index);
                                    message.inclusion_state = inclusion_state.clone();
                                    message.conflict_reason = conflict_reason.clone();
                                    // metadata exist means we already pushed the full message to the solidifier,
                                    // or the message doesn't belong to the solidifier
                                    if !milestone_index.0.eq(&try_ms_index) {
                                        self.push_close_to_solidifier(
                                            solidifier_id,
                                            message_id,
                                            try_ms_index,
                                            &solidifier_handles,
                                        );
                                    } else {
                                        // make sure to insert the message if it's requested by syncer
                                        if created_by == CreatedBy::Syncer {
                                            // re-filter the message
                                            let selected = self
                                                .selective_builder
                                                .filter_message(&self.uda_handle, &message)
                                                .await?;
                                            if let Some(solidifier_handle) = solidifier_handles.get(&solidifier_id) {
                                                solidifier_handle
                                                    .send(SolidifierEvent::Message(message.clone(), selected))
                                                    .ok();
                                            }
                                            self.ref_ms.0 = try_ms_index;
                                            self.insert_message_with_metadata(message, &solidifier_handles, selected)
                                                .await?
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
                                                    );
                                                } else {
                                                    // overwrite pre_ms_index by try_ms_index, which it will be
                                                    // eventually processed;
                                                    *pre_ms_index = try_ms_index;
                                                    // close pre_ms_index(old_ms) as it's greater than what we have atm
                                                    // (try_ms_index).
                                                    let solidifier_id = (old_ms % (self.partition_count as u32)) as u8;
                                                    self.push_close_to_solidifier(
                                                        solidifier_id,
                                                        message_id,
                                                        old_ms,
                                                        &solidifier_handles,
                                                    );
                                                }
                                            } else {
                                                // add it to back_pressured requests
                                                self.pending_requests
                                                    .insert(message_id, (try_ms_index, message.clone()));
                                            };
                                        } else {
                                            self.request_full_message(&mut requester_handles, message_id, try_ms_index);
                                        }
                                    } else {
                                        self.request_full_message(&mut requester_handles, message_id, try_ms_index);
                                    }
                                }
                            } else {
                                self.request_full_message(&mut requester_handles, message_id, try_ms_index);
                            }
                        }
                        AskCollector::MilestoneMessage(milestone_index) => {
                            // Request it from network
                            self.request_milestone_message(&mut requester_handles, milestone_index);
                        }
                    }
                }
                CollectorEvent::Shutdown => break,
            }
        }
        log::info!("{:?} exited its event loop", &rt.service().directory());
        Ok(())
    }
}

impl<T> Collector<T>
where
    T: FilterBuilder,
{
    /// Send an error event to the solidifier for a given milestone index
    fn send_err_solidify(&self, try_ms_index: u32, solidifier_handles: &HashMap<u8, SolidifierHandle>) {
        // inform solidifier
        let solidifier_id = (try_ms_index % (self.partition_count as u32)) as u8;
        let solidifier_handle = solidifier_handles
            .get(&solidifier_id)
            .expect("Invalid solidifier handles");
        solidifier_handle
            .send(SolidifierEvent::Solidify(Err(try_ms_index)))
            .ok();
    }
    /// Process the pending requests for a given milestone index
    fn process_pending_requests(&mut self, requester_handles: &mut RequesterHandles<T>, milestone_index: u32) {
        self.pending_requests = std::mem::take(&mut self.pending_requests)
            .into_iter()
            .filter_map(|(message_id, (ms, msg))| {
                if ms < milestone_index {
                    self.request_full_message(requester_handles, message_id, ms);
                    None
                } else {
                    Some((message_id, (ms, msg)))
                }
            })
            .collect();
    }
    /// Get the cloned solidifier handle
    fn clone_solidifier_handle(
        &self,
        milestone_index: u32,
        solidifier_handles: &HashMap<u8, SolidifierHandle>,
    ) -> SolidifierHandle {
        let solidifier_id = (milestone_index % (self.partition_count as u32)) as u8;
        solidifier_handles
            .get(&solidifier_id)
            .expect("Expected valid solidifier handles")
            .clone()
    }
    /// Request the milestone message of a given milestone index
    fn request_milestone_message(
        &self,
        requester_handles: &mut RequesterHandles<T>,
        milestone_index: u32,
    ) -> Option<()> {
        requester_handles.send(RequesterEvent::RequestMilestone(self.partition_id, milestone_index))
    }
    /// Request the full message (i.e., including both message and metadata) of a given message id and
    /// a milestone index
    fn request_full_message(
        &self,
        requester_handles: &mut RequesterHandles<T>,
        message_id: MessageId,
        try_ms_index: u32,
    ) -> Option<()> {
        requester_handles.send(RequesterEvent::RequestFullMessage(self.partition_id, message_id))
    }
    /// Clean up the message and message_id with the wrong estimated milestone index
    fn clean_up_wrong_est_msg(&mut self, message: &MessageRecord, wrong_est_ms: MilestoneIndex) -> ActorResult<()> {
        self.delete_parents(message.message_id, message.parents(), wrong_est_ms)?;
        match message.payload() {
            // delete indexation if any
            Some(Payload::Indexation(indexation)) => {
                let index_key = Indexation(hex::encode(indexation.index()));
                self.delete_indexation(message.message_id, index_key, wrong_est_ms)?;
            }
            // delete transactiion partitioned rows if any
            Some(Payload::Transaction(transaction_payload)) => {
                self.delete_transaction_partitioned_rows(message.message_id, transaction_payload, wrong_est_ms)?;
            }
            _ => {}
        }
        Ok(())
    }
    /// Push the full message (both the message and message metadata) to a given partiion of the solidifier
    fn push_fullmsg_to_solidifier(
        &self,
        partition_id: u8,
        message: MessageRecord,
        solidifier_handles: &HashMap<u8, SolidifierHandle>,
        selected: Option<Selected>,
    ) {
        if let Some(solidifier_handle) = solidifier_handles.get(&partition_id) {
            solidifier_handle.send(SolidifierEvent::Message(message, selected)).ok();
        };
    }
    /// Push a `Close` message_id (which doesn't belong to all solidifiers with a given milestone index) to the
    /// solidifier
    fn push_close_to_solidifier(
        &self,
        partition_id: u8,
        message_id: MessageId,
        milestone_index: u32,
        solidifier_handles: &HashMap<u8, SolidifierHandle>,
    ) {
        if let Some(solidifier_handle) = solidifier_handles.get(&partition_id) {
            let full_msg_event = SolidifierEvent::Close(message_id, milestone_index);
            solidifier_handle.send(full_msg_event).ok();
        };
    }
    /// Insert the message id and message to the table
    async fn insert_message(
        &mut self,
        message_id: MessageId,
        message: Message,
        solidifier_handles: &HashMap<u8, SolidifierHandle>,
    ) -> ActorResult<()> {
        // Check if metadata already exist in the cache;
        let mut message = MessageRecord::from(message);
        if let Some((milestone_index, inclusion_state, conflict_reason)) = self.lru_msg_ref.get(&message_id) {
            message.milestone_index = Some(*milestone_index);
            message.inclusion_state = *inclusion_state;
            message.conflict_reason = *conflict_reason;
            // filter the message
            let selected = self
                .selective_builder
                .filter_message(&self.uda_handle, &message)
                .await?;
            let solidifier_id = (milestone_index.0 % (self.partition_count as u32)) as u8;
            if let Some(solidifier_handle) = solidifier_handles.get(&solidifier_id) {
                solidifier_handle
                    .send(SolidifierEvent::Message(message.clone(), selected))
                    .ok();
            };
            if selected.is_none() {
                return Ok(());
            }
            let solidifier_handle = solidifier_handles
                .get(&solidifier_id)
                .ok_or_else(|| ActorError::exit_msg("Invalid solidifier handles"))?
                .clone();
            let inherent_worker = AtomicWorker::new(solidifier_handle, milestone_index.0, message_id, self.retries);
            let keyspace = self.get_keyspace();
            // store message and metadata
            inserts::insert(&keyspace, &inherent_worker, message.clone(), ())?;
            // Insert parents/children
            inserts::insert_parents(&keyspace, &inherent_worker, &message)?;
            // insert payload (if any)
            if let Some(payload) = message.payload() {
                inserts::insert_payload(
                    &keyspace,
                    &inherent_worker,
                    &payload,
                    &message,
                    Some(solidifier_handles),
                    selected,
                )?;
            }
        } else {
            // filter the message
            let selected = self
                .selective_builder
                .filter_message(&self.uda_handle, &message)
                .await?;
            if selected.is_none() {
                return Ok(());
            }
            let inherent_worker = SimpleWorker { retries: self.retries };
            message.milestone_index = Some(self.est_ms);
            let keyspace = self.get_keyspace();
            // store message only
            inserts::insert(&keyspace, &inherent_worker, message.clone(), ())?;
            // Insert parents/children
            inserts::insert_parents(&keyspace, &inherent_worker, &message)?;
            // insert payload (if any)
            if let Some(payload) = message.payload() {
                inserts::insert_payload(
                    &keyspace,
                    &inherent_worker,
                    &payload,
                    &message,
                    Some(solidifier_handles),
                    selected,
                )?;
            }
        };
        Ok(())
    }

    /// Insert the message with the associated metadata of a given message id to the table
    async fn insert_message_with_metadata(
        &mut self,
        message: MessageRecord,
        solidifier_handles: &HashMap<u8, SolidifierHandle>,
        selected: Option<Selected>,
    ) -> ActorResult<()> {
        if selected.is_none() {
            return Ok(());
        }
        let solidifier_handle = self.clone_solidifier_handle(*self.ref_ms, solidifier_handles);
        let inherent_worker = AtomicWorker::new(solidifier_handle, *self.ref_ms, message.message_id, self.retries);
        let keyspace = self.get_keyspace();
        // Insert parents/children
        inserts::insert_parents(&keyspace, &inherent_worker, &message)?;
        // insert payload (if any)
        if let Some(payload) = message.payload() {
            inserts::insert_payload(
                &keyspace,
                &inherent_worker,
                &payload,
                &message,
                Some(solidifier_handles),
                selected,
            )?;
        }
        // store message and metadata
        inserts::insert(&keyspace, &inherent_worker, message, ())?;
        Ok(())
    }
    /// Get the Chronicle keyspace
    fn get_keyspace(&self) -> ChronicleKeyspace {
        self.keyspace.clone()
    }

    /// Delete the `Parents` of a given message id in the table
    fn delete_parents(
        &self,
        message_id: MessageId,
        parents: &Parents,
        milestone_index: MilestoneIndex,
    ) -> anyhow::Result<()> {
        for parent_id in parents.iter() {
            self.delete(&Bee(*parent_id), &Bee(message_id))?;
        }
        Ok(())
    }
    /// Delete the `Indexation` of a given message id in the table
    fn delete_indexation(
        &self,
        message_id: MessageId,
        indexation: Indexation,
        milestone_index: MilestoneIndex,
    ) -> ActorResult<()> {
        let ms_range_id = TagRecord::range_id(milestone_index.0);
        self.delete(&(indexation.0, ms_range_id), &(Bee(milestone_index), Bee(message_id)))
    }
    /// Delete the transaction partitioned rows of a given message id in the table
    fn delete_transaction_partitioned_rows(
        &self,
        message_id: MessageId,
        transaction: &Box<TransactionPayload>,
        milestone_index: MilestoneIndex,
    ) -> anyhow::Result<()> {
        let transaction_id = transaction.id();
        let TransactionEssence::Regular(regular) = transaction.essence();
        {
            if let Some(Payload::Indexation(indexation)) = regular.payload() {
                let index_key = Indexation(hex::encode(indexation.index()));
                self.delete_indexation(message_id, index_key, milestone_index)?;
            }
            for (output_index, _output) in regular.outputs().iter().enumerate() {
                self.delete_legacy_output(transaction_id, output_index as u16, milestone_index)?;
            }
        }
        Ok(())
    }
    /// Delete the `Address` with a given `TransactionId` and the corresponding index in the table
    fn delete_legacy_output(
        &self,
        transaction_id: TransactionId,
        index: u16,
        milestone_index: MilestoneIndex,
    ) -> anyhow::Result<()> {
        let ms_range_id = LegacyOutputRecord::range_id(milestone_index.0);
        self.delete(&Bee(OutputId::new(transaction_id, index).unwrap()), &())?;
        Ok(())
    }
    /// Delete the key in the `Chronicle` keyspace
    fn delete<K, Var, V>(&self, key: &K, variables: &Var) -> ActorResult<()>
    where
        ChronicleKeyspace: Delete<K, Var, V>,
        K: 'static + Send + Sync + Clone + TokenEncoder,
        V: 'static + Send + Sync + Clone,
    {
        self.keyspace
            .delete(key, variables)
            .consistency(Consistency::Quorum)
            .build()
            .map_err(|e| ActorError::exit(e))?
            .worker()
            .with_retries(self.retries as usize)
            .send_local()
            .ok();
        Ok(())
    }
}

/// An atomic solidifier worker
pub struct AtomicWorker {
    /// The arced atomic solidifier handle
    arc_handle: Arc<AtomicSolidifierHandle>,
    /// The number of retires
    retries: u8,
}

impl AtomicWorker {
    /// Create a new atomic solidifier worker with a solidifier handle, an milestone index, a message id, and a number
    /// of retries
    fn new(solidifier_handle: SolidifierHandle, milestone_index: u32, message_id: MessageId, retries: u8) -> Self {
        let any_error = std::sync::atomic::AtomicBool::new(false);
        let atomic_handle = AtomicSolidifierHandle::new(solidifier_handle, milestone_index, message_id, any_error);
        let arc_handle = std::sync::Arc::new(atomic_handle);
        Self { arc_handle, retries }
    }
}

/// A simple worker with only a field which specifies the number of retires
#[derive(Debug, Clone)]
pub struct SimpleWorker {
    /// The number of retires
    retries: u8,
}

/// Implement the `Inherent` trait for the simple worker
impl<
        S: 'static + Insert<K, V> + Debug,
        K: 'static + Send + Sync + Clone + TokenEncoder + Debug,
        V: 'static + Send + Sync + Clone + Debug,
    > Inherent<S, K, V> for SimpleWorker
{
    type Output = InsertWorker<S, K, V>;
    fn inherent_boxed(&self, keyspace: S, key: K, value: V) -> Box<Self::Output>
    where
        S: 'static + Insert<K, V> + Debug,
        K: 'static + Send + Sync + Clone + Debug + TokenEncoder,
        V: 'static + Send + Sync + Clone + Debug,
    {
        InsertWorker::boxed(keyspace, key, value, self.retries)
    }
}

/// Implement the `Inherent` trait for the atomic solidifier worker
impl<
        S: 'static + Insert<K, V> + Debug,
        K: 'static + Send + Sync + Clone + Debug + TokenEncoder,
        V: 'static + Send + Sync + Clone + Debug,
    > Inherent<S, K, V> for AtomicWorker
{
    type Output = AtomicSolidifierWorker<S, K, V>;
    fn inherent_boxed(&self, keyspace: S, key: K, value: V) -> Box<Self::Output>
    where
        S: 'static + Insert<K, V> + Debug,
        K: 'static + Send + Sync + Clone + Debug + TokenEncoder,
        V: 'static + Send + Sync + Clone + Debug,
    {
        AtomicSolidifierWorker::boxed(self.arc_handle.clone(), keyspace, key, value, self.retries)
    }
}

#[derive(Clone, Debug)]
pub struct InsertWorker<S: Insert<K, V>, K, V> {
    keyspace: S,
    key: K,
    value: V,
    retries: u8,
}

impl<S: Insert<K, V>, K, V> InsertWorker<S, K, V>
where
    S: 'static + Insert<K, V>,
    K: 'static + Send + Sync + Clone + TokenEncoder,
    V: 'static + Send + Sync + Clone,
{
    /// Create a new insert worker with a number of retries
    pub fn new(keyspace: S, key: K, value: V, retries: u8) -> Self {
        Self {
            keyspace,
            key,
            value,
            retries,
        }
    }
    /// Create a new boxed insert worker with a number of retries
    pub fn boxed(keyspace: S, key: K, value: V, retries: u8) -> Box<Self> {
        Box::new(Self::new(keyspace, key, value, retries))
    }
}

impl<S, K, V> Worker for InsertWorker<S, K, V>
where
    S: 'static + Insert<K, V> + Debug,
    K: 'static + Send + Sync + Clone + TokenEncoder + Debug,
    V: 'static + Send + Sync + Clone + Debug,
{
    fn handle_response(self: Box<Self>, giveload: Vec<u8>) -> anyhow::Result<()> {
        Decoder::try_from(giveload)?;
        Ok(())
    }

    fn handle_error(
        mut self: Box<Self>,
        mut error: WorkerError,
        reporter: Option<&ReporterHandle>,
    ) -> anyhow::Result<()> {
        if let WorkerError::Cql(ref mut cql_error) = error {
            if let (Some(id), Some(reporter)) = (cql_error.take_unprepared_id(), reporter) {
                let statement = self.keyspace.statement();
                PrepareWorker::new(id, statement.into()).send_to_reporter(reporter).ok();
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
            let keyspace_name = self.keyspace.name();
            if let Err(RequestError::Ring(r)) = req.send_global_with_worker(self) {
                if let Err(worker) = retry_send(&keyspace_name, r, 2) {
                    // todo? maybe spawn future task
                    worker.handle_error(WorkerError::NoRing, None)?
                };
            };
        } else {
            bail!("Basic Inserter worker consumed all retries")
        }

        Ok(())
    }
}
