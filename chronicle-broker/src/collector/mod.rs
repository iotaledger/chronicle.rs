// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0
use super::*;
use crate::{
    application::*,
    requester::{
        RequesterHandle,
        *,
    },
    solidifier::*,
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
use chronicle_common::metrics::CONFIRMATION_TIME_COLLECTOR;
use chronicle_filter::{
    Selected,
    Selective,
    SelectiveBuilder,
};
use std::{
    collections::{
        BinaryHeap,
        VecDeque,
    },
    fmt::Debug,
    sync::Arc,
};

use chronicle_common::config::PartitionConfig;
use lru::LruCache;

use reqwest::Client;
use url::Url;
pub(crate) type CollectorId = u8;
pub(crate) type CollectorHandle = UnboundedHandle<CollectorEvent>;
pub(crate) type CollectorHandles = HashMap<CollectorId, CollectorHandle>;
/// Collector events
pub enum CollectorEvent {
    /// Requested Message and Metadata, u32 is the milestoneindex
    MessageAndMeta(u32, Option<MessageId>, Option<FullMessage>),
    /// Newly seen message from feed source(s)
    Message(MessageId, Message),
    /// Newly seen MessageMetadataObj from feed source(s)
    MessageReferenced(MessageMetadata),
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
    T: SelectiveBuilder,
{
    /// keysapce
    keyspace: ChronicleKeyspace,
    /// The keyspace partition config
    data_partition_config: PartitionConfig,
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
    lru_msg_ref: LruCache<MessageId, MessageMetadata>,
    /// The hashmap to facilitate the recording the pending requests, which maps from
    /// a message id to the corresponding (milestone index, message) pair
    pending_requests: HashMap<MessageId, (u32, Message)>,
    /// Selective state
    selective: T::State,
}

impl<T: SelectiveBuilder> Collector<T> {
    pub(super) fn new(
        keyspace: ChronicleKeyspace,
        data_partition_config: PartitionConfig,
        partition_id: u8,
        partition_count: u8,
        retries: u8,
        lru_capacity: usize,
        selective: T::State,
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
            selective,
            data_partition_config,
        }
    }
}

#[async_trait]
impl<S, T> Actor<S> for Collector<T>
where
    S: SupHandle<Self>,
    T: SelectiveBuilder,
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
                CollectorEvent::MessageAndMeta(try_ms_index, message_id, opt_full_msg) => {
                    if let Some(FullMessage(message, metadata)) = opt_full_msg {
                        let message_id = message_id
                            .ok_or_else(|| ActorError::exit_msg("Expected message_id in requester response"))?;
                        let partition_id = (try_ms_index % (self.partition_count as u32)) as u8;
                        let ref_ms = metadata
                            .referenced_by_milestone_index
                            .as_ref()
                            .ok_or_else(|| ActorError::exit_msg("Expected referenced_by_milestone_index"))?;
                        // check if the requested message actually belongs to the expected milestone_index
                        if ref_ms.eq(&try_ms_index) {
                            // push full message to solidifier;
                            self.push_fullmsg_to_solidifier(
                                partition_id,
                                message.clone(),
                                metadata.clone(),
                                &solidifier_handles,
                            );
                            // proceed to insert the message and put it in the cache.
                        } else {
                            // close the request
                            self.push_close_to_solidifier(partition_id, message_id, try_ms_index, &solidifier_handles);
                        }
                        // set the ref_ms to be the current requested message ref_ms
                        self.ref_ms.0 = *ref_ms;
                        // check if msg already in lru cache(if so then it's already presisted)
                        let wrong_msg_est_ms;
                        if let Some((_, est_ms, _)) = self.lru_msg.get_mut(&message_id) {
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
                            self.lru_msg.put(message_id, (None, self.ref_ms, message.clone()));
                            wrong_msg_est_ms = None;
                        }
                        // Cache metadata.
                        self.lru_msg_ref.put(message_id, metadata.clone());
                        if let Some(wrong_est_ms) = wrong_msg_est_ms {
                            self.clean_up_wrong_est_msg(&message_id, &message, wrong_est_ms)?
                        }
                        self.insert_message_with_metadata(message_id, message, metadata, &solidifier_handles)
                            .await?
                    } else {
                        error!(
                            "{:?} , unable to fetch message: {:?}, from network triggered by milestone_index: {}",
                            rt.service().directory(),
                            message_id,
                            try_ms_index
                        );
                        // inform solidifier
                        self.send_err_solidifiy(try_ms_index, &solidifier_handles);
                    }
                }
                CollectorEvent::Message(message_id, mut message) => {
                    // check if msg already in lru cache(if so then it's already presisted)
                    if let None = self.lru_msg.get(&message_id) {
                        // store message
                        self.insert_message(&message_id, &message, &solidifier_handles).await?;
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
                        .as_ref()
                        .expect("Expected referenced_by_milestone_index");
                    let partition_id = (ref_ms % (self.partition_count as u32)) as u8;
                    let message_id = metadata.message_id;
                    // set the ref_ms to be the most recent ref_ms
                    self.ref_ms.0 = *ref_ms;
                    // update the est_ms to be the most recent ref_ms+1
                    let new_ms = self.ref_ms.0 + 1;
                    if self.est_ms.0 < new_ms {
                        self.est_ms.0 = new_ms;
                    }
                    // check if msg already in lru cache(if so then it's already presisted)
                    if let None = self.lru_msg_ref.get(&message_id) {
                        // add it to the cache in order to not presist it again.
                        self.lru_msg_ref.put(message_id, metadata.clone());
                        // check if msg already exist in the cache, if so we push it to solidifier
                        let cached_msg: Option<Message>;
                        let wrong_msg_est_ms;
                        if let Some((timestamp, est_ms, message)) = self.lru_msg.get_mut(&message_id) {
                            if let Some(timestamp) = timestamp {
                                let ms = timestamp.elapsed().as_millis() as f64;
                                CONFIRMATION_TIME_COLLECTOR.set(ms);
                            }

                            // check if est_ms is not identical to ref_ms
                            if &est_ms.0 != ref_ms {
                                wrong_msg_est_ms = Some(*est_ms);
                                // adjust est_ms to match the actual ref_ms
                                est_ms.0 = *ref_ms;
                            } else {
                                wrong_msg_est_ms = None;
                            }
                            cached_msg = Some(message.clone());
                            // push to solidifier
                            if let Some(solidifier_handle) = solidifier_handles.get(&partition_id) {
                                let full_message = FullMessage::new(message.clone(), metadata.clone());
                                let full_msg_event = SolidifierEvent::Message(full_message);
                                solidifier_handle.send(full_msg_event).ok();
                            };
                            // however the message_id might had been requested,
                            if let Some((requested_by_this_ms, _)) = self.pending_requests.remove(&message_id) {
                                // check if we have to close it
                                if !requested_by_this_ms.eq(&*ref_ms) {
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
                            self.process_pending_requests(&mut requester_handles, *ref_ms);
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
                                cached_msg = Some(message);
                            } else {
                                cached_msg = None;
                            }
                            wrong_msg_est_ms = None;
                            self.process_pending_requests(&mut requester_handles, *ref_ms);
                        }
                        if let Some(message) = cached_msg {
                            if let Some(wrong_est_ms) = wrong_msg_est_ms {
                                self.clean_up_wrong_est_msg(&message_id, &message, wrong_est_ms)
                                    .unwrap_or_else(|e| {
                                        error!("{}", e);
                                    });
                            }
                            self.insert_message_with_metadata(message_id, message, metadata, &solidifier_handles)
                                .await?;
                        } else {
                            // store it as metadata
                            if self.selective.is_permanode() {
                                self.insert_message_metadata(metadata)?;
                            }
                        }
                    }
                }
                CollectorEvent::Ask(ask) => {
                    match ask {
                        AskCollector::FullMessage(solidifier_id, try_ms_index, message_id, created_by) => {
                            let mut message_tuple = None;
                            if let Some((_, _, message)) = self.lru_msg.get(&message_id) {
                                if let Some(metadata) = self.lru_msg_ref.get(&message_id) {
                                    // metadata exist means we already pushed the full message to the solidifier,
                                    // or the message doesn't belong to the solidifier
                                    if !metadata.referenced_by_milestone_index.unwrap().eq(&try_ms_index) {
                                        self.push_close_to_solidifier(
                                            solidifier_id,
                                            message_id,
                                            try_ms_index,
                                            &solidifier_handles,
                                        );
                                    } else {
                                        if let Some(solidifier_handle) = solidifier_handles.get(&solidifier_id) {
                                            let full_message = FullMessage::new(message.clone(), metadata.clone());
                                            let full_msg_event = SolidifierEvent::Message(full_message);
                                            let _ = solidifier_handle.send(full_msg_event);
                                        }
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
                            // insert the message if requested by syncer to ensure it gets cql responses for all the
                            // requested messages
                            if let Some((message, metadata)) = message_tuple.take() {
                                self.insert_message_with_metadata(
                                    message_id.clone(),
                                    message,
                                    metadata,
                                    &solidifier_handles,
                                )
                                .await?
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
    T: SelectiveBuilder,
{
    /// Send an error event to the solidifier for a given milestone index
    fn send_err_solidifiy(&self, try_ms_index: u32, solidifier_handles: &HashMap<u8, SolidifierHandle>) {
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
        requester_handles.send(RequesterEvent::RequestFullMessage(
            self.partition_id,
            message_id,
            try_ms_index,
        ))
    }
    /// Clean up the message and message_id with the wrong estimated milestone index
    fn clean_up_wrong_est_msg(
        &mut self,
        message_id: &MessageId,
        message: &Message,
        wrong_est_ms: MilestoneIndex,
    ) -> ActorResult<()> {
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
    fn push_fullmsg_to_solidifier(
        &self,
        partition_id: u8,
        message: Message,
        metadata: MessageMetadata,
        solidifier_handles: &HashMap<u8, SolidifierHandle>,
    ) {
        if let Some(solidifier_handle) = solidifier_handles.get(&partition_id) {
            let full_message = FullMessage::new(message, metadata);
            let full_msg_event = SolidifierEvent::Message(full_message);
            solidifier_handle.send(full_msg_event).ok();
        };
    }
    /// Push a `Close` message_id (which doesn't belong to all solidifiers with a given milestone index) to the
    /// solidifier
    fn push_close_to_solidifier(
        &self,
        partition_id: u8,
        message_id: MessageId,
        try_ms_index: u32,
        solidifier_handles: &HashMap<u8, SolidifierHandle>,
    ) {
        if let Some(solidifier_handle) = solidifier_handles.get(&partition_id) {
            let full_msg_event = SolidifierEvent::Close(message_id, try_ms_index);
            solidifier_handle.send(full_msg_event).ok();
        };
    }
    /// Get the Chronicle keyspace
    fn get_keyspace(&self) -> ChronicleKeyspace {
        self.keyspace.clone()
    }
    /// Get the partition id of a given milestone index
    fn get_partition_id(&self, milestone_index: MilestoneIndex) -> u16 {
        self.data_partition_config.partition_id(milestone_index.0)
    }
    /// Insert the message id and message to the table
    async fn insert_message(
        &mut self,
        message_id: &MessageId,
        message: &Message,
        solidifier_handles: &HashMap<u8, SolidifierHandle>,
    ) -> ActorResult<()> {
        // Check if metadata already exist in the cache
        let ledger_inclusion_state;
        let metadata;
        if let Some(meta) = self.lru_msg_ref.get(message_id) {
            // filter the message
            let selected_opt = self.selective.filter_message(message_id, message, Some(meta)).await?;
            if selected_opt.is_none() {
                // todo notify solidifier with PersistedMsg(MessageId, referenced_by_milestone_index, None)
                return Ok(());
            }
            metadata = Some(meta.clone());
            ledger_inclusion_state = meta.ledger_inclusion_state.clone();
            let milestone_index = MilestoneIndex(
                *meta
                    .referenced_by_milestone_index
                    .as_ref()
                    .expect("Expected referenced_by_milestone_index"),
            );
            let solidifier_id = (*milestone_index % (self.partition_count as u32)) as u8;
            let solidifier_handle = solidifier_handles
                .get(&solidifier_id)
                .ok_or_else(|| ActorError::exit_msg("Invalid solidifier handles"))?
                .clone();
            let inherent_worker = AtomicWorker::new(
                solidifier_handle,
                *milestone_index,
                *message_id,
                selected_opt,
                self.retries,
            );
            let message_tuple = (message.clone(), meta.clone());
            // store message and metadata
            self.insert(&inherent_worker, Bee(*message_id), message_tuple)?;
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
                )?;
            }
        } else {
            // filter the message
            let selected_opt = self.selective.filter_message(message_id, message, None).await?;
            if selected_opt.is_none() {
                return Ok(());
            }
            metadata = None;
            ledger_inclusion_state = None;
            let inherent_worker = SimpleWorker { retries: self.retries };
            // store message only
            self.insert(&inherent_worker, Bee(*message_id), message.clone())?;
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
                )?;
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
    ) -> ActorResult<()> {
        let partition_id = self.get_partition_id(milestone_index);
        for parent_id in parents {
            let partitioned = Partitioned::new(Bee(*parent_id), partition_id);
            let parent_record = ParentRecord::new(milestone_index, *message_id, inclusion_state);
            self.insert(inherent_worker, partitioned, parent_record)?;
            // insert hint record
            let hint = Hint::parent(parent_id.to_string());
            let partition = Partition::new(partition_id, *milestone_index);
            self.insert(inherent_worker, hint, partition)?
        }
        Ok(())
    }
    // NOT complete, TODO finish it.
    fn insert_payload<I: Inherent>(
        &mut self,
        inherent_worker: &I,
        message_id: &MessageId,
        message: &Message,
        payload: &Payload,
        milestone_index: MilestoneIndex,
        inclusion_state: Option<LedgerInclusionState>,
        metadata: Option<MessageMetadata>,
        solidifier_handles: &HashMap<u8, SolidifierHandle>,
    ) -> ActorResult<()> {
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
            Payload::Transaction(transaction) => self.insert_transaction(
                inherent_worker,
                message_id,
                message,
                transaction,
                inclusion_state,
                milestone_index,
                metadata,
                solidifier_handles,
            )?,
            Payload::Milestone(milestone) => {
                let ms_index = milestone.essence().index();
                let parents_check = message.parents().eq(milestone.essence().parents());
                if metadata.is_some() && parents_check {
                    // push to the right solidifier
                    let solidifier_id = (ms_index.0 % (self.partition_count as u32)) as u8;
                    if let Some(solidifier_handle) = solidifier_handles.get(&solidifier_id) {
                        let ms_message =
                            MilestoneMessage::new(*message_id, milestone.clone(), message.clone(), metadata);
                        let _ = solidifier_handle.send(SolidifierEvent::Milestone(ms_message));
                    };
                    self.insert(inherent_worker, Bee(ms_index), (Bee(*message_id), milestone.clone()))?
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
    ) -> ActorResult<()> {
        let partition_id = self.get_partition_id(milestone_index);
        let partitioned = Partitioned::new(index.clone(), partition_id);
        let index_record = IndexationRecord::new(milestone_index, *message_id, inclusion_state);
        self.insert(inherent_worker, partitioned, index_record)?;
        // insert hint record
        let hint = Hint::index(index.0);
        let partition = Partition::new(partition_id, *milestone_index);
        self.insert(inherent_worker, hint, partition)
    }
    /// Insert the message metadata to the table
    fn insert_message_metadata(&self, metadata: MessageMetadata) -> ActorResult<()> {
        let message_id = metadata.message_id;
        let inherent_worker = SimpleWorker { retries: self.retries };
        // store message and metadata
        self.insert(&inherent_worker, Bee(message_id), metadata.clone())?;
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
        solidifier_handles: &HashMap<u8, SolidifierHandle>,
    ) -> ActorResult<()> {
        let selected_opt = self
            .selective
            .filter_message(&message_id, &message, Some(&metadata))
            .await
            .map_err(|e| {
                error!("selective-permanode: {}, exiting in progress", e);
                ActorError::exit(e)
            })?;
        if selected_opt.is_none() {
            // todo notify solidifier with PersistedMsg(MessageId, referenced_by_milestone_index, None)
            return Ok(());
        }
        let solidifier_handle = self.clone_solidifier_handle(*self.ref_ms, solidifier_handles);
        let inherent_worker =
            AtomicWorker::new(solidifier_handle, *self.ref_ms, message_id, selected_opt, self.retries);
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
            )?;
        }
        let message_tuple = (message, metadata);
        // store message and metadata
        self.insert(&inherent_worker, Bee(message_id), message_tuple)?;
        Ok(())
    }
    /// Insert the transaction to the table
    fn insert_transaction<I: Inherent>(
        &mut self,
        inherent_worker: &I,
        message_id: &MessageId,
        message: &Message,
        transaction: &Box<TransactionPayload>,
        ledger_inclusion_state: Option<LedgerInclusionState>,
        milestone_index: MilestoneIndex,
        metadata: Option<MessageMetadata>,
        solidifier_handles: &HashMap<u8, SolidifierHandle>,
    ) -> ActorResult<()> {
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
                    error!("A new IOTA input variant was added to bee Input type!");
                    return Err(ActorError::exit_msg(
                        "A new IOTA input variant was added to bee Input type!",
                    ));
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
                )?
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
    ) -> ActorResult<()> {
        // -input variant: (InputTransactionId, InputIndex) -> UTXOInput data column
        let input_id = Bee(*transaction_id);
        let transaction_record =
            TransactionRecord::input(index, *message_id, input_data, inclusion_state, milestone_index);
        self.insert(inherent_worker, input_id, transaction_record)
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
    ) -> ActorResult<()> {
        // -unlock variant: (UtxoInputTransactionId, UtxoInputOutputIndex) -> Unlock data column
        let utxo_id = Bee(*utxo_transaction_id);
        let transaction_record =
            TransactionRecord::unlock(utxo_index, *message_id, unlock_data, inclusion_state, milestone_index);
        self.insert(inherent_worker, utxo_id, transaction_record)
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
    ) -> ActorResult<()> {
        // -output variant: (OutputTransactionId, OutputIndex) -> Output data column
        let output_id = Bee(*transaction_id);
        let transaction_record =
            TransactionRecord::output(index, *message_id, output, inclusion_state, milestone_index);
        self.insert(inherent_worker, output_id, transaction_record)
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
    ) -> ActorResult<()> {
        let partition_id = self.get_partition_id(milestone_index);
        let output_type = output.kind();
        match output {
            Output::SignatureLockedSingle(sls) => {
                let Address::Ed25519(ed_address) = sls.address();
                {
                    let partitioned = Partitioned::new(Bee(*ed_address), partition_id);
                    let address_record = AddressRecord::new(
                        milestone_index,
                        output_type,
                        *transaction_id,
                        index,
                        sls.amount(),
                        inclusion_state,
                    );
                    self.insert(inherent_worker, partitioned, address_record)?;
                    // insert hint record
                    let hint = Hint::address(ed_address.to_string());
                    let partition = Partition::new(partition_id, *milestone_index);
                    self.insert(inherent_worker, hint, partition)?
                }
            }
            Output::SignatureLockedDustAllowance(slda) => {
                let Address::Ed25519(ed_address) = slda.address();
                {
                    let partitioned = Partitioned::new(Bee(*ed_address), partition_id);
                    let address_record = AddressRecord::new(
                        milestone_index,
                        output_type,
                        *transaction_id,
                        index,
                        slda.amount(),
                        inclusion_state,
                    );
                    self.insert(inherent_worker, partitioned, address_record)?;
                    // insert hint record
                    let hint = Hint::address(ed_address.to_string());
                    let partition = Partition::new(partition_id, *milestone_index);
                    self.insert(inherent_worker, hint, partition)?
                }
            }
            e => {
                if let Output::Treasury(_) = e {
                } else {
                    return Err(ActorError::exit_msg(format!("Unexpected new output variant {:?}", e)));
                }
            }
        }
        Ok(())
    }
    /// The low-level insert function to insert a key/value pair through an inherent worker
    fn insert<I, K, V>(&self, inherent_worker: &I, key: K, value: V) -> ActorResult<()>
    where
        I: Inherent,
        ChronicleKeyspace: 'static + Insert<K, V>,
        K: 'static + Send + Sync + Clone + Debug + TokenEncoder,
        V: 'static + Send + Sync + Clone + Debug,
    {
        let insert_req = self
            .keyspace
            .insert(&key, &value)
            .consistency(Consistency::One)
            .build()
            .map_err(|e| ActorError::exit(e))?;
        let worker = inherent_worker.inherent_boxed(self.get_keyspace(), key, value);
        if let Err(RequestError::Ring(r)) = insert_req.send_local_with_worker(worker) {
            let keyspace_name = self.keyspace.name();
            if let Err(worker) = retry_send(&keyspace_name, r, 2) {
                worker.handle_error(WorkerError::NoRing, None)?;
            };
        };
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
            let key = (Bee(*parent_id), partition_id);
            let var = (Bee(milestone_index), Bee(*message_id));
            self.delete(key, var)?;
        }
        Ok(())
    }
    /// Delete the `Indexation` of a given message id in the table
    fn delete_indexation(
        &self,
        message_id: &MessageId,
        indexation: Indexation,
        milestone_index: MilestoneIndex,
    ) -> ActorResult<()> {
        let partition_id = self.get_partition_id(milestone_index);
        let key = (indexation, partition_id);
        let var = (Bee(milestone_index), Bee(*message_id));
        self.delete(key, var)
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
                    let key = (Bee(*ed_address), partition_id);
                    let var = (Bee(milestone_index), output_type, Bee(*transaction_id), index);
                    self.delete(key, var)?;
                }
            }
            Output::SignatureLockedDustAllowance(slda) => {
                let Address::Ed25519(ed_address) = slda.address();
                {
                    let key = (Bee(*ed_address), partition_id);
                    let var = (Bee(milestone_index), output_type, Bee(*transaction_id), index);
                    self.delete(key, var)?;
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
    fn delete<K, Var, V>(&self, key: K, variables: Var) -> ActorResult<()>
    where
        ChronicleKeyspace: Delete<K, Var, V>,
        K: 'static + Send + Sync + Clone + TokenEncoder,
        V: 'static + Send + Sync + Clone,
    {
        self.keyspace
            .delete(&key, &variables)
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
    fn new(
        solidifier_handle: SolidifierHandle,
        milestone_index: u32,
        message_id: MessageId,
        selected_opt: Option<Selected>,
        retries: u8,
    ) -> Self {
        let any_error = std::sync::atomic::AtomicBool::new(false);
        let atomic_handle =
            AtomicSolidifierHandle::new(solidifier_handle, milestone_index, message_id, selected_opt, any_error);
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

/// The inherent trait to return a boxed worker for a given key/value pair in a keyspace
trait Inherent {
    fn inherent_boxed<S, K, V>(&self, keyspace: S, key: K, value: V) -> Box<dyn Worker + 'static>
    where
        S: 'static + Insert<K, V> + Debug,
        K: 'static + Send + Sync + Clone + Debug + TokenEncoder,
        V: 'static + Send + Sync + Clone + Debug;
}

/// Implement the `Inherent` trait for the simple worker
impl Inherent for SimpleWorker {
    fn inherent_boxed<S, K, V>(&self, keyspace: S, key: K, value: V) -> Box<dyn Worker>
    where
        S: 'static + Insert<K, V> + Debug,
        K: 'static + Send + Sync + Clone + Debug + TokenEncoder,
        V: 'static + Send + Sync + Clone + Debug,
    {
        InsertWorker::boxed(keyspace, key, value, self.retries)
    }
}

/// Implement the `Inherent` trait for the atomic solidifier worker
impl Inherent for AtomicWorker {
    fn inherent_boxed<S, K, V>(&self, keyspace: S, key: K, value: V) -> Box<dyn Worker>
    where
        S: 'static + Insert<K, V> + Debug,
        K: 'static + Send + Sync + Clone + Debug + TokenEncoder,
        V: 'static + Send + Sync + Clone + Debug,
    {
        AtomicSolidifierWorker::boxed(self.arc_handle.clone(), keyspace, key, value, self.retries)
    }
}

#[derive(Clone, Debug)]
struct InsertWorker<S: Insert<K, V>, K, V> {
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
                let keyspace_name = self.keyspace.name();
                let statement = self.keyspace.statement();
                PrepareWorker::new(&keyspace_name, id, &statement)
                    .send_to_reporter(reporter)
                    .ok();
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
