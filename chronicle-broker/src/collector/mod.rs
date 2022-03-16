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
use backstage::core::Channel;
use bee_message::{
    milestone::MilestoneIndex,
    payload::Payload,
};
use bee_rest_api::types::responses::MessageMetadataResponse;
use chronicle_common::metrics::CONFIRMATION_TIME_COLLECTOR;
use linked_hash_map::LinkedHashMap;
use lru::LruCache;
use std::{
    fmt::Debug,
    str::FromStr,
};
pub(crate) type CollectorId = u8;
pub(crate) type CollectorHandle = UnboundedHandle<CollectorEvent>;
pub(crate) type CollectorHandles = HashMap<CollectorId, CollectorHandle>;

/// Collector events
#[derive(Debug)]
pub enum CollectorEvent {
    /// Requested Message and Metadata, u32 is the milestoneindex
    MsgFromRequester(MessageId, Option<(Message, MessageMetadataResponse)>),
    /// Requested milestone message
    MilestoneFromRequester(u32, Option<(Message, MessageMetadataResponse)>),
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
    lru_msg: LruCache<
        MessageId,
        (
            Option<MessageRecord>,
            Option<MessageMetadataResponse>,
            std::time::SystemTime,
            Option<Selected>,
        ),
    >,
    requester_budget: usize,
    requester_usage: usize,
    /// The hashmap to facilitate the recording the pending requests, which maps from
    /// a message id to the corresponding (try milestone index, message) pair
    pending_requests: LinkedHashMap<MessageId, HashSet<u32>>,
    /// Flag the requested messages
    requested_requests: HashMap<MessageId, HashSet<u32>>,
    /// selective builder
    selective_builder: T,
    /// uda actor handle
    uda_handle: <<T::Actor as Actor<BrokerHandle>>::Channel as Channel>::Handle,
}

impl<T: FilterBuilder> Collector<T> {
    pub(super) fn new(
        partition_id: u8,
        partition_count: u8,
        retries: u8,
        lru_capacity: usize,
        requester_budget: usize,
        selective_builder: T,
        uda_handle: <<T::Actor as Actor<BrokerHandle>>::Channel as Channel>::Handle,
    ) -> Self {
        Self {
            partition_id,
            partition_count,
            retries,
            est_ms: MilestoneIndex(0),
            ref_ms: MilestoneIndex(0),
            requester_budget,
            requester_usage: 0,
            lru_msg: LruCache::new(lru_capacity),
            pending_requests: LinkedHashMap::new(),
            requested_requests: HashMap::new(),
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
        (mut solidifier_handles, mut requester_handles): Self::Data,
    ) -> ActorResult<()> {
        log::info!("{:?} is running", rt.service().directory());
        while let Some(event) = rt.inbox_mut().next().await {
            match event {
                CollectorEvent::MilestoneFromRequester(ms_index, message_and_metadata) => {
                    self.requester_usage -= 1;
                    if let Some(message_and_metadata) = message_and_metadata {
                        // convert it to msg_record
                        let msg_record: MessageRecord = message_and_metadata.into();
                        let message_id = *msg_record.message_id();
                        if let Some(Payload::Milestone(milestone_payload)) = msg_record.message().payload() {
                            // filter the message
                            let selected = self
                                .selective_builder
                                .filter_message(&self.uda_handle, &msg_record)
                                .await?;
                            let solidifier_id = self.solidifier_id(ms_index);
                            let milestone_message = msg_record.try_into()?;
                            solidifier_handles
                                .get(&solidifier_id)
                                .and_then(|h| h.send(SolidifierEvent::Milestone(milestone_message, selected)).ok());
                        } else {
                            error!(
                                "Unexpected milestone response from requester, received a milestone without payload"
                            );
                            let solidifier_id = self.solidifier_id(ms_index);
                            solidifier_handles
                                .get(&solidifier_id)
                                .and_then(|h| h.send(SolidifierEvent::Solidify(Err(ms_index))).ok());
                        }
                    } else {
                        let solidifier_id = self.solidifier_id(ms_index);
                        solidifier_handles
                            .get(&solidifier_id)
                            .and_then(|h| h.send(SolidifierEvent::Solidify(Err(ms_index))).ok());
                    }
                }
                CollectorEvent::MsgFromRequester(message_id, message_and_metadata) => {
                    self.requester_usage -= 1;
                    self.request_a_pending_request(&mut requester_handles);
                    if !self.requested_requests.contains_key(&message_id) {
                        continue;
                    }
                    // check if the message still in requested hashmap, otherwise we skip it
                    if let Some((message, metadata)) = message_and_metadata {
                        let msg_record = (message, metadata.clone()).into();
                        // filter the message
                        let selected = self
                            .selective_builder
                            .filter_message(&self.uda_handle, &msg_record)
                            .await?;
                        // push message or milestone to solidifier
                        self.push_message_record_to_solidifier(msg_record.clone(), &solidifier_handles, selected);
                        // process requested requests for this message_id
                        self.close_successful_requested_request(
                            &solidifier_handles,
                            &message_id,
                            &msg_record,
                            selected,
                        );
                        // insert into the lru_msg cache
                        if let Some((msg_opt, metadata_opt, timestamp, selected_opt)) =
                            self.lru_msg.get_mut(&message_id)
                        {
                            msg_opt.replace(msg_record);
                            metadata_opt.replace(metadata);
                            *selected_opt = selected;
                        } else {
                            self.lru_msg.put(
                                message_id,
                                (Some(msg_record), Some(metadata), std::time::SystemTime::now(), selected),
                            );
                        };
                    } else {
                        warn!(
                            "{:?}, unable to fetch message: {:?}",
                            rt.service().directory(),
                            message_id
                        );
                        self.process_failed_requested_request(&message_id, &mut solidifier_handles);
                    }
                }
                CollectorEvent::Message(message_id, message) => {
                    // check if msg already in lru cache(if so then it's already presisted)
                    let entry = self.lru_msg.get_mut(&message_id);
                    match entry {
                        Some((msg_opt, metadata_opt, timestamp, selected)) => {
                            if msg_opt.is_some() {
                                // duplicated processed msg, no need to process it.
                                continue;
                            } else {
                                let metadata = metadata_opt
                                    .as_mut()
                                    .expect("Failed to get metadata entry from the cache");
                                let mut msg_record = MessageRecord::new(message_id, message);

                                msg_record.milestone_index = Some(MilestoneIndex(
                                    metadata.milestone_index.unwrap_or(
                                        metadata
                                            .referenced_by_milestone_index
                                            .expect("Unable to find referenced_by_milestone_index"),
                                    ),
                                ));
                                msg_record.inclusion_state =
                                    metadata.ledger_inclusion_state.clone().and_then(|l| Some(l.into()));
                                msg_record.conflict_reason =
                                    if let Some(conflict_reason_u8) = metadata.conflict_reason.as_ref() {
                                        Some(
                                            (*conflict_reason_u8)
                                                .try_into()
                                                .map_err(|error| ActorError::exit(error))?,
                                        )
                                    } else {
                                        None
                                    };
                                // message already has confirmed metadata
                                msg_opt.replace(msg_record.clone());
                                // filter the message
                                *selected = self
                                    .selective_builder
                                    .filter_message(&self.uda_handle, &msg_record)
                                    .await?;
                                let selected = selected.clone();
                                drop(entry);
                                // push it to solidifier
                                self.push_message_record_to_solidifier(msg_record, &solidifier_handles, selected);
                            }
                        }
                        None => {
                            let msg_record = MessageRecord::new(message_id, message);
                            // filter the message
                            let selected = self
                                .selective_builder
                                .filter_message(&self.uda_handle, &msg_record)
                                .await?;
                            // add it to the cache in order to filter it again.
                            self.lru_msg
                                .put(message_id, (Some(msg_record), None, std::time::SystemTime::now(), None));
                        }
                    }
                }
                CollectorEvent::MessageReferenced(metadata) => {
                    if metadata.referenced_by_milestone_index.as_ref().is_none()
                        || metadata.ledger_inclusion_state.as_ref().is_none()
                    {
                        // metadata is not referenced yet, so we discard it.
                        continue;
                    }
                    // check if the msg and metadata already in lru cache
                    let message_id = MessageId::from_str(&metadata.message_id).unwrap();
                    // check if msg already in lru cache
                    let entry = self.lru_msg.get_mut(&message_id);
                    match entry {
                        Some((Some(msg), metadata_opt, timestamp, selected)) => {
                            // check if it's already presisted
                            if metadata_opt.is_some() {
                                // already processed
                                continue;
                            } else {
                                let ref_ms = *metadata.milestone_index.as_ref().unwrap_or(
                                    metadata
                                        .referenced_by_milestone_index
                                        .as_ref()
                                        .expect("Unable to find referenced_by_milestone_index"),
                                );
                                let partition_id = (ref_ms % (self.partition_count as u32)) as u8;
                                msg.milestone_index.replace(ref_ms.into());
                                msg.inclusion_state = metadata.ledger_inclusion_state.and_then(|l| Some(l.into()));
                                msg.conflict_reason =
                                    if let Some(conflict_reason_u8) = metadata.conflict_reason.as_ref() {
                                        Some(
                                            (*conflict_reason_u8)
                                                .try_into()
                                                .map_err(|error| ActorError::exit(error))?,
                                        )
                                    } else {
                                        None
                                    };
                                timestamp
                                    .elapsed()
                                    .map(|elapsed| CONFIRMATION_TIME_COLLECTOR.set(elapsed.as_millis() as f64));
                                // filter the message
                                *selected = self.selective_builder.filter_message(&self.uda_handle, &msg).await?;
                                let selected = selected.clone();
                                let msg = msg.clone();
                                drop(entry);
                                // push message to solidifier
                                self.push_message_record_to_solidifier(msg, &solidifier_handles, selected);
                                // cleanup any pending or requested requests
                                self.close_requests(&message_id, ref_ms, &solidifier_handles);
                            }
                        }
                        _none => {
                            self.lru_msg
                                .put(message_id, (None, Some(metadata), std::time::SystemTime::now(), None));
                        }
                    }
                }
                CollectorEvent::Ask(ask) => {
                    match ask {
                        AskCollector::FullMessage(solidifier_id, try_ms_index, message_id, created_by) => {
                            // check if full message already exist\
                            let entry = self.lru_msg.get(&message_id);
                            match entry {
                                None => {
                                    if let Some(requested) = self.requested_requests.get_mut(&message_id) {
                                        requested.insert(try_ms_index);
                                    } else {
                                        if self.requester_usage < self.requester_budget {
                                            self.request_full_message(&mut requester_handles, message_id);
                                            // it might already has pending requests
                                            let mut value =
                                                self.pending_requests.remove(&message_id).unwrap_or_default();
                                            value.insert(try_ms_index);
                                            self.requested_requests.insert(message_id, value);
                                        } else {
                                            // insert it into pending to be requested eventaully
                                            self.pending_requests
                                                .entry(message_id)
                                                .or_insert_with(|| (Default::default()))
                                                .insert(try_ms_index);
                                        }
                                    }
                                }
                                Some((msg_opt, metadata_opt, _, selected_opt)) => {
                                    if msg_opt.is_none() || metadata_opt.is_none() {
                                        if let Some(requested) = self.requested_requests.get_mut(&message_id) {
                                            requested.insert(try_ms_index);
                                        } else {
                                            if self.requester_usage < self.requester_budget {
                                                self.request_full_message(&mut requester_handles, message_id);
                                                // it might already has pending requests
                                                let mut value =
                                                    self.pending_requests.remove(&message_id).unwrap_or_default();
                                                value.insert(try_ms_index);
                                                self.requested_requests.insert(message_id, value);
                                            } else {
                                                // insert it into pending to be requested eventaully
                                                self.pending_requests
                                                    .entry(message_id)
                                                    .or_insert_with(|| (Default::default()))
                                                    .insert(try_ms_index);
                                            }
                                        }
                                    } else {
                                        // check if ms is try_ms_index
                                        if let Some(ms) = metadata_opt.as_ref().unwrap().referenced_by_milestone_index {
                                            if ms == try_ms_index {
                                                let selected_opt = selected_opt.clone();
                                                let msg = msg_opt.clone().unwrap();
                                                drop(entry);
                                                // todo, either backpressure solidifier requests using TTL (preferred),
                                                // or wrap MilestoneMessage and Message using arc where we check
                                                // strong_count before pushing message/milestone.
                                                self.push_message_record_to_solidifier(
                                                    msg,
                                                    &solidifier_handles,
                                                    selected_opt,
                                                );
                                            } else {
                                                self.push_close_to_solidifier(
                                                    self.solidifier_id(try_ms_index),
                                                    message_id,
                                                    try_ms_index,
                                                    &solidifier_handles,
                                                );
                                            }
                                        } else {
                                            self.push_close_to_solidifier(
                                                self.solidifier_id(try_ms_index),
                                                message_id,
                                                try_ms_index,
                                                &solidifier_handles,
                                            );
                                        }
                                    }
                                }
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
    /// Return the solidifier id, derived from milestone index
    fn solidifier_id(&self, milestone_index: u32) -> u8 {
        (milestone_index % (self.partition_count as u32)) as u8
    }
    /// Send an error event to the solidifier for a given milestone index
    fn process_failed_requested_request(
        &mut self,
        message_id: &MessageId,
        solidifier_handles: &HashMap<u8, SolidifierHandle>,
    ) {
        self.requested_requests.remove(message_id).map(|i| {
            let mut iter = i.into_iter();
            while let Some(milestone_index) = iter.next() {
                let solidifier_id = self.solidifier_id(milestone_index);
                solidifier_handles
                    .get(&solidifier_id)
                    .and_then(|h| h.send(SolidifierEvent::Solidify(Err(milestone_index))).ok());
            }
        });
    }
    /// Close any pending and requested requests for a given message id
    fn close_requests(
        &mut self,
        message_id: &MessageId,
        ref_ms: u32,
        solidifier_handles: &HashMap<u8, SolidifierHandle>,
    ) {
        self.close_requested_requests(message_id, ref_ms, solidifier_handles);
        self.close_pending_requests(message_id, ref_ms, solidifier_handles);
    }

    /// Close requested requests for a given message id
    fn close_requested_requests(
        &mut self,
        message_id: &MessageId,
        ref_ms: u32,
        solidifier_handles: &HashMap<u8, SolidifierHandle>,
    ) {
        self.requested_requests.remove(message_id).map(|i| {
            let mut iter = i.into_iter();
            while let Some(milestone_index) = iter.next() {
                if ref_ms != milestone_index {
                    let solidifier_id = self.solidifier_id(milestone_index);
                    solidifier_handles
                        .get(&solidifier_id)
                        .and_then(|h| h.send(SolidifierEvent::Close(*message_id, milestone_index)).ok());
                }
            }
        });
    }
    /// close any pending requests for a given message id
    fn close_pending_requests(
        &mut self,
        message_id: &MessageId,
        ref_ms: u32,
        solidifier_handles: &HashMap<u8, SolidifierHandle>,
    ) {
        self.pending_requests.remove(message_id).map(|i| {
            let mut iter = i.into_iter();
            while let Some(milestone_index) = iter.next() {
                if ref_ms != milestone_index {
                    let solidifier_id = self.solidifier_id(milestone_index);
                    solidifier_handles
                        .get(&solidifier_id)
                        .and_then(|h| h.send(SolidifierEvent::Close(*message_id, milestone_index)).ok());
                }
            }
        });
    }

    /// Process the oldest pending request
    fn request_a_pending_request(&mut self, requester_handles: &mut RequesterHandles<T>) {
        if let Some((message_id, milestone_indexes)) = self.pending_requests.pop_front() {
            self.request_full_message(requester_handles, message_id);
            self.requested_requests.insert(message_id, milestone_indexes);
        };
    }
    /// Close requested request for a given message id
    fn close_successful_requested_request(
        &mut self,
        solidifier_handles: &HashMap<u8, SolidifierHandle>,
        message_id: &MessageId,
        message: &MessageRecord,
        selected: Option<Selected>,
    ) {
        self.requested_requests.remove(message_id).map(|i| {
            let ref_ms = message
                .milestone_index()
                .expect("Unable to find referenced milestone index");
            let mut iter = i.into_iter();
            while let Some(milestone_index) = iter.next() {
                if ref_ms.0 != milestone_index {
                    let solidifier_id = self.solidifier_id(milestone_index);
                    solidifier_handles
                        .get(&solidifier_id)
                        .and_then(|h| h.send(SolidifierEvent::Close(*message_id, milestone_index)).ok());
                }
            }
        });
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
        &mut self,
        requester_handles: &mut RequesterHandles<T>,
        milestone_index: u32,
    ) -> Option<()> {
        self.requester_usage += 1;
        requester_handles.send(RequesterEvent::RequestMilestone(self.partition_id, milestone_index))
    }
    /// Request the full message (i.e., including both message and metadata) of a given message id and
    /// a milestone index
    fn request_full_message(
        &mut self,
        requester_handles: &mut RequesterHandles<T>,
        message_id: MessageId,
    ) -> Option<()> {
        self.requester_usage += 1;
        requester_handles.send(RequesterEvent::RequestFullMessage(self.partition_id, message_id))
    }
    /// Push the message record to solidifier
    fn push_message_record_to_solidifier(
        &self,
        msg_record: MessageRecord,
        solidifier_handles: &HashMap<u8, SolidifierHandle>,
        selected: Option<Selected>,
    ) {
        let ref_ms = *msg_record
            .milestone_index()
            .expect("Failed to get milestone index within a message record");
        let solidifier_id = self.solidifier_id(ref_ms.0);
        // check if the message is milestone
        if let Some(Payload::Milestone(_)) = msg_record.message().payload() {
            // send it as milestone only if the ledger inclusion state not conflicting
            if let LedgerInclusionState::Conflicting = msg_record
                .inclusion_state()
                .expect("Failed to get inclusion state within a message record")
            {
                // send it as regular message, as it's invalid milestone
                solidifier_handles
                    .get(&solidifier_id)
                    .and_then(|h| h.send(SolidifierEvent::Message(msg_record, selected.clone())).ok());
            } else {
                let milestone_message = msg_record.try_into().expect(
                    "Failed to create milestone message from message record, in order to push it to solidifier",
                );
                solidifier_handles.get(&solidifier_id).and_then(|h| {
                    h.send(SolidifierEvent::Milestone(milestone_message, selected.clone()))
                        .ok()
                });
            };
        } else {
            // send it as regular message
            solidifier_handles
                .get(&solidifier_id)
                .and_then(|h| h.send(SolidifierEvent::Message(msg_record, selected.clone())).ok());
        }
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
}
