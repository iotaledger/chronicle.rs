// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;
use bee_message::{
    address::Address,
    input::Input,
    parents::Parents,
    payload::Payload,
    prelude::TransactionId,
};
use chronicle_common::metrics::CONFIRMATION_TIME_COLLECTOR;
use chronicle_filter::{
    filter_messages,
    FilterResponse,
};
use std::sync::Arc;

#[async_trait::async_trait]
impl<H: ChronicleBrokerScope> EventLoop<BrokerHandle<H>> for Collector {
    async fn event_loop(
        &mut self,
        _status: Result<(), Need>,
        supervisor: &mut Option<BrokerHandle<H>>,
    ) -> Result<(), Need> {
        self.service.update_status(ServiceStatus::Running);
        let event = BrokerEvent::Children(BrokerChild::Collector(self.service.clone()));
        let _ = supervisor
            .as_mut()
            .expect("Collector expected BrokerHandle")
            .send(event);
        while let Some(event) = self.inbox.recv().await {
            match event {
                CollectorEvent::MessageAndMeta(requester_id, try_ms_index, message_id, opt_full_msg) => {
                    self.adjust_heap(requester_id);
                    if let Some(FullMessage(mut message, metadata)) = opt_full_msg {
                        let filter = self.filter(&mut message);
                        let message_id = message_id.expect("Expected message_id in requester response");
                        let partition_id = (try_ms_index % (self.collector_count as u32)) as u8;
                        let ref_ms = metadata
                            .referenced_by_milestone_index
                            .as_ref()
                            .expect("Expected referenced_by_milestone_index");
                        // check if the requested message actually belongs to the expected milestone_index
                        if ref_ms.eq(&try_ms_index) {
                            // push full message to solidifier;
                            self.push_fullmsg_to_solidifier(partition_id, message.clone(), metadata.clone());
                            // proceed to insert the message and put it in the cache.
                        } else {
                            // close the request
                            self.push_close_to_solidifier(partition_id, message_id, try_ms_index);
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
                            self.clean_up_wrong_est_msg(&message_id, &message, wrong_est_ms)
                                .unwrap_or_else(|e| {
                                    error!("{}", e);
                                });
                        }
                        self.insert_message_with_metadata(message_id, message, metadata, filter)
                            .unwrap_or_else(|e| {
                                error!("{}", e);
                            });
                    } else {
                        error!(
                            "{} , unable to fetch message: {:?}, from network triggered by milestone_index: {}",
                            self.get_name(),
                            message_id,
                            try_ms_index
                        );
                        // inform solidifier
                        self.send_err_solidifiy(try_ms_index);
                    }
                }
                CollectorEvent::Message(message_id, mut message) => {
                    // check if msg already in lru cache(if so then it's already presisted)
                    if let None = self.lru_msg.get(&message_id) {
                        let filter = self.filter(&mut message);
                        // store message
                        self.insert_message(&message_id, &mut message, filter)
                            .unwrap_or_else(|e| {
                                error!("{}", e);
                            });
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
                    let ref_ms = metadata.referenced_by_milestone_index.as_ref().unwrap();
                    let _partition_id = (ref_ms % (self.collector_count as u32)) as u8;
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
                            if let Some(solidifier_handle) = self.solidifier_handles.get(&_partition_id) {
                                let full_message = FullMessage::new(message.clone(), metadata.clone());
                                let full_msg_event = SolidifierEvent::Message(full_message);
                                let _ = solidifier_handle.send(full_msg_event);
                            };
                            // however the message_id might had been requested,
                            if let Some((requested_by_this_ms, _)) = self.pending_requests.remove(&message_id) {
                                // check if we have to close it
                                if !requested_by_this_ms.eq(&*ref_ms) {
                                    // close it
                                    let solidifier_id = (requested_by_this_ms % (self.collector_count as u32)) as u8;
                                    self.push_close_to_solidifier(solidifier_id, message_id, requested_by_this_ms);
                                }
                            }
                            // request all pending_requests with less than the received milestone index
                            self.process_pending_requests(*ref_ms);
                        } else {
                            // check if it's in the pending_requests
                            if let Some((requested_by_this_ms, message)) = self.pending_requests.remove(&message_id) {
                                // check if we have to close or push full message
                                if requested_by_this_ms.eq(&*ref_ms) {
                                    // push full message
                                    self.push_fullmsg_to_solidifier(_partition_id, message.clone(), metadata.clone())
                                } else {
                                    // close it
                                    let solidifier_id = (requested_by_this_ms % (self.collector_count as u32)) as u8;
                                    self.push_close_to_solidifier(solidifier_id, message_id, requested_by_this_ms);
                                }
                                cached_msg = Some(message);
                            } else {
                                cached_msg = None;
                            }
                            wrong_msg_est_ms = None;
                            self.process_pending_requests(*ref_ms);
                        }
                        if let Some(mut message) = cached_msg {
                            let filter = self.filter(&mut message);
                            if let Some(wrong_est_ms) = wrong_msg_est_ms {
                                self.clean_up_wrong_est_msg(&message_id, &message, wrong_est_ms)
                                    .unwrap_or_else(|e| {
                                        error!("{}", e);
                                    });
                            }
                            self.insert_message_with_metadata(message_id, message, metadata, filter)
                                .unwrap_or_else(|e| {
                                    error!("{}", e);
                                });
                        } else {
                            // store it as metadata
                            self.insert_message_metadata(metadata).unwrap_or_else(|e| {
                                error!("{}", e);
                            });
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
                                        self.push_close_to_solidifier(solidifier_id, message_id, try_ms_index);
                                    } else {
                                        if let Some(solidifier_handle) = self.solidifier_handles.get(&solidifier_id) {
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
                                                    );
                                                } else {
                                                    // overwrite pre_ms_index by try_ms_index, which it will be
                                                    // eventually processed;
                                                    *pre_ms_index = try_ms_index;
                                                    // close pre_ms_index(old_ms) as it's greater than what we have atm
                                                    // (try_ms_index).
                                                    let solidifier_id = (old_ms % (self.collector_count as u32)) as u8;
                                                    self.push_close_to_solidifier(solidifier_id, message_id, old_ms);
                                                }
                                            } else {
                                                // add it to back_pressured requests
                                                self.pending_requests
                                                    .insert(message_id, (try_ms_index, message.clone()));
                                            };
                                        } else {
                                            self.request_full_message(message_id, try_ms_index);
                                        }
                                    } else {
                                        self.request_full_message(message_id, try_ms_index);
                                    }
                                }
                            } else {
                                self.request_full_message(message_id, try_ms_index);
                            }
                            // insert the message if requested by syncer to ensure it gets cql responses for all the
                            // requested messages
                            if let Some((mut message, metadata)) = message_tuple.take() {
                                let filter = self.filter(&mut message);
                                self.insert_message_with_metadata(message_id.clone(), message, metadata, filter)
                                    .unwrap_or_else(|e| {
                                        error!("{}", e);
                                    });
                            }
                        }
                        AskCollector::MilestoneMessage(milestone_index) => {
                            // Request it from network
                            self.request_milestone_message(milestone_index);
                        }
                    }
                }
                CollectorEvent::Internal(internal) => {
                    match internal {
                        Internal::Service(microservice) => {
                            self.service.update_microservice(microservice.get_name(), microservice);
                            if self.requester_count as usize == self.service.microservices.len() {
                                let event = BrokerEvent::Children(BrokerChild::Collector(self.service.clone()));
                                let _ = supervisor
                                    .as_mut()
                                    .expect("Collector expected BrokerHandle")
                                    .send(event);
                            }
                        }
                        Internal::Shutdown => {
                            // To shutdown the collector we simply drop the handle
                            self.handle.take();
                            // drop heap
                            self.requester_handles.drain().for_each(|h| {
                                h.shutdown();
                            });
                        }
                    }
                }
            }
        }
        Ok(())
    }
}

impl Collector {
    /// Send an error event to the solidifier for a given milestone index
    fn send_err_solidifiy(&self, try_ms_index: u32) {
        // inform solidifier
        let solidifier_id = (try_ms_index % (self.collector_count as u32)) as u8;
        let solidifier_handle = self.solidifier_handles.get(&solidifier_id).unwrap();
        let _ = solidifier_handle.send(SolidifierEvent::Solidify(Err(try_ms_index)));
    }
    /// Process the pending requests for a given milestone index
    fn process_pending_requests(&mut self, milestone_index: u32) {
        self.pending_requests = std::mem::take(&mut self.pending_requests)
            .into_iter()
            .filter_map(|(message_id, (ms, msg))| {
                if ms < milestone_index {
                    self.request_full_message(message_id, ms);
                    None
                } else {
                    Some((message_id, (ms, msg)))
                }
            })
            .collect();
    }
    /// Get the cloned solidifier handle
    fn clone_solidifier_handle(&self, milestone_index: u32) -> SolidifierHandle {
        let solidifier_id = (milestone_index % (self.collector_count as u32)) as u8;
        self.solidifier_handles.get(&solidifier_id).unwrap().clone()
    }
    /// Request the milestone message of a given milestone index
    fn request_milestone_message(&mut self, milestone_index: u32) {
        if let Some(mut requester_handle) = self.requester_handles.peek_mut() {
            requester_handle.send_event(RequesterEvent::RequestMilestone(milestone_index))
        }; // else collector is shutting down
    }
    /// Request the full message (i.e., including both message and metadata) of a given message id and
    /// a milestone index
    fn request_full_message(&mut self, message_id: MessageId, try_ms_index: u32) {
        if let Some(mut requester_handle) = self.requester_handles.peek_mut() {
            requester_handle.send_event(RequesterEvent::RequestFullMessage(message_id, try_ms_index))
        }; // else collector is shutting down
    }
    /// Adjust (refresh) the binary heap which stores the requester handels
    fn adjust_heap(&mut self, requester_id: RequesterId) {
        self.requester_handles = self
            .requester_handles
            .drain()
            .map(|mut requester_handle| {
                if requester_handle.id == requester_id {
                    requester_handle.decrement();
                    requester_handle
                } else {
                    requester_handle
                }
            })
            .collect();
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
    fn push_fullmsg_to_solidifier(&self, partition_id: u8, message: Message, metadata: MessageMetadata) {
        if let Some(solidifier_handle) = self.solidifier_handles.get(&partition_id) {
            let full_message = FullMessage::new(message, metadata);
            let full_msg_event = SolidifierEvent::Message(full_message);
            let _ = solidifier_handle.send(full_msg_event);
        };
    }
    /// Push a `Close` message_id (which doesn't belong to all solidifiers with a given milestone index) to the
    /// solidifier
    fn push_close_to_solidifier(&self, partition_id: u8, message_id: MessageId, try_ms_index: u32) {
        if let Some(solidifier_handle) = self.solidifier_handles.get(&partition_id) {
            let full_msg_event = SolidifierEvent::Close(message_id, try_ms_index);
            let _ = solidifier_handle.send(full_msg_event);
        };
    }

    /// Get the Chronicle keyspace
    fn get_keyspace(&self) -> ChronicleKeyspace {
        self.default_keyspace.clone()
    }
    fn filter(&self, message: &mut Message) -> Option<FilterResponse> {
        if std::cfg!(feature = "filter") {
            filter_messages(message)
        } else {
            Some(FilterResponse {
                keyspace: self.get_keyspace().name().clone(),
                ttl: None,
            })
        }
    }
    /// Get the partition id of a given milestone index
    fn get_partition_id(&self, milestone_index: MilestoneIndex) -> u16 {
        self.partition_config.partition_id(milestone_index.0)
    }
    /// Insert the message id and message to the table
    fn insert_message(
        &mut self,
        message_id: &MessageId,
        message: &mut Message,
        filter: Option<FilterResponse>,
    ) -> anyhow::Result<()> {
        // Check if metadata already exist in the cache
        let ledger_inclusion_state;

        let keyspace = filter
            .map(|f| {
                debug!("Using keyspace {}", f.keyspace);
                ChronicleKeyspace::new(f.keyspace.to_string())
            })
            .unwrap_or(self.get_keyspace());
        let metadata;
        if let Some(meta) = self.lru_msg_ref.get(message_id) {
            metadata = Some(meta.clone());
            ledger_inclusion_state = meta.ledger_inclusion_state.clone();
            let milestone_index = MilestoneIndex(*meta.referenced_by_milestone_index.as_ref().unwrap());
            let solidifier_id = (*milestone_index % (self.collector_count as u32)) as u8;
            let solidifier_handle = self.solidifier_handles.get(&solidifier_id).unwrap().clone();
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
                )?;
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
    fn insert_payload<I: Inherent>(
        &mut self,
        inherent_worker: &I,
        message_id: &MessageId,
        message: &Message,
        payload: &Payload,
        milestone_index: MilestoneIndex,
        inclusion_state: Option<LedgerInclusionState>,
        metadata: Option<MessageMetadata>,
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
            Payload::Transaction(transaction) => self.insert_transaction(
                inherent_worker,
                message_id,
                message,
                transaction,
                inclusion_state,
                milestone_index,
                metadata,
            )?,
            Payload::Milestone(milestone) => {
                let ms_index = milestone.essence().index();
                let parents_check = message.parents().eq(milestone.essence().parents());
                if metadata.is_some() && parents_check {
                    // push to the right solidifier
                    let solidifier_id = (ms_index.0 % (self.collector_count as u32)) as u8;
                    if let Some(solidifier_handle) = self.solidifier_handles.get(&solidifier_id) {
                        let ms_message =
                            MilestoneMessage::new(*message_id, milestone.clone(), message.clone(), metadata);
                        let _ = solidifier_handle.send(SolidifierEvent::Milestone(ms_message));
                    };
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
    #[allow(unused_mut)]
    fn insert_message_with_metadata(
        &mut self,
        message_id: MessageId,
        mut message: Message,
        metadata: MessageMetadata,
        filter: Option<FilterResponse>,
    ) -> anyhow::Result<()> {
        let keyspace = filter
            .map(|f| {
                debug!("Using keyspace {}", f.keyspace);
                ChronicleKeyspace::new(f.keyspace.to_string())
            })
            .unwrap_or(self.get_keyspace());
        let solidifier_handle = self.clone_solidifier_handle(*self.ref_ms);
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
            )?;
        }
        let message_tuple = (message, metadata);
        // store message and metadata
        self.insert(&inherent_worker, &keyspace, message_id, message_tuple)
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
        K: 'static + Send + Clone,
        V: 'static + Send + Clone,
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
        K: 'static + Send + Clone,
        V: 'static + Send + Clone,
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
    fn new(solidifier_handle: SolidifierHandle, milestone_index: u32, message_id: MessageId, retries: usize) -> Self {
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
        K: 'static + Send + Clone,
        V: 'static + Send + Clone;
}

/// Implement the `Inherent` trait for the simple worker
impl Inherent for SimpleWorker {
    fn inherent_boxed<S, K, V>(&self, keyspace: S, key: K, value: V) -> Box<dyn Worker>
    where
        S: 'static + Insert<K, V>,
        K: 'static + Send + Clone,
        V: 'static + Send + Clone,
    {
        InsertWorker::boxed(keyspace, key, value, self.retries)
    }
}

/// Implement the `Inherent` trait for the atomic solidifier worker
impl Inherent for AtomicWorker {
    fn inherent_boxed<S, K, V>(&self, keyspace: S, key: K, value: V) -> Box<dyn Worker>
    where
        S: 'static + Insert<K, V>,
        K: 'static + Send + Clone,
        V: 'static + Send + Clone,
    {
        AtomicSolidifierWorker::boxed(self.arc_handle.clone(), keyspace, key, value, self.retries)
    }
}
