// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use bee_message::input::Input;
use permanode_common::config::PartitionConfig;
use std::sync::Arc;

use super::*;
#[async_trait::async_trait]
impl<H: PermanodeBrokerScope> EventLoop<BrokerHandle<H>> for Collector {
    async fn event_loop(
        &mut self,
        _status: Result<(), Need>,
        _supervisor: &mut Option<BrokerHandle<H>>,
    ) -> Result<(), Need> {
        while let Some(event) = self.inbox.recv().await {
            match event {
                CollectorEvent::MessageAndMeta(requester_id, try_ms_index, message_id, opt_full_msg) => {
                    self.adjust_heap(requester_id);
                    if let Some(FullMessage(message, metadata)) = opt_full_msg {
                        let message_id = message_id.expect("Expected message_id in requester response");
                        let partition_id = (try_ms_index % (self.collectors_count as u32)) as u8;
                        let ref_ms = metadata.referenced_by_milestone_index.as_ref().unwrap();
                        // set the ref_ms to be the current requested message ref_ms
                        self.ref_ms.0 = *ref_ms;
                        // check if the requested message actually belongs to the expected milestone_index
                        if ref_ms.eq(&try_ms_index) {
                            // push full message to solidifier;
                            self.push_fullmsg_to_solidifier(partition_id, message.clone(), metadata.clone());
                        } else {
                            // close the request
                            self.push_close_to_solidifier(partition_id, message_id, try_ms_index);
                        }
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
                            self.clean_up_wrong_est_msg(&message_id, &message, wrong_est_ms)
                                .unwrap_or_else(|e| {
                                    error!("{}", e);
                                });
                        }
                        self.insert_message_with_metadata(message_id, message, metadata)
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
                        let solidifier_id = (try_ms_index % (self.collectors_count as u32)) as u8;
                        let solidifier_handle = self.solidifier_handles.get(&solidifier_id).unwrap();
                        let _ = solidifier_handle.send(SolidifierEvent::Solidify(Err(try_ms_index)));
                    }
                }
                CollectorEvent::Message(message_id, mut message) => {
                    // check if msg already in lru cache(if so then it's already presisted)
                    if let None = self.lru_msg.get(&message_id) {
                        // store message
                        self.insert_message(&message_id, &mut message).unwrap_or_else(|e| {
                            error!("{}", e);
                        });
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
                    let _partition_id = (ref_ms % (self.collectors_count as u32)) as u8;
                    let message_id = metadata.message_id;
                    // set the ref_ms to be the most recent ref_ms
                    self.ref_ms.0 = *ref_ms;
                    // update the est_ms to be the most recent ref_ms+1
                    let new_ms = self.ref_ms.0 + 1;
                    // TODO likely we should remove this check,as the MessageReferenced supposed to be used for fresh
                    // data
                    if self.est_ms.0 < new_ms {
                        self.est_ms.0 = new_ms;
                    }
                    // check if msg already in lru cache(if so then it's already presisted)
                    if let None = self.lru_msg_ref.get(&message_id) {
                        // add it to the cache in order to not presist it again.
                        self.lru_msg_ref.put(message_id, metadata.clone());
                        // check if msg already exist in the cache, if so we push it to solidifier
                        let cached_msg;
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
                            // push to solidifier
                            if let Some(solidifier_handle) = self.solidifier_handles.get(&_partition_id) {
                                let full_message = FullMessage::new(message.clone(), metadata.clone());
                                let full_msg_event = SolidifierEvent::Message(full_message);
                                let _ = solidifier_handle.send(full_msg_event);
                            };
                        } else {
                            cached_msg = None;
                            wrong_msg_est_ms = None;
                        }
                        if let Some(message) = cached_msg {
                            if let Some(wrong_est_ms) = wrong_msg_est_ms {
                                self.clean_up_wrong_est_msg(&message_id, &message, wrong_est_ms)
                                    .unwrap_or_else(|e| {
                                        error!("{}", e);
                                    });
                            }
                            self.insert_message_with_metadata(message_id, message, metadata)
                                .unwrap_or_else(|e| {
                                    error!("{}", e);
                                });
                        } else {
                            // store it as metadata
                            self.insert_message_metadata(metadata.clone()).unwrap_or_else(|e| {
                                error!("{}", e);
                            });
                        }
                    }
                }
                CollectorEvent::Ask(ask) => {
                    match ask {
                        AskCollector::FullMessage(solidifier_id, try_ms_index, message_id) => {
                            if let Some((_, message)) = self.lru_msg.get(&message_id) {
                                if let Some(metadata) = self.lru_msg_ref.get(&message_id) {
                                    // metadata exist means we already pushed the full message to the solidifier,
                                    // or the message doesn't belong to the solidifier
                                    if !metadata.referenced_by_milestone_index.unwrap().eq(&try_ms_index) {
                                        if let Some(solidifier_handle) = self.solidifier_handles.get(&solidifier_id) {
                                            let event = SolidifierEvent::Close(message_id, try_ms_index);
                                            let _ = solidifier_handle.send(event);
                                        }
                                    } else {
                                        if let Some(solidifier_handle) = self.solidifier_handles.get(&solidifier_id) {
                                            let full_message = FullMessage::new(message.clone(), metadata.clone());
                                            let full_msg_event = SolidifierEvent::Message(full_message);
                                            let _ = solidifier_handle.send(full_msg_event);
                                        }
                                    }
                                } else {
                                    self.request_full_message(message_id, try_ms_index);
                                }
                            } else {
                                self.request_full_message(message_id, try_ms_index);
                            }
                        }
                        AskCollector::MilestoneMessage(milestone_index) => {
                            // Request it from network
                            self.request_milestone_message(milestone_index);
                        }
                    }
                }
                CollectorEvent::Shutdown => {
                    // To shutdown the collector we simply drop the handle
                    self.handle.take();
                    // drop heap
                    self.requester_handles.drain().for_each(|h| {
                        h.shutdown();
                    });
                }
            }
        }
        Ok(())
    }
}

impl Collector {
    fn clone_solidifier_handle(&self, milestone_index: u32) -> SolidifierHandle {
        let solidifier_id = (milestone_index % (self.collectors_count as u32)) as u8;
        self.solidifier_handles.get(&solidifier_id).unwrap().clone()
    }
    fn request_milestone_message(&mut self, milestone_index: u32) {
        let remote_url = self.api_endpoints.pop_back().unwrap();
        self.api_endpoints.push_front(remote_url.clone());
        if let Some(mut requester_handle) = self.requester_handles.peek_mut() {
            requester_handle.send_event(RequesterEvent::RequestMilestone(milestone_index))
        }; // else collector is shutting down
    }
    fn request_full_message(&mut self, message_id: MessageId, try_ms_index: u32) {
        let remote_url = self.api_endpoints.pop_back().unwrap();
        self.api_endpoints.push_front(remote_url.clone());
        if let Some(mut requester_handle) = self.requester_handles.peek_mut() {
            requester_handle.send_event(RequesterEvent::RequestFullMessage(message_id, try_ms_index))
        }; // else collector is shutting down
    }
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
                let index_key = Indexation(String::from_utf8_lossy(indexation.index()).into_owned());
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
    fn push_fullmsg_to_solidifier(&self, partition_id: u8, message: Message, metadata: MessageMetadata) {
        if let Some(solidifier_handle) = self.solidifier_handles.get(&partition_id) {
            let full_message = FullMessage::new(message, metadata);
            let full_msg_event = SolidifierEvent::Message(full_message);
            let _ = solidifier_handle.send(full_msg_event);
        };
    }
    fn push_close_to_solidifier(&self, partition_id: u8, message_id: MessageId, try_ms_index: u32) {
        if let Some(solidifier_handle) = self.solidifier_handles.get(&partition_id) {
            let full_msg_event = SolidifierEvent::Close(message_id, try_ms_index);
            let _ = solidifier_handle.send(full_msg_event);
        };
    }
    #[cfg(feature = "filter")]
    fn get_keyspace_for_message(&self, message: &mut Message) -> PermanodeKeyspace {
        let res = futures::executor::block_on(permanode_filter::filter_messages(message));
        PermanodeKeyspace::new(res.keyspace.into_owned())
    }
    fn get_keyspace(&self) -> PermanodeKeyspace {
        self.default_keyspace.clone()
    }

    fn get_partition_id(&self, milestone_index: MilestoneIndex) -> u16 {
        self.storage_config
            .as_ref()
            .map(|config| &config.partition_config)
            .unwrap_or(&PartitionConfig::default())
            .partition_id(milestone_index.0)
    }

    fn insert_message(&mut self, message_id: &MessageId, message: &mut Message) -> anyhow::Result<()> {
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
            self.est_ms = MilestoneIndex(*meta.referenced_by_milestone_index.as_ref().unwrap());
            let milestone_index = *self.est_ms;
            let solidifier_id = (milestone_index % (self.collectors_count as u32)) as u8;
            let solidifier_handle = self.solidifier_handles.get(&solidifier_id).unwrap().clone();
            let inherent_worker = AtomicWorker::new(solidifier_handle, milestone_index, *message_id, self.retries);
            let message_tuple = (message.clone(), meta.clone());
            // store message and metadata
            self.insert(&inherent_worker, &keyspace, *message_id, message_tuple)?;
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
        } else {
            metadata = None;
            ledger_inclusion_state = None;
            let inherent_worker = SimpleWorker;
            // store message only
            self.insert(&inherent_worker, &keyspace, *message_id, message.clone())
                .unwrap_or_else(|e| {
                    error!("{}", e);
                });
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
            self.insert(inherent_worker, &self.get_keyspace(), partitioned, parent_record)
                .unwrap_or_else(|e| {
                    error!("{}", e);
                });
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
                    Indexation(String::from_utf8_lossy(indexation.index()).into_owned()),
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
                    let solidifier_id = (ms_index.0 % (self.collectors_count as u32)) as u8;
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
                error!("impl insert for remaining payloads, {:?}", e)
            }
        }
        Ok(())
    }
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
    fn insert_message_metadata(&self, metadata: MessageMetadata) -> anyhow::Result<()> {
        let message_id = metadata.message_id;
        let solidifier_handle = self.clone_solidifier_handle(*self.ref_ms);
        let inherent_worker = AtomicWorker::new(solidifier_handle, *self.ref_ms, message_id, self.retries);
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
    #[allow(unused_mut)]
    fn insert_message_with_metadata(
        &mut self,
        message_id: MessageId,
        mut message: Message,
        metadata: MessageMetadata,
    ) -> anyhow::Result<()> {
        #[cfg(feature = "filter")]
        let keyspace = self.get_keyspace_for_message(&mut message);
        #[cfg(not(feature = "filter"))]
        let keyspace = self.get_keyspace();
        let solidifier_handle = self.clone_solidifier_handle(*self.ref_ms);
        let inherent_worker = AtomicWorker::new(solidifier_handle, *self.ref_ms, message_id, self.retries);
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
        if let Essence::Regular(regular) = transaction.essence() {
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
                if let Address::Ed25519(ed_address) = sls.address() {
                    let partitioned = Partitioned::new(*ed_address, partition_id, milestone_index.0);
                    let address_record =
                        AddressRecord::new(output_type, *transaction_id, index, sls.amount(), inclusion_state);
                    self.insert(inherent_worker, &self.get_keyspace(), partitioned, address_record)?;
                    // insert hint record
                    let hint = Hint::address(ed_address.to_string());
                    let partition = Partition::new(partition_id, *milestone_index);
                    self.insert(inherent_worker, &self.get_keyspace(), hint, partition)
                } else {
                    bail!("Unexpected address variant");
                }
            }
            Output::SignatureLockedDustAllowance(slda) => {
                if let Address::Ed25519(ed_address) = slda.address() {
                    let partitioned = Partitioned::new(*ed_address, partition_id, milestone_index.0);
                    let address_record =
                        AddressRecord::new(output_type, *transaction_id, index, slda.amount(), inclusion_state);
                    self.insert(inherent_worker, &self.get_keyspace(), partitioned, address_record)?;
                    // insert hint record
                    let hint = Hint::address(ed_address.to_string());
                    let partition = Partition::new(partition_id, *milestone_index);
                    self.insert(inherent_worker, &self.get_keyspace(), hint, partition)
                } else {
                    bail!("Unexpected address variant");
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
    fn insert<I, S, K, V>(&self, inherent_worker: &I, keyspace: &S, key: K, value: V) -> anyhow::Result<()>
    where
        I: Inherent,
        S: 'static + Insert<K, V>,
        K: 'static + Send + Clone,
        V: 'static + Send + Clone,
    {
        let insert_req = keyspace.insert(&key, &value)?.consistency(Consistency::One).build()?;
        let worker = inherent_worker.inherent_boxed(keyspace.clone(), key, value);
        insert_req.send_local(worker);
        Ok(())
    }

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
    fn delete_transaction_partitioned_rows(
        &self,
        message_id: &MessageId,
        transaction: &Box<TransactionPayload>,
        milestone_index: MilestoneIndex,
    ) -> anyhow::Result<()> {
        let transaction_id = transaction.id();
        if let Essence::Regular(regular) = transaction.essence() {
            if let Some(Payload::Indexation(indexation)) = regular.payload() {
                let index_key = Indexation(String::from_utf8_lossy(indexation.index()).into_owned());
                self.delete_indexation(&message_id, index_key, milestone_index)?;
            }
            for (output_index, output) in regular.outputs().iter().enumerate() {
                self.delete_address(output, &transaction_id, output_index as u16, milestone_index)?;
            }
        }
        Ok(())
    }
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
                if let Address::Ed25519(ed_address) = sls.address() {
                    let address_pk = Ed25519AddressPK::new(
                        *ed_address,
                        partition_id,
                        milestone_index,
                        output_type,
                        *transaction_id,
                        index,
                    );
                    self.delete(address_pk)?;
                } else {
                    error!("Unexpected address variant");
                }
            }
            Output::SignatureLockedDustAllowance(slda) => {
                if let Address::Ed25519(ed_address) = slda.address() {
                    let address_pk = Ed25519AddressPK::new(
                        *ed_address,
                        partition_id,
                        milestone_index,
                        output_type,
                        *transaction_id,
                        index,
                    );
                    self.delete(address_pk)?;
                } else {
                    error!("Unexpected address variant");
                }
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
    fn delete<K, V>(&self, key: K) -> anyhow::Result<()>
    where
        PermanodeKeyspace: Delete<K, V>,
        K: 'static + Send + Clone,
        V: 'static + Send + Clone,
    {
        let delete_req = self
            .default_keyspace
            .delete(&key)?
            .consistency(Consistency::One)
            .build()?;
        let worker = DeleteWorker::boxed(self.default_keyspace.clone(), key);
        delete_req.send_local(worker);
        Ok(())
    }
}

pub struct AtomicWorker {
    arc_handle: Arc<AtomicSolidifierHandle>,
    retries: u16,
}

impl AtomicWorker {
    fn new(solidifier_handle: SolidifierHandle, milestone_index: u32, message_id: MessageId, retries: u16) -> Self {
        let any_error = std::sync::atomic::AtomicBool::new(false);
        let atomic_handle = AtomicSolidifierHandle::new(solidifier_handle, milestone_index, message_id, any_error);
        let arc_handle = std::sync::Arc::new(atomic_handle);
        Self { arc_handle, retries }
    }
}
pub struct SimpleWorker;

trait Inherent {
    fn inherent_boxed<S, K, V>(&self, keyspace: S, key: K, value: V) -> Box<dyn Worker>
    where
        S: 'static + Insert<K, V>,
        K: 'static + Send + Clone,
        V: 'static + Send + Clone;
}

impl Inherent for SimpleWorker {
    fn inherent_boxed<S, K, V>(&self, keyspace: S, key: K, value: V) -> Box<dyn Worker>
    where
        S: 'static + Insert<K, V>,
        K: 'static + Send + Clone,
        V: 'static + Send + Clone,
    {
        InsertWorker::boxed(keyspace, key, value)
    }
}

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
