// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

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
                #[allow(unused_mut)]
                CollectorEvent::Message(message_id, mut message) => {
                    // check if msg already in lru cache(if so then it's already presisted)
                    if let None = self.lru_msg.get(&message_id) {
                        {
                            // store message
                            self.insert_message(&message_id, &message);
                            let est_ms = MilestoneIndex(self.est_ms.0 + 1);
                            // add it to the cache in order to not presist it again.
                            self.lru_msg.put(message_id, (est_ms, message));
                        }
                    }
                }
                CollectorEvent::MessageReferenced(metadata) => {
                    let ref_ms = metadata.referenced_by_milestone_index.as_ref().unwrap();
                    let _partition_id = (ref_ms % (self.collectors_count as u32)) as u8;
                    let message_id = metadata.message_id;
                    // update the est_ms to be the most recent ref_ms
                    self.est_ms.0 = *ref_ms;
                    // check if msg already in lru cache(if so then it's already presisted)
                    if let None = self.lru_msg_ref.get(&message_id) {
                        // check if msg already exist in the cache, if so we push it to solidifier
                        let cached_msg;
                        if let Some((est_ms, message)) = self.lru_msg.get_mut(&message_id) {
                            // check if est_ms is not identical to ref_ms
                            if &est_ms.0 != ref_ms {
                                todo!("delete duplicated rows")
                            }
                            cached_msg = Some(message.clone());
                            // TODO push to solidifer
                        } else {
                            cached_msg = None;
                        }
                        if let Some(message) = cached_msg {
                            self.insert_message_with_metadata(&message_id.clone(), &message.clone(), &metadata);
                        } else {
                            // store it as metadata
                            self.insert_message_metadata(metadata.clone());
                        }
                        // add it to the cache in order to not presist it again.
                        self.lru_msg_ref.put(message_id, metadata);
                    }
                }
            }
        }
        Ok(())
    }
}

impl Collector {
    fn insert_message(&mut self, message_id: &MessageId, message: &Message) {
        // Check if metadata already exist in the cache
        let ledger_inclusion_state;
        if let Some(meta) = self.lru_msg_ref.get(message_id) {
            ledger_inclusion_state = meta.ledger_inclusion_state.clone();
            self.est_ms = MilestoneIndex(*meta.referenced_by_milestone_index.as_ref().unwrap());
            let message_tuple = (message.clone(), meta.clone());
            // store message and metadata
            self.insert(*message_id, message_tuple);
        } else {
            ledger_inclusion_state = None;
            self.est_ms.0 += 1;
            // store message only
            self.insert(*message_id, message.clone());
        };
        // Insert parents/children
        self.insert_parents(
            &message_id,
            &message.parents(),
            self.est_ms,
            ledger_inclusion_state.clone(),
        );
        // insert payload (if any)
        self.insert_payload(&message_id, &message, self.est_ms, ledger_inclusion_state);
    }
    fn insert_parents(
        &self,
        message_id: &MessageId,
        parents: &[MessageId],
        milestone_index: MilestoneIndex,
        inclusion_state: Option<LedgerInclusionState>,
    ) {
        let partition_id = self.partitioner.partition_id(milestone_index.0);
        for parent_id in parents {
            let partitioned = Partitioned::new(*parent_id, partition_id);
            let parent_record = ParentRecord::new(milestone_index, *message_id, inclusion_state);
            self.insert(partitioned, parent_record);
        }
    }
    fn insert_payload(
        &self,
        message_id: &MessageId,
        message: &Message,
        milestone_index: MilestoneIndex,
        inclusion_state: Option<LedgerInclusionState>,
    ) {
        if let Some(payload) = &message.payload() {
            match payload {
                Payload::Indexation(indexation) => {
                    self.insert_hashed_index(message_id, indexation.hash(), milestone_index, inclusion_state);
                }
                Payload::Transaction(transaction) => self.insert_transaction(message_id, transaction),
                // remaining payload types
                _ => {}
            }
        }
    }
    fn insert_hashed_index(
        &self,
        message_id: &MessageId,
        hashed_index: HashedIndex,
        milestone_index: MilestoneIndex,
        inclusion_state: Option<LedgerInclusionState>,
    ) {
        let partition_id = self.partitioner.partition_id(milestone_index.0);
        let partitioned = Partitioned::new(hashed_index, partition_id);
        let hashed_index_record = HashedIndexRecord::new(milestone_index, *message_id, inclusion_state);
        self.insert(partitioned, hashed_index_record);
    }
    fn insert_message_metadata(&mut self, metadata: MessageMetadataObj) {
        let message_id = metadata.message_id;
        // store message and metadata
        self.insert(message_id, metadata.clone());
        // Insert parents/children
        let parents = metadata.parent_message_ids;
        self.insert_parents(
            &message_id,
            &parents.as_slice(),
            self.est_ms,
            metadata.ledger_inclusion_state.clone(),
        );
    }
    fn insert_message_with_metadata(
        &mut self,
        message_id: &MessageId,
        message: &Message,
        metadata: &MessageMetadataObj,
    ) {
        let message_tuple = (message.clone(), metadata.clone());
        // store message and metadata
        self.insert(*message_id, message_tuple);
        // Insert parents/children
        self.insert_parents(
            &message_id,
            &message.parents(),
            self.est_ms,
            metadata.ledger_inclusion_state.clone(),
        );
        // insert payload (if any)
        self.insert_payload(
            &message_id,
            &message,
            self.est_ms,
            metadata.ledger_inclusion_state.clone(),
        );
    }
    fn insert_transaction(&self, message_id: &MessageId, transaction: &Box<TransactionPayload>) {
        let transaction_id = transaction.id();
        let unlock_blocks = transaction.unlock_blocks();
        if let Essence::Regular(regular) = transaction.essence() {
            for (index, input) in regular.inputs().iter().enumerate() {
                match input {
                    bee_message::input::Input::UTXO(utxo_input) => {}
                    bee_message::input::Input::Treasury(treasury_input) => {}
                }
                let _b = &unlock_blocks[index];
            }
            for (index, output) in regular.outputs().iter().enumerate() {}
            let payload = regular.payload();
        };
    }

    fn insert<K, V>(&self, key: K, value: V)
    where
        PermanodeKeyspace: Insert<K, V>,
        K: 'static + Send + Clone,
        V: 'static + Send + Clone,
    {
        let insert_req = self
            .default_keyspace
            .insert(&key, &value)
            .consistency(Consistency::One)
            .build();
        let worker = InsertWorker::boxed(self.default_keyspace.clone(), key, value);
        insert_req.send_local(worker);
    }
}
