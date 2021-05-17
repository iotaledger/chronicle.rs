// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use bee_message::{
    address::Address,
    input::Input,
    payload::Payload,
    prelude::{
        MilestoneIndex,
        TransactionId,
    },
};

use super::*;
#[async_trait::async_trait]
impl<H: ChronicleBrokerScope, T: ImportMode> EventLoop<BrokerHandle<H>> for Importer<T> {
    async fn event_loop(
        &mut self,
        mut status: Result<(), Need>,
        supervisor: &mut Option<BrokerHandle<H>>,
    ) -> Result<(), Need> {
        status?;
        info!("{} is running", self.get_name());
        // check if it's already EOF and nothing to progress
        if self.in_progress_milestones_data.is_empty() && self.eof {
            warn!("Skipped already imported LogFile: {}", self.get_name());
            return Ok(());
        }
        self.service.update_status(ServiceStatus::Running);
        let event = BrokerEvent::Children(BrokerChild::Importer(self.service.clone(), status, self.parallelism));
        if let Some(supervisor) = supervisor {
            supervisor.send(event).ok();
            while let Some(event) = self.inbox.recv().await {
                match event {
                    ImporterEvent::CqlResult(result) => {
                        match result {
                            Ok(milestone_index) => {
                                // remove it from in_progress
                                let _ = self
                                    .in_progress_milestones_data
                                    .remove(&milestone_index)
                                    .expect("Expected entry for a milestone data");
                                info!("Imported milestone data for milestone index: {}", milestone_index);
                                let ms_bytes_size = self
                                    .in_progress_milestones_data_bytes_size
                                    .remove(&milestone_index)
                                    .expect("Expected size-entry for a milestone data");
                                let skipped = false;
                                Self::imported(
                                    supervisor,
                                    self.from_ms,
                                    self.to_ms,
                                    self.log_file_size,
                                    milestone_index,
                                    ms_bytes_size,
                                    skipped,
                                );
                                // check if we should process more
                                if !self.service.is_stopping() {
                                    // process one more
                                    if let Some(milestone_data) =
                                        self.next_milestone_data(supervisor).await.map_err(|e| {
                                            error!("Unable to fetch next milestone data. Error: {}", e);
                                            Need::Abort
                                        })?
                                    {
                                        T::handle_milestone_data(milestone_data, self).map_err(|e| {
                                            error!("{}", e);
                                            Need::Abort
                                        })?;
                                    } else {
                                        // no more milestone data.
                                        if self.in_progress_milestones_data.is_empty() {
                                            // shut it down
                                            info!("Imported the LogFile: {}", self.get_name());
                                            return Ok(());
                                        }
                                    };
                                }
                            }
                            Err(_milestone_index) => {
                                // an outage in scylla so we abort
                                return Err(Need::Abort);
                            }
                        }
                    }
                    // note: we receive this variant in All mode.
                    ImporterEvent::ProcessMore(milestone_index) => {
                        if self.service.is_stopping() {
                            continue;
                        }
                        // extract the remaining milestone data iterator
                        let (mut iter, analytic_record) = self
                            .in_progress_milestones_data
                            .remove(&milestone_index)
                            .expect("Expected Entry for milestone data");
                        let is_empty = iter.len() == 0;
                        let importer_handle = self.handle.clone().expect("Expected importer handle");
                        let keyspace = self.get_keyspace();
                        if !is_empty {
                            self.insert_some_messages(milestone_index, &mut iter).map_err(|e| {
                                error!("Unable to insert/import more message ,Error: {}", e);
                                Need::Abort
                            })?;
                        } else {
                            // insert it into analytics and sync table
                            let milestone_index = MilestoneIndex(milestone_index);
                            let synced_by = Some(self.chronicle_id);
                            let logged_by = Some(self.chronicle_id);
                            let synced_record = SyncRecord::new(milestone_index, synced_by, logged_by);
                            let worker = AnalyzeAndSyncWorker::boxed(
                                importer_handle,
                                keyspace,
                                analytic_record.clone(),
                                synced_record,
                                self.retries_per_query,
                            );
                            self.default_keyspace
                                .insert_prepared(&Synckey, &analytic_record)
                                .consistency(Consistency::One)
                                .build()
                                .map_err(|_| Need::Abort)?
                                .send_local(worker);
                        }
                        // put it back
                        self.in_progress_milestones_data
                            .insert(milestone_index, (iter, analytic_record));
                        // NOTE: we only delete it once we get Ok CqlResult
                    }
                    ImporterEvent::Shutdown => {
                        self.service.update_status(ServiceStatus::Stopping);
                        self.handle.take();
                        status = Err(Need::Abort);
                    }
                }
            }
            status
        } else {
            Err(Need::Abort)
        }
    }
}
impl<T> Importer<T> {
    pub(crate) fn get_keyspace(&self) -> ChronicleKeyspace {
        self.default_keyspace.clone()
    }
    fn get_partition_id(&self, milestone_index: MilestoneIndex) -> u16 {
        self.partition_config.partition_id(milestone_index.0)
    }
}
impl<T: ImportMode> Importer<T> {
    pub(crate) fn insert_message_with_metadata<I: Inherent>(
        &mut self,
        inherent_worker: &I,
        message_id: MessageId,
        message: Message,
        metadata: MessageMetadata,
    ) -> anyhow::Result<()> {
        let milestone_index = metadata
            .referenced_by_milestone_index
            .expect("Expected referenced milestone index in metadata");
        // Insert parents/children
        self.insert_parents(
            inherent_worker,
            &message_id,
            &message.parents(),
            MilestoneIndex(milestone_index),
            metadata.ledger_inclusion_state.clone(),
        )?;
        // insert payload (if any)
        if let Some(payload) = message.payload() {
            self.insert_payload(
                inherent_worker,
                &message_id,
                &message,
                &payload,
                MilestoneIndex(milestone_index),
                metadata.ledger_inclusion_state.clone(),
                Some(metadata.clone()),
            )?;
        }
        let message_tuple = (message, metadata);
        // store message and metadata
        self.insert(inherent_worker, message_id, message_tuple)
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
            self.insert(inherent_worker, partitioned, parent_record)?;
            // insert hint record
            let hint = Hint::parent(parent_id.to_string());
            let partition = Partition::new(partition_id, *milestone_index);
            self.insert(inherent_worker, hint, partition)?;
        }
        Ok(())
    }
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
            Payload::Transaction(transaction) => {
                self.insert_transaction(
                    inherent_worker,
                    message_id,
                    message,
                    transaction,
                    inclusion_state,
                    milestone_index,
                    metadata,
                )?;
            }
            Payload::Milestone(milestone) => {
                let ms_index = *milestone.essence().index();
                let parents_check = message.parents().eq(milestone.essence().parents());
                if metadata.is_some() && parents_check {
                    self.insert(
                        inherent_worker,
                        MilestoneIndex(ms_index),
                        (*message_id, milestone.clone()),
                    )?;
                }
            }
            e => {
                // Skip remaining payload types.
                warn!("Skipping unsupported payload variant: {:?}", e);
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
        self.insert(inherent_worker, partitioned, index_record)?;
        // insert hint record
        let hint = Hint::index(index.0);
        let partition = Partition::new(partition_id, *milestone_index);
        self.insert(inherent_worker, hint, partition)
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
                };
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
                )?;
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
        self.insert(inherent_worker, input_id, transaction_record)
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
        self.insert(inherent_worker, utxo_id, transaction_record)
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
        self.insert(inherent_worker, output_id, transaction_record)
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
                let Address::Ed25519(ed_address) = sls.address();
                {
                    let partitioned = Partitioned::new(*ed_address, partition_id, milestone_index.0);
                    let address_record =
                        AddressRecord::new(output_type, *transaction_id, index, sls.amount(), inclusion_state);
                    self.insert(inherent_worker, partitioned, address_record)?;
                    // insert hint record
                    let hint = Hint::address(ed_address.to_string());
                    let partition = Partition::new(partition_id, *milestone_index);
                    self.insert(inherent_worker, hint, partition)?;
                };
            }
            Output::SignatureLockedDustAllowance(slda) => {
                let Address::Ed25519(ed_address) = slda.address();
                {
                    let partitioned = Partitioned::new(*ed_address, partition_id, milestone_index.0);
                    let address_record =
                        AddressRecord::new(output_type, *transaction_id, index, slda.amount(), inclusion_state);
                    self.insert(inherent_worker, partitioned, address_record)?;
                    // insert hint record
                    let hint = Hint::address(ed_address.to_string());
                    let partition = Partition::new(partition_id, *milestone_index);
                    self.insert(inherent_worker, hint, partition)?;
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
    fn insert<I, K, V>(&self, inherent_worker: &I, key: K, value: V) -> anyhow::Result<()>
    where
        I: Inherent,
        K: 'static + Send + Clone,
        V: 'static + Send + Clone,
        ChronicleKeyspace: Insert<K, V>,
    {
        let req = self
            .default_keyspace
            .insert(&key, &value)
            .consistency(Consistency::One)
            .build()?;
        let worker = inherent_worker.inherent_boxed(key, value);
        req.send_local(worker);
        Ok(())
    }
}
