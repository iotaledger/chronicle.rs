// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0
use super::*;
use crate::archiver::LogFile;
use bee_message::{
    address::Address,
    input::Input,
    output::Output,
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
use chronicle_common::{
    config::PartitionConfig,
    Synckey,
};
use chronicle_storage::access::SyncRecord;
use scylla_rs::{
    app::worker::handle_insert_unprepared_error,
    prelude::stage::Reporter,
};
use std::{
    collections::btree_map::IntoIter,
    ops::Range,
    path::PathBuf,
    sync::atomic::Ordering,
};
use tokio::sync::mpsc::UnboundedSender;

/// Import all records to all tables
pub struct All;
/// Import analytics records only which are stored in analytics table
pub struct Analytics;
/// Importer state
pub struct Importer<T> {
    /// The file path of the importer
    file_path: PathBuf,
    /// The log file
    log_file: Option<LogFile>,
    /// LogFile total_size,
    log_file_size: u64,
    /// from milestone index
    from_ms: u32,
    /// to milestone index
    to_ms: u32,
    /// The default Chronicle keyspace
    default_keyspace: ChronicleKeyspace,
    /// The partition configuration
    partition_config: PartitionConfig,
    /// The number of retries per query
    retries_per_query: usize,
    /// The chronicle id
    chronicle_id: u8,
    /// The number of parallelism
    parallelism: u8,
    /// The resume flag
    resume: bool,
    /// The range of requested milestones to import
    import_range: Range<u32>,
    /// The database sync data
    sync_data: SyncData,
    /// In progress milestones data
    in_progress_milestones_data: HashMap<u32, (IntoIter<MessageId, FullMessage>, AnalyticRecord)>,
    in_progress_milestones_data_bytes_size: HashMap<u32, usize>,
    /// The flag of end of file
    eof: bool,
    responder: tokio::sync::mpsc::UnboundedSender<ImporterSession>,
    /// Import mode marker
    _mode: std::marker::PhantomData<T>,
}

#[build]
pub fn build<T>(
    keyspace: ChronicleKeyspace,
    partition_config: PartitionConfig,
    file_path: PathBuf,
    retries_per_query: Option<usize>,
    resume: Option<bool>,
    import_range: Option<Range<u32>>,
    parallelism: Option<u8>,
    chronicle_id: u8,
    responder: tokio::sync::mpsc::UnboundedSender<ImporterSession>,
) -> Importer<T> {
    let import_range = import_range.unwrap_or(Range {
        start: 1,
        end: i32::MAX as u32,
    });
    Importer::<T> {
        file_path,
        log_file: None,
        log_file_size: 0,
        from_ms: 0,
        to_ms: 0,
        default_keyspace: keyspace,
        partition_config,
        parallelism: parallelism.unwrap_or(10),
        chronicle_id,
        in_progress_milestones_data: HashMap::new(),
        in_progress_milestones_data_bytes_size: HashMap::new(),
        retries_per_query: retries_per_query.unwrap_or(10),
        resume: resume.unwrap_or(true),
        import_range,
        sync_data: SyncData::default(),
        eof: false,
        responder,
        _mode: std::marker::PhantomData::<T>,
    }
}

#[async_trait]
impl<T: 'static + Send + Sync + ImportMode> Actor for Importer<T> {
    type Dependencies = Act<Scylla>;
    type Event = ImporterEvent;
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
        info!(
            "{} is Initializing, with permanode keyspace: {}",
            self.name(),
            self.default_keyspace.name()
        );
        rt.update_status(ServiceStatus::Initializing).await.ok();
        let log_file = LogFile::try_from(self.file_path.clone())
            .map_err(|e| anyhow::anyhow!("Unable to create LogFile. Error: {}", e))?;
        self.from_ms = log_file.from_ms_index();
        self.to_ms = log_file.to_ms_index();
        self.log_file_size = log_file.len();
        let importer_session = ImporterSession::ProgressBar {
            log_file_size: self.log_file_size,
            from_ms: self.from_ms,
            to_ms: self.to_ms,
            ms_bytes_size: 0,
            milestone_index: 0,
            skipped: true,
        };
        self.responder.send(importer_session).ok();
        // fetch sync data from the keyspace
        if self.resume {
            let sync_range = SyncRange {
                from: self.from_ms,
                to: self.to_ms,
            };
            self.sync_data = SyncData::try_fetch(&self.default_keyspace, &sync_range, 10)
                .await
                .map_err(|e| anyhow::anyhow!("Unable to fetch SyncData. Error:  {}", e))?;
        }
        self.log_file.replace(log_file);
        self.init_importing(&rt.handle())
            .await
            .map_err(|e| anyhow::anyhow!("Unable to init importing process. Error: {}", e))?;

        Ok(())
    }

    async fn run<Reg: RegistryAccess + Send + Sync, Sup: EventDriven>(
        &mut self,
        rt: &mut ActorScopedRuntime<Self, Reg, Sup>,
        _: Self::Dependencies,
    ) -> Result<(), ActorError>
    where
        Self: Sized,
        Sup::Event: SupervisorEvent,
        <Sup::Event as SupervisorEvent>::Children: From<PhantomData<Self>>,
    {
        // check if it's already EOF and nothing to progress
        if self.in_progress_milestones_data.is_empty() && self.eof {
            warn!("Skipped already imported LogFile: {}", self.file_path.to_string_lossy());
            return Ok(());
        }
        let my_handle = rt.handle();
        rt.update_status(ServiceStatus::Running).await.ok();
        while let Some(event) = rt.next_event().await {
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
                            self.imported(
                                self.from_ms,
                                self.to_ms,
                                self.log_file_size,
                                milestone_index,
                                ms_bytes_size,
                                skipped,
                            );

                            // process one more
                            if let Some(milestone_data) = self
                                .next_milestone_data()
                                .await
                                .map_err(|e| anyhow::anyhow!("Unable to fetch next milestone data. Error: {}", e))?
                            {
                                T::handle_milestone_data(milestone_data, self, &my_handle)
                                    .await
                                    .map_err(|e| anyhow::anyhow!("{}", e))?;
                            } else {
                                // no more milestone data.
                                if self.in_progress_milestones_data.is_empty() {
                                    // shut it down
                                    info!("Imported the LogFile: {}", self.file_path.to_string_lossy());
                                    return Ok(());
                                }
                            };
                        }
                        Err(_milestone_index) => {
                            // an outage in scylla so we abort
                            return Err(anyhow::anyhow!("Scylla error!").into());
                        }
                    }
                }
                // note: we receive this variant in All mode.
                ImporterEvent::ProcessMore(milestone_index) => {
                    // extract the remaining milestone data iterator
                    let (mut iter, analytic_record) = self
                        .in_progress_milestones_data
                        .remove(&milestone_index)
                        .expect("Expected Entry for milestone data");
                    let is_empty = iter.len() == 0;
                    let keyspace = self.get_keyspace();
                    if !is_empty {
                        self.insert_some_messages(milestone_index, &mut iter, &my_handle)
                            .await
                            .map_err(|e| anyhow::anyhow!("Unable to insert/import more message ,Error: {}", e))?;
                    } else {
                        // insert it into analytics and sync table
                        let milestone_index = MilestoneIndex(milestone_index);
                        let synced_by = Some(self.chronicle_id);
                        let logged_by = Some(self.chronicle_id);
                        let synced_record = SyncRecord::new(milestone_index, synced_by, logged_by);
                        let worker = AnalyzeAndSyncWorker::boxed(
                            my_handle.clone().into_inner().into_inner(),
                            keyspace,
                            analytic_record.clone(),
                            synced_record,
                            self.retries_per_query,
                        );
                        self.default_keyspace
                            .insert_prepared(&Synckey, &analytic_record)
                            .consistency(Consistency::One)
                            .build()?
                            .send_local(worker);
                    }
                    // put it back
                    self.in_progress_milestones_data
                        .insert(milestone_index, (iter, analytic_record));
                    // NOTE: we only delete it once we get Ok CqlResult
                }
                ImporterEvent::Shutdown => {
                    break;
                }
            }
        }
        rt.update_status(ServiceStatus::Stopping).await.ok();
        let log_file = self.log_file.as_ref().unwrap();
        let importer_session = ImporterSession::Finish {
            from_ms: log_file.from_ms_index(),
            to_ms: log_file.to_ms_index(),
            msg: "done".to_string(),
        };
        self.responder.send(importer_session).ok();

        Ok(())
    }

    fn name(&self) -> std::borrow::Cow<'static, str> {
        format!("Importer ({})", self.file_path.to_string_lossy()).into()
    }
}

/// Defines the Importer Mode
#[async_trait]
pub trait ImportMode: Sized + Send + 'static
where
    Self: Send + Sync,
{
    /// Instruct how to import the milestone data
    async fn handle_milestone_data(
        milestone_data: MilestoneData,
        importer: &mut Importer<Self>,
        importer_handle: &Act<Importer<Self>>,
    ) -> anyhow::Result<()>;
}

#[async_trait]
impl ImportMode for All {
    async fn handle_milestone_data(
        milestone_data: MilestoneData,
        importer: &mut Importer<All>,
        importer_handle: &Act<Importer<All>>,
    ) -> anyhow::Result<()> {
        let analytic_record = milestone_data.get_analytic_record().map_err(|e| {
            error!("Unable to get analytic record for milestone data. Error: {}", e);
            e
        })?;
        let milestone_index = milestone_data.milestone_index();
        let mut iterator = milestone_data.into_iter();
        importer
            .insert_some_messages(milestone_index, &mut iterator, importer_handle)
            .await?;
        importer
            .in_progress_milestones_data
            .insert(milestone_index, (iterator, analytic_record));
        Ok(())
    }
}

#[async_trait]
impl ImportMode for Analytics {
    async fn handle_milestone_data(
        milestone_data: MilestoneData,
        importer: &mut Importer<Analytics>,
        importer_handle: &Act<Importer<Analytics>>,
    ) -> anyhow::Result<()> {
        let analytic_record = milestone_data.get_analytic_record().map_err(|e| {
            error!("Unable to get analytic record for milestone data. Error: {}", e);
            e
        })?;
        let milestone_index = milestone_data.milestone_index();
        let iterator = milestone_data.into_iter();
        importer.insert_analytic_record(&analytic_record, importer_handle)?;
        // note: iterator is not needed to presist analytic record in Analytics mode,
        // however we kept them for simplicty sake.
        importer
            .in_progress_milestones_data
            .insert(milestone_index, (iterator, analytic_record));
        Ok(())
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

impl<T: 'static + Send + Sync + ImportMode> Importer<T> {
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
        K: 'static + Send + Sync + Clone,
        V: 'static + Send + Sync + Clone,
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

    async fn init_importing(&mut self, my_handle: &Act<Self>) -> anyhow::Result<()> {
        for _ in 0..self.parallelism {
            if let Some(milestone_data) = self.next_milestone_data().await? {
                T::handle_milestone_data(milestone_data, self, my_handle).await?;
            } else {
                self.eof = true;
                break;
            }
        }
        Ok(())
    }
    async fn next_milestone_data(&mut self) -> anyhow::Result<Option<MilestoneData>> {
        let mut scan_budget: usize = 100;
        loop {
            let log_file = self.log_file.as_mut().unwrap();
            let pre_len = log_file.len();
            if let Some(milestone_data) = log_file.next().await? {
                let milestone_index = milestone_data.milestone_index();
                let not_in_import_range = !self.import_range.contains(&milestone_index);
                let resume = self.resume && self.sync_data.completed.iter().any(|r| r.contains(&milestone_index));
                if resume || not_in_import_range {
                    warn!(
                        "Skipping imported milestone data for milestone index: {}",
                        milestone_index
                    );
                    let skipped = true;
                    let ms_bytes_size = (pre_len - log_file.len()) as usize;
                    let (from_ms, to_ms) = (log_file.from_ms_index(), log_file.to_ms_index());
                    self.imported(
                        from_ms,
                        to_ms,
                        self.log_file_size,
                        milestone_index,
                        ms_bytes_size,
                        skipped,
                    );
                    // skip this synced milestone data
                    if scan_budget > 0 {
                        scan_budget -= 1;
                    } else {
                        scan_budget = 100;
                        tokio::task::yield_now().await;
                    }
                } else {
                    let ms_bytes_size = (pre_len - log_file.len()) as usize;
                    self.in_progress_milestones_data_bytes_size
                        .insert(milestone_index, ms_bytes_size);
                    return Ok(Some(milestone_data));
                }
            } else {
                return Ok(None);
            }
        }
    }
    fn imported(
        &mut self,
        from_ms: u32,
        to_ms: u32,
        log_file_size: u64,
        milestone_index: u32,
        ms_bytes_size: usize,
        skipped: bool,
    ) {
        let importer_session = ImporterSession::ProgressBar {
            log_file_size,
            from_ms,
            to_ms,
            ms_bytes_size,
            milestone_index,
            skipped,
        };
        self.responder.send(importer_session).ok();
    }
    async fn insert_some_messages(
        &mut self,
        milestone_index: u32,
        milestone_data: &mut IntoIter<MessageId, FullMessage>,
        my_handle: &Act<Self>,
    ) -> anyhow::Result<()> {
        let keyspace = self.get_keyspace();
        let inherent_worker = MilestoneDataWorker::new(
            my_handle.clone().into_inner().into_inner(),
            keyspace,
            milestone_index,
            self.retries_per_query,
        );
        for _ in 0..self.parallelism {
            if let Some((message_id, FullMessage(message, metadata))) = milestone_data.next() {
                // Insert the message
                self.insert_message_with_metadata(&inherent_worker, message_id, message, metadata)?;
            } else {
                // break for loop
                break;
            }
        }
        Ok(())
    }
}

/// Importer events
pub enum ImporterEvent {
    /// The result of an insert into the database
    CqlResult(Result<u32, u32>),
    /// Indicator to continue processing
    ProcessMore(u32),
    /// Shutdown the importer
    Shutdown,
}

impl Importer<Analytics> {
    fn insert_analytic_record(&self, analytic_record: &AnalyticRecord, my_handle: &Act<Self>) -> anyhow::Result<()> {
        let keyspace = self.get_keyspace();
        let worker = AnalyzeWorker::boxed(
            my_handle.clone().into_inner().into_inner(),
            keyspace,
            analytic_record.clone(),
            self.retries_per_query,
        );
        self.default_keyspace
            .insert_prepared(&Synckey, analytic_record)
            .consistency(Consistency::One)
            .build()?
            .send_local(worker);
        Ok(())
    }
}

/// Scylla worker implementation for the importer
#[derive(Clone)]
pub struct AtomicImporterWorker<S, K, V>
where
    S: 'static + Insert<K, V> + Insert<Synckey, SyncRecord>,
    K: 'static + Send,
    V: 'static + Send,
{
    handle: std::sync::Arc<AtomicImporterHandle<S>>,
    keyspace: S,
    key: K,
    value: V,
    retries: usize,
}

/// An atomic importer handle
pub struct AtomicImporterHandle<S>
where
    S: 'static + Insert<Synckey, SyncRecord>,
{
    /// The importer handle
    pub(crate) handle: UnboundedSender<ImporterEvent>,
    /// The keyspace
    pub(crate) keyspace: S,
    /// The milestone index
    pub(crate) milestone_index: u32,
    /// The atomic flag to indicate any error
    pub(crate) any_error: std::sync::atomic::AtomicBool,
    /// The number of retires
    pub(crate) retries: usize,
}

impl<S> AtomicImporterHandle<S>
where
    S: 'static + Insert<Synckey, SyncRecord>,
{
    /// Create a new atomic importer handle with an importer handle, a keyspace, a milestone index, an atomic error
    /// indicator, and a number of retires
    pub fn new(
        handle: UnboundedSender<ImporterEvent>,
        keyspace: S,
        milestone_index: u32,
        any_error: std::sync::atomic::AtomicBool,
        retries: usize,
    ) -> Self {
        Self {
            handle,
            keyspace,
            milestone_index,
            any_error,
            retries,
        }
    }
}
impl<S: Insert<K, V>, K, V> AtomicImporterWorker<S, K, V>
where
    S: 'static + Insert<K, V> + Insert<Synckey, SyncRecord>,
    K: 'static + Send,
    V: 'static + Send,
{
    /// Create a new atomic importer worker with an atomic importer handle, a keyspace, a key, a value, and a number of
    /// retries
    pub fn new(handle: std::sync::Arc<AtomicImporterHandle<S>>, key: K, value: V) -> Self {
        let keyspace = handle.keyspace.clone();
        let retries = handle.retries;
        Self {
            handle,
            keyspace,
            key,
            value,
            retries,
        }
    }
    /// Create a new boxed atomic importer worker with an atomic importer handle, a keyspace, a key, a value, and a
    /// number of retries
    pub fn boxed(handle: std::sync::Arc<AtomicImporterHandle<S>>, key: K, value: V) -> Box<Self> {
        Box::new(Self::new(handle, key, value))
    }
}

impl<S, K, V> Worker for AtomicImporterWorker<S, K, V>
where
    S: 'static + Insert<K, V> + Insert<Synckey, SyncRecord> + Insert<Synckey, AnalyticRecord>,
    K: 'static + Send + Sync + Clone,
    V: 'static + Send + Sync + Clone,
{
    fn handle_response(self: Box<Self>, giveload: Vec<u8>) -> anyhow::Result<()> {
        Decoder::from(giveload.try_into()?).get_void()
    }
    fn handle_error(
        mut self: Box<Self>,
        mut error: WorkerError,
        reporter: Option<&mut UnboundedSender<<Reporter as Actor>::Event>>,
    ) -> anyhow::Result<()> {
        if let WorkerError::Cql(ref mut cql_error) = error {
            if let (Some(id), Some(reporter)) = (cql_error.take_unprepared_id(), reporter) {
                handle_insert_unprepared_error(&self, &self.keyspace, &self.key, &self.value, id, reporter)?;
                return Ok(());
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
            tokio::spawn(async { req.send_global(self) });
        } else {
            // no more retries
            self.handle.any_error.store(true, Ordering::Acquire);
        }
        Ok(())
    }
}

impl<S> Drop for AtomicImporterHandle<S>
where
    S: 'static + Insert<Synckey, SyncRecord>,
{
    fn drop(&mut self) {
        let any_error = self.any_error.load(Ordering::Relaxed);
        if any_error {
            let _ = self.handle.send(ImporterEvent::CqlResult(Err(self.milestone_index)));
        } else {
            // tell importer to process more
            let _ = self.handle.send(ImporterEvent::ProcessMore(self.milestone_index));
        }
    }
}

/// Analyze allownd Sync worker for the importer
#[derive(Clone)]
pub struct AnalyzeAndSyncWorker<S>
where
    S: 'static + Insert<Synckey, SyncRecord>,
{
    /// The importer handle
    handle: UnboundedSender<ImporterEvent>,
    /// The keyspace
    keyspace: S,
    /// The `analytics` table row
    analytic_record: AnalyticRecord,
    /// Identify if we analyzed the
    analyzed: bool, // if true we sync
    /// The `sync` table row
    synced_record: SyncRecord,
    /// The number of retries
    retries: usize,
}

impl<S> AnalyzeAndSyncWorker<S>
where
    S: 'static + Insert<Synckey, SyncRecord>,
{
    /// Create a new sync worker with an importer handle, a keyspace, a `sync` table row (`SyncRecord`), and a number of
    /// retries
    pub fn new(
        handle: UnboundedSender<ImporterEvent>,
        keyspace: S,
        analytic_record: AnalyticRecord,
        synced_record: SyncRecord,
        retries: usize,
    ) -> Self {
        Self {
            handle,
            keyspace,
            analytic_record,
            synced_record,
            analyzed: false,
            retries,
        }
    }
    ///  Create a new boxed ync worker with an importer handle, a keyspace, a `sync` table row (`SyncRecord`), and a
    /// number of retries
    pub fn boxed(
        handle: UnboundedSender<ImporterEvent>,
        keyspace: S,
        analytic_record: AnalyticRecord,
        synced_record: SyncRecord,
        retries: usize,
    ) -> Box<Self> {
        Box::new(Self::new(handle, keyspace, analytic_record, synced_record, retries))
    }
}

/// Implement the Scylla `Worker` trait
impl<S> Worker for AnalyzeAndSyncWorker<S>
where
    S: 'static + Insert<Synckey, SyncRecord> + Insert<Synckey, AnalyticRecord>,
{
    fn handle_response(mut self: Box<Self>, giveload: Vec<u8>) -> anyhow::Result<()> {
        Decoder::from(giveload.try_into()?).get_void()?;
        let milestone_index = *self.synced_record.milestone_index;
        if self.analyzed {
            // tell importer
            self.handle.send(ImporterEvent::CqlResult(Ok(milestone_index))).ok();
        } else {
            // set it to be analyzed, as this response is for analytic record
            self.analyzed = true;
            // insert sync record
            let req = self
                .keyspace
                .insert_prepared(&Synckey, &self.synced_record)
                .consistency(Consistency::One)
                .build()?;
            req.send_local(self);
        }
        Ok(())
    }
    fn handle_error(
        mut self: Box<Self>,
        mut error: WorkerError,
        reporter: Option<&mut UnboundedSender<<Reporter as Actor>::Event>>,
    ) -> anyhow::Result<()> {
        if let WorkerError::Cql(ref mut cql_error) = error {
            if let (Some(id), Some(reporter)) = (cql_error.take_unprepared_id(), reporter) {
                // check if the worker is handling analyzed or synced record error
                if self.analyzed {
                    handle_insert_unprepared_error(&self, &self.keyspace, &Synckey, &self.synced_record, id, reporter)?;
                } else {
                    handle_insert_unprepared_error(
                        &self,
                        &self.keyspace,
                        &Synckey,
                        &self.analytic_record,
                        id,
                        reporter,
                    )?;
                }
                return Ok(());
            }
        }
        if self.retries > 0 {
            self.retries -= 1;
            // currently we assume all cql/worker errors are retryable, but we might change this in future
            if self.analyzed {
                // retry inserting an sync record
                let req = self
                    .keyspace
                    .insert_query(&Synckey, &self.synced_record)
                    .consistency(Consistency::One)
                    .build()?;
                tokio::spawn(async { req.send_global(self) });
            } else {
                // retry inserting an analytic record
                let req = self
                    .keyspace
                    .insert_query(&Synckey, &self.analytic_record)
                    .consistency(Consistency::One)
                    .build()?;
                tokio::spawn(async { req.send_global(self) });
            }
        } else {
            // no more retries
            // respond with error
            let milestone_index = *self.synced_record.milestone_index;
            let _ = self.handle.send(ImporterEvent::CqlResult(Err(milestone_index)));
        }
        Ok(())
    }
}

/// A milestone data worker
pub struct MilestoneDataWorker<S>
where
    S: 'static + Insert<Synckey, SyncRecord>,
{
    /// The arced atomic importer handle for a given keyspace
    arc_handle: std::sync::Arc<AtomicImporterHandle<S>>,
}

impl<S> MilestoneDataWorker<S>
where
    S: 'static + Insert<Synckey, SyncRecord>,
{
    /// Create a new milestone data worker with an importer handle, a keyspace, a milestone index, and a number of
    /// retries
    fn new(importer_handle: UnboundedSender<ImporterEvent>, keyspace: S, milestone_index: u32, retries: usize) -> Self {
        let any_error = std::sync::atomic::AtomicBool::new(false);
        let atomic_handle =
            AtomicImporterHandle::new(importer_handle, keyspace.clone(), milestone_index, any_error, retries);
        let arc_handle = std::sync::Arc::new(atomic_handle);
        Self { arc_handle }
    }
}

/// The inherent trait to return a boxed worker for a given key/value pair
pub(crate) trait Inherent {
    fn inherent_boxed<K, V>(&self, key: K, value: V) -> Box<dyn Worker>
    where
        ChronicleKeyspace: 'static + Insert<K, V> + Insert<Synckey, SyncRecord>,
        K: 'static + Send + Sync + Clone,
        V: 'static + Send + Sync + Clone;
}

/// Implement the `Inherent` trait for the milestone data worker, so we can get the atomic importer worker
/// which contains the atomic importer handle of the milestone data worker
impl Inherent for MilestoneDataWorker<ChronicleKeyspace> {
    fn inherent_boxed<K, V>(&self, key: K, value: V) -> Box<dyn Worker>
    where
        ChronicleKeyspace: 'static + Insert<K, V> + Insert<Synckey, SyncRecord>,
        K: 'static + Send + Sync + Clone,
        V: 'static + Send + Sync + Clone,
    {
        AtomicImporterWorker::boxed(self.arc_handle.clone(), key, value)
    }
}

/// Scylla worker implementation for importer when running in Analytics mode
#[derive(Clone)]
pub struct AnalyzeWorker<S>
where
    S: 'static + Insert<Synckey, AnalyticRecord>,
{
    /// The importer handle
    handle: UnboundedSender<ImporterEvent>,
    /// The keyspace
    keyspace: S,
    /// The `analytics` table row
    analytic_record: AnalyticRecord,
    /// The number of retries
    retries: usize,
}

impl<S> AnalyzeWorker<S>
where
    S: 'static + Insert<Synckey, AnalyticRecord>,
{
    /// Create a new AnalyzeWorker with an importer handle, a keyspace, a `analytics` table row (`AnalyticRecord`), and
    /// a number of retries
    pub fn new(
        handle: UnboundedSender<ImporterEvent>,
        keyspace: S,
        analytic_record: AnalyticRecord,
        retries: usize,
    ) -> Self {
        Self {
            handle,
            keyspace,
            analytic_record,
            retries,
        }
    }
    /// Create a new boxed AnalyzeWorker with an importer handle, a keyspace, a `analytics` table row
    /// (`AnalyticRecord`), and a number of retries
    pub fn boxed(
        handle: UnboundedSender<ImporterEvent>,
        keyspace: S,
        analytic_record: AnalyticRecord,
        retries: usize,
    ) -> Box<Self> {
        Box::new(Self::new(handle, keyspace, analytic_record, retries))
    }
}

/// Implement the Scylla `Worker` trait
impl<S> Worker for AnalyzeWorker<S>
where
    S: 'static + Insert<Synckey, AnalyticRecord>,
{
    fn handle_response(self: Box<Self>, giveload: Vec<u8>) -> anyhow::Result<()> {
        Decoder::from(giveload.try_into()?).get_void()?;
        let milestone_index = self.analytic_record.milestone_index();
        self.handle.send(ImporterEvent::CqlResult(Ok(**milestone_index))).ok();
        Ok(())
    }
    fn handle_error(
        mut self: Box<Self>,
        mut error: WorkerError,
        reporter: Option<&mut UnboundedSender<<Reporter as Actor>::Event>>,
    ) -> anyhow::Result<()> {
        if let WorkerError::Cql(ref mut cql_error) = error {
            if let (Some(id), Some(reporter)) = (cql_error.take_unprepared_id(), reporter) {
                handle_insert_unprepared_error(&self, &self.keyspace, &Synckey, &self.analytic_record, id, reporter)?;
                return Ok(());
            }
        }
        if self.retries > 0 {
            self.retries -= 1;
            // currently we assume all cql/worker errors are retryable, but we might change this in future
            // retry inserting an analytic record
            let req = self
                .keyspace
                .insert_query(&Synckey, &self.analytic_record)
                .consistency(Consistency::One)
                .build()?;
            tokio::spawn(async { req.send_global(self) });
        } else {
            // no more retries
            // respond with error
            let milestone_index = self.analytic_record.milestone_index();
            let _ = self.handle.send(ImporterEvent::CqlResult(Err(**milestone_index)));
        }
        Ok(())
    }
}
