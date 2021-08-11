// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0
use super::{
    archiver::{
        Archiver,
        ArchiverEvent,
    },
    collector::{
        AskCollector,
        Collector,
        CollectorEvent,
        MessageIdPartitioner,
    },
    syncer::{
        Syncer,
        SyncerEvent,
    },
    *,
};
use bee_message::prelude::{
    MilestoneIndex,
    MilestonePayload,
};
use chronicle_common::Synckey;
use scylla_rs::prelude::stage::Reporter;
use tokio::sync::mpsc::UnboundedSender;

use std::{
    collections::HashSet,
    sync::atomic::Ordering,
};

/// Solidifier state, each Solidifier solidifiy subset of (milestones_index % solidifier_count == partition_id)
pub struct Solidifier {
    /// It's the chronicle id.
    chronicle_id: u8,
    keyspace: ChronicleKeyspace,
    partition_id: u8,
    milestones_data: HashMap<u32, MilestoneData>,
    in_database: HashMap<u32, InDatabase>,
    unreachable: HashSet<u32>,
    collector_count: u8,
    message_id_partitioner: MessageIdPartitioner,
    first: Option<u32>,
    gap_start: u32,
    expected: u32,
    retries: u16,
}

#[build]
#[derive(Clone)]
pub fn build_solidifier(
    chronicle_id: Option<u8>,
    keyspace: ChronicleKeyspace,
    partition_id: u8,
    gap_start: u32,
    retries: Option<u16>,
    collector_count: u8,
) -> Solidifier {
    Solidifier {
        partition_id,
        keyspace,
        chronicle_id: chronicle_id.unwrap_or(0),
        in_database: Default::default(),
        unreachable: Default::default(),
        milestones_data: Default::default(),
        collector_count,
        message_id_partitioner: MessageIdPartitioner::new(collector_count),
        first: None,
        gap_start,
        expected: 0,
        retries: retries.unwrap_or(100),
    }
}

#[async_trait]
impl Actor for Solidifier {
    type Dependencies = (
        Act<Syncer>,
        Act<Scylla>,
        Pool<MapPool<Collector, u8>>,
        Option<Act<Archiver>>,
    );
    type Event = SolidifierEvent;
    type Channel = UnboundedTokioChannel<Self::Event>;

    async fn init<Reg: RegistryAccess + Send + Sync, Sup: EventDriven>(
        &mut self,
        _rt: &mut ActorScopedRuntime<Self, Reg, Sup>,
    ) -> Result<(), ActorError> {
        Ok(())
    }

    async fn run<Reg: RegistryAccess + Send + Sync, Sup: EventDriven>(
        &mut self,
        rt: &mut ActorScopedRuntime<Self, Reg, Sup>,
        (syncer_handle, _scylla_handle, collector_handles, archiver_handle): Self::Dependencies,
    ) -> Result<(), ActorError>
    where
        Self: Sized,
        Sup::Event: SupervisorEvent,
        <Sup::Event as SupervisorEvent>::Children: From<PhantomData<Self>>,
    {
        rt.update_status(ServiceStatus::Running).await.ok();
        let my_handle = rt.handle();
        while let Some(event) = rt.next_event().await {
            match event {
                SolidifierEvent::Message(full_message) => {
                    self.handle_new_msg(
                        full_message,
                        &my_handle,
                        &archiver_handle,
                        &collector_handles,
                        &syncer_handle,
                    )
                    .await?;
                }
                SolidifierEvent::CqlResult(result) => {
                    match result {
                        Ok(cql_result) => {
                            match cql_result {
                                CqlResult::PersistedMsg(message_id, milestone_index) => {
                                    // ensure we have entry for the following milestone_index
                                    if let Some(in_database) = self.in_database.get_mut(&milestone_index) {
                                        in_database.add_message_id(message_id);
                                        // check_if_in_database
                                        if in_database.check_if_all_in_database() {
                                            // Insert record into sync table
                                            self.handle_in_database(milestone_index, &my_handle)?;
                                        }
                                    }
                                }
                                CqlResult::SyncedMilestone(milestone_index) => {
                                    // Inform syncer (maybe it wants to update the dashboard or something)
                                    info!("Synced this milestone {}", milestone_index);
                                }
                                CqlResult::AnalyzedMilestone(milestone_index) => {
                                    if let Some(in_database) = self.in_database.get_mut(&milestone_index) {
                                        in_database.set_analyzed(true);
                                        info!("Analyzed this milestone {}", milestone_index);
                                        if in_database.check_if_all_in_database() {
                                            // Insert record into sync table
                                            self.handle_in_database(milestone_index, &my_handle)?;
                                        }
                                    } else {
                                        error!("Analyzed Milestone should have in_database entry");
                                    }
                                }
                            }
                        }
                        Err(cql_result) => {
                            match cql_result {
                                CqlResult::PersistedMsg(message_id, milestone_index) => {
                                    error!(
                                        "Unable to persist message with id: {}, referenced by milestone index: {}",
                                        message_id, milestone_index
                                    );
                                }
                                CqlResult::SyncedMilestone(milestone_index) => {
                                    error!("Unable to update sync table for milestone index: {}", milestone_index,);
                                }
                                CqlResult::AnalyzedMilestone(milestone_index) => {
                                    error!(
                                        "Unable to update analytics table for milestone index: {}",
                                        milestone_index,
                                    );
                                }
                            }
                            // Abort solidifier in order to let broker app reschedule itself after few mins
                            // with reasonable retries, it means our cluster is likely in outage or partial outage (ie
                            // all replicas for given token).
                            return Err(anyhow::anyhow!(
                                "Scylla cluster is likely having a complete outage, so we are shutting down broker."
                            )
                            .into());
                        }
                    }
                }
                SolidifierEvent::Close(message_id, milestone_index) => {
                    self.close_message_id(
                        milestone_index,
                        &message_id,
                        &my_handle,
                        &archiver_handle,
                        &syncer_handle,
                    )
                    .await?;
                }
                SolidifierEvent::Milestone(milestone_message) => {
                    self.handle_milestone_msg(
                        milestone_message,
                        &my_handle,
                        &archiver_handle,
                        &syncer_handle,
                        &collector_handles,
                    )
                    .await?;
                }
                SolidifierEvent::Solidify(milestone_index) => {
                    match milestone_index {
                        Ok(milestone_index) => {
                            // this is request to solidify this milestone
                            self.handle_solidify(milestone_index, &syncer_handle, &collector_handles)
                                .await;
                        }
                        Err(milestone_index) => {
                            // This is response from collector(s) that we are unable to solidify
                            // this milestone_index
                            self.handle_solidify_failure(milestone_index, &syncer_handle).await;
                        }
                    }
                }
                SolidifierEvent::Shutdown => break,
            }
        }
        Ok(())
    }

    fn name(&self) -> std::borrow::Cow<'static, str> {
        format!("Solidifier ({})", self.partition_id).into()
    }
}

impl Solidifier {
    async fn handle_solidify_failure(&mut self, milestone_index: u32, syncer_handle: &Act<Syncer>) {
        error!(
            "Solidifier id: {}. was unable to solidify milestone_index: {}",
            self.partition_id, milestone_index
        );
        // check if it's not already in our unreachable cache
        if !self.unreachable.contains(&milestone_index) {
            // remove its milestone_data
            if let Some(ms_data) = self.milestones_data.remove(&milestone_index) {
                self.in_database.remove(&milestone_index);
                // move to unreachable atm
                self.unreachable.insert(milestone_index);
                // ensure it's created by syncer
                if ms_data.created_by.eq(&CreatedBy::Syncer) {
                    // tell syncer to skip it
                    warn!(
                        "Solidifier id: {}, failed to solidify syncer requested index: {} milestone data",
                        self.partition_id, milestone_index
                    );
                    syncer_handle.send(SyncerEvent::Unreachable(milestone_index)).ok();
                } else {
                    // there is a glitch in the new incoming data, however the archiver and syncer will take care of
                    // that.
                    warn!(
                        "Solidifier id: {}, failed to solidify new incoming milestone data for index: {}",
                        self.partition_id, milestone_index
                    );
                }
            }
        }
    }
    async fn handle_solidify(
        &mut self,
        milestone_index: u32,
        syncer_handle: &Act<Syncer>,
        collector_handles: &Pool<MapPool<Collector, u8>>,
    ) {
        if milestone_index >= self.expected {
            syncer_handle.send(SyncerEvent::Current(milestone_index)).ok();
            return;
        }
        // remove it from unreachable (if we already tried to solidify it before)
        self.unreachable.remove(&milestone_index);
        info!(
            "Solidifier id: {}. got solidifiy request for milestone_index: {}",
            self.partition_id, milestone_index
        );
        // this is request from syncer in order for solidifier to collect,
        // the milestone data for the provided milestone index.
        if let Some(ms_data) = self.milestones_data.get_mut(&milestone_index) {
            // NOTE: this likely will never happen
            let created_by = ms_data.created_by();
            warn!(
                "Received solidify request on an existing milestone data: index: {} created_by: {:?}, pending: {}, messages: {}, milestone_exist: {}, unless this is an expected race condition",
                milestone_index, created_by, ms_data.pending().len(), ms_data.messages().len(), ms_data.milestone_exist(),
            );
            if created_by == &CreatedBy::Expected && !ms_data.milestone_exist() {
                // convert the ownership to syncer
                ms_data.created_by = CreatedBy::Syncer;
                // request milestone in order to respark solidification process
                Self::request_milestone_message(&collector_handles, self.partition_id, milestone_index).await;
                // insert empty entry for in_database
                self.in_database
                    .entry(milestone_index)
                    .or_insert_with(|| InDatabase::new(milestone_index));
            } else {
                // tell syncer to skip this atm
                syncer_handle.send(SyncerEvent::Unreachable(milestone_index)).ok();
            }
        } else {
            // Asking any collector (as we don't know the message id of the milestone)
            // however, we use milestone_index % collector_count to have unfirom distribution.
            // note: solidifier_id/partition_id is actually = milestone_index % collector_count;
            // as both solidifiers and collectors have the same count.
            // this event should be enough to spark the solidification process
            Self::request_milestone_message(&collector_handles, self.partition_id, milestone_index).await;
            // insert empty entry
            self.milestones_data
                .insert(milestone_index, MilestoneData::new(milestone_index, CreatedBy::Syncer));
            // insert empty entry for in_database
            self.in_database
                .entry(milestone_index)
                .or_insert_with(|| InDatabase::new(milestone_index));
        }
    }
    async fn close_message_id(
        &mut self,
        milestone_index: u32,
        message_id: &MessageId,
        my_handle: &Act<Self>,
        archiver_handle: &Option<Act<Archiver>>,
        syncer_handle: &Act<Syncer>,
    ) -> anyhow::Result<()> {
        if let Some(milestone_data) = self.milestones_data.get_mut(&milestone_index) {
            // remove it from pending
            milestone_data.remove_from_pending(message_id);
            let check_if_completed = milestone_data.check_if_completed();
            let created_by = milestone_data.created_by;
            if check_if_completed {
                match created_by {
                    CreatedBy::Incoming | CreatedBy::Expected => {
                        self.push_to_logger(milestone_index, my_handle, archiver_handle).await?
                    }
                    CreatedBy::Syncer => self.push_to_syncer(milestone_index, my_handle, syncer_handle).await?,
                    CreatedBy::Exporter => (),
                }
            }
        } else {
            if milestone_index < self.expected {
                warn!("Already deleted milestone data for milestone index: {}, this happens when solidify request has failure", milestone_index)
            } else {
                error!(
                    "Not supposed to get close response on non-existing milestone data {}",
                    milestone_index
                )
            }
        }
        Ok(())
    }
    async fn push_to_logger(
        &mut self,
        milestone_index: u32,
        my_handle: &Act<Self>,
        archiver_handle: &Option<Act<Archiver>>,
    ) -> anyhow::Result<()> {
        // Remove milestoneData from self state and pass it to archiver
        let milestone_data = self
            .milestones_data
            .remove(&milestone_index)
            .expect("Expected milestone data for milestone_index");
        let analytic_record = milestone_data.get_analytic_record()?;
        self.insert_analytic(milestone_index, analytic_record, my_handle)?;
        // Update in_database
        let in_database = self
            .in_database
            .entry(milestone_index)
            .or_insert_with(|| InDatabase::from(&milestone_data));
        in_database.set_messages_len(milestone_data.messages().len());
        if in_database.check_if_all_in_database() {
            // Insert record into sync table
            self.handle_in_database(milestone_index, my_handle)?;
        }
        if let Some(archiver_handle) = archiver_handle.as_ref() {
            info!(
                "solidifier_id: {}, is pushing the milestone data for index: {}, to Logger",
                self.partition_id, milestone_index
            );
            archiver_handle
                .send(ArchiverEvent::MilestoneData(milestone_data, None))
                .ok();
        };
        Ok(())
    }
    async fn push_to_syncer(
        &mut self,
        milestone_index: u32,
        my_handle: &Act<Self>,
        syncer_handle: &Act<Syncer>,
    ) -> anyhow::Result<()> {
        info!(
            "Solidifier is pushing the milestone data for index: {}, to Syncer",
            milestone_index
        );
        // Remove milestoneData from self state and pass it to syncer
        let milestone_data = self
            .milestones_data
            .remove(&milestone_index)
            .expect("Expected milestone data for milestone_index");
        let analytic_record = milestone_data.get_analytic_record()?;
        self.insert_analytic(milestone_index, analytic_record, my_handle)?;
        // Update in_database
        let in_database = self
            .in_database
            .entry(milestone_index)
            .or_insert_with(|| InDatabase::from(&milestone_data));
        in_database.set_messages_len(milestone_data.messages().len());
        if in_database.check_if_all_in_database() {
            // Insert record into sync table
            self.handle_in_database(milestone_index, my_handle)?;
        }
        let syncer_event = SyncerEvent::MilestoneData(milestone_data);
        syncer_handle.send(syncer_event).ok();
        Ok(())
    }
    fn handle_in_database(&mut self, milestone_index: u32, my_handle: &Act<Self>) -> anyhow::Result<()> {
        self.in_database.remove(&milestone_index);
        let sync_key = Synckey;
        let synced_by = Some(self.chronicle_id);
        let synced_record = SyncRecord::new(MilestoneIndex(milestone_index), synced_by, None);
        let request = self
            .keyspace
            .insert(&sync_key, &synced_record)
            .consistency(Consistency::One)
            .build()?;
        let worker = SyncedMilestoneWorker::boxed(
            my_handle.clone(),
            milestone_index,
            self.keyspace.clone(),
            sync_key,
            synced_record,
            self.retries,
        );
        request.send_local(worker);
        Ok(())
    }
    fn insert_analytic(
        &self,
        milestone_index: u32,
        analytic_record: AnalyticRecord,
        my_handle: &Act<Self>,
    ) -> anyhow::Result<()> {
        let sync_key = Synckey;
        let request = self
            .keyspace
            .insert(&sync_key, &analytic_record)
            .consistency(Consistency::One)
            .build()?;
        let worker = AnalyzedMilestoneWorker::boxed(
            my_handle.clone(),
            milestone_index,
            self.keyspace.clone(),
            sync_key,
            analytic_record,
            self.retries,
        );
        request.send_local(worker);
        Ok(())
    }
    async fn handle_milestone_msg(
        &mut self,
        MilestoneMessage(_message_id, milestone_payload, message, metadata): MilestoneMessage,
        my_handle: &Act<Self>,
        archiver_handle: &Option<Act<Archiver>>,
        syncer_handle: &Act<Syncer>,
        collector_handles: &Pool<MapPool<Collector, u8>>,
    ) -> anyhow::Result<()> {
        let milestone_index = milestone_payload.essence().index().0;
        let partitioner = &self.message_id_partitioner;
        let solidifier_id = self.partition_id;
        let ms_count = self.milestones_data.len();
        if let Some(milestone_data) = self.milestones_data.get_mut(&milestone_index) {
            Self::process_parents(
                message.parents(),
                milestone_data,
                collector_handles,
                partitioner,
                solidifier_id,
                milestone_index,
            )
            .await;
            // insert milestone into milestone_data
            if let Some(metadata) = metadata {
                info!(
                    "solidifier_id: {}, got full milestone {}, in progress: {}, created by {:?}",
                    self.partition_id, milestone_index, ms_count, milestone_data.created_by
                );
                milestone_data.set_milestone(milestone_payload);
                milestone_data.remove_from_pending(&metadata.message_id);
                milestone_data.add_full_message(FullMessage::new(message, metadata));
                let check_if_completed = milestone_data.check_if_completed();
                let created_by = milestone_data.created_by;
                if check_if_completed {
                    match created_by {
                        CreatedBy::Incoming | CreatedBy::Expected => {
                            self.push_to_logger(milestone_index, my_handle, archiver_handle).await?
                        }
                        CreatedBy::Syncer => self.push_to_syncer(milestone_index, my_handle, syncer_handle).await?,
                        CreatedBy::Exporter => (),
                    }
                }
            }
        } else {
            if let Some(metadata) = metadata {
                // We have to decide whether to insert entry for milestone_index or not.
                self.insert_new_entry_or_not(milestone_index, FullMessage::new(message, metadata), collector_handles)
                    .await;
            }
        }
        Ok(())
    }
    async fn process_parents(
        parents: &[MessageId],
        milestone_data: &mut MilestoneData,
        collector_handles: &Pool<MapPool<Collector, u8>>,
        partitioner: &MessageIdPartitioner,
        solidifier_id: u8,
        milestone_index: u32,
    ) {
        // Ensure all parents exist in milestone_data
        // Note: Some or all parents might belong to older milestone,
        // and it's the job of the collector to tell us when to close message_id
        // and remove it from pending
        for parent_id in parents.iter() {
            let in_messages = milestone_data.messages().contains_key(&parent_id);
            let in_pending = milestone_data.pending().contains(&parent_id);
            let genesis = parent_id.eq(&MessageId::null());
            // Check if parent NOT in messages nor pending
            if !in_messages && !in_pending && !genesis {
                // Request it from collector
                Self::request_full_message(
                    collector_handles,
                    partitioner,
                    solidifier_id,
                    milestone_index,
                    *parent_id,
                    *milestone_data.created_by(),
                )
                .await;
                // Add it to pending
                milestone_data.pending.insert(*parent_id);
            };
        }
    }
    async fn request_full_message(
        collector_handles: &Pool<MapPool<Collector, u8>>,
        partitioner: &MessageIdPartitioner,
        solidifier_id: u8,
        milestone_index: u32,
        parent_id: MessageId,
        created_by: CreatedBy,
    ) {
        // Request it from collector
        let collector_id = partitioner.partition_id(&parent_id);
        collector_handles
            .send(
                &collector_id,
                CollectorEvent::Ask(AskCollector::FullMessage(
                    solidifier_id,
                    milestone_index,
                    parent_id,
                    created_by,
                )),
            )
            .await;
    }
    async fn request_milestone_message(
        collector_handles: &Pool<MapPool<Collector, u8>>,
        collector_id: u8,
        milestone_index: u32,
    ) {
        collector_handles
            .send(
                &collector_id,
                CollectorEvent::Ask(AskCollector::MilestoneMessage(milestone_index)),
            )
            .await;
    }
    async fn handle_new_msg(
        &mut self,
        full_message: FullMessage,
        my_handle: &Act<Self>,
        archiver_handle: &Option<Act<Archiver>>,
        collector_handles: &Pool<MapPool<Collector, u8>>,
        syncer_handle: &Act<Syncer>,
    ) -> anyhow::Result<()> {
        // check what milestone_index referenced this message
        let milestone_index = full_message.ref_ms().unwrap();
        let partitioner = &self.message_id_partitioner;
        let solidifier_id = self.partition_id;
        if let Some(milestone_data) = self.milestones_data.get_mut(&milestone_index) {
            Self::process_milestone_data(
                solidifier_id,
                collector_handles,
                milestone_data,
                partitioner,
                milestone_index,
                full_message,
            )
            .await;
            let check_if_completed = milestone_data.check_if_completed();
            let created_by = milestone_data.created_by;
            if check_if_completed {
                match created_by {
                    CreatedBy::Incoming | CreatedBy::Expected => {
                        self.push_to_logger(milestone_index, my_handle, archiver_handle).await?
                    }
                    CreatedBy::Syncer => self.push_to_syncer(milestone_index, my_handle, syncer_handle).await?,
                    CreatedBy::Exporter => (),
                }
            }
        } else {
            // We have to decide whether to insert entry for milestone_index or not.
            self.insert_new_entry_or_not(milestone_index, full_message, collector_handles)
                .await;
        }
        Ok(())
    }
    async fn insert_new_entry_or_not(
        &mut self,
        milestone_index: u32,
        full_message: FullMessage,
        collector_handles: &Pool<MapPool<Collector, u8>>,
    ) {
        // do not insert new entry for unreachable milestone_index atm
        // this happens when we get one or few solidify_failures so we deleted an active milestone_data that still
        // getting new messages which will reinvoke insert_new_entry_or_not
        if self.unreachable.contains(&milestone_index) {
            return;
        }
        let partitioner = &self.message_id_partitioner;
        let solidifier_id = self.partition_id;
        // Check if this is the first observed message
        if self.first.is_none() {
            // Ensure to proceed only if ms_index >= provided static gap lower bound from syncer.
            if milestone_index >= self.gap_start {
                // Set it as static bound.
                self.first.replace(milestone_index);
                info!(
                    "solidifier id: {:?}, observed its first milestone index: {}",
                    solidifier_id, milestone_index
                );
                // For safety reasons, we ask collector for this milestone,
                // as it's the first observed message/milestone which we want to ensure it's solid.

                Self::request_milestone_message(collector_handles, solidifier_id, milestone_index).await;
                self.expected = milestone_index + (self.collector_count as u32);

                // Create the first entry using syncer
                let milestone_data = self
                    .milestones_data
                    .entry(milestone_index)
                    .or_insert_with(|| MilestoneData::new(milestone_index, CreatedBy::Syncer));
                Self::process_milestone_data(
                    solidifier_id,
                    collector_handles,
                    milestone_data,
                    partitioner,
                    milestone_index,
                    full_message,
                )
                .await;
                // No need to check if it's completed.
            }
        } else if milestone_index >= self.expected {
            // Insert it as new incoming entry
            let milestone_data = self
                .milestones_data
                .entry(milestone_index)
                .or_insert_with(|| MilestoneData::new(milestone_index, CreatedBy::Incoming));
            // check if the full_message has MilestonePayload
            if let Some(bee_message::payload::Payload::Milestone(milestone_payload)) = full_message.0.payload() {
                milestone_data.set_milestone(milestone_payload.clone());
            }
            Self::process_milestone_data(
                solidifier_id,
                collector_handles,
                milestone_data,
                partitioner,
                milestone_index,
                full_message,
            )
            .await;
            // No need to check if it's completed.
            // still for safety reasons, we should ask collector for its milestone,
            // but we are going to let syncer sends us an event when it observes a glitch

            // Insert entries for anything in between(belongs to self solidifier_id) as Expected,
            for expected in self.expected..milestone_index {
                let id = (expected % self.collector_count as u32) as u8;
                if id.eq(&self.partition_id) {
                    error!(
                        "solidifier_id: {}, expected: {}, but got: {}",
                        id, expected, milestone_index
                    );
                    // Insert it as new expected entry, only if we don't already have an existing entry for it
                    let milestone_data = self
                        .milestones_data
                        .entry(expected)
                        .or_insert_with(|| MilestoneData::new(expected, CreatedBy::Expected));
                    if !milestone_data.milestone_exist() {
                        error!(
                            "solidifier_id: {}, however syncer will fill the expected index: {} milestone",
                            id, expected
                        );
                    }
                }
            }
            // set it as recent expected
            self.expected = milestone_index + (self.collector_count as u32);
            info!(
                "solidifier_id: {}, set new expected {}",
                self.partition_id, self.expected
            );
        }
    }
    async fn process_milestone_data(
        solidifier_id: u8,
        collector_handles: &Pool<MapPool<Collector, u8>>,
        milestone_data: &mut MilestoneData,
        partitioner: &MessageIdPartitioner,
        ms_index: u32,
        full_message: FullMessage,
    ) {
        Self::process_parents(
            &full_message.metadata().parent_message_ids,
            milestone_data,
            collector_handles,
            partitioner,
            solidifier_id,
            ms_index,
        )
        .await;
        // remove it from the pending(if it does already exist)
        milestone_data.remove_from_pending(full_message.message_id());
        // Add full message
        milestone_data.add_full_message(full_message);
    }
}

/// Solidifier events
pub enum SolidifierEvent {
    /// Milestone fullmessage;
    Milestone(MilestoneMessage),
    /// Pushed or requested messages, that definitely belong to self solidifier
    Message(FullMessage),
    /// Close MessageId that doesn't belong at all to Solidifier of milestone u32
    Close(MessageId, u32),
    /// Solidifiy request from Syncer.
    /// Solidifier should collect milestonedata and pass it to Syncer(not archiver)
    Solidify(Result<u32, u32>),
    /// CqlResult from scylla worker;
    CqlResult(Result<CqlResult, CqlResult>),
    /// Shutdown the solidifier
    Shutdown,
}

/// Cql Results
pub enum CqlResult {
    /// Message was persisted or not
    PersistedMsg(MessageId, u32),
    /// Milestone was synced or not
    SyncedMilestone(u32),
    /// Analyzed MilestoneData or not
    AnalyzedMilestone(u32),
}

/// A milestone message payload
pub struct MilestoneMessage(MessageId, Box<MilestonePayload>, Message, Option<MessageMetadata>);
impl MilestoneMessage {
    /// Create a new milestone message payload
    pub fn new(
        message_id: MessageId,
        milestone_payload: Box<MilestonePayload>,
        message: Message,
        metadata: Option<MessageMetadata>,
    ) -> Self {
        Self(message_id, milestone_payload, message, metadata)
    }
}

struct InDatabase {
    #[allow(unused)]
    milestone_index: u32,
    analyzed: bool,
    messages_len: usize,
    in_database: HashSet<MessageId>,
}

impl InDatabase {
    fn new(milestone_index: u32) -> Self {
        Self {
            milestone_index,
            analyzed: false,
            messages_len: usize::MAX,
            in_database: Default::default(),
        }
    }
    fn set_messages_len(&mut self, message_len: usize) {
        self.messages_len = message_len
    }
    fn add_message_id(&mut self, message_id: MessageId) {
        self.in_database.insert(message_id);
    }
    fn set_analyzed(&mut self, analyzed: bool) {
        self.analyzed = analyzed;
    }
    fn check_if_all_in_database(&self) -> bool {
        self.messages_len == self.in_database.len() && self.analyzed
    }
}

impl From<&MilestoneData> for InDatabase {
    fn from(milestone_data: &MilestoneData) -> Self {
        let mut in_database = Self::new(milestone_data.milestone_index());
        in_database.set_messages_len(milestone_data.messages().len());
        in_database
    }
}
/// Scylla worker implementation
#[derive(Clone)]
pub struct AtomicSolidifierWorker<S, K, V>
where
    S: 'static + Insert<K, V>,
    K: 'static + Send,
    V: 'static + Send,
{
    handle: std::sync::Arc<AtomicSolidifierHandle>,
    keyspace: S,
    key: K,
    value: V,
    retries: usize,
}

/// Atomic solidifier handle
pub struct AtomicSolidifierHandle {
    pub(crate) handle: UnboundedSender<SolidifierEvent>,
    pub(crate) milestone_index: u32,
    pub(crate) message_id: MessageId,
    pub(crate) any_error: std::sync::atomic::AtomicBool,
}
impl AtomicSolidifierHandle {
    /// Create a new Atomic solidifier handle
    pub fn new(
        handle: Act<Solidifier>,
        milestone_index: u32,
        message_id: MessageId,
        any_error: std::sync::atomic::AtomicBool,
    ) -> Self {
        Self {
            handle: handle.into_inner().into_inner(),
            milestone_index,
            message_id,
            any_error,
        }
    }
}
impl<S: Insert<K, V>, K, V> AtomicSolidifierWorker<S, K, V>
where
    S: 'static + Insert<K, V>,
    K: 'static + Send,
    V: 'static + Send,
{
    /// Create a new atomic solidifier worker with a handle and retries
    pub fn new(handle: std::sync::Arc<AtomicSolidifierHandle>, keyspace: S, key: K, value: V, retries: usize) -> Self {
        Self {
            handle,
            keyspace,
            key,
            value,
            retries,
        }
    }
    /// Create a new boxed atomic solidifier worker with a handle and retries
    pub fn boxed(
        handle: std::sync::Arc<AtomicSolidifierHandle>,
        keyspace: S,
        key: K,
        value: V,
        retries: usize,
    ) -> Box<Self> {
        Box::new(Self::new(handle, keyspace, key, value, retries))
    }
}

impl<S, K, V> Worker for AtomicSolidifierWorker<S, K, V>
where
    S: 'static + Insert<K, V>,
    K: 'static + Send + Sync + Clone,
    V: 'static + Send + Sync + Clone,
{
    fn handle_response(self: Box<Self>, giveload: Vec<u8>) -> anyhow::Result<()> {
        Decoder::try_from(giveload).and_then(|decoder| decoder.get_void())
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
            match self
                .keyspace
                .insert_query(&self.key, &self.value)
                .consistency(Consistency::One)
                .build()
            {
                Ok(req) => {
                    tokio::spawn(async { req.send_global(self) });
                }
                Err(e) => {
                    error!("{}", e);
                }
            }
        } else {
            // no more retries
            self.handle.any_error.store(true, Ordering::Relaxed);
        }
        Ok(())
    }
}

impl Drop for AtomicSolidifierHandle {
    fn drop(&mut self) {
        let cql_result = CqlResult::PersistedMsg(self.message_id, self.milestone_index);
        let any_error = self.any_error.load(Ordering::Relaxed);
        if any_error {
            self.handle.send(SolidifierEvent::CqlResult(Err(cql_result))).ok();
        } else {
            // respond with void
            self.handle.send(SolidifierEvent::CqlResult(Ok(cql_result))).ok();
        }
    }
}

/// Solidifier worker
#[derive(Clone)]
pub struct SyncedMilestoneWorker<S, K, V>
where
    S: 'static + Insert<K, V>,
    K: 'static + Send,
    V: 'static + Send,
{
    handle: Act<Solidifier>,
    milestone_index: u32,
    keyspace: S,
    key: K,
    value: V,
    retries: u16,
}

impl<S: Insert<K, V>, K, V> SyncedMilestoneWorker<S, K, V>
where
    S: 'static + Insert<K, V>,
    K: 'static + Send,
    V: 'static + Send,
{
    /// Create a new solidifier worker with a handle and retries
    pub fn new(handle: Act<Solidifier>, milestone_index: u32, keyspace: S, key: K, value: V, retries: u16) -> Self {
        Self {
            handle,
            milestone_index,
            keyspace,
            key,
            value,
            retries,
        }
    }
    /// Create a new boxed solidifier worker with a handle and retries
    pub fn boxed(
        handle: Act<Solidifier>,
        milestone_index: u32,
        keyspace: S,
        key: K,
        value: V,
        retries: u16,
    ) -> Box<Self> {
        Box::new(Self::new(handle, milestone_index, keyspace, key, value, retries))
    }
}

impl<S, K, V> Worker for SyncedMilestoneWorker<S, K, V>
where
    S: 'static + Insert<K, V>,
    K: 'static + Send + Sync + Clone,
    V: 'static + Send + Sync + Clone,
{
    fn handle_response(self: Box<Self>, giveload: Vec<u8>) -> anyhow::Result<()> {
        Decoder::try_from(giveload).and_then(|decoder| decoder.get_void())?;
        let synced_ms = CqlResult::SyncedMilestone(self.milestone_index);
        let _ = self.handle.send(SolidifierEvent::CqlResult(Ok(synced_ms)));
        Ok(())
    }
    fn handle_error(
        mut self: Box<Self>,
        mut error: WorkerError,
        reporter: Option<&mut UnboundedSender<<Reporter as Actor>::Event>>,
    ) -> anyhow::Result<()> {
        error!(
            "{:?}, retries remaining: {}, reporter running: {}",
            error,
            self.retries,
            reporter.is_some()
        );
        if let WorkerError::Cql(ref mut cql_error) = error {
            if let (Some(id), Some(reporter)) = (cql_error.take_unprepared_id(), reporter) {
                handle_insert_unprepared_error(&self, &self.keyspace, &self.key, &self.value, id, reporter)?;
                return Ok(());
            }
        }
        if self.retries > 0 {
            self.retries -= 1;
            // currently we assume all cql/worker errors are retryable, but we might change this in future
            match self
                .keyspace
                .insert_query(&self.key, &self.value)
                .consistency(Consistency::One)
                .build()
            {
                Ok(req) => {
                    tokio::spawn(async { req.send_global(self) });
                }
                Err(e) => {
                    error!("{}", e);
                }
            }
        } else {
            // no more retries
            // respond with error
            let synced_ms = CqlResult::SyncedMilestone(self.milestone_index);
            let _ = self.handle.send(SolidifierEvent::CqlResult(Err(synced_ms)));
        }
        Ok(())
    }
}

/// Solidifier worker
#[derive(Clone)]
pub struct AnalyzedMilestoneWorker<S, K, V>
where
    S: 'static + Insert<K, V>,
    K: 'static + Send,
    V: 'static + Send,
{
    handle: UnboundedSender<SolidifierEvent>,
    milestone_index: u32,
    keyspace: S,
    key: K,
    value: V,
    retries: u16,
}

impl<S: Insert<K, V>, K, V> AnalyzedMilestoneWorker<S, K, V>
where
    S: 'static + Insert<K, V>,
    K: 'static + Send,
    V: 'static + Send,
{
    /// Create a new solidifier worker with a handle and retries
    pub fn new(handle: Act<Solidifier>, milestone_index: u32, keyspace: S, key: K, value: V, retries: u16) -> Self {
        Self {
            handle: handle.into_inner().into_inner(),
            milestone_index,
            keyspace,
            key,
            value,
            retries,
        }
    }
    /// Create a new boxed solidifier worker with a handle and retries
    pub fn boxed(
        handle: Act<Solidifier>,
        milestone_index: u32,
        keyspace: S,
        key: K,
        value: V,
        retries: u16,
    ) -> Box<Self> {
        Box::new(Self::new(handle, milestone_index, keyspace, key, value, retries))
    }
}

impl<S, K, V> Worker for AnalyzedMilestoneWorker<S, K, V>
where
    S: 'static + Insert<K, V>,
    K: 'static + Send + Sync + Clone,
    V: 'static + Send + Sync + Clone,
{
    fn handle_response(self: Box<Self>, giveload: Vec<u8>) -> anyhow::Result<()> {
        Decoder::try_from(giveload).and_then(|decoder| decoder.get_void())?;
        let analyzed_ms = CqlResult::AnalyzedMilestone(self.milestone_index);
        let _ = self.handle.send(SolidifierEvent::CqlResult(Ok(analyzed_ms)));
        Ok(())
    }
    fn handle_error(
        mut self: Box<Self>,
        mut error: WorkerError,
        reporter: Option<&mut UnboundedSender<<Reporter as Actor>::Event>>,
    ) -> anyhow::Result<()> {
        error!(
            "{:?}, retries remaining: {}, reporter running: {}",
            error,
            self.retries,
            reporter.is_some()
        );
        if let WorkerError::Cql(ref mut cql_error) = error {
            if let (Some(id), Some(reporter)) = (cql_error.take_unprepared_id(), reporter) {
                handle_insert_unprepared_error(&self, &self.keyspace, &self.key, &self.value, id, reporter)?;
                return Ok(());
            }
        }
        if self.retries > 0 {
            self.retries -= 1;
            // currently we assume all cql/worker errors are retryable, but we might change this in future
            match self
                .keyspace
                .insert_query(&self.key, &self.value)
                .consistency(Consistency::One)
                .build()
            {
                Ok(req) => {
                    tokio::spawn(async { req.send_global(self) });
                }
                Err(e) => {
                    error!("{}", e);
                }
            }
        } else {
            // no more retries
            // respond with error
            let analyzed_ms = CqlResult::AnalyzedMilestone(self.milestone_index);
            let _ = self.handle.send(SolidifierEvent::CqlResult(Err(analyzed_ms)));
        }
        Ok(())
    }
}
