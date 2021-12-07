// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0
use super::{
    archiver::{
        ArchiverEvent,
        ArchiverHandle,
    },
    collector::{
        AskCollector,
        CollectorEvent,
        CollectorHandle,
        MessageIdPartitioner,
    },
    syncer::{
        SyncerEvent,
        SyncerHandle,
    },
    *,
};
use bee_message::prelude::{
    MilestoneIndex,
    MilestonePayload,
};
use chronicle_common::alert;
use chronicle_filter::Selected;
use chronicle_storage::access::MilestoneData;
use lru::LruCache;
use std::{
    fmt::Debug,
    sync::atomic::Ordering,
};
/// The solidifier handle type
pub type SolidifierHandle = UnboundedHandle<SolidifierEvent>;
/// A milestone message payload
#[derive(Debug)]
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
    in_database: HashMap<MessageId, ()>,
}

impl InDatabase {
    fn new(milestone_index: u32) -> Self {
        Self {
            milestone_index,
            analyzed: false,
            messages_len: usize::MAX,
            in_database: HashMap::new(),
        }
    }
    fn set_messages_len(&mut self, message_len: usize) {
        self.messages_len = message_len
    }
    fn add_message_id(&mut self, message_id: MessageId) {
        self.in_database.insert(message_id, ());
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

/// Solidifier events
#[derive(Debug)]
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

impl ShutdownEvent for SolidifierEvent {
    fn shutdown_event() -> Self {
        Self::Shutdown
    }
}

/// Cql Results
#[derive(Debug)]
pub enum CqlResult {
    /// Message was persisted or not
    PersistedMsg(MessageId, u32, Option<Selected>),
    /// Milestone was synced or not
    SyncedMilestone(u32),
    /// Analyzed MilestoneData or not
    AnalyzedMilestone(u32),
}

/// Solidifier state, each Solidifier solidifiy subset of (milestones_index % solidifier_count == partition_id)
pub struct Solidifier {
    keyspace: ChronicleKeyspace,
    partition_id: u8,
    milestones_data: HashMap<u32, MilestoneData>,
    in_database: HashMap<u32, InDatabase>,
    lru_in_database: LruCache<u32, ()>,
    unreachable: LruCache<u32, ()>,
    message_id_partitioner: MessageIdPartitioner,
    gap_start: u32,
    expected: u32,
    retries: u8,
}

impl Solidifier {
    pub(super) fn new(
        keyspace: ChronicleKeyspace,
        partition_id: u8,
        partitioner: MessageIdPartitioner,
        gap_start: u32,
        retries: u8,
    ) -> Self {
        Self {
            keyspace,
            partition_id,
            milestones_data: HashMap::new(),
            in_database: HashMap::new(),
            lru_in_database: LruCache::new(100),
            unreachable: LruCache::new(100),
            message_id_partitioner: partitioner,
            gap_start,
            expected: 0,
            retries,
        }
    }
}

//////////////////////// Actor impl ////////////////////////////

#[async_trait]
impl<S: SupHandle<Self>> Actor<S> for Solidifier {
    type Data = (Option<ArchiverHandle>, SyncerHandle, HashMap<u8, CollectorHandle>);
    type Channel = UnboundedChannel<SolidifierEvent>;
    async fn init(&mut self, rt: &mut Rt<Self, S>) -> ActorResult<Self::Data> {
        log::info!("{:?} is initializing", &rt.service().directory());
        let parent_id = rt
            .parent_id()
            .ok_or_else(|| ActorError::exit_msg("solidifier without parent id"))?;
        let archiver_handle;
        if let Some(archiver_id) = rt.sibling("archiver").scope_id().await {
            archiver_handle = rt.lookup(archiver_id).await;
        } else {
            archiver_handle = None;
        };
        let syncer_id = rt
            .sibling("syncer")
            .scope_id()
            .await
            .ok_or_else(|| ActorError::aborted_msg("Solidifier unable to get syncer scope id"))?;
        let syncer_handle = rt.depends_on(syncer_id).await?;
        let collector_handles = rt.depends_on(parent_id).await?;
        Ok((archiver_handle, syncer_handle, collector_handles))
    }
    async fn run(
        &mut self,
        rt: &mut Rt<Self, S>,
        (archiver, syncer, collector_handles): Self::Data,
    ) -> ActorResult<()> {
        log::info!("{:?} is running", &rt.service().directory());
        while let Some(event) = rt.inbox_mut().next().await {
            match event {
                SolidifierEvent::Message(full_message) => {
                    self.handle_new_msg(rt, full_message, &collector_handles, &archiver, &syncer)
                        .await
                        .map_err(|e| {
                            error!("{}", e);
                            e
                        })?;
                }
                SolidifierEvent::CqlResult(result) => {
                    match result {
                        Ok(cql_result) => {
                            match cql_result {
                                CqlResult::PersistedMsg(message_id, milestone_index, selected_opt) => {
                                    // ensure we have entry for the following milestone_index
                                    if let Some(in_database) = self.in_database.get_mut(&milestone_index) {
                                        in_database.add_message_id(message_id);
                                        // check_if_in_database
                                        if in_database.check_if_all_in_database() {
                                            // Insert record into sync table
                                            self.handle_in_database(rt, milestone_index).await.map_err(|e| {
                                                error!("{}", e);
                                                e
                                            })?;
                                        }
                                    } else {
                                        // in rare condition there is a chance that collector will reinsert the same
                                        // message twice, therefore we ensure to
                                        // only insert new entry if the milestone_index is not already in
                                        // lru_in_database cache and not in unreachable
                                        if self.lru_in_database.get(&milestone_index).is_none()
                                            && self.unreachable.get(&milestone_index).is_none()
                                        {
                                            let mut in_database = InDatabase::new(milestone_index);
                                            in_database.add_message_id(message_id);
                                            self.in_database.insert(milestone_index, in_database);
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
                                            self.handle_in_database(rt, milestone_index).await.map_err(|e| {
                                                error!("{}", e);
                                                e
                                            })?;
                                        }
                                    } else {
                                        error!("Analyzed Milestone should have in_database entry");
                                    }
                                }
                            }
                        }
                        Err(cql_result) => {
                            match cql_result {
                                CqlResult::PersistedMsg(message_id, milestone_index, selected_opt) => {
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
                            alert!("Scylla cluster appears to be having an outage! The Chronicle Broker is pausing.")
                                .await
                                .ok();
                            // Abort solidifier in order to let broker app reschedule itself after few mins
                            // with reasonable retries, it means our cluster is likely in outage or partial outage (ie
                            // all replicas for given token).
                            return Err(ActorError::restart_msg(
                                "Scylla cluster appears to be having an outage!",
                                None,
                            ));
                        }
                    }
                }
                SolidifierEvent::Close(message_id, milestone_index) => {
                    self.close_message_id(rt, milestone_index, &message_id, &archiver, &syncer)
                        .await
                        .map_err(|e| {
                            error!("{}", e);
                            e
                        })?;
                }
                SolidifierEvent::Milestone(milestone_message) => {
                    self.handle_milestone_msg(rt, milestone_message, &collector_handles, &archiver, &syncer)
                        .await
                        .map_err(|e| {
                            error!("{}", e);
                            e
                        })?;
                }
                SolidifierEvent::Solidify(milestone_index) => {
                    match milestone_index {
                        Ok(milestone_index) => {
                            // this is request to solidify this milestone
                            self.handle_solidify(milestone_index, &collector_handles, &syncer)
                        }
                        Err(milestone_index) => {
                            // This is response from collector(s) that we are unable to solidify
                            // this milestone_index
                            self.handle_solidify_failure(milestone_index, &syncer);
                        }
                    }
                }
                SolidifierEvent::Shutdown => break,
            }
        }
        log::info!("{:?} exited its event loop", &rt.service().directory());
        Ok(())
    }
}

////////////////////////////// Solidifier Impl //////////////////////////////

impl Solidifier {
    fn partition_count(&self) -> u8 {
        self.message_id_partitioner.partition_count()
    }
    fn handle_solidify_failure(&mut self, milestone_index: u32, syncer_handle: &SyncerHandle) {
        error!(
            "Solidifier id: {}. was unable to solidify milestone_index: {}",
            self.partition_id, milestone_index
        );
        // check if it's not already in our unreachable cache
        if self.unreachable.get(&milestone_index).is_none() {
            // remove its milestone_data
            if let Some(ms_data) = self.milestones_data.remove(&milestone_index) {
                // move it out lru_in_database (if any)
                self.lru_in_database.pop(&milestone_index);
                self.in_database.remove(&milestone_index);
                // move to unreachable atm
                self.unreachable.put(milestone_index, ());
                // ensure it's created by syncer
                if ms_data.created_by().eq(&CreatedBy::Syncer) {
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
    fn handle_solidify(
        &mut self,
        milestone_index: u32,
        collector_handles: &HashMap<u8, CollectorHandle>,
        syncer_handle: &SyncerHandle,
    ) {
        // open solidify requests only for less than the expected
        if milestone_index >= self.expected {
            warn!(
                "SolidifierId: {}, cannot open solidify request for milestone_index: {} >= expected: {}",
                self.partition_id, milestone_index, self.expected
            );
            // tell syncer to skip this atm
            syncer_handle.send(SyncerEvent::Unreachable(milestone_index)).ok();
            return ();
        }
        // remove it from unreachable (if we already tried to solidify it before)
        self.unreachable.pop(&milestone_index);
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
                ms_data.set_created_by(CreatedBy::Syncer);
                // request milestone in order to respark solidification process
                Self::request_milestone_message(collector_handles, self.partition_id, milestone_index);
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
            // however, we use milestone_index % partition_count to have unfirom distribution.
            // note: solidifier_id/partition_id is actually = milestone_index % partition_count;
            // as both solidifiers and collectors have the same partition_count.
            // this event should be enough to spark the solidification process
            Self::request_milestone_message(collector_handles, self.partition_id, milestone_index);
            // insert empty entry
            self.milestones_data
                .insert(milestone_index, MilestoneData::new(milestone_index, CreatedBy::Syncer));
            // insert empty entry for in_database
            self.in_database
                .entry(milestone_index)
                .or_insert_with(|| InDatabase::new(milestone_index));
        }
    }
    async fn close_message_id<S: SupHandle<Self>>(
        &mut self,
        rt: &mut Rt<Self, S>,
        milestone_index: u32,
        message_id: &MessageId,
        archiver_handle: &Option<ArchiverHandle>,
        syncer_handle: &SyncerHandle,
    ) -> ActorResult<()> {
        if let Some(milestone_data) = self.milestones_data.get_mut(&milestone_index) {
            // remove it from pending
            milestone_data.remove_from_pending(message_id);
            let check_if_completed = milestone_data.check_if_completed();
            let created_by = milestone_data.created_by();
            if check_if_completed && !created_by.eq(&CreatedBy::Syncer) {
                self.push_to_logger(rt, milestone_index, archiver_handle).await?;
            } else if check_if_completed {
                self.push_to_syncer(rt, milestone_index, syncer_handle).await?;
            };
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
    async fn push_to_logger<S: SupHandle<Self>>(
        &mut self,
        rt: &mut Rt<Self, S>,
        milestone_index: u32,
        archive_handle: &Option<ArchiverHandle>,
    ) -> ActorResult<()> {
        // Remove milestoneData from self state and pass it to archiver
        let milestone_data = self
            .milestones_data
            .remove(&milestone_index)
            .expect("Expected milestone data for milestone_index");
        let analytic_record = milestone_data.get_analytic_record()?;
        self.insert_analytic(rt, milestone_index, analytic_record).await?;
        // Update in_database
        let in_database = self
            .in_database
            .entry(milestone_index)
            .or_insert_with(|| InDatabase::from(&milestone_data));
        in_database.set_messages_len(milestone_data.messages().len());
        if in_database.check_if_all_in_database() {
            // Insert record into sync table
            self.handle_in_database(rt, milestone_index).await?;
        }
        if let Some(archiver_handle) = archive_handle.as_ref() {
            info!(
                "solidifier_id: {}, is pushing the milestone data for index: {}, to Logger",
                self.partition_id, milestone_index
            );
            let archiver_event = ArchiverEvent::MilestoneData(milestone_data, None);
            let _ = archiver_handle.send(archiver_event);
        };
        Ok(())
    }
    async fn push_to_syncer<S: SupHandle<Self>>(
        &mut self,
        rt: &mut Rt<Self, S>,
        milestone_index: u32,
        syncer_handle: &SyncerHandle,
    ) -> ActorResult<()> {
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
        self.insert_analytic(rt, milestone_index, analytic_record).await?;
        // Update in_database
        let in_database = self
            .in_database
            .entry(milestone_index)
            .or_insert_with(|| InDatabase::from(&milestone_data));
        in_database.set_messages_len(milestone_data.messages().len());
        if in_database.check_if_all_in_database() {
            // Insert record into sync table
            self.handle_in_database(rt, milestone_index).await?;
        }
        let syncer_event = SyncerEvent::MilestoneData(milestone_data);
        syncer_handle.send(syncer_event).ok();
        Ok(())
    }
    async fn handle_in_database<S: SupHandle<Self>>(
        &mut self,
        rt: &mut Rt<Self, S>,
        milestone_index: u32,
    ) -> ActorResult<()> {
        self.in_database.remove(&milestone_index);
        self.lru_in_database.put(milestone_index, ());
        let sync_key = "permanode".to_string();
        let synced_by = Some(0);
        let synced_record = SyncRecord::new(MilestoneIndex(milestone_index), synced_by, None);
        let request = self
            .keyspace
            .insert(&sync_key, &synced_record)
            .consistency(Consistency::One)
            .build()?;
        let worker = SyncedMilestoneWorker::boxed(
            rt.handle().clone(),
            milestone_index,
            self.keyspace.clone(),
            sync_key,
            synced_record,
            self.retries,
        );
        // Request might fail due to a node just got disconnected
        if let Err(RequestError::Ring(r)) = request.send_local_with_worker(worker) {
            let keyspace_name = self.keyspace.name();
            if let Err(_) = retry_send(&keyspace_name, r, self.retries) {
                return Err(ActorError::restart_msg("unable to send sync record to the ring", None));
            };
        };
        Ok(())
    }
    async fn insert_analytic<S: SupHandle<Self>>(
        &self,
        rt: &mut Rt<Self, S>,
        milestone_index: u32,
        analytic_record: AnalyticRecord,
    ) -> anyhow::Result<()> {
        let sync_key = "permanode".to_string();
        let request = self
            .keyspace
            .insert(&sync_key, &analytic_record)
            .consistency(Consistency::One)
            .build()?;
        let worker = AnalyzedMilestoneWorker::boxed(
            rt.handle().clone(),
            milestone_index,
            self.keyspace.clone(),
            sync_key,
            analytic_record,
            self.retries,
        );
        request.send_local_with_worker(worker);
        Ok(())
    }
    async fn handle_milestone_msg<S: SupHandle<Self>>(
        &mut self,
        rt: &mut Rt<Self, S>,
        MilestoneMessage(_message_id, milestone_payload, message, metadata): MilestoneMessage,
        collector_handles: &HashMap<u8, CollectorHandle>,
        archiver_handle: &Option<ArchiverHandle>,
        syncer_handle: &SyncerHandle,
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
            );
            // insert milestone into milestone_data
            if let Some(metadata) = metadata {
                info!(
                    "solidifier_id: {}, got full milestone {}, in progress: {}",
                    self.partition_id, milestone_index, ms_count
                );
                milestone_data.set_milestone(milestone_payload);
                milestone_data.remove_from_pending(&metadata.message_id);
                milestone_data.add_full_message(FullMessage::new(message, metadata));
                let check_if_completed = milestone_data.check_if_completed();
                let created_by = milestone_data.created_by();
                if check_if_completed && !created_by.eq(&CreatedBy::Syncer) {
                    self.push_to_logger(rt, milestone_index, archiver_handle).await?;
                } else if check_if_completed {
                    self.push_to_syncer(rt, milestone_index, syncer_handle).await?;
                };
            }
        } else {
            if let Some(metadata) = metadata {
                // We have to decide whether to insert entry for milestone_index or not.
                self.insert_new_entry_or_not(
                    rt,
                    milestone_index,
                    FullMessage::new(message, metadata),
                    collector_handles,
                    archiver_handle,
                )
                .await?
            }
        }
        Ok(())
    }
    fn process_parents(
        parents: &[MessageId],
        milestone_data: &mut MilestoneData,
        collectors_handles: &HashMap<u8, CollectorHandle>,
        partitioner: &MessageIdPartitioner,
        solidifier_id: u8,
        milestone_index: u32,
    ) {
        // Ensure all parents exist in milestone_data
        // Note: Some or all parents might belong to older milestone,
        // and it's the job of the collector to tell us when to close message_id
        // and remove it from pending
        parents.iter().for_each(|parent_id| {
            let in_messages = milestone_data.messages().contains_key(&parent_id);
            let in_pending = milestone_data.pending().contains(&parent_id);
            let genesis = parent_id.eq(&MessageId::null());
            // Check if parent NOT in messages nor pending
            if !in_messages && !in_pending && !genesis {
                // Request it from collector
                Self::request_full_message(
                    collectors_handles,
                    partitioner,
                    solidifier_id,
                    milestone_index,
                    *parent_id,
                    *milestone_data.created_by(),
                );
                // Add it to pending
                milestone_data.add_pending(*parent_id);
            };
        });
    }
    fn request_full_message(
        collectors_handles: &HashMap<u8, CollectorHandle>,
        partitioner: &MessageIdPartitioner,
        solidifier_id: u8,
        milestone_index: u32,
        parent_id: MessageId,
        created_by: CreatedBy,
    ) {
        // Request it from collector
        let collector_id = partitioner.partition_id(&parent_id);
        if let Some(collector_handle) = collectors_handles.get(&collector_id) {
            let ask_event = CollectorEvent::Ask(AskCollector::FullMessage(
                solidifier_id,
                milestone_index,
                parent_id,
                created_by,
            ));
            collector_handle.send(ask_event).ok();
        }
    }
    fn request_milestone_message(
        collectors_handles: &HashMap<u8, CollectorHandle>,
        collector_id: u8,
        milestone_index: u32,
    ) {
        if let Some(collector_handle) = collectors_handles.get(&collector_id) {
            let ask_event = CollectorEvent::Ask(AskCollector::MilestoneMessage(milestone_index));
            collector_handle.send(ask_event).ok();
        }
    }
    async fn handle_new_msg<S: SupHandle<Self>>(
        &mut self,
        rt: &mut Rt<Self, S>,
        full_message: FullMessage,
        collector_handles: &HashMap<u8, CollectorHandle>,
        archiver_handle: &Option<ArchiverHandle>,
        syncer_handle: &SyncerHandle,
    ) -> ActorResult<()> {
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
            );
            let check_if_completed = milestone_data.check_if_completed();
            let created_by = milestone_data.created_by();
            if check_if_completed && !created_by.eq(&CreatedBy::Syncer) {
                self.push_to_logger(rt, milestone_index, archiver_handle).await?;
            } else if check_if_completed {
                self.push_to_syncer(rt, milestone_index, syncer_handle).await?;
            };
        } else {
            // We have to decide whether to insert entry for milestone_index or not.
            self.insert_new_entry_or_not(rt, milestone_index, full_message, collector_handles, archiver_handle)
                .await?
        }
        Ok(())
    }
    async fn insert_new_entry_or_not<S: SupHandle<Self>>(
        &mut self,
        rt: &mut Rt<Self, S>,
        milestone_index: u32,
        full_message: FullMessage,
        collector_handles: &HashMap<u8, CollectorHandle>,
        archiver_handle: &Option<ArchiverHandle>,
    ) -> ActorResult<()> {
        // do not insert new entry for unreachable milestone_index atm
        // this happens when we get one or few solidify_failures so we deleted an active milestone_data that still
        // getting new messages which will reinvoke insert_new_entry_or_not
        if self.unreachable.get(&milestone_index).is_some() {
            return Ok(());
        }
        let partitioner = &self.message_id_partitioner;
        let solidifier_id = self.partition_id;
        // Check if this is the first observed message
        if self.expected == 0 {
            // Ensure to proceed only if ms_index >= provided static gap lower bound from syncer.
            if milestone_index >= self.gap_start {
                info!(
                    "solidifier id: {:?}, observed its first milestone index: {}",
                    solidifier_id, milestone_index
                );
                // For safety reasons, we ask collector for this milestone,
                // as it's the first observed message/milestone which we want to ensure it's solid.

                Self::request_milestone_message(collector_handles, solidifier_id, milestone_index);
                self.expected = milestone_index + (self.partition_count() as u32);
                let d = MilestoneData::new(milestone_index, CreatedBy::Incoming);
                // Create the first entry using syncer
                let milestone_data = self
                    .milestones_data
                    .entry(milestone_index)
                    .or_insert_with(|| MilestoneData::new(milestone_index, CreatedBy::Incoming));
                Self::process_milestone_data(
                    solidifier_id,
                    collector_handles,
                    milestone_data,
                    partitioner,
                    milestone_index,
                    full_message,
                );
                if milestone_data.check_if_completed() {
                    self.push_to_logger(rt, milestone_index, archiver_handle).await?;
                }
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
            );
            // No need to check if it's completed.
            // still for safety reasons, we should ask collector for its milestone,
            // but we are going to let syncer sends us an event when it observes a glitch

            // Insert entries for anything in between(belongs to self solidifier_id) as Expected,
            for expected in self.expected..milestone_index {
                let id = (expected % self.partition_count() as u32) as u8;
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
            self.expected = milestone_index + (self.partition_count() as u32);
            info!(
                "solidifier_id: {}, set new expected {}",
                self.partition_id, self.expected
            );
        }
        Ok(())
    }
    fn process_milestone_data(
        solidifier_id: u8,
        collector_handles: &HashMap<u8, CollectorHandle>,
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
        );
        // remove it from the pending(if it does already exist)
        milestone_data.remove_from_pending(full_message.message_id());
        // Add full message
        milestone_data.add_full_message(full_message);
    }
}

/////////////////////////////////// Workers //////////////////////////////////////
/// Scylla worker implementation
#[derive(Clone, Debug)]
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
    retries: u8,
}

/// Atomic solidifier handle
#[derive(Debug)]
pub struct AtomicSolidifierHandle {
    pub(crate) handle: SolidifierHandle,
    pub(crate) milestone_index: u32,
    pub(crate) message_id: MessageId,
    pub(crate) selected_opt: Option<Selected>,
    pub(crate) any_error: std::sync::atomic::AtomicBool,
}
impl AtomicSolidifierHandle {
    /// Create a new Atomic solidifier handle
    pub fn new(
        handle: SolidifierHandle,
        milestone_index: u32,
        message_id: MessageId,
        selected_opt: Option<Selected>,
        any_error: std::sync::atomic::AtomicBool,
    ) -> Self {
        Self {
            handle,
            milestone_index,
            message_id,
            selected_opt,
            any_error,
        }
    }
    /// set any_error to true
    pub(crate) fn set_error(&self) {
        self.any_error.store(true, Ordering::Relaxed);
    }
}
impl<S: Insert<K, V>, K, V> AtomicSolidifierWorker<S, K, V>
where
    S: 'static + Insert<K, V>,
    K: 'static + Send,
    V: 'static + Send,
{
    /// Create a new atomic solidifier worker with a handle and retries
    pub fn new(handle: std::sync::Arc<AtomicSolidifierHandle>, keyspace: S, key: K, value: V, retries: u8) -> Self {
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
        retries: u8,
    ) -> Box<Self> {
        Box::new(Self::new(handle, keyspace, key, value, retries))
    }
}

impl<S, K, V> Worker for AtomicSolidifierWorker<S, K, V>
where
    S: 'static + Insert<K, V> + Debug,
    K: 'static + Send + Clone + Debug + Sync + TokenEncoder,
    V: 'static + Send + Clone + Debug + Sync,
{
    fn handle_response(self: Box<Self>, giveload: Vec<u8>) -> anyhow::Result<()> {
        Decoder::try_from(giveload).and_then(|decoder| decoder.get_void())
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
            match self
                .keyspace
                .insert_query(&self.key, &self.value)
                .consistency(Consistency::One)
                .build()
            {
                Ok(req) => {
                    let keyspace_name = self.keyspace.name();
                    if let Err(RequestError::Ring(r)) = req.send_global_with_worker(self) {
                        if let Err(worker) = retry_send(&keyspace_name, r, 2) {
                            worker.handle_error(WorkerError::NoRing, None)?
                        };
                    };
                }
                Err(e) => {
                    error!("{}", e);
                    self.handle.set_error();
                }
            }
        } else {
            // no more retries
            self.handle.set_error();
        }
        Ok(())
    }
}

impl Drop for AtomicSolidifierHandle {
    fn drop(&mut self) {
        let cql_result = CqlResult::PersistedMsg(self.message_id, self.milestone_index, self.selected_opt);
        // releaxed is fine, because Droping an Arc forces memory barrier
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
#[derive(Clone, Debug)]
pub struct SyncedMilestoneWorker<S, K, V>
where
    S: 'static + Insert<K, V>,
    K: 'static + Send,
    V: 'static + Send,
{
    handle: SolidifierHandle,
    milestone_index: u32,
    keyspace: S,
    key: K,
    value: V,
    retries: u8,
}

impl<S: Insert<K, V>, K, V> SyncedMilestoneWorker<S, K, V>
where
    S: 'static + Insert<K, V>,
    K: 'static + Send,
    V: 'static + Send,
{
    /// Create a new solidifier worker with a handle and retries
    pub fn new(handle: SolidifierHandle, milestone_index: u32, keyspace: S, key: K, value: V, retries: u8) -> Self {
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
        handle: SolidifierHandle,
        milestone_index: u32,
        keyspace: S,
        key: K,
        value: V,
        retries: u8,
    ) -> Box<Self> {
        Box::new(Self::new(handle, milestone_index, keyspace, key, value, retries))
    }
}

impl<S, K, V> Worker for SyncedMilestoneWorker<S, K, V>
where
    S: 'static + Insert<K, V> + Debug,
    K: 'static + Send + Clone + Debug + Sync + TokenEncoder,
    V: 'static + Send + Clone + Debug + Sync,
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
        reporter: Option<&ReporterHandle>,
    ) -> anyhow::Result<()> {
        error!("{:?}, left retries: {}", error, self.retries);
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
            match self
                .keyspace
                .insert_query(&self.key, &self.value)
                .consistency(Consistency::One)
                .build()
            {
                Ok(req) => {
                    let keyspace_name = self.keyspace.name();
                    if let Err(RequestError::Ring(r)) = req.send_global_with_worker(self) {
                        if let Err(worker) = retry_send(&keyspace_name, r, 2) {
                            worker.handle_error(WorkerError::NoRing, reporter)?
                        };
                    };
                }
                Err(e) => {
                    error!("{}", e);
                    let synced_ms = CqlResult::SyncedMilestone(self.milestone_index);
                    self.handle.send(SolidifierEvent::CqlResult(Err(synced_ms))).ok();
                }
            }
        } else {
            let synced_ms = CqlResult::SyncedMilestone(self.milestone_index);
            self.handle.send(SolidifierEvent::CqlResult(Err(synced_ms))).ok();
        }
        Ok(())
    }
}

/// Solidifier worker
#[derive(Clone, Debug)]
pub struct AnalyzedMilestoneWorker<S, K, V>
where
    S: 'static + Insert<K, V>,
    K: 'static + Send,
    V: 'static + Send,
{
    handle: SolidifierHandle,
    milestone_index: u32,
    keyspace: S,
    key: K,
    value: V,
    retries: u8,
}

impl<S: Insert<K, V>, K, V> AnalyzedMilestoneWorker<S, K, V>
where
    S: 'static + Insert<K, V>,
    K: 'static + Send,
    V: 'static + Send,
{
    /// Create a new solidifier worker with a handle and retries
    pub fn new(handle: SolidifierHandle, milestone_index: u32, keyspace: S, key: K, value: V, retries: u8) -> Self {
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
        handle: SolidifierHandle,
        milestone_index: u32,
        keyspace: S,
        key: K,
        value: V,
        retries: u8,
    ) -> Box<Self> {
        Box::new(Self::new(handle, milestone_index, keyspace, key, value, retries))
    }
}

impl<S, K, V> Worker for AnalyzedMilestoneWorker<S, K, V>
where
    S: 'static + Insert<K, V> + Debug,
    K: 'static + Send + Clone + Debug + Sync + TokenEncoder,
    V: 'static + Send + Clone + Debug + Sync,
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
        reporter: Option<&ReporterHandle>,
    ) -> anyhow::Result<()> {
        error!("{:?}, left retries: {}", error, self.retries);
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
            match self
                .keyspace
                .insert_query(&self.key, &self.value)
                .consistency(Consistency::One)
                .build()
            {
                Ok(req) => {
                    req.send_global_with_worker(self);
                    // todo retry in loop if error
                }
                Err(e) => {
                    error!("{}", e);
                    let analyzed_ms = CqlResult::AnalyzedMilestone(self.milestone_index);
                    let _ = self.handle.send(SolidifierEvent::CqlResult(Err(analyzed_ms)));
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
