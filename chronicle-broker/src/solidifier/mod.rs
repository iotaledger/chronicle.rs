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
    filter::{
        FilterBuilder,
        FilterHandle,
    },
    syncer::{
        SyncerEvent,
        SyncerHandle,
    },
    *,
};
use backstage::core::{
    Actor,
    ActorError,
    ActorResult,
    Rt,
    ShutdownEvent,
    SupHandle,
    UnboundedChannel,
    UnboundedHandle,
};
use chronicle_common::types::{
    CreatedBy,
    MessageId,
    MessageRecord,
    MilestoneDataBuilder,
    MilestoneMessage,
    Selected,
};
use futures::stream::StreamExt;
use std::fmt::Debug;

/// The solidifier handle type
pub type SolidifierHandle = UnboundedHandle<SolidifierEvent>;

/// Solidifier events
#[derive(Debug)]
pub enum SolidifierEvent {
    /// Milestone fullmessage;
    Milestone(MilestoneMessage, Option<Selected>),
    /// Pushed or requested messages, that definitely belong to self solidifier, and flag whether it's selected or not.
    Message(MessageRecord, Option<Selected>),
    /// Close MessageId that doesn't belong at all to Solidifier of milestone u32
    Close(MessageId, u32),
    /// Solidifiy request from Syncer.
    /// Solidifier should collect milestonedata and pass it to Syncer(not archiver)
    Solidify(Result<u32, u32>),
    /// Shutdown the solidifier
    Shutdown,
}

impl ShutdownEvent for SolidifierEvent {
    fn shutdown_event() -> Self {
        Self::Shutdown
    }
}

/// Solidifier state, each Solidifier solidifiy subset of (milestones_index % solidifier_count == partition_id)
pub struct Solidifier<T: FilterBuilder> {
    partition_id: u8,
    milestones_data: HashMap<u32, MilestoneDataBuilder>,
    message_id_partitioner: MessageIdPartitioner,
    expected: u32,
    filter_handle: T::Handle,
}

impl<T: FilterBuilder> Solidifier<T> {
    pub(super) fn new(partition_id: u8, partitioner: MessageIdPartitioner, filter_handle: T::Handle) -> Self {
        Self {
            partition_id,
            milestones_data: HashMap::new(),
            message_id_partitioner: partitioner,
            expected: 1,
            filter_handle,
        }
    }
}

//////////////////////// Actor impl ////////////////////////////

#[async_trait]
impl<S: SupHandle<Self>, T: FilterBuilder> Actor<S> for Solidifier<T> {
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
                SolidifierEvent::Message(full_message, selected) => {
                    self.handle_new_msg(rt, full_message, selected, &collector_handles, &archiver, &syncer)
                        .await
                        .map_err(|e| {
                            error!("{}", e);
                            e
                        })?;
                }
                SolidifierEvent::Close(message_id, milestone_index) => {
                    self.close_message_id(rt, milestone_index, message_id, &archiver, &syncer)
                        .await
                        .map_err(|e| {
                            error!("{}", e);
                            e
                        })?;
                }
                SolidifierEvent::Milestone(milestone_message, selected) => {
                    self.handle_milestone_msg(rt, milestone_message, selected, &collector_handles, &archiver, &syncer)
                        .await
                        .map_err(|e| {
                            error!("{}", e);
                            e
                        })?;
                }
                SolidifierEvent::Solidify(milestone_res) => {
                    match milestone_res {
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

impl<T: FilterBuilder> Solidifier<T> {
    fn partition_count(&self) -> u8 {
        self.message_id_partitioner.partition_count()
    }
    fn handle_solidify_failure(&mut self, milestone_index: u32, syncer_handle: &SyncerHandle) {
        error!(
            "Solidifier id: {}. was unable to solidify milestone_index: {}",
            self.partition_id, milestone_index
        );
        // remove its milestone_data
        if let Some(ms_data) = self.milestones_data.remove(&milestone_index) {
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
                milestone_index, created_by, ms_data.pending().len(), ms_data.messages().len(), ms_data.milestone().is_some(),
            );
            if created_by == &CreatedBy::Expected && ms_data.milestone().is_none() {
                // convert the ownership to syncer
                ms_data.set_created_by(CreatedBy::Syncer);
                // request milestone in order to respark solidification process
                Self::request_milestone_message(collector_handles, self.partition_id, milestone_index);
            } else {
                self.milestones_data.remove(&milestone_index);
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
        }
    }
    async fn close_message_id<S: SupHandle<Self>>(
        &mut self,
        rt: &mut Rt<Self, S>,
        milestone_index: u32,
        message_id: MessageId,
        archiver_handle: &Option<ArchiverHandle>,
        syncer_handle: &SyncerHandle,
    ) -> ActorResult<()> {
        if let Some(milestone_data) = self.milestones_data.get_mut(&milestone_index) {
            // remove it from pending
            milestone_data.remove_pending(message_id);
            let ms_data_valid = milestone_data.valid();
            let created_by = milestone_data.created_by();
            if ms_data_valid && !created_by.eq(&CreatedBy::Syncer) {
                self.push_to_archiver(rt, milestone_index, archiver_handle).await?;
            } else if ms_data_valid {
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
    async fn push_to_archiver<S: SupHandle<Self>>(
        &mut self,
        rt: &mut Rt<Self, S>,
        milestone_index: u32,
        archive_handle: &Option<ArchiverHandle>,
    ) -> ActorResult<()> {
        // Remove milestoneData from self state and pass it to archiver
        let milestone_data = self
            .milestones_data
            .remove(&milestone_index)
            .ok_or_else(|| anyhow!("Expected milestone data for milestone_index"))?;
        let created_by = *milestone_data.created_by();
        // uda process
        let milestone_index = milestone_data.milestone_index();
        let milestone_data = self.filter_handle.process_milestone_data(milestone_data).await?;
        self.filter_handle.synced(milestone_index.into()).await?;
        if let Some(archiver_handle) = archive_handle.as_ref() {
            info!(
                "solidifier_id: {}, is pushing the milestone data for index: {}, to Archiver",
                self.partition_id, milestone_index
            );
            let archiver_event = ArchiverEvent::MilestoneData(milestone_data, created_by, None);
            archiver_handle.send(archiver_event).ok();
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
            .ok_or_else(|| anyhow!("Expected milestone data for milestone_index"))?;
        // uda process
        let milestone_data = self.filter_handle.process_milestone_data(milestone_data).await?;
        info!(
            "solidifier_id: {}, is pushing the milestone data for index: {}, to Syncer",
            self.partition_id, milestone_index
        );
        self.filter_handle.synced(milestone_index.into()).await?;
        let syncer_event = SyncerEvent::MilestoneData(milestone_data);
        syncer_handle.send(syncer_event).ok();
        Ok(())
    }
    async fn handle_milestone_msg<S: SupHandle<Self>>(
        &mut self,
        rt: &mut Rt<Self, S>,
        milestone_message: MilestoneMessage,
        selected: Option<Selected>,
        collector_handles: &HashMap<u8, CollectorHandle>,
        archiver_handle: &Option<ArchiverHandle>,
        syncer_handle: &SyncerHandle,
    ) -> anyhow::Result<()> {
        let milestone_index = milestone_message.milestone_index().0;
        let partitioner = &self.message_id_partitioner;
        let solidifier_id = self.partition_id;
        let ms_count = self.milestones_data.len();
        if let Some(milestone_data) = self.milestones_data.get_mut(&milestone_index) {
            Self::process_parents(
                milestone_message.message().parents(),
                milestone_data,
                collector_handles,
                partitioner,
                solidifier_id,
                milestone_index,
            );
            // insert milestone into milestone_data
            info!(
                "solidifier_id: {}, got milestone {}, in progress: {}",
                self.partition_id, milestone_index, ms_count
            );
            let message = milestone_message.message().clone();
            milestone_data.set_milestone(milestone_message);
            milestone_data.remove_pending(message.message_id);
            milestone_data.add_message(message, selected);
            let ms_data_valid = milestone_data.valid();
            let created_by = milestone_data.created_by();
            if ms_data_valid && !created_by.eq(&CreatedBy::Syncer) {
                self.push_to_archiver(rt, milestone_index, archiver_handle).await?;
            } else if ms_data_valid {
                self.push_to_syncer(rt, milestone_index, syncer_handle).await?;
            };
        } else {
            let message = milestone_message.message().clone();
            // We have to decide whether to insert entry for milestone_index or not.
            self.insert_new_entry_or_not(
                rt,
                milestone_index,
                Some(milestone_message),
                message,
                selected,
                collector_handles,
                archiver_handle,
                syncer_handle,
            )
            .await?
        }
        Ok(())
    }
    fn process_parents(
        parents: impl Iterator<Item = MessageId>,
        milestone_data: &mut MilestoneDataBuilder,
        collectors_handles: &HashMap<u8, CollectorHandle>,
        partitioner: &MessageIdPartitioner,
        solidifier_id: u8,
        milestone_index: u32,
    ) {
        // Ensure all parents exist in milestone_data
        // Note: Some or all parents might belong to older milestone,
        // and it's the job of the collector to tell us when to close message_id
        // and remove it from pending
        parents.for_each(|parent_id| {
            let in_messages = milestone_data.messages().contains_key(&parent_id);
            let in_pending = milestone_data.pending().contains(&parent_id);
            let genesis = parent_id.is_null();
            // Check if parent NOT in messages nor pending
            if !in_messages && !in_pending && !genesis {
                // Request it from collector
                Self::request_full_message(
                    collectors_handles,
                    partitioner,
                    solidifier_id,
                    milestone_index,
                    parent_id,
                    *milestone_data.created_by(),
                );
                // Add it to pending
                milestone_data.add_pending(parent_id);
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
        message: MessageRecord,
        selected: Option<Selected>,
        collector_handles: &HashMap<u8, CollectorHandle>,
        archiver_handle: &Option<ArchiverHandle>,
        syncer_handle: &SyncerHandle,
    ) -> ActorResult<()> {
        // check what milestone_index referenced this message
        let milestone_index = message.milestone_index().unwrap();
        let partitioner = &self.message_id_partitioner;
        let solidifier_id = self.partition_id;
        if let Some(milestone_data) = self.milestones_data.get_mut(&milestone_index) {
            Self::process_milestone_data(
                solidifier_id,
                collector_handles,
                milestone_data,
                partitioner,
                milestone_index,
                message,
                selected,
            );
            let ms_data_valid = milestone_data.valid();
            let created_by = milestone_data.created_by();
            if ms_data_valid && !created_by.eq(&CreatedBy::Syncer) {
                self.push_to_archiver(rt, milestone_index, archiver_handle).await?;
            } else if ms_data_valid {
                self.push_to_syncer(rt, milestone_index, syncer_handle).await?;
            };
        } else {
            // We have to decide whether to insert entry for milestone_index or not.
            self.insert_new_entry_or_not(
                rt,
                milestone_index,
                None,
                message,
                selected,
                collector_handles,
                archiver_handle,
                syncer_handle,
            )
            .await?
        }
        Ok(())
    }
    async fn insert_new_entry_or_not<S: SupHandle<Self>>(
        &mut self,
        rt: &mut Rt<Self, S>,
        milestone_index: u32,
        mut milestone_message: Option<MilestoneMessage>,
        message: MessageRecord,
        selected: Option<Selected>,
        collector_handles: &HashMap<u8, CollectorHandle>,
        archiver_handle: &Option<ArchiverHandle>,
        syncer_handle: &SyncerHandle,
    ) -> ActorResult<()> {
        // do not insert new entry for unreachable milestone_index atm
        // this happens when we get one or few solidify_failures so we deleted an active milestone_data that still
        // getting new messages which will reinvoke insert_new_entry_or_not
        let partitioner = &self.message_id_partitioner;
        let solidifier_id = self.partition_id;
        if milestone_index >= self.expected {
            // Insert it as new incoming entry
            let milestone_data = self
                .milestones_data
                .entry(milestone_index)
                .or_insert_with(|| MilestoneDataBuilder::new(milestone_index, CreatedBy::Incoming));
            // check if the message is milestone
            if let Some(milestone_message) = milestone_message.take() {
                milestone_data.set_milestone(milestone_message);
            }
            Self::process_milestone_data(
                solidifier_id,
                collector_handles,
                milestone_data,
                partitioner,
                milestone_index,
                message,
                selected,
            );
            // the milestone data might be completed/valid in-case it's the only message in the whole milestone data
            let ms_data_valid = milestone_data.valid();
            let created_by = milestone_data.created_by();
            if ms_data_valid && !created_by.eq(&CreatedBy::Syncer) {
                self.push_to_archiver(rt, milestone_index, archiver_handle).await?;
            } else if ms_data_valid {
                self.push_to_syncer(rt, milestone_index, syncer_handle).await?;
            };
            // Insert entries for anything in between(belongs to self solidifier_id) as Expected,
            // Only insert expected entry when the gap len is reasonable (ie 4)
            // todo maybe make it configurable
            // note, this likely will never be invoked partition count greater than 1;
            if (milestone_index - self.expected) < 4 {
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
                            .or_insert_with(|| MilestoneDataBuilder::new(expected, CreatedBy::Expected));
                        if milestone_data.milestone().is_none() {
                            error!(
                                "solidifier_id: {}, however syncer will fill the expected index: {} milestone",
                                id, expected
                            );
                        }
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
        milestone_data: &mut MilestoneDataBuilder,
        partitioner: &MessageIdPartitioner,
        ms_index: u32,
        message: MessageRecord,
        selected: Option<Selected>,
    ) {
        Self::process_parents(
            message.parents(),
            milestone_data,
            collector_handles,
            partitioner,
            solidifier_id,
            ms_index,
        );
        // remove it from the pending(if it does already exist)
        milestone_data.remove_pending(message.message_id);
        // Add full message
        milestone_data.add_message(message, selected);
    }
}
