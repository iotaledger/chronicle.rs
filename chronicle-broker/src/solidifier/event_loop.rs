// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;
use bee_message::prelude::MilestoneIndex;
use chronicle_common::Synckey;

#[async_trait::async_trait]
impl<H: ChronicleBrokerScope> EventLoop<BrokerHandle<H>> for Solidifier {
    async fn event_loop(
        &mut self,
        _status: Result<(), Need>,
        _supervisor: &mut Option<BrokerHandle<H>>,
    ) -> Result<(), Need> {
        self.service.update_status(ServiceStatus::Running);
        let event = BrokerEvent::Children(BrokerChild::Solidifier(self.service.clone(), _status));
        let _ = _supervisor
            .as_mut()
            .expect("Solidifier expected BrokerHandle")
            .send(event);
        while let Some(event) = self.inbox.recv().await {
            match event {
                SolidifierEvent::Message(full_message) => {
                    self.handle_new_msg(full_message).unwrap_or_else(|e| {
                        error!("{}", e);
                    });
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
                                            self.handle_in_database(milestone_index).unwrap_or_else(|e| {
                                                error!("{}", e);
                                            });
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
                            }
                            error!("Scylla cluster is likely having a complete outage, so we are shutting down broker for meantime.");
                            // Abort solidifier in order to let broker app reschedule itself after few mins
                            // with reasonable retries, it means our cluster is likely in outage or partial outage (ie
                            // all replicas for given token).
                            return Err(Need::Abort);
                        }
                    }
                }
                SolidifierEvent::Close(message_id, milestone_index) => {
                    self.close_message_id(milestone_index, &message_id).unwrap_or_else(|e| {
                        error!("{}", e);
                    });
                }
                SolidifierEvent::Milestone(milestone_message) => {
                    self.handle_milestone_msg(milestone_message).unwrap_or_else(|e| {
                        error!("{}", e);
                    });
                }
                SolidifierEvent::Solidify(milestone_index) => {
                    match milestone_index {
                        Ok(milestone_index) => {
                            // this is request to solidify this milestone
                            self.handle_solidify(milestone_index)
                        }
                        Err(milestone_index) => {
                            // This is response from collector(s) that we are unable to solidify
                            // this milestone_index
                            self.handle_solidify_failure(milestone_index);
                        }
                    }
                }
                SolidifierEvent::Shutdown => break,
            }
        }
        Ok(())
    }
}

impl Solidifier {
    fn handle_solidify_failure(&mut self, milestone_index: u32) {
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
                if ms_data.created_by.eq(&CreatedBy::Syncer) {
                    // tell syncer to skip it
                    warn!(
                        "Solidifier id: {}, failed to solidify syncer requested index: {} milestone data",
                        self.partition_id, milestone_index
                    );
                    let _ = self.syncer_handle.send(SyncerEvent::Unreachable(milestone_index));
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
    fn handle_solidify(&mut self, milestone_index: u32) {
        // open solidify requests only for less than the expected
        if milestone_index >= self.expected {
            warn!(
                "SolidifierId: {}, cannot open solidify request for milestone_index: {} >= expected: {}",
                self.partition_id, milestone_index, self.expected
            );
            // tell syncer to skip this atm
            let _ = self.syncer_handle.send(SyncerEvent::Unreachable(milestone_index));
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
            // NOTE: this likely will never happens
            warn!(
                "Received solidify request on an existing milestone data: index: {} created_by: {:?}, pending: {}, messages: {}, milestone_exist: {}, unless this is an expected race condition",
                milestone_index, ms_data.created_by(), ms_data.pending().len(), ms_data.messages().len(), ms_data.milestone_exist(),
            );
            // tell syncer to skip this atm
            let _ = self.syncer_handle.send(SyncerEvent::Unreachable(milestone_index));
        } else {
            // Asking any collector (as we don't know the message id of the milestone)
            // however, we use milestone_index % collector_count to have unfirom distribution.
            // note: solidifier_id/partition_id is actually = milestone_index % collector_count;
            // as both solidifiers and collectors have the same count.
            // this event should be enough to spark the solidification process
            let ask_collector = AskCollector::MilestoneMessage(milestone_index);
            if let Some(collector_handle) = self.collector_handles.get(&self.partition_id) {
                let ask_event = CollectorEvent::Ask(ask_collector);
                let _ = collector_handle.send(ask_event);
            }
            // insert empty entry
            self.milestones_data
                .insert(milestone_index, MilestoneData::new(milestone_index, CreatedBy::Syncer));
            // insert empty entry for in_database
            self.in_database
                .entry(milestone_index)
                .or_insert_with(|| InDatabase::new(milestone_index));
        }
    }
    fn close_message_id(&mut self, milestone_index: u32, message_id: &MessageId) -> anyhow::Result<()> {
        if let Some(milestone_data) = self.milestones_data.get_mut(&milestone_index) {
            // remove it from pending
            milestone_data.remove_from_pending(message_id);
            let check_if_completed = Self::check_if_completed(milestone_data);
            let created_by = milestone_data.created_by;
            if check_if_completed && !created_by.eq(&CreatedBy::Syncer) {
                self.push_to_logger(milestone_index)?;
            } else if check_if_completed {
                self.push_to_syncer(milestone_index)?;
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
    fn push_to_logger(&mut self, milestone_index: u32) -> anyhow::Result<()> {
        // Remove milestoneData from self state and pass it to archiver
        let milestone_data = self.milestones_data.remove(&milestone_index).unwrap();
        // Update in_database
        let in_database = self
            .in_database
            .entry(milestone_index)
            .or_insert_with(|| InDatabase::from(&milestone_data));
        in_database.set_messages_len(milestone_data.messages().len());
        if in_database.check_if_all_in_database() {
            // Insert record into sync table
            self.handle_in_database(milestone_index)?;
        }
        if let Some(archiver_handle) = self.archiver_handle.as_ref() {
            info!(
                "solidifier_id: {}, is pushing the milestone data for index: {}, to Logger",
                self.partition_id, milestone_index
            );
            let archiver_event = ArchiverEvent::MilestoneData(milestone_data, None);
            let _ = archiver_handle.send(archiver_event);
        };
        Ok(())
    }
    fn push_to_syncer(&mut self, milestone_index: u32) -> anyhow::Result<()> {
        info!(
            "Solidifier is pushing the milestone data for index: {}, to Syncer",
            milestone_index
        );
        // Remove milestoneData from self state and pass it to syncer
        let milestone_data = self.milestones_data.remove(&milestone_index).unwrap();
        // Update in_database
        let in_database = self
            .in_database
            .entry(milestone_index)
            .or_insert_with(|| InDatabase::from(&milestone_data));
        in_database.set_messages_len(milestone_data.messages().len());
        if in_database.check_if_all_in_database() {
            // Insert record into sync table
            self.handle_in_database(milestone_index)?;
        }
        let syncer_event = SyncerEvent::MilestoneData(milestone_data);
        let _ = self.syncer_handle.send(syncer_event);
        Ok(())
    }
    fn handle_in_database(&mut self, milestone_index: u32) -> anyhow::Result<()> {
        self.in_database.remove(&milestone_index);
        self.lru_in_database.put(milestone_index, ());
        let sync_key = Synckey;
        let synced_by = Some(self.chronicle_id);
        let synced_record = SyncRecord::new(MilestoneIndex(milestone_index), synced_by, None);
        let request = self
            .keyspace
            .insert(&sync_key, &synced_record)
            .consistency(Consistency::One)
            .build()?;
        let worker = SolidifierWorker::boxed(
            self.handle.clone(),
            milestone_index,
            self.keyspace.clone(),
            sync_key,
            synced_record,
            self.retries,
        );
        request.send_local(worker);
        Ok(())
    }
    fn handle_milestone_msg(
        &mut self,
        MilestoneMessage(_message_id, milestone_payload, message, metadata): MilestoneMessage,
    ) -> anyhow::Result<()> {
        let milestone_index = milestone_payload.essence().index().0;
        let partitioner = &self.message_id_partitioner;
        let collectors_handles = &self.collector_handles;
        let solidifier_id = self.partition_id;
        let ms_count = self.milestones_data.len();
        if let Some(milestone_data) = self.milestones_data.get_mut(&milestone_index) {
            Self::process_parents(
                message.parents(),
                milestone_data,
                collectors_handles,
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
                let check_if_completed = Self::check_if_completed(milestone_data);
                let created_by = milestone_data.created_by;
                if check_if_completed && !created_by.eq(&CreatedBy::Syncer) {
                    self.push_to_logger(milestone_index)?;
                } else if check_if_completed {
                    self.push_to_syncer(milestone_index)?;
                };
            }
        } else {
            if let Some(metadata) = metadata {
                // We have to decide whether to insert entry for milestone_index or not.
                self.insert_new_entry_or_not(milestone_index, FullMessage::new(message, metadata))
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
            let in_pending = milestone_data.pending().contains_key(&parent_id);
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
                milestone_data.pending.insert(*parent_id, ());
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
            let _ = collector_handle.send(ask_event);
        }
    }
    fn request_milestone_message(
        collectors_handles: &HashMap<u8, CollectorHandle>,
        collector_id: u8,
        milestone_index: u32,
    ) {
        if let Some(collector_handle) = collectors_handles.get(&collector_id) {
            let ask_event = CollectorEvent::Ask(AskCollector::MilestoneMessage(milestone_index));
            let _ = collector_handle.send(ask_event);
        }
    }
    fn handle_new_msg(&mut self, full_message: FullMessage) -> anyhow::Result<()> {
        // check what milestone_index referenced this message
        let milestone_index = full_message.ref_ms().unwrap();
        let partitioner = &self.message_id_partitioner;
        let collector_handles = &self.collector_handles;
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
            let check_if_completed = Self::check_if_completed(milestone_data);
            let created_by = milestone_data.created_by;
            if check_if_completed && !created_by.eq(&CreatedBy::Syncer) {
                self.push_to_logger(milestone_index)?;
            } else if check_if_completed {
                self.push_to_syncer(milestone_index)?;
            };
        } else {
            // We have to decide whether to insert entry for milestone_index or not.
            self.insert_new_entry_or_not(milestone_index, full_message)
        }
        Ok(())
    }
    fn insert_new_entry_or_not(&mut self, milestone_index: u32, full_message: FullMessage) {
        // do not insert new entry for unreachable milestone_index atm
        // this happens when we get one or few solidify_failures so we deleted an active milestone_data that still
        // getting new messages which will reinvoke insert_new_entry_or_not
        if self.unreachable.get(&milestone_index).is_some() {
            return ();
        }
        let partitioner = &self.message_id_partitioner;
        let collector_handles = &self.collector_handles;
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

                Self::request_milestone_message(collector_handles, solidifier_id, milestone_index);
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
                );
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
            );
            // No need to check if it's completed.
            // still for safety reasons, we should ask collector for its milestone,
            // but we are going to let syncer sends us an event when it observes a glitch

            // Insert anything in between(belongs to self solidifier_id) as Expected
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
                            "solidifier_id: {}, however it will request the expected index: {} milestone",
                            id, expected
                        );
                        // For safety reasons, we ask collector for expected milestone
                        Self::request_milestone_message(collector_handles, solidifier_id, expected)
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
    fn check_if_completed(milestone_data: &mut MilestoneData) -> bool {
        // Check if there are no pending at all to set complete to true
        let index = milestone_data.milestone_index();
        let no_pending_left = milestone_data.pending().is_empty();
        let milestone_exist = milestone_data.milestone_exist();
        if no_pending_left && milestone_exist {
            // milestone data is complete now
            info!(
                "{} is solid, with total messages: {}",
                index,
                milestone_data.messages().len()
            );
            return true;
        }
        false
    }
}
