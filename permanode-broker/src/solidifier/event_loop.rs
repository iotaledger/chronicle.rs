// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[async_trait::async_trait]
impl<H: PermanodeBrokerScope> EventLoop<BrokerHandle<H>> for Solidifier {
    async fn event_loop(
        &mut self,
        _status: Result<(), Need>,
        _supervisor: &mut Option<BrokerHandle<H>>,
    ) -> Result<(), Need> {
        while let Some(event) = self.inbox.recv().await {
            match event {
                SolidifierEvent::Message(full_message) => {
                    self.handle_new_msg(full_message);
                }
                SolidifierEvent::Close(message_id, milestone_index) => {
                    self.close_message_id(milestone_index, &message_id);
                }
                SolidifierEvent::Milestone(milestone_message) => {
                    self.handle_milestone_msg(milestone_message);
                }
                SolidifierEvent::Solidifiy(milestone_index) => self.handle_solidifiy(milestone_index),
                SolidifierEvent::Shutdown => break,
            }
        }
        Ok(())
    }
}

impl Solidifier {
    fn handle_solidifiy(&mut self, milestone_index: u32) {
        // this is request from syncer in order for solidifier to collect,
        // the milestone data for the provided milestone index.
        if let None = self.milestones_data.get_mut(&milestone_index) {
            // Asking any collector (as we don't know the message id of the milestone)
            // however, we use milestone_index % collectors_count to have unfirom distribution.
            // note: solidifier_id/partition_id is actually = milestone_index % collectors_count;
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
        } else {
            error!("Not supposed to get solidifiy request on existing milestone data")
        }
    }
    fn close_message_id(&mut self, milestone_index: u32, message_id: &MessageId) {
        if let Some(milestone_data) = self.milestones_data.get_mut(&milestone_index) {
            // remove it from pending
            milestone_data.remove_from_pending(message_id);
            let check_if_completed = Self::check_if_completed(milestone_data);
            let created_by = milestone_data.created_by;
            if check_if_completed && !created_by.eq(&CreatedBy::Syncer) {
                self.push_to_logger(milestone_index);
                // TODO inform syncer
            } else if check_if_completed {
                self.push_to_syncer(milestone_index);
            };
        } else {
            error!("Not supposed to get close response on non-existing milestone data")
        }
    }
    fn push_to_logger(&mut self, milestone_index: u32) {
        info!(
            "solidifier_id: {}, is  pushing the milestone data for index: {}, to Logger",
            self.partition_id, milestone_index
        );
        // Remove milestoneData from self state and pass it to logger
        let ms_data = self.milestones_data.remove(&milestone_index).unwrap();
        let logger_event = LoggerEvent::MilestoneData(ms_data);
        let _ = self.logger_handle.send(logger_event);
    }
    fn push_to_syncer(&mut self, milestone_index: u32) {
        info!(
            "Solidifier is pushing the milestone data for index: {}, to Syncer",
            milestone_index
        );
        // Remove milestoneData from self state and pass it to syncer
        let ms_data = self.milestones_data.remove(&milestone_index).unwrap();
        let syncer_event = SyncerEvent::MilestoneData(ms_data);
        let _ = self.syncer_handle.send(syncer_event);
    }
    fn handle_milestone_msg(
        &mut self,
        MilestoneMessage(_message_id, milestone_payload, message, metadata): MilestoneMessage,
    ) {
        let milestone_index = milestone_payload.essence().index();
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
            // insert milestone to milestone_data
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
                    self.push_to_logger(milestone_index);
                } else if check_if_completed {
                    self.push_to_syncer(milestone_index);
                };
            }
        } else {
            if let Some(metadata) = metadata {
                // We have to decide whether to insert entry for milestone_index or not.
                self.insert_new_entry_or_not(milestone_index, FullMessage::new(message, metadata))
            }
        }
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
            // Check if parent NOT in messages nor pending
            if !in_messages && !in_pending {
                // Request it from collector
                Self::request_full_message(
                    collectors_handles,
                    partitioner,
                    solidifier_id,
                    milestone_index,
                    *parent_id,
                );
                // Add it to pending
                milestone_data.pending.insert(*parent_id, None);
            };
        });
    }
    fn request_full_message(
        collectors_handles: &HashMap<u8, CollectorHandle>,
        partitioner: &MessageIdPartitioner,
        solidifier_id: u8,
        milestone_index: u32,
        parent_id: MessageId,
    ) {
        // Request it from collector
        let collector_id = partitioner.partition_id(&parent_id);
        if let Some(collector_handle) = collectors_handles.get(&collector_id) {
            let ask_event = CollectorEvent::Ask(AskCollector::FullMessage(solidifier_id, milestone_index, parent_id));
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
    fn handle_new_msg(&mut self, full_message: FullMessage) {
        // check what milestone_index referenced this message
        let milestone_index = full_message.ref_ms();
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
                self.push_to_logger(milestone_index);
            } else if check_if_completed {
                self.push_to_syncer(milestone_index);
            };
        } else {
            // We have to decide whether to insert entry for milestone_index or not.
            self.insert_new_entry_or_not(milestone_index, full_message)
        }
    }
    fn insert_new_entry_or_not(&mut self, milestone_index: u32, full_message: FullMessage) {
        let partitioner = &self.message_id_partitioner;
        let collector_handles = &self.collector_handles;
        let solidifier_id = self.partition_id;
        // Check if this is the first observed message
        if self.first.is_none() {
            // Ensure to proceed only if ms_index >= provided static gap lower bound from syncer.
            if milestone_index >= self.gap_start {
                // Set it as static bound.
                self.first.replace(milestone_index);
                // TODO Tell syncer about this bound.
                info!("solidifier id: {:?}, observed its first is: {}",solidifier_id, milestone_index);
                // For safety reasons, we ask collector for this milestone,
                // as it's the first observed message/milestone which we want to ensure it's solid.

                Self::request_milestone_message(collector_handles, solidifier_id, milestone_index);
                self.expected = milestone_index + (self.collectors_count as u32);

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
                let id = (expected % self.collectors_count as u32) as u8;
                if id.eq(&self.partition_id) {
                    // Insert it as new expected entry, only if we don't already have an existing entry for it
                    let milestone_data = self
                        .milestones_data
                        .entry(expected)
                        .or_insert_with(|| MilestoneData::new(expected, CreatedBy::Expected));
                    if !milestone_data.milestone_exist() {
                        // For safety reasons, we ask collector for expected milestone, as
                        Self::request_milestone_message(collector_handles, solidifier_id, milestone_index)
                    }
                }
            }
            // set it as recent expected
            self.expected = milestone_index + (self.collectors_count as u32);
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
            milestone_data.set_completed();
            info!("{} is solid", index);
            return true;
        } else if no_pending_left {
            warn!("Milestone: {}, doesn't exist yet", index);
        }
        false
    }
}
