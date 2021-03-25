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
                SolidifierEvent::Shutdown => break,
            }
        }
        Ok(())
    }
}

impl Solidifier {
    fn close_message_id(&mut self, milestone_index: u32, message_id: &MessageId) {
        if let Some(milestone_data) = self.milestones_data.get_mut(&milestone_index) {
            // remove it from pending
            milestone_data.remove_from_pending(message_id);
            if Self::check_if_completed(milestone_data) {
                info!(
                    "Solidifier is pushing the milestone data for index: {}, to Logger",
                    milestone_index
                );
                // Remove milestoneData from self state and pass it to logger
                let ms_data = self.milestones_data.remove(&milestone_index).unwrap();
                let logger_event = LoggerEvent::MilestoneData(ms_data);
                let _ = self.logger_handle.send(logger_event);
            };
        } else {
            panic!("Not supposed to get close response on non-existing")
        }
    }
    fn handle_milestone_msg(
        &mut self,
        MilestoneMessage(_message_id, milestone_payload, message, metadata): MilestoneMessage,
    ) {
        let ms_index = milestone_payload.essence().index();
        let partitioner = &self.message_id_partitioner;
        let collector_handles = &self.collector_handles;
        let solidifier_id = self.partition_id;
        let ms_count = self.milestones_data.len();
        if let Some(milestone_data) = self.milestones_data.get_mut(&ms_index) {
            message.parents().iter().for_each(|parent_id| {
                let in_messages = milestone_data.messages().contains_key(&parent_id);
                let in_pending = milestone_data.pending().contains_key(&parent_id);
                // Check if parent NOT in messages nor pending
                if !in_messages && !in_pending {
                    // Request it from collector
                    let collector_id = partitioner.partition_id(parent_id);
                    if let Some(collector_handle) = collector_handles.get(&collector_id) {
                        let ask_event = CollectorEvent::Ask(Ask::FullMessage(solidifier_id, ms_index, *parent_id));
                        let _ = collector_handle.send(ask_event);
                    }
                    // Add it to pending
                    milestone_data.pending.insert(*parent_id, None);
                };
            });
            // insert milestone to milestone_data
            if let Some(metadata) = metadata {
                info!(
                    "SolidifierId: {}, got Full Milestone {}, left: {}",
                    self.partition_id, ms_index, ms_count
                );
                milestone_data.set_milestone(milestone_payload);
                milestone_data.remove_from_pending(&metadata.message_id);
                milestone_data.add_full_message(FullMessage::new(message, metadata));
                if Self::check_if_completed(milestone_data) {
                    info!(
                        "Solidifier is pushing the milestone data for index: {}, to Logger",
                        ms_index
                    );
                    // Remove milestoneData from self state and pass it to logger
                    let ms_data = self.milestones_data.remove(&ms_index).unwrap();
                    let logger_event = LoggerEvent::MilestoneData(ms_data);
                    let _ = self.logger_handle.send(logger_event);
                };
            }
        } else {
            // TODO sync
            info!("{} got milestone {}", solidifier_id, ms_index);
        }
    }
    fn handle_new_msg(&mut self, full_message: FullMessage) {
        // check what milestone_index referenced this message
        let ms_index = full_message.ref_ms();
        let message_id = full_message.message_id();
        // check if we already have active milestonedata for the ref_ms
        let milestone_data = self
            .milestones_data
            .entry(ms_index)
            .or_insert_with(|| MilestoneData::new(ms_index));
        // Ensure all parents exist in milestone_data
        // Note: Some or all parents might belong to older milestone,
        // and it's the job of the collector to tell us when to close message_id
        // and remove it from pending
        let partitioner = &self.message_id_partitioner;
        let collector_handles = &self.collector_handles;
        let solidifier_id = self.partition_id;
        full_message.metadata().parent_message_ids.iter().for_each(|parent_id| {
            let in_messages = milestone_data.messages().contains_key(&parent_id);
            let in_pending = milestone_data.pending().contains_key(&parent_id);
            // Check if parent NOT in messages nor pending
            if !in_messages && !in_pending {
                // Request it from collector
                let collector_id = partitioner.partition_id(parent_id);
                if let Some(collector_handle) = collector_handles.get(&collector_id) {
                    let ask_event = CollectorEvent::Ask(Ask::FullMessage(solidifier_id, ms_index, *parent_id));
                    let _ = collector_handle.send(ask_event);
                }
                // Add it to pending
                milestone_data.pending.insert(*parent_id, None);
            };
        });
        // remove it from the pending(if it does already exist)
        milestone_data.remove_from_pending(message_id);
        // Add full message
        milestone_data.add_full_message(full_message);
        if Self::check_if_completed(milestone_data) {
            info!(
                "Solidifier is pushing the milestone data for index: {}, to Logger",
                ms_index
            );
            // Remove milestoneData from self state and pass it to logger
            let ms_data = self.milestones_data.remove(&ms_index).unwrap();
            let logger_event = LoggerEvent::MilestoneData(ms_data);
            let _ = self.logger_handle.send(logger_event);
        };
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
            // TODO push to write ahead logger
            return true;
        } else if no_pending_left {
            warn!("Milestone: {}, doesn't exist yet", index);
            // TODO request milestone from collector
        }
        false
    }
}
