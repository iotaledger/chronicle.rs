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
                SolidifierEvent::Tbd(ms, message_id, message) => {
                    // info!("got tbd {:?}", message);
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
        let milestone_data = self
            .milestones_data
            .get_mut(&milestone_index)
            .expect("Not existing MilestoneData");
        // remove it from pending
        milestone_data.remove_from_pending(message_id);
        Self::check_if_completed(milestone_data);
    }
    fn handle_milestone_msg(&mut self, milestone_message: MilestoneMessage) {
        let milestone_payload = milestone_message.1;
        let message = milestone_message.2;
        let metadata = milestone_message.3;
        let ms_index = milestone_payload.essence().index();
        let partitioner = &self.message_id_partitioner;
        let collector_handles = &self.collector_handles;
        let solidifier_id = self.partition_id;
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
                info!("Got Full Milestone {}", ms_index);
                milestone_data.set_milestone(milestone_payload);
                milestone_data.add_full_message(FullMessage::new(message, metadata));
                Self::check_if_completed(milestone_data);
            }
        }
    }
    fn handle_new_msg(&mut self, full_message: FullMessage) {
        // check what milestone_index referenced this message
        let ref_ms = full_message.ref_ms();
        let message_id = full_message.message_id();
        // check if we already have active milestonedata for the ref_ms
        if let Some(milestone_data) = self.milestones_data.get_mut(&ref_ms) {
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
                        let ask_event = CollectorEvent::Ask(Ask::FullMessage(solidifier_id, ref_ms, *parent_id));
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
            Self::check_if_completed(milestone_data);
        } else {
            // create new milestone_data
            let mut milestone_data = MilestoneData::new(ref_ms);
            // Add all parents to pending map
            full_message.metadata().parent_message_ids.iter().for_each(|parent_id| {
                // Request it from the right collector
                let collector_id = self.message_id_partitioner.partition_id(parent_id);
                if let Some(collector_handle) = self.collector_handles.get(&collector_id) {
                    let ask_event = CollectorEvent::Ask(Ask::FullMessage(self.partition_id, ref_ms, *parent_id));
                    let _ = collector_handle.send(ask_event);
                }
                // Add it to pending
                milestone_data.pending.insert(*parent_id, None);
            });
            // Add full message
            milestone_data.add_full_message(full_message);
            // Insert new milestonedata
            info!("Solidifier with id {}, solidifying milestone: {}", self.partition_id, ref_ms);
            self.milestones_data.insert(ref_ms, milestone_data);
        };
    }
}

impl Solidifier {
    fn check_if_completed(milestone_data: &mut MilestoneData) {
        // Check if there are no pending at all to set complete to true
        let index = milestone_data.milestone_index();
        let no_pending_left = milestone_data.pending().is_empty();
        let milestone_exist = milestone_data.milestone_exist();
        if no_pending_left && milestone_exist {
            // milestone data is complete now
            milestone_data.set_completed();
            info!("{} is solid", index);
            // TODO push to write ahead logger
        } else if no_pending_left {
            warn!("Milestone: {}, doesn't exist yet", index);
            // TODO request milestone from collector
        }
    }
}
