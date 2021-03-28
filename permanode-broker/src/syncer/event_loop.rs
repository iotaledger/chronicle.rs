// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;
#[async_trait::async_trait]
impl<H: PermanodeBrokerScope> EventLoop<BrokerHandle<H>> for Syncer {
    async fn event_loop(
        &mut self,
        _status: Result<(), Need>,
        _supervisor: &mut Option<BrokerHandle<H>>,
    ) -> Result<(), Need> {
        while let Some(event) = self.inbox.recv().await {
            match event {
                SyncerEvent::Process => {
                    self.sync_next();
                }
                SyncerEvent::Ask(ask) => {
                    // Don't accept ask events when there is something already in progress.
                    if let None = self.active {
                        match ask {
                            AskSyncer::Complete => {
                                self.complete();
                            }
                            AskSyncer::FillGaps => {
                                self.fill_gaps();
                            }
                            AskSyncer::UpdateSyncData => {
                                todo!("Updating the sync data is not implemented yet")
                            }
                        }
                    } else {
                        error!(
                            "Cannot accept Ask request: {:?}, while processing: {:?}",
                            &ask, self.active
                        );
                    }
                }
                SyncerEvent::MilestoneData(milestone_data) => {
                    // TODO force order of milestones_data
                    // and push them to logger
                    
                }
            }
        }
        Ok(())
    }
}

impl Syncer {
    pub(crate) fn sync_next(&mut self) {
        if let Some(ref mut active) = self.active {
            match active {
                Active::Complete(range) => {
                    for _ in 0..self.solidifier_count {
                        if let Some(milestone_index) = range.next() {
                            let solidifier_id = milestone_index % (self.solidifier_count as u32);
                        } else {
                            // Finished the current active range, therefore we drop it
                            self.active.take();
                            // move to next complete (if any)
                            self.complete();
                            // break the For loop for the curret
                            break;
                        }
                    }
                }
                Active::FillGaps(range) => {
                    for _ in 0..self.solidifier_count {
                        if let Some(milestone_index) = range.next() {
                            let solidifier_id = milestone_index % (self.solidifier_count as u32);
                        } else {
                            // Finished the current active range, therefore we drop it
                            self.active.take();
                            // move to next gap (if any)
                            self.fill_gaps();
                            // break the For loop for the curret
                            break;
                        }
                    }
                }
            }
        } else {
        }
    }
    pub(crate) fn complete(&mut self) {
        // start from the lowest uncomplete
        if let Some(gap) = self.sync_data.take_lowest_uncomplete() {
            // ensure gap.end != i32::MAX
            if !gap.end.eq(&(i32::MAX as u32)) {
                self.active.replace(Active::Complete(gap));
            } else {
                info!("Cannot complete futuristic gap: {:?}", gap);
            }
        } else {
            info!("There are no more gaps neither unlogged in the current sync data");
        }
    }
    pub(crate) fn fill_gaps(&mut self) {
        // start from the lowest gap
        if let Some(gap) = self.sync_data.take_lowest_gap() {
            // ensure gap.end != i32::MAX
            if !gap.end.eq(&(i32::MAX as u32)) {
                self.active.replace(Active::FillGaps(gap));
            } else {
                info!("Cannot fill futuristic gap: {:?}", gap);
            }
        } else {
            info!("There are no more gaps in the current sync data");
        }
    }
}
