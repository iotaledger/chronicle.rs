// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;
use chronicle_common::alert;

#[async_trait::async_trait]
impl<H: ChronicleBrokerScope> EventLoop<BrokerHandle<H>> for Syncer {
    async fn event_loop(
        &mut self,
        _status: Result<(), Need>,
        _supervisor: &mut Option<BrokerHandle<H>>,
    ) -> Result<(), Need> {
        self.service.update_status(ServiceStatus::Running);
        let event = BrokerEvent::Children(BrokerChild::Syncer(self.service.clone(), _status));
        let _ = _supervisor.as_mut().expect("Syncer expected BrokerHandle").send(event);
        while let Some(event) = self.inbox.recv().await {
            match event {
                SyncerEvent::Ask(ask) => {
                    // Don't accept ask events when there is something already in progress.
                    if let None = self.active {
                        match ask {
                            AskSyncer::Complete => {
                                if !self.highest.eq(&0) {
                                    self.complete();
                                } else {
                                    self.first_ask.replace(ask);
                                }
                            }
                            AskSyncer::FillGaps => {
                                if !self.highest.eq(&0) {
                                    self.fill_gaps();
                                } else {
                                    self.first_ask.replace(ask);
                                }
                            }
                            AskSyncer::UpdateSyncData => {
                                info!("Trying to update the sync data");
                                self.update_sync().await;
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
                    self.handle_milestone_data(milestone_data).await;
                }
                SyncerEvent::Unreachable(milestone_index) => {
                    self.pending -= 1;
                    // This happens when all the peers don't have the requested milestone_index
                    alert!(
                        "Chronicle syncer is unable to reach milestone index {} because no peers were able to provide it!",
                        milestone_index
                    );
                    self.handle_skip();
                    self.trigger_process_more();
                }
                SyncerEvent::Shutdown => break,
            }
        }
        Ok(())
    }
}

impl Syncer {
    async fn update_sync(&mut self) {
        if self.eof {
            if let Some(sync_range) = self.sync_range.as_ref() {
                // try to fetch and update sync_data
                if let Ok(sync_data) = SyncData::try_fetch(&self.keyspace, sync_range, 10).await {
                    info!("Updated the sync data");
                    self.sync_data = sync_data;
                    self.eof = false;
                    self.complete_or_fillgaps();
                } else {
                    self.schedule_update_sync_data();
                }
            }
        }
    }

    pub(crate) async fn handle_milestone_data(&mut self, milestone_data: MilestoneData) {
        self.pending -= 1;
        self.milestones_data.push(Ascending::new(milestone_data));
        if self.highest.eq(&0) && self.pending.eq(&0) {
            // these are the first milestones data, which we didn't even request it.
            let milestone_data = self.milestones_data.pop().unwrap().into_inner();
            self.highest = milestone_data.milestone_index();
            let mut next = self.highest + 1;
            // push it to archiver
            self.try_send_to_archiver(ArchiverEvent::MilestoneData(milestone_data, None));
            // push the rest
            while let Some(ms_data) = self.milestones_data.pop() {
                let milestone_data = ms_data.into_inner();
                let ms_index = milestone_data.milestone_index();
                if next != ms_index {
                    self.try_send_to_archiver(ArchiverEvent::Close(next));
                    // identify self.highest as glitch.
                    // eventually we will fill up this glitch
                    warn!(
                        "Noticed a glitch: {}..{} in the first observed milestones data",
                        self.highest + 1,
                        ms_index,
                    );
                    // we update our highest to be the ms_index which caused the glitch
                    // this enable us later to solidify the last gap up to this ms.
                    self.highest = ms_index;
                }
                next = ms_index + 1;
                // push it to archiver
                self.try_send_to_archiver(ArchiverEvent::MilestoneData(milestone_data, None));
            }
            // push the start point to archiver
            let _ = self.oneshot.take().expect("Expected oneshot channel").send(next);
            // tell archiver to finish the logfile
            let _ = self.try_send_to_archiver(ArchiverEvent::Close(next));
            // set the first ask request
            self.complete_or_fillgaps();
        } else if !self.highest.eq(&0) && !self.skip {
            self.try_solidify_one_more();
            let upper_ms_limit = Some(self.initial_gap_end);
            // check if we could send the next expected milestone_index
            while let Some(ms_data) = self.milestones_data.pop() {
                let ms_index = ms_data.milestone_index();
                if self.next.eq(&ms_index) {
                    // push it to archiver
                    let _ =
                        self.try_send_to_archiver(ArchiverEvent::MilestoneData(ms_data.into_inner(), upper_ms_limit));
                    self.next += 1;
                } else {
                    // put it back and then break
                    self.milestones_data.push(ms_data);
                    break;
                }
            }
        } else if self.skip {
            self.handle_skip();
            // close the current file
        }
        // check if pending is zero which is an indicator that all milestones_data
        // has been processed, in order to move further
        self.trigger_process_more();
    }
    pub(crate) fn handle_skip(&mut self) {
        self.skip = true;
        // we should skip/drop the current active slot but only when pending == 0
        if self.pending.eq(&0) {
            while let Some(d) = self.milestones_data.pop() {
                let d = d.into_inner();
                error!("We got milestone data for index: {}, but we're skipping it due to previous unreachable indexex within the same gap range", d.milestone_index());
            }
            match self.active.as_mut().unwrap() {
                Active::Complete(ref mut range) => {
                    error!("Complete: Skipping the remaining gap range: {:?}", range);
                    // we just consume the range in order for the trigger_process_more to move further
                    while let Some(_) = range.next() {}
                }
                Active::FillGaps(ref mut range) => {
                    error!("FillGaps: Skipping the remaining gap range: {:?}", range);
                    // we just consume the range in order for the trigger_process_more to move further
                    while let Some(_) = range.next() {}
                }
            };
            // reset skip back to false
            self.skip = false;
        }
    }
    fn complete_or_fillgaps(&mut self) {
        match self.first_ask.as_ref() {
            Some(AskSyncer::Complete) => {
                self.complete();
            }
            Some(AskSyncer::FillGaps) => self.fill_gaps(),
            _ => {}
        }
    }
    fn try_send_to_archiver(&self, archiver_event: ArchiverEvent) {
        if let Some(archiver_handle) = self.archiver_handle.as_ref() {
            let _ = archiver_handle.send(archiver_event);
        }
    }
    fn close_log_file(&mut self) {
        let created_log_file = self.initial_gap_start != self.next;
        if self.prev_closed_log_filename != self.initial_gap_start && created_log_file {
            if let Some(archiver_handle) = self.archiver_handle.as_ref() {
                info!(
                    "Informing Archiver to close {}.part, and should be renamed to: {}to{}.log",
                    self.initial_gap_start, self.initial_gap_start, self.next
                );
                // We should close any part file related to the current gap
                let _ = archiver_handle.send(ArchiverEvent::Close(self.next));
            };
            self.prev_closed_log_filename = self.initial_gap_start;
        } else {
            self.prev_closed_log_filename = 0;
        }
    }
    fn try_solidify_one_more(&mut self) {
        match self.active.as_mut().unwrap() {
            Active::Complete(ref mut range) => {
                if let Some(milestone_index) = range.next() {
                    Self::request_solidify(self.solidifier_count, &self.solidifier_handles, milestone_index);
                    self.pending += 1;
                }
            }
            Active::FillGaps(ref mut range) => {
                if let Some(milestone_index) = range.next() {
                    Self::request_solidify(self.solidifier_count, &self.solidifier_handles, milestone_index);
                    self.pending += 1;
                }
            }
        };
    }
    pub(crate) fn process_more(&mut self) {
        if let Some(ref mut active) = self.active {
            match active {
                Active::Complete(range) => {
                    for _ in 0..self.parallelism {
                        if let Some(milestone_index) = range.next() {
                            Self::request_solidify(self.solidifier_count, &self.solidifier_handles, milestone_index);
                            // update pending
                            self.pending += 1;
                        } else {
                            // move to next gap (only if pending is zero)
                            if self.pending.eq(&0) {
                                // We should close any part file related to the current(above finished range) gap
                                self.close_log_file();
                                // Finished the current active range, therefore we drop it
                                self.active.take();
                                self.complete();
                            }
                            break;
                        }
                    }
                }
                Active::FillGaps(range) => {
                    for _ in 0..self.parallelism {
                        if let Some(milestone_index) = range.next() {
                            Self::request_solidify(self.solidifier_count, &self.solidifier_handles, milestone_index);
                            // update pending
                            self.pending += 1;
                        } else {
                            // move to next gap (only if pending is zero)
                            if self.pending.eq(&0) {
                                // We should close any part file related to the current(above finished range) gap
                                self.close_log_file();
                                // Finished the current active range, therefore we drop it
                                self.active.take();
                                self.fill_gaps();
                            }
                            break;
                        }
                    }
                }
            }
        } else {
            self.eof = true;
            info!("SyncData reached EOF");
            self.schedule_update_sync_data();
        }
    }
    fn schedule_update_sync_data(&self) {
        info!("Scheduling update sync after: {:?}", self.update_sync_data_every);
        let update_sync_data_every = self.update_sync_data_every;
        let handle = self.handle.clone();
        let update_sync = async move {
            tokio::time::sleep(update_sync_data_every).await;
            let ask = AskSyncer::UpdateSyncData;
            let _ = handle.send(SyncerEvent::Ask(ask));
        };
        tokio::spawn(update_sync);
    }
    fn request_solidify(
        solidifier_count: u8,
        solidifier_handles: &HashMap<u8, SolidifierHandle>,
        milestone_index: u32,
    ) {
        let solidifier_id = (milestone_index % (solidifier_count as u32)) as u8;
        let solidifier_handle = solidifier_handles.get(&solidifier_id).unwrap();
        let solidify_event = SolidifierEvent::Solidify(Ok(milestone_index));
        let _ = solidifier_handle.send(solidify_event);
    }
    fn trigger_process_more(&mut self) {
        // move to next range (only if pending is zero)
        if self.pending.eq(&0) {
            // start processing it
            self.process_more();
        }
    }
    pub(crate) fn complete(&mut self) {
        // start from the lowest uncomplete
        if let Some(mut gap) = self.sync_data.take_lowest_uncomplete() {
            // ensure gap.end != i32::MAX
            if !gap.end.eq(&(i32::MAX as u32)) {
                info!("Completing the gap {:?}", gap);
                // set next to be the start
                self.next = gap.start;
                self.initial_gap_start = self.next;
                self.initial_gap_end = gap.end;
                self.active.replace(Active::Complete(gap));
                self.trigger_process_more();
            } else {
                // fill this with the gap.start up to self.highest
                // this is the last gap in our sync data
                // First we ensure highest is larger than gap.start
                if self.highest > gap.start {
                    // set next to be the start
                    self.next = gap.start;
                    self.initial_gap_start = self.next;
                    // update the end of the gap
                    gap.end = self.highest;
                    self.initial_gap_end = gap.end;
                    info!("Completing the last gap {:?}", gap);
                    self.active.replace(Active::Complete(gap));
                    self.trigger_process_more();
                } else {
                    info!("There are no more gaps neither unlogged in the current sync data");
                    self.trigger_process_more();
                }
            }
        } else {
            info!("There are no more gaps neither unlogged in the current sync data");
            self.trigger_process_more();
        }
    }
    pub(crate) fn fill_gaps(&mut self) {
        // start from the lowest gap
        if let Some(mut gap) = self.sync_data.take_lowest_gap() {
            // ensure gap.end != i32::MAX
            if !gap.end.eq(&(i32::MAX as u32)) {
                info!("Filling the gap {:?}", gap);
                // set next to be the start
                self.next = gap.start;
                self.initial_gap_start = self.next;
                self.initial_gap_end = gap.end;
                self.active.replace(Active::FillGaps(gap));
                self.trigger_process_more();
            } else {
                // fill this with the gap.start up to self.highest
                // this is the last gap in our sync data
                // First we ensure highest is larger than gap.start
                if self.highest > gap.start {
                    info!("Filling the last gap {:?}", gap);
                    // set next to be the start
                    self.next = gap.start;
                    self.initial_gap_start = self.next;
                    // update the end of the gap
                    gap.end = self.highest;
                    self.initial_gap_end = gap.end;
                    self.active.replace(Active::FillGaps(gap));
                    self.trigger_process_more();
                } else {
                    info!("There are no more gaps in the current sync data");
                    self.trigger_process_more();
                }
            }
        } else {
            info!("There are no more gaps in the current sync data");
            self.trigger_process_more();
        }
    }
}
