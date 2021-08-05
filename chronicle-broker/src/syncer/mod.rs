// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0
use super::{
    archiver::{
        Archiver,
        ArchiverEvent,
    },
    solidifier::{
        Solidifier,
        SolidifierEvent,
    },
    *,
};
use async_recursion::async_recursion;
use chronicle_common::Wrapper;
use chronicle_storage::keyspaces::ChronicleKeyspace;
use std::{
    ops::Deref,
    time::Duration,
};

/// Syncer events
pub enum SyncerEvent {
    /// Ask for sync data
    Ask(AskSyncer),
    /// Sync milestone data
    MilestoneData(MilestoneData),
    /// Notify of an unreachable cluster
    Unreachable(u32),
    /// Shutdown the syncer
    Shutdown,
}

/// Commands that can be given to the syncer
#[derive(Debug)]
pub enum AskSyncer {
    /// Complete Everything.
    /// NOTE: Complete means it's synced and logged
    Complete,
    /// Fill the missing gaps
    FillGaps,
    /// Update sync data to the most up to date version from sync table.
    // (This is still work in progress)
    UpdateSyncData,
}
/// Syncer state
pub struct Syncer {
    sync_data: SyncData,
    update_sync_data_every: Duration,
    keyspace: ChronicleKeyspace,
    sync_range: Option<SyncRange>,
    solidifier_count: u8,
    parallelism: u8,
    active: Option<Active>,
    first_ask: Option<AskSyncer>,
    milestones_data: std::collections::BinaryHeap<Ascending<MilestoneData>>,
    highest: u32,
    pending: u32,
    eof: bool,
    next: u32,
    skip: bool,
    initial_gap_start: u32,
    initial_gap_end: u32,
    prev_closed_log_filename: u32,
}

#[build]
pub fn build_syncer(
    sync_data: SyncData,
    update_sync_data_every: Option<Duration>,
    sync_range: Option<SyncRange>,
    solidifier_count: u8,
    parallelism: Option<u8>,
    first_ask: Option<AskSyncer>,
) -> Syncer {
    let config = chronicle_common::get_config();
    let keyspace = ChronicleKeyspace::new(
        config
            .storage_config
            .keyspaces
            .first()
            .and_then(|keyspace| Some(keyspace.name.clone()))
            .unwrap_or("permanode".to_owned()),
    );
    Syncer {
        sync_data,
        solidifier_count,
        sync_range,
        keyspace,
        update_sync_data_every: update_sync_data_every.unwrap_or(std::time::Duration::from_secs(60 * 60)),
        parallelism: parallelism.unwrap_or(solidifier_count),
        active: None,
        first_ask: first_ask,
        milestones_data: std::collections::BinaryHeap::new(),
        highest: 0,
        pending: solidifier_count as u32,
        next: 0,
        eof: false,
        skip: false,
        initial_gap_start: 0,
        initial_gap_end: 0,
        prev_closed_log_filename: 0,
    }
}

#[async_trait]
impl Actor for Syncer {
    type Dependencies = (Pool<MapPool<Solidifier, u8>>, Act<Archiver>);
    type Event = SyncerEvent;
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
        (solidifier_handles, archiver_handle): Self::Dependencies,
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
                SyncerEvent::Ask(ask) => {
                    // Don't accept ask events when there is something already in progress.
                    if let None = self.active {
                        match ask {
                            AskSyncer::Complete => {
                                if !self.highest.eq(&0) {
                                    self.complete(&my_handle, &solidifier_handles, &archiver_handle).await;
                                } else {
                                    self.first_ask.replace(ask);
                                }
                            }
                            AskSyncer::FillGaps => {
                                if !self.highest.eq(&0) {
                                    self.fill_gaps(&my_handle, &solidifier_handles, &archiver_handle).await;
                                } else {
                                    self.first_ask.replace(ask);
                                }
                            }
                            AskSyncer::UpdateSyncData => {
                                info!("Trying to update the sync data");
                                self.update_sync(&my_handle, &solidifier_handles, &archiver_handle)
                                    .await;
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
                    self.handle_milestone_data(milestone_data, &my_handle, &solidifier_handles, &archiver_handle)
                        .await;
                }
                SyncerEvent::Unreachable(milestone_index) => {
                    self.pending -= 1;
                    // This happens when all the peers don't have the requested milestone_index
                    error!("Syncer unable to reach milestone_index: {}", milestone_index);
                    self.handle_skip();
                    self.trigger_process_more(&my_handle, &solidifier_handles, &archiver_handle)
                        .await;
                }
                SyncerEvent::Shutdown => break,
            }
        }
        Ok(())
    }
}

impl Syncer {
    async fn update_sync(
        &mut self,
        my_handle: &Act<Self>,
        solidifier_handles: &Pool<MapPool<Solidifier, u8>>,
        archiver_handle: &Act<Archiver>,
    ) {
        if self.eof {
            if let Some(sync_range) = self.sync_range.as_ref() {
                // try to fetch and update sync_data
                if let Ok(sync_data) = SyncData::try_fetch(&self.keyspace, sync_range, 10).await {
                    info!("Updated the sync data");
                    self.sync_data = sync_data;
                    self.eof = false;
                    self.complete_or_fillgaps(my_handle, solidifier_handles, archiver_handle)
                        .await;
                } else {
                    self.schedule_update_sync_data(my_handle.clone());
                }
            }
        }
    }

    async fn handle_milestone_data(
        &mut self,
        milestone_data: MilestoneData,
        my_handle: &Act<Self>,
        solidifier_handles: &Pool<MapPool<Solidifier, u8>>,
        archiver_handle: &Act<Archiver>,
    ) {
        self.pending -= 1;
        self.milestones_data.push(Ascending::new(milestone_data));
        if self.highest.eq(&0) && self.pending.eq(&0) {
            // these are the first milestones data, which we didn't even request it.
            let milestone_data = self.milestones_data.pop().unwrap().into_inner();
            self.highest = milestone_data.milestone_index();
            let mut next = self.highest + 1;
            // push it to archiver
            archiver_handle
                .send(ArchiverEvent::MilestoneData(milestone_data, None))
                .ok();
            // push the rest
            while let Some(ms_data) = self.milestones_data.pop() {
                let milestone_data = ms_data.into_inner();
                let ms_index = milestone_data.milestone_index();
                if next != ms_index {
                    archiver_handle.send(ArchiverEvent::Close(next)).ok();
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
                archiver_handle
                    .send(ArchiverEvent::MilestoneData(milestone_data, None))
                    .ok();
            }
            // set the first ask request
            self.complete_or_fillgaps(my_handle, solidifier_handles, archiver_handle)
                .await;
        } else if !self.highest.eq(&0) && !self.skip {
            self.try_solidify_one_more(solidifier_handles).await;
            let upper_ms_limit = Some(self.initial_gap_end);
            // check if we could send the next expected milestone_index
            while let Some(ms_data) = self.milestones_data.pop() {
                let ms_index = ms_data.milestone_index();
                if self.next.eq(&ms_index) {
                    // push it to archiver
                    let _ = archiver_handle
                        .send(ArchiverEvent::MilestoneData(ms_data.into_inner(), upper_ms_limit))
                        .ok();
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
        self.trigger_process_more(my_handle, solidifier_handles, archiver_handle)
            .await;
    }

    fn handle_skip(&mut self) {
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

    async fn complete_or_fillgaps(
        &mut self,
        my_handle: &Act<Self>,
        solidifier_handles: &Pool<MapPool<Solidifier, u8>>,
        archiver_handle: &Act<Archiver>,
    ) {
        match self.first_ask.as_ref() {
            Some(AskSyncer::Complete) => {
                self.complete(my_handle, solidifier_handles, archiver_handle).await;
            }
            Some(AskSyncer::FillGaps) => self.fill_gaps(my_handle, solidifier_handles, archiver_handle).await,
            _ => {}
        }
    }

    fn close_log_file(&mut self, archiver_handle: &Act<Archiver>) {
        let created_log_file = self.initial_gap_start != self.next;
        if self.prev_closed_log_filename != self.initial_gap_start && created_log_file {
            info!(
                "Informing Archiver to close {}.part, and should be renamed to: {}to{}.log",
                self.initial_gap_start, self.initial_gap_start, self.next
            );
            // We should close any part file related to the current gap
            archiver_handle.send(ArchiverEvent::Close(self.next)).ok();

            self.prev_closed_log_filename = self.initial_gap_start;
        } else {
            self.prev_closed_log_filename = 0;
        }
    }

    async fn try_solidify_one_more(&mut self, solidifier_handles: &Pool<MapPool<Solidifier, u8>>) {
        match self.active.as_mut().unwrap() {
            Active::Complete(ref mut range) => {
                if let Some(milestone_index) = range.next() {
                    Self::request_solidify(self.solidifier_count, solidifier_handles, milestone_index).await;
                    self.pending += 1;
                }
            }
            Active::FillGaps(ref mut range) => {
                if let Some(milestone_index) = range.next() {
                    Self::request_solidify(self.solidifier_count, solidifier_handles, milestone_index).await;
                    self.pending += 1;
                }
            }
        };
    }

    #[async_recursion]
    async fn process_more(
        &mut self,
        my_handle: &Act<Self>,
        solidifier_handles: &Pool<MapPool<Solidifier, u8>>,
        archiver_handle: &Act<Archiver>,
    ) {
        if let Some(ref mut active) = self.active {
            match active {
                Active::Complete(range) => {
                    for _ in 0..self.parallelism {
                        if let Some(milestone_index) = range.next() {
                            Self::request_solidify(self.solidifier_count, solidifier_handles, milestone_index).await;
                            // update pending
                            self.pending += 1;
                        } else {
                            // move to next gap (only if pending is zero)
                            if self.pending.eq(&0) {
                                // We should close any part file related to the current(above finished range) gap
                                self.close_log_file(archiver_handle);
                                // Finished the current active range, therefore we drop it
                                self.active.take();
                                self.complete(my_handle, solidifier_handles, archiver_handle).await;
                            }
                            break;
                        }
                    }
                }
                Active::FillGaps(range) => {
                    for _ in 0..self.parallelism {
                        if let Some(milestone_index) = range.next() {
                            Self::request_solidify(self.solidifier_count, solidifier_handles, milestone_index).await;
                            // update pending
                            self.pending += 1;
                        } else {
                            // move to next gap (only if pending is zero)
                            if self.pending.eq(&0) {
                                // We should close any part file related to the current(above finished range) gap
                                self.close_log_file(archiver_handle);
                                // Finished the current active range, therefore we drop it
                                self.active.take();
                                self.fill_gaps(my_handle, solidifier_handles, archiver_handle).await;
                            }
                            break;
                        }
                    }
                }
            }
        } else {
            self.eof = true;
            info!("SyncData reached EOF");
            self.schedule_update_sync_data(my_handle.clone());
        }
    }
    fn schedule_update_sync_data(&self, my_handle: Act<Self>) {
        info!("Scheduling update sync after: {:?}", self.update_sync_data_every);
        let update_sync_data_every = self.update_sync_data_every;
        tokio::spawn(async move {
            tokio::time::sleep(update_sync_data_every).await;
            let ask = AskSyncer::UpdateSyncData;
            my_handle.send(SyncerEvent::Ask(ask)).ok();
        });
    }
    async fn request_solidify(
        solidifier_count: u8,
        solidifier_handles: &Pool<MapPool<Solidifier, u8>>,
        milestone_index: u32,
    ) {
        let solidifier_id = (milestone_index % (solidifier_count as u32)) as u8;
        solidifier_handles
            .send(&solidifier_id, SolidifierEvent::Solidify(Ok(milestone_index)))
            .await;
    }
    async fn trigger_process_more(
        &mut self,
        my_handle: &Act<Self>,
        solidifier_handles: &Pool<MapPool<Solidifier, u8>>,
        archiver_handle: &Act<Archiver>,
    ) {
        // move to next range (only if pending is zero)
        if self.pending.eq(&0) {
            // start processing it
            self.process_more(my_handle, solidifier_handles, archiver_handle).await;
        }
    }

    async fn complete(
        &mut self,
        my_handle: &Act<Self>,
        solidifier_handles: &Pool<MapPool<Solidifier, u8>>,
        archiver_handle: &Act<Archiver>,
    ) {
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
                self.trigger_process_more(my_handle, solidifier_handles, archiver_handle)
                    .await;
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
                    self.trigger_process_more(my_handle, solidifier_handles, archiver_handle)
                        .await;
                } else {
                    info!("There are no more gaps neither unlogged in the current sync data");
                    self.trigger_process_more(my_handle, solidifier_handles, archiver_handle)
                        .await;
                }
            }
        } else {
            info!("There are no more gaps neither unlogged in the current sync data");
            self.trigger_process_more(my_handle, solidifier_handles, archiver_handle)
                .await;
        }
    }
    async fn fill_gaps(
        &mut self,
        my_handle: &Act<Self>,
        solidifier_handles: &Pool<MapPool<Solidifier, u8>>,
        archiver_handle: &Act<Archiver>,
    ) {
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
                self.trigger_process_more(my_handle, solidifier_handles, archiver_handle)
                    .await;
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
                    self.trigger_process_more(my_handle, solidifier_handles, archiver_handle)
                        .await;
                } else {
                    info!("There are no more gaps in the current sync data");
                    self.trigger_process_more(my_handle, solidifier_handles, archiver_handle)
                        .await;
                }
            }
        } else {
            info!("There are no more gaps in the current sync data");
            self.trigger_process_more(my_handle, solidifier_handles, archiver_handle)
                .await;
        }
    }
}

#[derive(Debug)]
enum Active {
    Complete(std::ops::Range<u32>),
    FillGaps(std::ops::Range<u32>),
}
/// ASC ordering wrapper
pub struct Ascending<T> {
    inner: T,
}

impl<T> Deref for Ascending<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<T> Wrapper for Ascending<T> {
    fn into_inner(self) -> Self::Target {
        self.inner
    }
}

impl Ascending<MilestoneData> {
    /// Wrap milestone data with ASC ordering
    pub fn new(milestone_data: MilestoneData) -> Self {
        Self { inner: milestone_data }
    }
}

impl std::cmp::Ord for Ascending<MilestoneData> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other.inner.milestone_index().cmp(&self.inner.milestone_index())
    }
}
impl std::cmp::PartialOrd for Ascending<MilestoneData> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(other.inner.milestone_index().cmp(&self.inner.milestone_index()))
    }
}
impl std::cmp::PartialEq for Ascending<MilestoneData> {
    fn eq(&self, other: &Self) -> bool {
        if self.inner.milestone_index() == other.inner.milestone_index() {
            true
        } else {
            false
        }
    }
}
impl std::cmp::Eq for Ascending<MilestoneData> {}
