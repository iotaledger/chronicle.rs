// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0
use super::{
    archiver::{
        ArchiverEvent,
        ArchiverHandle,
    },
    filter::FilterHandle,
    solidifier::{
        SolidifierEvent,
        SolidifierHandle,
    },
    *,
};
use backstage::core::{
    AbortableUnboundedChannel,
    AbortableUnboundedHandle,
    Actor,
    ActorError,
    ActorResult,
    Rt,
    SupHandle,
};
use chronicle_common::types::{
    Ascending,
    CreatedBy,
    MilestoneData,
    SyncData,
};
use futures::stream::StreamExt;
use std::time::Duration;
pub(crate) type SyncerHandle = AbortableUnboundedHandle<SyncerEvent>;

#[async_trait]
impl<S: SupHandle<Self>, T: FilterBuilder> Actor<S> for Syncer<T> {
    type Data = (HashMap<u8, SolidifierHandle>, Option<ArchiverHandle>); // solidifiers_handles and archiver handle
    type Channel = AbortableUnboundedChannel<SyncerEvent>;
    async fn init(&mut self, rt: &mut Rt<Self, S>) -> ActorResult<Self::Data> {
        log::info!("{:?} is initializing", &rt.service().directory());
        let solidifier_handles = rt
            .depends_on(
                rt.parent_id()
                    .ok_or_else(|| anyhow::anyhow!("Syncer without parent!"))?,
            )
            .await?;
        if let Some(archiver_scope_id) = rt.sibling("archiver").scope_id().await {
            let archiver_handle_opt = rt.lookup(archiver_scope_id).await;
            Ok((solidifier_handles, archiver_handle_opt))
        } else {
            Ok((solidifier_handles, None))
        }
    }
    async fn run(&mut self, rt: &mut Rt<Self, S>, (solidifiers, archiver): Self::Data) -> ActorResult<()> {
        log::info!("{:?} is running", &rt.service().directory());
        if archiver.is_some() {
            self.complete(rt, &solidifiers, &archiver).await?;
        } else {
            self.fill_gaps(rt, &solidifiers).await?;
        }
        while let Some(event) = rt.inbox_mut().next().await {
            match event {
                SyncerEvent::Unreachable(milestone_index) => {
                    // This happens when all the peers don't have the requested milestone_index
                    // alert!(
                    // "Chronicle syncer is unable to reach milestone index {} because no peers were able to provide
                    // it!", milestone_index
                    // ).await.ok();
                    self.handle_skip(rt, &solidifiers, &archiver).await?;
                }
                SyncerEvent::MilestoneData(milestone_data) => {
                    self.handle_milestone_data(rt, milestone_data, &solidifiers, &archiver)
                        .await?;
                }
            }
        }
        log::info!("{:?} exited its event loop", &rt.service().directory());
        Ok(())
    }
}

/// Syncer events
#[derive(Debug)]
pub enum SyncerEvent {
    /// Sync milestone data
    MilestoneData(MilestoneData),
    /// Notify of an unreachable cluster
    Unreachable(u32),
}

/// Syncer state
pub struct Syncer<T: FilterBuilder> {
    sync_data: SyncData,
    update_sync_data_every: Duration,
    parallelism: u8,
    active: Option<Active>,
    milestones_data: BinaryHeap<Ascending<MilestoneData>>,
    pending: u32,
    next: u32,
    skip: bool,
    initial_gap_start: u32,
    initial_gap_end: u32,
    prev_closed_log_filename: u32,
    filter_handle: T::Handle,
}

impl<T: FilterBuilder> Syncer<T> {
    pub(super) fn new(update_sync_data_every: Duration, parallelism: u8, filter_handle: T::Handle) -> Self {
        Self {
            sync_data: SyncData::default(),
            update_sync_data_every,
            parallelism,
            active: None,
            milestones_data: BinaryHeap::new(),
            pending: 0,
            next: 0,
            skip: false,
            initial_gap_start: 0,
            initial_gap_end: 0,
            prev_closed_log_filename: 0,
            filter_handle,
        }
    }
}

#[derive(Debug)]
enum Active {
    Complete(std::ops::Range<u32>),
    FillGaps(std::ops::Range<u32>),
}

impl<T: FilterBuilder> Syncer<T> {
    async fn update_sync<S: SupHandle<Self>>(
        &mut self,
        rt: &mut Rt<Self, S>,
        solidifier_handles: &HashMap<u8, SolidifierHandle>,
        archiver: &Option<ArchiverHandle>,
    ) -> ActorResult<()> {
        info!("Scheduling update sync after: {:?}", self.update_sync_data_every);
        rt.abortable(tokio::time::sleep(self.update_sync_data_every))
            .await
            .map_err(|_| {
                warn!("Syncer got aborted while sleeping");
                ActorError::aborted_msg("Syncer got aborted while sleeping")
            })?;
        if let Ok(sync_data) = self.filter_handle.sync_data(None).await {
            info!("Updated the sync data");
            self.sync_data = sync_data;
            if archiver.is_some() {
                self.complete(rt, solidifier_handles, archiver).await?
            } else {
                self.fill_gaps(rt, solidifier_handles).await?
            }
        } else {
            error!("Unable to update sync data");
            rt.abortable(tokio::time::sleep(self.update_sync_data_every))
                .await
                .map_err(|_| {
                    warn!("Syncer got aborted while sleeping");
                    ActorError::aborted_msg("Syncer got aborted while sleeping")
                })?
        }
        Ok(())
    }

    async fn handle_milestone_data<S: SupHandle<Self>>(
        &mut self,
        rt: &mut Rt<Self, S>,
        milestone_data: MilestoneData,
        solidifier_handles: &HashMap<u8, SolidifierHandle>,
        archiver_handle: &Option<ArchiverHandle>,
    ) -> ActorResult<()> {
        self.milestones_data.push(Ascending::new(milestone_data));
        if !self.skip {
            self.pending -= 1;
            self.try_solidify_one_more(solidifier_handles);
            let upper_ms_limit = Some(self.initial_gap_end);
            // check if we could send the next expected milestone_index
            while let Some(ms_data) = self.milestones_data.pop() {
                let ms_index = ms_data.milestone_index();
                if self.next == ms_index.0 {
                    // push it to archiver
                    archiver_handle.as_ref().and_then(|h| {
                        h.send(ArchiverEvent::MilestoneData(
                            ms_data.into(),
                            CreatedBy::Syncer,
                            upper_ms_limit,
                        ))
                        .ok()
                    });
                    self.next += 1;
                } else {
                    // put it back and then break
                    self.milestones_data.push(ms_data);
                    break;
                }
            }
            self.trigger_process_more(rt, solidifier_handles, archiver_handle)
                .await?
        } else {
            self.handle_skip(rt, solidifier_handles, archiver_handle).await?
        }
        Ok(())
    }
    async fn handle_skip<S: SupHandle<Self>>(
        &mut self,
        rt: &mut Rt<Self, S>,
        solidifier_handles: &HashMap<u8, SolidifierHandle>,
        archiver_handle: &Option<ArchiverHandle>,
    ) -> ActorResult<()> {
        self.pending -= 1;
        self.skip = true;
        // we should skip/drop the current active slot but only when pending == 0
        if self.pending.eq(&0) {
            self.skip = false;
            while let Some(d) = self.milestones_data.pop() {
                let d: MilestoneData = d.into();
                error!("We got milestone data for index: {}, but we're skipping it due to previous unreachable indexes within the same gap range", d.milestone_index());
            }
            if let Some(mut active) = self.active.take() {
                match active {
                    Active::Complete(ref mut range) => {
                        warn!("Complete: Skipping the remaining gap range: {:?}", range);
                        // we just consume the range in order for the trigger_process_more to move further
                        self.close_log_file(archiver_handle);
                        self.complete(rt, solidifier_handles, archiver_handle).await?
                    }
                    Active::FillGaps(ref mut range) => {
                        warn!("FillGaps: Skipping the remaining gap range: {:?}", range);
                        self.close_log_file(archiver_handle);
                        self.fill_gaps(rt, solidifier_handles).await?
                    }
                };
            }
        }
        Ok(())
    }

    fn close_log_file(&mut self, archiver: &Option<ArchiverHandle>) {
        let created_log_file = self.initial_gap_start != self.next;
        if self.prev_closed_log_filename != self.initial_gap_start && created_log_file {
            if let Some(archiver_handle) = archiver.as_ref() {
                info!(
                    "Informing Archiver to close {}.part, and should be renamed to: {}to{}.log",
                    self.initial_gap_start, self.initial_gap_start, self.next
                );
                // We should close any part file related to the current gap
                let _ = archiver_handle.send(ArchiverEvent::Close(self.next));
            };
            self.prev_closed_log_filename = self.initial_gap_start;
        }
    }

    fn try_solidify_one_more(&mut self, solidifier_handles: &HashMap<u8, SolidifierHandle>) {
        match self.active.as_mut().unwrap() {
            Active::Complete(ref mut range) => {
                if let Some(milestone_index) = range.next() {
                    Self::request_solidify(solidifier_handles, milestone_index);
                    self.pending += 1;
                }
            }
            Active::FillGaps(ref mut range) => {
                if let Some(milestone_index) = range.next() {
                    Self::request_solidify(solidifier_handles, milestone_index);
                    self.pending += 1;
                }
            }
        };
    }
    async fn process_more<S: SupHandle<Self>>(
        &mut self,
        rt: &mut Rt<Self, S>,
        solidifier_handles: &HashMap<u8, SolidifierHandle>,
        archiver_handle: &Option<ArchiverHandle>,
    ) -> ActorResult<()> {
        if let Some(ref mut active) = self.active {
            match active {
                Active::Complete(range) => {
                    for _ in 0..self.parallelism {
                        if let Some(milestone_index) = range.next() {
                            Self::request_solidify(solidifier_handles, milestone_index);
                            // update pending
                            self.pending += 1;
                        } else {
                            // move to next gap (only if pending is zero)
                            if self.pending.eq(&0) {
                                // We should close any part file related to the current(above finished range) gap
                                self.close_log_file(archiver_handle);
                                // Finished the current active range, therefore we drop it
                                self.active.take();
                                self.complete(rt, solidifier_handles, archiver_handle).await?
                            }
                            break;
                        }
                    }
                }
                Active::FillGaps(range) => {
                    for _ in 0..self.parallelism {
                        if let Some(milestone_index) = range.next() {
                            Self::request_solidify(solidifier_handles, milestone_index);
                            // update pending
                            self.pending += 1;
                        } else {
                            // move to next gap (only if pending is zero)
                            if self.pending.eq(&0) {
                                // We should close any part file related to the current(above finished range) gap
                                self.close_log_file(archiver_handle);
                                // Finished the current active range, therefore we drop it
                                self.active.take();
                                self.fill_gaps(rt, solidifier_handles).await?
                            }
                            break;
                        }
                    }
                }
            }
        } else {
            info!("SyncData reached EOF");
            self.update_sync(rt, solidifier_handles, archiver_handle).await?
        }
        Ok(())
    }
    fn request_solidify(solidifier_handles: &HashMap<u8, SolidifierHandle>, milestone_index: u32) {
        let solidifier_id = (milestone_index % (solidifier_handles.len() as u32)) as u8;
        let solidifier_handle = solidifier_handles.get(&solidifier_id).unwrap();
        let solidify_event = SolidifierEvent::Solidify(Ok(milestone_index));
        let _ = solidifier_handle.send(solidify_event);
    }
    #[async_recursion::async_recursion]
    async fn trigger_process_more<S: SupHandle<Self>>(
        &mut self,
        rt: &mut Rt<Self, S>,
        solidifier_handles: &HashMap<u8, SolidifierHandle>,
        archiver: &Option<ArchiverHandle>,
    ) -> ActorResult<()> {
        // move to next range (only if pending is zero)
        if self.pending.eq(&0) {
            // start processing it
            self.process_more(rt, solidifier_handles, archiver).await?
        }
        Ok(())
    }
    async fn complete<S: SupHandle<Self>>(
        &mut self,
        rt: &mut Rt<Self, S>,
        solidifier_handles: &HashMap<u8, SolidifierHandle>,
        archiver: &Option<ArchiverHandle>,
    ) -> ActorResult<()> {
        // start from the lowest uncomplete
        if let Some(gap) = self.sync_data.take_lowest_uncomplete() {
            // ensure gap.end != i32::MAX
            if !gap.end.eq(&(i32::MAX as u32)) {
                info!("Completing the gap {:?}", gap);
                // set next to be the start
                self.next = gap.start;
                self.initial_gap_start = self.next;
                self.initial_gap_end = gap.end;
                self.active.replace(Active::Complete(gap));
                self.trigger_process_more(rt, solidifier_handles, archiver).await?
            } else {
                info!("There are no more gaps neither unlogged in the current sync data");
                self.trigger_process_more(rt, solidifier_handles, archiver).await?
            }
        } else {
            info!("There are no more gaps neither unlogged in the current sync data");
            self.trigger_process_more(rt, solidifier_handles, archiver).await?
        }
        Ok(())
    }
    async fn fill_gaps<S: SupHandle<Self>>(
        &mut self,
        rt: &mut Rt<Self, S>,
        solidifier_handles: &HashMap<u8, SolidifierHandle>,
    ) -> ActorResult<()> {
        // start from the lowest gap
        if let Some(gap) = self.sync_data.take_lowest_gap() {
            // ensure gap.end != i32::MAX
            if !gap.end.eq(&(i32::MAX as u32)) {
                info!("Filling the gap {:?}", gap);
                // set next to be the start
                self.next = gap.start;
                self.initial_gap_start = self.next;
                self.initial_gap_end = gap.end;
                self.active.replace(Active::FillGaps(gap));
                self.trigger_process_more(rt, solidifier_handles, &None).await?
            } else {
                info!("There are no more gaps in the current sync data");
                self.trigger_process_more(rt, solidifier_handles, &None).await?
            }
        } else {
            info!("There are no more gaps in the current sync data");
            self.trigger_process_more(rt, solidifier_handles, &None).await?
        }
        Ok(())
    }
}
