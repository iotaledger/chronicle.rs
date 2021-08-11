// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0
use super::{
    application::{
        BrokerRequest,
        ChronicleBroker,
    },
    archiver::{
        Archiver,
        ArchiverEvent,
    },
    exporter::ExporterStatus,
    solidifier::{
        Solidifier,
        SolidifierEvent,
    },
    *,
};
use chronicle_common::Wrapper;
use std::{
    cmp::Ord,
    collections::HashSet,
    ops::{
        Deref,
        Range,
    },
};

/// Syncer events
pub enum SyncerEvent {
    /// Sync milestone data
    MilestoneData(MilestoneData),
    /// Notify of an unreachable cluster
    Unreachable(u32),
    /// Notify that a milestone is current and the syncer should stop here
    Current(u32),
    /// Successfully exported a range of milestones from the database
    Exported(Result<Range<u32>, Range<u32>>),
}

/// Syncer state
pub struct Syncer {
    sync_data: SyncData,
    sync_range: SyncRange,
    solidifier_count: u8,
    parallelism: u8,
    active: Option<Active>,
    pending: u32,
    next: u32,
    need_close: bool,
    keyspace: ChronicleKeyspace,
}

#[build]
pub fn build_syncer(
    sync_data: SyncData,
    sync_range: Option<SyncRange>,
    solidifier_count: u8,
    parallelism: Option<u8>,
    keyspace: ChronicleKeyspace,
) -> Syncer {
    Syncer {
        sync_data,
        solidifier_count,
        sync_range: sync_range.unwrap_or_default(),
        parallelism: parallelism.unwrap_or(solidifier_count),
        active: None,
        pending: solidifier_count as u32,
        next: 0,
        need_close: false,
        keyspace,
    }
}

#[async_trait]
impl Actor for Syncer {
    type Dependencies = (Act<ChronicleBroker>, Pool<MapPool<Solidifier, u8>>);
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
        (broker_handle, solidifier_handles): Self::Dependencies,
    ) -> Result<(), ActorError>
    where
        Self: Sized,
        Sup::Event: SupervisorEvent,
        <Sup::Event as SupervisorEvent>::Children: From<PhantomData<Self>>,
    {
        rt.update_status(ServiceStatus::Running).await.ok();
        let my_handle = rt.handle();
        let mut archiver_handle = rt.actor_event_handle().await;
        if let Err(_) = self
            .process_more(&solidifier_handles, &archiver_handle, &broker_handle, &my_handle)
            .await
        {
            if self.pending == 0 {
                return Ok(());
            }
        }
        let mut milestones = std::collections::BinaryHeap::new();
        let mut skipped = HashSet::new();
        let mut current = None;
        while let Some(event) = rt.next_event().await {
            match event {
                SyncerEvent::MilestoneData(milestone_data) => {
                    self.pending -= 1;
                    milestones.push(Ascending::new(milestone_data));
                    if archiver_handle.is_closed() {
                        archiver_handle = rt.actor_event_handle().await;
                    }
                    // check if we could send the next expected milestone_index
                    while let Some(ms_data) = milestones.peek() {
                        if self.next == ms_data.milestone_index() {
                            let ms_data = milestones.pop().unwrap();
                            // push it to archiver
                            archiver_handle
                                .send(ArchiverEvent::MilestoneData(ms_data.into_inner(), None))
                                .ok();
                            self.next += 1;
                            self.need_close = true;
                        } else if skipped.remove(&self.next) {
                            if self.need_close {
                                archiver_handle
                                    .send(ArchiverEvent::Close(ms_data.milestone_index()))
                                    .ok();
                            }
                            self.next += 1;
                            self.need_close = false;
                        } else {
                            break;
                        }
                    }
                    if let Some(c) = current {
                        if self.next >= c {
                            return Ok(());
                        }
                    }
                    if let Err(_) = self
                        .process_more(&solidifier_handles, &archiver_handle, &broker_handle, &my_handle)
                        .await
                    {
                        if self.pending == 0 {
                            return Ok(());
                        }
                    }
                }
                SyncerEvent::Unreachable(milestone_index) => {
                    self.pending -= 1;
                    skipped.insert(milestone_index);
                    if archiver_handle.is_closed() {
                        archiver_handle = rt.actor_event_handle().await;
                    }
                    // This happens when all the peers don't have the requested milestone_index
                    error!("Syncer unable to reach milestone_index: {}", milestone_index);
                    if let Err(_) = self
                        .process_more(&solidifier_handles, &archiver_handle, &broker_handle, &my_handle)
                        .await
                    {
                        if self.pending == 0 {
                            return Ok(());
                        }
                    }
                }
                SyncerEvent::Exported(res) => {
                    self.pending -= 1;
                    match res {
                        Ok(range) => info!("Successfully exported {:?}", range),
                        Err(range) => error!("Failed to export {:?}", range),
                    }
                    if let Err(_) = self
                        .process_more(&solidifier_handles, &archiver_handle, &broker_handle, &my_handle)
                        .await
                    {
                        if self.pending == 0 {
                            return Ok(());
                        }
                    }
                }
                SyncerEvent::Current(milestone_index) => {
                    self.pending -= 1;
                    skipped.insert(milestone_index);
                    current.as_mut().map(|c| *c = Ord::min(*c, milestone_index));
                }
            }
        }
        Ok(())
    }
}

impl Syncer {
    async fn process_more(
        &mut self,
        solidifier_handles: &Pool<MapPool<Solidifier, u8>>,
        archiver_handle: &Option<Act<Archiver>>,
        broker_handle: &Act<ChronicleBroker>,
        handle: &Act<Self>,
    ) -> anyhow::Result<()> {
        while self.parallelism as u32 > self.pending {
            match self.active.as_mut() {
                Some(Active::Complete(ref range)) => {
                    let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
                    if broker_handle
                        .send(BrokerEvent::Websocket(BrokerRequest::Export {
                            range: range.clone(),
                            responder: sender,
                        }))
                        .is_ok()
                    {
                        let my_handle = handle.clone();
                        let range = range.clone();
                        tokio::spawn(async move {
                            while let Some(evt) = receiver.recv().await {
                                match evt {
                                    ExporterStatus::Done => {
                                        my_handle.send(SyncerEvent::Exported(Ok(range))).ok();
                                        break;
                                    }
                                    ExporterStatus::Failed(s) => {
                                        error!("Failed to export {:?}: {}", range, s);
                                        my_handle.send(SyncerEvent::Exported(Err(range))).ok();
                                        break;
                                    }
                                    _ => (),
                                }
                            }
                        });
                        self.pending += 1;
                    }
                    self.get_next_range()?;
                }
                Some(Active::FillGaps(ref mut range)) => {
                    if let Some(milestone_index) = range.next() {
                        if self.sync_range.contains(milestone_index) {
                            Self::request_solidify(self.solidifier_count, solidifier_handles, milestone_index).await;
                            // update pending
                            self.pending += 1;
                        }
                    } else {
                        // move to next gap (only if pending is zero)
                        if self.pending == 0 {
                            // We should close any part file related to the current(above finished range) gap
                            archiver_handle.send(ArchiverEvent::Close(range.end)).ok();
                            // Finished the current active range, therefore we drop it
                            self.get_next_range()?;
                            continue;
                        }
                        break;
                    }
                }
                None => {
                    self.get_next_range()?;
                }
            }
        }

        Ok(())
    }

    fn get_next_range(&mut self) -> anyhow::Result<()> {
        while let Some(unlogged) = self.sync_data.synced_but_unlogged.pop() {
            if self.sync_range.contains(unlogged.start) || self.sync_range.contains(unlogged.end - 1) {
                log::debug!(
                    "Syncing {} to {}",
                    Ord::max(self.sync_range.from, unlogged.start),
                    Ord::min(self.sync_range.to, unlogged.end)
                );
                self.active = Some(Active::Complete(unlogged));
                return Ok(());
            }
        }
        while let Some(gap) = self.sync_data.gaps.pop() {
            if self.sync_range.contains(gap.start) || self.sync_range.contains(gap.end - 1) {
                log::debug!(
                    "Syncing {} to {}",
                    Ord::max(self.sync_range.from, gap.start),
                    Ord::min(self.sync_range.to, gap.end)
                );
                self.next = Ord::max(self.sync_range.from, gap.start);
                self.active = Some(Active::FillGaps(gap));
                return Ok(());
            }
        }
        anyhow::bail!("Nothing to sync!");
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
