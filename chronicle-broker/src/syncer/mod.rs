// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0
use crate::{
    application::*,
    archiver::{
        ArchiverEvent,
        ArchiverHandle,
    },
    collector::*,
    solidifier::{
        FullMessage,
        MilestoneData,
        SolidifierEvent,
        SolidifierHandle,
    },
};
use tokio::sync::oneshot::Sender;

use std::ops::{
    Deref,
    DerefMut,
};

mod event_loop;
mod init;
mod terminating;

// Syncer builder
builder!(SyncerBuilder {
    sync_data: SyncData,
    solidifier_handles: HashMap<u8, SolidifierHandle>,
    archiver_handle: ArchiverHandle,
    first_ask: AskSyncer,
    oneshot: Sender<u32>,
    handle: SyncerHandle,
    inbox: SyncerInbox
});

pub enum SyncerEvent {
    Ask(AskSyncer),
    Process,
    MilestoneData(MilestoneData),
    Unreachable(u32),
    Shutdown,
}

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

#[derive(Clone)]
pub struct SyncerHandle {
    pub(crate) tx: tokio::sync::mpsc::UnboundedSender<SyncerEvent>,
}

impl Deref for SyncerHandle {
    type Target = tokio::sync::mpsc::UnboundedSender<SyncerEvent>;

    fn deref(&self) -> &Self::Target {
        &self.tx
    }
}

impl DerefMut for SyncerHandle {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.tx
    }
}

/// SyncerInbox is used to recv requests from collector
pub struct SyncerInbox {
    pub(crate) rx: tokio::sync::mpsc::UnboundedReceiver<SyncerEvent>,
}

impl Deref for SyncerInbox {
    type Target = tokio::sync::mpsc::UnboundedReceiver<SyncerEvent>;

    fn deref(&self) -> &Self::Target {
        &self.rx
    }
}

impl DerefMut for SyncerInbox {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.rx
    }
}

impl Shutdown for SyncerHandle {
    fn shutdown(self) -> Option<Self>
    where
        Self: Sized,
    {
        self.send(SyncerEvent::Shutdown).ok();
        None
    }
}

// Syncer state
pub struct Syncer {
    service: Service,
    sync_data: SyncData,
    solidifier_handles: HashMap<u8, SolidifierHandle>,
    solidifier_count: u8,
    active: Option<Active>,
    first_ask: Option<AskSyncer>,
    archiver_handle: ArchiverHandle,
    milestones_data: std::collections::BinaryHeap<Ascending<MilestoneData>>,
    highest: u32,
    pending: u32,
    eof: bool,
    next: u32,
    skip: bool,
    initial_gap_start: u32,
    initial_gap_end: u32,
    prev_closed_log_filename: u32,
    oneshot: Option<Sender<u32>>,
    handle: SyncerHandle,
    inbox: SyncerInbox,
}

impl<H: ChronicleBrokerScope> ActorBuilder<BrokerHandle<H>> for SyncerBuilder {}

/// implementation of builder
impl Builder for SyncerBuilder {
    type State = Syncer;
    fn build(self) -> Self::State {
        let solidifier_handles = self.solidifier_handles.unwrap();
        let solidifier_count = solidifier_handles.len() as u8;
        let sync_data = self.sync_data.unwrap();

        Self::State {
            service: Service::new(),
            sync_data,
            solidifier_handles,
            solidifier_count,
            active: None,
            first_ask: self.first_ask,
            archiver_handle: self.archiver_handle.unwrap(),
            milestones_data: std::collections::BinaryHeap::new(),
            highest: 0,
            pending: solidifier_count as u32,
            next: 0,
            eof: false,
            skip: false,
            initial_gap_start: 0,
            initial_gap_end: 0,
            prev_closed_log_filename: 0,
            oneshot: self.oneshot,
            handle: self.handle.unwrap(),
            inbox: self.inbox.unwrap(),
        }
        .set_name()
    }
}
#[derive(Debug)]
pub enum Active {
    Complete(std::ops::Range<u32>),
    FillGaps(std::ops::Range<u32>),
}
/// impl name of the Syncer
impl Name for Syncer {
    fn set_name(mut self) -> Self {
        let name = format!("Syncer");
        self.service.update_name(name);
        self
    }
    fn get_name(&self) -> String {
        self.service.get_name()
    }
}

#[async_trait::async_trait]
impl<H: ChronicleBrokerScope> AknShutdown<Syncer> for BrokerHandle<H> {
    async fn aknowledge_shutdown(self, mut _state: Syncer, _status: Result<(), Need>) {}
}

pub struct Ascending<T> {
    inner: T,
}

impl<T> Ascending<T> {
    pub fn into_inner(self) -> T {
        self.inner
    }
    pub fn get_ref(&self) -> &T {
        &self.inner
    }
}

impl Ascending<MilestoneData> {
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
