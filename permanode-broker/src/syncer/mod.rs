// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0
use crate::{
    application::*,
    archiver::ArchiverHandle,
    collector::*,
    solidifier::{
        FullMessage,
        MilestoneData,
        SolidifierHandle,
    },
};
use std::collections::VecDeque;

use bee_rest_api::types::dtos::MessageDto;
use std::convert::TryFrom;

use std::ops::{
    Deref,
    DerefMut,
};

mod event_loop;
mod init;
mod terminating;
use reqwest::Client;
use url::Url;
// Syncer builder
builder!(SyncerBuilder {
    sync_data: SyncData,
    solidifier_handles: HashMap<u8, SolidifierHandle>,
    archiver_handle: ArchiverHandle,
    inbox: SyncerInbox
});

pub enum SyncerEvent {
    Ask(AskSyncer),
    Process,
    MilestoneData(MilestoneData),
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
        todo!()
    }
}

// Syncer state
pub struct Syncer {
    service: Service,
    sync_data: SyncData,
    solidifier_handles: HashMap<u8, SolidifierHandle>,
    solidifier_count: u8,
    active: Option<Active>,
    archiver_handle: ArchiverHandle,
    inbox: SyncerInbox,
}

impl<H: PermanodeBrokerScope> ActorBuilder<BrokerHandle<H>> for SyncerBuilder {}

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
            archiver_handle: self.archiver_handle.unwrap(),
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
impl<H: PermanodeBrokerScope> AknShutdown<Syncer> for BrokerHandle<H> {
    async fn aknowledge_shutdown(self, mut _state: Syncer, _status: Result<(), Need>) {}
}
