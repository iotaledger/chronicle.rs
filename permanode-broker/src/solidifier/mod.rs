// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use crate::{
    application::*,
    collector::*,
};
use lru::LruCache;
use permanode_storage::access::*;
use std::ops::{
    Deref,
    DerefMut,
};

mod event_loop;
mod init;
mod terminating;

// Solidifier builder
builder!(SolidifierBuilder {
    partition_id: u8,
    lru_capacity: usize,
    inbox: SolidifierInbox,
    collector_handles: HashMap<u8, CollectorHandle>,
    collectors_count: u8
});

pub enum SolidifierEvent {
    /// Pushed or requested messages, that definitely belong to self solidifier
    Message(Message, MessageReferenced),
    /// Close MessageId that doesn't belong at all to Solidifier
    Close(MessageId, u64),
    /// To be determined Message, that might belong to self solidifier
    /// u64 is milestone index which requested the msg, assuming it belongs to it,
    /// and unfortunately the collector doesn't have the MessageReferenced.
    /// collector likely will re request it from the network.
    Tbd(u64, MessageId, Message),
    /// Shutdown the solidifier
    Shutdown,
}

/// SolidifierHandle
#[derive(Clone)]
pub struct SolidifierHandle {
    pub(crate) tx: tokio::sync::mpsc::UnboundedSender<SolidifierEvent>,
}
/// SolidifierInbox is used to recv events
pub struct SolidifierInbox {
    pub(crate) rx: tokio::sync::mpsc::UnboundedReceiver<SolidifierEvent>,
}
impl Deref for SolidifierHandle {
    type Target = tokio::sync::mpsc::UnboundedSender<SolidifierEvent>;

    fn deref(&self) -> &Self::Target {
        &self.tx
    }
}

impl DerefMut for SolidifierHandle {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.tx
    }
}

impl Deref for SolidifierInbox {
    type Target = tokio::sync::mpsc::UnboundedReceiver<SolidifierEvent>;

    fn deref(&self) -> &Self::Target {
        &self.rx
    }
}

impl DerefMut for SolidifierInbox {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.rx
    }
}

impl Shutdown for SolidifierHandle {
    fn shutdown(self) -> Option<Self>
    where
        Self: Sized,
    {
        let _ = self.tx.send(SolidifierEvent::Shutdown);
        None
    }
}

// Solidifier state, each Solidifier solidifiy subset of (milestones_index % solidifier_count == partition_id)
pub struct Solidifier {
    service: Service,
    partition_id: u8,
    inbox: SolidifierInbox,
}

impl<H: PermanodeBrokerScope> ActorBuilder<BrokerHandle<H>> for SolidifierBuilder {}

/// implementation of builder
impl Builder for SolidifierBuilder {
    type State = Solidifier;
    fn build(self) -> Self::State {
        let lru_cap = self.lru_capacity.unwrap_or(1000);
        Self::State {
            service: Service::new(),
            partition_id: self.partition_id.unwrap(),
            inbox: self.inbox.unwrap(),
        }
        .set_name()
    }
}

/// impl name of the Collector
impl Name for Solidifier {
    fn set_name(mut self) -> Self {
        let name = format!("Solidifier_{}", self.partition_id);
        self.service.update_name(name);
        self
    }
    fn get_name(&self) -> String {
        self.service.get_name()
    }
}

#[async_trait::async_trait]
impl<H: PermanodeBrokerScope> AknShutdown<Solidifier> for BrokerHandle<H> {
    async fn aknowledge_shutdown(self, mut _state: Solidifier, _status: Result<(), Need>) {
        _state.service.update_status(ServiceStatus::Stopped);
        let event = BrokerEvent::Children(BrokerChild::Solidifier(_state.service.clone()));
        let _ = self.send(event);
    }
}
