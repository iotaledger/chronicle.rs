// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use crate::{
    application::*,
    mqtt::*,
};

use lru::LruCache;
use std::ops::{
    Deref,
    DerefMut,
};

mod event_loop;
mod init;
mod terminating;

// Collector builder
builder!(CollectorBuilder {
    partition_id: u8,
    lru_capacity: usize,
    collectors_count: u8
});

pub enum CollectorEvent {
    /// Newly seen message from feed source(s)
    Message(MessageId, Message),
    /// Newly seen MessageReferenced from feed source(s)
    MessageReferenced(MessageReferenced),
}
/// CollectorHandle to be passed to siblings(feed sources) and the supervisor(in order to shutdown)
#[derive(Clone)]
pub struct CollectorHandle {
    tx: tokio::sync::mpsc::UnboundedSender<CollectorEvent>,
}
/// CollectorInbox is used to recv events
pub struct CollectorInbox {
    rx: tokio::sync::mpsc::UnboundedReceiver<CollectorEvent>,
}
impl Deref for CollectorHandle {
    type Target = tokio::sync::mpsc::UnboundedSender<CollectorEvent>;

    fn deref(&self) -> &Self::Target {
        &self.tx
    }
}

impl DerefMut for CollectorHandle {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.tx
    }
}

impl Deref for CollectorInbox {
    type Target = tokio::sync::mpsc::UnboundedReceiver<CollectorEvent>;

    fn deref(&self) -> &Self::Target {
        &self.rx
    }
}

impl DerefMut for CollectorInbox {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.rx
    }
}

impl Shutdown for CollectorHandle {
    fn shutdown(self) -> Option<Self>
    where
        Self: Sized,
    {
        // to shutdown te collector, we simply drop the collectorhandle
        None
    }
}

// collector state, each collector is basically LRU cache
pub struct Collector {
    service: Service,
    partition_id: u8,
    lru_msg: LruCache<MessageId, Message>,
    lru_msg_ref: LruCache<MessageId, MessageReferenced>,
    handle: Option<CollectorHandle>,
    inbox: CollectorInbox,
}

impl Collector {
    pub(crate) fn take_handle(&mut self) -> Option<CollectorHandle> {
        self.handle.take()
    }
}

impl<H: PermanodeBrokerScope> ActorBuilder<BrokerHandle<H>> for CollectorBuilder {}

/// implementation of builder
impl Builder for CollectorBuilder {
    type State = Collector;
    fn build(self) -> Self::State {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let handle = Some(CollectorHandle { tx });
        let inbox = CollectorInbox { rx };
        let lru_cap = self.lru_capacity.unwrap_or(1000);
        Self::State {
            service: Service::new(),
            lru_msg: LruCache::new(lru_cap),
            lru_msg_ref: LruCache::new(lru_cap),
            partition_id: self.partition_id.unwrap(),
            handle,
            inbox,
        }
        .set_name()
    }
}

/// impl name of the Collector
impl Name for Collector {
    fn set_name(mut self) -> Self {
        let name = format!("{}", self.partition_id);
        self.service.update_name(name);
        self
    }
    fn get_name(&self) -> String {
        self.service.get_name()
    }
}

#[async_trait::async_trait]
impl<H: PermanodeBrokerScope> AknShutdown<Collector> for BrokerHandle<H> {
    async fn aknowledge_shutdown(self, mut _state: Collector, _status: Result<(), Need>) {
        _state.service.update_status(ServiceStatus::Stopped);
        let event = BrokerEvent::Children(BrokerChild::Collector(_state.service.clone()));
        let _ = self.send(event);
    }
}
