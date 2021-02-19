// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0
use crate::{listener::*, websocket::*};

pub use chronicle::*;
pub use log::*;
pub use tokio::{spawn, sync::mpsc};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    net::SocketAddr,
    ops::{Deref, DerefMut},
};

mod event_loop;
mod init;
mod starter;
mod terminating;

/// Define the application scope trait
pub trait BrokerScope: LauncherSender<BrokerBuilder<Self>> {}
impl<H: LauncherSender<BrokerBuilder<H>>> BrokerScope for H {}

// Scylla builder
builder!(
    #[derive(Clone)]
    BrokerBuilder<H> {
        listen_address: SocketAddr,
        listener_handle: ListenerHandle
});

#[derive(Deserialize, Serialize)]
/// It's the Interface of the broker app to dynamiclly configure the application during runtime
pub enum BrokerThrough {
    /// Shutdown json to gracefully shutdown broker app
    Shutdown,
    Topology(Topology),
}

/// BrokerHandle to be passed to the children
pub struct BrokerHandle<H: BrokerScope> {
    tx: tokio::sync::mpsc::UnboundedSender<BrokerEvent<H::AppsEvents>>,
}
/// BrokerInbox used to recv events
pub struct BrokerInbox<H: BrokerScope> {
    rx: tokio::sync::mpsc::UnboundedReceiver<BrokerEvent<H::AppsEvents>>,
}

impl<H: BrokerScope> Clone for BrokerHandle<H> {
    fn clone(&self) -> Self {
        BrokerHandle::<H> { tx: self.tx.clone() }
    }
}

/// Application state
pub struct PermanodeBroker<H: BrokerScope> {
    service: Service,
    websockets: HashMap<String, WsTx>,
    handle: Option<BrokerHandle<H>>,
    inbox: BrokerInbox<H>,
}

/// SubEvent type, indicated the children
pub enum BrokerChild {
    /// Used by Listener to keep broker up to date with its service
    Listener(Service),
    /// Used by Mqtt to keep Broker up to date with its service
    Mqtt(Service),
    /// Used by Websocket to keep Broker up to date with its service
    Websocket(Service, Option<WsTx>),
}

/// Event type of the broker Application
pub enum BrokerEvent<T> {
    /// It's the passthrough event, which the scylla application will receive from
    Passthrough(T),
    /// Used by broker children to push their service
    Children(BrokerChild),
}

#[derive(Deserialize, Serialize, Debug)]
/// Topology event
pub enum Topology {
    // todo!( add broker topology like adding new feed source or removing one)
}

#[derive(Deserialize, Serialize)]
// use PermanodeBroker to indicate to the msg is from/to PermanodeBroker
pub enum SocketMsg<T> {
    PermanodeBroker(T),
}

/// implementation of the AppBuilder
impl<H: BrokerScope> AppBuilder<H> for BrokerBuilder<H> {}

/// implementation of through type
impl<H: BrokerScope> ThroughType for BrokerBuilder<H> {
    type Through = BrokerThrough;
}

/// implementation of builder
impl<H: BrokerScope> Builder for BrokerBuilder<H> {
    type State = PermanodeBroker<H>;
    fn build(self) -> Self::State {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let handle = Some(BrokerHandle { tx });
        let inbox = BrokerInbox { rx };
        PermanodeBroker::<H> {
            service: Service::new(),
            websockets: HashMap::new(),
            handle,
            inbox,
        }
        .set_name()
    }
}

// TODO integrate well with other services;
/// implementation of passthrough functionality
impl<H: BrokerScope> Passthrough<BrokerThrough> for BrokerHandle<H> {
    fn launcher_status_change(&mut self, _service: &Service) {}
    fn app_status_change(&mut self, _service: &Service) {}
    fn passthrough(&mut self, _event: BrokerThrough, _from_app_name: String) {}
    fn service(&mut self, _service: &Service) {}
}

/// implementation of shutdown functionality
impl<H: BrokerScope> Shutdown for BrokerHandle<H> {
    fn shutdown(self) -> Option<Self>
    where
        Self: Sized,
    {
        let broker_shutdown: H::AppsEvents = serde_json::from_str("{\"PermanodeBroker\": \"Shutdown\"}").unwrap();
        let _ = self.send(BrokerEvent::Passthrough(broker_shutdown));
        None
    }
}

impl<H: BrokerScope> Deref for BrokerHandle<H> {
    type Target = tokio::sync::mpsc::UnboundedSender<BrokerEvent<H::AppsEvents>>;

    fn deref(&self) -> &Self::Target {
        &self.tx
    }
}

impl<H: BrokerScope> DerefMut for BrokerHandle<H> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.tx
    }
}

impl<H: BrokerScope> Deref for BrokerInbox<H> {
    type Target = tokio::sync::mpsc::UnboundedReceiver<BrokerEvent<H::AppsEvents>>;

    fn deref(&self) -> &Self::Target {
        &self.rx
    }
}

impl<H: BrokerScope> DerefMut for BrokerInbox<H> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.rx
    }
}

/// impl name of the application
impl<H: BrokerScope> Name for PermanodeBroker<H> {
    fn set_name(mut self) -> Self {
        self.service.update_name("PermanodeBroker".to_string());
        self
    }
    fn get_name(&self) -> String {
        self.service.get_name()
    }
}
