// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0
use crate::{
    collector::*,
    config::BrokerConfig,
    listener::*,
    logger::*,
    mqtt::*,
    solidifier::*,
    websocket::*,
};

use async_trait::async_trait;
pub use chronicle::*;
pub use log::*;
pub(crate) use paho_mqtt::{
    AsyncClient,
    CreateOptionsBuilder,
};
pub(crate) use permanode_storage::{
    access::*,
    StorageConfig,
};
use serde::{
    Deserialize,
    Serialize,
};
pub(crate) use std::{
    collections::HashMap,
    convert::TryFrom,
    net::SocketAddr,
    ops::{
        Deref,
        DerefMut,
    },
    path::PathBuf,
};

pub use tokio::{
    spawn,
    sync::mpsc,
};

pub(crate) use bee_common::packable::Packable;
pub(crate) use bee_message::{
    Message,
    MessageId,
};

mod event_loop;
mod init;
mod starter;
mod terminating;

/// Define the application scope trait
pub trait PermanodeBrokerScope: LauncherSender<PermanodeBrokerBuilder<Self>> {}
impl<H: LauncherSender<PermanodeBrokerBuilder<H>>> PermanodeBrokerScope for H {}

// Scylla builder
builder!(
    #[derive(Clone)]
    PermanodeBrokerBuilder<H> {
        listen_address: SocketAddr,
        listener_handle: ListenerHandle,
        logs_dir_path: PathBuf,
        collectors_count: u8,
        broker_config: BrokerConfig,
        storage_config: StorageConfig
});

#[derive(Deserialize, Serialize)]
/// It's the Interface of the broker app to dynamiclly configure the application during runtime
pub enum PermanodeBrokerThrough {
    /// Shutdown json to gracefully shutdown broker app
    Shutdown,
    Topology(Topology),
}

/// BrokerHandle to be passed to the children
pub struct BrokerHandle<H: PermanodeBrokerScope> {
    tx: tokio::sync::mpsc::UnboundedSender<BrokerEvent<H::AppsEvents>>,
}
/// BrokerInbox used to recv events
pub struct BrokerInbox<H: PermanodeBrokerScope> {
    rx: tokio::sync::mpsc::UnboundedReceiver<BrokerEvent<H::AppsEvents>>,
}

impl<H: PermanodeBrokerScope> Clone for BrokerHandle<H> {
    fn clone(&self) -> Self {
        BrokerHandle::<H> { tx: self.tx.clone() }
    }
}

/// Application state
pub struct PermanodeBroker<H: PermanodeBrokerScope> {
    service: Service,
    websockets: HashMap<String, WsTx>,
    listener_handle: Option<ListenerHandle>,
    mqtt_handles: HashMap<String, MqttHandle>,
    asked_to_shutdown: HashMap<String, ()>,
    collectors_count: u8,
    collector_handles: HashMap<u8, CollectorHandle>,
    solidifier_handles: HashMap<u8, SolidifierHandle>,
    logs_dir_path: PathBuf,
    handle: Option<BrokerHandle<H>>,
    inbox: BrokerInbox<H>,
    broker_config: BrokerConfig,
    storage_config: Option<StorageConfig>,
}

/// SubEvent type, indicated the children
pub enum BrokerChild {
    /// Used by Listener to keep broker up to date with its service
    Listener(Service),
    /// Used by Mqtt to keep Broker up to date with its service
    Mqtt(Service, Option<MqttHandle>, Result<(), Need>),
    /// Used by Collector(s) to keep Broker up to date with its service
    Collector(Service),
    /// Used by Solidifier(s) to keep Broker up to date with its service
    Solidifier(Service),
    /// Used by Logger to keep Broker up to date with its service
    Logger(Service, Result<(), Need>),
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
    AddMqttMessages(Url),
    AddMqttMessagesReferenced(Url),
    RemoveMqttMessages(Url),
    RemoveMqttMessagesReferenced(Url),
}

#[derive(Deserialize, Serialize)]
// use PermanodeBroker to indicate to the msg is from/to PermanodeBroker
pub enum SocketMsg<T> {
    PermanodeBroker(T),
}

/// implementation of the AppBuilder
impl<H: PermanodeBrokerScope> AppBuilder<H> for PermanodeBrokerBuilder<H> {}

/// implementation of through type
impl<H: PermanodeBrokerScope> ThroughType for PermanodeBrokerBuilder<H> {
    type Through = PermanodeBrokerThrough;
}

/// implementation of builder
impl<H: PermanodeBrokerScope> Builder for PermanodeBrokerBuilder<H> {
    type State = PermanodeBroker<H>;
    fn build(self) -> Self::State {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let handle = Some(BrokerHandle { tx });
        let inbox = BrokerInbox { rx };
        PermanodeBroker::<H> {
            service: Service::new(),
            websockets: HashMap::new(),
            listener_handle: self.listener_handle,
            mqtt_handles: HashMap::new(),
            asked_to_shutdown: HashMap::new(),
            collectors_count: self.collectors_count.unwrap_or(10),
            collector_handles: HashMap::new(),
            solidifier_handles: HashMap::new(),
            logs_dir_path: self.logs_dir_path.expect("Expected logs directory path"),
            handle,
            inbox,
            broker_config: self.broker_config.unwrap(),
            storage_config: self.storage_config,
        }
        .set_name()
    }
}

// TODO integrate well with other services;
/// implementation of passthrough functionality
impl<H: PermanodeBrokerScope> Passthrough<PermanodeBrokerThrough> for BrokerHandle<H> {
    fn launcher_status_change(&mut self, _service: &Service) {}
    fn app_status_change(&mut self, _service: &Service) {}
    fn passthrough(&mut self, _event: PermanodeBrokerThrough, _from_app_name: String) {}
    fn service(&mut self, _service: &Service) {}
}

/// implementation of shutdown functionality
impl<H: PermanodeBrokerScope> Shutdown for BrokerHandle<H> {
    fn shutdown(self) -> Option<Self>
    where
        Self: Sized,
    {
        let broker_shutdown: H::AppsEvents = serde_json::from_str("{\"PermanodeBroker\": \"Shutdown\"}").unwrap();
        let _ = self.send(BrokerEvent::Passthrough(broker_shutdown));
        None
    }
}

impl<H: PermanodeBrokerScope> Deref for BrokerHandle<H> {
    type Target = tokio::sync::mpsc::UnboundedSender<BrokerEvent<H::AppsEvents>>;

    fn deref(&self) -> &Self::Target {
        &self.tx
    }
}

impl<H: PermanodeBrokerScope> DerefMut for BrokerHandle<H> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.tx
    }
}

impl<H: PermanodeBrokerScope> Deref for BrokerInbox<H> {
    type Target = tokio::sync::mpsc::UnboundedReceiver<BrokerEvent<H::AppsEvents>>;

    fn deref(&self) -> &Self::Target {
        &self.rx
    }
}

impl<H: PermanodeBrokerScope> DerefMut for BrokerInbox<H> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.rx
    }
}

/// impl name of the application
impl<H: PermanodeBrokerScope> Name for PermanodeBroker<H> {
    fn set_name(mut self) -> Self {
        self.service.update_name("PermanodeBroker".to_string());
        self
    }
    fn get_name(&self) -> String {
        self.service.get_name()
    }
}
