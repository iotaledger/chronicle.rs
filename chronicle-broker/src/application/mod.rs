// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0
use crate::{
    archiver::*,
    collector::*,
    listener::*,
    mqtt::*,
    solidifier::*,
    syncer::*,
    websocket::*,
};
use async_trait::async_trait;
pub(crate) use backstage::*;
pub(crate) use bee_common::packable::Packable;
pub(crate) use bee_message::{
    Message,
    MessageId,
};
use chronicle_common::{
    config::MqttType,
    get_config,
    get_config_async,
    SyncRange,
};
pub(crate) use chronicle_storage::access::*;
pub(crate) use log::*;
pub(crate) use paho_mqtt::{
    AsyncClient,
    CreateOptionsBuilder,
};
use serde::{
    Deserialize,
    Serialize,
};
pub(crate) use std::{
    collections::HashMap,
    convert::TryFrom,
    ops::{
        Deref,
        DerefMut,
    },
    path::PathBuf,
};
use std::{
    ops::Range,
    str::FromStr,
    time::Duration,
};

pub use tokio::{
    spawn,
    sync::mpsc,
};

mod event_loop;
mod init;
mod starter;
mod terminating;

/// Define the application scope trait
pub trait ChronicleBrokerScope: LauncherSender<ChronicleBrokerBuilder<Self>> {}
impl<H: LauncherSender<ChronicleBrokerBuilder<H>>> ChronicleBrokerScope for H {}

// Scylla builder
builder!(
    #[derive(Clone)]
    ChronicleBrokerBuilder<H> {
        listener_handle: ListenerHandle,
        reschedule_after: Duration,
        collectors_count: u8
});

#[derive(Deserialize, Serialize)]
/// It's the Interface of the broker app to dynamiclly configure the application during runtime
pub enum ChronicleBrokerThrough {
    /// Shutdown json to gracefully shutdown broker app
    Shutdown,
    /// Alter the topology of the broker app
    Topology(Topology),
    /// Exit the broker app
    ExitProgram,
}

/// BrokerHandle to be passed to the children
pub struct BrokerHandle<H: ChronicleBrokerScope> {
    tx: tokio::sync::mpsc::UnboundedSender<BrokerEvent<H::AppsEvents>>,
}
/// BrokerInbox used to recv events
pub struct BrokerInbox<H: ChronicleBrokerScope> {
    rx: tokio::sync::mpsc::UnboundedReceiver<BrokerEvent<H::AppsEvents>>,
}

impl<H: ChronicleBrokerScope> Clone for BrokerHandle<H> {
    fn clone(&self) -> Self {
        BrokerHandle::<H> { tx: self.tx.clone() }
    }
}

/// Application state
pub struct ChronicleBroker<H: ChronicleBrokerScope> {
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
    default_keyspace: ChronicleKeyspace,
    sync_range: SyncRange,
    sync_data: SyncData,
    syncer_handle: Option<SyncerHandle>,
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
    Solidifier(Service, Result<(), Need>),
    /// Used by Archiver to keep Broker up to date with its service
    Archiver(Service, Result<(), Need>),
    /// Used by Syncer to keep Broker up to date with its service
    Syncer(Service, Result<(), Need>),
    /// Used by Websocket to keep Broker up to date with its service
    Websocket(Service, Option<WsTx>),
}

/// Event type of the broker Application
pub enum BrokerEvent<T> {
    /// It's the passthrough event, which the scylla application will receive from
    Passthrough(T),
    /// Used by broker children to push their service
    Children(BrokerChild),
    /// Used by Scylla to keep Broker up to date with scylla status
    Scylla(Service),
}

#[derive(Deserialize, Serialize, Debug)]
/// Topology event
pub enum Topology {
    /// Add new MQTT Messages feed source
    AddMqttMessages(Url),
    /// Add new MQTT Messages Referenced feed source
    AddMqttMessagesReferenced(Url),
    /// Remove a MQTT Messages feed source
    RemoveMqttMessages(Url),
    /// Remove a MQTT Messages Referenced feed source
    RemoveMqttMessagesReferenced(Url),
}

#[derive(Deserialize, Serialize)]
/// Defines a message to/from the Broker or its children
pub enum SocketMsg<T> {
    /// A message to/from the Broker
    ChronicleBroker(T),
}

/// Representation of the database sync data
#[derive(Debug, Clone)]
pub struct SyncData {
    /// The completed(synced and logged) milestones data
    pub(crate) completed: Vec<Range<u32>>,
    /// Synced milestones data but unlogged
    pub(crate) synced_but_unlogged: Vec<Range<u32>>,
    /// Gaps/missings milestones data
    pub(crate) gaps: Vec<Range<u32>>,
}

impl SyncData {
    pub(crate) fn take_lowest_gap(&mut self) -> Option<Range<u32>> {
        self.gaps.pop()
    }
    #[allow(dead_code)]
    pub(crate) fn take_lowest_unlogged(&mut self) -> Option<Range<u32>> {
        self.synced_but_unlogged.pop()
    }
    pub(crate) fn take_lowest_gap_or_unlogged(&mut self) -> Option<Range<u32>> {
        let lowest_gap = self.gaps.last();
        let lowest_unlogged = self.synced_but_unlogged.last();
        match (lowest_gap, lowest_unlogged) {
            (Some(gap), Some(unlogged)) => {
                if gap.start < unlogged.start {
                    self.gaps.pop()
                } else {
                    self.synced_but_unlogged.pop()
                }
            }
            (Some(_), None) => self.gaps.pop(),
            (None, Some(_)) => self.synced_but_unlogged.pop(),
            _ => None,
        }
    }
    pub(crate) fn take_lowest_uncomplete(&mut self) -> Option<Range<u32>> {
        if let Some(mut pre_range) = self.take_lowest_gap_or_unlogged() {
            loop {
                if let Some(next_range) = self.get_lowest_gap_or_unlogged() {
                    if next_range.start.eq(&pre_range.end) {
                        pre_range.end = next_range.end;
                        let _ = self.take_lowest_gap_or_unlogged();
                    } else {
                        return Some(pre_range);
                    }
                } else {
                    return Some(pre_range);
                }
            }
        } else {
            None
        }
    }
    fn get_lowest_gap_or_unlogged(&self) -> Option<&Range<u32>> {
        let lowest_gap = self.gaps.last();
        let lowest_unlogged = self.synced_but_unlogged.last();
        match (lowest_gap, lowest_unlogged) {
            (Some(gap), Some(unlogged)) => {
                if gap.start < unlogged.start {
                    self.gaps.last()
                } else {
                    self.synced_but_unlogged.last()
                }
            }
            (Some(_), None) => self.gaps.last(),
            (None, Some(_)) => self.synced_but_unlogged.last(),
            _ => None,
        }
    }
}
/// implementation of the AppBuilder
impl<H: ChronicleBrokerScope> AppBuilder<H> for ChronicleBrokerBuilder<H> {}

/// implementation of through type
impl<H: ChronicleBrokerScope> ThroughType for ChronicleBrokerBuilder<H> {
    type Through = ChronicleBrokerThrough;
}

/// implementation of builder
impl<H: ChronicleBrokerScope> Builder for ChronicleBrokerBuilder<H> {
    type State = ChronicleBroker<H>;
    fn build(self) -> Self::State {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let handle = Some(BrokerHandle { tx });
        let inbox = BrokerInbox { rx };
        let config = get_config();
        let default_keyspace = ChronicleKeyspace::new(
            config
                .storage_config
                .keyspaces
                .first()
                .and_then(|keyspace| Some(keyspace.name.clone()))
                .unwrap_or("chronicle".to_owned()),
        );
        let sync_range = config
            .broker_config
            .sync_range
            .and_then(|range| Some(range))
            .unwrap_or(SyncRange::default());
        let sync_data = SyncData {
            completed: Vec::new(),
            synced_but_unlogged: Vec::new(),
            gaps: Vec::new(),
        };
        let logs_dir_path =
            PathBuf::from_str(&config.broker_config.logs_dir).expect("Failed to parse configured logs path!");
        ChronicleBroker::<H> {
            service: Service::new(),
            websockets: HashMap::new(),
            listener_handle: self.listener_handle,
            mqtt_handles: HashMap::new(),
            asked_to_shutdown: HashMap::new(),
            collectors_count: self.collectors_count.unwrap_or(10),
            collector_handles: HashMap::new(),
            solidifier_handles: HashMap::new(),
            syncer_handle: None,
            logs_dir_path,
            handle,
            inbox,
            default_keyspace,
            sync_range,
            sync_data,
        }
        .set_name()
    }
}

// TODO integrate well with other services;
/// implementation of passthrough functionality
impl<H: ChronicleBrokerScope> Passthrough<ChronicleBrokerThrough> for BrokerHandle<H> {
    fn launcher_status_change(&mut self, _service: &Service) {}
    fn app_status_change(&mut self, service: &Service) {
        if service.is_running() && service.get_name().eq(&"Scylla") {
            let _ = self.send(BrokerEvent::Scylla(service.clone()));
        }
    }
    fn passthrough(&mut self, _event: ChronicleBrokerThrough, _from_app_name: String) {}
    fn service(&mut self, _service: &Service) {}
}

/// implementation of shutdown functionality
impl<H: ChronicleBrokerScope> Shutdown for BrokerHandle<H> {
    fn shutdown(self) -> Option<Self>
    where
        Self: Sized,
    {
        let broker_shutdown: H::AppsEvents = serde_json::from_str("{\"ChronicleBroker\": \"Shutdown\"}").unwrap();
        let _ = self.send(BrokerEvent::Passthrough(broker_shutdown));
        None
    }
}

impl<H: ChronicleBrokerScope> Deref for BrokerHandle<H> {
    type Target = tokio::sync::mpsc::UnboundedSender<BrokerEvent<H::AppsEvents>>;

    fn deref(&self) -> &Self::Target {
        &self.tx
    }
}

impl<H: ChronicleBrokerScope> DerefMut for BrokerHandle<H> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.tx
    }
}

impl<H: ChronicleBrokerScope> Deref for BrokerInbox<H> {
    type Target = tokio::sync::mpsc::UnboundedReceiver<BrokerEvent<H::AppsEvents>>;

    fn deref(&self) -> &Self::Target {
        &self.rx
    }
}

impl<H: ChronicleBrokerScope> DerefMut for BrokerInbox<H> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.rx
    }
}

/// impl name of the application
impl<H: ChronicleBrokerScope> Name for ChronicleBroker<H> {
    fn set_name(mut self) -> Self {
        self.service.update_name("ChronicleBroker".to_string());
        self
    }
    fn get_name(&self) -> String {
        self.service.get_name()
    }
}
