// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0
use super::*;
use crate::{
    archiver::*,
    collector::*,
    importer::*,
    listener::*,
    mqtt::*,
    requester::*,
    solidifier::*,
    syncer::*,
    websocket::*,
};
use async_trait::async_trait;
use chronicle_common::config::BrokerConfig;
use std::{
    ops::Range,
    str::FromStr,
    time::Duration,
};

mod event_loop;
mod init;
mod starter;
mod terminating;

/// Define the application scope trait
pub trait ChronicleBrokerScope: LauncherSender<ChronicleBrokerBuilder<Self>> {}
impl<H: LauncherSender<ChronicleBrokerBuilder<H>>> ChronicleBrokerScope for H {}

// Broker builder
builder!(
    #[derive(Clone)]
    ChronicleBrokerBuilder<H> {
        listener_handle: ListenerHandle,
        complete_gaps_interval_secs: u64,
        parallelism: u8,
        collector_count: u8
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
    importer_handles: HashMap<String, ImporterHandle>,
    asked_to_shutdown: HashMap<String, ()>,
    parallelism: u8,
    complete_gaps_interval: Duration,
    parallelism_points: u8,
    pending_imports: Vec<Topology>,
    in_progress_importers: usize,
    collector_count: u8,
    collector_handles: HashMap<u8, CollectorHandle>,
    solidifier_handles: HashMap<u8, SolidifierHandle>,
    logs_dir_path: Option<PathBuf>,
    handle: Option<BrokerHandle<H>>,
    inbox: BrokerInbox<H>,
    default_keyspace: ChronicleKeyspace,
    sync_range: SyncRange,
    sync_data: SyncData,
    syncer_handle: Option<SyncerHandle>,
}

/// SubEvent type, indicates the children
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
    /// Used by Importer to keep Broker up to date with its service, u8 is parallelism
    Importer(Service, Result<(), Need>, u8),
    /// Used by Websocket to keep Broker up to date with its service
    Websocket(Service, Option<WsTx>),
}

/// Event type of the broker Application
pub enum BrokerEvent<T> {
    /// Importer Session
    Importer(ImporterSession),
    /// It's the passthrough event, which the scylla application will receive from
    Passthrough(T),
    /// Used by broker children to push their service
    Children(BrokerChild),
    /// Used by Scylla to keep Broker up to date with scylla status
    Scylla(Service),
}

/// Topology event
#[derive(Deserialize, Serialize, Debug, Clone)]
pub enum Topology {
    /// Add new MQTT Messages feed source
    AddMqttMessages(Url),
    /// Add new MQTT Messages Referenced feed source
    AddMqttMessagesReferenced(Url),
    /// Remove a MQTT Messages feed source
    RemoveMqttMessages(Url),
    /// Remove a MQTT Messages Referenced feed source
    RemoveMqttMessagesReferenced(Url),
    /// Import a log file using the given url
    Import {
        /// File or dir path which supposed to contain LogFiles
        path: PathBuf,
        /// Resume the importing process
        resume: bool,
        /// Provide optional import range
        import_range: Option<Range<u32>>,
    },
    /// AddEndpoint
    Requesters(RequesterTopology),
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
                .unwrap_or("permanode".to_owned()),
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
        let logs_dir_path;
        if let Some(logs_dir) = config.broker_config.logs_dir {
            logs_dir_path = Some(PathBuf::from_str(&logs_dir).expect("Failed to parse configured logs path!"));
        } else {
            logs_dir_path = None;
        }
        let parallelism = self.parallelism.unwrap_or(25);
        ChronicleBroker::<H> {
            service: Service::new(),
            websockets: HashMap::new(),
            listener_handle: self.listener_handle,
            mqtt_handles: HashMap::new(),
            importer_handles: HashMap::new(),
            asked_to_shutdown: HashMap::new(),
            collector_count: self.collector_count.unwrap_or(10),
            collector_handles: HashMap::new(),
            solidifier_handles: HashMap::new(),
            syncer_handle: None,
            parallelism,
            parallelism_points: parallelism,
            pending_imports: Vec::new(),
            in_progress_importers: 0,
            logs_dir_path,
            handle,
            inbox,
            default_keyspace,
            sync_range,
            sync_data,
            complete_gaps_interval: Duration::from_secs(self.complete_gaps_interval_secs.unwrap()),
        }
        .set_name()
    }
}

/// implementation of passthrough functionality
impl<H: ChronicleBrokerScope> Passthrough<ChronicleBrokerThrough> for BrokerHandle<H> {
    fn launcher_status_change(&mut self, _service: &Service) {}
    fn app_status_change(&mut self, service: &Service) {
        if service.is_running() && service.get_name() == "Scylla" {
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
