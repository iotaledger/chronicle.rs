// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0
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
pub(crate) use backstage::*;
pub(crate) use bee_common::packable::Packable;
pub(crate) use bee_message::{
    Message,
    MessageId,
};
pub(crate) use chronicle_common::{
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
        reschedule_after: Duration,
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
    parallelism_points: u8,
    pending_imports: Vec<Topology>,
    in_progress_importers: usize,
    collector_count: u8,
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

/// Enum used by importer to keep the sockets up to date with most recent progress.
#[derive(Deserialize, Serialize, Debug)]
pub enum ImporterSession {
    /// Create/update progress bar state
    ProgressBar {
        /// Total size of the logfile
        log_file_size: u64,
        /// LogFile start range
        from_ms: u32,
        /// LogFile end range
        to_ms: u32,
        /// milestone data bytes size
        ms_bytes_size: usize,
        /// Milestone index
        milestone_index: u32,
        /// Identify whether it skipped/resume the milestone_index or imported.
        skipped: bool,
    },
    /// Finish the progress bar with message
    Finish {
        /// LogFile start range
        from_ms: u32,
        /// LogFile end range
        to_ms: u32,
        /// Finish the progress bar using this msg.
        msg: String,
    },
    /// Return error
    PathError {
        /// Invalid dir or file path
        path: PathBuf,
        /// Useful debug message
        msg: String,
    },
    /// Close session
    Close,
}

/// Topology event
#[derive(Deserialize, Serialize, Debug)]
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

#[derive(Deserialize, Serialize)]
/// Defines a message to/from the Broker or its children
pub enum SocketMsg<T> {
    /// A message to/from the Broker
    ChronicleBroker(T),
}

/// Representation of the database sync data
#[derive(Debug, Clone, Default)]
pub struct SyncData {
    /// The completed(synced and logged) milestones data
    pub(crate) completed: Vec<Range<u32>>,
    /// Synced milestones data but unlogged
    pub(crate) synced_but_unlogged: Vec<Range<u32>>,
    /// Gaps/missings milestones data
    pub(crate) gaps: Vec<Range<u32>>,
}

impl SyncData {
    /// Try to fetch the sync data from the sync table for the provided keyspace and sync range
    pub async fn try_fetch<S: 'static + Select<SyncRange, Iter<SyncRecord>>>(
        keyspace: &S,
        sync_range: &SyncRange,
        retries: usize,
    ) -> anyhow::Result<SyncData> {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let _ = keyspace
            .select(sync_range)
            .consistency(Consistency::One)
            .build()?
            .send_local(ValueWorker::boxed(
                tx,
                keyspace.clone(),
                sync_range.clone(),
                retries,
                std::marker::PhantomData,
            ));
        let select_response = rx
            .recv()
            .await
            .ok_or_else(|| anyhow::anyhow!("Expected Rx inbox to receive the sync data response"))??;
        let mut sync_data = SyncData::default();
        if let Some(mut sync_rows) = select_response {
            // Get the first row, note: the first row is always with the largest milestone_index
            let SyncRecord {
                milestone_index,
                logged_by,
                ..
            } = sync_rows.next().unwrap();
            // push missing row/gap (if any)
            sync_data.process_gaps(sync_range.to, *milestone_index);
            sync_data.process_rest(&logged_by, *milestone_index, &None);
            let mut pre_ms = milestone_index;
            let mut pre_lb = logged_by;
            // Generate and identify missing gaps in order to fill them
            while let Some(SyncRecord {
                milestone_index,
                logged_by,
                ..
            }) = sync_rows.next()
            {
                // check if there are any missings
                sync_data.process_gaps(*pre_ms, *milestone_index);
                sync_data.process_rest(&logged_by, *milestone_index, &pre_lb);
                pre_ms = milestone_index;
                pre_lb = logged_by;
            }
            // pre_ms is the most recent milestone we processed
            // it's also the lowest milestone index in the select response
            // so anything < pre_ms && anything >= (self.sync_range.from - 1)
            // (lower provided sync bound) are missing
            // push missing row/gap (if any)
            sync_data.process_gaps(*pre_ms, sync_range.from - 1);
            Ok(sync_data)
        } else {
            // Everything is missing as gaps
            sync_data.process_gaps(sync_range.to, sync_range.from - 1);
            Ok(sync_data)
        }
    }
    /// Takes the lowest gap from the sync_data
    pub fn take_lowest_gap(&mut self) -> Option<Range<u32>> {
        self.gaps.pop()
    }
    /// Takes the lowest unlogged range from the sync_data
    pub fn take_lowest_unlogged(&mut self) -> Option<Range<u32>> {
        self.synced_but_unlogged.pop()
    }
    /// Takes the lowest unlogged or gap from the sync_data
    pub fn take_lowest_gap_or_unlogged(&mut self) -> Option<Range<u32>> {
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
    /// Takes the lowest uncomplete(mixed range for unlogged and gap) from the sync_data
    pub fn take_lowest_uncomplete(&mut self) -> Option<Range<u32>> {
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
    fn process_rest(&mut self, logged_by: &Option<u8>, milestone_index: u32, pre_lb: &Option<u8>) {
        if logged_by.is_some() {
            // process logged
            Self::proceed(&mut self.completed, milestone_index, pre_lb.is_some());
        } else {
            // process_unlogged
            let unlogged = &mut self.synced_but_unlogged;
            Self::proceed(unlogged, milestone_index, pre_lb.is_none());
        }
    }
    fn process_gaps(&mut self, pre_ms: u32, milestone_index: u32) {
        let gap_start = milestone_index + 1;
        if gap_start != pre_ms {
            // create missing gap
            let gap = Range {
                start: gap_start,
                end: pre_ms,
            };
            self.gaps.push(gap);
        }
    }
    fn proceed(ranges: &mut Vec<Range<u32>>, milestone_index: u32, check: bool) {
        let end_ms = milestone_index + 1;
        if let Some(Range { start, .. }) = ranges.last_mut() {
            if check && *start == end_ms {
                *start = milestone_index;
            } else {
                let range = Range {
                    start: milestone_index,
                    end: end_ms,
                };
                ranges.push(range)
            }
        } else {
            let range = Range {
                start: milestone_index,
                end: end_ms,
            };
            ranges.push(range);
        };
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
        let logs_dir_path =
            PathBuf::from_str(&config.broker_config.logs_dir).expect("Failed to parse configured logs path!");
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
