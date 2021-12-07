use super::{
    archiver::Archiver,
    collector::{
        Collector,
        CollectorHandle,
        MessageIdPartitioner,
    },
    mqtt::Mqtt,
    requester::{
        Requester,
        RequesterHandles,
    },
    solidifier::{
        Solidifier,
        SolidifierHandle,
    },
    syncer::Syncer,
};
use crate::SyncRange;
use anyhow::{
    anyhow,
    bail,
    Result,
};
use async_trait::async_trait;
use backstage::{
    core::{
        Actor,
        ActorError,
        ActorRequest,
        ActorResult,
        EolEvent,
        Event,
        ReportEvent,
        Rt,
        ScopeId,
        Service,
        ServiceStatus,
        Shutdown,
        ShutdownEvent,
        StreamExt,
        SupHandle,
        UnboundedChannel,
        UnboundedHandle,
    },
    prefab::websocket::{
        GenericResponder,
        JsonMessage,
        Responder,
    },
};
use bee_message::Message;
use chronicle_common::config::PartitionConfig;
use chronicle_filter::SelectiveBuilder;
use chronicle_storage::{
    access::{
        MessageMetadata,
        SyncData,
    },
    keyspaces::ChronicleKeyspace,
};
use serde::{
    Deserialize,
    Serialize,
};
use std::{
    collections::{
        HashMap,
        HashSet,
    },
    ops::Range,
    path::PathBuf,
    time::Duration,
};
use thiserror::Error;
use url::Url;
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
/// Chronicle Broker config/state
pub struct ChronicleBroker<T: SelectiveBuilder> {
    pub parallelism: u8,
    pub complete_gaps_interval: Duration,
    pub partition_count: u8,
    pub retries: u8,
    pub logs_dir: Option<PathBuf>,
    pub max_log_size: Option<u64>,
    pub mqtt_brokers: HashSet<Url>,
    /// Mqtt stream capacity and lru capacity per collector
    pub cache_capacity: usize,
    pub api_endpoints: HashSet<Url>,
    request_timeout_secs: u8,
    pub keyspace: ChronicleKeyspace,
    pub partition_config: PartitionConfig,
    pub sync_range: SyncRange,
    pub selective_builder: T,
}

impl<T: SelectiveBuilder> Default for ChronicleBroker<T> {
    fn default() -> Self {
        Self {
            parallelism: 25,
            complete_gaps_interval: Duration::from_secs(60 * 60),
            partition_count: 10,
            logs_dir: Some("chronicle/logs/".into()),
            max_log_size: Some(super::archiver::MAX_LOG_SIZE),
            cache_capacity: 10000,
            request_timeout_secs: 5,
            retries: 5,
            keyspace: Default::default(),
            api_endpoints: Default::default(),
            partition_config: Default::default(),
            sync_range: Default::default(),
            mqtt_brokers: Default::default(),
            selective_builder: Default::default(),
        }
    }
}

impl<T: SelectiveBuilder> ChronicleBroker<T> {
    /// Create new chronicle broker instance
    pub fn new(
        keyspace: ChronicleKeyspace,
        partition_config: chronicle_common::config::PartitionConfig,
        retries: u8,
        parallelism: u8,
        gaps_interval: Duration,
        partition_count: u8,
        logs_dir: Option<PathBuf>,
        mut max_log_size: Option<u64>,
        sync_range: SyncRange,
        cache_capacity: usize,
        request_timeout_secs: u8,
        selective_builder: T,
    ) -> Self {
        if logs_dir.is_some() && max_log_size.is_none() {
            max_log_size = Some(super::archiver::MAX_LOG_SIZE);
        }
        Self {
            keyspace,
            parallelism,
            retries,
            complete_gaps_interval: gaps_interval,
            partition_count,
            logs_dir,
            sync_range,
            partition_config,
            max_log_size,
            mqtt_brokers: HashSet::new(),
            api_endpoints: HashSet::new(),
            request_timeout_secs,
            cache_capacity,
            selective_builder,
        }
    }
    /// Add mqtt broker, and verify it.
    pub async fn add_mqtt(&mut self, mqtt: Url) -> Result<&mut Self> {
        let random_id: u64 = rand::random();
        let create_opts = paho_mqtt::create_options::CreateOptionsBuilder::new()
            .server_uri(mqtt.as_str())
            .client_id(&format!("{}|{}", "verifier", random_id))
            .persistence(None)
            .finalize();
        let _client = paho_mqtt::AsyncClient::new(create_opts)
            .map_err(|e| anyhow::anyhow!("Error verifying mqtt broker {}: {}", mqtt, e))?;
        self.mqtt_brokers.insert(mqtt);
        Ok(self)
    }
    fn contain_mqtt(&self, mqtt: &Url) -> bool {
        self.mqtt_brokers.contains(mqtt)
    }
    /// Add api endpoint, and verify it.
    pub async fn add_endpoint(&mut self, endpoint: &mut Url) -> Result<&mut Self> {
        let path = endpoint.as_str();
        if path.is_empty() {
            bail!("Empty endpoint provided!");
        }
        if !path.ends_with("/") {
            *endpoint = format!("{}/", path).parse()?;
        }
        Self::verify_endpoint(endpoint).await?;
        self.api_endpoints.insert(endpoint.clone());
        Ok(self)
    }
    /// Verify if the IOTA api endpoint is active and correct
    async fn verify_endpoint(endpoint: &Url) -> anyhow::Result<()> {
        let client = reqwest::Client::new();
        let res = client
            .get(
                endpoint
                    .join("info")
                    .map_err(|e| anyhow!("Error verifying endpoint {}: {}", endpoint, e))?,
            )
            .send()
            .await
            .map_err(|e| anyhow!("Error verifying endpoint {}: {}", endpoint, e))?;
        if !res.status().is_success() {
            let url = res.url().clone();
            let err = res.json::<serde_json::Value>().await;
            bail!(
                "Error verifying endpoint \"{}\"\nRequest URL: \"{}\"\nResult: {:#?}",
                endpoint,
                url,
                err
            );
        }
        Ok(())
    }
    /// Remove mqtt broker
    pub fn remove_mqtt(&mut self, mqtt: &Url) -> bool {
        self.mqtt_brokers.remove(mqtt)
    }
    /// Remove api endpoint
    pub fn remove_endpoint(&mut self, endpoint: &Url) -> bool {
        self.api_endpoints.remove(endpoint)
    }
}

/// Event type of the broker Application
//#[derive(Debug)]
pub enum BrokerEvent {
    /// Request the cluster handle
    Topology(Topology, Option<TopologyResponder>),
    /// Used by scylla children to push their service
    Microservice(ScopeId, Service, Option<ActorResult<()>>),
    /// Scylla Service (outsourcing)
    Scylla(Event<Service>),
    /// Shutdown signal
    Shutdown,
}

impl<T> EolEvent<T> for BrokerEvent {
    fn eol_event(scope_id: ScopeId, service: Service, _actor: T, r: ActorResult<()>) -> Self {
        Self::Microservice(scope_id, service, Some(r))
    }
}

impl<T> ReportEvent<T> for BrokerEvent {
    fn report_event(scope_id: ScopeId, service: Service) -> Self {
        Self::Microservice(scope_id, service, None)
    }
}

impl ShutdownEvent for BrokerEvent {
    fn shutdown_event() -> Self {
        Self::Shutdown
    }
}

impl From<Event<Service>> for BrokerEvent {
    fn from(service_event: Event<Service>) -> Self {
        Self::Scylla(service_event)
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
/// The topology enum of the broker
pub enum Topology {
    // Import,
    /// Add mqtt feed source
    AddMqtt(Url),
    /// Remove an existing mqtt feed source
    RemoveMqtt(Url),
    /// Import a log file using the given url
    Import {
        /// File or dir path which supposed to contain LogFiles
        path: PathBuf,
        /// Resume the importing process
        resume: bool,
        /// Provide optional import range
        import_range: Option<Range<u32>>,
        /// The type of import requested
        import_type: ImportType,
    },
}

/// Import types
#[derive(Debug, Deserialize, Serialize, PartialEq, Eq, Copy, Clone)]
pub enum ImportType {
    /// Import everything
    All,
    /// Import only Analytics data
    Analytics,
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

/// Topology responder
pub enum TopologyResponder {
    /// Websocket responder
    WsResponder(Responder),
    /// Mpsc Responder
    Mpsc(tokio::sync::mpsc::UnboundedSender<TopologyResponse>),
}

impl TopologyResponder {
    async fn reply(&self, response: TopologyResponse) -> anyhow::Result<()> {
        match self {
            Self::WsResponder(r) => r.inner_reply(response).await,
            Self::Mpsc(tx) => tx.send(response).map_err(|_| anyhow::Error::msg("caller out of scope")),
        }
    }
}

/// The topology response, sent after the cluster processes a topology event
pub type TopologyResponse = Result<Topology, TopologyErr>;

#[derive(serde::Deserialize, serde::Serialize, Debug, Error)]
#[error("message: {message:?}")]
/// Topology error,
pub struct TopologyErr {
    message: String,
}

impl TopologyErr {
    fn new(message: String) -> Self {
        Self { message }
    }
}

#[async_trait]
impl<S: SupHandle<Self>, T: SelectiveBuilder> Actor<S> for ChronicleBroker<T> {
    type Data = (Service, T::State);
    type Channel = UnboundedChannel<BrokerEvent>;
    async fn init(&mut self, rt: &mut Rt<Self, S>) -> ActorResult<Self::Data> {
        log::info!("ChronicleBroker is initializing");
        // register self as resource
        rt.add_resource(self.clone()).await;
        // subscribe to scylla service
        let scylla_scope_id = rt.sibling("scylla").scope_id().await.ok_or_else(|| {
            log::error!("Scylla doesn't exist as sibling");
            ActorError::exit_msg("ChronicleBroker doesn't have Scylla as sibling")
        })?;
        let scylla_service = rt
            .subscribe::<Service>(scylla_scope_id, "scylla".into())
            .await
            .map_err(|e| {
                log::error!(
                    "ChronicleBroker cannot proceed initializing without an existing scylla in scope: {}",
                    e
                );
                ActorError::exit(e)
            })?
            .ok_or_else(|| {
                log::error!("ChronicleBroker cannot proceed initializing without scylla service");
                ActorError::exit_msg("ChronicleBroker cannot proceed initializing without scylla service")
            })?;
        // build Selective mode
        let selective = self.selective_builder.clone().build().await.map_err(|e| {
            log::error!("Broker unable to build selective");
            ActorError::exit_msg(format!("Broker unable to build selective: {}", e))
        })?;
        if let Ok(sync_data) = self.query_sync_table().await {
            // start only if there is at least one mqtt feed source in each topic (messages and refmessages)
            self.maybe_start(rt, sync_data, &selective).await?; //?
        } else {
            rt.update_status(ServiceStatus::Idle).await;
        };
        Ok((scylla_service, selective))
    }
    async fn run(&mut self, rt: &mut Rt<Self, S>, (mut scylla_service, selective): Self::Data) -> ActorResult<()> {
        log::info!("ChronicleBroker is {}", rt.service().status());
        while let Some(event) = rt.inbox_mut().next().await {
            match event {
                BrokerEvent::Topology(topology, responder_opt) => {
                    // only configure topology if it's not stopping
                    if rt.service().is_stopping() {
                        // response
                        log::warn!("Cannot configure topology while the broker is stopping");
                        // todo!("responde to responder")
                        if let Some(responder) = responder_opt.as_ref() {
                            let ok_response: Result<_, TopologyErr> = Err(TopologyErr::new(format!(
                                "Cannot configure topology while the broker is stopping",
                            )));
                            responder.reply(ok_response).await.ok();
                        }
                        continue;
                    }
                    match topology {
                        Topology::AddMqtt(mqtt) => {
                            // check if it's already exit
                            if self.contain_mqtt(&mqtt) {
                                // todo!("responde to responder")
                                if let Some(responder) = responder_opt.as_ref() {
                                    log::warn!("Cannot add an existing mqtt: {}", mqtt);
                                    let ok_response: Result<_, TopologyErr> =
                                        Err(TopologyErr::new(format!("Cannot add an existing mqtt: {}", mqtt)));
                                    responder.reply(ok_response).await.ok();
                                    continue;
                                } // else the mqtt will be readded
                            } else if responder_opt.is_none() {
                                // skip re-adding mqtt, because it got removed
                                log::warn!("skipping re-adding a {} mqtt, as it got removed", mqtt);
                                continue;
                            }
                            let partitioner = MessageIdPartitioner::new(self.partition_count);
                            let mqtt_messages = Mqtt::<Message>::new(mqtt.clone(), self.cache_capacity, partitioner);
                            let mqtt_msg_ref =
                                Mqtt::<MessageMetadata>::new(mqtt.clone(), self.cache_capacity, partitioner);
                            let dir = format!("messages@{}", mqtt);
                            match rt.start(dir, mqtt_messages).await {
                                Ok(h) => {
                                    // try to start mqtt_msg_ref feed_source
                                    let dir = format!("referenced@{}", mqtt);
                                    if let Err(e) = rt.start(dir, mqtt_msg_ref).await {
                                        log::error!("{}", e);
                                        // shutdown the messages feeder
                                        rt.shutdown_child(&h.scope_id())
                                            .await
                                            .expect("Expected the messages mqtt join handle to exist")
                                            .await
                                            .ok();
                                        log::error!("Unable to add referenced mqtt: {}, error: {}", mqtt, e);
                                        if let Some(responder) = responder_opt {
                                            let err_response: Result<_, TopologyErr> = Err(TopologyErr::new(format!(
                                                "unable to add {} mqtt, error: {}",
                                                mqtt, e
                                            )));
                                            responder.reply(err_response).await.ok();
                                        } else {
                                            rt.handle().send_after(
                                                BrokerEvent::Topology(Topology::AddMqtt(mqtt.clone()), None),
                                                Duration::from_secs(10),
                                            );
                                        }
                                    } else {
                                        log::info!("Successfully added mqtt: {}", &mqtt);
                                        self.mqtt_brokers.insert(mqtt.clone());
                                        let ok_response: Result<_, TopologyErr> = Ok(Topology::AddMqtt(mqtt));
                                        if let Some(responder) = responder_opt {
                                            responder.reply(ok_response).await.ok();
                                        }
                                    }
                                }
                                Err(e) => {
                                    log::error!("Unable to add messages mqtt: {}, error: {}", mqtt, e);
                                    if let Some(responder) = responder_opt.as_ref() {
                                        let err_response: Result<_, TopologyErr> = Err(TopologyErr::new(format!(
                                            "unable to add messages {} mqtt, error: {}",
                                            mqtt, e
                                        )));
                                        responder.reply(err_response).await.ok();
                                    } else {
                                        rt.handle().send_after(
                                            BrokerEvent::Topology(Topology::AddMqtt(mqtt.clone()), None),
                                            Duration::from_secs(10),
                                        );
                                    }
                                }
                            }
                        }
                        Topology::RemoveMqtt(mqtt) => {
                            let responder = responder_opt.as_ref().ok_or_else(|| {
                                ActorError::exit_msg("cannot use remove mqtt topology variant without responder")
                            })?;
                            log::info!("Removing {} mqtt!", mqtt);
                            if self.mqtt_brokers.remove(&mqtt) {
                                let mqtt_scope_ids: Vec<usize> = rt
                                    .service()
                                    .microservices()
                                    .iter()
                                    .filter_map(|(scope_id, ms)| {
                                        let dir_name = ms.directory();
                                        let is_mqtt_msgs = dir_name == &Some(format!("messages@{}", mqtt));
                                        let is_mqtt_msg_ref = dir_name == &Some(format!("referenced@{}", mqtt));
                                        if is_mqtt_msgs || is_mqtt_msg_ref {
                                            Some(*scope_id)
                                        } else {
                                            None
                                        }
                                    })
                                    .collect();
                                for mqtt_scope_id in mqtt_scope_ids {
                                    if let Some(join_handle) = rt.shutdown_child(&mqtt_scope_id).await {
                                        join_handle.await.ok();
                                    }
                                }
                                log::info!("Removed {} mqtt!", mqtt);
                                let ok_response: Result<_, TopologyErr> = Ok(Topology::RemoveMqtt(mqtt.clone()));
                                responder.reply(ok_response).await.ok();
                            } else {
                                log::error!("unable to remove non-existing {} mqtt!", mqtt);
                                // Cannot remove non-existing mqtt.
                                let error_response: Result<Topology, _> =
                                    Err(TopologyErr::new(format!("unable to remove non-existing {} mqtt", mqtt)));
                                responder.reply(error_response).await.ok();
                            };
                        }
                        Topology::Import { .. } => {
                            todo!("handle importer")
                        }
                    }
                }
                BrokerEvent::Microservice(scope_id, service, service_result) => {
                    // exit broker if any child had an exit error
                    match service_result.as_ref() {
                        Some(Err(ActorError {
                            request: Some(ActorRequest::Exit),
                            ..
                        })) => service_result.expect("Failed to unwrap service_result in broker")?,
                        Some(Err(e)) => {
                            // a child might shutdown due to overload in scylla, where scylla still running
                            // todo rt.shutdown_children().await;
                        }
                        _ => {}
                    }
                    rt.upsert_microservice(scope_id, service);
                    // if children got shutdown but scylla is running
                    if !rt.service().is_stopping() {
                        if rt.microservices_stopped() && (scylla_service.is_running() || scylla_service.is_degraded()) {
                            if let Ok(sync_data) = self.query_sync_table().await {
                                // todo maybe sleep for 10 seconds in-case the shutdown was due to overload in scylla
                                // todo self.maybe_start(rt, sync_data, &selective).await?;
                            }
                        }
                    } else {
                        if rt.microservices_stopped() {
                            rt.inbox_mut().close();
                        }
                    }
                }
                BrokerEvent::Scylla(service) => {
                    if let Event::Published(_, _, scylla_service_res) = service {
                        // check if children already started
                        let children_dont_exist = rt.service().microservices().is_empty();
                        // start children if not already started
                        if (scylla_service_res.is_running() || scylla_service_res.is_degraded()) && children_dont_exist
                        {
                            if let Ok(sync_data) = self.query_sync_table().await {
                                self.maybe_start(rt, sync_data, &selective).await?;
                            } else {
                                return Err(ActorError::restart_msg(
                                    format!("Unable to query sync table, scylla is {}", scylla_service_res.status()),
                                    Some(std::time::Duration::from_secs(60)),
                                ));
                            }
                        } else if scylla_service_res.is_idle() | scylla_service_res.is_outage() {
                            // shutdown children (if any)
                            rt.shutdown_children().await;
                        }
                        scylla_service = scylla_service_res;
                    } else {
                        // is dropped, stop the whole broker with Restart request
                        return Err(ActorError::restart_msg("Scylla service got stopped", None));
                    }
                }
                BrokerEvent::Shutdown => rt.stop().await,
            }
        }
        log::info!("ChronicleBroker exited its event loop");
        Ok(())
    }
}

impl<T: SelectiveBuilder> ChronicleBroker<T> {
    async fn maybe_start<S: SupHandle<Self>>(
        &self,
        rt: &mut Rt<Self, S>,
        sync_data: SyncData,
        selective: &T::State,
    ) -> ActorResult<()> {
        // start only if there is at least one mqtt feed source in each topic (messages and refmessages)
        if self.mqtt_brokers.is_empty() || self.api_endpoints.is_empty() {
            log::warn!("Cannot start children of the broker without at least one broker and endpoint!");
            rt.update_status(ServiceStatus::Idle).await;
            return Ok(());
        }
        // First remove the old resources ( if any, as this is possible if maybe_start is invoked to restart the
        // children)
        rt.remove_resource::<HashMap<u8, CollectorHandle>>().await;
        rt.remove_resource::<RequesterHandles<T>>().await;
        rt.remove_resource::<HashMap<u8, SolidifierHandle>>().await;
        // -- spawn feed sources (mqtt)
        let mqtt_brokers = self.mqtt_brokers.iter();
        let mut balanced = false;
        for url in mqtt_brokers {
            let partitioner = MessageIdPartitioner::new(self.partition_count);
            let mqtt_messages = Mqtt::<Message>::new(url.clone(), self.cache_capacity, partitioner);
            let mqtt_msg_ref = Mqtt::<MessageMetadata>::new(url.clone(), self.cache_capacity, partitioner);
            // try to start mqtt_message feed_source
            let dir = format!("messages@{}", url);
            match rt.spawn(dir, mqtt_messages).await {
                Ok((h, _signal)) => {
                    // try to start mqtt_msg_ref feed_source
                    let dir = format!("referenced@{}", url);
                    if let Err(e) = rt.spawn(dir, mqtt_msg_ref).await {
                        log::error!("{}", e);
                        // shutdown the messages feeder
                        h.shutdown().await;
                        // try to add it later
                        rt.handle().send_after(
                            BrokerEvent::Topology(Topology::AddMqtt(url.clone()), None),
                            Duration::from_secs(10),
                        );
                    } else {
                        balanced = true;
                    }
                }
                Err(e) => {
                    log::error!("{}", e);
                    // try to add it later
                    rt.handle().send_after(
                        BrokerEvent::Topology(Topology::AddMqtt(url.clone()), None),
                        Duration::from_secs(10),
                    );
                }
            }
        }
        // check if mqtts are in healthy state, else we shutdown everything;
        if !balanced {
            log::error!("Unable to reach balanced feed sources state");
            rt.update_status(ServiceStatus::Outage).await;
            return Ok(());
        }
        // -- (optional) start archiver
        if let Some(dir_path) = self.logs_dir.as_ref() {
            let keyspace = self.keyspace.clone();
            let next = sync_data.gaps.first().map(|r| r.start).unwrap_or(1);
            let max_log_size = self.max_log_size;
            let archiver = Archiver::new(dir_path, keyspace, next, max_log_size);
            rt.start("archiver".to_string(), archiver).await?;
        }
        let mut collector_handles = HashMap::new();
        let mut requester_handles = RequesterHandles::<T>::new();
        let mut solidifier_handles = HashMap::new();
        let mut initialized_rx = Vec::new();
        let gap_start = sync_data.gaps.first().expect("Invalid gap start").start;
        let reqwest_client = reqwest::Client::builder()
            .timeout(Duration::from_secs(self.request_timeout_secs.into()))
            .build()
            .expect("Expected reqwest client to build correctly");
        // -- spawn syncer
        let sync_range = self.sync_range;
        let update_sync_data_every = self.complete_gaps_interval;
        let parallelism = self.parallelism;
        let keyspace = self.keyspace.clone();
        let syncer = Syncer::new(sync_data, sync_range, update_sync_data_every, parallelism, keyspace);
        rt.spawn("syncer".to_string(), syncer).await?;
        // -- spawn collectors and solidifiers
        for partition_id in 0..self.partition_count {
            let collector = Collector::<T>::new(
                self.keyspace.clone(),
                self.partition_config.clone(),
                partition_id,
                self.partition_count,
                self.retries,
                self.cache_capacity,
                selective.clone(),
            );
            let (c_handle, signal) = rt.spawn(format!("collector{}", partition_id), collector).await?;
            collector_handles.insert(partition_id, c_handle);
            initialized_rx.push(signal);
            let solidifier = Solidifier::new(
                self.keyspace.clone(),
                partition_id,
                MessageIdPartitioner::new(self.partition_count),
                gap_start,
                self.retries,
            );
            let (s_handle, signal) = rt.spawn(format!("solidifier{}", partition_id), solidifier).await?;
            solidifier_handles.insert(partition_id, s_handle);
            initialized_rx.push(signal);
            let requester = Requester::<T>::new(reqwest_client.clone(), self.retries);
            let (r_handle, signal) = rt.spawn(format!("requester{}", partition_id), requester).await?;
            requester_handles.push_front(r_handle);
            initialized_rx.push(signal);
        }
        // -- publish solidifiers handles, collectors handles and requester handles as resources
        rt.publish(collector_handles).await;
        rt.publish(requester_handles).await;
        rt.publish(solidifier_handles).await;
        // -- ensure all spawned are initialized
        for i in initialized_rx {
            rt.abortable(i.initialized())
                .await
                .map_err(|e| ActorError::aborted(e))??;
        }
        log::info!("ChronicleBroker successfully started");
        Ok(())
    }
}
impl<T: SelectiveBuilder> ChronicleBroker<T> {
    pub(super) async fn query_sync_table(&self) -> ActorResult<SyncData> {
        SyncData::try_fetch(&self.keyspace, &self.sync_range, 10)
            .await
            .map_err(|e| {
                log::error!("{}", e);
                ActorError::restart(e, std::time::Duration::from_secs(60))
            })
    }
}
