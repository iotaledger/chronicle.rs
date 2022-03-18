use super::{
    archiver::Archiver,
    collector::{
        Collector,
        CollectorHandle,
        MessageIdPartitioner,
    },
    exporter::{
        Exporter,
        ExporterStatus,
    },
    filter::FilterBuilder,
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
use crate::{
    importer::Importer,
    SyncRange,
};
use anyhow::{
    anyhow,
    bail,
    Result,
};

use async_trait::async_trait;
use backstage::{
    core::{
        paho_mqtt,
        Actor,
        ActorError,
        ActorRequest,
        ActorResult,
        EolEvent,
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
use chronicle_common::types::Message;

use bee_rest_api_old::types::responses::MessageMetadataResponse;
use serde::{
    Deserialize,
    Serialize,
};
use std::{
    any::TypeId,
    collections::{
        HashMap,
        HashSet,
    },
    convert::TryFrom,
    ops::Range,
    path::PathBuf,
    time::Duration,
};
pub type BrokerHandle = UnboundedHandle<BrokerEvent>;
use thiserror::Error;
use url::Url;
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
/// Chronicle Broker config
pub struct ChronicleBroker<T: FilterBuilder> {
    pub parallelism: u8,
    pub complete_gaps_interval: Duration,
    pub partition_count: u8,
    pub retries: u8,
    pub logs_dir: Option<PathBuf>,
    pub max_log_size: Option<u64>,
    pub mqtt_brokers: HashSet<Url>,
    /// Mqtt stream capacity and lru capacity per collector
    pub cache_capacity: usize,
    pub requester_budget: usize,
    pub api_endpoints: HashSet<Url>,
    pub request_timeout_secs: u8,
    pub filter_builder: T,
}

impl<T: FilterBuilder> Default for ChronicleBroker<T> {
    fn default() -> Self {
        Self {
            parallelism: 25,
            complete_gaps_interval: Duration::from_secs(60 * 60),
            partition_count: 10,
            logs_dir: Some("chronicle/logs/".into()),
            max_log_size: Some(super::archiver::MAX_LOG_SIZE),
            cache_capacity: 10000,
            requester_budget: 10,
            request_timeout_secs: 5,
            retries: 5,
            api_endpoints: Default::default(),
            mqtt_brokers: Default::default(),
            filter_builder: Default::default(),
        }
    }
}

impl<T: FilterBuilder> ChronicleBroker<T> {
    /// Create new chronicle broker instance
    pub fn new(
        retries: u8,
        parallelism: u8,
        gaps_interval: Duration,
        partition_count: u8,
        logs_dir: Option<PathBuf>,
        mut max_log_size: Option<u64>,
        sync_range: SyncRange,
        cache_capacity: usize,
        requester_budget: usize,
        request_timeout_secs: u8,
        filter_builder: T,
    ) -> Self {
        if logs_dir.is_some() && max_log_size.is_none() {
            max_log_size = Some(super::archiver::MAX_LOG_SIZE);
        }
        Self {
            parallelism,
            retries,
            complete_gaps_interval: gaps_interval,
            partition_count,
            logs_dir,
            max_log_size,
            mqtt_brokers: HashSet::new(),
            api_endpoints: HashSet::new(),
            request_timeout_secs,
            cache_capacity,
            requester_budget,
            filter_builder,
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
pub enum BrokerEvent {
    /// Request the cluster handle
    Topology(Topology, Option<TopologyResponder>),
    /// Used by scylla children to push their service
    Microservice(ScopeId, Service, Option<ActorResult<()>>),
    /// Importer session
    ImporterSession(ImporterSession),
    /// Shutdown signal
    Shutdown,
}

impl TryFrom<(JsonMessage, Responder)> for BrokerEvent {
    type Error = anyhow::Error;
    fn try_from((msg, responder): (JsonMessage, Responder)) -> Result<Self, Self::Error> {
        Ok(BrokerEvent::Topology(
            serde_json::from_str(msg.0.as_ref())?,
            Some(TopologyResponder::WsResponder(responder)),
        ))
    }
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
    },
    Export {
        range: Range<u32>,
    },
}

/// Enum used by importer to keep the sockets up to date with most recent progress.
#[derive(Deserialize, Serialize, Debug, Clone)]
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
#[derive(Clone)]
pub enum TopologyResponder {
    /// Websocket responder
    WsResponder(Responder),
    /// Mpsc Responder
    Mpsc(tokio::sync::mpsc::UnboundedSender<TopologyResponse>),
}

impl TopologyResponder {
    pub async fn reply(&self, response: TopologyResponse) -> anyhow::Result<()> {
        match self {
            Self::WsResponder(r) => r.inner_reply(response).await,
            Self::Mpsc(tx) => tx.send(response).map_err(|_| anyhow::Error::msg("caller out of scope")),
        }
    }
}

/// The topology response, sent after the cluster processes a topology event
pub type TopologyResponse = Result<TopologyOk, TopologyErr>;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum TopologyOk {
    Import(ImporterSession),
    Export(ExporterStatus),
    AddMqtt,
    RemoveMqtt,
    // TODO: fill in the rest
}

#[derive(serde::Deserialize, serde::Serialize, Debug, Error, Clone)]
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
impl<S: SupHandle<Self>, T: FilterBuilder> Actor<S> for ChronicleBroker<T> {
    type Data = Option<T::Handle>;
    type Channel = UnboundedChannel<BrokerEvent>;
    async fn init(&mut self, rt: &mut Rt<Self, S>) -> ActorResult<Self::Data> {
        log::info!("ChronicleBroker is initializing");
        // register self as resource
        rt.add_resource(self.clone()).await;
        rt.add_route::<(JsonMessage, Responder)>().await.ok();
        let filter_handle = self.maybe_start(rt).await?;
        Ok(filter_handle)
    }
    async fn run(&mut self, rt: &mut Rt<Self, S>, mut filter_handle: Self::Data) -> ActorResult<()> {
        let mut parallelism_points = self.parallelism;
        let mut pending_imports = Vec::new();
        let mut in_progress_importers = HashMap::new();
        let mut active_import_responders = Vec::new();
        log::info!("ChronicleBroker is {}", rt.service().status());
        while let Some(event) = rt.inbox_mut().next().await {
            match event {
                BrokerEvent::ImporterSession(importer_session) => {
                    // let socket_msg = BrokerSocketMsg::ChronicleBroker(importer_session);
                    // self.response_to_sockets(&socket_msg).await;
                }

                BrokerEvent::Topology(topology, responder_opt) => {
                    // only configure topology if it's not stopping
                    if rt.service().is_stopping() {
                        // response
                        log::warn!("Cannot configure topology while the broker is stopping");
                        if let Some(responder) = responder_opt.as_ref() {
                            let ok_response: Result<_, TopologyErr> = Err(TopologyErr::new(format!(
                                "Cannot configure topology while the broker is stopping",
                            )));
                            responder.reply(ok_response).await.ok();
                        }
                        continue;
                    }
                    match &topology {
                        Topology::AddMqtt(mqtt) => {
                            if rt.service().is_outage() {
                                // ensure all children are stopped, else ask the admin to try later
                                if rt.microservices_stopped() {
                                    if !self.contain_mqtt(&mqtt) {
                                        if let Some(responder) = responder_opt.as_ref() {
                                            if let Err(err) = self.add_mqtt(mqtt.clone()).await {
                                                log::error!("Unable to add mqtt: {}, error: {}", &mqtt, err);
                                                let error_response: Result<_, TopologyErr> = Err(TopologyErr::new(
                                                    format!("Unable to add mqtt: {}, error: {}", &mqtt, err),
                                                ));
                                                responder.reply(error_response).await.ok();
                                            } else {
                                                log::info!("Successfully inserted mqtt: {}", &mqtt);
                                                responder.reply(Ok(TopologyOk::AddMqtt)).await.ok();
                                                filter_handle = self.maybe_start(rt).await?;
                                            };
                                        } else {
                                            log::warn!("skipping re-adding a {} mqtt, as it got removed", mqtt);
                                        }
                                    } else {
                                        if let Some(responder) = responder_opt.as_ref() {
                                            log::warn!("Cannot add an existing mqtt: {}", mqtt);
                                            let err_response: Result<_, TopologyErr> =
                                                Err(TopologyErr::new(format!("Cannot add an existing mqtt: {}", mqtt)));
                                            responder.reply(err_response).await.ok();
                                            continue;
                                        }
                                        // we are trying to re-add mqtt while broker is in outage which will require
                                        // invoking maybe_start
                                        filter_handle = self.maybe_start(rt).await?;
                                    }
                                } else {
                                    if let Some(responder) = responder_opt.as_ref() {
                                        log::warn!(
                                            "Cannot add mqtt: {}, while the broker children are not fully stopped",
                                            mqtt
                                        );
                                        let err_response: Result<_, TopologyErr> = Err(TopologyErr::new(format!(
                                            "Cannot add mqtt: {}, while the broker children are not fully stopped",
                                            mqtt
                                        )));
                                        responder.reply(err_response).await.ok();
                                    } else {
                                        if self.contain_mqtt(&mqtt) {
                                            // reschedule to add it later, this happens due to rare race condition
                                            // unable to resume the broker, so we reschedule it again
                                            log::warn!(
                                                "Rescheduling re-add mqtt: {}, till broker microservices are stopped",
                                                mqtt
                                            );
                                            rt.handle().send_after(
                                                BrokerEvent::Topology(Topology::AddMqtt(mqtt.clone()), None),
                                                Duration::from_secs(10),
                                            );
                                        } else {
                                            log::warn!("skipping re-adding a {} mqtt, as it got removed. And broker children are not fully stopped", mqtt);
                                        }
                                    }
                                }
                                continue;
                            };
                            // check if it's exit already
                            if self.contain_mqtt(&mqtt) {
                                if let Some(responder) = responder_opt.as_ref() {
                                    log::warn!("Cannot add an existing mqtt: {}", mqtt);
                                    let err_response: Result<_, TopologyErr> =
                                        Err(TopologyErr::new(format!("Cannot add an existing mqtt: {}", mqtt)));
                                    responder.reply(err_response).await.ok();
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
                                Mqtt::<MessageMetadataResponse>::new(mqtt.clone(), self.cache_capacity, partitioner);
                            let dir = format!("messages@{}", mqtt);
                            match rt.spawn(dir, mqtt_messages).await {
                                Ok((h, _)) => {
                                    // try to start mqtt_msg_ref feed_source
                                    let dir = format!("referenced@{}", mqtt);
                                    if let Err(e) = rt.spawn(dir, mqtt_msg_ref).await {
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
                                        if let Some(responder) = responder_opt {
                                            responder.reply(Ok(TopologyOk::AddMqtt)).await.ok();
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
                                responder.reply(Ok(TopologyOk::RemoveMqtt)).await.ok();
                            } else {
                                log::error!("unable to remove non-existing {} mqtt!", mqtt);
                                responder
                                    .reply(Err(TopologyErr::new(format!(
                                        "unable to remove non-existing {} mqtt",
                                        mqtt
                                    ))))
                                    .await
                                    .ok();
                            };
                        }
                        Topology::Import {
                            path,
                            resume,
                            import_range,
                        } => {
                            if !rt.service().is_stopping() {
                                if let Some(filter_handle) = filter_handle.as_ref() {
                                    self.handle_import(
                                        rt,
                                        topology,
                                        &mut parallelism_points,
                                        &mut pending_imports,
                                        &mut active_import_responders,
                                        &mut in_progress_importers,
                                        filter_handle,
                                    )
                                    .await;
                                    self.try_close_importer_session(
                                        &mut in_progress_importers,
                                        &mut active_import_responders,
                                    )
                                    .await;
                                }
                            } else {
                                // respond with error
                            }
                        }
                        Topology::Export { range: ms_range } => {
                            let responder =
                                responder_opt.ok_or_else(|| ActorError::exit_msg("Responder required for export"))?;
                            if let Some(filter_h) = filter_handle.as_ref() {
                                let exporter =
                                    Exporter::<T>::new(ms_range.clone(), responder.clone(), filter_h.clone());
                                match rt
                                    .spawn(format!("exporter_{}_to_{}", ms_range.start, ms_range.end), exporter)
                                    .await
                                {
                                    Err(e) => {
                                        log::error!("Error creating exporter: {}", e);
                                        responder.reply(Err(TopologyErr::new(e.to_string()))).await.ok();
                                    }
                                    _ => (),
                                }
                            } else {
                                log::error!("Error creating exporter, no filter handle");
                                responder
                                    .reply(Err(TopologyErr::new(
                                        "Error creating exporter, no filter handle".to_string(),
                                    )))
                                    .await
                                    .ok();
                            }
                        }
                    }
                }
                BrokerEvent::Microservice(scope_id, service, service_result) => {
                    let mut service_status = rt.service().status().clone();
                    let mqtt_message_type_id = TypeId::of::<Mqtt<Message>>();
                    let mqtt_msg_ref_type_id = TypeId::of::<Mqtt<MessageMetadataResponse>>();
                    if service.is_stopped() {
                        // check if it's mqtt
                        if service.actor_type_id == mqtt_message_type_id
                            || service.actor_type_id == mqtt_msg_ref_type_id
                        {
                            // todo ensure to shutdown the other mqtt broker

                            // deserialize the url
                            let mqtt_url = service
                                .directory()
                                .as_ref()
                                .and_then(|dir| {
                                    dir.split('@').last().and_then(|url| {
                                        if let Ok(url) = Url::parse(url) {
                                            Some(url)
                                        } else {
                                            None
                                        }
                                    })
                                })
                                .ok_or_else(|| {
                                    log::error!("Invalid MicroService directory for stopped mqtt");
                                    ActorError::exit_msg("Invalid MicroService directory for stopped mqtt")
                                })?;
                            if self.contain_mqtt(&mqtt_url) {
                                rt.upsert_microservice(scope_id, service.clone());
                            } else {
                                rt.remove_microservice(scope_id);
                            }
                            // check if the mqtt are still in balanced state
                            if !self.mqtt_brokers.iter().any(|u| {
                                rt.service()
                                    .microservices()
                                    .iter()
                                    .filter(|(_, ms)| {
                                        ms.directory()
                                            .as_ref()
                                            .expect("Invalid directory in broker microservices")
                                            .ends_with(u.as_str())
                                    })
                                    .all(|(scope_id, _)| {
                                        if let Some(ms_node) = rt.service().microservices().get(&scope_id) {
                                            ms_node.is_running()
                                        } else {
                                            false
                                        }
                                    })
                            }) {
                                // unbalanced state (not enough feed sources)
                                // change the status to outage (only if it's not stopping)
                                if service_status != ServiceStatus::Stopping {
                                    service_status = ServiceStatus::Outage;
                                    // shutting down the children will force the broker to enter into pause state
                                    rt.shutdown_children().await;
                                }
                            }
                        } else {
                            // rest micro service
                            if service.actor_type_id == TypeId::of::<Exporter<T>>()
                                || service.actor_type_id == TypeId::of::<Importer<T>>()
                            {
                                rt.remove_microservice(scope_id);
                                if let Some(maybe_importer_dir) = service.directory().as_ref() {
                                    if let Some(parallelism) = in_progress_importers.remove(maybe_importer_dir) {
                                        parallelism_points += parallelism;
                                        // check if there are any pending
                                        'a: while let Some(import_topology) = pending_imports.pop() {
                                            if let Some(filter_h) = filter_handle.as_ref() {
                                                self.handle_import(
                                                    rt,
                                                    import_topology,
                                                    &mut parallelism_points,
                                                    pending_imports.as_mut(),
                                                    active_import_responders.as_mut(),
                                                    &mut in_progress_importers,
                                                    filter_h,
                                                )
                                                .await;
                                                break 'a;
                                            } else {
                                                // abort all pending imports
                                                let socket_msg = TopologyResponse::Err(TopologyErr::new(format!(
                                                    "Unable to handle import for topology: {:?}, broker status: {}, error: no user defined handle",
                                                    import_topology,
                                                    rt.service().status(),
                                                )));
                                                self.respond_to_active_importer_sessions(
                                                    &active_import_responders,
                                                    socket_msg,
                                                )
                                                .await;
                                            }
                                            // check if we finished all in_progress_importers
                                            self.try_close_importer_session(
                                                &mut in_progress_importers,
                                                &mut active_import_responders,
                                            )
                                            .await;
                                        }
                                    };
                                }
                            } else {
                                rt.upsert_microservice(scope_id, service.clone());
                            }
                        };
                    } else {
                        rt.upsert_microservice(scope_id, service.clone());
                        // adjust the service_status based on the change
                        if !self.mqtt_brokers.iter().any(|u| {
                            rt.service()
                                .microservices()
                                .iter()
                                .filter(|(_, ms)| {
                                    ms.directory()
                                        .as_ref()
                                        .expect("Invalid directory in broker microservices")
                                        .ends_with(u.as_str())
                                })
                                .all(|(scope_id, _)| {
                                    if let Some(ms_mqtt) = rt.service().microservices().get(&scope_id) {
                                        ms_mqtt.is_running() || ms_mqtt.is_initializing()
                                    } else {
                                        false
                                    }
                                })
                        }) {
                            // unbalanced state (not enough feed sources)
                            // change the status to outage (only if it's not stopping)
                            if service_status != ServiceStatus::Stopping {
                                service_status = ServiceStatus::Outage;
                                // shutting down the children will force the broker to enter into pause state
                                rt.shutdown_children().await;
                            }
                        } else {
                            // balanced ensure to
                            // check if we were in outage due to the lack of feed sources
                            // if rt.service().is_outage()
                        }
                        if rt.microservices_all(|ms| ms.is_running()) {
                            if service_status != ServiceStatus::Stopping {
                                service_status = ServiceStatus::Running;
                            }
                        }
                    }
                    // exit broker if any child had an exit error
                    match service_result.as_ref() {
                        Some(Err(ActorError {
                            request: Some(ActorRequest::Exit),
                            ..
                        })) => service_result.unwrap()?,
                        _ => {
                            if (service.is_type::<Collector<T>>()
                                || service.is_type::<Solidifier<T>>()
                                || service.is_type::<Syncer<T>>()
                                || service.is_type::<Archiver<T>>())
                                && service_result.is_some()
                            {
                                if !rt.service().is_stopping() {
                                    // a child might shutdown due to overload/outage in data-layer
                                    rt.shutdown_children().await;
                                    service_status = ServiceStatus::Outage;
                                    if let Some(Err(ActorError {
                                        request: Some(ActorRequest::Restart(_)),
                                        ..
                                    })) = service_result.as_ref()
                                    {
                                        // Request restart
                                        service_result.unwrap()?
                                    }
                                }
                            }
                        }
                    }
                    rt.update_status(service_status).await;
                    if rt.service().is_stopping() && rt.microservices_stopped() {
                        rt.inbox_mut().close();
                    }
                }
                BrokerEvent::Shutdown => {
                    rt.stop().await;
                    if rt.microservices_stopped() {
                        rt.inbox_mut().close();
                    }
                }
            }
        }
        log::info!("ChronicleBroker exited its event loop");
        Ok(())
    }
}

impl<T: FilterBuilder> ChronicleBroker<T> {
    // Note: It should never be invoked if scylla were in outage, or self service is stopping
    async fn maybe_start<S: SupHandle<Self>>(&self, rt: &mut Rt<Self, S>) -> ActorResult<Option<T::Handle>> {
        // start only if there is at least one mqtt feed source in each topic (messages and refmessages)
        if self.mqtt_brokers.is_empty() || self.api_endpoints.is_empty() {
            log::warn!("Cannot start children of the broker without at least one broker and endpoint!");
            rt.update_status(ServiceStatus::Idle).await;
            return Ok(None);
        } // todo verfiy at least one endpoint is working, else return Ok(()) and set outage status
          // First remove the old resources ( if any, as this is possible if maybe_start is invoked to restart the
          // children)
        rt.remove_resource::<HashMap<u8, CollectorHandle>>().await;
        rt.remove_resource::<RequesterHandles<T>>().await;
        rt.remove_resource::<HashMap<u8, SolidifierHandle>>().await;
        // build the filter
        let uda_actor = self.filter_builder.clone().build().await.map_err(|e| {
            log::error!("Broker unable to build selective, error: {}", e);
            ActorError::exit_msg(format!("Broker unable to build selective, error: {}", e))
        })?;
        // -- spawn feed sources (mqtt)
        let mqtt_brokers = self.mqtt_brokers.iter();
        let mut balanced = 0;
        for url in mqtt_brokers {
            let partitioner = MessageIdPartitioner::new(self.partition_count);
            let mqtt_messages = Mqtt::<Message>::new(url.clone(), self.cache_capacity, partitioner);
            let mqtt_msg_ref = Mqtt::<MessageMetadataResponse>::new(url.clone(), self.cache_capacity, partitioner);
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
                        balanced += 1;
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
        let mqtt_pair_count = self.mqtt_brokers.len();
        // check if mqtts are in healthy state, else we shutdown everything;
        if balanced == 0 {
            log::error!("Unable to reach balanced feed sources state");
            rt.update_status(ServiceStatus::Outage).await;
            return Ok(None);
        } else if balanced == mqtt_pair_count {
            log::info!("Successfully reached balanced feed source state");
            rt.update_status(ServiceStatus::Running).await;
        } else if balanced < mqtt_pair_count {
            log::warn!("Unable to reach optimal feed sources state, use the topology to remove any dead mqtt");
            // set as degraded if not all feed sources are working
            rt.update_status(ServiceStatus::Degraded).await;
        }
        // if balanced < self

        let mut collector_handles = HashMap::new();
        let mut requester_handles = RequesterHandles::<T>::new();
        let mut solidifier_handles = HashMap::new();
        let mut initialized_rx = Vec::new();
        let reqwest_client = reqwest::Client::builder()
            .timeout(Duration::from_secs(self.request_timeout_secs.into()))
            .build()
            .expect("Expected reqwest client to build correctly");
        // spawn selective_actor
        let (uda_handle, _) = rt.spawn(T::NAME.to_string(), uda_actor).await?;
        // build the filter handle
        let filter_handle = self.filter_builder.handle(uda_handle).await?;
        // -- (optional) start archiver
        if let Some(dir_path) = self.logs_dir.as_ref() {
            let max_log_size = self.max_log_size;
            let archiver = Archiver::<T>::new(dir_path, max_log_size, filter_handle.clone());
            rt.start("archiver".to_string(), archiver).await?;
        }
        // -- spawn syncer
        let update_sync_data_every = self.complete_gaps_interval;
        let parallelism = self.parallelism;
        let syncer = Syncer::<T>::new(update_sync_data_every, parallelism, filter_handle.clone());
        rt.spawn("syncer".to_string(), syncer).await?;

        // -- spawn collectors and solidifiers
        for partition_id in 0..self.partition_count {
            let collector = Collector::<T>::new(
                partition_id,
                self.partition_count,
                self.retries,
                self.cache_capacity,
                self.requester_budget,
                filter_handle.clone(),
            );
            let (c_handle, signal) = rt.spawn(format!("collector{}", partition_id), collector).await?;
            collector_handles.insert(partition_id, c_handle);
            initialized_rx.push(signal);
            let solidifier = Solidifier::<T>::new(
                partition_id,
                MessageIdPartitioner::new(self.partition_count),
                filter_handle.clone(),
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
        Ok(Some(filter_handle))
    }
    async fn handle_import<S: SupHandle<Self>>(
        &mut self,
        rt: &mut Rt<Self, S>,
        import_topology: Topology,
        parallelism_points: &mut u8,
        pending_imports: &mut Vec<Topology>,
        active_import_responders: &mut Vec<TopologyResponder>,
        in_progress_importers: &mut HashMap<String, u8>,
        filter_handle: &T::Handle,
    ) {
        if let Topology::Import {
            ref path,
            resume,
            ref import_range,
        } = import_topology
        {
            // check if we have enough parallelism points
            if *parallelism_points == 0 {
                // add it to pending list
                pending_imports.push(import_topology);
                return ();
            }
            let import_range = import_range.clone();
            if path.is_file() {
                self.spawn_importer(
                    rt,
                    path.clone(),
                    resume,
                    import_range.clone(),
                    *parallelism_points,
                    in_progress_importers,
                    parallelism_points,
                    filter_handle.clone(),
                    active_import_responders,
                )
                .await;
            } else if path.is_dir() {
                self.spawn_importers(
                    rt,
                    path.clone(),
                    resume,
                    import_range,
                    in_progress_importers,
                    parallelism_points,
                    filter_handle,
                    pending_imports,
                    active_import_responders,
                )
                .await;
            } else {
                let event = ImporterSession::PathError {
                    path: path.clone(),
                    msg: "Invalid path".into(),
                };
                let socket_msg = TopologyResponse::Ok(TopologyOk::Import(event));
                self.respond_to_active_importer_sessions(active_import_responders, socket_msg)
                    .await;
            }
        }
    }
    async fn try_close_importer_session(
        &self,
        in_progress_importers: &mut HashMap<String, u8>,
        active_import_responders: &mut Vec<TopologyResponder>,
    ) {
        if in_progress_importers.is_empty() {
            let event = ImporterSession::Close;
            let socket_msg = TopologyResponse::Ok(TopologyOk::Import(event));
            self.respond_to_active_importer_sessions(active_import_responders, socket_msg)
                .await;
            // clear any active importer, as the sessions are closed
            active_import_responders.clear();
        }
    }
    async fn start_importer<S: SupHandle<Self>>(
        &mut self,
        rt: &mut Rt<Self, S>,
        file_path: PathBuf,
        resume: bool,
        import_range: Option<Range<u32>>,
        parallelism: u8,
        in_progress_importers: &mut HashMap<String, u8>,
        parallelism_points: &mut u8,
        filter_handle: T::Handle,
    ) {
        let dir = format!("{}", file_path.to_str().expect("Failed to convert file path to string"));
        if in_progress_importers.contains_key(&dir) {
            log::warn!("Cannot start an existing importer for LogFile: {}", dir);
            return ();
        }
        let importer = Importer::<T>::new(file_path, resume, import_range, filter_handle, parallelism);
        if let Err(e) = rt.start(dir.clone(), importer).await {
            log::error!("Unable to start importer for LogFile: {}, error: {}", dir, e);
        } else {
            in_progress_importers.insert(dir, parallelism);
        }
    }
    async fn spawn_importer<S: SupHandle<Self>>(
        &mut self,
        rt: &mut Rt<Self, S>,
        file_path: PathBuf,
        resume: bool,
        import_range: Option<Range<u32>>,
        parallelism: u8,
        in_progress_importers: &mut HashMap<String, u8>,
        parallelism_points: &mut u8,
        filter_handle: T::Handle,
        active_import_responders: &mut Vec<TopologyResponder>,
    ) {
        if let Some(path_str) = file_path.to_str() {
            self.start_importer(
                rt,
                file_path,
                resume,
                import_range,
                parallelism,
                in_progress_importers,
                parallelism_points,
                filter_handle,
            );
        } else {
            // return the reducted points
            *parallelism_points += parallelism;
            let event = ImporterSession::PathError {
                path: file_path,
                msg: "Unable to convert path to string".into(),
            };

            let socket_msg = TopologyResponse::Ok(TopologyOk::Import(event));
            self.respond_to_active_importer_sessions(active_import_responders, socket_msg)
                .await;
        }
    }
    async fn spawn_importers<S: SupHandle<Self>>(
        &mut self,
        rt: &mut Rt<Self, S>,
        path: PathBuf,
        resume: bool,
        import_range: Option<Range<u32>>,
        in_progress_importers: &mut HashMap<String, u8>,
        parallelism_points: &mut u8,
        filter_handle: &T::Handle,
        pending_imports: &mut Vec<Topology>,
        active_import_responders: &mut Vec<TopologyResponder>,
    ) {
        let mut import_files = Vec::new();
        if let Ok(mut dir_entry) = tokio::fs::read_dir(&path).await {
            while let Ok(Some(p)) = dir_entry.next_entry().await {
                let file_path = p.path();
                if file_path.is_file() {
                    import_files.push(file_path);
                }
            }
        };
        let import_files_len = import_files.len();
        if import_files_len == 0 {
            let event = ImporterSession::PathError {
                path,
                msg: "No LogFiles in the provided path".into(),
            };
            let socket_msg = TopologyResponse::Ok(TopologyOk::Import(event));
            self.respond_to_active_importer_sessions(active_import_responders, socket_msg)
                .await;
            return ();
        }
        if *parallelism_points as usize > import_files_len {
            let parallelism = (*parallelism_points as usize / import_files_len) as u8;
            for file_path in import_files {
                self.spawn_importer(
                    rt,
                    file_path,
                    resume,
                    import_range.clone(),
                    parallelism,
                    in_progress_importers,
                    parallelism_points,
                    filter_handle.clone(),
                    active_import_responders,
                )
                .await
            }
        } else {
            // unwrap is safe
            let file_path = import_files.pop().expect("Expected import file");
            self.spawn_importer(
                rt,
                file_path,
                resume,
                import_range.clone(),
                *parallelism_points,
                in_progress_importers,
                parallelism_points,
                filter_handle.clone(),
                active_import_responders,
            )
            .await;
            // convert any remaining into pending_imports
            for file_path in import_files {
                let topology = Topology::Import {
                    path: file_path,
                    resume,
                    import_range: import_range.clone(),
                };
                pending_imports.push(topology);
            }
        }
    }
    pub(crate) async fn respond_to_active_importer_sessions(
        &self,
        responders: &Vec<TopologyResponder>,
        response: TopologyResponse,
    ) {
        for responder in responders.iter() {
            responder.reply(response.clone()).await;
        }
    }
}

// pub mod permanode; uncomment when ready
pub mod null;
