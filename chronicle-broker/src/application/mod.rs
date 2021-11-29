use super::{
    archiver::Archiver,
    syncer::Syncer,
};
use crate::SyncRange;
use anyhow::{
    anyhow,
    bail,
    Result,
};
use async_trait::async_trait;
use backstage::core::{
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
    ShutdownEvent,
    StreamExt,
    SupHandle,
    UnboundedChannel,
    UnboundedHandle,
};
use chronicle_common::config::PartitionConfig;
use chronicle_filter::SelectiveBuilder;
use chronicle_storage::{
    access::SyncData,
    keyspaces::ChronicleKeyspace,
};
use serde::{
    Deserialize,
    Serialize,
};
use std::{
    collections::HashSet,
    path::PathBuf,
    time::Duration,
};
use url::Url;

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
/// Chronicle Broker config/state
pub struct ChronicleBroker<T: SelectiveBuilder> {
    pub parallelism: u8,
    pub complete_gaps_interval: Duration,
    pub partition_count: u8,
    pub logs_dir: Option<PathBuf>,
    pub max_log_size: Option<u64>,
    pub mqtt_brokers: HashSet<Url>,
    pub api_endpoints: HashSet<Url>,
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
            ..Default::default()
        }
    }
}

impl<T: SelectiveBuilder> ChronicleBroker<T> {
    /// Create new chronicle broker instance
    pub fn new(
        keyspace: ChronicleKeyspace,
        partition_config: chronicle_common::config::PartitionConfig,
        parallelism: u8,
        gaps_interval: Duration,
        partition_count: u8,
        logs_dir: Option<PathBuf>,
        mut max_log_size: Option<u64>,
        sync_range: SyncRange,
        selective_builder: T,
    ) -> Self {
        if logs_dir.is_some() && max_log_size.is_none() {
            max_log_size = Some(super::archiver::MAX_LOG_SIZE);
        }
        Self {
            keyspace,
            parallelism,
            complete_gaps_interval: gaps_interval,
            partition_count,
            logs_dir,
            sync_range,
            partition_config,
            max_log_size,
            mqtt_brokers: HashSet::new(),
            api_endpoints: HashSet::new(),
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
    Topology(Topology),
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

#[derive(Debug)]
pub enum Topology {
    Import,
}

impl From<Event<Service>> for BrokerEvent {
    fn from(service_event: Event<Service>) -> Self {
        Self::Scylla(service_event)
    }
}

#[async_trait]
impl<S: SupHandle<Self>, T: SelectiveBuilder> Actor<S> for ChronicleBroker<T> {
    type Data = (Service, T::State);
    type Channel = UnboundedChannel<BrokerEvent>;
    async fn init(&mut self, rt: &mut Rt<Self, S>) -> ActorResult<Self::Data> {
        // register self as resource
        rt.add_resource(self.clone()).await;
        // subscribe to scylla service
        let scylla_scope_id = rt.sibling("scylla").scope_id().await.ok_or_else(|| {
            log::error!("Scylla doesn't exist as sibling");
            ActorError::exit_msg("ChronicleBroker doesn't have Scylla as grandparent")
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
            self.maybe_start(rt, sync_data).await; //?
        } else {
            rt.update_status(ServiceStatus::Idle).await;
        };
        Ok((scylla_service, selective))
    }
    async fn run(&mut self, rt: &mut Rt<Self, S>, (mut scylla_service, selective): Self::Data) -> ActorResult<()> {
        while let Some(event) = rt.inbox_mut().next().await {
            match event {
                BrokerEvent::Topology(topology) => {
                    todo!()
                }
                BrokerEvent::Microservice(scope_id, service, mut service_result) => {
                    // exit broker if any child had an exit error
                    match service_result.as_ref() {
                        Some(Err(ActorError {
                            request: Some(ActorRequest::Exit),
                            ..
                        })) => service_result.expect("Failed to unwrap service_result in broker")?,
                        Some(Err(_)) => {
                            // a child might shutdown due to overload in scylla, where scylla still running
                            rt.shutdown_children().await;
                        }
                        _ => {}
                    }
                    rt.upsert_microservice(scope_id, service);
                    // if children got shutdown but scylla is running
                    if !rt.service().is_stopping() {
                        if rt.microservices_stopped() && (scylla_service.is_running() || scylla_service.is_degraded()) {
                            if let Ok(sync_data) = self.query_sync_table().await {
                                // todo maybe sleep for 5 seconds in-case the shutdown was due to overload in scylla
                                self.maybe_start(rt, sync_data).await;
                            }
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
                                self.maybe_start(rt, sync_data).await;
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
                    todo!()
                }
                BrokerEvent::Shutdown => break,
            }
        }
        Ok(())
    }
}

impl<T: SelectiveBuilder> ChronicleBroker<T> {
    async fn maybe_start<S: SupHandle<Self>>(&self, rt: &mut Rt<Self, S>, sync_data: SyncData) -> ActorResult<()> {
        // start only if there is at least one mqtt feed source in each topic (messages and refmessages)
        if self.mqtt_brokers.is_empty() || self.api_endpoints.is_empty() {
            log::warn!("Cannot start children of the broker without at least one broker and endpoint!");
            rt.update_status(ServiceStatus::Idle).await;
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
        // -- spawn feed sources (mqtt)
        // (set to outage(and spawn task to keep trying adding them later) if we were unable to spawn all the mqtt)
        // -- spawn collectors and solidifiers
        // -- add solidifiers handles and collectors hadles as resources
        // -- start syncer
        let sync_range = self.sync_range;
        let update_sync_data_every = self.complete_gaps_interval;
        let parallelism = self.parallelism;
        let keyspace = self.keyspace.clone();
        let syncer = Syncer::new(sync_data, sync_range, update_sync_data_every, parallelism, keyspace);
        rt.start("syncer".to_string(), syncer).await?;
        // -- ensure all are initialized
        todo!()
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
