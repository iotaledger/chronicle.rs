// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0
use super::*;
use crate::{
    archiver::*,
    collector::*,
    exporter::*,
    importer::*,
    mqtt::*,
    solidifier::*,
    syncer::*,
};
use async_trait::async_trait;
use chronicle_common::config::{
    BrokerConfig,
    Config,
};
use std::{
    collections::HashSet,
    ops::Range,
    str::FromStr,
    sync::Arc,
    time::Duration,
};
use tokio::sync::{
    oneshot,
    RwLock,
};

/// Application state
pub struct ChronicleBroker {
    parallelism: u8,
    complete_gaps_interval: Duration,
    parallelism_points: u8,
    in_progress_importers: usize,
    collector_count: u8,
    logs_dir_path: Option<PathBuf>,
    default_keyspace: ChronicleKeyspace,
    sync_range: SyncRange,
    sync_data: SyncData,
}

/// Event type of the broker Application
#[supervise(Mqtt<Messages>, Mqtt<MessagesReferenced>, Collector, Solidifier, Archiver, Syncer, Importer<All>, Importer<Analytics>, Exporter)]
pub enum BrokerEvent {
    Websocket(BrokerRequest),
}

pub enum BrokerRequest {
    /// Add new MQTT Messages feed source
    AddMqttMessages(Url, oneshot::Sender<anyhow::Result<()>>),
    /// Add new MQTT Messages Referenced feed source
    AddMqttMessagesReferenced(Url, oneshot::Sender<anyhow::Result<()>>),
    /// Remove a MQTT Messages feed source
    RemoveMqttMessages(Url, oneshot::Sender<anyhow::Result<()>>),
    /// Remove a MQTT Messages Referenced feed source
    RemoveMqttMessagesReferenced(Url, oneshot::Sender<anyhow::Result<()>>),
    Requester(RequesterTopology),
    Import {
        /// File or dir path which supposed to contain LogFiles
        path: PathBuf,
        /// Resume the importing process
        resume: bool,
        /// Provide optional import range
        import_range: Option<Range<u32>>,
        /// The type of import requested
        import_type: ImportType,
        responder: tokio::sync::mpsc::UnboundedSender<ImporterSession>,
    },
    Export {
        range: Range<u32>,
        responder: tokio::sync::mpsc::UnboundedSender<ExporterStatus>,
    },
}

/// Requester topology used by admins to add/remove IOTA api endpoints
pub enum RequesterTopology {
    /// Add new Api Endpoint
    AddEndpoint(Url, oneshot::Sender<anyhow::Result<()>>),
    /// Remove existing Api Endpoint
    RemoveEndpoint(Url, oneshot::Sender<anyhow::Result<()>>),
}

/// Import types
#[derive(Deserialize, Serialize, Debug, Copy, Clone)]
pub enum ImportType {
    /// Import everything
    All,
    /// Import only Analytics data
    Analytics,
}

#[build]
#[derive(Clone)]
pub fn build_broker(config: Config) -> ChronicleBroker {
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
    let parallelism = config.broker_config.parallelism;
    ChronicleBroker {
        collector_count: config.broker_config.collector_count,
        parallelism,
        parallelism_points: parallelism,
        in_progress_importers: 0,
        logs_dir_path,
        default_keyspace,
        sync_range,
        sync_data,
        complete_gaps_interval: Duration::from_secs(config.broker_config.complete_gaps_interval_secs),
    }
}

#[async_trait]
impl Actor for ChronicleBroker {
    type Dependencies = ();
    type Event = BrokerEvent;
    type Channel = UnboundedTokioChannel<Self::Event>;

    async fn init<Reg: RegistryAccess + Send + Sync, Sup: EventDriven>(
        &mut self,
        rt: &mut ActorScopedRuntime<Self, Reg, Sup>,
    ) -> Result<(), ActorError>
    where
        Self: Sized,
        Sup::Event: SupervisorEvent,
        <Sup::Event as SupervisorEvent>::Children: From<PhantomData<Self>>,
    {
        rt.update_status(ServiceStatus::Initializing).await.ok();
        let config = get_config_async().await;
        // Query sync table
        self.sync_data = self.query_sync_table().await?;
        info!("Current: {:#?}", self.sync_data);
        // Get the gap_start
        let gap_start = self.sync_data.gaps.first().unwrap().start;
        // create syncer_builder

        let mut syncer_builder = SyncerBuilder::new()
            .sync_data(self.sync_data.clone())
            .first_ask(AskSyncer::FillGaps)
            .sync_range(self.sync_range)
            .parallelism(self.parallelism)
            .update_sync_data_every(self.complete_gaps_interval)
            .solidifier_count(self.collector_count);
        if let Some(dir_path) = self.logs_dir_path.as_ref() {
            let max_log_size = config.broker_config.max_log_size.unwrap_or(MAX_LOG_SIZE);
            // create archiver_builder
            let archiver = ArchiverBuilder::new()
                .dir_path(dir_path.clone())
                .keyspace(self.default_keyspace.clone())
                .solidifiers_count(self.collector_count)
                .max_log_size(max_log_size)
                .build();
            syncer_builder = syncer_builder.first_ask(AskSyncer::Complete);
            // start archiver
            rt.spawn_actor(archiver).await?;
        } else {
            info!("Initializing Broker without Archiver");
        };
        let reqwest_client = reqwest::Client::builder()
            .timeout(Duration::from_secs(config.broker_config.request_timeout_secs))
            .build()
            .expect("Expected reqwest client to build correctly");
        let collector_builder = CollectorBuilder::new()
            .collector_count(self.collector_count)
            .requester_count(config.broker_config.requester_count)
            .storage_config(config.storage_config.clone())
            .reqwest_client(reqwest_client.clone())
            .retries_per_query(config.broker_config.retries_per_query)
            .retries_per_endpoint(config.broker_config.retries_per_endpoint);
        let solidifier_builder = SolidifierBuilder::new()
            .collector_count(self.collector_count)
            .gap_start(gap_start)
            .keyspace(self.default_keyspace.clone());
        for partition_id in 0..self.collector_count {
            let collector = collector_builder.clone().partition_id(partition_id).build();
            rt.spawn_into_pool_keyed::<MapPool<_, _>>(partition_id, collector)
                .await?;

            let solidifier = solidifier_builder.clone().partition_id(partition_id).build();
            rt.spawn_into_pool_keyed::<MapPool<_, _>>(partition_id, solidifier)
                .await?;
        }
        // Finalize and Spawn Syncer
        rt.spawn_actor(syncer_builder.build()).await?;
        // Spawn mqtt brokers
        for broker_url in config
            .broker_config
            .mqtt_brokers
            .get(&MqttType::Messages)
            .iter()
            .flat_map(|v| v.iter())
            .cloned()
        {
            self.add_mqtt(rt, Messages, MqttType::Messages, broker_url).await?;
        }
        for broker_url in config
            .broker_config
            .mqtt_brokers
            .get(&MqttType::MessagesReferenced)
            .iter()
            .flat_map(|v| v.iter())
            .cloned()
        {
            self.add_mqtt(rt, MessagesReferenced, MqttType::MessagesReferenced, broker_url)
                .await?;
        }
        rt.add_resource(Arc::new(RwLock::new(config.broker_config.api_endpoints)))
            .await;

        Ok(())
    }

    async fn run<Reg: RegistryAccess + Send + Sync, Sup: EventDriven>(
        &mut self,
        rt: &mut ActorScopedRuntime<Self, Reg, Sup>,
        _: Self::Dependencies,
    ) -> Result<(), ActorError>
    where
        Self: Sized,
        Sup::Event: SupervisorEvent,
        <Sup::Event as SupervisorEvent>::Children: From<PhantomData<Self>>,
    {
        rt.update_status(ServiceStatus::Running).await.ok();
        while let Some(event) = rt.next_event().await {
            match event {
                BrokerEvent::Websocket(request) => match request {
                    BrokerRequest::AddMqttMessages(url, responder) => {
                        responder.send(self.add_mqtt(rt, Messages, MqttType::Messages, url).await);
                    }
                    BrokerRequest::AddMqttMessagesReferenced(url, responder) => {
                        responder.send(
                            self.add_mqtt(rt, MessagesReferenced, MqttType::MessagesReferenced, url)
                                .await,
                        );
                    }
                    BrokerRequest::RemoveMqttMessages(url, responder) => {
                        self.remove_mqtt::<_, _, Messages>(rt, MqttType::Messages, url).await;
                        responder.send(Ok(()));
                    }
                    BrokerRequest::RemoveMqttMessagesReferenced(url, responder) => {
                        self.remove_mqtt::<_, _, MessagesReferenced>(rt, MqttType::MessagesReferenced, url)
                            .await;
                        responder.send(Ok(()));
                    }
                    BrokerRequest::Requester(t) => match t {
                        RequesterTopology::AddEndpoint(url, responder) => {
                            if let Some(url) = BrokerConfig::adjust_api_endpoint(url.clone()) {
                                let reqwest_client = reqwest::Client::new();
                                if let Err(e) = BrokerConfig::verify_endpoint(&reqwest_client, &url).await {
                                    responder.send(Err(e)).ok();
                                } else {
                                    if let Some(endpoints) = rt.resource::<Arc<RwLock<HashSet<Url>>>>().await {
                                        if !endpoints.read().await.contains(&url) {
                                            endpoints.write().await.insert(url);
                                        } else {
                                            responder
                                                .send(Err(anyhow::anyhow!("Endpoint {} already exists!", url)))
                                                .ok();
                                            break;
                                        }
                                    }
                                    if let Some(collector_handles) = rt.pool::<MapPool<Collector, u8>>().await {
                                        for handle in collector_handles.handles().await {
                                            handle.send(CollectorEvent::RequesterTopologyChange).ok();
                                        }
                                    }
                                    responder.send(Ok(())).ok();
                                }
                            } else {
                                responder
                                    .send(Err(anyhow::anyhow!("Endpoint {} is invalid!", url)))
                                    .ok();
                            }
                        }
                        RequesterTopology::RemoveEndpoint(url, responder) => {
                            if let Some(endpoints) = rt.resource::<Arc<RwLock<HashSet<Url>>>>().await {
                                if !endpoints.write().await.remove(&url) {
                                    responder
                                        .send(Err(anyhow::anyhow!("Endpoint {} did not exist!", url)))
                                        .ok();
                                    break;
                                }
                            }
                            if let Some(collector_handles) = rt.pool::<MapPool<Collector, u8>>().await {
                                for handle in collector_handles.handles().await {
                                    handle.send(CollectorEvent::RequesterTopologyChange).ok();
                                }
                            }
                            responder.send(Ok(())).ok();
                        }
                    },
                    BrokerRequest::Import {
                        path,
                        resume,
                        import_range,
                        import_type,
                        responder,
                    } => {
                        self.handle_import(rt, path, resume, import_range, import_type, responder)
                            .await;
                    }
                    BrokerRequest::Export { range, responder } => {
                        let exporter = ExporterBuilder::new()
                            .ms_range(range)
                            .keyspace(self.default_keyspace.clone())
                            .responder(responder.clone())
                            .build();
                        match rt.spawn_into_pool::<RandomPool<_>>(exporter).await {
                            Err(e) => {
                                log::error!("Error creating exporter: {}", e);
                                responder.send(ExporterStatus::Failed(e.to_string())).ok();
                            }
                            _ => (),
                        }
                    }
                },
                BrokerEvent::StatusChange(s) => {
                    // TODO
                }
                BrokerEvent::ReportExit(r) => match r {
                    Ok(s) => match s.state {
                        ChildStates::Importer_All(i) => {
                            self.in_progress_importers -= 1;
                        }
                        ChildStates::Importer_Analytics(i) => {
                            self.in_progress_importers -= 1;
                        }
                        _ => {
                            // TODO
                        }
                    },
                    Err(e) => {
                        log::error!("{}", e.error);
                        match e.state {
                            ChildStates::Importer_All(i) => {
                                self.in_progress_importers -= 1;
                            }
                            ChildStates::Importer_Analytics(i) => {
                                self.in_progress_importers -= 1;
                            }
                            _ => {
                                // TODO
                            }
                        }
                    }
                },
            }
        }
        Ok(())
    }
}

impl ChronicleBroker {
    async fn query_sync_table(&mut self) -> anyhow::Result<SyncData> {
        SyncData::try_fetch(&self.default_keyspace, &self.sync_range, 10).await
    }

    async fn remove_mqtt<Reg, Sup, T>(
        &mut self,
        rt: &mut ActorScopedRuntime<Self, Reg, Sup>,
        mqtt_type: MqttType,
        url: Url,
    ) where
        Sup: EventDriven,
        Reg: RegistryAccess + Send + Sync,
        Sup::Event: SupervisorEvent,
        <Sup::Event as SupervisorEvent>::Children: From<PhantomData<Self>>,
        T: Topic,
        Mqtt<T>: Actor,
        <BrokerEvent as SupervisorEvent>::Children: From<PhantomData<Mqtt<T>>>,
        <BrokerEvent as SupervisorEvent>::ChildStates: From<Mqtt<T>>,
    {
        if let Some(pool) = rt.pool::<MapPool<Mqtt<T>, Url>>().await {
            if let Some(handle) = pool.get(&url).await {
                handle.shutdown();
                let config = get_config_async().await;
                let mut new_config = config.clone();
                if let Some(list) = new_config.broker_config.mqtt_brokers.get_mut(&mqtt_type) {
                    list.remove(&url);
                }
                if new_config != config {
                    get_history_mut_async().await.update(new_config.into());
                }
            }
        }
    }

    async fn add_mqtt<Reg, Sup, T>(
        &mut self,
        rt: &mut ActorScopedRuntime<Self, Reg, Sup>,
        topic: T,
        mqtt_type: MqttType,
        url: Url,
    ) -> anyhow::Result<()>
    where
        Sup: EventDriven,
        Reg: RegistryAccess + Send + Sync,
        Sup::Event: SupervisorEvent,
        <Sup::Event as SupervisorEvent>::Children: From<PhantomData<Self>>,
        T: Topic,
        Mqtt<T>: Actor,
        <BrokerEvent as SupervisorEvent>::Children: From<PhantomData<Mqtt<T>>>,
        <BrokerEvent as SupervisorEvent>::ChildStates: From<Mqtt<T>>,
    {
        let config = get_config_async().await;
        let mqtt = MqttBuilder::new()
            .topic(topic)
            .url(url.clone())
            .stream_capacity(config.broker_config.mqtt_stream_capacity)
            .collector_count(self.collector_count)
            .build();
        rt.spawn_into_pool_keyed::<MapPool<Mqtt<T>, Url>>(url.clone(), mqtt)
            .await
            .map_err(|_| anyhow::anyhow!("The Mqtt for url {} already exists!", url))?;
        let mut new_config = config.clone();
        if let Some(list) = new_config.broker_config.mqtt_brokers.get_mut(&mqtt_type) {
            list.insert(url);
        }
        if new_config != config {
            get_history_mut_async().await.update(new_config.into());
        }
        Ok(())
    }

    async fn handle_import<Reg, Sup>(
        &mut self,
        rt: &mut ActorScopedRuntime<Self, Reg, Sup>,
        path: PathBuf,
        resume: bool,
        import_range: Option<Range<u32>>,
        import_type: ImportType,
        responder: tokio::sync::mpsc::UnboundedSender<ImporterSession>,
    ) -> anyhow::Result<()>
    where
        Sup: EventDriven,
        Reg: RegistryAccess + Send + Sync,
        Sup::Event: SupervisorEvent,
        <Sup::Event as SupervisorEvent>::Children: From<PhantomData<Self>>,
    {
        if path.is_file() {
            // build importer
            self.spawn_importer(rt, path.clone(), resume, import_range.clone(), import_type, responder)
                .await?;
        } else if path.is_dir() {
            self.spawn_importers(rt, path.clone(), resume, import_range.clone(), import_type, responder)
                .await?;
        } else {
            responder
                .send(ImporterSession::PathError {
                    path: path.clone(),
                    msg: "Invalid path".into(),
                })
                .ok();
        }

        Ok(())
    }

    async fn spawn_importer<Reg, Sup>(
        &mut self,
        rt: &mut ActorScopedRuntime<Self, Reg, Sup>,
        file_path: PathBuf,
        resume: bool,
        import_range: Option<Range<u32>>,
        import_type: ImportType,
        responder: tokio::sync::mpsc::UnboundedSender<ImporterSession>,
    ) -> anyhow::Result<()>
    where
        Sup: EventDriven,
        Reg: RegistryAccess + Send + Sync,
        Sup::Event: SupervisorEvent,
        <Sup::Event as SupervisorEvent>::Children: From<PhantomData<Self>>,
    {
        match import_type {
            ImportType::All => {
                let importer = crate::importer::ImporterBuilder::<All>::new()
                    .import_range(import_range)
                    .file_path(file_path.clone())
                    .resume(resume)
                    .retries_per_query(50) // TODO get it from config
                    .chronicle_id(0) // TODO get it from config
                    .responder(responder)
                    .build();
                rt.spawn_into_pool_keyed::<MapPool<_, _>>(file_path.clone(), importer)
                    .await?;
            }
            ImportType::Analytics => {
                let importer = crate::importer::ImporterBuilder::<Analytics>::new()
                    .import_range(import_range)
                    .file_path(file_path.clone())
                    .resume(resume)
                    .retries_per_query(50) // TODO get it from config
                    .chronicle_id(0) // TODO get it from config
                    .responder(responder)
                    .build();
                rt.spawn_into_pool_keyed::<MapPool<_, _>>(file_path.clone(), importer)
                    .await?;
            }
        }
        self.in_progress_importers += 1;

        Ok(())
    }
    async fn spawn_importers<Reg, Sup>(
        &mut self,
        rt: &mut ActorScopedRuntime<Self, Reg, Sup>,
        path: PathBuf,
        resume: bool,
        import_range: Option<Range<u32>>,
        import_type: ImportType,
        responder: tokio::sync::mpsc::UnboundedSender<ImporterSession>,
    ) -> anyhow::Result<()>
    where
        Sup: EventDriven,
        Reg: RegistryAccess + Send + Sync,
        Sup::Event: SupervisorEvent,
        <Sup::Event as SupervisorEvent>::Children: From<PhantomData<Self>>,
    {
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
            responder
                .send(ImporterSession::PathError {
                    path,
                    msg: "No LogFiles in the provided path".into(),
                })
                .ok();
            return Ok(());
        }
        for file_path in import_files {
            self.spawn_importer(
                rt,
                file_path,
                resume,
                import_range.clone(),
                import_type,
                responder.clone(),
            )
            .await?;
        }

        Ok(())
    }
}
