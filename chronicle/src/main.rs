// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

#![warn(missing_docs)]
//! # Chronicle
use async_trait::async_trait;
use backstage::core::*;
use chronicle_api::application::*;
use chronicle_broker::application::*;
use chronicle_common::{
    alert,
    config::AlertConfig,
    metrics::*,
};
use chronicle_storage::keyspaces::ChronicleKeyspace;
use scylla_rs::prelude::*;
use serde::{
    Deserialize,
    Serialize,
};

const TOKIO_THREAD_STACK_SIZE: usize = 4 * 4 * 1024 * 1024;

/// Chronicle system struct
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Default, Clone)]
pub struct Chronicle {
    /// Scylla application
    scylla: Scylla,
    #[cfg(all(feature = "permanode", not(feature = "selective-permanode")))]
    /// Permanode application
    broker: ChronicleBroker<chronicle_filter::PermanodeConfig>,
    /// Selective Permanode application
    #[cfg(all(feature = "selective-permanode", not(feature = "permanode")))]
    broker: ChronicleBroker<chronicle_filter::SelectivePermanodeConfig>,
    /// The Api application
    api: ChronicleAPI,
    /// Alert config
    alert: AlertConfig,
}

/// Chronicle event type
pub enum ChronicleEvent {
    /// Get up to date scylla config copy
    Scylla(Event<Scylla>),
    /// Get up to date -api copy
    Api(Event<ChronicleAPI>),
    /// Get up to date -broker copy
    #[cfg(all(feature = "permanode", not(feature = "selective-permanode")))]
    Broker(Event<ChronicleBroker<chronicle_filter::PermanodeConfig>>),
    #[cfg(all(feature = "selective-permanode", not(feature = "permanode")))]
    Broker(Event<ChronicleBroker<chronicle_filter::SelectivePermanodeConfig>>),
    /// Report and Eol variant used by children
    MicroService(ScopeId, Service, Option<ActorResult<()>>),
    /// Shutdown chronicle variant
    Shutdown,
}

impl ShutdownEvent for ChronicleEvent {
    fn shutdown_event() -> Self {
        Self::Shutdown
    }
}

impl<T> ReportEvent<T> for ChronicleEvent {
    fn report_event(scope_id: ScopeId, service: Service) -> Self {
        Self::MicroService(scope_id, service, None)
    }
}

impl<T> EolEvent<T> for ChronicleEvent {
    fn eol_event(scope_id: ScopeId, service: Service, _actor: T, r: ActorResult<()>) -> Self {
        Self::MicroService(scope_id, service, Some(r))
    }
}

impl From<Event<Scylla>> for ChronicleEvent {
    fn from(e: Event<Scylla>) -> Self {
        Self::Scylla(e)
    }
}
impl From<Event<ChronicleAPI>> for ChronicleEvent {
    fn from(e: Event<ChronicleAPI>) -> Self {
        Self::Api(e)
    }
}

#[cfg(all(feature = "permanode", not(feature = "selective-permanode")))]
impl From<Event<ChronicleBroker<chronicle_filter::PermanodeConfig>>> for ChronicleEvent {
    fn from(e: Event<ChronicleBroker<chronicle_filter::PermanodeConfig>>) -> Self {
        Self::Broker(e)
    }
}

#[cfg(all(feature = "selective-permanode", not(feature = "permanode")))]
impl From<Event<ChronicleBroker<chronicle_filter::SelectivePermanodeConfig>>> for ChronicleEvent {
    fn from(e: Event<ChronicleBroker<chronicle_filter::SelectivePermanodeConfig>>) -> Self {
        Self::Broker(e)
    }
}

/// Chronicle system actor implementation
#[async_trait]
impl<S> Actor<S> for Chronicle
where
    S: SupHandle<Self>,
{
    type Data = ();
    type Channel = UnboundedChannel<ChronicleEvent>;
    async fn init(&mut self, rt: &mut Rt<Self, S>) -> ActorResult<Self::Data> {
        // init the alert
        alert::init(self.alert.clone());
        //
        // - Scylla
        let scylla_scope_id = rt.start("scylla".to_string(), self.scylla.clone()).await?.scope_id();
        log::info!("Chronicle Started Scylla");
        let scylla = rt
            .subscribe::<Scylla>(scylla_scope_id, "scylla".to_string())
            .await?
            .expect("Expected scylla to publish itself");
        if self.scylla != scylla {
            self.scylla = scylla;
            log::info!("Chronicle published new Scylla");
            rt.publish(self.scylla.clone()).await;
        }
        log::info!("Chronicle subscribed to Scylla");
        let keyspaces = self.scylla.keyspaces.iter();
        for keyspace_config in keyspaces {
            init_database(keyspace_config).await.map_err(|e| {
                log::error!("{}", e);
                e
            })?;
        }
        //
        // - brokern
        #[cfg(any(feature = "permanode", feature = "selective-permanode"))]
        let broker = self.broker.clone();
        #[cfg(any(feature = "permanode", feature = "selective-permanode"))]
        let broker_scope_id = rt.start("broker".to_string(), broker).await?.scope_id();
        #[cfg(any(feature = "permanode", feature = "selective-permanode"))]
        {
            if let Some(broker) = rt.subscribe(broker_scope_id, "broker".to_string()).await? {
                if self.broker != broker {
                    self.broker = broker;
                    rt.publish(self.broker.clone()).await;
                }
            }
        }
        log::info!("Chronicle Started Broker");
        //
        // - api
        let api_scope_id = rt.start("api".to_string(), self.api.clone()).await?.scope_id();
        if let Some(api) = rt.subscribe::<ChronicleAPI>(api_scope_id, "api".to_string()).await? {
            if self.api != api {
                self.api = api;
                rt.publish(self.api.clone()).await;
            }
        }
        log::info!("Chronicle Started Api");
        Ok(())
    }
    async fn run(&mut self, rt: &mut Rt<Self, S>, _data: Self::Data) -> ActorResult<Self::Data> {
        while let Some(event) = rt.inbox_mut().next().await {
            match event {
                #[cfg(any(feature = "permanode", feature = "selective-permanode"))]
                ChronicleEvent::Broker(broker_event) => {
                    if let Event::Published(_, _, broker) = broker_event {
                        self.broker = broker;
                        rt.publish(self.broker.clone()).await;
                    }
                }
                ChronicleEvent::Scylla(scylla_event) => {
                    if let Event::Published(_, _, scylla) = scylla_event {
                        self.scylla = scylla;
                        rt.publish(self.scylla.clone()).await;
                    }
                }
                ChronicleEvent::Api(api_event) => {
                    if let Event::Published(_, _, api) = api_event {
                        self.api = api;
                        rt.publish(self.api.clone()).await;
                    }
                }
                ChronicleEvent::MicroService(scope_id, service, _result_opt) => {
                    rt.upsert_microservice(scope_id, service);
                    if rt.microservices_stopped() {
                        break;
                    }
                }
                ChronicleEvent::Shutdown => rt.inbox_mut().close(),
            }
        }
        Ok(())
    }
}

fn main() {
    dotenv::dotenv().ok();
    #[cfg(not(feature = "console"))]
    {
        let env = env_logger::Env::new().filter_or("RUST_LOG", "info");
        env_logger::Builder::from_env(env).init();
    }
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_stack_size(TOKIO_THREAD_STACK_SIZE)
        .build()
        .expect("Expected to build tokio runtime");
    runtime.block_on(chronicle());
}

async fn chronicle() {
    Runtime::from_config::<Chronicle>()
        .await
        .expect("Chronicle Runtime to run")
        .block_on()
        .await
        .expect("Runtime to shutdown gracefully");
}

async fn init_database(keyspace_config: &KeyspaceConfig) -> anyhow::Result<()> {
    log::warn!(
        "Initializing Chronicle data model for keyspace: {}",
        keyspace_config.name
    );
    let datacenters = keyspace_config
        .data_centers
        .iter()
        .map(|(datacenter_name, datacenter_config)| {
            format!("'{}': {}", datacenter_name, datacenter_config.replication_factor)
        })
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "CREATE KEYSPACE IF NOT EXISTS {0}
            WITH replication = {{'class': 'NetworkTopologyStrategy', {1}}}
            AND durable_writes = true;",
        keyspace_config.name, datacenters
    )
    .as_execute_query(&[])
    .consistency(Consistency::All)
    .build()?
    .get_local()
    .await
    .map_err(|e| anyhow::Error::msg(format!("Could not verify if keyspace was created! error: {}", e)))?;
    log::info!("Created Chronicle keyspace with name: {}", keyspace_config.name);
    let table_queries = format!(
        "CREATE TABLE IF NOT EXISTS {0}.messages (
                message_id text PRIMARY KEY,
                message blob,
                metadata blob,
            );
            CREATE TABLE IF NOT EXISTS {0}.addresses  (
                address text,
                partition_id smallint,
                milestone_index int,
                output_type tinyint,
                transaction_id text,
                idx smallint,
                amount bigint,
                address_type tinyint,
                inclusion_state blob,
                PRIMARY KEY ((address, partition_id), milestone_index, output_type, transaction_id, idx)
            ) WITH CLUSTERING ORDER BY (milestone_index DESC, output_type DESC, transaction_id DESC, idx DESC);
            CREATE TABLE IF NOT EXISTS {0}.indexes  (
                indexation text,
                partition_id smallint,
                milestone_index int,
                message_id text,
                inclusion_state blob,
                PRIMARY KEY ((indexation, partition_id), milestone_index, message_id)
            ) WITH CLUSTERING ORDER BY (milestone_index DESC);
            CREATE TABLE IF NOT EXISTS {0}.parents  (
                parent_id text,
                partition_id smallint,
                milestone_index int,
                message_id text,
                inclusion_state blob,
                PRIMARY KEY ((parent_id, partition_id), milestone_index, message_id)
            ) WITH CLUSTERING ORDER BY (milestone_index DESC);
            CREATE TABLE IF NOT EXISTS {0}.transactions  (
                transaction_id text,
                idx smallint,
                variant text,
                message_id text,
                data blob,
                inclusion_state blob,
                milestone_index int,
                PRIMARY KEY (transaction_id, idx, variant, message_id, data)
            );
            CREATE TABLE IF NOT EXISTS {0}.milestones  (
                milestone_index int,
                message_id text,
                timestamp bigint,
                payload blob,
                PRIMARY KEY (milestone_index, message_id)
            );
            CREATE TABLE IF NOT EXISTS {0}.hints  (
                hint text,
                variant text,
                partition_id smallint,
                milestone_index int,
                PRIMARY KEY (hint, variant, partition_id)
            ) WITH CLUSTERING ORDER BY (variant DESC, partition_id DESC);
            CREATE TABLE IF NOT EXISTS {0}.sync  (
                key text,
                milestone_index int,
                synced_by tinyint,
                logged_by tinyint,
                PRIMARY KEY (key, milestone_index)
            ) WITH CLUSTERING ORDER BY (milestone_index DESC);

            CREATE TABLE IF NOT EXISTS {0}.analytics (
                key text,
                milestone_index int,
                message_count int,
                transaction_count int,
                transferred_tokens bigint,
                PRIMARY KEY (key, milestone_index)
            ) WITH CLUSTERING ORDER BY (milestone_index DESC);",
        keyspace_config.name,
    );

    for query in table_queries.split(";").map(str::trim).filter(|s| !s.is_empty()) {
        query
            .as_execute_query(&[])
            .consistency(Consistency::All)
            .build()?
            .get_global()
            .await
            .map_err(|e| {
                anyhow::Error::msg(format!(
                    "Could not verify if table: {}, was created! error: {}",
                    query, e
                ))
            })?;
    }
    log::info!("Created Chronicle tables for keyspace name: {}", keyspace_config.name);
    log::info!(
        "Initialized Chronicle data model for keyspace: {}",
        keyspace_config.name
    );
    Ok(())
}
