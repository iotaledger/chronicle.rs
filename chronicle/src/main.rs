// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

#![warn(missing_docs)]
//! # Chronicle
use async_trait::async_trait;
use backstage::core::*;
use chronicle_api::application::*;
use chronicle_broker::{
    application::ChronicleBroker,
    SelectivePermanodeConfig,
};
use chronicle_common::{
    alert,
    config::AlertConfig,
};
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
    /// Permanode application
    #[cfg(all(feature = "permanode", not(feature = "selective-permanode")))]
    broker: ChronicleBroker<PermanodeConfig>,
    /// Selective Permanode application
    #[cfg(feature = "selective-permanode")]
    broker: ChronicleBroker<SelectivePermanodeConfig>,
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
    Broker(Event<ChronicleBroker<PermanodeConfig>>),
    #[cfg(feature = "selective-permanode")]
    Broker(Event<ChronicleBroker<SelectivePermanodeConfig>>),
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
impl From<Event<ChronicleBroker<PermanodeConfig>>> for ChronicleEvent {
    fn from(e: Event<ChronicleBroker<PermanodeConfig>>) -> Self {
        Self::Broker(e)
    }
}

#[cfg(feature = "selective-permanode")]
impl From<Event<ChronicleBroker<SelectivePermanodeConfig>>> for ChronicleEvent {
    fn from(e: Event<ChronicleBroker<SelectivePermanodeConfig>>) -> Self {
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
        // - api
        let api_scope_id = rt.start("api".to_string(), self.api.clone()).await?.scope_id();
        if let Some(api) = rt.subscribe::<ChronicleAPI>(api_scope_id, "api".to_string()).await? {
            if self.api != api {
                self.api = api;
                rt.publish(self.api.clone()).await;
            }
        }
        log::info!("Chronicle Started Api");
        #[cfg(any(feature = "permanode", feature = "selective-permanode"))]
        {
            let broker = self.broker.clone();
            let broker_scope_id = rt.start("broker".to_string(), broker).await?.scope_id();
            if let Some(broker) = rt.subscribe(broker_scope_id, "broker".to_string()).await? {
                if self.broker != broker {
                    self.broker = broker;
                    rt.publish(self.broker.clone()).await;
                }
            }
        }
        log::info!("Chronicle Started Broker");
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
                    let is_stopped = service.is_stopped();
                    rt.upsert_microservice(scope_id, service);
                    if !rt.service().is_stopping() {
                        if is_stopped {
                            rt.stop().await;
                        } else {
                            let same_status = rt.service().status().clone();
                            rt.update_status(same_status).await;
                        }
                    } else {
                        rt.update_status(ServiceStatus::Stopping).await;
                    }
                    if rt.microservices_stopped() {
                        rt.inbox_mut().close();
                    }
                }
                ChronicleEvent::Shutdown => rt.stop().await,
            }
        }
        log::info!("Chronicle exited its event loop");
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
    let backserver_addr: std::net::SocketAddr = std::env::var("BACKSERVER_ADDR").map_or_else(
        |_| ([127, 0, 0, 1], 9999).into(),
        |n| {
            n.parse()
                .expect("Invalid BACKSERVER_ADDR env, use this format '127.0.0.1:9999' ")
        },
    );
    Runtime::from_config::<Chronicle>()
        .await
        .expect("Failed to run chronicle system")
        .backserver(backserver_addr)
        .await
        .expect("Failed to run backserver")
        .block_on()
        .await
        .expect("Failed to shutdown the runtime gracefully");
}

async fn init_database(keyspace_config: &KeyspaceConfig) -> anyhow::Result<()> {
    log::warn!(
        "Initializing Chronicle data model for keyspace: {}",
        keyspace_config.name
    );
    let replications = format!(
        "{{{}}}",
        keyspace_config
            .data_centers
            .iter()
            .map(|(datacenter_name, datacenter_config)| {
                format!("'{}': {}", datacenter_name, datacenter_config.replication_factor)
            })
            .chain(std::iter::once("'class': 'NetworkTopologyStrategy'".to_string()))
            .collect::<Vec<_>>()
            .join(", ")
    )
    .parse::<Replication>()?;
    parse_statement!(
        "CREATE KEYSPACE IF NOT EXISTS #0
            WITH replication = #1
            AND durable_writes = true;",
        keyspace_config.name.clone(),
        replications
    )
    .execute()
    .consistency(Consistency::All)
    .build()?
    .get_local()
    .await
    .map_err(|e| anyhow::Error::msg(format!("Could not verify if keyspace was created! error: {}", e)))?;
    log::info!("Created Chronicle keyspace with name: {}", keyspace_config.name);
    let table_queries = parse_statements!(
        "CREATE TABLE IF NOT EXISTS #0.messages (
            message_id text PRIMARY KEY,
            message blob,
            milestone_index int,
            inclusion_state tinyint,
            conflict_reason tinyint,
            proof blob
        );

        CREATE TABLE IF NOT EXISTS #0.transactions (
            transaction_id text,
            idx smallint,
            variant text,
            message_id text,
            data blob,
            inclusion_state tinyint,
            milestone_index int,
            PRIMARY KEY (transaction_id, idx, variant, message_id)
        );
        
        CREATE MATERIALIZED VIEW IF NOT EXISTS #0.transactions_by_state AS 
            SELECT transaction_id, idx, variant, message_id, inclusion_state
            FROM #0.transactions
            WHERE transaction_id IS NOT NULL
            AND idx IS NOT NULL
            AND variant IS NOT NULL
            AND message_id IS NOT NULL
            AND inclusion_state IS NOT NULL
        PRIMARY KEY (transaction_id, inclusion_state, idx, variant, message_id);
        
        CREATE TABLE IF NOT EXISTS #0.milestones (
            milestone_index int,
            message_id text,
            timestamp bigint,
            payload blob,
            PRIMARY KEY (milestone_index, message_id)
        );
        
        CREATE TABLE IF NOT EXISTS #0.tag_hints (
            tag text,
            table_kind text,
            ms_range_id int,
            PRIMARY KEY (tag, ms_range_id)
        ) WITH CLUSTERING ORDER BY (ms_range_id DESC);
        
        CREATE TABLE IF NOT EXISTS #0.addresses_hints (
            address text,
            output_kind text, 
            variant text, 
            ms_range_id int,
            PRIMARY KEY (address, output_kind, variant, ms_range_id)
        ) WITH CLUSTERING ORDER BY (ms_range_id DESC);
        
        CREATE TABLE IF NOT EXISTS #0.basic_outputs (
            output_id text,
            ms_range_id int,
            milestone_index int,
            ms_timestamp timestamp,
            inclusion_state tinyint,
            address text,
            sender text,
            tag text,
            data blob,
            PRIMARY KEY (output_id, ms_range_id, milestone_index, ms_timestamp)
        ) WITH CLUSTERING ORDER BY (ms_range_id DESC, milestone_index DESC, ms_timestamp DESC);
        
        CREATE MATERIALIZED VIEW IF NOT EXISTS #0.basic_outputs_by_address AS
            SELECT * from #0.basic_outputs
            WHERE output_id IS NOT NULL
            AND address IS NOT NULL
            AND ms_range_id IS NOT NULL
            AND ms_timestamp IS NOT NULL
            AND milestone_index IS NOT NULL
        PRIMARY KEY ((address, ms_range_id), ms_timestamp, milestone_index, output_id)
        WITH CLUSTERING ORDER BY (ms_timestamp DESC, milestone_index DESC);
        
        CREATE MATERIALIZED VIEW IF NOT EXISTS #0.basic_outputs_by_tag AS
            SELECT * from #0.basic_outputs
            WHERE output_id IS NOT NULL
            AND tag IS NOT NULL
            AND ms_range_id IS NOT NULL
            AND ms_timestamp IS NOT NULL
            AND milestone_index IS NOT NULL
        PRIMARY KEY ((tag, ms_range_id), ms_timestamp, milestone_index, output_id)
        WITH CLUSTERING ORDER BY (ms_timestamp DESC, milestone_index DESC);
        
        CREATE MATERIALIZED VIEW IF NOT EXISTS #0.basic_outputs_by_sender AS
            SELECT * from #0.basic_outputs
            WHERE output_id IS NOT NULL
            AND sender IS NOT NULL
            AND ms_range_id IS NOT NULL
            AND ms_timestamp IS NOT NULL
            AND milestone_index IS NOT NULL
        PRIMARY KEY ((sender, ms_range_id), ms_timestamp, milestone_index, output_id)
        WITH CLUSTERING ORDER BY (ms_timestamp DESC, milestone_index DESC);
        
        CREATE TABLE IF NOT EXISTS #0.alias_outputs (
            alias_id text,
            ms_range_id int,
            milestone_index int,
            ms_timestamp timestamp,
            inclusion_state tinyint,
            sender text,
            issuer text,
            state_controller text,
            governor text,
            data blob,
            PRIMARY KEY (alias_id, ms_range_id, milestone_index, ms_timestamp)
        ) WITH CLUSTERING ORDER BY (ms_range_id DESC, milestone_index DESC, ms_timestamp DESC);
        
        CREATE MATERIALIZED VIEW IF NOT EXISTS #0.alias_outputs_by_sender AS
            SELECT * from #0.alias_outputs
            WHERE alias_id IS NOT NULL
            AND sender IS NOT NULL
            AND ms_range_id IS NOT NULL
            AND ms_timestamp IS NOT NULL
            AND milestone_index IS NOT NULL
        PRIMARY KEY ((sender, ms_range_id), ms_timestamp, milestone_index, alias_id)
        WITH CLUSTERING ORDER BY (ms_timestamp DESC, milestone_index DESC);
        
        CREATE MATERIALIZED VIEW IF NOT EXISTS #0.alias_outputs_by_issuer AS
            SELECT * from #0.alias_outputs
            WHERE alias_id IS NOT NULL
            AND issuer IS NOT NULL
            AND ms_range_id IS NOT NULL
            AND ms_timestamp IS NOT NULL
            AND milestone_index IS NOT NULL
        PRIMARY KEY ((issuer, ms_range_id), ms_timestamp, milestone_index, alias_id)
        WITH CLUSTERING ORDER BY (ms_timestamp DESC, milestone_index DESC);
        
        CREATE MATERIALIZED VIEW IF NOT EXISTS #0.alias_outputs_by_state_controller AS
            SELECT * from #0.alias_outputs
            WHERE alias_id IS NOT NULL
            AND state_controller IS NOT NULL
            AND ms_range_id IS NOT NULL
            AND ms_timestamp IS NOT NULL
            AND milestone_index IS NOT NULL
        PRIMARY KEY ((state_controller, ms_range_id), ms_timestamp, milestone_index, alias_id)
        WITH CLUSTERING ORDER BY (ms_timestamp DESC, milestone_index DESC);
        
        CREATE MATERIALIZED VIEW IF NOT EXISTS #0.alias_outputs_by_governor AS
            SELECT * from #0.alias_outputs
            WHERE alias_id IS NOT NULL
            AND governor IS NOT NULL
            AND ms_range_id IS NOT NULL
            AND ms_timestamp IS NOT NULL
            AND milestone_index IS NOT NULL
        PRIMARY KEY ((governor, ms_range_id), ms_timestamp, milestone_index, alias_id)
        WITH CLUSTERING ORDER BY (ms_timestamp DESC, milestone_index DESC);
        
        CREATE TABLE IF NOT EXISTS #0.foundry_outputs (
            foundry_id text,
            ms_range_id int,
            milestone_index int,
            ms_timestamp timestamp,
            inclusion_state tinyint,
            address text,
            data blob,
            PRIMARY KEY (foundry_id, ms_range_id, milestone_index, ms_timestamp)
        ) WITH CLUSTERING ORDER BY (ms_range_id DESC, milestone_index DESC, ms_timestamp DESC);
        
        CREATE MATERIALIZED VIEW IF NOT EXISTS #0.foundry_outputs_by_address AS
            SELECT * from #0.foundry_outputs
            WHERE foundry_id IS NOT NULL
            AND address IS NOT NULL
            AND ms_range_id IS NOT NULL
            AND ms_timestamp IS NOT NULL
            AND milestone_index IS NOT NULL
        PRIMARY KEY ((address, ms_range_id), ms_timestamp, milestone_index, foundry_id)
        WITH CLUSTERING ORDER BY (ms_timestamp DESC, milestone_index DESC);
        
        CREATE TABLE IF NOT EXISTS #0.nft_outputs (
            nft_id text,
            ms_range_id int,
            milestone_index int,
            ms_timestamp timestamp,
            inclusion_state tinyint,
            address text,
            dust_return_address text,
            sender text,
            issuer text,
            tag text,
            data blob,
            PRIMARY KEY (nft_id, ms_range_id, milestone_index, ms_timestamp)
        ) WITH CLUSTERING ORDER BY (ms_range_id DESC, milestone_index DESC, ms_timestamp DESC);
        
        CREATE MATERIALIZED VIEW IF NOT EXISTS #0.nft_outputs_by_address AS
            SELECT * from #0.nft_outputs
            WHERE nft_id IS NOT NULL
            AND address IS NOT NULL
            AND ms_range_id IS NOT NULL
            AND ms_timestamp IS NOT NULL
            AND milestone_index IS NOT NULL
        PRIMARY KEY ((address, ms_range_id), ms_timestamp, milestone_index, nft_id)
        WITH CLUSTERING ORDER BY (ms_timestamp DESC, milestone_index DESC);
        
        CREATE MATERIALIZED VIEW IF NOT EXISTS #0.nft_outputs_by_dust_return_address AS
            SELECT * from #0.nft_outputs
            WHERE nft_id IS NOT NULL
            AND dust_return_address IS NOT NULL
            AND ms_range_id IS NOT NULL
            AND ms_timestamp IS NOT NULL
            AND milestone_index IS NOT NULL
        PRIMARY KEY ((dust_return_address, ms_range_id), ms_timestamp, milestone_index, nft_id)
        WITH CLUSTERING ORDER BY (ms_timestamp DESC, milestone_index DESC);
        
        CREATE MATERIALIZED VIEW IF NOT EXISTS #0.nft_outputs_by_sender AS
            SELECT * from #0.nft_outputs
            WHERE nft_id IS NOT NULL
            AND sender IS NOT NULL
            AND ms_range_id IS NOT NULL
            AND ms_timestamp IS NOT NULL
            AND milestone_index IS NOT NULL
        PRIMARY KEY ((sender, ms_range_id), ms_timestamp, milestone_index, nft_id)
        WITH CLUSTERING ORDER BY (ms_timestamp DESC, milestone_index DESC);
        
        CREATE MATERIALIZED VIEW IF NOT EXISTS #0.nft_outputs_by_issuer AS
            SELECT * from #0.nft_outputs
            WHERE nft_id IS NOT NULL
            AND issuer IS NOT NULL
            AND ms_range_id IS NOT NULL
            AND ms_timestamp IS NOT NULL
            AND milestone_index IS NOT NULL
        PRIMARY KEY ((issuer, ms_range_id), ms_timestamp, milestone_index, nft_id)
        WITH CLUSTERING ORDER BY (ms_timestamp DESC, milestone_index DESC);
        
        CREATE MATERIALIZED VIEW IF NOT EXISTS #0.nft_outputs_by_tag AS
            SELECT * from #0.nft_outputs
            WHERE nft_id IS NOT NULL
            AND tag IS NOT NULL
            AND ms_range_id IS NOT NULL
            AND ms_timestamp IS NOT NULL
            AND milestone_index IS NOT NULL
        PRIMARY KEY ((tag, ms_range_id), ms_timestamp, milestone_index, nft_id)
        WITH CLUSTERING ORDER BY (ms_timestamp DESC, milestone_index DESC);
        
        CREATE TABLE IF NOT EXISTS #0.legacy_outputs (
            output_id text,
            output_type tinyint,
            ms_range_id int,
            milestone_index int,
            ms_timestamp timestamp,
            inclusion_state tinyint,
            address text,
            data blob,
            PRIMARY KEY (output_id, output_type, ms_range_id, milestone_index, ms_timestamp)
        ) WITH CLUSTERING ORDER BY (output_type ASC, ms_range_id DESC, milestone_index DESC, ms_timestamp DESC);
        
        CREATE MATERIALIZED VIEW IF NOT EXISTS #0.legacy_outputs_by_address AS
            SELECT * from #0.legacy_outputs
            WHERE output_id IS NOT NULL
            AND output_type IS NOT NULL
            AND address IS NOT NULL
            AND ms_range_id IS NOT NULL
            AND ms_timestamp IS NOT NULL
            AND milestone_index IS NOT NULL
        PRIMARY KEY ((address, ms_range_id), output_type, ms_timestamp, milestone_index, output_id)
        WITH CLUSTERING ORDER BY (output_type ASC, ms_timestamp DESC, milestone_index DESC);
        
        CREATE TABLE IF NOT EXISTS #0.tags (
            tag text,
            ms_range_id int,
            milestone_index int,
            ms_timestamp timestamp,
            message_id text,
            inclusion_state blob,
            PRIMARY KEY ((tag, ms_range_id), ms_timestamp, milestone_index)
        ) WITH CLUSTERING ORDER BY (ms_timestamp DESC, milestone_index DESC);
        
        CREATE TABLE IF NOT EXISTS #0.parents (
            parent_id text,
            milestone_index int,
            ms_timestamp timestamp,
            message_id text,
            inclusion_state tinyint,
            PRIMARY KEY (parent_id, message_id)
        );
        
        CREATE MATERIALIZED VIEW IF NOT EXISTS #0.parents_by_ms AS
            SELECT * from #0.parents
            WHERE parent_id IS NOT NULL
            AND message_id IS NOT NULL
            AND milestone_index IS NOT NULL
            AND ms_timestamp IS NOT NULL
        PRIMARY KEY (parent_id, ms_timestamp, message_id)
        WITH CLUSTERING ORDER BY (ms_timestamp DESC);
        
        CREATE TABLE IF NOT EXISTS #0.sync (
            ms_range_id int,
            milestone_index int,
            synced_by tinyint,
            logged_by tinyint,
            PRIMARY KEY (ms_range_id, milestone_index)
        ) WITH CLUSTERING ORDER BY (milestone_index DESC);
        
        CREATE TABLE IF NOT EXISTS #0.ms_analytics (
            ms_range_id int,
            milestone_index int,
            message_count int,
            transaction_count int,
            transferred_tokens bigint,
            PRIMARY KEY (ms_range_id, milestone_index)
        ) WITH CLUSTERING ORDER BY (milestone_index DESC);
        
        CREATE TABLE IF NOT EXISTS #0.daily_analytics (
            year int,
            date date,
            total_addresses int,
            send_addresses int,
            recv_addresses int,
            PRIMARY KEY (year, date)
        ) WITH CLUSTERING ORDER BY (date DESC);
        
        CREATE TABLE IF NOT EXISTS #0.address_analytics (
            address text,
            milestone_index int,
            sent_tokens bigint,
            recv_tokens bigint,
            PRIMARY KEY (address, milestone_index)
        ) WITH CLUSTERING ORDER BY (milestone_index DESC);
        
        CREATE TABLE IF NOT EXISTS #0.date_cache (
            date date,
            start_ms int,
            end_ms int,
            PRIMARY KEY (date)
        );
        
        CREATE TABLE IF NOT EXISTS #0.metrics_cache (
            date date,
            variant text,
            value text,
            metric text,
            metric_value blob,
            PRIMARY KEY (date, variant, metric, value)
        );",
        keyspace_config.name.clone(),
    );

    for query in table_queries {
        log::info!(
            "Creating table: {}",
            match query {
                Statement::DataDefinition(DataDefinitionStatement::CreateTable(ref s)) => &s.table,
                _ => unreachable!(),
            }
        );
        log::info!("Query: {}", query);
        query
            .execute()
            .consistency(Consistency::All)
            .build()?
            .get_global()
            .await
            .map_err(|e| anyhow::Error::msg(format!("Could not verify if table was created! error: {}", e)))?;
    }
    log::info!("Created Chronicle tables for keyspace name: {}", keyspace_config.name);
    log::info!(
        "Initialized Chronicle data model for keyspace: {}",
        keyspace_config.name
    );
    Ok(())
}
