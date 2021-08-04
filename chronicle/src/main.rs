// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

#![warn(missing_docs)]
//! # Chronicle
use anyhow::bail;
use async_trait::async_trait;
use backstage::prelude::*;
use chronicle_api::application::ChronicleAPI;
use chronicle_broker::application::*;
use chronicle_common::{
    config::*,
    get_config,
    get_config_async,
    get_history_mut,
    metrics::*,
};
use chronicle_storage::access::ChronicleKeyspace;
use scylla_rs::prelude::{
    stage::Reporter,
    *,
};
use std::time::Duration;
use tokio::sync::mpsc::{
    unbounded_channel,
    UnboundedSender,
};

struct Launcher;

#[supervise(Scylla, ChronicleBroker, ChronicleAPI)]
enum LauncherEvent {}

#[async_trait]
impl Actor for Launcher {
    type Dependencies = ();

    type Event = LauncherEvent;

    type Channel = UnboundedTokioChannel<Self::Event>;

    async fn init<Reg: RegistryAccess + Send + Sync, Sup: EventDriven>(
        &mut self,
        rt: &mut ActorScopedRuntime<Self, Reg, Sup>,
    ) -> Result<(), ActorError>
    where
        Self: Sized,
        Sup::Event: SupervisorEvent,
        <Sup::Event as SupervisorEvent>::Children: From<std::marker::PhantomData<Self>>,
    {
        rt.update_status(ServiceStatus::Initializing).await.ok();
        let config = get_config_async().await;

        let scylla = ScyllaBuilder::new()
            .listen_address(config.storage_config.listen_address)
            .thread_count(match config.storage_config.thread_count {
                ThreadCount::Count(c) => c,
                ThreadCount::CoreMultiple(c) => num_cpus::get() * c,
            })
            .reporter_count(config.storage_config.reporter_count)
            .local_dc(config.storage_config.local_datacenter.clone())
            .build();

        rt.spawn_actor(scylla).await?;
        rt.spawn_actor(ChronicleAPI).await?;
        let ws = format!("ws://{}/", config.storage_config.listen_address);
        let nodes = config.storage_config.nodes.iter().cloned().collect::<Vec<_>>();
        while let Err(e) = add_nodes(&ws, nodes.clone(), 1).await {
            log::error!("Error adding nodes: {}", e);
            log::info!("Trying again after 5 seconds...");
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
        tokio::task::spawn(ctrl_c(rt.handle()));
        log::info!("{}", rt.service_tree().await);
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
        <Sup::Event as SupervisorEvent>::Children: From<std::marker::PhantomData<Self>>,
    {
        rt.update_status(ServiceStatus::Running).await.ok();
        let mut broker_handle = None;
        while let Some(evt) = rt.next_event().await {
            match evt {
                LauncherEvent::StatusChange(s) => match s.actor_type {
                    Children::Scylla => {
                        if s.prev_status == ScyllaStatus::Disconnected.as_str() {
                            if let Err(e) = init_database().await {
                                log::error!("{}", e);
                                log::debug!("{}", rt.service_tree().await);
                            } else {
                                let config = get_config_async().await;
                                let chronicle_broker = ChronicleBrokerBuilder::new().config(config).build();
                                broker_handle.replace(rt.spawn_actor(chronicle_broker).await?);
                            }
                        } else if s.service.status() == ScyllaStatus::Disconnected.as_str() {
                            if let Some(handle) = broker_handle.take() {
                                handle.shutdown();
                            }
                        }
                    }
                    _ => (),
                },
                LauncherEvent::ReportExit(res) => match res {
                    Ok(s) => match s.state {
                        ChildStates::Scylla(_) | ChildStates::ChronicleAPI(_) => break,
                        _ => (),
                    },
                    Err(mut e) => match e.error.request().clone() {
                        ActorRequest::Restart => match e.state {
                            ChildStates::Scylla(s) => {
                                rt.spawn_actor(s).await?;
                            }
                            ChildStates::ChronicleAPI(a) => {
                                rt.spawn_actor(a).await?;
                            }
                            _ => (),
                        },
                        ActorRequest::Reschedule(dur) => match e.state {
                            ChildStates::Scylla(_) | ChildStates::ChronicleAPI(_) => {
                                e.error = ActorError::RuntimeError(ActorRequest::Restart);
                                let handle = rt.handle();
                                let evt = Self::Event::report_err(e);
                                tokio::spawn(async move {
                                    tokio::time::sleep(dur).await;
                                    handle.send(evt).ok();
                                });
                            }
                            _ => (),
                        },
                        ActorRequest::Finish | ActorRequest::Panic => match e.state {
                            ChildStates::Scylla(_) | ChildStates::ChronicleAPI(_) => break,
                            _ => (),
                        },
                    },
                },
            }
        }
        rt.update_status(ServiceStatus::Stopped).await.ok();
        Ok(())
    }
}

fn main() {
    dotenv::dotenv().unwrap();
    std::panic::set_hook(Box::new(|info| {
        log::error!("{}", info);
    }));
    env_logger::init();
    register_metrics();
    let config = get_config();
    let thread_count;
    match config.storage_config.thread_count {
        ThreadCount::Count(c) => {
            thread_count = c;
        }
        ThreadCount::CoreMultiple(c) => {
            thread_count = num_cpus::get() * c;
        }
    }
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(thread_count)
        .thread_name("chronicle")
        .thread_stack_size(4 * 1024 * 1024)
        .build()
        .expect("Failed to build tokio runtime!");
    let new_config = runtime.block_on(config.clone().verify()).unwrap();
    if new_config != config {
        get_history_mut().update(new_config.into());
    }
    runtime.block_on(chronicle());
}

async fn ctrl_c(shutdown_handle: Act<Launcher>) {
    tokio::signal::ctrl_c().await.unwrap();
    shutdown_handle.shutdown();
}

async fn chronicle() {
    Launcher.start_as_root::<ArcedRegistry>().await.unwrap()
}

fn register_metrics() {
    REGISTRY
        .register(Box::new(INCOMING_REQUESTS.clone()))
        .expect("Could not register collector");

    REGISTRY
        .register(Box::new(RESPONSE_CODE_COLLECTOR.clone()))
        .expect("Could not register collector");

    REGISTRY
        .register(Box::new(RESPONSE_TIME_COLLECTOR.clone()))
        .expect("Could not register collector");
}

async fn init_database() -> anyhow::Result<()> {
    let storage_config = get_config_async().await.storage_config;

    for keyspace_config in storage_config.keyspaces.first().iter() {
        let keyspace = ChronicleKeyspace::new(keyspace_config.name.clone());
        let datacenters = keyspace_config
            .data_centers
            .iter()
            .map(|(datacenter_name, datacenter_config)| {
                format!("'{}': {}", datacenter_name, datacenter_config.replication_factor)
            })
            .collect::<Vec<_>>()
            .join(", ");
        let (sender, mut inbox) = unbounded_channel::<Result<(), WorkerError>>();
        let worker = BatchWorker::boxed(sender.clone());
        let token = 1;
        let keyspace_statement = Query::new()
            .statement(&format!(
                "CREATE KEYSPACE IF NOT EXISTS {0}
                WITH replication = {{'class': 'NetworkTopologyStrategy', {1}}}
                AND durable_writes = true;",
                keyspace.name(),
                datacenters
            ))
            .consistency(Consistency::One)
            .build()?;
        send_local(token, keyspace_statement.0, worker, keyspace.name().to_string());
        if let Some(msg) = inbox.recv().await {
            match msg {
                Ok(_) => (),
                Err(e) => bail!(e),
            }
        } else {
            bail!("Could not verify if keyspace was created!")
        }
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
            keyspace.name()
        );
        for query in table_queries.split(";").map(str::trim).filter(|s| !s.is_empty()) {
            let worker = BatchWorker::boxed(sender.clone());
            let statement = Query::new().statement(query).consistency(Consistency::One).build()?;
            send_local(token, statement.0, worker, keyspace.name().to_string());
            if let Some(msg) = inbox.recv().await {
                match msg {
                    Ok(_) => (),
                    Err(e) => bail!(e),
                }
            } else {
                bail!("Could not verify if table was created!")
            }
        }
    }
    Ok(())
}

struct BatchWorker {
    sender: UnboundedSender<Result<(), WorkerError>>,
}

impl BatchWorker {
    pub fn boxed(sender: UnboundedSender<Result<(), WorkerError>>) -> Box<Self> {
        Box::new(Self { sender: sender.into() })
    }
}

impl Worker for BatchWorker {
    fn handle_response(self: Box<Self>, _giveload: Vec<u8>) -> anyhow::Result<()> {
        self.sender.send(Ok(()))?;
        Ok(())
    }

    fn handle_error(
        self: Box<Self>,
        error: WorkerError,
        _reporter: Option<&mut UnboundedSender<<Reporter as Actor>::Event>>,
    ) -> anyhow::Result<()> {
        self.sender.send(Err(error))?;
        Ok(())
    }
}
