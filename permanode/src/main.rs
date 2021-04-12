#![warn(missing_docs)]
//! # Permanode
use permanode_api::application::*;
use permanode_broker::application::*;
use permanode_common::{
    config::*,
    get_config,
    get_config_async,
    get_history_mut,
    metrics::*,
};
use permanode_storage::access::{
    PermanodeKeyspace,
    Query,
    Statements,
};
use scylla::{
    access::{
        send_local,
        Consistency,
        Keyspace,
        ReporterHandle,
    },
    application::*,
    Worker,
    WorkerError,
};
use std::time::Duration;
use tokio::sync::mpsc::{
    unbounded_channel,
    UnboundedSender,
};
use websocket::*;

mod websocket;

launcher!
(
    builder: AppsBuilder
    {
        [] -> PermanodeBroker<Sender>: PermanodeBrokerBuilder<Sender>,
        [] -> PermanodeAPI<Sender>: PermanodeAPIBuilder<Sender>,
        [] -> Websocket<Sender>: WebsocketBuilder<Sender>,
        [PermanodeBroker, PermanodeAPI] -> Scylla<Sender>: ScyllaBuilder<Sender>
    },
    state: Apps {}
);

impl Builder for AppsBuilder {
    type State = Apps;

    fn build(self) -> Self::State {
        let storage_config = get_config().storage_config;
        let permanode_api_builder = PermanodeAPIBuilder::new();
        let permanode_broker_builder = PermanodeBrokerBuilder::new();
        let scylla_builder = ScyllaBuilder::new()
            .listen_address(storage_config.listen_address.to_string())
            .thread_count(match storage_config.thread_count {
                ThreadCount::Count(c) => c,
                ThreadCount::CoreMultiple(c) => num_cpus::get() * c,
            })
            .reporter_count(storage_config.reporter_count)
            .local_dc(storage_config.local_datacenter.clone());
        let websocket_builder = WebsocketBuilder::new();

        self.PermanodeAPI(permanode_api_builder)
            .PermanodeBroker(permanode_broker_builder)
            .Scylla(scylla_builder)
            .Websocket(websocket_builder)
            .to_apps()
    }
}

fn main() {
    dotenv::dotenv().unwrap();
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
    let apps = AppsBuilder::new().build();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(thread_count)
        .thread_name("permanode")
        .thread_stack_size(apps.app_count * 4 * 1024 * 1024)
        .build()
        .expect("Expected to build tokio runtime");
    let mut new_config = config.clone();
    if let Err(e) = runtime.block_on(new_config.verify()) {
        panic!("{}", e)
    }
    if new_config != config {
        get_history_mut().update(new_config);
    }
    runtime.block_on(permanode(apps));
}

async fn permanode(apps: Apps) {
    apps.Scylla()
        .await
        .future(|apps| async {
            let storage_config = get_config_async().await.storage_config;
            debug!("Adding nodes: {:?}", storage_config.nodes);
            let ws = format!("ws://{}/", storage_config.listen_address);
            add_nodes(&ws, storage_config.nodes.iter().cloned().collect(), 1)
                .await
                .ok();
            tokio::time::sleep(Duration::from_secs(3)).await;
            init_database().await;
            apps
        })
        .await
        .PermanodeAPI()
        .await
        .PermanodeBroker()
        .await
        .Websocket()
        .await
        .start(None)
        .await;
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

async fn init_database() {
    let storage_config = get_config_async().await.storage_config;

    for keyspace_config in storage_config.keyspaces.first().iter() {
        let keyspace = PermanodeKeyspace::new(keyspace_config.name.clone());
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
            .build();
        send_local(token, keyspace_statement.0, worker, keyspace.name().to_string());
        if let Some(msg) = inbox.recv().await {
            match msg {
                Ok(_) => (),
                Err(e) => panic!("{}", e),
            }
        } else {
            panic!("Could not verify if keyspace was created!")
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
            ) WITH CLUSTERING ORDER BY (milestone_index DESC);",
            keyspace.name()
        );
        for query in table_queries.split(";").map(str::trim).filter(|s| !s.is_empty()) {
            let worker = BatchWorker::boxed(sender.clone());
            let statement = Query::new().statement(query).consistency(Consistency::One).build();
            send_local(token, statement.0, worker, keyspace.name().to_string());
            if let Some(msg) = inbox.recv().await {
                match msg {
                    Ok(_) => (),
                    Err(e) => panic!("{}", e),
                }
            } else {
                panic!("Could not verify if table was created!")
            }
        }
    }
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
    fn handle_response(self: Box<Self>, _giveload: Vec<u8>) {
        self.sender.send(Ok(())).ok();
    }

    fn handle_error(self: Box<Self>, error: WorkerError, _reporter: &Option<ReporterHandle>) {
        self.sender.send(Err(error)).ok();
    }
}
