// import the apps you want to build
use chronicle_api::api::ApiBuilder;
use chronicle_broker::broker::BrokerBuilder;
use chronicle_storage::storage::StorageBuilder;
// import launcher macro
use chronicle_common::launcher;
// import helper async fns to add scylla nodes and build ring, initialize schema, import dmps
use chronicle_broker::importer::ImporterBuilder;
use chronicle_storage::{
    dashboard::client::add_nodes,
    worker::schema_cql::SchemaCqlBuilder,
};
use serde::Deserialize;
use std::{
    fs,
    path::PathBuf,
};
use structopt::StructOpt;
use tokio::runtime::Builder;

#[derive(Debug, StructOpt)]
#[structopt(name = "chronicle-alpha-v0_1_0", about = "Chronicle Permanode Alpha v0.1.0")]
struct Args {
    /// Configure file
    #[structopt(parse(from_os_str))]
    path: PathBuf,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    scylla_cluster: ScyllaCluster,
    dmp_files: Option<DmpFiles>,
    tokio: Tokio,
    storage: Storage,
    api: Api,
    broker: Broker,
}

#[derive(Debug, Clone, Deserialize)]
struct ScyllaCluster {
    addresses: Vec<String>,
    keyspace_name: String,
    replication_factor_per_data_center: u8,
    data_centers: Vec<String>,
    local_dc: String,
}

#[derive(Debug, Clone, Deserialize)]
struct DmpFiles {
    files: Option<Vec<(String, u64)>>,
}

#[derive(Debug, Clone, Deserialize)]
struct Tokio {
    core_threads: usize,
}

#[derive(Debug, Clone, Deserialize)]
struct Storage {
    dashboard_websocket: String,
}

#[derive(Debug, Clone, Deserialize)]
struct Api {
    endpoint: String,
}

#[derive(Debug, Clone, Deserialize)]
struct Broker {
    trytes_nodes: Option<Vec<String>>,
    sn_trytes_nodes: Option<Vec<String>>,
}

launcher!(
    apps_builder: AppsBuilder {storage: StorageBuilder, api: ApiBuilder, broker: BrokerBuilder},
    apps: Apps{config: Config}
);

// build your apps
impl AppsBuilder {
    fn build(self, config: Config) -> Apps {
        // 
        // - storage app:
        let storage = StorageBuilder::new()
            .listen_address(config.storage.dashboard_websocket.clone())
            .thread_count(config.tokio.core_threads)
            .local_dc(config.scylla_cluster.local_dc.clone())
            .reporter_count(2)
            .buffer_size(1024000)
            .recv_buffer_size(1024000)
            .send_buffer_size(1024000);
        // 
        // - api app
        let api = ApiBuilder::new().listen_address(config.api.endpoint.clone());
        // 
        // - broker app
        let mut broker = BrokerBuilder::new();
        if let Some(trytes_nodes) = config.broker.trytes_nodes.as_ref() {
            broker = broker.trytes(trytes_nodes.to_vec());
        }
        if let Some(sn_trytes_nodes) = config.broker.sn_trytes_nodes.as_ref() {
            broker = broker.sn_trytes(sn_trytes_nodes.to_vec());
        }
        // add app to AppsBuilder then transform it to Apps
        self.storage(storage).api(api).broker(broker).to_apps().config(config)
    }
}

fn main() {
    let args = Args::from_args();
    let config_as_string = fs::read_to_string(args.path).unwrap();
    let config: Config = toml::from_str(&config_as_string).unwrap();
    // build tokio runtime
    let mut runtime = Builder::new()
        .threaded_scheduler()
        .core_threads(config.tokio.core_threads)
        .enable_io()
        .thread_name("chronicle")
        .thread_stack_size(3 * 1024 * 1024)
        .build()
        .unwrap();
    println!("Welcome to Chronicle Permanode Alpha v0.1.0");
    let apps = AppsBuilder::new().build(config); // build apps first, then start them in order you want.
                                                 // run chronicle.
    runtime.block_on(async {
        apps.function(|apps| {
            // for instance this is helpful to spawn ctrl_c future
            tokio::spawn(ctrl_c(apps.tx.clone()));
        })
        .await // you can start some function(it must never block)
        .storage()
        .await // start storage app
        .api()
        .await // start api app
        .future(|mut apps| async {
            let mut config = apps.config.take().unwrap();
            let dashboard_websocket = format!("ws://{}/", config.storage.dashboard_websocket);
            let scylla_nodes = config.scylla_cluster.addresses.clone();
            let rf = config.scylla_cluster.replication_factor_per_data_center;
            // add nodes and initialize ring
            add_nodes(dashboard_websocket.as_str(), scylla_nodes, rf)
                .await
                .expect("failed to add nodes");
            // create tangle keyspace
            SchemaCqlBuilder::new()
                .statement(CREATE_TANGLE_KEYSPACE_QUERY.to_string())
                .build()
                .run()
                .await
                .expect("failed to create tangle keyspace");
            // create transaction table
            SchemaCqlBuilder::new()
                .statement(CREATE_TANGLE_TX_TABLE_QUERY.to_string())
                .build()
                .run()
                .await
                .expect("failed to create tangle.transaction table");
            // create edge table
            SchemaCqlBuilder::new()
                .statement(CREATE_TANGLE_EDGE_TABLE_QUERY.to_string())
                .build()
                .run()
                .await
                .expect("failed to create tangle.edge table");
            // create data table
            SchemaCqlBuilder::new()
                .statement(CREATE_TANGLE_DATA_TABLE_QUERY.to_string())
                .build()
                .run()
                .await
                .expect("failed to create tangle.data table");
            if let Some(dmp_files) = config.dmp_files {
                import_files(dmp_files.files.unwrap()).await;
            }
            apps
        })
        .await
        .broker()
        .await // start broker app
        .one_for_one()
        .await; // instead you can define your own .run() strategy
    });
}

/// Useful function to exit program using ctrl_c signal
async fn ctrl_c(mut launcher: Sender) {
    // await on ctrl_c
    tokio::signal::ctrl_c().await.unwrap();
    // exit program using launcher
    launcher.exit_program();
}

async fn import_files(mut tuples: Vec<(String, u64)>) {
    tuples.sort_by(|a, b| b.1.cmp(&a.1));
    for t in tuples.iter() {
        if let Ok(_) = ImporterBuilder::new()
            .filepath(t.0.clone())
            .milestone(t.1)
            .max_retries(0)
            .build()
            .run()
            .await
        {
            println!("succesfully imported: {}", t.0);
        } else {
            panic!("failed to import file: {}", t.0);
        }
    }
}

// useful consts for the example
const CREATE_TANGLE_KEYSPACE_QUERY: &str = r#"
CREATE KEYSPACE IF NOT EXISTS tangle
WITH REPLICATION = {
  'class': 'SimpleStrategy',
  'replication_factor': 1
};
"#;

const CREATE_TANGLE_TX_TABLE_QUERY: &str = r#"
CREATE TABLE IF NOT EXISTS tangle.transaction (
  hash blob PRIMARY KEY,
  payload blob,
  address blob,
  value blob,
  obsolete_tag blob,
  timestamp blob,
  current_index blob,
  last_index blob,
  bundle blob,
  trunk blob,
  branch blob,
  tag blob,
  attachment_timestamp blob,
  attachment_timestamp_lower blob,
  attachment_timestamp_upper blob,
  nonce blob,
  milestone bigint,
);
"#;

const CREATE_TANGLE_EDGE_TABLE_QUERY: &str = r#"
CREATE TABLE IF NOT EXISTS tangle.edge (
  vertex blob,
  kind text,
  timestamp bigint,
  tx blob,
  value bigint,
  extra blob,
  PRIMARY KEY(vertex, kind, timestamp, tx)
);
"#;

const CREATE_TANGLE_DATA_TABLE_QUERY: &str = r#"
CREATE TABLE IF NOT EXISTS tangle.data (
  vertex blob,
  year smallint,
  month tinyint,
  kind text,
  timestamp bigint,
  tx blob,
  extra blob,
  PRIMARY KEY((vertex,year,month), kind, timestamp, tx)
);
"#;
