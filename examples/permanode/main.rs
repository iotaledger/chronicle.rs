// import the apps you want to build
use chronicle_api::api::ApiBuilder;
use chronicle_broker::broker::BrokerBuilder;
use chronicle_storage::storage::StorageBuilder;
// import launcher macro and logger
use chronicle_common::{
    launcher,
    logger::{
        logger_init,
        LoggerConfigBuilder,
    },
};
use log::*;
// import helper async fns to add scylla nodes and build ring, initialize schema, import dmps
use chronicle_broker::importer::ImporterBuilder;
use chronicle_storage::{
    dashboard::client::add_nodes,
    worker::schema_cql::SchemaCqlBuilder,
};
use serde::Deserialize;
use std::{
    fmt::Write as FmtWrite,
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
    logger: LoggerConfigBuilder,
    version: Version,
    scylla_cluster: ScyllaCluster,
    dmp_files: Option<DmpFiles>,
    tokio: Tokio,
    storage: Storage,
    api: Api,
    broker: Broker,
}

#[derive(Debug, Clone, Deserialize)]
struct Version {
    version: String,
    service: Service,
}

#[derive(Debug, Clone, Deserialize)]
enum Service {
    Permanode,
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
    import_only_confirmed_transactions: Option<bool>,
    max_retries: Option<usize>,
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
    content_length: Option<u32>,
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
        // - logger
        logger_init(config.logger.clone().finish()).unwrap();
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
        let api = ApiBuilder::new()
            .listen_address(config.api.endpoint.clone())
            .content_length(config.api.content_length);
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
    let apps = AppsBuilder::new().build(config);
    info!("Welcome to Chronicle Permanode Alpha v0.1.0");
    // run chronicle.
    runtime.block_on(async {
        apps.storage()
            .await // start storage app
            .api()
            .await // start api app
            .future(|mut apps| async {
                let config = apps.config.take().unwrap();
                let dashboard_websocket = format!("ws://{}/", config.storage.dashboard_websocket);
                let scylla_nodes = config.scylla_cluster.addresses.clone();
                let rf = config.scylla_cluster.replication_factor_per_data_center;
                let statement_map = create_statements(config.scylla_cluster);
                // add nodes and initialize ring
                add_nodes(dashboard_websocket.as_str(), scylla_nodes, rf)
                    .await
                    .expect("failed to add nodes");
                // create tangle keyspace
                SchemaCqlBuilder::new()
                    .statement(statement_map["CREATE_KEYSPACE_QUERY"].clone())
                    .build()
                    .run()
                    .await
                    .expect("failed to create keyspace");
                // create transaction table
                SchemaCqlBuilder::new()
                    .statement(statement_map["CREATE_TX_TABLE_QUERY"].clone())
                    .build()
                    .run()
                    .await
                    .expect("failed to create transaction table");
                // create edge table
                SchemaCqlBuilder::new()
                    .statement(statement_map["CREATE_EDGE_TABLE_QUERY"].clone())
                    .build()
                    .run()
                    .await
                    .expect("failed to create edge table");
                // create data table
                SchemaCqlBuilder::new()
                    .statement(statement_map["CREATE_DATE_TABLE_QUERY"].clone())
                    .build()
                    .run()
                    .await
                    .expect("failed to create data table");
                if let Some(dmp_files) = config.dmp_files {
                    import_files(dmp_files).await;
                }
                apps
            })
            .await
            .broker()
            .await
            .function(|apps| {
                // for instance this is helpful to spawn ctrl_c future
                tokio::spawn(ctrl_c(apps.tx.clone()));
            })
            .await
            .one_for_one()
            .await;
    });
}

fn create_statements(scylla_cluster: ScyllaCluster) -> HashMap<String, String> {
    let keyspace_name = scylla_cluster.keyspace_name;
    let mut statement_map: HashMap<String, String> = HashMap::new();
    let mut create_tx_table_statement = String::new();
    let mut create_edge_table_statement = String::new();
    let mut create_data_table_statement = String::new();
    let mut create_key_space_statement = String::from("CREATE KEYSPACE IF NOT EXISTS ");
    create_key_space_statement.push_str(&keyspace_name);
    create_key_space_statement.push_str(" ");
    create_key_space_statement.push_str("WITH replication = {'class': 'NetworkTopologyStrategy',");
    create_key_space_statement.push_str(" ");
    for dc in &scylla_cluster.data_centers {
        create_key_space_statement.push_str("'");
        create_key_space_statement.push_str(dc);
        create_key_space_statement.push_str("'");
        create_key_space_statement.push_str(" : ");
        create_key_space_statement.push_str(&scylla_cluster.replication_factor_per_data_center.to_string());
        if dc != scylla_cluster.data_centers.last().unwrap() {
            create_key_space_statement.push_str(", ")
        }
    }
    create_key_space_statement.push_str("};");
    write!(
        &mut create_tx_table_statement,
        "CREATE TABLE IF NOT EXISTS {}.transaction (
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
          );",
        keyspace_name
    )
    .unwrap();

    write!(
        &mut create_edge_table_statement,
        "CREATE TABLE IF NOT EXISTS {}.edge (
            vertex blob,
            kind text,
            timestamp bigint,
            tx blob,
            value bigint,
            extra blob,
            PRIMARY KEY(vertex, kind, timestamp, tx)
          );",
        keyspace_name
    )
    .unwrap();

    write!(
        &mut create_data_table_statement,
        "CREATE TABLE IF NOT EXISTS {}.data (
            vertex blob,
            year smallint,
            month tinyint,
            kind text,
            timestamp bigint,
            tx blob,
            extra blob,
            PRIMARY KEY((vertex,year,month), kind, timestamp, tx)
          );",
        keyspace_name
    )
    .unwrap();

    statement_map.insert(
        "CREATE_KEYSPACE_QUERY".to_string(),
        create_key_space_statement.to_string(),
    );
    statement_map.insert(
        "CREATE_TX_TABLE_QUERY".to_string(),
        create_tx_table_statement.to_string(),
    );
    statement_map.insert(
        "CREATE_EDGE_TABLE_QUERY".to_string(),
        create_edge_table_statement.to_string(),
    );
    statement_map.insert(
        "CREATE_DATE_TABLE_QUERY".to_string(),
        create_data_table_statement.to_string(),
    );
    statement_map
}

/// Useful function to exit program using ctrl_c signal
async fn ctrl_c(mut launcher: Sender) {
    // await on ctrl_c
    tokio::signal::ctrl_c().await.unwrap();
    // exit program using launcher
    launcher.exit_program();
}

async fn import_files(dmp_files: DmpFiles) {
    let mut files: Vec<(String, u64)> = dmp_files.files.unwrap();
    let mut only_confirmed = false;
    if let Some(is_only_confirmed) = dmp_files.import_only_confirmed_transactions {
        only_confirmed = is_only_confirmed;
    }
    files.sort_by(|a, b| a.1.cmp(&b.1));
    for t in files.iter() {
        info!("starting to import: {}, milestone: {}", t.0, t.1);
        if let Ok(_) = ImporterBuilder::new()
            .filepath(t.0.clone())
            .milestone(t.1)
            .only_confirmed(only_confirmed)
            .max_retries(0)
            .build()
            .run()
            .await
        {
            info!("succesfully imported: {}", t.0);
        } else {
            panic!("failed to import file: {}", t.0);
        }
    }
}
