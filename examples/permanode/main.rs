// import the apps you want to build
use chronicle_api::api::ApiBuilder;
use chronicle_broker::broker::BrokerBuilder;
use chronicle_storage::storage::StorageBuilder;
// import launcher macro and logger,
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
    conf_trytes_nodes: Option<Vec<String>>,
    stream_capacity: usize,
    max_retries: usize,
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
        let api = ApiBuilder::new()
            .listen_address(config.api.endpoint.clone())
            .content_length(config.api.content_length);
        // 
        // - broker app
        let mut broker = BrokerBuilder::new()
            .max_retries(config.broker.max_retries)
            .stream_capacity(config.broker.stream_capacity);
        if let Some(trytes_nodes) = config.broker.trytes_nodes.as_ref() {
            broker = broker.trytes(trytes_nodes.to_vec());
        }
        if let Some(conf_trytes_nodes) = config.broker.conf_trytes_nodes.as_ref() {
            broker = broker.conf_trytes(conf_trytes_nodes.to_vec());
        }
        // add app to AppsBuilder then transform it to Apps
        self.storage(storage).api(api).broker(broker).to_apps().config(config)
    }
}

fn main() {
    let args = Args::from_args();
    let config_as_string = fs::read_to_string(args.path).unwrap();
    let config: Config = toml::from_str(&config_as_string).unwrap();
    logger_init(config.logger.clone().finish()).unwrap();
    // build tokio runtime
    let mut runtime = Builder::new()
        .threaded_scheduler()
        .core_threads(config.tokio.core_threads)
        .enable_io()
        .enable_time()
        .thread_name("chronicle")
        .thread_stack_size(3 * 1024 * 1024)
        .build()
        .unwrap();
    info!("Welcome to Chronicle Permanode Alpha v0.2.1");
    let apps = AppsBuilder::new().build(config);
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
                    .max_retries(50)
                    .build()
                    .run()
                    .await
                    .expect("failed to create keyspace");
                // create transaction table
                SchemaCqlBuilder::new()
                    .statement(statement_map["CREATE_TX_TABLE_QUERY"].clone())
                    .max_retries(50)
                    .build()
                    .run()
                    .await
                    .expect("failed to create transaction table");
                // create index on milestone column, this is useful to lookup by milestone index
                SchemaCqlBuilder::new()
                    .statement(statement_map["CREATE_INDEX_ON_TX_TABLE_QUERY"].clone())
                    .max_retries(50)
                    .build()
                    .run()
                    .await
                    .expect("failed to create index on transaction table for milestone column");
                // create edge table
                SchemaCqlBuilder::new()
                    .statement(statement_map["CREATE_HINT_TABLE_QUERY"].clone())
                    .max_retries(50)
                    .build()
                    .run()
                    .await
                    .expect("failed to create edge table");
                // create data table
                SchemaCqlBuilder::new()
                    .statement(statement_map["CREATE_DATE_TABLE_QUERY"].clone())
                    .max_retries(50)
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
    let keyspace_name;
    if cfg!(feature = "devnet") {
        keyspace_name = "devnet"
    } else if cfg!(feature = "comnet") {
        keyspace_name = "comnet"
    } else {
        keyspace_name = "mainnet"
    }
    let mut statement_map: HashMap<String, String> = HashMap::new();
    let mut create_tx_table_statement = String::new();
    let mut create_index_on_tx_table_milestone_col_statement = String::new();
    let mut create_hint_table_statement = String::new();
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
            hash varchar,
            payload varchar,
            address varchar,
            value varchar,
            obsolete_tag varchar,
            timestamp varchar,
            current_index varchar,
            last_index varchar,
            bundle varchar,
            trunk varchar,
            branch varchar,
            tag varchar,
            attachment_timestamp varchar,
            attachment_timestamp_lower varchar,
            attachment_timestamp_upper varchar,
            nonce varchar,
            milestone bigint,
            PRIMARY KEY(hash, payload, address, value, obsolete_tag, timestamp, current_index, last_index, bundle, trunk, branch, tag, attachment_timestamp,attachment_timestamp_lower,attachment_timestamp_upper, nonce)
          );",
        keyspace_name
    )
    .unwrap();

    // create index on milestone column
    write!(
        &mut create_index_on_tx_table_milestone_col_statement,
        "CREATE INDEX IF NOT EXISTS ON {}.transaction (milestone);",
        keyspace_name
    )
    .unwrap();

    write!(
        &mut create_hint_table_statement,
        "CREATE TABLE IF NOT EXISTS {}.hint (
            vertex varchar,
            kind varchar,
            year smallint,
            month tinyint,
            milestone bigint,
            PRIMARY KEY(vertex, kind, year, month)
        ) WITH CLUSTERING ORDER BY (kind DESC, year DESC, month DESC);",
        keyspace_name
    )
    .unwrap();

    write!(
        &mut create_data_table_statement,
        "CREATE TABLE IF NOT EXISTS {}.data (
            vertex varchar,
            year smallint,
            month tinyint,
            kind varchar,
            timestamp bigint,
            tx varchar,
            value bigint,
            milestone bigint,
            PRIMARY KEY((vertex,year,month), kind, timestamp, tx, value)
        ) WITH CLUSTERING ORDER BY (kind DESC, timestamp DESC);",
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
        "CREATE_INDEX_ON_TX_TABLE_QUERY".to_string(),
        create_index_on_tx_table_milestone_col_statement.to_string(),
    );
    statement_map.insert(
        "CREATE_HINT_TABLE_QUERY".to_string(),
        create_hint_table_statement.to_string(),
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
    let mut max_retries = 1000;
    if let Some(max) = dmp_files.max_retries {
        max_retries = max;
    }
    files.sort_by(|a, b| a.1.cmp(&b.1));
    for t in files.iter() {
        info!("starting to import: {}, milestone: {}", t.0, t.1);
        if let Ok(_) = ImporterBuilder::new()
            .filepath(t.0.clone())
            .milestone(t.1)
            .only_confirmed(only_confirmed)
            .max_retries(max_retries)
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
