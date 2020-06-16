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

launcher!(
    apps_builder: AppsBuilder {storage: StorageBuilder, api: ApiBuilder, broker: BrokerBuilder}, // Apps
    apps: Apps{} // Launcher state
);

// build your apps
impl AppsBuilder {
    fn build(self) -> Apps {
        // 
        // - storage app:
        let storage = StorageBuilder::new()
            .listen_address("0.0.0.0:8080".to_string())
            .thread_count(8)
            .local_dc("datacenter1".to_string())
            .reporter_count(1)
            .buffer_size(1024000)
            .recv_buffer_size(1024000)
            .send_buffer_size(1024000);
        // 
        // - api app
        let api = ApiBuilder::new().listen_address("0.0.0.0:4000".to_string());
        // 
        // - broker app
        let broker = BrokerBuilder::new()
            .trytes(vec!["tcp://zmq.iota.org:5556".to_owned()])
            .sn_trytes(vec!["tcp://zmq.iota.org:5556".to_owned()]);
        // add app to AppsBuilder then transform it to Apps
        self.storage(storage).api(api).broker(broker).to_apps()
    }
}

#[tokio::main(core_threads = 8)]
async fn main() {
    println!("Starting broker example");
    AppsBuilder::new()
        .build() // build apps first, then start them in order you want.
        .function(|apps| {
            // for instance this is helpful to spawn ctrl_c future
            tokio::spawn(ctrl_c(apps.tx.clone()));
        })
        .await // you can start some function(it must never block)
        .storage()
        .await // start storage app
        .api()
        .await // start api app
        .future(|apps| async {
            // add nodes and initialize ring
            add_nodes(
                "ws://0.0.0.0:8080/",
                vec!["172.17.0.2:9042".to_string()],
                1, // the least replication_factor in all data_centers .
            )
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
            apps
        })
        .await
        .broker()
        .await // start broker app
        .one_for_one()
        .await; // instead you can define your own .run() strategy
}

/// Useful function to exit program using ctrl_c signal
async fn ctrl_c(mut launcher: Sender) {
    // await on ctrl_c
    tokio::signal::ctrl_c().await.unwrap();
    // exit program using launcher
    launcher.exit_program();
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
