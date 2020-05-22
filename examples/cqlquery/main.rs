use chronicle_api::api::api::ApiBuilder;
use chronicle_common::{
    actor,
    launcher,
};
use chronicle_cql::{
    compression::compression::UNCOMPRESSED,
    frame::{
        consistency::Consistency,
        decoder::{
            Decoder,
            Frame,
        },
        header::{
            Header,
            IGNORE,
        },
        query::Query,
        queryflags::{
            SKIP_METADATA,
            VALUES,
        },
    },
};
use chronicle_storage::{
    importer::importer::InsertTransactionsFromFileBuilder,
    ring::ring::Ring,
    stage::reporter,
    storage::storage::StorageBuilder,
    worker::{
        self,
        Worker,
    },
};
use rand;
use std::{
    error::Error,
    time::Duration,
};
use tokio;

const CREATE_EXAMPLE_KEYSPACE_QUERY: &str = r#"
CREATE KEYSPACE IF NOT EXISTS chronicle_example
WITH REPLICATION = {
  'class': 'SimpleStrategy',
  'replication_factor': 1
};
"#;

const CREATE_EXAMPLE_TX_TABLE_QUERY: &str = r#"
CREATE TABLE IF NOT EXISTS chronicle_example.transaction (
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

const INSERT_EXAMPLE_TX_QUERY: &str = r#"
  INSERT INTO chronicle_example.transaction (
    hash,
    payload,
    address,
    value,
    obsolete_tag,
    timestamp,
    current_index,
    last_index,
    bundle,
    trunk,
    branch,
    tag,
    attachment_timestamp,
    attachment_timestamp_lower,
    attachment_timestamp_upper,
    nonce,
    milestone
) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);
"#;

#[derive(Debug)]
pub struct CqlQueryId(mpsc::UnboundedSender<CqlQueryEvent>);

actor!(CqlQueryBuilder { statement: String });

impl CqlQueryBuilder {
    pub fn build(self) -> CqlQuery {
        CqlQuery {
            statement: self.statement.unwrap(),
        }
    }
}

pub struct CqlQuery {
    statement: String,
}

// Note that this event name should not be Event to avoid naming collision w/ the event in launcher
pub enum CqlQueryEvent {
    Response { giveload: Vec<u8>, pid: Box<CqlQueryId> },
    Error { kind: worker::Error, pid: Box<CqlQueryId> },
}

impl Worker for CqlQueryId {
    fn send_response(self: Box<Self>, _: &Option<reporter::Sender>, giveload: Vec<u8>) {
        unsafe {
            let raw = Box::into_raw(self);
            let pid = Box::from_raw(raw);
            let event = CqlQueryEvent::Response { giveload, pid };
            let _ = (*raw).0.send(event);
        }
    }
    fn send_error(self: Box<Self>, kind: worker::Error) {
        unsafe {
            let raw = Box::into_raw(self);
            let pid = Box::from_raw(raw);
            let event = CqlQueryEvent::Error { kind, pid };
            let _ = (*raw).0.send(event);
        }
    }
}

impl CqlQuery {
    pub async fn run(self) -> Result<(), Box<dyn Error>> {
        let (tx, mut rx) = mpsc::unbounded_channel::<CqlQueryEvent>();
        let worker = Box::new(CqlQueryId(tx));
        let _ = Self::process(&self.statement, worker, &mut rx).await;
        Ok(())
    }

    async fn process(
        statement: &str,
        worker: Box<CqlQueryId>,
        rx: &mut mpsc::UnboundedReceiver<CqlQueryEvent>,
    ) -> Box<CqlQueryId> {
        let request = reporter::Event::Request {
            payload: Self::query(statement),
            worker,
        };
        Ring::send_local_random_replica(rand::random::<i64>(), request);
        match rx.recv().await.unwrap() {
            CqlQueryEvent::Response { giveload, pid } => {
                // TODO: process the responsed giveload
                let decoder = Decoder::new(giveload, UNCOMPRESSED);
                if decoder.is_void() {
                    // Nothing to do
                } else {
                    // TODO: Add retry mechanism
                }
                return pid;
            }
            CqlQueryEvent::Error { kind: _, pid } => {
                return pid;
            }
        }
    }
    fn query(statement: &str) -> Vec<u8> {
        let Query(payload) = Query::new()
            .version()
            .flags(IGNORE)
            .stream(0)
            .opcode()
            .length()
            .statement(statement)
            .consistency(Consistency::One)
            .query_flags(SKIP_METADATA)
            .build(UNCOMPRESSED);
        payload
    }
}

launcher!(
    apps_builder: AppsBuilder {storage: StorageBuilder}, // Apps
    apps: Apps{} // Launcher state
);

// build your apps
impl AppsBuilder {
    fn build(self) -> Apps {
        // - storage app:
        let storage = StorageBuilder::new()
            .listen_address("0.0.0.0:8080".to_string())
            .thread_count(8)
            .local_dc("datacenter1".to_string())
            .reporter_count(1)
            .buffer_size(1024000)
            .recv_buffer_size(1024000)
            .send_buffer_size(1024000)
            .nodes(vec!["172.17.0.2:9042".to_string()]);
        // add app to AppsBuilder then transform it to Apps
        self.storage(storage).to_apps()
    }
}
async fn delay_for_10_sec() {
    println!("Sleep Begin");
    let ten_seconds = Duration::new(10, 0);
    tokio::time::delay_for(ten_seconds).await;
    println!("Sleep End");
}

#[tokio::main(core_threads = 8)]
#[allow(unused_must_use)]
async fn main() {
    println!("Starting chronicle-example");
    // instead you can define your own .run() strategy
    AppsBuilder::new()
        .build()
        .storage()
        .await
        .function(|apps| {
            tokio::spawn(ctrl_c(apps.tx.clone()));
        })
        .await
        .future(|apps| async {
            delay_for_10_sec().await;
            CqlQueryBuilder::new()
                .statement(CREATE_EXAMPLE_KEYSPACE_QUERY.to_string())
                .build()
                .run()
                .await;
            CqlQueryBuilder::new()
                .statement(CREATE_EXAMPLE_TX_TABLE_QUERY.to_string())
                .build()
                .run()
                .await;
            InsertTransactionsFromFileBuilder::new()
                .filepath("./cqlquery/dmp/18675_head10.dmp".to_string())
                .statement(INSERT_EXAMPLE_TX_QUERY.to_string())
                .build()
                .run()
                .await;
            apps
        })
        .await
        .one_for_one()
        .await; // instead you can define your own .run() strategy
}

/// Useful function to exit program using ctrl_c signal
#[allow(dead_code)]
async fn ctrl_c(mut launcher: Sender) {
    // await on ctrl_c
    tokio::signal::ctrl_c().await.unwrap();
    // exit program using launcher
    launcher.exit_program();
}
