// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use bee_message::{
    address::Ed25519Address,
    parents::Parents,
    prelude::{
        MilestoneIndex,
        TransactionId,
    },
    Message,
    MessageBuilder,
    MessageId,
};
use bee_pow::providers::miner::Miner;
use chronicle_storage::access::{
    AddressRecord,
    Paged,
    Partitioned,
};
use core::marker::PhantomData;

use chronicle_common::config::*;
use chronicle_storage::{
    access::{
        Consistency,
        Ed25519AddressPK,
        GetDeleteRequest,
    },
    keyspaces::ChronicleKeyspace,
};

use scylla::{
    access::*,
    application::*,
};
use tokio::sync::mpsc::{
    unbounded_channel,
    UnboundedSender,
};

const CONFIG_TEST_PATH: &str = "../fixtures/config.test.ron";

// launcher
launcher!
(
    builder: AppsBuilder
    {
        [] -> Scylla<Sender>: ScyllaBuilder<Sender>
    },
    state: Apps {}
);

impl Builder for AppsBuilder {
    type State = Apps;

    fn build(self) -> Self::State {
        let config = Config::load(CONFIG_TEST_PATH.to_string()).unwrap();
        let storage_config = config.storage_config;
        let scylla_builder = ScyllaBuilder::new()
            .listen_address(storage_config.listen_address.to_string())
            .thread_count(match storage_config.thread_count {
                ThreadCount::Count(c) => c,
                ThreadCount::CoreMultiple(c) => num_cpus::get() * c,
            })
            .reporter_count(storage_config.reporter_count)
            .local_dc(storage_config.local_datacenter.clone());

        self.Scylla(scylla_builder).to_apps()
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
    fn handle_response(self: Box<Self>, _giveload: Vec<u8>) -> anyhow::Result<()> {
        self.sender.send(Ok(()))?;
        Ok(())
    }

    fn handle_error(self: Box<Self>, error: WorkerError, _reporter: &Option<ReporterHandle>) -> anyhow::Result<()> {
        self.sender.send(Err(error))?;
        Ok(())
    }
}

/// Init the scylla database
async fn init_database() {
    let storage_config = Config::load(CONFIG_TEST_PATH.to_string()).unwrap().storage_config;

    for keyspace_config in storage_config.keyspaces.first().iter() {
        let keyspace = ChronicleKeyspace::new(keyspace_config.name.clone());
        assert_eq!(keyspace.name(), "chronicle_test");
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
            .build()
            .unwrap();
        send_local(token, keyspace_statement.0, worker, keyspace.name().to_string());
        if let Some(msg) = inbox.recv().await {
            match msg {
                Ok(_) => (),
                Err(e) => panic!("Inbox recv() error: {}", e),
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
            let statement = Query::new()
                .statement(query)
                .consistency(Consistency::One)
                .build()
                .unwrap();
            send_local(token, statement.0, worker, keyspace.name().to_string());
            if let Some(msg) = inbox.recv().await {
                match msg {
                    Ok(_) => println!("Created table successfully."),
                    Err(e) => panic!("Inbox recv() error: {}", e),
                }
            } else {
                panic!("Could not verify if table was created!")
            }
        }
    }
}

pub async fn init_scylla_application() {
    // Add nodes
    let apps = AppsBuilder::new().build();

    // Create tables
    tokio::spawn(
        apps.Scylla()
            .await
            .future(|apps| async {
                let storage_config = Config::load(CONFIG_TEST_PATH.to_string()).unwrap().storage_config;
                let ws = format!("ws://{}/", storage_config.listen_address);
                add_nodes(&ws, storage_config.nodes.iter().cloned().collect(), 1)
                    .await
                    .ok();
                init_database().await;
                apps
            })
            .await
            .start(None),
    );
}

#[tokio::test]
pub async fn test_insert_select_delete() {
    // Init Scylla Application
    init_scylla_application().await;

    // Insert rows
    let keyspace = ChronicleKeyspace::new("chronicle_test".to_owned());

    let key = MessageId::new([0; 32]);
    let value = MessageBuilder::<Miner>::new()
        .with_network_id(0)
        .with_parents(Parents::new(vec![MessageId::new([1; 32]), MessageId::new([2; 32])]).unwrap())
        .finish()
        .unwrap();

    let (sender, mut inbox) = unbounded_channel::<Result<(), WorkerError>>();
    let worker = BatchWorker::boxed(sender.clone());

    let insert_req = keyspace
        .insert_query(&key, &value)
        .consistency(Consistency::One)
        .build()
        .unwrap();

    insert_req.send_local(worker);
    if let Some(msg) = inbox.recv().await {
        match msg {
            Ok(_) => println!("(MessageID, Message) Inserted"),
            Err(e) => panic!("Inbox recv() error: {}", e),
        }
    } else {
        panic!("Could not verify if keyspace was created!")
    }

    let request = keyspace
        .select::<Message>(&key)
        .consistency(Consistency::One)
        .paging_state(&None)
        .build()
        .unwrap();

    let (sender, mut inbox) = unbounded_channel::<Result<Option<Message>, WorkerError>>();
    let worker = ValueWorker::new(sender, keyspace.clone(), key, 0, PhantomData);
    let worker = Box::new(worker);

    request.send_local(worker);

    if let Some(msg) = inbox.recv().await {
        match msg {
            Ok(res) => assert_eq!(res, Some(value)),
            Err(e) => panic!("Inbox recv() worker error: {}", e),
        }
    } else {
        panic!("Could not verify if keyspace was created!")
    }

    // Insert (Partiitoned, AddressRecord) pair
    let ed_address = Ed25519Address::new([3; 32]);
    let key = Partitioned::new(ed_address, 0, 0);
    let value = AddressRecord::new(0, TransactionId::new([4; 32]), 0, 0, None);

    let (sender, mut inbox) = unbounded_channel::<Result<(), WorkerError>>();
    let worker = BatchWorker::boxed(sender.clone());
    let insert_req = keyspace
        .insert_query(&key, &value)
        .consistency(Consistency::One)
        .build()
        .unwrap();

    insert_req.send_local(worker);
    if let Some(msg) = inbox.recv().await {
        match msg {
            Ok(_) => println!("(Partitioned, AddressRecord) Inserted"),
            Err(e) => panic!("Inbox recv() error: {}", e),
        }
    } else {
        panic!("Could not verify if keyspace was created!")
    }

    // Select (Partiitoned, AddressRecord) pair
    let request = keyspace
        .select::<Paged<VecDeque<Partitioned<AddressRecord>>>>(&key)
        .consistency(Consistency::One)
        .paging_state(&None)
        .build()
        .unwrap();

    let (sender, mut inbox) =
        unbounded_channel::<Result<Option<Paged<VecDeque<Partitioned<AddressRecord>>>>, WorkerError>>();
    let worker = ValueWorker::new(sender, keyspace.clone(), key, 0, PhantomData);
    let worker = Box::new(worker);

    request.send_local(worker);

    if let Some(msg) = inbox.recv().await {
        match msg {
            Ok(res) => assert_eq!(
                (*res.unwrap().pop_front().unwrap()).transaction_id,
                value.transaction_id
            ),

            Err(e) => panic!("Inbox recv() worker error: {}", e),
        }
    } else {
        panic!("Could not verify if keyspace was created!")
    }

    // Delete (Partiitoned, AddressRecord) pair
    let key = Ed25519AddressPK::new(ed_address, 0, MilestoneIndex::new(0), 0, TransactionId::new([4; 32]), 0);

    let delete_req = keyspace
        .delete_query::<AddressRecord>(&key)
        .consistency(Consistency::One)
        .build()
        .unwrap();
    let (sender, mut inbox) = unbounded_channel::<Result<(), WorkerError>>();
    let worker = BatchWorker::boxed(sender.clone());
    delete_req.send_local(worker);
    if let Some(msg) = inbox.recv().await {
        match msg {
            Ok(_) => println!("(Ed25519AddressPK, AddressRecord) has been deleted successfully"),
            Err(e) => panic!("Inbox recv() worker error: {}", e),
        }
    } else {
        panic!("Could not verify if keyspace was created!")
    }

    // Select (Partiitoned, AddressRecord) pair again
    let key = Partitioned::new(ed_address, 0, 0);
    let request = keyspace
        .select::<Paged<VecDeque<Partitioned<AddressRecord>>>>(&key)
        .consistency(Consistency::One)
        .paging_state(&None)
        .build()
        .unwrap();

    let (sender, mut inbox) =
        unbounded_channel::<Result<Option<Paged<VecDeque<Partitioned<AddressRecord>>>>, WorkerError>>();
    let worker = ValueWorker::new(sender, keyspace.clone(), key, 0, PhantomData);
    let worker = Box::new(worker);

    request.send_local(worker);

    if let Some(msg) = inbox.recv().await {
        match msg {
            Ok(res) => assert_eq!((*res.unwrap()).pop_front().is_none(), true),
            Err(e) => panic!("Inbox recv() worker error: {}", e),
        }
    } else {
        panic!("Could not verify if keyspace was created!")
    }
}
