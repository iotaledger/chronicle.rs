// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use bee_message::{
    milestone::Milestone,
    prelude::MilestonePayload,
};
use bee_test::rand::{
    bytes::{
        rand_bytes,
        rand_bytes_32,
    },
    parents::rand_parents,
};
use chronicle_common::{
    SyncRange,
    Synckey,
};
use chronicle_storage::access::SyncRecord;
// use bee_message::prelude::MilestonePayload;
use bee_message::{
    prelude::{
        Input,
        MilestoneIndex,
        MilestonePayloadEssence,
        Output,
        OutputId,
        TreasuryInput,
    },
    Message,
    MessageId,
};
use bee_test::rand::{
    address::rand_address,
    milestone::{
        rand_milestone,
        rand_milestone_id,
    },
    output::{
        rand_ledger_treasury_output,
        rand_output_id,
        rand_signature_locked_single_output,
    },
};
use chronicle_common::config::*;
use chronicle_storage::{
    access::{
        AddressRecord,
        Ed25519AddressPK,
        Hint,
        HintVariant,
        Indexation,
        IndexationRecord,
        InputData,
        MessageMetadata,
        OutputRes,
        Paged,
        ParentRecord,
        Partition,
        PartitionId,
        Partitioned,
        TransactionData,
        TransactionRecord,
        TransactionVariant,
    },
    keyspaces::ChronicleKeyspace,
};
use core::marker::PhantomData;

use bee_test::rand::{
    address::rand_ed25519_address,
    message::{
        rand_message,
        rand_message_id,
        rand_message_ids,
    },
    metadata::rand_message_metadata,
    milestone::rand_milestone_index,
    transaction::rand_transaction_id,
};
use rand::{
    thread_rng,
    Rng,
};
use scylla_rs::prelude::*;
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

async fn init_scylla_application() {
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_operations() {
    // Init Scylla Application
    init_scylla_application().await;

    insert_select_message_id_and_message().await;
    insert_select_message_id_and_message_metadata().await;
    insert_select_message_id_and_messsage_message_metadata().await;
    insert_select_ed25519_address_and_address_record().await;
    insert_select_indexation_and_indexation_record().await;
    insert_select_message_id_and_parent_record().await;

    // Error to fix!
    // insert_select_transaction_id_index_and_transaction_record().await;
    // Error to fix!
    // insert_select_output_id_and_transaction_record().await;
    insert_select_hint_and_partition().await;
    insert_milestone_index_and_message_id_milestone_payload().await;
    insert_sync_key_and_sync_record().await;
}

async fn insert_select_message_id_and_message() {
    // Insert row
    let keyspace = ChronicleKeyspace::new("chronicle_test".to_owned());
    let key = rand_message_id();
    let value = rand_message();
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

    // Select row
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
}

async fn insert_select_message_id_and_message_metadata() {
    // Insert row
    let keyspace = ChronicleKeyspace::new("chronicle_test".to_owned());
    let key = rand_message_id();
    let value = MessageMetadata {
        message_id: rand_message_id(),
        parent_message_ids: rand_message_ids(2),
        is_solid: false,
        referenced_by_milestone_index: None,
        ledger_inclusion_state: None,
        should_promote: None,
        should_reattach: None,
    };
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
            Ok(_) => println!("(MessageID, MessageMetadata) Inserted"),
            Err(e) => panic!("Inbox recv() error: {}", e),
        }
    } else {
        panic!("Could not verify if keyspace was created!")
    }

    // Select row
    let request = keyspace
        .select::<MessageMetadata>(&key)
        .consistency(Consistency::One)
        .paging_state(&None)
        .build()
        .unwrap();
    let (sender, mut inbox) = unbounded_channel::<Result<Option<MessageMetadata>, WorkerError>>();
    let worker = ValueWorker::new(sender, keyspace.clone(), key, 0, PhantomData);
    let worker = Box::new(worker);
    request.send_local(worker);
    if let Some(msg) = inbox.recv().await {
        match msg {
            Ok(res) => {
                assert_eq!(res.clone().unwrap().message_id, value.message_id);
                assert_eq!(res.clone().unwrap().parent_message_ids, value.parent_message_ids);
                assert_eq!(res.clone().unwrap().is_solid, value.is_solid);
                assert_eq!(
                    res.clone().unwrap().referenced_by_milestone_index,
                    value.referenced_by_milestone_index
                );
                assert_eq!(
                    res.clone().unwrap().ledger_inclusion_state,
                    value.ledger_inclusion_state
                );
                assert_eq!(res.clone().unwrap().should_promote, value.should_promote);
                assert_eq!(res.unwrap().should_reattach, value.should_reattach);
            }
            Err(e) => panic!("Inbox recv() worker error: {}", e),
        }
    } else {
        panic!("Could not verify if keyspace was created!")
    }
}

async fn insert_select_message_id_and_messsage_message_metadata() {
    // Insert row
    let keyspace = ChronicleKeyspace::new("chronicle_test".to_owned());
    let key = rand_message_id();
    let value = (
        rand_message(),
        MessageMetadata {
            message_id: rand_message_id(),
            parent_message_ids: rand_message_ids(2),
            is_solid: false,
            referenced_by_milestone_index: None,
            ledger_inclusion_state: None,
            should_promote: None,
            should_reattach: None,
        },
    );
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
            Ok(_) => println!("(MessageID, (Message, MessageMetadata)) Inserted"),
            Err(e) => panic!("Inbox recv() error: {}", e),
        }
    } else {
        panic!("Could not verify if keyspace was created!")
    }

    // Select row
    let request = keyspace
        .select::<(Option<Message>, Option<MessageMetadata>)>(&key)
        .consistency(Consistency::One)
        .paging_state(&None)
        .build()
        .unwrap();
    let (sender, mut inbox) =
        unbounded_channel::<Result<Option<(Option<Message>, Option<MessageMetadata>)>, WorkerError>>();
    let worker = ValueWorker::new(sender, keyspace.clone(), key, 0, PhantomData);
    let worker = Box::new(worker);
    request.send_local(worker);
    if let Some(msg) = inbox.recv().await {
        match msg {
            Ok(res) => {
                assert_eq!(res.clone().unwrap().0.unwrap(), value.0);
                assert_eq!(res.clone().unwrap().1.unwrap().message_id, value.1.message_id);
                assert_eq!(
                    res.clone().unwrap().1.unwrap().parent_message_ids,
                    value.1.parent_message_ids
                );
                assert_eq!(res.clone().unwrap().1.unwrap().is_solid, value.1.is_solid);
                assert_eq!(
                    res.clone().unwrap().1.unwrap().referenced_by_milestone_index,
                    value.1.referenced_by_milestone_index
                );
                assert_eq!(
                    res.clone().unwrap().1.unwrap().ledger_inclusion_state,
                    value.1.ledger_inclusion_state
                );
                assert_eq!(res.clone().unwrap().1.unwrap().should_promote, value.1.should_promote);
                assert_eq!(res.unwrap().1.unwrap().should_reattach, value.1.should_reattach);
            }
            Err(e) => panic!("Inbox recv() worker error: {}", e),
        }
    } else {
        panic!("Could not verify if keyspace was created!")
    }
}

async fn insert_select_ed25519_address_and_address_record() {
    // Insert row
    let keyspace = ChronicleKeyspace::new("chronicle_test".to_owned());
    let ed_address = rand_ed25519_address();
    let milestone_index = rand_milestone_index();
    let key = Partitioned::new(ed_address, 0, milestone_index.0);
    let transaction_id = rand_transaction_id();
    let value = AddressRecord::new(0, transaction_id, 0, 0, None);
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
            Ok(_) => println!("(Partitioned<Ed25519Address>, AddressRecord) Inserted"),
            Err(e) => panic!("Inbox recv() error: {}", e),
        }
    } else {
        panic!("Could not verify if keyspace was created!")
    }

    // Select row
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
            Ok(res) => {
                assert_eq!(
                    (*res.clone().unwrap().pop_front().unwrap()).output_type,
                    value.output_type
                );
                assert_eq!(
                    (*res.clone().unwrap().pop_front().unwrap()).transaction_id,
                    value.transaction_id
                );
                assert_eq!((*res.clone().unwrap().pop_front().unwrap()).index, value.index);
                assert_eq!((*res.clone().unwrap().pop_front().unwrap()).amount, value.amount);
                assert_eq!(
                    (*res.unwrap().pop_front().unwrap()).ledger_inclusion_state,
                    value.ledger_inclusion_state
                );
            }

            Err(e) => panic!("Inbox recv() worker error: {}", e),
        }
    } else {
        panic!("Could not verify if keyspace was created!")
    }

    // Delete row
}

pub async fn insert_select_indexation_and_indexation_record() {
    // Insert row
    let keyspace = ChronicleKeyspace::new("chronicle_test".to_owned());
    let indexation = Indexation("indexation_test".to_string());
    let milestone_index = rand_milestone_index();
    let key = Partitioned::new(indexation, 0, milestone_index.0);
    let value = IndexationRecord::new(rand_message_id(), None);
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
            Ok(_) => println!("(Partitioned<Indexation>, IndexationRecord) Inserted"),
            Err(e) => panic!("Inbox recv() error: {}", e),
        }
    } else {
        panic!("Could not verify if keyspace was created!")
    }

    // Select row
    let request = keyspace
        .select::<Paged<VecDeque<Partitioned<IndexationRecord>>>>(&key)
        .consistency(Consistency::One)
        .paging_state(&None)
        .build()
        .unwrap();
    let (sender, mut inbox) =
        unbounded_channel::<Result<Option<Paged<VecDeque<Partitioned<IndexationRecord>>>>, WorkerError>>();
    let worker = ValueWorker::new(sender, keyspace.clone(), key, 0, PhantomData);
    let worker = Box::new(worker);
    request.send_local(worker);
    if let Some(msg) = inbox.recv().await {
        match msg {
            Ok(res) => {
                assert_eq!(
                    (*res.clone().unwrap().pop_front().unwrap()).message_id,
                    value.message_id
                );
                assert_eq!(
                    (*res.clone().unwrap().pop_front().unwrap()).ledger_inclusion_state,
                    value.ledger_inclusion_state
                );
            }
            Err(e) => panic!("Inbox recv() worker error: {}", e),
        }
    } else {
        panic!("Could not verify if keyspace was created!")
    }
}

pub async fn insert_select_message_id_and_parent_record() {
    // Insert row
    let keyspace = ChronicleKeyspace::new("chronicle_test".to_owned());
    let message_id = rand_message_id();
    let milestone_index = rand_milestone_index();
    let key = Partitioned::new(message_id, 0, milestone_index.0);
    let value = ParentRecord::new(rand_message_id(), None);
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
            Ok(_) => println!("(Partitioned<MessageId>, ParentRecord) Inserted"),
            Err(e) => panic!("Inbox recv() error: {}", e),
        }
    } else {
        panic!("Could not verify if keyspace was created!")
    }

    // Select row
    let request = keyspace
        .select::<Paged<VecDeque<Partitioned<ParentRecord>>>>(&key)
        .consistency(Consistency::One)
        .paging_state(&None)
        .build()
        .unwrap();
    let (sender, mut inbox) =
        unbounded_channel::<Result<Option<Paged<VecDeque<Partitioned<ParentRecord>>>>, WorkerError>>();
    let worker = ValueWorker::new(sender, keyspace.clone(), key, 0, PhantomData);
    let worker = Box::new(worker);
    request.send_local(worker);
    if let Some(msg) = inbox.recv().await {
        match msg {
            Ok(res) => {
                assert_eq!(
                    (*res.clone().unwrap().pop_front().unwrap()).message_id,
                    value.message_id
                );
                assert_eq!(
                    (*res.clone().unwrap().pop_front().unwrap()).ledger_inclusion_state,
                    value.ledger_inclusion_state
                );
            }
            Err(e) => panic!("Inbox recv() worker error: {}", e),
        }
    } else {
        panic!("Could not verify if keyspace was created!")
    }
}

pub async fn insert_select_transaction_id_index_and_transaction_record() {
    let mut rng = rand::thread_rng();

    // Insert row
    let keyspace = ChronicleKeyspace::new("chronicle_test".to_owned());
    let message_id = rand_message_id();
    let key = (rand_transaction_id(), rng.gen());
    let value = TransactionRecord {
        variant: TransactionVariant::Output,
        message_id: message_id,
        data: TransactionData::Output(Output::SignatureLockedSingle(rand_signature_locked_single_output())),
        inclusion_state: None,
        milestone_index: None,
    };
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
            Ok(_) => println!("((TransactionId, Index), TransactionRecord)"),
            Err(e) => panic!("Inbox recv() error: {}", e),
        }
    } else {
        panic!("Could not verify if keyspace was created!")
    }

    // Select row
    let request = keyspace
        .select::<MessageId>(&key.0)
        .consistency(Consistency::One)
        .paging_state(&None)
        .build()
        .unwrap();
    let (sender, mut inbox) = unbounded_channel::<Result<Option<MessageId>, WorkerError>>();
    let worker = ValueWorker::new(sender, keyspace.clone(), key.0, 0, PhantomData);
    let worker = Box::new(worker);
    request.send_local(worker);
    if let Some(msg) = inbox.recv().await {
        match msg {
            Ok(res) => {
                assert_eq!(res.unwrap(), message_id);
            }
            Err(e) => panic!("Inbox recv() worker error: {}", e),
        }
    } else {
        panic!("Could not verify if keyspace was created!")
    }
}

pub async fn insert_select_output_id_and_transaction_record() {
    // Insert row
    let keyspace = ChronicleKeyspace::new("chronicle_test".to_owned());
    let message_id = rand_message_id();
    let key = rand_output_id();
    let value = TransactionRecord {
        variant: TransactionVariant::Output,
        message_id: message_id,
        data: TransactionData::Output(Output::SignatureLockedSingle(rand_signature_locked_single_output())),
        inclusion_state: None,
        milestone_index: Some(rand_milestone_index()),
    };
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
            Ok(_) => println!("(OutputId, TransactionRecord) Inserted"),
            Err(e) => panic!("Inbox recv() error: {}", e),
        }
    } else {
        panic!("Could not verify if keyspace was created!")
    }

    // Select row
    let request = keyspace
        .select::<OutputRes>(&key)
        .consistency(Consistency::One)
        .paging_state(&None)
        .build()
        .unwrap();
    let (sender, mut inbox) = unbounded_channel::<Result<Option<OutputRes>, WorkerError>>();
    let worker = ValueWorker::new(sender, keyspace.clone(), key, 0, PhantomData);
    let worker = Box::new(worker);
    request.send_local(worker);
    if let Some(msg) = inbox.recv().await {
        match msg {
            Ok(res) => {
                assert_eq!(res.unwrap().message_id, message_id);
            }
            Err(e) => panic!("Inbox recv() worker error: {}", e),
        }
    } else {
        panic!("Could not verify if keyspace was created!")
    }
}

pub async fn insert_select_hint_and_partition() {
    let mut rng = rand::thread_rng();

    // Insert row
    let keyspace = ChronicleKeyspace::new("chronicle_test".to_owned());
    let key = Hint {
        hint: rand_address().to_bech32("atoi"),
        variant: HintVariant::Address,
    };
    let partition_id = rng.gen();
    let milestone_index = rand_milestone_index();
    let value = Partition::new(partition_id, *milestone_index);
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
            Ok(_) => println!("(Hint, Partition) Inserted"),
            Err(e) => panic!("Inbox recv() error: {}", e),
        }
    } else {
        panic!("Could not verify if keyspace was created!")
    }

    // Select row
    let request = keyspace
        .select::<Vec<(MilestoneIndex, PartitionId)>>(&key)
        .consistency(Consistency::One)
        .paging_state(&None)
        .build()
        .unwrap();
    let (sender, mut inbox) = unbounded_channel::<Result<Option<Vec<(MilestoneIndex, PartitionId)>>, WorkerError>>();
    let worker = ValueWorker::new(sender, keyspace.clone(), key, 0, PhantomData);
    let worker = Box::new(worker);
    request.send_local(worker);
    if let Some(msg) = inbox.recv().await {
        match msg {
            Ok(res) => {
                let value = res.unwrap().pop().unwrap();
                assert_eq!(value.0, milestone_index);
                assert_eq!(value.1, partition_id);
            }
            Err(e) => panic!("Inbox recv() worker error: {}", e),
        }
    } else {
        panic!("Could not verify if keyspace was created!")
    }
}

pub async fn insert_milestone_index_and_message_id_milestone_payload() {
    // Insert row
    let keyspace = ChronicleKeyspace::new("chronicle_test".to_owned());
    let key = rand_milestone_index();
    let message_id = rand_message_id();
    let timestamp = 0;
    let milestone_payload = MilestonePayload::new(
        MilestonePayloadEssence::new(
            MilestoneIndex(0),
            timestamp,
            rand_parents(),
            [0; 32],
            0,
            0,
            vec![[0; 32]],
            None,
        )
        .unwrap(),
        vec![[0; 64]],
    )
    .unwrap();

    let value = (message_id, Box::new(milestone_payload));
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
            Ok(_) => println!("(MilestoneIndex, (MessageId, Box<MilestonePayload>)) Inserted"),
            Err(e) => panic!("Inbox recv() error: {}", e),
        }
    } else {
        panic!("Could not verify if keyspace was created!")
    }

    // Select row
    let request = keyspace
        .select::<Milestone>(&key)
        .consistency(Consistency::One)
        .paging_state(&None)
        .build()
        .unwrap();
    let (sender, mut inbox) = unbounded_channel::<Result<Option<Milestone>, WorkerError>>();
    let worker = ValueWorker::new(sender, keyspace.clone(), key, 0, PhantomData);
    let worker = Box::new(worker);
    request.send_local(worker);
    if let Some(msg) = inbox.recv().await {
        match msg {
            Ok(res) => {
                let value = res.unwrap();
                assert_eq!(value.message_id(), &message_id);
                assert_eq!(value.timestamp(), timestamp);
            }
            Err(e) => panic!("Inbox recv() worker error: {}", e),
        }
    } else {
        panic!("Could not verify if keyspace was created!")
    }
}

pub async fn insert_sync_key_and_sync_record() {
    // Insert row
    let keyspace = ChronicleKeyspace::new("chronicle_test".to_owned());
    let milestone_index = rand_milestone_index();
    let key = SyncRange {
        from: *milestone_index,
        to: *milestone_index + 1,
    };
    let value = SyncRecord::new(milestone_index, None, None);
    let (sender, mut inbox) = unbounded_channel::<Result<(), WorkerError>>();
    let worker = BatchWorker::boxed(sender.clone());
    let insert_req = keyspace
        .insert_query(&Synckey, &value)
        .consistency(Consistency::One)
        .build()
        .unwrap();
    insert_req.send_local(worker);
    if let Some(msg) = inbox.recv().await {
        match msg {
            Ok(_) => println!("(Synckey, SyncRecord) Inserted"),
            Err(e) => panic!("Inbox recv() error: {}", e),
        }
    } else {
        panic!("Could not verify if keyspace was created!")
    }

    // Select row
    let request = keyspace
        .select::<Iter<SyncRecord>>(&key)
        .consistency(Consistency::One)
        .paging_state(&None)
        .build()
        .unwrap();
    let (sender, mut inbox) = unbounded_channel::<Result<Option<Iter<SyncRecord>>, WorkerError>>();
    let worker = ValueWorker::new(sender, keyspace.clone(), key, 0, PhantomData);
    let worker = Box::new(worker);
    request.send_local(worker);
    if let Some(msg) = inbox.recv().await {
        match msg {
            Ok(res) => {
                let value = res.unwrap().next().unwrap();
                assert_eq!(value.milestone_index, milestone_index);
                assert_eq!(value.synced_by, None);
                assert_eq!(value.logged_by, None);
            }
            Err(e) => panic!("Inbox recv() worker error: {}", e),
        }
    } else {
        panic!("Could not verify if keyspace was created!")
    }
}
