use std::{collections::HashMap, collections::HashSet, rc::Rc};

/// A transaction hash. To be replaced later with whatever implementation is required.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
pub struct TxHash(u64);
impl TxHash {
    pub fn is_genesis(&self) -> bool {
        todo!()
    }
}

/// A transaction address. To be replaced later with whatever implementation is required.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
pub struct TxAddress(u64);

/// [https://github.com/iotaledger/bee-rfcs/pull/20]
pub struct Tx {
    hash: TxHash,
    trunk: TxHash,
    branch: TxHash,
    body: (),
}

/// A milestone hash. To be replaced later with whatever implementation is required.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
pub struct MilestoneHash(u64);
impl MilestoneHash {
    //Returns true if the milestone is the genesis of the Tangle
    pub fn is_genesis(&self) -> bool {
        todo!()
    }
}

pub struct Milestone {
    hash: MilestoneHash,
    index: u32,
}

pub enum StorageError {
    TransactionAlreadyExist,
    TransactionNotFound,
    UnknownError,
    //...
}

pub enum ConnectionError {
    InvalidUrl,
    UnknownError,
    //...
}

type HashesToApprovers = HashMap<TxHash, Vec<TxHash>>;
type MissingHashesToRCApprovers = HashMap<TxHash, Vec<Rc<TxHash>>>;
//This is a mapping between an iota address and it's balance change
//practically, a map for total balance change over an addresses will be collected
//per milestone (snapshot_index), when we no longer have milestones, we will have to find
//another way to decide on a check point where to store an address's delta if we want to snapshot
type StateDeltaMap = HashMap<TxAddress, i64>;

use async_trait::async_trait;

#[async_trait]
pub trait Connection<Conn> {
    async fn establish_connection(url: &str) -> Result<Conn, ConnectionError>;
    async fn destroy_connection(connection: Conn) -> Result<(), ConnectionError>;
}


#[async_trait]
pub trait StorageBackend {
    //**Operations over transaction's schema**//
    async fn insert_transaction(&self, tx: &Tx) -> Result<(), StorageError>;
    async fn find_transaction(&self, tx_hash: TxHash) -> Result<Tx, StorageError>;
    async fn update_transactions_set_solid(
        &self,
        transaction_hashes: HashSet<TxHash>,
    ) -> Result<(), StorageError>;
    async fn update_transactions_set_snapshot_index(
        &self,
        transaction_hashes: HashSet<TxHash>,
        snapshot_index: u32,
    ) -> Result<(), StorageError>;
    async fn delete_transactions(&self, transaction_hashes: HashSet<TxHash>) -> Result<(), StorageError>;

    //This method is heavy weighted and will be used to populate Tangle struct on initialization
    fn map_existing_transaction_hashes_to_approvers(
        &self,
    ) -> Result<HashesToApprovers, StorageError>;
    //This method is heavy weighted and will be used to populate Tangle struct on initialization
    fn map_missing_transaction_hashes_to_approvers(
        &self,
    ) -> Result<MissingHashesToRCApprovers, StorageError>;

    //**Operations over milestone's schema**//
    async fn insert_milestone(&self, milestone: &Milestone) -> Result<(), StorageError>;
    async fn find_milestone(&self, milestone_hash: MilestoneHash) -> Result<Milestone, StorageError>;
    async fn delete_milestones(
        &self,
        milestone_hashes: HashSet<MilestoneHash>,
    ) -> Result<(), StorageError>;

    //**Operations over state_delta's schema**//
    async fn insert_state_delta(
        &self,
        state_delta: StateDeltaMap,
        index: u32,
    ) -> Result<(), StorageError>;

    async fn load_state_delta(&self, index: u32) -> Result<StateDeltaMap, StorageError>;
}

pub struct Storage<Conn: Connection<Conn>> {
    pub connection:   Conn,
}

/*
impl Storage<DummyConnection> {
    async fn establish_connection(&mut self, url: &str) -> Result<(), ConnectionError> {
        self.connection = DummyConnection::establish_connection(url)?;
        Ok(())
    }
    async fn destroy_connection(connection: DummyConnection) -> Result<(), ConnectionError> {
        DummyConnection::destroy_connection(connection);
        Ok(())
    }
}

pub struct DummyConnection {}

impl DummyConnection {
    fn new() -> Self {
        Self {}
    }
}

impl Connection<DummyConnection> for DummyConnection {
    async fn establish_connection(url: &str) -> Result<DummyConnection, ConnectionError> {
        Ok(DummyConnection::new())
    }
    async fn destroy_connection(connection: DummyConnection) -> Result<(), ConnectionError> {
        Ok(())
    }
}

type DummyStorage = Storage<DummyConnection>;
*/

//impl StorageBackend for DummyStorage {
    //Implement all methods here
//}