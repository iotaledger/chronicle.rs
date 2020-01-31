use std::str::from_utf8;
use async_trait::async_trait;
use bundle::{
    Hash,
    Transaction,
    TransactionBuilder,
    Payload, Address, Tag, Index, Value, Nonce, Timestamp,
};
use cdrs::{
    authenticators::NoneAuthenticator,
    cluster::session::{new as new_session, Session},
    cluster::{ClusterTcpConfig, NodeTcpConfigBuilder, TcpConnectionPool},
    load_balancing::RoundRobinSync,
    query::QueryExecutor,
    query_values,
    types::{blob::Blob, rows::Row, from_cdrs::FromCDRSByName},
    frame::traits::TryFromRow,
    Error as CDRSError,
};

use crate::{
    statements::*,
};

#[async_trait]
/// Methods for initilize and destroy database connection session. User should know they type of which database session is used.
pub trait Connection {
    type Session;
    type StorageError;

    async fn establish_connection(url: &str) -> Result<Self::Session, Self::StorageError>;
    async fn destroy_connection(connection: Self::Session) -> Result<(), Self::StorageError>;
}

#[async_trait]
/// Methods of database query collections.
pub trait StorageBackend {
    type StorageError;

    async fn insert_transaction(&self, tx_hash: &Hash, tx: &Transaction) -> Result<(), Self::StorageError>;
    async fn find_transaction(&self, tx_hash: &Hash) -> Result<Transaction, Self::StorageError>;
    // TODO: find transactions by bundle/address/tag/approvee
}

/// Session works for any CQL database like Cassandra and ScyllaDB.
pub struct CQLSession(Session<RoundRobinSync<TcpConnectionPool<NoneAuthenticator>>>);

#[derive(Debug, TryFromRow)]
struct CQLTx {
    hash: Blob,
    payload: Blob,
    address: Blob,
    value: i32,
    obsolete_tag: Blob,
    timestamp: i32,
    current_index: i16,
    last_index: i16,
    bundle: Blob,
    trunk: Blob,
    branch: Blob,
    tag: Blob,
    attachment_timestamp: i32,
    attachment_timestamp_lower: i32,
    attachment_timestamp_upper: i32,
    nonce: Blob
}

// TODO: Error handling
#[async_trait]
impl Connection for CQLSession {
    type Session = CQLSession;
    type StorageError = CDRSError;

    async fn establish_connection(url: &str) -> Result<CQLSession, CDRSError> {
        let node = NodeTcpConfigBuilder::new(url, NoneAuthenticator {}).build();
        let cluster = ClusterTcpConfig(vec![node]);
        let balance = RoundRobinSync::new();
        let conn = CQLSession(new_session(&cluster, balance).expect("session should be created"));
        conn.create_keyspace()?;
        conn.create_table()?;

        Ok(conn)
    }

    async fn destroy_connection(_connection: CQLSession) -> Result<(), CDRSError> {
        Ok(())
    }
}

impl CQLSession {
    fn create_keyspace(&self) -> Result<(), CDRSError> {
        self.0.query(CREATE_KEYSPACE_QUERY)?;
        Ok(())
    }

    fn create_table(&self) -> Result<(), CDRSError> {
        self.0.query(CREATE_TX_TABLE_QUERY)?;
        self.0.query(CREATE_EDGE_TABLE_QUERY)?;
        Ok(())
    }
}

#[async_trait]
impl StorageBackend for CQLSession {
    type StorageError = CDRSError;

    async fn insert_transaction(&self, tx_hash: &Hash, tx: &Transaction) -> Result<(), CDRSError> {
        let values = query_values!(
            "hash" => tx_hash.to_string(),
            "payload" => tx.payload().to_string(),
            "address" => tx.address().to_string(),
            "value" => tx.value().0 as i32,
            "obsolete_tag" => tx.obsolete_tag().to_string(),
            "timestamp" => tx.timestamp().0 as i32,
            "current_index" => tx.index().0 as i16,
            "last_index" => tx.last_index().0 as i16,
            "bundle" => tx.bundle().to_string(),
            "trunk" => tx.trunk().to_string(),
            "branch" => tx.branch().to_string(),
            "tag" => tx.tag().to_string(),
            "attachment_timestamp" => tx.attachment_ts().0 as i32,
            "attachment_timestamp_lower" => tx.attachment_lbts().0 as i32,
            "attachment_timestamp_upper" => tx.attachment_ubts().0 as i32,
            "nonce" => tx.nonce().to_string()
        );
        self.0.query_with_values(INSERT_TX_QUERY, values)?;
        //TODO: Also insert to edge table (bundle, address, tag, approvee)

        Ok(())
    }

    async fn find_transaction(&self, tx_hash: &Hash) -> Result<Transaction, CDRSError> {
        let mut builder = TransactionBuilder::new();
        if let Some(rows) = self.0.query_with_values(SELECT_TX_QUERY, query_values!(tx_hash.to_string()))?
        .get_body()?
        .into_rows() {
            for row in rows {
                // TODO: parse into better transaction builder
                let tx = CQLTx::try_from_row(row)?;
                builder
                .payload(Payload::from_str(from_utf8(&tx.payload.into_vec()).unwrap()))
                .address(Address::from_str(from_utf8(&tx.address.into_vec()).unwrap()))
                .value(Value(tx.value as i64))
                .obsolete_tag(Tag::from_str(from_utf8(&tx.obsolete_tag.into_vec()).unwrap()))
                .timestamp(Timestamp(tx.timestamp as u64))
                .index(Index(tx.current_index as usize))
                .last_index(Index(tx.last_index as usize))
                .bundle(Hash::from_str(from_utf8(&tx.bundle.into_vec()).unwrap()))
                .trunk(Hash::from_str(from_utf8(&tx.trunk.into_vec()).unwrap()))
                .branch(Hash::from_str(from_utf8(&tx.branch.into_vec()).unwrap()))
                .tag(Tag::from_str(from_utf8(&tx.tag.into_vec()).unwrap()))
                .attachment_ts(Timestamp(tx.attachment_timestamp as u64))
                .attachment_lbts(Timestamp(tx.attachment_timestamp_lower as u64))
                .attachment_ubts(Timestamp(tx.attachment_timestamp_upper as u64))
                .nonce(Nonce::from_str(from_utf8(&tx.nonce.into_vec()).unwrap()));
            }
        };


        Ok(builder.build())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::executor::block_on;

    // TODO: Current test required working scylladb on local. We should have a setup script for this.
    #[test]
    fn test_connection() {
        let tx = TransactionBuilder::default().build();
        let tx_hash = Hash::default();

        let s = block_on(CQLSession::establish_connection("0.0.0.0:9042")).unwrap();

        block_on(s.insert_transaction(&tx_hash, &tx)).unwrap();
        block_on(s.find_transaction(&tx_hash)).unwrap();

        CQLSession::destroy_connection(s);
    }
}
