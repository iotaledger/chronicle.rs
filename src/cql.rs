use async_trait::async_trait;
use bundle::{
    Hash,
    Transaction,
};
use cdrs::{
    authenticators::NoneAuthenticator,
    cluster::session::{new as new_session, Session},
    cluster::{ClusterTcpConfig, NodeTcpConfigBuilder, TcpConnectionPool},
    load_balancing::RoundRobinSync,
    query::QueryExecutor,
    query_values,
    Error as CDRSError,
};

use crate::{
    statements::*,
};

#[async_trait]
pub trait Connection {
    type Session;
    type StorageError;

    async fn establish_connection(url: &str) -> Result<Self::Session, Self::StorageError>;
    async fn destroy_connection(connection: Self::Session) -> Result<(), Self::StorageError>;
}

#[async_trait]
pub trait StorageBackend {
    type StorageError;

    async fn insert_transaction(&self, tx_hash: &Hash, tx: &Transaction) -> Result<(), Self::StorageError>;
    async fn find_transaction(&self, tx_hash: &Hash) -> Result<Transaction, Self::StorageError>;
    // TODO: find transactions by bundle/address/tag/approvee
}


pub struct CQLSession(Session<RoundRobinSync<TcpConnectionPool<NoneAuthenticator>>>);

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
            "index" => tx.index().0 as i16,
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

    async fn find_transaction(&self, tx_hash: &Hash) -> Result<Transaction, Self::StorageError> {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::executor::block_on;

    // TODO: Current test required working scylladb on local. We should have a setup script for this.
    #[test]
    fn test_connection() {
        let session = CQLSession::establish_connection("0.0.0.0:9042");
        let s = block_on(session).unwrap();
        CQLSession::destroy_connection(s);
    }
}
