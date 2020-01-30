use cdrs::{
    authenticators::NoneAuthenticator,
    cluster::session::{new as new_session, Session},
    cluster::{ClusterTcpConfig, NodeTcpConfigBuilder, TcpConnectionPool},
    load_balancing::RoundRobinSync,
    query::QueryExecutor,
    Error as CDRSError,
};

use async_trait::async_trait;

use crate::{Connection, statements::*};

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
        Ok(())
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
