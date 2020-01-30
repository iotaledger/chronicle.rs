use cdrs::{
    authenticators::NoneAuthenticator,
    cluster::session::{new as new_session, Session},
    cluster::{ClusterTcpConfig, NodeTcpConfigBuilder, TcpConnectionPool},
    load_balancing::RoundRobinSync,
    query::QueryExecutor,
    Error as CDRSError,
};

use async_trait::async_trait;

use crate::{ConnectionError, Connection, statements::*};

pub struct CQLSession(Session<RoundRobinSync<TcpConnectionPool<NoneAuthenticator>>>);

// TODO: Error handling
#[async_trait]
impl Connection for CQLSession {
    type Session = CQLSession;

    async fn establish_connection(url: &str) -> Result<CQLSession, ConnectionError> {
        let node = NodeTcpConfigBuilder::new(url, NoneAuthenticator {}).build();
        let cluster = ClusterTcpConfig(vec![node]);
        let balance = RoundRobinSync::new();
        let conn = CQLSession(new_session(&cluster, balance).expect("session should be created"));

        Ok(conn)
    }

    async fn destroy_connection(_connection: CQLSession) -> Result<(), ConnectionError> {
        Ok(())
    }
}

impl CQLSession {
    pub fn create_keyspace(&self) -> Result<(), CDRSError> {
        self.0.query(CREATE_KEYSPACE_QUERY)?;
        Ok(())
    }

    pub fn create_table(&self) -> Result<(), CDRSError> {
        self.0.query(CREATE_TX_TABLE_QUERY)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::executor::block_on;

    #[test]
    fn test_connection() {
        let session = CQLSession::establish_connection("0.0.0.0:9042");
        if let Ok(s) = block_on(session) {
            s.create_keyspace().unwrap();
            s.create_table().unwrap();
            CQLSession::destroy_connection(s);
        } else {
            panic!("fail!");
        }
    }
}
