use async_trait::async_trait;
use bee_bundle::{
    Address, Hash, Index, Nonce, Payload, Tag, Timestamp, Transaction, TransactionBuilder, Value,
};
use cdrs::{
    authenticators::NoneAuthenticator,
    cluster::session::{new as new_session, Session},
    cluster::{ClusterTcpConfig, NodeTcpConfigBuilder, PagerState, TcpConnectionPool},
    frame::frame_result::{RowsMetadata, RowsMetadataFlag},
    frame::traits::TryFromRow,
    load_balancing::RoundRobinSync,
    query::{BatchExecutor, BatchQueryBuilder, QueryExecutor, QueryParamsBuilder, QueryValues},
    query_values,
    types::{blob::Blob, from_cdrs::FromCDRSByName, rows::Row, IntoRustByName},
    Error as CDRSError,
};
use std::{str::from_utf8, sync::Arc};

use crate::statements::*;

#[async_trait]
/// Methods for initilize and destroy database connection session. User should know they type of which database session is used.
pub trait Connection: Clone + Send + Sync {
    type Session;
    type StorageError;

    async fn establish_connection(url: &str) -> Result<Self::Session, Self::StorageError>;
    async fn destroy_connection(connection: Self::Session) -> Result<(), Self::StorageError>;
}

#[async_trait]
/// Methods of database query collections.
pub trait StorageBackend {
    type StorageError;

    async fn insert_transaction(
        &self,
        tx_hash: &Hash,
        tx: &Transaction,
    ) -> Result<(), Self::StorageError>;
    async fn select_transaction(&self, tx_hash: &Hash) -> Result<Transaction, Self::StorageError>;
    async fn select_transaction_hashes(
        &self,
        hash: &Hash,
        kind: EdgeKind,
    ) -> Result<Vec<Hash>, Self::StorageError>;
}

/// Session works for any CQL database like Cassandra and ScyllaDB.
#[derive(Clone)]
pub struct CQLSession(Arc<Session<RoundRobinSync<TcpConnectionPool<NoneAuthenticator>>>>);

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
    nonce: Blob,
}

#[derive(Debug, TryFromRow)]
struct CQLEdge {
    hash: Blob,
    kind: i8,
    timestamp: i32,
    tx: Blob,
}

#[repr(i8)]
#[derive(Copy, Clone, PartialEq, Eq)]
pub enum EdgeKind {
    Bundle = 0,
    Address = 1,
    Tag = 2,
    Approvee = 3,
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
        let conn = CQLSession(Arc::new(
            new_session(&cluster, balance).expect("session should be created"),
        ));
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

    fn paged_query(
        &self,
        query: &str,
        values: QueryValues,
        page_size: i32,
    ) -> Result<Vec<Row>, CDRSError> {
        let mut rows: Vec<Row> = Vec::new();
        let mut pager_state: PagerState = PagerState::new();
        loop {
            let mut params = QueryParamsBuilder::new().page_size(page_size);
            if pager_state.get_cursor().is_some() {
                params = params.paging_state(pager_state.get_cursor().clone().unwrap());
            }
            params = params.values(values.clone());

            let body = self
                .0
                .query_with_params(query.clone(), params.finalize())
                .and_then(|frame| frame.get_body())?;

            let metadata_res: Result<RowsMetadata, CDRSError> = body
                .as_rows_metadata()
                .ok_or("Pager query should yield a vector of rows".into());
            let metadata = metadata_res?;

            if let Some(has_more_pages) =
                Some(RowsMetadataFlag::has_has_more_pages(metadata.flags.clone()))
            {
                let mut new_rows = body.into_rows().unwrap();
                rows.append(&mut new_rows);
                if !has_more_pages {
                    break;
                }
                let cursor = metadata.paging_state.clone();
                if cursor.is_some() {
                    pager_state =
                        PagerState::with_cursor_and_more_flag(cursor.unwrap(), has_more_pages);
                }
            }
        }
        Ok(rows)
    }
}

#[async_trait]
impl StorageBackend for CQLSession {
    type StorageError = CDRSError;

    async fn insert_transaction(&self, tx_hash: &Hash, tx: &Transaction) -> Result<(), CDRSError> {
        let values = query_values!(
            tx_hash.to_string(),
            tx.payload().to_string(),
            tx.address().to_string(),
            tx.value().0 as i32,
            tx.obsolete_tag().to_string(),
            tx.timestamp().0 as i32,
            tx.index().0 as i16,
            tx.last_index().0 as i16,
            tx.bundle().to_string(),
            tx.trunk().to_string(),
            tx.branch().to_string(),
            tx.tag().to_string(),
            tx.attachment_ts().0 as i32,
            tx.attachment_lbts().0 as i32,
            tx.attachment_ubts().0 as i32,
            tx.nonce().to_string()
        );
        let bundle = query_values!(
            tx.bundle().to_string(),
            0i8,
            tx.timestamp().0 as i32,
            tx_hash.to_string()
        );
        let address = query_values!(
            tx.address().to_string(),
            1i8,
            tx.timestamp().0 as i32,
            tx_hash.to_string()
        );
        let tag = query_values!(
            tx.tag().to_string(),
            2i8,
            tx.timestamp().0 as i32,
            tx_hash.to_string()
        );
        let approvee1 = query_values!(
            tx_hash.to_string(),
            3i8,
            tx.timestamp().0 as i32,
            tx.trunk().to_string()
        );
        let approvee2 = query_values!(
            tx_hash.to_string(),
            3i8,
            tx.timestamp().0 as i32,
            tx.branch().to_string()
        );

        let batch = BatchQueryBuilder::new()
            .add_query(INSERT_TX_QUERY, values)
            .add_query(INSERT_EDGE_QUERY, bundle)
            .add_query(INSERT_EDGE_QUERY, address)
            .add_query(INSERT_EDGE_QUERY, tag)
            .add_query(INSERT_EDGE_QUERY, approvee1)
            .add_query(INSERT_EDGE_QUERY, approvee2);
        self.0.batch_with_params(batch)?;
        Ok(())
    }

    async fn select_transaction(&self, tx_hash: &Hash) -> Result<Transaction, CDRSError> {
        let mut builder = TransactionBuilder::new();
        if let Some(rows) = self
            .0
            .query_with_values(SELECT_TX_QUERY, query_values!(tx_hash.to_string()))?
            .get_body()?
            .into_rows()
        {
            for row in rows {
                // TODO: parse into better transaction builder
                let tx = CQLTx::try_from_row(row)?;
                builder
                    .payload(Payload::from_str(
                        from_utf8(&tx.payload.into_vec()).unwrap(),
                    ))
                    .address(Address::from_str(
                        from_utf8(&tx.address.into_vec()).unwrap(),
                    ))
                    .value(Value(tx.value as i64))
                    .obsolete_tag(Tag::from_str(
                        from_utf8(&tx.obsolete_tag.into_vec()).unwrap(),
                    ))
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

    async fn select_transaction_hashes(
        &self,
        hash: &Hash,
        kind: EdgeKind,
    ) -> Result<Vec<Hash>, Self::StorageError> {
        let mut hashes: Vec<Hash> = vec![];
        let values = query_values!(
            "hash" => hash.to_string(),
            "kind" => kind as i8
        );

        // TODO: Refactor to paged query.
        let rows = self.paged_query(SELECT_EDGE_QUERY, values, 2)?;
        for row in rows {
            let s: Blob = row.get_r_by_name("tx")?;
            hashes.push(Hash::from_str(from_utf8(&s.into_vec()).unwrap()));
        }

        Ok(hashes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // TODO: Current test required working scylladb on local. We should have a setup script for this.
    #[tokio::test]
    async fn test_connection() {
        let tx = TransactionBuilder::default().build();
        let tx_hash = Hash::default();

        let s = CQLSession::establish_connection("0.0.0.0:9042")
            .await
            .expect("storage connection");

        s.insert_transaction(&tx_hash, &tx)
            .await
            .expect("insert tx");
        s.select_transaction(&tx_hash).await.expect("select tx");
        s.select_transaction_hashes(&tx_hash, EdgeKind::Address)
            .await
            .expect("select tx hashes");

        CQLSession::destroy_connection(s)
            .await
            .expect("storage disconnect");
    }
}
