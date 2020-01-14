#[allow(dead_code)]
use cdrs::{
    authenticators::NoneAuthenticator,
    cluster::{
        session::{
            new as new_session,
            // other option: new_lz4 as new_lz4_session,
            // other option: new_snappy as new_snappy_session
            Session,
        },
        ClusterTcpConfig, NodeTcpConfigBuilder, TcpConnectionPool,
    },
    load_balancing::SingleNode,
    query::*,
    query_values,
    transport::CDRSTransport,
    types::prelude::*,
    Error as CDRSError, Result as CDRSResult,
};
use std::cell::RefCell;
use time::Timespec;

use crate::psedo_bundle::PseudoBundle;

pub type CurrentSession = Session<SingleNode<TcpConnectionPool<NoneAuthenticator>>>;

static CREATE_KEYSPACE_QUERY: &'static str = r#"
  CREATE KEYSPACE IF NOT EXISTS fast_logger
    WITH REPLICATION = {
      'class': 'SimpleStrategy',
      'replication_factor': 1
    };
"#;

static CREATE_BUNDLE_TABLE_QUERY: &'static str = r#"
  CREATE TABLE IF NOT EXISTS fast_logger.info (
    bundle text,
    time timestamp,
    info text,
    PRIMARY KEY(bundle, time)
  );
"#;

static ADD_BUNDLE_QUERY: &'static str = r#"
  INSERT INTO fast_logger.info (bundle, time, info)
    VALUES (?, ?, ?);
"#;

static SELECT_BUNDLES_BY_TIME_RANGE_QUERY: &'static str = r#"
  SELECT * FROM fast_logger.info
    WHERE time > ?
      AND time < ?
      ALLOW FILTERING;
"#;

pub fn create_db_session() -> CDRSResult<CurrentSession> {
    let auth = NoneAuthenticator;
    let node = NodeTcpConfigBuilder::new("127.0.0.1:9042", auth).build();
    let cluster_config = ClusterTcpConfig(vec![node]);
    new_session(&cluster_config, SingleNode::new())
}

pub fn create_keyspace<T, M>(session: &mut impl QueryExecutor<T, M>) -> CDRSResult<()>
where
    T: CDRSTransport + 'static,
    M: r2d2::ManageConnection<Connection = RefCell<T>, Error = CDRSError> + Sized,
{
    session.query(CREATE_KEYSPACE_QUERY).map(|_| (()))
}

pub fn create_bundle_table<T, M>(session: &mut impl QueryExecutor<T, M>) -> CDRSResult<()>
where
    T: CDRSTransport + 'static,
    M: r2d2::ManageConnection<Connection = RefCell<T>, Error = CDRSError> + Sized,
{
    session.query(CREATE_BUNDLE_TABLE_QUERY).map(|_| (()))
}

pub fn add_bundle<T, M>(
    session: &mut impl QueryExecutor<T, M>,
    bundle: PseudoBundle,
) -> CDRSResult<()>
where
    T: CDRSTransport + 'static,
    M: r2d2::ManageConnection<Connection = RefCell<T>, Error = CDRSError> + Sized,
{
    session
        .query_with_values(ADD_BUNDLE_QUERY, bundle.into_query_values())
        .map(|_| (()))
}

pub fn prepare_add_bundle<T, M>(
    session: &mut impl PrepareExecutor<T, M>,
) -> CDRSResult<PreparedQuery>
where
    T: CDRSTransport + 'static,
    M: r2d2::ManageConnection<Connection = RefCell<T>, Error = CDRSError> + Sized,
{
    session.prepare(ADD_BUNDLE_QUERY)
}

pub fn execute_add_bundle<T, M>(
    session: &mut impl ExecExecutor<T, M>,
    prepared_query: &PreparedQuery,
    bundle: PseudoBundle,
) -> CDRSResult<()>
where
    T: CDRSTransport + 'static,
    M: r2d2::ManageConnection<Connection = RefCell<T>, Error = CDRSError> + Sized,
{
    session
        .exec_with_values(prepared_query, bundle.into_query_values())
        .map(|_| (()))
}

pub fn select_bundles_by_time_range<T, M>(
    session: &mut impl QueryExecutor<T, M>,
    time_from: Timespec,
    time_to: Timespec,
) -> CDRSResult<Vec<PseudoBundle>>
where
    T: CDRSTransport + 'static,
    M: r2d2::ManageConnection<Connection = RefCell<T>, Error = CDRSError> + Sized,
{
    let values = query_values!(time_from, time_to);
    session
        .query_with_values(SELECT_BUNDLES_BY_TIME_RANGE_QUERY, values)
        .and_then(|res| res.get_body())
        .and_then(|body| {
            body.into_rows()
                .ok_or(CDRSError::from("cannot get rows from a response body"))
        })
        .and_then(|rows| {
            let mut bundles: Vec<PseudoBundle> = Vec::with_capacity(rows.len());

            for row in rows {
                bundles.push(PseudoBundle::try_from_row(row)?);
            }

            Ok(bundles)
        })
}
