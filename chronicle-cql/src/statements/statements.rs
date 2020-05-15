use crate::frame::query::Query;
use crate::frame::header::{self, Header};
use crate::frame::queryflags;

pub const CREATE_KEYSPACE_QUERY: &str = r#"
CREATE KEYSPACE IF NOT EXISTS chronicle
WITH REPLICATION = {
  'class': 'SimpleStrategy',
  'replication_factor': 1
};
"#;

pub const CREATE_TX_TABLE_QUERY: &str = r#"
CREATE TABLE IF NOT EXISTS tangle.transaction (
  hash blob PRIMARY KEY,
  payload blob,
  address blob,
  value i64,
  obsolete_tag blob,
  timestamp i64,
  current_index i64,
  last_index i64,
  bundle blob,
  trunk blob,
  branch blob,
  tag blob,
  attachment_timestamp i64,
  attachment_timestamp_lower i64,
  attachment_timestamp_upper i64,
  nonce blob,
  milestone i64,
);
"#;

pub const CREATE_EDGE_TABLE_QUERY: &str = r#"
CREATE TABLE IF NOT EXISTS tangle.edge (
  vertex blob,
  kind text,
  timestamp i64,
  tx blob,
  value i64,
  milestone i64,
  extra blob,
  PRIMARY KEY(vertex, kind, timestamp, tx)
);
"#;

pub const INSERT_TX_QUERY: &str = r#"
  INSERT INTO tangle.transaction (
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

pub const INSERT_EDGE_QUERY: &str = r#"
  INSERT INTO tangle.edge (
    vertex,
    kind,
    timestamp,
    tx,
    value,
    milestone,
    extra
) VALUES (?,?,?,?,?,?,?);
"#;

pub const SELECT_TX_QUERY: &str = r#"
  SELECT * FROM tangle.transaction
  WHERE hash = ?;
"#;

pub const SELECT_EDGE_QUERY: &str = r#"
  SELECT * FROM tangle.edge
  WHERE vertex = ?;
"#;
