pub const CREATE_KEYSPACE_QUERY: &str = r#"
CREATE KEYSPACE IF NOT EXISTS tangle
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
  value blob,
  obsolete_tag blob,
  timestamp blob,
  current_index blob,
  last_index blob,
  bundle blob,
  trunk blob,
  branch blob,
  tag blob,
  attachment_timestamp blob,
  attachment_timestamp_lower blob,
  attachment_timestamp_upper blob,
  nonce blob,
  milestone bigint,
);
"#;

pub const CREATE_EDGE_TABLE_QUERY: &str = r#"
CREATE TABLE IF NOT EXISTS tangle.edge (
  vertex blob,
  kind text,
  timestamp bigint,
  tx blob,
  value bigint,
  extra blob,
  PRIMARY KEY(vertex, kind, timestamp, tx)
);
"#;

pub const CREATE_DATA_TABLE_QUERY: &str = r#"
CREATE TABLE IF NOT EXISTS tangle.data (
  vertex blob,
  year smallint,
  month tinyint,
  kind text,
  timestamp bigint,
  tx blob,
  extra blob,
  PRIMARY KEY((vertex,year,month), kind, timestamp, tx)
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
    extra
) VALUES (?,?,?,?,?,?);
"#;

pub const INSERT_DATA_QUERY: &str = r#"
  INSERT INTO tangle.data (
    vertex,
    year,
    month,
    kind,
    timestamp,
    tx,
    extra
) VALUES (?,?,?,?,?,?,?);
"#;

pub const SELECT_TX_QUERY: &str = r#"
  SELECT
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
  FROM tangle.transaction
  WHERE hash = ?;
"#;

pub const SELECT_EDGE_QUERY: &str = r#"
  SELECT * FROM tangle.edge
  WHERE vertex = ?;
"#;
