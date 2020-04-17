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
  value varint,
  obsolete_tag blob,
  timestamp varint,
  current_index varint,
  last_index varint,
  bundle blob,
  trunk blob,
  branch blob,
  tag blob,
  attachment_timestamp varint,
  attachment_timestamp_lower varint,
  attachment_timestamp_upper varint,
  nonce blob,
  milestone varint,
);
"#;

pub const CREATE_EDGE_TABLE_QUERY: &str = r#"
CREATE TABLE IF NOT EXISTS tangle.edge (
  vertex blob,
  kind text,
  timestamp varint,
  tx blob,
  PRIMARY KEY(vertex, kind, timestamp)
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
    nonce
  ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);
"#;

pub const INSERT_EDGE_QUERY: &str = r#"
  INSERT INTO tangle.edge (
    vertex,
    kind,
    timestamp,
    tx
  ) VALUES (?,?,?,?);
"#;

pub const SELECT_TX_QUERY: &str = r#"
  SELECT * FROM tangle.transaction
  WHERE hash = ?;
"#;

pub const SELECT_EDGE_QUERY: &str = r#"
  SELECT * FROM tangle.edge
  WHERE vertex = ?;
"#;
