pub const CREATE_KEYSPACE_QUERY: &str = r#"
CREATE KEYSPACE IF NOT EXISTS chronicle
WITH REPLICATION = {
  'class': 'SimpleStrategy',
  'replication_factor': 1
};
"#;

pub const CREATE_TX_TABLE_QUERY: &str = r#"
CREATE TABLE IF NOT EXISTS chronicle.transaction (
  hash blob PRIMARY KEY,
  payload blob,
  address blob,
  value int,
  obsolete_tag blob,
  timestamp int,
  current_index smallint,
  last_index smallint,
  bundle blob,
  trunk blob,
  branch blob,
  tag blob,
  attachment_timestamp int,
  attachment_timestamp_lower int,
  attachment_timestamp_upper int,
  nonce blob,
);
"#;

pub const CREATE_EDGE_TABLE_QUERY: &str = r#"
CREATE TABLE IF NOT EXISTS chronicle.edge (
  hash blob,
  kind tinyint,
  timestamp int,
  tx blob,
  PRIMARY KEY(hash, kind, timestamp)
);
"#;

pub const INSERT_TX_QUERY: &str = r#"
  INSERT INTO chronicle.transaction (
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
  INSERT INTO chronicle.edge (
    hash,
    kind,
    timestamp,
    tx
  ) VALUES (?,?,?,?);
"#;

pub const SELECT_TX_QUERY: &str = r#"
  SELECT * FROM chronicle.transaction
  WHERE hash = ?;
"#;
