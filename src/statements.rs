pub const CREATE_KEYSPACE_QUERY: &str = r#"
CREATE KEYSPACE IF NOT EXISTS chronicle
WITH REPLICATION = {
  'class': 'SimpleStrategy',
  'replication_factor': 1
};
"#;

pub const CREATE_TX_TABLE_QUERY: &str = r#"
  CREATE TABLE IF NOT EXISTS chronicle.transaction (
    transaction text,
    time timestamp,
    info text,
    PRIMARY KEY(transaction, time)
  );
"#;

pub const ADD_BUNDLE_QUERY: &str = r#"
  INSERT INTO chronicle.bundle (bundle, time, info)
    VALUES (?, ?, ?);
"#;

pub const SELECT_BUNDLES_BY_TIME_RANGE_QUERY: &str = r#"
  SELECT * FROM chronicle.bundle
    WHERE time > ?
      AND time < ?
      ALLOW FILTERING;
"#;
