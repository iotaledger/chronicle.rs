const CREATE_KEYSPACE_QUERY: &str = r#"
CREATE KEYSPACE IF NOT EXISTS chronicle
WITH REPLICATION = {
  'class': 'SimpleStrategy',
  'replication_factor': 1
};
"#;

const CREATE_BUNDLE_TABLE_QUERY: &str = r#"
  CREATE TABLE IF NOT EXISTS chronicle.bundle (
    bundle text,
    time timestamp,
    info text,
    PRIMARY KEY(bundle, time)
  );
"#;

const ADD_BUNDLE_QUERY: &str = r#"
  INSERT INTO chronicle.bundle (bundle, time, info)
    VALUES (?, ?, ?);
"#;

const SELECT_BUNDLES_BY_TIME_RANGE_QUERY: &str = r#"
  SELECT * FROM chronicle.bundle
    WHERE time > ?
      AND time < ?
      ALLOW FILTERING;
"#;
