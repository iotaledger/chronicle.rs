// Copyright 2020 IOTA Stiftung
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and limitations under the License.

//! This crate defines the Cassandra statements used in Chronicle.

/// Query of a transaction table creation.
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

/// Query of hint table creation.
pub const CREATE_HINT_TABLE_QUERY: &str = r#"
CREATE TABLE IF NOT EXISTS tangle.hint (
  vertex blob,
  kind text,
  timestamp bigint,
  tx blob,
  value bigint,
  extra blob,
  PRIMARY KEY(vertex, kind, timestamp, tx)
);
"#;

/// Query of data table creation.
pub const CREATE_DATA_TABLE_QUERY: &str = r#"
CREATE TABLE IF NOT EXISTS tangle.data (
  vertex blob,
  year smallint,
  month tinyint,
  kind text,
  timestamp bigint,
  tx blob,
  value bigint,
  extra blob,
  PRIMARY KEY((vertex,year,month), kind, timestamp, tx)
);
"#;

/// Query of transaction insertion.
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

/// Query of hint insertion.
pub const INSERT_HINT_QUERY: &str = r#"
  INSERT INTO tangle.hint (
    vertex,
    kind,
    timestamp,
    tx,
    value,
    extra
) VALUES (?,?,?,?,?,?);
"#;

/// Query of data insertion.
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

/// Query of transaction selection.
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

/// Query of edge selection.
pub const SELECT_EDGE_QUERY: &str = r#"
  SELECT * FROM tangle.edge
  WHERE vertex = ?;
"#;
