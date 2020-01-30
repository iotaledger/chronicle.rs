/// This is the tangle CQL Keyspace,
/// A keyspace is a collection of tables with attributes which
/// define how data is replicated on nodes, the current
/// tables(bundle/edge) reside in this keyspace.
/// # NOTE: Currently we only support SimpleStrategy,
/// that is recommended for one datacenter setup.
/// currently we are working on a better structure.

pub const CREATE_KEYSPACE_QUERY: &str = r#"
CREATE KEYSPACE IF NOT EXISTS chronicle
WITH REPLICATION = {
  'class': 'SimpleStrategy',
  'replication_factor': 3
};
"#;

/// This is a CQL table schema for Edge,
/// Please Check The following link for more details about DataTypes
/// https://docs.scylladb.com/getting-started/types/
/// :edge is the table name,
/// :v1 is a vertex_one alias, and also the partition key,
/// which act as distrbuited shard secondary index for the bundle
/// relations, for now it only hold outputs/inputs/tx_hashes/tips.
/// :lb is a label alias, and also the first clustering key.
/// :ts is a bundle_timestamp alias, and also the second clustering key.
/// :v2 is an vertex_two alias, also the third clustering key,
/// and hold the bundle_hash.
/// :ex is an extra_vertex alias, also the 4th clustering key,
/// and hold the address's value(trytes form) when the row's label
/// is input/output and the head_hash when the labels are
/// tx_hash/approve/head_hash.
/// :ix is an index alias, also the 5th clustering key,
/// which hold the current_index(signed-varint form)
/// we consider the transaction is an input if the following cond is met:
///   - value of the transaction is less than zero.
/// we consider the transaction is an output if the following cond is met:
///   - value of the transaction is equal or greater than zero.
/// example: tx_hash for an input at current_index 5 will be stored as -5.
/// if the label is approve,then the indexes are only (0 or 1) where
///  0 indicates trunk_tip(tip0)
///  1 indicates branch_tip(tip1)
/// :el is an extra label alias, and non-clustering-column,
/// it hold an extra label in varint form, mostly it's used for tx-hash
/// rows, it will indicate whether the tx-hash row belongs to input or output.
/// # NOTE: the labels are stored in tinyint form, %{
///   10 => :output,
///   20 => :input,
///   30 => :txhash,
///   40 => :headHash,
///   50 => :approve,
///   60 => :hint # is address hint,
///   70 => :hint # is tag hint
/// }
/// :lx is a last_index alias, and non-clustering-column,
/// it hold the same value(last_index in varint form) no matter
/// what the labels are.
/// :sx is a snapshot_index alias, and non-clustering-column,
/// it hold the same value(snapshot_index in trytes form),
/// keep in mind the intitial state of snapshot_index is NULL
/// for any bundle (tx-objects siblings in a given attachment)
/// this field should be inserted only for confirmed bundles,
/// never insert snapshot_index for un-confirmed bundles, at
/// anycost, as it might overwrite the confirmed status.

pub const CREATE_EDGE_TABLE_QUERY: &str = r#"
  CREATE TABLE IF NOT EXISTS chronicle.edge (
    v1 blob,
    lb tinyint,
    ts varint,
    v2 blob,
    ex blob,
    ix varint,
    el varint,
    lx varint,
    sx varint,
    PRIMARY KEY(v1, lb, ts, v2, ex, ix)
  )WITH CLUSTERING ORDER BY (lb ASC, ts DESC);
"#;

/// This is a CQL table schema for bundle,
/// Please Check The following link for more details about DataTypes
/// https://docs.scylladb.com/getting-started/types/
/// :bundle is the table name,
/// :bh is a bundle_hash alias, and also the partition_key(pk),
/// which mean the whole parition(the rows with same pk) stored in same shard.
/// :lb is a label alias, and also the first clustering key.
/// :ts is a bundle_timestamp alias, and also the second clustering key.
/// :ix is an index alias, and also the third clustering key.
/// :id is an id, and also the forth clustering key.
/// :va is a value, and also the last clustering key,
///     if labels(lb) are output or input, the :va hold the address trytes,
///     if labels(lb) are txhash or headhash, the :va indicates whether it's
///     input or output.
/// all the next non-clustering-columns are named to form
/// alphabetical/asc order, and the names don't hold anymeaning,
/// because they depend on the row's label.
/// # NOTE: lb clustering column hold the labels in tinyint form.
/// the bundle table is consuming the following tinyint-labels %{
///     10 => :output,
///     20 => :input,
///     30 => :txhash,
///     40 => :headHash
/// }
/// :a column hold the address's value(trytes form) when the row's label/lb is :output or :input,
/// and hold the snapshot_index(trytes form) when the the row's label is :txhash or :headHash.
/// keep in mind we should not touch the snapshot_index for un-confirmed bundles,
/// :b column hold lastIndex(trytes form) when the row's label is :output or :input,
/// and hold the transaction hash(trytes form) when the row's label is :txHash or :headHash.
/// :c column hold bundle_nonce(trytes form) when the row's label is :output or :input,
/// and hold the transaction_nonce(trytes form) when the row's label is :txHash or :headHash.
/// :d column hold obsoleteTag(trytes form) when the row's label is :output or :input,
/// and hold the transaction_tag when the row's label is :txHash or :headHash.
/// :e column hold signatureFragment(trytes form) when the row's label is :output or :input,
/// and hold the trunk(trytes form) when the row's label is :txHash or :headHash.
/// :f column hold nothing when the row's label is :output or :input,
/// but hold the branch(trytes form) when the row's label is :txHash or :headHash.
/// :g column hold nothing when the row's label is :output or :input,
/// but hold the attachment_timestamp(trytes form) when the row's label is :txHash or :headHash.
/// :h column hold nothing when the row's label is :output or :input,
/// but hold the attachment_timestamp_upper_bound(trytes form) when the row's label is :txHash or :headHash.
/// :i column hold nothing when the row's label is :output or :input,
/// but hold the attachment_timestamp_lower_bound(trytes form) when the row's label is :txHash or :headHash.

pub const CREATE_BUNDLE_TABLE_QUERY: &str = r#"
  CREATE TABLE IF NOT EXISTS chronicle.bundle (
    bh blob,
    lb tinyin,
    ts varint,
    ix varint,
    id blob,
    va blob,
    a varint,
    b blob,
    c blob,
    d blob,
    e blob,
    f blob,
    g varint,
    h varint,
    i varint,
    PRIMARY KEY(bh, lb, ts, ix, id, va)
  )WITH CLUSTERING ORDER BY (lb ASC, ts DESC);
"#;

/// This is a CQL table schema for Tag,
/// Please Check The following link for more details about DataTypes
/// https://docs.scylladb.com/getting-started/types/
/// :tag is the table name,
/// :p0, is pair0 alias first 2 chars of tag, in IAC they represent 2200km # first component in pk,
/// :p1, is pair1 alias, second 2 chars from tag, in IAC they represent 110km # second component in pk
/// :yy, is year alias, third component partition key
/// :mm, is month alias, 4th component partition key
/// :p2, is pair2 alias, third 2 chars from tag, in IAC they represent 5.5 km
/// :p3, is pair3 alias, 4th 2 chars from tag, in IAC they represent 275m
// :rt is remaining tag alias, remaining 19 chars from tag.
//// :ts is a timestamp alias, and also the 6th clustering key.
/// :th is txhash alias, also the last clustering key,

pub const CREATE_TAG_TABLE_QUERY: &str = r#"
  CREATE TABLE IF NOT EXISTS chronicle.tag (
    p0 varchar,
    p1 varchar,
    yy smallint,
    mm smallint,
    p2 varchar,
    p3 varchar,
    rt varchar,
    ts varint,
    th blob,
    PRIMARY KEY((p0, p1, yy, mm), p2, p3, rt, ts, th)
  )WITH CLUSTERING ORDER BY (p2 ASC, p3 ASC, rt ASC, ts DESC)
   AND default_time_to_live = 0;
"#;

/// This is a CQL table schema for ZeroValue,
/// Please Check The following link for more details about DataTypes
/// https://docs.scylladb.com/getting-started/types/
/// it's also important to mention this table is meant to store the
/// address's relations for zero_value_bundles. and it consumes
/// two labels (:output, :input).
/// :zero_value is the table name,
/// :v1 is a vertex_one alias, and also the first component_partition_key,
/// :yy is a year alias, and also the second component_partition_key,
/// :mm is a month alias, and the last component_partition_key, thus
/// the complete composite partition_key is (:v1, :yy, :mm)
/// :lb is a label alias, and also the first clustering key.
/// :ts is a bundle_timestamp alias, and also the second clustering key.
/// :v2 is an vertex_two alias, also the third clustering key,
/// and hold the bundle_hash.
/// :ix is an index alias, also the 4th clustering key,
/// which hold the current_index(signed-varint form)
/// # NOTE: the consumed labels are stored in tinyint form, %{
///     10 => :output,
///     20 => :input,
/// }
/// :lx is a last_index alias, and non-clustering-column,
/// it hold the same value(last_index in varint form) no matter
/// what the labels are.

pub const CREATE_ZERO_VALUE_TABLE_QUERY: &str = r#"
  CREATE TABLE IF NOT EXISTS chronicle.zerovalue (
    v1 blob,
    yy smallin,
    mm smallin,
    lb tinyint,
    ts varint,
    v2 blob,
    ex blob,
    ix varint,
    el varint,
    lx varint,
    sx varint,
    PRIMARY KEY((v1, yy, mm), lb, ts, v2, ex, ix)
  )WITH CLUSTERING ORDER BY (lb ASC, ts DESC);
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

pub const SELECT_EDGE_QUERY: &str = r#"
  SELECT * FROM chronicle.edge
  WHERE hash = ?
  AND kind = ?;
"#;
