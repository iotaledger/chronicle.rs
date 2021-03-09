use serde::{
    Deserialize,
    Serialize,
};
use std::collections::HashMap;

/// Type alias for datacenter names
pub type DatacenterName = String;
/// Type alias for scylla keysapce names
pub type KeyspaceName = String;

/// Scylla storage configuration. Defines data which can be used
/// to construct and access the scylla cluster.
#[derive(Debug, Default, Serialize, Deserialize, PartialEq, Eq, Clone)]
pub struct StorageConfig {
    /// Keyspace definition for this cluster, keyed by the network
    /// they will pull data from
    pub keyspaces: Vec<KeyspaceConfig>,
}

/// Configuration for a scylla keyspace
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
pub struct KeyspaceConfig {
    /// The name of the keyspace
    pub name: KeyspaceName,
    /// Datacenters configured for this keyspace, keyed by name
    pub data_centers: HashMap<DatacenterName, DatacenterConfig>,
}

/// Configuration for a scylla datacenter
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
pub struct DatacenterConfig {
    /// The scylla replication factor for this datacenter
    pub replication_factor: usize,
}
