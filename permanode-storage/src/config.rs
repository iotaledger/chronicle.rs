use serde::{
    Deserialize,
    Serialize,
};
use std::collections::HashMap;

/// Type alias for datacenter names
pub type DatacenterName = String;
/// Type alias for scylla keysapce names
pub type KeyspaceName = String;

/// Enum specifying a thread count
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
pub enum ThreadCount {
    /// A scalar count of threads
    Count(usize),
    /// A multiple of the available cores
    CoreMultiple(usize),
}

impl Default for ThreadCount {
    fn default() -> Self {
        Self::CoreMultiple(1)
    }
}

/// Scylla storage configuration. Defines data which can be used
/// to construct and access the scylla cluster.
#[derive(Debug, Default, Serialize, Deserialize, PartialEq, Eq, Clone)]
pub struct StorageConfig {
    /// Keyspace definition for this cluster, keyed by the network
    /// they will pull data from
    pub keyspaces: Vec<KeyspaceConfig>,
    /// The Scylla listen address
    pub listen_address: String,
    /// The Scylla thread count
    pub thread_count: ThreadCount,
    /// The Scylla reporter count
    pub reporter_count: u8,
    /// The name of the local datacenter
    pub local_datacenter: String,
    /// The partition config
    #[serde(default)]
    pub partition_config: PartitionConfig,
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

/// The partition config. Defaults to using 1000 partitions and a chunk size of 60480.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
pub struct PartitionConfig {
    /// The total number of partitions to use
    pub partition_count: u16,
    /// The number of sequential milestones to store side-by-side in a partition
    pub milestone_chunk_size: u32,
}

impl Default for PartitionConfig {
    fn default() -> Self {
        PartitionConfig {
            partition_count: 1000,
            milestone_chunk_size: 60480,
        }
    }
}

impl PartitionConfig {
    /// Calculate partition id from a milestone index.
    /// The formula is:<br>
    ///    `(I / C) % P`<br>
    ///    where:<br>
    ///          `I` = milestone index<br>
    ///          `C` = configured milestone chunk size<br>
    ///          `P` = configured partition count<br>
    pub fn partition_id(&self, milestone_index: u32) -> u16 {
        ((milestone_index / self.milestone_chunk_size) % (self.partition_count as u32)) as u16
    }
}
