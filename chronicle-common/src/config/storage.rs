use maplit::{
    hashmap,
    hashset,
};

use super::*;
use std::{
    collections::HashSet,
    net::SocketAddr,
};

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
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
pub struct StorageConfig {
    /// Keyspace definition for this cluster, keyed by the network
    /// they will pull data from
    pub keyspaces: Vec<KeyspaceConfig>,
    /// The Scylla listen address
    pub listen_address: SocketAddr,
    /// The Scylla thread count
    pub thread_count: ThreadCount,
    /// The Scylla reporter count
    pub reporter_count: u8,
    /// The name of the local datacenter
    pub local_datacenter: String,
    /// Nodes to initialize in the cluster
    pub nodes: HashSet<SocketAddr>,
    /// The partition config
    #[serde(default)]
    pub partition_config: PartitionConfig,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            keyspaces: vec![Default::default()],
            listen_address: ([127, 0, 0, 1], 8080).into(),
            thread_count: Default::default(),
            reporter_count: 1,
            local_datacenter: "datacenter1".to_string(),
            nodes: hashset![([127, 0, 0, 1], 9042).into()],
            partition_config: Default::default(),
        }
    }
}

impl StorageConfig {
    /// Verify that the storage config is valid
    pub async fn verify(&mut self) -> Result<(), Cow<'static, str>> {
        if self.reporter_count.eq(&0) {
            return Err("reporter_count must be greater than zero, ensure your config is correct".into());
        }
        if self.local_datacenter.eq(&"") {
            return Err("local_datacenter must be non-empty string, ensure your config is correct".into());
        }
        Ok(())
    }
}

/// Configuration for a scylla keyspace
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
pub struct KeyspaceConfig {
    /// The name of the keyspace
    pub name: KeyspaceName,
    /// Datacenters configured for this keyspace, keyed by name
    pub data_centers: HashMap<DatacenterName, DatacenterConfig>,
}

impl Default for KeyspaceConfig {
    fn default() -> Self {
        Self {
            name: "chronicle".to_string(),
            data_centers: hashmap! {
                "USA".to_string() => DatacenterConfig {
                    replication_factor: 2,
                },
                "Canada".to_string() => DatacenterConfig {
                    replication_factor: 1,
                },
            },
        }
    }
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
