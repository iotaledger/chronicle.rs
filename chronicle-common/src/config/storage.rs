// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;
use maplit::{
    hashmap,
    hashset,
};
use std::collections::HashSet;

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
    #[serde(deserialize_with = "super::deserialize_socket_addr")]
    pub listen_address: SocketAddr,
    /// The Scylla thread count
    pub thread_count: ThreadCount,
    /// The Scylla reporter count
    pub reporter_count: u8,
    /// The name of the local datacenter
    pub local_datacenter: String,
    /// Nodes to initialize in the cluster
    #[serde(deserialize_with = "super::deserialize_socket_addr_collected")]
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
    pub async fn verify(&mut self) -> anyhow::Result<()> {
        if self
            .keyspaces
            .iter()
            .any(|k| k.data_centers.iter().any(|dc| dc.1.replication_factor == 0))
        {
            bail!("replication_factor must be greater than zero, ensure your config is correct");
        }
        if self.reporter_count.eq(&0) {
            bail!("reporter_count must be greater than zero, ensure your config is correct");
        }
        if self.local_datacenter.eq(&"") {
            bail!("local_datacenter must be non-empty string, ensure your config is correct");
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
            name: "permanode".to_string(),
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
impl StorageConfig {
    /// Try to get the uniform replication factor, which is the lowest rf in all keyspace across all dc
    pub fn try_get_uniform_rf(&self) -> Option<u8> {
        // collect all data_centers from all keyspaces
        let mut replication_factors = Vec::new();
        self.keyspaces.iter().for_each(|k| {
            for (_, data_center_config) in k.data_centers.iter() {
                replication_factors.push(data_center_config.replication_factor)
            }
        });
        replication_factors.iter().min().and_then(|e| Some(*e))
    }
}

/// Configuration for a scylla datacenter
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
pub struct DatacenterConfig {
    /// The scylla replication factor for this datacenter
    pub replication_factor: u8,
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
            milestone_chunk_size: 8640,
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
