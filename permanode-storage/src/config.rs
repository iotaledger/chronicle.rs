use serde::Deserialize;
use std::collections::HashMap;

pub type DatacenterName = String;
pub type KeyspaceName = String;

#[derive(Debug, Deserialize, PartialEq, Eq, Clone)]
pub struct StorageConfig {
    pub keyspaces: HashMap<KeyspaceName, KeyspaceConfig>,
}
#[derive(Debug, Deserialize, PartialEq, Eq, Clone)]
pub struct KeyspaceConfig {
    pub keyspace: IotaKeyspace,
    pub data_centers: HashMap<DatacenterName, DatacenterConfig>,
}

#[derive(Debug, Deserialize, PartialEq, Eq, Clone)]
pub struct DatacenterConfig {
    pub replication_factor: usize,
}

#[derive(Debug, Deserialize, PartialEq, Eq, Copy, Clone)]
pub enum IotaKeyspace {
    Mainnet,
    Devnet,
}
