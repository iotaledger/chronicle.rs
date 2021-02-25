use serde::Deserialize;
use std::{
    borrow::Cow,
    collections::HashMap,
};

pub type DatacenterName = String;
pub type KeyspaceName = String;

#[derive(Debug, Deserialize)]
pub struct Config {
    pub keyspaces: HashMap<KeyspaceName, KeyspaceConfig>,
}

impl Config {
    pub fn from_file(path: &std::path::Path) -> Result<Config, Cow<'static, str>> {
        let f = std::fs::File::open(path).map_err(|e| Cow::from(e.to_string()))?;
        ron::de::from_reader(f).map_err(|e| e.to_string().into())
    }
}

#[derive(Debug, Deserialize)]
pub struct KeyspaceConfig {
    name: KeyspaceName,
    keyspace: IotaKeyspace,
    data_centers: HashMap<DatacenterName, DatacenterConfig>,
}

#[derive(Debug, Deserialize)]
pub struct DatacenterConfig {
    name: DatacenterName,
    replication_factor: usize,
}

#[derive(Debug, Deserialize)]
pub enum IotaKeyspace {
    Mainnet,
    Devnet,
}
