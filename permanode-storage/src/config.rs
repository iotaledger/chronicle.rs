use serde::Deserialize;
use std::{
    borrow::Cow,
    collections::HashMap,
};

pub type DatacenterName = String;
pub type KeyspaceName = String;

#[derive(Debug, Deserialize, PartialEq, Eq)]
pub struct Config {
    pub keyspaces: HashMap<KeyspaceName, KeyspaceConfig>,
}

impl Config {
    pub fn from_file(path: &std::path::Path) -> Result<Config, Cow<'static, str>> {
        let f = std::fs::File::open(path).map_err(|e| Cow::from(e.to_string()))?;
        ron::de::from_reader(f).map_err(|e| e.to_string().into())
    }
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
pub struct KeyspaceConfig {
    keyspace: IotaKeyspace,
    data_centers: HashMap<DatacenterName, DatacenterConfig>,
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
pub struct DatacenterConfig {
    replication_factor: usize,
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
pub enum IotaKeyspace {
    Mainnet,
    Devnet,
}

#[cfg(test)]
mod test {
    use std::path::Path;

    use super::*;
    use maplit::hashmap;

    #[test]
    pub fn example_config() {
        let config = Config {
            keyspaces: hashmap! {
                "my_mainnet".to_string() => KeyspaceConfig {
                    keyspace: IotaKeyspace::Mainnet,
                    data_centers: hashmap!{
                        "USA".to_string() => DatacenterConfig {
                            replication_factor: 2,
                        },
                        "Canada".to_string() => DatacenterConfig {
                            replication_factor: 1,
                        },
                    }
                }
            },
        };

        let deserialized_config =
            Config::from_file(Path::new("../example_config.ron")).expect("Failed to deserialize example_config!");

        assert_eq!(config, deserialized_config);
    }
}
