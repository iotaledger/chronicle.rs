use permanode_api::ApiConfig;
use permanode_broker::BrokerConfig;
use permanode_storage::StorageConfig;
use serde::Deserialize;
use std::borrow::Cow;

#[derive(Debug, Deserialize, PartialEq, Eq, Clone)]
pub struct Config {
    pub storage_config: StorageConfig,
    pub api_config: ApiConfig,
    pub broker_config: BrokerConfig,
}

impl Config {
    pub fn from_file(path: &std::path::Path) -> Result<Config, Cow<'static, str>> {
        let f = std::fs::File::open(path).map_err(|e| Cow::from(e.to_string()))?;
        ron::de::from_reader(f).map_err(|e| e.to_string().into())
    }
}

#[cfg(test)]
mod test {
    use std::path::Path;

    use super::*;
    use maplit::hashmap;
    use permanode_storage::{
        DatacenterConfig,
        KeyspaceConfig,
        TangleNetwork,
    };

    #[test]
    pub fn example_config() {
        let config = Config {
            storage_config: StorageConfig {
                keyspaces: hashmap! {
                    TangleNetwork::Mainnet => KeyspaceConfig {
                        name: "my_mainnet".to_string(),
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
            },
            api_config: ApiConfig {},
            broker_config: BrokerConfig {},
        };

        let deserialized_config =
            Config::from_file(Path::new("../config.example.ron")).expect("Failed to deserialize example_config!");

        assert_eq!(config, deserialized_config);
    }
}
