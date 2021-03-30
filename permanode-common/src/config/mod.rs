use super::*;
pub use api::*;
pub use broker::*;
use std::{
    borrow::Cow,
    collections::HashMap,
    path::Path,
};
pub use storage::*;

mod api;
mod broker;
mod storage;

pub const CONFIG_PATH: &str = "./config.ron";

#[derive(Debug, Default, Serialize, Deserialize, PartialEq, Eq, Clone)]
pub struct Config {
    pub storage_config: StorageConfig,
    pub api_config: ApiConfig,
    pub broker_config: BrokerConfig,
}

impl Config {
    pub fn load() -> Result<Config, Cow<'static, str>> {
        let path = std::env::var("CONFIG_PATH").unwrap_or(CONFIG_PATH.to_string());
        let path = Path::new(&path);
        match std::fs::File::open(path) {
            Ok(f) => ron::de::from_reader(f).map_err(|e| e.to_string().into()),
            Err(e) => match e.kind() {
                std::io::ErrorKind::NotFound => {
                    let config = Self::default();
                    config.save()?;
                    Ok(config)
                }
                _ => Err(e.to_string().into()),
            },
        }
    }

    pub fn save(&self) -> Result<(), Cow<'static, str>> {
        let path = std::env::var("CONFIG_PATH").unwrap_or(CONFIG_PATH.to_string());
        let path = Path::new(&path);
        let f = std::fs::File::create(path).map_err(|e| Cow::from(e.to_string()))?;
        ron::ser::to_writer_pretty(f, self, ron::ser::PrettyConfig::default()).map_err(|e| e.to_string().into())
    }

    pub async fn verify(&mut self) -> Result<(), Cow<'static, str>> {
        self.storage_config.verify().await?;
        self.api_config.verify().await?;
        self.broker_config.verify().await?;
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use maplit::hashmap;

    #[test]
    pub fn example_config() {
        let config = Config {
            storage_config: StorageConfig {
                keyspaces: vec![KeyspaceConfig {
                    name: "permanode".to_string(),
                    data_centers: hashmap! {
                        "USA".to_string() => DatacenterConfig {
                            replication_factor: 2,
                        },
                        "Canada".to_string() => DatacenterConfig {
                            replication_factor: 1,
                        },
                    },
                }],
                listen_address: ([127, 0, 0, 1], 8080).into(),
                thread_count: ThreadCount::CoreMultiple(1),
                reporter_count: 2,
                local_datacenter: "datacenter1".to_owned(),
                nodes: vec![([127, 0, 0, 1], 9042).into()],
                partition_config: PartitionConfig::default(),
            },
            api_config: ApiConfig {},
            broker_config: BrokerConfig {
                websocket_address: ([127, 0, 0, 1], 9000).into(),
                mqtt_brokers: vec![
                    url::Url::parse("tcp://api.hornet-0.testnet.chrysalis2.com:1883").unwrap(),
                    url::Url::parse("tcp://api.hornet-1.testnet.chrysalis2.com:1883").unwrap(),
                ],
                api_endpoints: vec![
                    url::Url::parse("https://api.hornet-0.testnet.chrysalis2.com/api/v1").unwrap(),
                    url::Url::parse("https://api.hornet-1.testnet.chrysalis2.com/api/v1").unwrap(),
                ]
                .into(),
                sync_range: Some(SyncRange::default()),
            },
        };

        std::env::set_var("CONFIG_PATH", "../config.example.ron");

        let mut deserialized_config = Config::load().expect("Failed to deserialize example config!");

        if config != deserialized_config {
            config.save().expect("Failed to serialize example config!");
            deserialized_config = Config::load().expect("Failed to deserialize example config!");
        }

        assert_eq!(config, deserialized_config);
    }
}
