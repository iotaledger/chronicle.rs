use super::*;
use anyhow::{
    anyhow,
    bail,
};
pub use api::*;
pub use broker::*;
use log::error;
use maplit::{
    hashmap,
    hashset,
};
use std::{
    borrow::Cow,
    collections::HashMap,
    net::SocketAddr,
    path::Path,
};
pub use storage::*;

mod api;
mod broker;
mod storage;

pub const CONFIG_PATH: &str = "./config.ron";
pub const HISTORICAL_CONFIG_PATH: &str = "./historical_config";

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
pub struct Config {
    pub websocket_address: SocketAddr,
    pub storage_config: StorageConfig,
    pub api_config: ApiConfig,
    pub broker_config: BrokerConfig,
    pub historical_config_path: String,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            websocket_address: ([127, 0, 0, 1], 8081).into(),
            storage_config: Default::default(),
            api_config: Default::default(),
            broker_config: Default::default(),
            historical_config_path: HISTORICAL_CONFIG_PATH.to_owned(),
        }
    }
}

impl Config {
    pub fn load<P: Into<Option<String>>>(path: P) -> anyhow::Result<Config> {
        let path = path
            .into()
            .or_else(|| std::env::var("CONFIG_PATH").ok())
            .unwrap_or(CONFIG_PATH.to_string());
        match std::fs::File::open(Path::new(&path)) {
            Ok(f) => ron::de::from_reader(f).map_err(|e| anyhow!(e)),
            Err(e) => match e.kind() {
                std::io::ErrorKind::NotFound => {
                    let config = Self::default();
                    config.save(path.clone())?;
                    bail!(
                        "Config file was not found! Saving a default config file at {}. Please edit it and restart the application!", 
                        std::fs::canonicalize(&path).map(|p| p.to_string_lossy().into_owned()).unwrap_or(path)
                    );
                }
                _ => bail!(e.to_string()),
            },
        }
    }

    pub fn save<P: Into<Option<String>>>(&self, path: P) -> anyhow::Result<()> {
        let path = path
            .into()
            .or_else(|| std::env::var("CONFIG_PATH").ok())
            .unwrap_or(CONFIG_PATH.to_string());
        debug!("Saving config to {}", path);
        let path = Path::new(&path);
        if let Some(dir) = path.parent() {
            if !dir.exists() {
                std::fs::create_dir_all(dir)?;
            }
        }
        let f = std::fs::File::create(Path::new(&path))?;
        ron::ser::to_writer_pretty(f, self, ron::ser::PrettyConfig::default()).map_err(|e| anyhow!(e))
    }

    pub async fn verify(&mut self) -> Result<(), Cow<'static, str>> {
        self.storage_config.verify().await?;
        self.api_config.verify().await?;
        self.broker_config.verify().await?;
        Ok(())
    }
}

impl Persist for Config {
    fn persist(&self) {
        if let Err(e) = self.save(None) {
            error!("{}", e);
        } else {
            std::env::set_var("HISTORICAL_CONFIG_PATH", self.historical_config_path.clone());
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use maplit::{
        hashmap,
        hashset,
    };

    #[test]
    pub fn example_config() {
        let config = Config {
            websocket_address: ([127, 0, 0, 1], 8081).into(),
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
                nodes: hashset![([127, 0, 0, 1], 9042).into()],
                partition_config: PartitionConfig::default(),
            },
            api_config: ApiConfig {},
            broker_config: BrokerConfig {
                websocket_address: ([127, 0, 0, 1], 9000).into(),
                mqtt_brokers: hashmap! {
                    MqttType::Messages => hashset![
                        url::Url::parse("tcp://api.hornet-0.testnet.chrysalis2.com:1883").unwrap(),
                        url::Url::parse("tcp://api.hornet-1.testnet.chrysalis2.com:1883").unwrap(),
                    ],
                    MqttType::MessagesReferenced => hashset![
                        url::Url::parse("tcp://api.hornet-0.testnet.chrysalis2.com:1883").unwrap(),
                        url::Url::parse("tcp://api.hornet-1.testnet.chrysalis2.com:1883").unwrap(),
                    ]
                },
                api_endpoints: hashset![
                    url::Url::parse("https://api.hornet-0.testnet.chrysalis2.com/api/v1").unwrap(),
                    url::Url::parse("https://api.hornet-1.testnet.chrysalis2.com/api/v1").unwrap(),
                ]
                .into(),
                sync_range: Some(SyncRange::default()),
                logs_dir: "permanode/logs/".to_owned(),
            },
            historical_config_path: HISTORICAL_CONFIG_PATH.to_owned(),
        };

        std::env::set_var("CONFIG_PATH", "../config.example.ron");

        let mut deserialized_config = Config::load(None).expect("Failed to deserialize example config!");

        if config != deserialized_config {
            config.save(None).expect("Failed to serialize example config!");
            deserialized_config = Config::load(None).expect("Failed to deserialize example config!");
        }

        assert_eq!(config, deserialized_config);
    }
}
