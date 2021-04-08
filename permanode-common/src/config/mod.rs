use super::*;
use anyhow::{
    anyhow,
    bail,
    ensure,
};
pub use api::*;
pub use broker::*;
use ron::value::Value;
use std::{
    collections::HashMap,
    convert::TryInto,
    io::Read,
    net::SocketAddr,
    path::Path,
};
pub use storage::*;

mod api;
mod broker;
mod storage;

pub const CONFIG_PATH: &str = "./config.ron";
pub const HISTORICAL_CONFIG_PATH: &str = "./historical_config";
pub const CURRENT_VERSION: u32 = 0;

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Eq)]
pub struct VersionedConfig {
    version: u32,
    config: Config,
}

#[derive(Debug, Deserialize, Clone)]
pub struct VersionedValue {
    version: u32,
    config: Value,
}

impl VersionedConfig {
    pub fn new(config: Config) -> Self {
        Self {
            version: CURRENT_VERSION,
            config,
        }
    }

    pub fn load<P: Into<Option<String>>>(path: P) -> anyhow::Result<Self> {
        VersionedValue::load(path)
    }

    pub fn load_unchecked<P: Into<Option<String>>>(path: P) -> anyhow::Result<Self> {
        let path = path
            .into()
            .or_else(|| std::env::var("CONFIG_PATH").ok())
            .unwrap_or(CONFIG_PATH.to_string());
        match std::fs::File::open(Path::new(&path)) {
            Ok(f) => ron::de::from_reader(f).map_err(|e| anyhow!(e)),
            Err(e) => match e.kind() {
                std::io::ErrorKind::NotFound => {
                    let config: VersionedConfig = Config::default().try_into()?;
                    config.save(path)?;
                    Ok(config)
                }
                _ => bail!(e),
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

    pub async fn verify(self) -> anyhow::Result<Config> {
        ensure!(
            self.version == CURRENT_VERSION,
            "Config file version mismatch! Expected: {}, Actual: {}",
            CURRENT_VERSION,
            self.version
        );
        self.config.verify().await
    }
}

impl VersionedValue {
    pub fn load<P: Into<Option<String>>>(path: P) -> anyhow::Result<VersionedConfig> {
        let path = path
            .into()
            .or_else(|| std::env::var("CONFIG_PATH").ok())
            .unwrap_or(CONFIG_PATH.to_string());
        match std::fs::File::open(Path::new(&path)) {
            Ok(mut f) => {
                let mut val = String::new();
                f.read_to_string(&mut val)?;
                ron::de::from_str::<VersionedValue>(&val)
                    .map_err(|e| anyhow!(e))
                    .and_then(|v| {
                        v.verify_version()
                            .and_then(|_| ron::de::from_str::<VersionedConfig>(&val).map_err(|e| anyhow!(e)))
                    })
            }
            Err(e) => match e.kind() {
                std::io::ErrorKind::NotFound => {
                    let config: VersionedConfig = Config::default().try_into()?;
                    config.save(path)?;
                    Ok(config)
                }
                _ => bail!(e),
            },
        }
    }

    fn verify_version(self) -> anyhow::Result<()> {
        ensure!(
            self.version == CURRENT_VERSION,
            "Config file version mismatch! Expected: {}, Actual: {}",
            CURRENT_VERSION,
            self.version
        );
        Ok(())
    }
}

impl From<Config> for VersionedConfig {
    fn from(config: Config) -> Self {
        Self::new(config)
    }
}

impl Default for VersionedConfig {
    fn default() -> Self {
        Config::default().into()
    }
}

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
        VersionedConfig::load(path).map(|c| c.config)
    }

    pub fn save<P: Into<Option<String>>>(&self, path: P) -> anyhow::Result<()> {
        VersionedConfig::new(self.clone()).save(path)
    }

    pub async fn verify(mut self) -> anyhow::Result<Self> {
        self.storage_config.verify().await?;
        self.api_config.verify().await?;
        self.broker_config.verify().await?;
        Ok(self)
    }
}

impl Persist for Config {
    fn persist(&self) -> anyhow::Result<()> {
        self.save(None).map(|_| {
            std::env::set_var("HISTORICAL_CONFIG_PATH", self.historical_config_path.clone());
        })
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
        let config: VersionedConfig = config.try_into().unwrap();

        std::env::set_var("CONFIG_PATH", "../config.example.ron");

        let mut deserialized_config = VersionedConfig::load(None).expect("Failed to deserialize example config!");

        if config != deserialized_config {
            config.save(None).expect("Failed to serialize example config!");
            deserialized_config = VersionedConfig::load(None).expect("Failed to deserialize example config!");
        }

        assert_eq!(config, deserialized_config);
    }
}
