// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;
pub use alert::*;
use anyhow::{
    anyhow,
    bail,
    ensure,
};
pub use api::*;
pub use broker::*;
use maplit::{
    hashmap,
    hashset,
};
use ron::value::Value;
use std::{
    collections::HashMap,
    convert::TryInto,
    io::Read,
    net::SocketAddr,
    path::Path,
};
pub use storage::*;

mod alert;
mod api;
mod broker;
mod storage;

/// The default config file path
pub const CONFIG_PATH: &str = "./config.ron";
/// The default historical config path
pub const HISTORICAL_CONFIG_PATH: &str = "./historical_config";
/// The current config version.
/// **Must be updated with each change to the config format.**
const CURRENT_VERSION: u32 = 3;

/// Versioned config. Tracks version between config changes so that it can be validated on load.
#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Eq)]
pub struct VersionedConfig {
    version: u32,
    config: Config,
}

/// A variant of `VersionedConfig` which is used to deserialize
/// a config file independent of its inner structure so the version can
/// be validated.
#[derive(Debug, Deserialize, Clone)]
struct VersionedValue {
    version: u32,
    config: Value,
}

impl VersionedConfig {
    /// Wrap a config with the current version
    pub fn new(config: Config) -> Self {
        Self {
            version: CURRENT_VERSION,
            config,
        }
    }

    /// Load versioned config from a RON file. Will check the following paths in this order:
    /// 1. Provided path
    /// 2. Environment variable CONFIG_PATH
    /// 3. Default config path: ./config.ron
    pub fn load<P: Into<Option<String>>>(path: P) -> anyhow::Result<Self> {
        VersionedValue::load(path)
    }

    /// Load versioned config from a RON file without checking the version. Will check the
    /// following paths in this order:
    /// 1. Provided path
    /// 2. Environment variable CONFIG_PATH
    /// 3. Default config path: ./config.ron
    pub fn load_unchecked<P: Into<Option<String>>>(path: P) -> anyhow::Result<Self> {
        let opt_path = path.into();
        let paths = vec![
            opt_path.clone(),
            std::env::var("CONFIG_PATH").ok(),
            Some(CONFIG_PATH.to_string()),
        ]
        .into_iter();
        for path in paths.filter_map(|v| v) {
            match std::fs::File::open(Path::new(&path)) {
                Ok(f) => return ron::de::from_reader(f).map_err(|e| anyhow!(e)),
                Err(e) => match e.kind() {
                    std::io::ErrorKind::NotFound => {
                        continue;
                    }
                    _ => bail!(e.to_string()),
                },
            }
        }
        let path = opt_path
            .or_else(|| std::env::var("CONFIG_PATH").ok())
            .unwrap_or(CONFIG_PATH.to_string());
        let config = Self::default();
        config.save(path.clone())?;
        bail!(
            "Config file was not found! Saving a default config file at {}. Please edit it and restart the application!",
            std::fs::canonicalize(&path).map(|p| p.to_string_lossy().into_owned()).unwrap_or(path)
        );
    }

    /// Save versioned config to a RON file. Will use the first of the following possible locations:
    /// 1. Provided path
    /// 2. Environment variable CONFIG_PATH
    /// 3. Default config path: ./config.ron
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

    /// Verify this config and its version are correct
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
    fn load<P: Into<Option<String>>>(path: P) -> anyhow::Result<VersionedConfig> {
        let opt_path = path.into();
        let paths = vec![
            opt_path.clone(),
            std::env::var("CONFIG_PATH").ok(),
            Some(CONFIG_PATH.to_string()),
        ]
        .into_iter();
        for path in paths.filter_map(|v| v) {
            match std::fs::File::open(Path::new(&path)) {
                Ok(mut f) => {
                    let mut val = String::new();
                    f.read_to_string(&mut val)?;
                    return ron::de::from_str::<VersionedValue>(&val)
                        .map_err(|e| anyhow!(e))
                        .and_then(|v| {
                            v.verify_version()
                                .and_then(|_| ron::de::from_str::<VersionedConfig>(&val).map_err(|e| anyhow!(e)))
                        });
                }
                Err(e) => match e.kind() {
                    std::io::ErrorKind::NotFound => {
                        continue;
                    }
                    _ => bail!(e.to_string()),
                },
            }
        }
        let path = opt_path
            .or_else(|| std::env::var("CONFIG_PATH").ok())
            .unwrap_or(CONFIG_PATH.to_string());
        let config: VersionedConfig = Config::default().try_into()?;
        config.save(path.clone())?;
        bail!(
                "Config file was not found! Saving a default config file at {}. Please edit it and restart the application!",
                std::fs::canonicalize(&path).map(|p| p.to_string_lossy().into_owned()).unwrap_or(path)
            );
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

/// Chronicle application config
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
pub struct Config {
    /// The top-level command websocket listener address
    pub websocket_address: SocketAddr,
    /// Scylla storage configuration
    pub storage_config: StorageConfig,
    /// API configuration
    pub api_config: ApiConfig,
    /// Broker configuration
    pub broker_config: BrokerConfig,
    /// Historical config file path
    pub historical_config_path: String,
    /// Alert notification config
    pub alert_config: AlertConfig,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            websocket_address: ([127, 0, 0, 1], 8081).into(),
            storage_config: Default::default(),
            api_config: Default::default(),
            broker_config: Default::default(),
            historical_config_path: HISTORICAL_CONFIG_PATH.to_owned(),
            alert_config: Default::default(),
        }
    }
}

impl Config {
    /// Load unversioned config from a RON file.
    /// The config must still be the correct version in order to deserialize correctly.
    /// Will check the following paths in this order:
    /// 1. Provided path
    /// 2. Environment variable CONFIG_PATH
    /// 3. Default config path: ./config.ron
    pub fn load<P: Into<Option<String>>>(path: P) -> anyhow::Result<Config> {
        VersionedConfig::load(path).map(|c| c.config)
    }

    /// Save config to a RON file with the current version. Will use the first of the following possible locations:
    /// 1. Provided path
    /// 2. Environment variable CONFIG_PATH
    /// 3. Default config path: ./config.ron
    pub fn save<P: Into<Option<String>>>(&self, path: P) -> anyhow::Result<()> {
        VersionedConfig::new(self.clone()).save(path)
    }

    /// Verify this config
    pub async fn verify(mut self) -> anyhow::Result<Self> {
        self.storage_config.verify().await?;
        self.api_config.verify().await?;
        self.broker_config.verify().await?;
        self.alert_config.verify().await?;
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
                collector_count: 10,
                requester_count: 10,
                request_timeout_secs: 5,
                parallelism: 25,
                retries_per_endpoint: 5,
                retries_per_query: 100,
                complete_gaps_interval_secs: 3600,
                mqtt_stream_capacity: 10000,
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
                logs_dir: Some("chronicle/logs/".to_owned()),
                max_log_size: Some(4294967296),
            },
            historical_config_path: HISTORICAL_CONFIG_PATH.to_owned(),
            alert_config: Default::default(),
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
