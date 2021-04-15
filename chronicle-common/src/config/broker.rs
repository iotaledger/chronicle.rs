use super::*;
use log::warn;
use paho_mqtt::{
    AsyncClient,
    CreateOptionsBuilder,
};
use reqwest::Client;
use serde_json::Value;
use std::{
    collections::HashSet,
    net::SocketAddr,
};
use url::Url;
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
pub struct BrokerConfig {
    pub websocket_address: SocketAddr,
    pub mqtt_brokers: HashMap<MqttType, HashSet<Url>>,
    pub api_endpoints: HashSet<Url>,
    pub sync_range: Option<SyncRange>,
    pub logs_dir: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum MqttType {
    Messages,
    MessagesReferenced,
}

impl Default for BrokerConfig {
    fn default() -> Self {
        Self {
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
            sync_range: Some(Default::default()),
            logs_dir: "chronicle/logs/".to_owned(),
        }
    }
}

impl BrokerConfig {
    /// Verify that the broker's config is valid
    pub async fn verify(&mut self) -> anyhow::Result<()> {
        for mqtt_broker in self.mqtt_brokers.values().flatten() {
            let random_id: u64 = rand::random();
            let create_opts = CreateOptionsBuilder::new()
                .server_uri(mqtt_broker.as_str())
                .client_id(&format!("{}|{}", "verifier", random_id))
                .persistence(None)
                .finalize();
            let _client = AsyncClient::new(create_opts)
                .map_err(|e| anyhow!("Error verifying mqtt broker {}: {}", mqtt_broker, e))?;
        }
        let client = Client::new();
        self.api_endpoints = self
            .api_endpoints
            .drain()
            .filter_map(|endpoint| {
                let path = endpoint.as_str();
                if path.is_empty() {
                    warn!("Empty endpoint provided!");
                    return None;
                }
                if !path.ends_with("/") {
                    warn!("Endpoint provided without trailing slash: {}", endpoint);
                    let new_endpoint = format!("{}/", path).parse();
                    if let Ok(new_endpoint) = new_endpoint {
                        return Some(new_endpoint);
                    } else {
                        warn!("Could not append trailing slash!");
                        return None;
                    }
                }
                Some(endpoint)
            })
            .collect();
        for endpoint in self.api_endpoints.iter() {
            let res = client
                .get(
                    endpoint
                        .join("info")
                        .map_err(|e| anyhow!("Error verifying endpoint {}: {}", endpoint, e))?,
                )
                .send()
                .await
                .map_err(|e| anyhow!("Error verifying endpoint {}: {}", endpoint, e))?;
            if !res.status().is_success() {
                let url = res.url().clone();
                let err = res.json::<Value>().await;
                bail!(
                    "Error verifying endpoint \"{}\"\nRequest URL: \"{}\"\nResult: {:#?}",
                    endpoint,
                    url,
                    err
                );
            }
        }
        let sync_range = self.sync_range.get_or_insert_with(|| SyncRange::default());
        if sync_range.from == 0 || sync_range.to == 0 {
            bail!("Error verifying sync from/to, zero provided!\nPlease provide non-zero milestone index");
        } else if sync_range.from >= sync_range.to {
            bail!("Error verifying sync from/to, greater or equal provided!\nPlease provide lower \"Sync range from\" milestone index");
        }
        Ok(())
    }
}
