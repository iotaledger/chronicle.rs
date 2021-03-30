use super::*;
use log::warn;
use paho_mqtt::{
    AsyncClient,
    CreateOptionsBuilder,
};
use reqwest::Client;
use serde_json::Value;
use std::{
    collections::VecDeque,
    net::SocketAddr,
};
use url::Url;
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
pub struct BrokerConfig {
    pub websocket_address: SocketAddr,
    pub mqtt_brokers: Vec<Url>,
    pub api_endpoints: VecDeque<Url>,
    pub sync_range: Option<SyncRange>,
}

impl Default for BrokerConfig {
    fn default() -> Self {
        Self {
            websocket_address: "127.0.0.1:9000".parse().unwrap(),
            mqtt_brokers: Vec::new(),
            api_endpoints: VecDeque::default(),
            sync_range: None,
        }
    }
}

impl BrokerConfig {
    /// Verify that the broker's config is valid
    pub async fn verify(&mut self) -> Result<(), Cow<'static, str>> {
        for mqtt_broker in self.mqtt_brokers.iter() {
            let random_id: u64 = rand::random();
            let create_opts = CreateOptionsBuilder::new()
                .server_uri(mqtt_broker.as_str())
                .client_id(&format!("{}|{}", "verifier", random_id))
                .persistence(None)
                .finalize();
            let _client = AsyncClient::new(create_opts)
                .map_err(|e| format!("Error verifying mqtt broker {}: {}", mqtt_broker, e))?;
        }
        let client = Client::new();
        for endpoint in self.api_endpoints.iter_mut() {
            let path = endpoint.as_str();
            if path.is_empty() {
                return Err("Empty endpoint provided!".into());
            }
            if !path.ends_with("/") {
                warn!("Endpoint provided without trailing slash: {}", endpoint);
                let new_endpoint = format!("{}/", path).parse();
                if let Ok(new_endpoint) = new_endpoint {
                    *endpoint = new_endpoint;
                } else {
                    warn!("Could not append trailing slash!");
                }
            }
            let res = client
                .get(
                    endpoint
                        .join("info")
                        .map_err(|e| format!("Error verifying endpoint {}: {}", endpoint, e))?,
                )
                .send()
                .await
                .map_err(|e| format!("Error verifying endpoint {}: {}", endpoint, e))?;
            if !res.status().is_success() {
                let url = res.url().clone();
                let err = res.json::<Value>().await;
                return Err(format!(
                    "Error verifying endpoint \"{}\"\nRequest URL: \"{}\"\nResult: {:#?}",
                    endpoint, url, err
                )
                .into());
            }
        }
        let sync_range = self.sync_range.get_or_insert_with(|| SyncRange::default());
        if sync_range.from == 0 || sync_range.to == 0 {
            return Err("Error verifying sync from/to, zero provided!\nPlease provide non-zero milestone index".into());
        } else if sync_range.from >= sync_range.to {
            return Err("Error verifying sync from/to, greater or equal provided!\nPlease provide lower \"Sync range from\" milestone index".into());
        }
        Ok(())
    }
}
