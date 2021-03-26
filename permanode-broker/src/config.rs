use log::warn;
use paho_mqtt::{
    AsyncClient,
    CreateOptionsBuilder,
};
use reqwest::Client;
use serde::{
    Deserialize,
    Serialize,
};
use serde_json::Value;
use std::{
    borrow::Cow,
    collections::VecDeque,
};
use url::Url;

#[derive(Debug, Default, Serialize, Deserialize, PartialEq, Eq, Clone)]
pub struct BrokerConfig {
    pub mqtt_brokers: Vec<Url>,
    pub api_endpoints: VecDeque<Url>,
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
        Ok(())
    }
}
