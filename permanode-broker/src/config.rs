use serde::{
    Deserialize,
    Serialize,
};
use std::collections::VecDeque;
use url::Url;

#[derive(Debug, Default, Serialize, Deserialize, PartialEq, Eq, Clone)]
pub struct BrokerConfig {
    pub mqtt_brokers: Vec<Url>,
    pub api_endpoints: VecDeque<Url>,
}
