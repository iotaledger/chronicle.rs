// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0
use super::*;
use arc_swap::ArcSwap;
use ron::value::Value;
use url::Url;

lazy_static! {
    /// Lazy static holding the alert config
    pub static ref ALERT_CONFIG: ArcSwap<AlertConfig> = Default::default();
}

/// An alert request that will be sent to a given url
///
/// ## Example
/// The JSON object can be any valid json structure which
/// will be sent as the body of the request. The special token
/// `$msg` will be replaced with the string alert message.
///
/// ```no_compile
/// AlertRequest (
///     url: "http://hooks.slack.com/services/T00000000/B00000000/XXXXXXXXXXXXXXXXXXXXXXXX",
///     json: Some(
///         {
///             text: "$msg",
///         }
///     )
/// )
/// ```
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
pub struct AlertRequest {
    /// The URL to which alert requests will be sent
    pub url: Url,
    /// The json which will be sent (with replaced $msg token)
    pub json: Option<Value>,
}

/// Alert notification config, which specifies where chronicle should send
/// important notifications such as outages or disk overflow
#[derive(Debug, Default, Serialize, Deserialize, PartialEq, Eq, Clone)]
pub struct AlertConfig {
    /// List of alert requests
    pub requests: Vec<AlertRequest>,
}

/// Store new alert config
pub fn init(alert_config: AlertConfig) {
    ALERT_CONFIG.store(alert_config.into())
}
