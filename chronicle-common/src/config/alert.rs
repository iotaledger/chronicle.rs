// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0
use super::*;
use url::Url;

/// An alert request that will be sent to a given url
///
/// ## Example
/// The JSON object can be any valid json structure which
/// will be sent as the body of the request. The special token
/// `$msg` will be replaced with the string alert message.
///
/// ```
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

impl AlertConfig {
    /// Verify the alert configuration
    pub async fn verify(&self) -> anyhow::Result<()> {
        Ok(())
    }
}
