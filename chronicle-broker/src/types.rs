// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use bee_message::{
    prelude::{
        Essence,
        MilestonePayload,
        Output,
        Payload,
    },
    MessageId,
};
use chronicle_storage::access::{
    AnalyticRecord,
    FullMessage,
    LedgerInclusionState,
    MessageCount,
    TransactionCount,
    TransferredTokens,
};

#[cfg(feature = "scylla-rs")]
use scylla_rs::cql::Rows;
use serde::{
    Deserialize,
    Serialize,
};
use std::{
    collections::HashMap,
    ops::Range,
    path::PathBuf,
};
use url::Url;

#[derive(Deserialize, Serialize)]
/// Defines a message to/from the Broker or its children
pub enum BrokerSocketMsg<T> {
    /// A message to/from the Broker
    ChronicleBroker(T),
}

#[derive(Deserialize, Serialize)]
/// It's the Interface of the broker app to dynamiclly configure the application during runtime
pub enum ChronicleBrokerThrough {
    /// Shutdown json to gracefully shutdown broker app
    Shutdown,
    /// Alter the topology of the broker app
    Topology(BrokerTopology),
    /// Exit the broker app
    ExitProgram,
}

/// Topology event
#[derive(Deserialize, Serialize, Debug, Clone)]
pub enum BrokerTopology {
    /// Add new MQTT Messages feed source
    AddMqttMessages(Url),
    /// Add new MQTT Messages Referenced feed source
    AddMqttMessagesReferenced(Url),
    /// Remove a MQTT Messages feed source
    RemoveMqttMessages(Url),
    /// Remove a MQTT Messages Referenced feed source
    RemoveMqttMessagesReferenced(Url),
    /// Import a log file using the given url
    Import {
        /// File or dir path which supposed to contain LogFiles
        path: PathBuf,
        /// Resume the importing process
        resume: bool,
        /// Provide optional import range
        import_range: Option<Range<u32>>,
        /// The type of import requested
        import_type: ImportType,
    },
    /// Add Endpoint
    Requesters(RequesterTopology),
}

/// Import types
#[derive(Deserialize, Serialize, Debug, Copy, Clone)]
pub enum ImportType {
    /// Import everything
    All,
    /// Import only Analytics data
    Analytics,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
/// Requester topology used by admins to add/remove IOTA api endpoints
pub enum RequesterTopology {
    /// Add new Api Endpoint
    AddEndpoint(Url),
    /// Remove existing Api Endpoint
    RemoveEndpoint(Url),
}
