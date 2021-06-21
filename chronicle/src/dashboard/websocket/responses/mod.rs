// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use crate::dashboard::websocket::{
    responses::{
        database_size_metrics::DatabaseSizeMetricsResponse,
        public_node_status::PublicNodeStatusResponse,
        sync_status::SyncStatusResponse,
    },
    topics::WsTopic,
};

use serde::Serialize;

pub(crate) mod database_size_metrics;
pub(crate) mod public_node_status;
pub(crate) mod sync_status;

#[derive(Clone, Debug, Serialize)]
pub(crate) struct WsEvent {
    #[serde(rename = "type")]
    pub(crate) kind: WsTopic,
    #[serde(rename = "data")]
    pub(crate) inner: WsEventInner,
}

impl WsEvent {
    pub(crate) fn new(kind: WsTopic, inner: WsEventInner) -> Self {
        Self { kind, inner }
    }
}

// TODO: Define the events of frontend update
#[derive(Clone, Debug, Serialize)]
#[serde(untagged)]
pub(crate) enum WsEventInner {
    SyncStatus(SyncStatusResponse),
    DatabaseSizeMetrics(DatabaseSizeMetricsResponse),
    PublicNodeStatus(PublicNodeStatusResponse),
}
