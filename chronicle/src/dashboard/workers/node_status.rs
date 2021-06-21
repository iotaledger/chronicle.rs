// Copyright 2020-2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use crate::dashboard::websocket::{
    broadcast,
    WsUsers,
};

use bee_runtime::shutdown_stream::ShutdownStream;
use futures::{
    stream::Fuse,
    StreamExt,
};
use log::info;
use serde::Serialize;
use tokio_stream::wrappers::IntervalStream;

use cap::Cap;
use std::alloc;

#[global_allocator]
pub static ALLOCATOR: Cap<alloc::System> = Cap::new(alloc::System, usize::max_value());

pub(crate) fn node_status_worker(mut ticker: ShutdownStream<Fuse<IntervalStream>>, users: &WsUsers) {
    let users = users.clone();

    let f = async move {
        info!("NodeStatus Worker Running");
        info!("Start to send fake data");

        while ticker.next().await.is_some() {
            let public_node_status = PublicNodeStatus {
                snapshot_index: 0,
                pruning_index: 0,
                is_healthy: true,
                is_synced: true,
            };
            broadcast(public_node_status.into(), &users).await;
        }

        info!("NodeStatus Worker stopped.");
    };

    let _ = tokio::spawn(f);
}

#[derive(Clone, Debug, Serialize)]
pub struct PublicNodeStatus {
    pub snapshot_index: u32,
    pub pruning_index: u32,
    pub is_healthy: bool,
    pub is_synced: bool,
}
