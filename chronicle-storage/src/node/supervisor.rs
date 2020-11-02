// Copyright 2020 IOTA Stiftung
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and limitations under the License.

//! The supervisor of each ScyllaDB node.

use super::stage;
use crate::{
    cluster::supervisor,
    ring::{NodeId, DC},
};
use chronicle_common::actor;
use chronicle_cql::frame::auth_response::PasswordAuth;
use std::collections::HashMap;
use tokio::{self, sync::mpsc};

// types
type Stages = HashMap<u8, stage::supervisor::Sender>;
/// The sender of node events.
pub type Sender = mpsc::UnboundedSender<Event>;
/// The receiver of node events.
pub type Receiver = mpsc::UnboundedReceiver<Event>;
/// The nodes registration vector to register each node_id (shard_id) in its stage.
pub type NodeRegistry = Vec<(NodeId, stage::supervisor::Reporters)>; // u8 is shard_id

#[derive(Debug)]
/// Node event enum.
pub enum Event {
    /// Shutdown the node.
    Shutdown,
    /// Register the node in its corresponding stage.
    RegisterReporters(u8, stage::supervisor::Reporters),
}

actor!(SupervisorBuilder {
    address: String,
    node_id: NodeId,
    data_center: DC,
    reporter_count: u8,
    shard_count: u8,
    supervisor_tx: supervisor::Sender,
    buffer_size: usize,
    recv_buffer_size: Option<usize>,
    send_buffer_size: Option<usize>,
    authenticator: Option<PasswordAuth>
});

impl SupervisorBuilder {
    /// Build a node supervisor.
    pub fn build(self) -> Supervisor {
        let (tx, rx) = mpsc::unbounded_channel::<Event>();
        let stages: Stages = HashMap::new();
        let address: String = self.address.unwrap();
        let node_id: NodeId = gen_node_id(&address);
        Supervisor {
            address,
            node_id,
            reporter_count: self.reporter_count.unwrap(),
            spawned: false,
            shard_count: self.shard_count.unwrap(),
            tx: Some(tx),
            rx,
            stages,
            node_registry: Vec::new(),
            supervisor_tx: self.supervisor_tx.unwrap(),
            buffer_size: self.buffer_size.unwrap(),
            recv_buffer_size: self.recv_buffer_size.unwrap(),
            send_buffer_size: self.send_buffer_size.unwrap(),
            authenticator: self.authenticator.unwrap(),
        }
    }
}

/// Suerpvisor state structure.
pub struct Supervisor {
    address: String,
    node_id: NodeId,
    reporter_count: u8,
    spawned: bool,
    tx: Option<Sender>,
    rx: Receiver,
    shard_count: u8,
    stages: Stages,
    node_registry: NodeRegistry,
    supervisor_tx: supervisor::Sender,
    buffer_size: usize,
    recv_buffer_size: Option<usize>,
    send_buffer_size: Option<usize>,
    authenticator: Option<PasswordAuth>,
}

impl Supervisor {
    /// Clone the transmission channel of the node.
    pub fn clone_tx(&self) -> Sender {
        self.tx.as_ref().unwrap().clone()
    }
    /// Run the node event loop.
    pub async fn run(mut self) {
        // spawn stage supervisor for each shard_id
        for shard_id in 0..self.shard_count {
            let (stage_tx, stage_rx) = mpsc::unbounded_channel::<stage::supervisor::Event>();
            let stage = stage::supervisor::SupervisorBuilder::new()
                .node_tx(self.clone_tx())
                .address(self.address.clone())
                .shard_id(shard_id)
                .reporter_count(self.reporter_count)
                .tx(stage_tx.clone())
                .rx(stage_rx)
                .buffer_size(self.buffer_size)
                .recv_buffer_size(self.recv_buffer_size)
                .send_buffer_size(self.send_buffer_size)
                .authenticator(self.authenticator.clone())
                .build();
            tokio::spawn(stage.run());
            self.stages.insert(shard_id, stage_tx);
        }
        self.spawned = true;
        // Event loop
        while let Some(event) = self.rx.recv().await {
            match event {
                Event::Shutdown => {
                    if self.spawned {
                        // this mean the stages are still running, therefore we send shutdown events
                        for (_, stage) in self.stages.drain() {
                            let event = stage::supervisor::Event::Shutdown;
                            stage.send(event).unwrap();
                        }
                    }
                    // set self.tx to None
                    self.tx = None;
                    // contract design:
                    // the node supervisor will only shutdown when stages drop node_txs
                    // and this will only happen if reporters dropped stage_txs,
                    // still reporters_txs have to go out of the scope (from ring&stages).
                }
                Event::RegisterReporters(shard_id, reporters) => {
                    // collect them and make sure the node_reporters len == shard_count,
                    // only then we push it to cluster to use them to build the RING and
                    // expose it to the public
                    // push the reporters that belong to a given shard_id(stage_num)
                    let mut node_id = self.node_id;
                    // assign shard_id to node_id to use it later as key in registry
                    node_id[4] = shard_id;
                    self.node_registry.push((node_id, reporters));
                    // check if we pushed all reporters of the node.
                    if self.node_registry.len() as u8 == self.shard_count {
                        let node_registry = self.node_registry.to_owned();
                        self.node_registry.clear();
                        self.node_registry.shrink_to_fit();
                        // node_registry should be passed to cluster supervisor
                        let event = supervisor::Event::RegisterReporters(node_registry, self.address.clone());
                        self.supervisor_tx.send(event).unwrap();
                    }
                }
            }
        }
    }
}

/// The helper function of transforming the address string to `NodeID`.
pub fn gen_node_id(address: &str) -> NodeId {
    let address_port: Vec<&str> = address.split(':').collect();
    let ipv4: Vec<&str> = address_port.first().unwrap().split('.').collect();
    let mut node_id = [0; 5];
    for (i, ipv4_byte) in ipv4.into_iter().enumerate() {
        node_id[i] = ipv4_byte.parse::<u8>().unwrap();
    }
    node_id
}
