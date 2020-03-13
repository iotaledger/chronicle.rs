// cluster supervisor
use crate::connection::cql::{fetch_tokens,connect};
use std::sync::Arc;
use crate::ring::ring::{
    DC,
    NodeId,
    Registry,
    Token,
    Msb,
    ShardCount,
    GlobalRing,
    build_ring};
use super::node;
use std::collections::HashMap;
use tokio::sync::mpsc;
use crate::node::supervisor::gen_node_id;
//types
pub type Sender = mpsc::UnboundedSender<Event>;
pub type Receiver = mpsc::UnboundedReceiver<Event>;
pub type Tokens = Vec<(Token,NodeId, DC, Msb, ShardCount)>;
pub type Address = String;
pub type Nodes = HashMap<Address, NodeInfo>;

pub struct NodeInfo {
    node_tx: node::supervisor::Sender,
    pub tokens: Tokens,
    node_id: NodeId,
    shard_count: ShardCount,
    data_center: DC,
}

#[derive(Debug)]
pub enum Event {
    RegisterReporters(node::supervisor::NodeRegistry),
    SpawnNode(DC, Address),
    ShutDownNode(DC, Address),
    TryBuild,
}

// Arguments struct
pub struct SupervisorBuilder {
    reporter_count: Option<u8>,
}

impl SupervisorBuilder {
    pub fn new() -> Self {
        SupervisorBuilder {
            reporter_count: None,
        }
    }

    set_builder_option_field!(reporter_count, u8);

    pub fn build(self) -> Supervisor {
        let (tx, rx) = mpsc::unbounded_channel::<Event>();
        Supervisor {
            reporter_count: self.reporter_count.unwrap(),
            registry: HashMap::new(),
            arc_ring: None,
            nodes: HashMap::new(),
            ready: 0,
            tx,
            rx,
        }
    }
}

// suerpvisor state struct
pub struct Supervisor {
    reporter_count: u8,
    registry: Registry,
    arc_ring: Option<Arc<GlobalRing>>,
    nodes: Nodes,
    ready: u8,
    tx: Sender,
    rx: Receiver,
}

impl Supervisor {
    pub async fn run(mut self) {
        while let Some(event) = self.rx.recv().await {
            match event {
                Event::SpawnNode(dc, address) => {
                    match fetch_tokens(
                        connect(&address).await
                    ).await {
                        Ok(mut cqlconn) => {
                            let shard_count = cqlconn.get_shard_count();
                            let tokens = cqlconn.take_tokens();
                            let node = node::SupervisorBuilder::new()
                                .address(address.clone())
                                .reporter_count(self.reporter_count)
                                .shard_count(shard_count)
                                .data_center(dc)
                                .supervisor_tx(self.tx.clone())
                                .build();
                            let node_tx = node.tx();
                            let node_id = gen_node_id(&address);
                            // generate nodeinfo
                            let node_info = NodeInfo{
                                data_center: dc,
                                node_id,
                                shard_count,
                                node_tx,
                                tokens};
                            // add node_info to nodes
                            self.nodes.insert(address, node_info);
                            // increase ready and only decrease it on RegisterReporters events
                            self.ready += 1;
                            // spawn node,
                            tokio::spawn(node.run());
                            // todo reply to ring supervisor

                        },
                        err => {
                            // todo reply to ring supervisor with unable to reach
                        },
                    };
                }
                Event::ShutDownNode(_, address) => {
                    // get and remove node_info
                    let mut node_info =  self.nodes.remove(&address).unwrap();
                    // update(remove from) registry
                    for shard_id in 0..node_info.shard_count {
                        // make node_id to reflect the correct shard_id
                        node_info.node_id[4] = shard_id;
                        // remove the shard_reporters for "address" node in shard_id from registry
                        self.registry.remove(&node_info.node_id);
                    }
                }
                Event::RegisterReporters(node_registry) => {
                    // decrease the ready counter
                    self.ready -= 1;
                    // merge the node_registry with self.registry
                    for (node_id, stage_reporters) in node_registry {
                        self.registry.insert(node_id, stage_reporters);
                    }
                }
                Event::TryBuild => {
                    if self.ready == 0 {
                        // ready to build
                        // NOTE the global_ring must be initialized state
                        // re/build
                        let version = 1; // todo generate version
                        let new_arc_ring = build_ring(&self.nodes, self.registry.clone(), version);
                        // replace self.arc_ring
                        self.arc_ring.replace(new_arc_ring);
                        // reply to ring-supervisor
                    } else {
                        // reply to ring-suerpvisor
                        // not ready to build
                    }
                }
            }
        }
    }
}
