// cluster supervisor
use super::node;
use crate::connection::cql::{connect, fetch_tokens};
use crate::dashboard::dashboard;
use crate::node::supervisor::gen_node_id;
use crate::ring::ring::{
    build_ring, initialize_ring, ArcRing, AtomicRing, GlobalRing, Msb, NodeId, Registry,
    ShardCount, Token, WeakRing, DC,
};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::time::delay_for;
//types
pub type Sender = mpsc::UnboundedSender<Event>;
pub type Receiver = mpsc::UnboundedReceiver<Event>;
pub type Tokens = Vec<(Token, NodeId, DC, Msb, ShardCount)>;
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
    RegisterReporters(node::supervisor::NodeRegistry, Address),
    SpawnNode(Address),
    ShutDownNode(Address),
    TryBuild,
}

actor!(SupervisorBuilder {
    reporter_count: u8,
    thread_count: usize,
    buffer_size: usize,
    recv_buffer_size: Option<usize>,
    send_buffer_size: Option<usize>,
    dashboard_tx: dashboard::Sender
});

impl SupervisorBuilder {
    pub fn build(self) -> Supervisor {
        let (tx, rx) = mpsc::unbounded_channel::<Event>();
        // initialize global_ring
        let arc_ring = initialize_ring();
        Supervisor {
            reporter_count: self.reporter_count.unwrap(),
            thread_count: self.thread_count.unwrap(),
            buffer_size: self.buffer_size.unwrap(),
            recv_buffer_size: self.recv_buffer_size.unwrap(),
            send_buffer_size: self.send_buffer_size.unwrap(),
            dashboard_tx: self.dashboard_tx.unwrap(),
            registry: HashMap::new(),
            arc_ring: Some(arc_ring),
            weak_rings: Vec::new(),
            nodes: HashMap::new(),
            ready: 0,
            build: false,
            version: 0,
            tx,
            rx,
        }
    }
}

// suerpvisor state struct
pub struct Supervisor {
    reporter_count: u8,
    thread_count: usize,
    buffer_size: usize,
    recv_buffer_size: Option<usize>,
    send_buffer_size: Option<usize>,
    dashboard_tx: dashboard::Sender,
    registry: Registry,
    arc_ring: Option<ArcRing>,
    weak_rings: Vec<Box<WeakRing>>,
    nodes: Nodes,
    ready: u8,
    build: bool,
    version: u8,
    tx: Sender,
    rx: Receiver,
}

impl Supervisor {
    pub async fn run(mut self) {
        while let Some(event) = self.rx.recv().await {
            match event {
                Event::SpawnNode(address) => {
                    match fetch_tokens(connect(&address, None, None).await).await {
                        Ok(mut cqlconn) => {
                            let shard_count = cqlconn.get_shard_count();
                            let tokens = cqlconn.take_tokens();
                            let dc = cqlconn.take_dc();
                            let node = node::SupervisorBuilder::new()
                                .address(address.clone())
                                .reporter_count(self.reporter_count)
                                .shard_count(shard_count)
                                .data_center(dc.clone())
                                .supervisor_tx(self.tx.clone())
                                .buffer_size(self.buffer_size)
                                .recv_buffer_size(self.recv_buffer_size)
                                .send_buffer_size(self.send_buffer_size)
                                .build();
                            let node_tx = node.clone_tx();
                            let node_id = gen_node_id(&address);
                            // generate nodeinfo
                            let node_info = NodeInfo {
                                data_center: dc,
                                node_id,
                                shard_count,
                                node_tx,
                                tokens,
                            };
                            // add node_info to nodes
                            self.nodes.insert(address, node_info);
                            // increase ready and only decrease it on RegisterReporters events
                            self.ready += 1;
                            // spawn node,
                            tokio::spawn(node.run());
                        }
                        _ => {
                            let event = dashboard::Event::Result(dashboard::Result::Err(address));
                            self.dashboard_tx.send(event);
                        }
                    };
                }
                Event::ShutDownNode(address) => {
                    // get and remove node_info
                    let mut node_info = self.nodes.remove(&address).unwrap();
                    // update(remove from) registry
                    for shard_id in 0..node_info.shard_count {
                        // make node_id to reflect the correct shard_id
                        node_info.node_id[4] = shard_id;
                        // remove the shard_reporters for "address" node in shard_id from registry
                        self.registry.remove(&node_info.node_id);
                    }
                    // send shutdown event to node
                    node_info
                        .node_tx
                        .send(node::supervisor::Event::Shutdown)
                        .unwrap();
                    // update waiting for build to true
                    self.build = true;
                    // note: the node tree will not get shutdown unless we drop the ring
                    // but we cannot drop the ring unless we build a new one and atomically swap it,
                    // therefore dashboard admin supposed to trybuild
                }
                Event::RegisterReporters(node_registry, address) => {
                    // decrease the ready counter
                    self.ready -= 1;
                    // update waiting for build to true
                    self.build = true;
                    // merge the node_registry with self.registry
                    for (node_id, stage_reporters) in node_registry {
                        self.registry.insert(node_id, stage_reporters);
                    }
                    // tell dashboard
                    let event = dashboard::Event::Result(dashboard::Result::Ok(address));
                    self.dashboard_tx.send(event);
                }
                Event::TryBuild => {
                    // do cleanup on weaks
                    self.cleanup();
                    if self.ready == 0 && self.build {
                        // re/build
                        let version = self.new_version();
                        let (new_arc_ring, old_weak_ring) = build_ring(
                            &self.nodes,
                            self.registry.clone(),
                            self.reporter_count,
                            version,
                        );
                        // replace self.arc_ring
                        self.arc_ring.replace(new_arc_ring);
                        // push weak to weak_rings
                        self.weak_rings.push(old_weak_ring);
                        // reset build state to false becaue we built it and we don't want to rebuild again incase of another TryBuild event
                        self.build = false;
                        // reply to dashboard
                        let event = dashboard::Event::Result(dashboard::Result::TryBuild(true));
                        self.dashboard_tx.send(event);
                    } else {
                        let event = dashboard::Event::Result(dashboard::Result::TryBuild(false));
                        self.dashboard_tx.send(event);
                    }
                }
            }
        }
    }
    fn cleanup(&mut self) {
        // total_weak_count = thread_count + 1(the global weak)
        // so we clear all old weaks once weak_count > self.thread_count
        if Arc::weak_count(self.arc_ring.as_ref().unwrap()) > self.thread_count {
            self.weak_rings.clear();
        };
    }
    pub fn clone_tx(&self) -> Sender {
        self.tx.clone()
    }
    fn new_version(&mut self) -> u8 {
        let mut m: u8 = 1;
        while (self.version & m) != 0 {
            self.version ^= m;
            m <<= 1 as u8;
        }
        self.version ^= m;
        self.version
    }
}

#[cfg(test)]
mod tests {
    // Note this useful idiom: importing names from outer (for mod tests) scope.
    use super::*;

    #[tokio::test]
    async fn test_build_supervisor() {
        let (mut dashboard_tx, dashboard_rx) = mpsc::unbounded_channel::<dashboard::Event>();
        let cluster = self::SupervisorBuilder::new()
            .reporter_count(1)
            .thread_count(1)
            .buffer_size(1024000)
            .recv_buffer_size(None)
            .send_buffer_size(None)
            .dashboard_tx(dashboard_tx)
            .build();
        let cluster_tx = cluster.clone_tx();

        if let Sender = &cluster_tx {
            //    tokio::task::spawn(cluster.run());
            cluster_tx.send(Event::TryBuild);
            tokio::task::yield_now().await;
        } else {
            assert!(false, "Cluster tx clone fail.");
        }
    }
}
