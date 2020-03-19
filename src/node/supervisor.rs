// node supervisor .. spawn stages // WIP
use crate::ring::ring::DC;
use crate::ring::ring::NodeId;
use crate::cluster::supervisor;
use super::stage;
use std::collections::HashMap;
use tokio;
use tokio::sync::mpsc;

// types
type Stages = HashMap<u8, stage::supervisor::Sender>;
pub type Sender = mpsc::UnboundedSender<Event>;
pub type Receiver = mpsc::UnboundedReceiver<Event>;
pub type NodeRegistry = Vec<(NodeId, stage::supervisor::Reporters)>; // u8 is shard_id

// event Enum
#[derive(Debug)]
pub enum Event {
    Shutdown,
    RegisterReporters(u8, stage::supervisor::Reporters),
}

// Arguments struct
pub struct SupervisorBuilder {
    address: Option<String>,
    node_id: Option<NodeId>,
    data_center: Option<DC>,
    reporter_count: Option<u8>,
    shard_count: Option<u8>,
    supervisor_tx: Option<supervisor::Sender>,
}

impl SupervisorBuilder {
    pub fn new() -> Self {
        SupervisorBuilder {
            address: None,
            node_id: None,
            data_center: None,
            reporter_count: None,
            shard_count: None,
            supervisor_tx: None,
        }
    }

    set_builder_option_field!(address, String);
    set_builder_option_field!(node_id, NodeId);
    set_builder_option_field!(data_center, DC);
    set_builder_option_field!(reporter_count, u8);
    set_builder_option_field!(shard_count, u8);
    set_builder_option_field!(supervisor_tx, supervisor::Sender);

    pub fn build(self) -> Supervisor {
        let (tx, rx) = mpsc::unbounded_channel::<Event>();
        let stages: Stages = HashMap::new();
        let address: String = self.address.unwrap();
        let node_id: NodeId = gen_node_id(&address);
        Supervisor {
            address,
            node_id,
            data_center: self.data_center.unwrap(),
            reporter_count: self.reporter_count.unwrap(),
            spawned: false,
            shard_count: self.shard_count.unwrap(),
            tx: Some(tx),
            rx,
            stages,
            node_registry: Vec::new(),
            supervisor_tx: self.supervisor_tx.unwrap(),
        }
    }
}

// suerpvisor state struct
pub struct Supervisor {
    address: String,
    node_id: NodeId,
    data_center: DC,
    reporter_count: u8,
    spawned: bool,
    tx: Option<Sender>,
    rx: Receiver,
    shard_count: u8,
    stages: Stages,
    node_registry: NodeRegistry,
    supervisor_tx: supervisor::Sender,
}

impl Supervisor {
    pub fn clone_tx(&self) -> Sender {
        self.tx.as_ref().unwrap().clone()
    }
    pub async fn run(mut self) {
        // spawn stage supervisor for each shard_id
        for shard_id in 0..self.shard_count {
            let (stage_tx, stage_rx) =
                mpsc::unbounded_channel::<stage::supervisor::Event>();
            let stage = stage::supervisor::SupervisorBuilder::new()
                .node_tx(self.clone_tx())
                .address(self.address.clone())
                .shard_id(shard_id)
                .reporter_count(self.reporter_count)
                .tx(stage_tx.clone())
                .rx(stage_rx)
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
                    let mut node_id = self.node_id.clone();
                    // assign shard_id to node_id to use it later as key in registry
                    node_id[4] = shard_id;
                    self.node_registry.push((node_id, reporters));
                    // check if we pushed all reporters of the node.
                    if self.node_registry.len() as u8 == self.shard_count {
                        let node_registry = self.node_registry.to_owned();
                        self.node_registry.clear();
                        self.node_registry.shrink_to_fit();
                        // node_registry should be passed to cluster supervisor
                        let event = supervisor::Event::RegisterReporters(
                            node_registry
                        );
                        self.supervisor_tx.send(event).unwrap();
                    }
                }
            }
        }
    }
}

pub fn gen_node_id(address: &String) -> NodeId {
    let address_port: Vec<&str> = address.split(':').collect();
    let ipv4: Vec<&str> = address_port.first().unwrap().split('.').collect();
    let mut node_id = [0; 5];
    let mut i = 0;
    for ipv4_byte in ipv4 {
        node_id[i] = ipv4_byte.parse::<u8>().unwrap();
        i += 1;
    }
    node_id
}
