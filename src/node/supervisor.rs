// node supervisor .. spawn stages // WIP
use crate::ring::ring::DC;
use crate::ring::ring::NodeId;
//use crate::cluster::supervisor;
use super::stage;
use std::collections::HashMap;
use tokio;
use tokio::sync::mpsc;

// types
type Stages = HashMap<u8, stage::supervisor::Sender>;
pub type Sender = mpsc::UnboundedSender<Event>;
pub type Receiver = mpsc::UnboundedReceiver<Event>;
pub type NodeReporters = Vec<(NodeId, stage::supervisor::Reporters)>; // u8 is shard_id

// event Enum
#[derive(Debug)]
pub enum Event {
    GetShardsNum,
    Shutdown,
    RegisterReporters(u8, stage::supervisor::Reporters),
}

// Arguments struct
pub struct SupervisorBuilder {
    address: Option<String>,
    node_id: Option<NodeId>,
    data_center: Option<DC>,
    reporter_count: Option<u8>,
    node_reporters: Option<NodeReporters>,
//    supervisor_tx: Option<supervisor::Sender>,
}

impl SupervisorBuilder {
    pub fn new() -> Self {
        SupervisorBuilder {
            address: None,
            node_id: None,
            data_center: None,
            reporter_count: None,
            node_reporters: None,
            //supervisor_tx: None,
        }
    }

    set_builder_option_field!(address, String);
    set_builder_option_field!(node_id, NodeId);
    set_builder_option_field!(data_center, DC);
    set_builder_option_field!(reporter_count, u8);
    set_builder_option_field!(node_reporters, NodeReporters);
//    set_builder_option_field!(supervisor_tx, supervisor::Sender);

    pub fn build(self) -> Supervisor {
        let (tx, rx) = mpsc::unbounded_channel::<Event>();
        let stages: Stages = HashMap::new();
        let address: String = self.address.unwrap();
        let node_id: NodeId = gen_node_id(address.clone());
        Supervisor {
            address,
            node_id,
            data_center: self.data_center.unwrap(),
            reporter_count: self.reporter_count.unwrap(),
            spawned: false,
            shard_count: 0,
            tx,
            rx,
            stages,
            node_reporters: self.node_reporters.unwrap(),
    //        supervisor_tx: self.supervisor_tx.unwrap(),
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
    tx: Sender,
    rx: Receiver,
    shard_count: u8,
    stages: Stages,
    node_reporters: NodeReporters,
//    supervisor_tx: supervisor::Sender,
}

impl Supervisor {
    pub async fn run(mut self) {
        // Send self GetShardsNum
        self.tx.send(Event::GetShardsNum).unwrap();

        // Event loop
        while let Some(event) = self.rx.recv().await {
            match event {
                Event::GetShardsNum => {
                    // TODO connect to scylla-shard-zero and get_cql_opt to finally get the shards_num
                    // for testing purposes, we will manually define it.
                    self.shard_count = 1; // shard(shard-0)
                                          // ready to spawn stages
                    for shard_id in 0..self.shard_count {
                        let (stage_tx, stage_rx) =
                            mpsc::unbounded_channel::<stage::supervisor::Event>();
                        let stage = stage::supervisor::SupervisorBuilder::new()
                            .node_tx(self.tx.clone())
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
                }
                Event::Shutdown => {
                    if self.spawned {
                        // this mean the stages are still running, therefore we send shutdown events
                        for (_, stage) in self.stages.drain() {
                            let event = stage::supervisor::Event::Shutdown;
                            stage.send(event).unwrap();
                        }
                    }
                    break; // we break to prevent pushing RegisterReporters to cluster,
                    // because it asked node to shutdown and doesn't expect it to push anything further
                }
                Event::RegisterReporters(shard_id, reporters) => {
                    // TODO.. collect them and make sure the node_reporters len == shard_count,
                    // only then we push it to cluster to use them to build the RING and
                    // expose it to the public
                    // push the reporters that belong to a given shard_id(stage_num)
                    let mut node_id = self.node_id.clone();
                    // assign shard_id to node_id to use it later as key in registry
                    node_id[4] = shard_id;
                    self.node_reporters.push((node_id, reporters));
                    // check if we pushed all reporters of the node.
                    if self.node_reporters.len() as u8 == self.shard_count {
                        let node_registry = self.node_reporters.to_owned();
                        self.node_reporters.clear();
                        self.node_reporters.shrink_to_fit();
                        // reporters_registry should be passed to cluster supervisor
//                        let event = supervisor::Event::RegisterReporters(self.data_center,node_id, node_registry);
//                        self.supervisor_tx.send(event).unwrap();
                    }
                }
            }
        }
    }
}

fn gen_node_id(address: String) -> NodeId {
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
#[tokio::test]
async fn run_node() {
    let node_reporters = Vec::new();
    let address = String::from("0.0.0.0:9042");
    let reporter_count = 1;
    let node = SupervisorBuilder::new()
        .address(address)
        .reporter_count(reporter_count)
        .node_reporters(node_reporters)
        .data_center("US")
        .build();
    let tx = node.tx.clone();

    let node_exec = tokio::spawn(node.run());
    // Remove this line should make whole test stuck.
    tx.send(Event::Shutdown).unwrap();

    node_exec.await.unwrap();
}
