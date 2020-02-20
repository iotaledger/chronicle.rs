// node supervisor .. spawn stages // WIP
use super::stage;
use crate::cluster::supervisor::Address;
use crate::stage::supervisor::ReporterNum;
use std::collections::HashMap;
use tokio;
use tokio::sync::mpsc;

// types
pub type StageNum = u8;
type Stages = HashMap<u8, stage::supervisor::Sender>; // childern
pub type Sender = mpsc::UnboundedSender<Event>;
pub type Receiver = mpsc::UnboundedReceiver<Event>;
pub type NodeReporters = Vec<(StageNum, stage::supervisor::Reporters)>;

// event Enum
#[derive(Debug)]
pub enum Event {
    GetShardsNum,
    Shutdown,
    Expose(u8, stage::supervisor::Reporters),
}

// Arguments struct
pub struct SupervisorBuilder {
    address: Option<Address>,
    reporters_num: Option<ReporterNum>,
    // pub supervisor_tx:
}

impl SupervisorBuilder {
    pub fn new() -> Self {
        SupervisorBuilder {
            address: None,
            reporters_num: None,
        }
    }

    set_builder_option_field!(address, Address);
    set_builder_option_field!(reporters_num, ReporterNum);

    pub fn build(self) -> Supervisor {
        let (tx, rx) = mpsc::unbounded_channel::<Event>();
        let stages: Stages = HashMap::new();
        let node_reporters: NodeReporters = Vec::new();

        Supervisor {
            address: self.address.unwrap(),
            reporters_num: self.reporters_num.unwrap(),
            spawned: false,
            shards_num: 0,
            tx,
            rx,
            stages,
            node_reporters,
        }
    }
}

// suerpvisor state struct
pub struct Supervisor {
    address: Address,
    reporters_num: ReporterNum,
    spawned: bool,
    tx: Sender,
    rx: Receiver,
    shards_num: u8,
    stages: Stages,
    node_reporters: NodeReporters,
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
                    self.shards_num = 1; // shard(shard-0)
                                         // ready to spawn stages
                    for shard in 0..self.shards_num {
                        let (stage_tx, stage_rx) =
                            mpsc::unbounded_channel::<stage::supervisor::Event>();
                        let stage = stage::supervisor::SupervisorBuilder::new()
                            .supervisor_tx(self.tx.clone())
                            .address(self.address.clone())
                            .shard(shard)
                            .reporters_num(self.reporters_num)
                            .tx(stage_tx.clone())
                            .rx(stage_rx)
                            .build();

                        tokio::spawn(stage.run());
                        self.stages.insert(shard, stage_tx);
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
                    self.rx.close();
                }
                Event::Expose(stage_num, reporters) => {
                    self.node_reporters.push((stage_num, reporters));
                    if self.shards_num == (self.node_reporters.len() as u8) {
                        // now we have all stage's reporters, therefore we expose the node_reporters to cluster supervisor
                    }
                }
            }
        }
    }
}

#[tokio::test]
#[ignore]
async fn run_node() {
    use crate::cluster::supervisor::Address;

    let address: Address = String::from("172.17.0.2:9042");
    let reporters_num: u8 = 1;
    SupervisorBuilder::new()
        .address(address)
        .reporters_num(reporters_num)
        .build()
        .run()
        .await;
}
