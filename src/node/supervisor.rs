// node supervisor .. spawn stages // WIP
use super::stage;
use std::collections::HashMap;
use std::cmp::{Eq, PartialEq};
use evmap::shallow_copy::ShallowCopy;
use tokio;
use tokio::sync::mpsc;

// types
type Stages = HashMap<u8, stage::supervisor::Sender>;
pub type Sender = mpsc::UnboundedSender<Event>;
pub type Receiver = mpsc::UnboundedReceiver<Event>;
type Registry = evmap::WriteHandle<u8, RegistryReporter>;

#[derive(Clone)]
pub struct RegistryReporter {
    id: u8,
    tx: mpsc::UnboundedSender<stage::reporter::Event>,
}

impl PartialEq for RegistryReporter {
    fn eq(&self, other: &RegistryReporter) -> bool {
        self.id == other.id
    }
}

impl Eq for RegistryReporter {}

impl ShallowCopy for RegistryReporter {
    unsafe fn shallow_copy(&mut self) -> Self {
        self.clone()
    }
}

// event Enum
#[derive(Debug)]
pub enum Event {
    GetShardsNum,
    Shutdown,
    Expose(u8, stage::supervisor::Reporters),
}

// Arguments struct
pub struct SupervisorBuilder {
    address: Option<String>,
    reporters: u8,
    registry: Option<Registry>,
    // pub supervisor_tx:
}

impl SupervisorBuilder {
    pub fn new() -> Self {
        SupervisorBuilder {
            address: None,
            reporters: 1,
            registry: None,
        }
    }

    set_builder_option_field!(address, String);
    set_builder_field!(reporters, u8);
    set_builder_option_field!(registry, Registry);

    pub fn build(self) -> Supervisor {
        let (tx, rx) = mpsc::unbounded_channel::<Event>();
        let stages: Stages = HashMap::new();

        Supervisor {
            address: self.address.unwrap(),
            reporters: self.reporters,
            spawned: false,
            shard: 0,
            tx,
            rx,
            stages,
            registry: self.registry.unwrap(),
        }
    }
}

// suerpvisor state struct
pub struct Supervisor {
    address: String,
    reporters: u8,
    spawned: bool,
    tx: Sender,
    rx: Receiver,
    shard: u8,
    stages: Stages,
    registry: Registry,
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
                    self.shard = 1; // shard(shard-0)
                                         // ready to spawn stages
                    for shard in 0..self.shard {
                        let (stage_tx, stage_rx) =
                            mpsc::unbounded_channel::<stage::supervisor::Event>();
                        let stage = stage::supervisor::SupervisorBuilder::new()
                            .supervisor_tx(self.tx.clone())
                            .address(self.address.clone())
                            .shard(shard)
                            .reporters(self.reporters)
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
                Event::Expose(stage, reporters) => {
                    // now we have all stage's reporters, therefore we expose the node_reporters to cluster supervisor
                    for (id, tx) in reporters {
                        self.registry.insert(stage, RegistryReporter{id, tx});
                    }
                    self.registry.refresh();
                    //if self.shard == (self.node_reporters.len() as u8) {}
                }
            }
        }
    }
}

#[tokio::test]
async fn run_node() {
    let (_, register) = evmap::new();
    let address = String::from("0.0.0.0:9042");
    let reporters = 1;
    let node = SupervisorBuilder::new()
        .address(address)
        .reporters(reporters)
        .registry(register)
        .build();
    let tx = node.tx.clone();

    let node_exec = tokio::spawn(node.run());
    // Remove this line should make whole test stuck.
    tx.send(Event::Shutdown).unwrap();

    let (result,..) = tokio::join!(node_exec);
    result.unwrap();
}
