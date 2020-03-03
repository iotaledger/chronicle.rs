// cluster supervisor WIP
use super::node;
use std::collections::HashMap;
use tokio::sync::mpsc;
//types
pub type Address = String;
type Nodes = HashMap<Address, node::supervisor::Sender>; // childern
pub type Sender = mpsc::UnboundedSender<Event>;
pub type Receiver = mpsc::UnboundedReceiver<Event>;
pub type NodesReporters = Vec<(Address, node::supervisor::NodeReporters)>;

enum Event {
    Topology(Topology)
    Status(Status),
    // Schema(..),
}

enum Topology {
    NewNode(Address),
    RemovedNode(Address),
}
enum Status {
    Up(Address),
    Down(Address),
}
// Arguments struct
pub struct SupervisorBuilder {
    address: Option<Address>,
    reporter_count: u8,
    // pub supervisor_tx:
}

impl SupervisorBuilder {
    pub fn new() -> Self {
        SupervisorBuilder {
            address: None,
            reporter_count: 1,
        }
    }

    set_builder_option_field!(address, Address);
    set_builder_field!(reporters, u8);

    pub fn build(self) -> Supervisor {
        let (tx, rx) = mpsc::unbounded_channel::<Event>();

        Supervisor {
            address: self.address.unwrap(),
            reporter_count: self.reporter_count,
            tx,
            rx,
        }
    }
}

// suerpvisor state struct
pub struct Supervisor {
    address: Address,
    reporters_count: u8,
    tx: Sender,
    rx: Receiver,
}

impl Supervisor {
    pub async fn run(mut self) {
        while let Some(event) = self.rx.recv().await {
            match event {
                Event::Topology(topology) => {
                    match topology {
                        Topology::NewNode => {
                            // TOPOLOGY_CHANGE event as a result of a new_node
                        }
                        Topology::RemovedNode => {
                            // TOPOLOGY_CHANGE event as a result of a removed_node
                        }
                    }
                }
                Event::Status(status) => {
                    match status {
                        Event::Up(address) => {
                            // STATUS_CHANGE event "UP" as a result of a node returned online
                        }
                        Event::Down(address) => {
                            // STATUS_CHANGE event "DOWN" as a result of a node went offline
                        }
                    }
                }

            }
        }
    }
}
