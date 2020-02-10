// cluster supervisor WIP
use super::node;
use crate::stage::supervisor::ReporterNum;
use std::collections::HashMap;
use tokio::sync::mpsc;
//types
pub type Address = String;
type Nodes = HashMap<Address, node::supervisor::Sender>; // childern
pub type Sender = mpsc::UnboundedSender<Event>;
pub type Receiver = mpsc::UnboundedReceiver<Event>;
pub type NodesReporters = Vec<(Address, node::supervisor::NodeReporters)>;

static mut S: u8 = 0;

enum Event {
    AddNode(Address),
    RemoveNode(Address),
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

        Supervisor {
            address: self.address.unwrap(),
            reporters_num: self.reporters_num.unwrap(),
            tx,
            rx,
        }
    }
}

// suerpvisor state struct
pub struct Supervisor {
    address: Address,
    reporters_num: ReporterNum,
    tx: Sender,
    rx: Receiver,
}

impl Supervisor {
    pub async fn run(mut self) {
        while let Some(event) = self.rx.recv().await {
            match event {
                Event::AddNode(address) => { // TODO
                     // this is a change toplogy event.
                     // first exposing the node to registry (ex evmap for now)
                     // then rebuilding the global ring to make it accessable through shard-awareness lookup.
                }
                _ => {} // TODO
            }
        }
    }
}
