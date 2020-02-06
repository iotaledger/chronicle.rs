// cluster supervisor WIP
use std::collections::HashMap;
use tokio::sync::mpsc;
use super::node;
use crate::stage::supervisor::ReporterNum;
//types
pub type Address = String;
type Nodes = HashMap<Address, node::supervisor::Sender>; // childern
pub type Sender = mpsc::UnboundedSender<Event>;
pub type Receiver = mpsc::UnboundedReceiver<Event>;
pub type NodesReporters = Vec<(Address,node::supervisor::NodeReporters)>;

static mut S: u8 = 0;

// suerpvisor state struct
struct State {
    tx: Sender,
    rx: Receiver,
}

// Arguments struct
pub struct Args {
    pub address: Address,
    pub reporters_num: ReporterNum,
    // pub supervisor_tx:
}

enum Event {
    AddNode(Address),
    RemoveNode(Address),
}


pub async fn supervisor(args: Args) -> () {
    let State {tx, mut rx} = init(args).await;

    while let Some(event) = rx.recv().await {
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


async fn init(args: Args) -> State {
    // init the channel
    let (tx, rx) = mpsc::unbounded_channel::<Event>();
    let nodes: Nodes = HashMap::new();
    let nodes_reporters: NodesReporters = Vec::new();
    State {tx, rx}
}
