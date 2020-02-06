// node supervisor .. spawn stages // WIP
use crate::stage::supervisor::ReporterNum;
use crate::cluster::supervisor::Address;
use std::collections::HashMap;
use super::stage;
use tokio::sync::mpsc;
use tokio;

// types
pub type StageNum = u8;
type Stages = HashMap<u8, stage::supervisor::Sender>; // childern
pub type Sender = mpsc::UnboundedSender<Event>;
pub type Receiver = mpsc::UnboundedReceiver<Event>;
pub type NodeReporters = Vec<(StageNum, stage::supervisor::Reporters)>;

// suerpvisor state struct
struct State {
    args: Args,
    spawned: bool,
    tx: Sender,
    rx: Receiver,
    shards_num: u8,
    stages: Stages,
    node_reporters: NodeReporters,
}

// Arguments struct
pub struct Args {
    pub address: Address,
    pub reporters_num: ReporterNum,
    // pub supervisor_tx:
}


// event Enum
#[derive(Debug)]
pub enum Event {
    GetShardsNum,
    Shutdown,
    Expose(u8, stage::supervisor::Reporters),
}


pub async fn supervisor(args: Args) -> () {
    let State{mut rx,tx,mut spawned, mut shards_num, mut stages, args, mut node_reporters} = init(args).await;
    // send self GetShardsNum
    tx.send(Event::GetShardsNum);
    // event loop
    while let Some(event) = rx.recv().await {
        match event {
            Event::GetShardsNum => {
                // TODO connect to scylla-shard-zero and get_cql_opt to finally get the shards_num
                // for testing purposes, we will manually define it.
                shards_num = 1; // shard(shard-0)
                // ready to spawn stages
                for shard in 0..shards_num {
                    let (stage_tx, stage_rx) = mpsc::unbounded_channel::<stage::supervisor::Event>();
                    let stage = stage::stage(tx.clone(),args.address.clone(), shard, args.reporters_num.clone(), stage_tx.clone(), stage_rx);
                    tokio::spawn(stage);
                    stages.insert(shard, stage_tx);
                }
                spawned = true;
            }
            Event::Shutdown => {
                if spawned {
                    // this mean the stages are still running, therefore we send shutdown events
                    for (_,stage) in stages.drain() {
                        let event = stage::supervisor::Event::Shutdown;
                        stage.send(event);
                    }
                }
                rx.close();
            }
            Event::Expose(stage_num, reporters) => {
                node_reporters.push((stage_num, reporters));
                if shards_num == (node_reporters.len() as u8) {
                    // now we have all stage's reporters, therefore we expose the node_reporters to cluster supervisor

                }
            }
        }
    }


}

async fn init(args: Args) -> State {
    // init the channel
    let (tx, rx) = mpsc::unbounded_channel::<Event>();
    let stages: Stages = HashMap::new();
    let node_reporters: NodeReporters = Vec::new();
    // return state
    State {tx,rx,stages,shards_num: 0, spawned: false, args, node_reporters}
}
