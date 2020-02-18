mod preparer;
mod receiver;
pub mod reporter;
mod sender;
pub mod supervisor;

use crate::node;
use supervisor::SupervisorBuilder;

pub async fn stage(
    supervisor_tx: node::supervisor::Sender,
    address: String,
    shard: u8,
    reporters_num: u8,
    tx: supervisor::Sender,
    rx: supervisor::Receiver,
) {
    // create stage supervisor args
    //let args = supervisor::Args{address,shard,reporters_num,tx,rx, supervisor_tx};
    // now await on stage
    //supervisor::supervisor(args).await;

    let _ = SupervisorBuilder::new()
        .address(address)
        .reporters_num(reporters_num)
        .shard(shard)
        .tx(tx)
        .rx(rx)
        .supervisor_tx(supervisor_tx)
        .build();
}
