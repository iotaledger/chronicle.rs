use chronicle::node::supervisor::{Event, SupervisorBuilder};
use evmap;

#[tokio::test]
async fn simple_broker() {
    let (registry_read, registry_write) = evmap::new();
    let address = String::from("0.0.0.0:9042");
    let reporters = 1;
    let node = SupervisorBuilder::new()
        .address(address)
        .reporters(reporters)
        .registry(registry_write)
        .build();
    let tx = node.tx.clone();
    let node_exec = tokio::spawn(node.run());

    tokio::time::delay_for(tokio::time::Duration::new(1, 0)).await;
    registry_read.get_and(&1, |_|{
        // TODO broker logic
    });

    // Remove this line should make whole test stuck.
    tx.send(Event::Shutdown).unwrap();

    let (result,..) = tokio::join!(node_exec);
    result.unwrap();
}