use crate::stage;
pub mod supervisor;
use crate::cluster::supervisor::Address;
use crate::stage::supervisor::ReporterNum;

pub async fn node(address: Address, reporters_num: ReporterNum) {
    // create node supervisor args
    let args = supervisor::Args{address, reporters_num};
    // now await on node supervisor
    supervisor::supervisor(args).await;
}


#[test]
pub fn test() -> () {
    use tokio;
    use tokio::runtime::Runtime;
    let mut rt = Runtime::new();
    rt.unwrap().block_on(async {
        let address: Address = String::from("172.17.0.2:9042");
        let reporters_num: u8 = 1;
        node(address, reporters_num).await;
    });

    ()
}
