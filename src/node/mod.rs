use crate::stage;
pub mod supervisor;
use crate::cluster::supervisor::Address;
use crate::stage::supervisor::ReporterNum;

pub use supervisor::{SupervisorBuilder};

#[test]
pub fn test() -> () {
    use tokio;
    use tokio::runtime::Runtime;    
    let rt = Runtime::new();
    rt.unwrap().block_on(async {
        let address: Address = String::from("172.17.0.2:9042");
        let reporters_num: u8 = 1;
        SupervisorBuilder::new()
            .address(address)
            .reporters_num(reporters_num)
            .build()
            .run()
            .await;
    });

    ()
}
