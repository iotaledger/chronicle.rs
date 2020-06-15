pub mod supervisor;
pub mod zmq;

use chronicle_common::app;
app!(BrokerBuilder { trytes: Vec<String>, sn_trytes: Vec<String>, sn: Vec<String> });

impl BrokerBuilder {
    pub fn build(self) -> Broker {
        let supervisor_builder = supervisor::SupervisorBuilder::new()
            .trytes(self.trytes)
            .sn_trytes(self.sn_trytes)
            .sn(self.sn)
            .launcher_tx(self.launcher_tx.unwrap());
        Broker { supervisor_builder }
    }
}
pub struct Broker {
    supervisor_builder: supervisor::SupervisorBuilder,
}
impl Broker {
    pub async fn run(self) {
        // build and spawn supervisor
        tokio::spawn(self.supervisor_builder.build().run());
    }
}
