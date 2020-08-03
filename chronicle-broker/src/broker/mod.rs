pub mod mqtt;
pub mod supervisor;

use chronicle_common::app;
app!(BrokerBuilder { trytes: Vec<String>, conf_trytes: Vec<String> });

impl BrokerBuilder {
    pub fn build(self) -> Broker {
        let supervisor_builder = supervisor::SupervisorBuilder::new()
            .trytes(self.trytes)
            .conf_trytes(self.conf_trytes)
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
