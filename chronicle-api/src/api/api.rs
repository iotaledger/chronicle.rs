use super::endpoint::EndpointBuilder;
use chronicle_common::app;
app!(ApiBuilder { listen_address: String });

impl ApiBuilder {
    pub fn build(self) -> Api {
        Api {
            listen_address: self.listen_address.unwrap(),
            launcher_tx: self.launcher_tx.unwrap(),
        }
    }
}

pub struct Api {
    listen_address: String,
    launcher_tx: Box<dyn LauncherTx>,
}

impl Api {
    pub async fn run(self) {
        let server = EndpointBuilder::new()
            .listen_address(self.listen_address)
            .launcher_tx(self.launcher_tx)
            .build();
        tokio::spawn(server.run());
    }
}
