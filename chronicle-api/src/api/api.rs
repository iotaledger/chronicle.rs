use chronicle_common::app;
use tokio::sync::mpsc;
use super::endpoint::EndpointBuilder;
app!(ApiBuilder { listen_address: String });

impl ApiBuilder {
    pub fn build(self) -> Api {
        Api {
            listen_address: self.listen_address.unwrap(),
        }
    }
}

pub struct Api {
    listen_address: String,
}

impl Api {
    pub async fn run(self) {
        let server = EndpointBuilder::new()
        .listen_address(self.listen_address)
        .build();
        tokio::spawn(server.run());
    }
}
