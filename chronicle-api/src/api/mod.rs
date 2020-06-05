macro_rules! response {
    (body: $body:expr) => {
        hyper::Response::builder()
            .header("Content-Type", "application/json")
            .body(Body::from($body))
            .unwrap()
    };
    (status: $status:tt, body: $body:expr) => {
        hyper::Response::builder()
            .header("Content-Type", "application/json")
            .status(hyper::StatusCode::$status)
            .body(Body::from($body))
            .unwrap()
    };
}

pub mod endpoint;
pub mod findtransactions;
pub mod gettrytes;
pub mod router;
pub mod types;

use chronicle_common::app;
use endpoint::EndpointBuilder;
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
