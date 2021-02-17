pub use async_trait::async_trait;
pub use chronicle::*;
use permanode_api::application::*;
use scylla::application::*;
use std::env;

launcher!(builder: AppsBuilder {[] -> Permanode<Sender>: PermanodeBuilder<Sender>, [Permanode] -> Scylla<Sender>: ScyllaBuilder<Sender>}, state: Apps {});

impl Builder for AppsBuilder {
    type State = Apps;

    fn build(self) -> Self::State {
        todo!()
    }
}

#[tokio::main]
async fn main() {
    env::set_var("RUST_LOG", "info");

    env_logger::init();

    let apps = AppsBuilder::new().build();

    apps.Scylla().await.Permanode().await.start(None).await;
}
