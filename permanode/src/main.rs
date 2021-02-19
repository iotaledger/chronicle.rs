pub use async_trait::async_trait;
pub use chronicle::*;
use permanode_api::application::*;
use scylla::application::*;

launcher!(builder: AppsBuilder {[] -> Permanode<Sender>: PermanodeBuilder<Sender>, [Permanode] -> Scylla<Sender>: ScyllaBuilder<Sender>}, state: Apps {});

impl Builder for AppsBuilder {
    type State = Apps;

    fn build(self) -> Self::State {
        let permanode_builder = PermanodeBuilder::new();
        let scylla_builder = ScyllaBuilder::new()
            .listen_address("127.0.0.1:8080".to_owned())
            .thread_count(num_cpus::get())
            .reporter_count(2)
            .local_dc("datacenter1".to_owned());

        self.Permanode(permanode_builder).Scylla(scylla_builder).to_apps()
    }
}

#[tokio::main]
async fn main() {
    dotenv::dotenv().unwrap();
    env_logger::init();

    let apps = AppsBuilder::new().build();

    apps.Scylla().await.Permanode().await.start(None).await;
}
