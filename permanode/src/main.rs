use std::path::Path;

pub use async_trait::async_trait;
pub use chronicle::*;
use permanode_api::application::*;
use permanode_storage::config::Config;
use scylla::application::*;

launcher!(builder: AppsBuilder {[] -> PermanodeAPI<Sender>: PermanodeAPIBuilder<Sender>, [Permanode] -> Scylla<Sender>: ScyllaBuilder<Sender>}, state: Apps {});

impl Builder for AppsBuilder {
    type State = Apps;

    fn build(self) -> Self::State {
        let config = Config::from_file(Path::new("./example_config.ron")).expect("Failed to deserialize config!");
        let permanode_api_builder = PermanodeAPIBuilder::new().config(config);
        let scylla_builder = ScyllaBuilder::new()
            .listen_address("127.0.0.1:8080".to_owned())
            .thread_count(num_cpus::get())
            .reporter_count(2)
            .local_dc("datacenter1".to_owned());

        self.PermanodeAPI(permanode_api_builder)
            .Scylla(scylla_builder)
            .to_apps()
    }
}

#[tokio::main]
async fn main() {
    dotenv::dotenv().unwrap();
    env_logger::init();

    let apps = AppsBuilder::new().build();

    apps.Scylla()
        .await
        .future(|apps| async {
            let ws = format!("ws://{}/", "127.0.0.1:8080");
            let nodes = vec!["127.0.0.1:9042".parse().unwrap()];
            add_nodes(&ws, nodes, 1)
                .await
                .unwrap_or_else(|e| panic!("Unable to add nodes: {}", e));
            apps
        })
        .await
        .PermanodeAPI()
        .await
        .start(None)
        .await;
}
