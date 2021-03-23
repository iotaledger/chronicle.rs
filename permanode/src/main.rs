#![warn(missing_docs)]
//! # Permanode

use config::*;
use permanode_api::application::*;
use permanode_broker::application::*;
use scylla::application::*;

mod config;

launcher!
(
    builder: AppsBuilder
    {
        [] -> PermanodeBroker<Sender>: PermanodeBrokerBuilder<Sender>,
        [] -> PermanodeAPI<Sender>: PermanodeAPIBuilder<Sender>,
        [PermanodeBroker, PermanodeAPI] -> Scylla<Sender>: ScyllaBuilder<Sender>
    },
    state: Apps {}
);

impl Builder for AppsBuilder {
    type State = Apps;

    fn build(self) -> Self::State {
        let config = Config::load().expect("Failed to deserialize config!");
        let permanode_api_builder = PermanodeAPIBuilder::new()
            .api_config(config.api_config)
            .storage_config(config.storage_config.clone());
        let logs_dir_path = std::path::PathBuf::from("./");
        let permanode_broker_builder = PermanodeBrokerBuilder::new()
            .logs_dir_path(logs_dir_path)
            .storage_config(config.storage_config.clone());
        let scylla_builder = ScyllaBuilder::new()
            .listen_address(config.storage_config.listen_address)
            .thread_count(match config.storage_config.thread_count {
                permanode_storage::ThreadCount::Count(c) => c,
                permanode_storage::ThreadCount::CoreMultiple(c) => num_cpus::get() * c,
            })
            .reporter_count(config.storage_config.reporter_count)
            .local_dc(config.storage_config.local_datacenter.clone());

        self.PermanodeAPI(permanode_api_builder)
            .PermanodeBroker(permanode_broker_builder)
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
            #[cfg(target_os = "windows")]
            let nodes = vec!["127.0.0.1:19042".parse().unwrap()];
            #[cfg(not(target_os = "windows"))]
            let nodes = vec!["172.17.0.2:19042".parse().unwrap()];
            add_nodes(&ws, nodes, 1)
                .await
                .unwrap_or_else(|e| panic!("Unable to add nodes: {}", e));
            apps
        })
        .await
        .PermanodeAPI()
        .await
        .PermanodeBroker()
        .await
        .start(None)
        .await;
}
