#![warn(missing_docs)]
//! # Permanode
use permanode_api::application::*;
use permanode_broker::application::*;
use permanode_common::{
    config::*,
    get_config,
    get_config_async,
    get_history_mut,
    metrics::*,
};
use scylla::application::*;
use websocket::*;

mod websocket;

launcher!
(
    builder: AppsBuilder
    {
        [] -> PermanodeBroker<Sender>: PermanodeBrokerBuilder<Sender>,
        [] -> PermanodeAPI<Sender>: PermanodeAPIBuilder<Sender>,
        [] -> Websocket<Sender>: WebsocketBuilder<Sender>,
        [PermanodeBroker, PermanodeAPI] -> Scylla<Sender>: ScyllaBuilder<Sender>
    },
    state: Apps {}
);

impl Builder for AppsBuilder {
    type State = Apps;

    fn build(self) -> Self::State {
        let storage_config = get_config().storage_config;
        let permanode_api_builder = PermanodeAPIBuilder::new();
        let permanode_broker_builder = PermanodeBrokerBuilder::new();
        let scylla_builder = ScyllaBuilder::new()
            .listen_address(storage_config.listen_address.to_string())
            .thread_count(match storage_config.thread_count {
                ThreadCount::Count(c) => c,
                ThreadCount::CoreMultiple(c) => num_cpus::get() * c,
            })
            .reporter_count(storage_config.reporter_count)
            .local_dc(storage_config.local_datacenter.clone());
        let websocket_builder = WebsocketBuilder::new();

        self.PermanodeAPI(permanode_api_builder)
            .PermanodeBroker(permanode_broker_builder)
            .Scylla(scylla_builder)
            .Websocket(websocket_builder)
            .to_apps()
    }
}

fn main() {
    dotenv::dotenv().unwrap();
    env_logger::init();
    register_metrics();
    let config = get_config();
    let thread_count;
    match config.storage_config.thread_count {
        ThreadCount::Count(c) => {
            thread_count = c;
        }
        ThreadCount::CoreMultiple(c) => {
            thread_count = num_cpus::get() * c;
        }
    }
    let apps = AppsBuilder::new().build();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(thread_count)
        .thread_name("permanode")
        .thread_stack_size(apps.app_count * 4 * 1024 * 1024)
        .build()
        .expect("Expected to build tokio runtime");
    let mut new_config = config.clone();
    if let Err(e) = runtime.block_on(new_config.verify()) {
        panic!("{}", e)
    }
    if new_config != config {
        get_history_mut().update(new_config);
    }
    runtime.block_on(permanode(apps));
}

async fn permanode(apps: Apps) {
    apps.Scylla()
        .await
        .future(|apps| async {
            let storage_config = get_config_async().await.storage_config;
            debug!("Adding nodes: {:?}", storage_config.nodes);
            let ws = format!("ws://{}/", storage_config.listen_address);
            add_nodes(&ws, storage_config.nodes.iter().cloned().collect(), 1)
                .await
                .unwrap_or_else(|e| panic!("Unable to add nodes: {}", e));
            apps
        })
        .await
        .PermanodeAPI()
        .await
        .PermanodeBroker()
        .await
        .Websocket()
        .await
        .start(None)
        .await;
}

fn register_metrics() {
    REGISTRY
        .register(Box::new(INCOMING_REQUESTS.clone()))
        .expect("Could not register collector");

    REGISTRY
        .register(Box::new(RESPONSE_CODE_COLLECTOR.clone()))
        .expect("Could not register collector");

    REGISTRY
        .register(Box::new(RESPONSE_TIME_COLLECTOR.clone()))
        .expect("Could not register collector");
}
