// import the apps you want to build
use chronicle_storage::storage::storage::StorageBuilder;
use chronicle_api::api::api::ApiBuilder;
// import launcher macro
use chronicle_common::launcher;

launcher!(
    apps_builder: AppsBuilder {storage: StorageBuilder, api: ApiBuilder}, // Apps
    apps: Apps{} // Launcher state
);

// build your apps
impl AppsBuilder {
    fn build(self) -> Apps {
        // - storage app:
        let storage = StorageBuilder::new()
        .listen_address("0.0.0.0:8080".to_string())
        .thread_count(8)
        .local_dc("datacenter1".to_string())
        .reporter_count(1)
        .buffer_size(1024000)
        .recv_buffer_size(1024000)
        .send_buffer_size(1024000)
        .nodes(vec!["172.17.0.2:9042".to_string()]);
        // - api app
        let api = ApiBuilder::new()
        .listen_address("0.0.0.0:4000".to_string());
        // add app to AppsBuilder then transform it to Apps
        self.storage(storage)
        .api(api)
        .to_apps()
    }
}

#[tokio::main(core_threads = 8)]
async fn main() {
    println!("Starting chronicle-example");
    AppsBuilder::new()
    .build() // build apps first, then start them in order you want.
    .function(|apps| {
        // for instance this is helpful to spawn ctrl_c future
        tokio::spawn(ctrl_c(apps.tx.clone()));
    }).await // you can start some function(it must never block)
    .storage().await // start storage app
    .api().await // start api app
    .one_for_one().await; // instead you can define your own .run() strategy
}

/// Useful function to exit program using ctrl_c signal
async fn ctrl_c(mut launcher: Sender) {
    // await on ctrl_c
    tokio::signal::ctrl_c().await.unwrap();
    // exit program using launcher
    launcher.exit_program();
}
