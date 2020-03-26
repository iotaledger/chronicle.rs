// this is an example of how the operator should use the lib
// therefore launcher.rs should be defined in the userspace
use crate::engine::engine::EngineBuilder;

launcher!(AppsBuilder{engine: EngineBuilder});

#[allow(dead_code)]
impl AppsBuilder {
    fn build(self) -> Apps {
        // build your apps
        // - engine app:
        let engine = EngineBuilder::new()
        .listen_address("0.0.0.0:8080".to_string())
        .thread_count(2)
        .reporter_count(1)
        .nodes(vec!["172.17.0.2:9042".to_string()]);
        // add app to AppsBuilder then transform it to Apps
        self.engine(engine)
        .to_apps()
    }
}

impl Apps {
    // here you can impl other breaking strategies than all/one
}

#[tokio::test(core_threads = 2)]
async fn apps_builder() {
    AppsBuilder::new()
    .build() // build apps first
    .engine().await // start the first app
    // here we can start other apps(if any) .api().await etc,
    .all().await // await once all apps break
}
