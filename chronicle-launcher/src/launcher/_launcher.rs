// this is an example of how the operator should use the lib
// therefore launcher.rs should be defined in the userspace

// define launcher channel types


pub type Sender = mpsc::UnboundedSender<String>;
pub type Receiver = mpsc::UnboundedReceiver<String>;

launcher!(apps_builder: AppsBuilder{storage: StorageBuilder}, apps: Apps{}, tx: Sender, rx: Receiver);

// impl LauncherTx for launcher sender
impl LauncherTx for Sender {
    fn start_app(&mut self, app_name: String) {
        // how apps(or dashboard) should send a start app event
    }
    fn shutdown_app(&mut self, app_name: String) {
        // how apps(or dashboard) should send a shutdown app event
    }
    fn aknowledge_shutdown(&mut self, app_name: String) {
        // how apps should aknowledge_shutdown
    }
    fn register_dashboard(&mut self, dashboard_tx: Box<dyn DashboardTx>) {
        // dashboard will invoke this to register itself with the launcher
    }
}

#[allow(dead_code)]
impl AppsBuilder {
    fn build(self) -> Apps {
        // build your apps
        // - storage app:
        let storage = StorageBuilder::new()
        .listen_address("0.0.0.0:8080".to_string())
        .thread_count(2)
        .local_dc("datacenter1".to_string())
        .reporter_count(1)
        .buffer_size(1024000)
        .recv_buffer_size(1024000)
        .send_buffer_size(1024000)
        .nodes(vec!["172.17.0.2:9042".to_string()]);
        // add app to AppsBuilder then transform it to Apps
        self.storage(storage)
        .to_apps()
    }
}

impl Apps {
    fn exit() {

    }
}

#[tokio::test(core_threads = 2)]
async fn apps_builder() {
    // create your launcher channel
    let (tx, rx) = mpsc::unbounded_channel::<String>();
    AppsBuilder::new(tx,rx)
    .build() // build apps first
    .engine().await // start the first app
    // here we can start other apps(if any) .api().await etc,
    .all().await // await once all apps break
}
