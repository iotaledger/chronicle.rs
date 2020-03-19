macro_rules! set_builder_option_field {
    ($i:ident, $t:ty) => {
        pub fn $i(mut self, $i: $t) -> Self {
            self.$i.replace($i);
            self
        }
    };
}

macro_rules! set_builder_field {
    ($i:ident, $t:ty) => {
        pub fn $i(mut self, $i: $t) -> Self {
            self.$i = $i;
            self
        }
    };
}

// uses
use crate::cluster;
use crate::dashboard::dashboard;
use tokio::sync::mpsc;

// types
pub type Sender = mpsc::UnboundedSender<Event>;
pub type Receiver = mpsc::UnboundedReceiver<Event>;
type ThreadCount = usize;
type ReporterCount = u8;

// event enum
pub enum Event {

}

// Arguments struct
pub struct EngineBuilder {
    listen_address: Option<String>,
    reporter_count: Option<u8>,
    thread_count: Option<usize>,
    nodes: Option<Vec<String>>,
}

impl EngineBuilder {
    pub fn new() -> Self {
        Self {
            listen_address: None,
            reporter_count: None,
            thread_count: None,
            nodes: None,
        }
    }

    set_builder_option_field!(listen_address, String);
    set_builder_option_field!(reporter_count, u8);
    set_builder_option_field!(thread_count, usize);
    set_builder_option_field!(nodes, Vec<String>);

    pub fn build(self) -> Engine {
        let (tx, rx) = mpsc::unbounded_channel::<Event>();
        Engine {
            listen_address: self.listen_address.unwrap(),
            reporter_count: self.reporter_count.unwrap(),
            thread_count: self.thread_count.unwrap(),
            nodes: self.nodes,
            tx,
            rx,
        }
    }

}

// suerpvisor state struct
pub struct Engine {
    listen_address: String,
    reporter_count: u8,
    thread_count: usize,
    nodes: Option<Vec<String>>,
    tx: Sender,
    rx: Receiver,
}

impl Engine {
    pub async fn run(mut self) {
        // init
        self.init().await;
        // check if nodes is provided to start in local mode
        if let Some(nodes) = self.nodes {
            // spawn async local_starter future
        };
        // event loop
        while let Some(event) = self.rx.recv().await {
            match event {
                
            }
        }
    }
    async fn init(&mut self) {
        // build dashboard
        let dashboard = dashboard::DashboardBuilder::new()
        .listen_address(self.listen_address.clone())
        .build();
        // build cluster
        let cluster = cluster::SupervisorBuilder::new()
        .reporter_count(self.reporter_count)
        .thread_count(self.thread_count)
        .dashboard_tx(dashboard.clone_tx())
        .build();
        // spawn dashboard
        tokio::spawn(dashboard.run(cluster.clone_tx()));
        // spawn cluster
        tokio::spawn(cluster.run());
    }
}
