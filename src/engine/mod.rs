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
    Break
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
            // build helper (is supposed to simulate the websocket)
            let helper = HelperBuilder::new()
            .nodes(nodes)
            // it needs engine_tx to respond
            .engine_tx(self.tx.clone())
            .build();
            // spawn helper
            tokio::spawn(helper.run());
        };
        // await on Break event from dashboard to deattach the engine
        while let Some(Event::Break) = self.rx.recv().await {
            break
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

struct HelperBuilder {
    nodes: Option<Vec<String>>,
    engine_tx: Option<Sender>,
}
impl HelperBuilder {
    fn new() -> Self {
        Self {
            nodes: None,
            engine_tx: None,
            // dashboard_tx
        }
    }
    set_builder_option_field!(nodes, Vec<String>);
    set_builder_option_field!(engine_tx, Sender);
    fn build(self) -> Helper {
        Helper {
            nodes: self.nodes.unwrap(),
            engine_tx: self.engine_tx.unwrap(),
        }
    }
}

enum HelperEvent {

}

struct Helper {
    nodes: Vec<String>,
    engine_tx: Sender,
}
impl Helper {
    async fn run(mut self) {
        // create channel
        let (tx, rx) = mpsc::unbounded_channel::<HelperEvent>();

    }
}
