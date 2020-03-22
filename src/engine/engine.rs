// uses
use crate::cluster;
use crate::dashboard::dashboard;
use tokio::sync::mpsc;

type ThreadCount = usize;
type ReporterCount = u8;

use crate::launcher::launcher;

app!(EngineBuilder {
    listen_address: String,
    reporter_count: ReporterCount,
    thread_count: ThreadCount,
    nodes: Vec<String>
});

impl EngineBuilder {

    pub fn build(self) -> Engine {
        Engine {
            listen_address: self.listen_address.unwrap(),
            reporter_count: self.reporter_count.unwrap(),
            thread_count: self.thread_count.unwrap(),
            nodes: self.nodes,
        }
    }

}

pub struct Engine {
    listen_address: String,
    reporter_count: u8,
    thread_count: usize,
    nodes: Option<Vec<String>>,
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
            .build();
            // spawn helper
            tokio::spawn(helper.run());
        };

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

actor!(
    HelperBuilder {
        nodes: Vec<String>
});

impl HelperBuilder {

    fn build(self) -> Helper {
        Helper {
            nodes: self.nodes.unwrap(),
        }
    }
}

enum HelperEvent {

}

// work in progress
struct Helper {
    nodes: Vec<String>,
    // dashboard_tx
}
impl Helper {
    async fn run(mut self) {
        // create channel
        let (tx, rx) = mpsc::unbounded_channel::<HelperEvent>();

    }
}
