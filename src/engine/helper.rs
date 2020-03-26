// helper for unit test and not for production,
// production setup should manage and setup everything through dashboard. therefore this mod will be removed eventually once we have dashboard ready.
use crate::dashboard::dashboard;
use std::time::Duration;

actor!(
    HelperBuilder {
        nodes: Vec<String>,
        dashboard_tx: dashboard::Sender
});

impl HelperBuilder {
    pub fn build(self) -> Helper {
        Helper {
            nodes: self.nodes.unwrap(),
            dashboard_tx: self.dashboard_tx.unwrap(),
        }
    }
}

pub struct Helper {
    nodes: Vec<String>,
    dashboard_tx: dashboard::Sender,
}
impl Helper {
    pub async fn run(mut self) {
        // create delay duration
        let five_seconds = Duration::new(5, 0);
        // spawn nodes with delay in-between for simplicty
        for node_address in self.nodes {
            let event = dashboard::Event::Toplogy(dashboard::Toplogy::AddNode(node_address));
            self.dashboard_tx.send(event);
            // duration
            tokio::time::delay_for(five_seconds).await;
        }
        // send tryBuild (assuming the nodes have been added)
    }
}
