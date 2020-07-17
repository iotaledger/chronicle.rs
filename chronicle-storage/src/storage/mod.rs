pub mod helper;
// uses
use crate::{
    cluster,
    dashboard,
    ring::DC,
    storage::helper::HelperBuilder,
};
use chronicle_common::app;
use tokio::net::TcpListener;

type ThreadCount = usize;
type ReporterCount = u8;

app!(StorageBuilder {
    listen_address: String,
    reporter_count: ReporterCount,
    thread_count: ThreadCount,
    local_dc: DC,
    buffer_size: usize,
    recv_buffer_size: usize,
    send_buffer_size: usize,
    nodes: Vec<String>
});

impl StorageBuilder {
    pub fn build(self) -> Storage {
        Storage {
            listen_address: self.listen_address.unwrap(),
            reporter_count: self.reporter_count.unwrap(),
            thread_count: self.thread_count.unwrap(),
            local_dc: self.local_dc.unwrap(),
            buffer_size: self.buffer_size.unwrap(),
            recv_buffer_size: self.recv_buffer_size,
            send_buffer_size: self.send_buffer_size,
            nodes: self.nodes,
            launcher_tx: self.launcher_tx,
        }
    }
}

pub struct Storage {
    listen_address: String,
    reporter_count: u8,
    thread_count: usize,
    local_dc: DC,
    buffer_size: usize,
    recv_buffer_size: Option<usize>,
    send_buffer_size: Option<usize>,
    nodes: Option<Vec<String>>,
    launcher_tx: Option<Box<dyn LauncherTx>>,
}

impl Storage {
    pub async fn run(mut self) {
        // init
        let dashboard_tx = self.init().await;
        // check if nodes is provided to start in local mode
        if let Some(nodes) = self.nodes {
            // build helper (is supposed to simulate the websocket)
            let helper = HelperBuilder::new()
                .nodes(nodes)
                .dashboard_tx(dashboard_tx.unwrap())
                .build();
            // spawn helper
            tokio::spawn(helper.run());
        };
    }
    async fn init(&mut self) -> Option<dashboard::Sender> {
        let mut launcher_tx = self.launcher_tx.take().unwrap();
        let tcp_listener = TcpListener::bind(self.listen_address.clone()).await.unwrap();
        // build dashboard
        let dashboard = dashboard::DashboardBuilder::new()
            .launcher_tx(launcher_tx.clone())
            .tcp_listener(tcp_listener)
            .build();
        // register dashboard with launcher
        launcher_tx.register_dashboard("StorageDashboard".to_string(), Box::new(dashboard.clone_tx()));
        // build cluster
        let cluster = cluster::SupervisorBuilder::new()
            .reporter_count(self.reporter_count)
            .thread_count(self.thread_count)
            .data_centers(vec![self.local_dc.clone()])
            .buffer_size(self.buffer_size)
            .recv_buffer_size(self.recv_buffer_size)
            .send_buffer_size(self.send_buffer_size)
            .dashboard_tx(dashboard.clone_tx())
            .build();
        // clone dashboard_tx to return in case some(nodes) used for testing
        let dashboard_tx = Some(dashboard.clone_tx());
        // spawn dashboard
        tokio::spawn(dashboard.run(cluster.clone_tx()));
        // spawn cluster
        tokio::spawn(cluster.run());
        if self.nodes.as_ref().is_some() {
            dashboard_tx
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_storage_from_builder() {
        let _ = StorageBuilder::new()
            .listen_address("0.0.0.0:8080".to_string())
            .thread_count(2)
            .local_dc("datacenter1".to_string())
            .reporter_count(1)
            .buffer_size(1024000)
            .nodes(vec!["0.0.0.0:9042".to_string()])
            .build();
    }
}
