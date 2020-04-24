// uses
use chronicle_common::traits::{
    launcher::LauncherTx,
    dashboard::{
        DashboardTx,
        AppStatus,
    }
};
use super::listener;
use crate::{
    cluster::supervisor,
    connection::cql::Address,
};
use chronicle_common::actor;
use futures::stream::SplitSink;
use std::{
    collections::HashMap,
    net::SocketAddr,
};
use tokio::{
    net::TcpStream,
    sync::mpsc,
};
use tokio_tungstenite::{
    tungstenite::Message,
    WebSocketStream,
};
// types
#[derive(Clone)]
pub struct Sender(pub mpsc::UnboundedSender<Event>);
impl DashboardTx for Sender {
    fn started_app(&mut self, app_name: std::string::String) {
        let event = Event::App(AppStatus::Running(app_name));
        let _ = self.0.send(event);
    }
    fn shutdown_app(&mut self, app_name: std::string::String) {
        let event = Event::App(AppStatus::Shutdown(app_name));
        let _ = self.0.send(event);
    }
    fn apps_status(&mut self, apps_status: HashMap<String, AppStatus>) {
        let event = Event::AppsStatus(apps_status);
        let _ = self.0.send(event);
    }
}
pub type Receiver = mpsc::UnboundedReceiver<Event>;
type WsTx = SplitSink<WebSocketStream<TcpStream>, Message>;

// event
pub enum Event {
    Session(Session),
    Toplogy(Toplogy),
    Result(Result),
    App(AppStatus),
    AppsStatus(HashMap<String, AppStatus>)
}
pub enum Session {
    // todo auth events
    Socket { peer: SocketAddr, ws_tx: WsTx },
}

pub enum Toplogy {
    AddNode(Address),
    RemoveNode(Address),
    TryBuild(usize),
}
pub enum Result {
    Ok(Address),
    Err(Address),
    TryBuild(bool),
}

actor!(
    DashboardBuilder {
        listen_address: String,
        launcher_tx: Box<dyn LauncherTx>
});

impl DashboardBuilder {
    pub fn build(self) -> Dashboard {
        let (tx, rx) = mpsc::unbounded_channel::<Event>();
        Dashboard {
            launcher_tx: self.launcher_tx.unwrap(),
            listen_address: self.listen_address.unwrap(),
            sockets: HashMap::new(),
            tx: Sender(tx),
            rx,
        }
    }
}

pub struct Dashboard {
    launcher_tx: Box<dyn LauncherTx>,
    listen_address: String,
    sockets: HashMap<SocketAddr, WsTx>,
    tx: Sender,
    rx: Receiver,
}

impl Dashboard {
    pub async fn run(mut self, cluster_tx: supervisor::Sender) {
        // build dashboard listener
        let listener = listener::ListenerBuilder::new()
            .listen_address(self.listen_address.clone())
            .dashboard_tx(self.tx.clone())
            .build();
        // spawn dashboard listener
        tokio::spawn(listener.run());
        // event loop
        while let Some(event) = self.rx.recv().await {
            // events from websocket(read-half)
            match event {
                Event::Session(session) => {
                    match session {
                        // todo: here we put our ticket based auth events
                        Session::Socket { peer, ws_tx } => {
                            // once we recv this event means authentication succeed
                            // therefore we add the peer to the sockets we are handling right now
                            self.sockets.insert(peer, ws_tx);
                        }
                    }
                }
                Event::Toplogy(toplogy) => match toplogy {
                    Toplogy::AddNode(address) => {
                        let event = supervisor::Event::SpawnNode(address);
                        cluster_tx.send(event).unwrap();
                    }
                    Toplogy::RemoveNode(address) => {
                        let event = supervisor::Event::ShutDownNode(address);
                        cluster_tx.send(event).unwrap();
                    }
                    Toplogy::TryBuild(replication_factor) => {
                        let event = supervisor::Event::TryBuild(replication_factor);
                        cluster_tx.send(event).unwrap();
                    }
                },
                Event::Result(result) => {
                    match result {
                        Result::Ok(address) => {
                            // successfully spawned the node
                            println!("addnode ok address: {}", address);
                        }
                        Result::Err(address) => {
                            println!("addnode err address: {}", address);
                        }
                        Result::TryBuild(built) => {
                            println!("built status: {}", built);
                        }
                    }
                } /* todo handle websocket decoded msgs (add node, remove node, build,
                   * get status, get dashboard log, import dump file, etc) */
                _ => {
                    //
                }
            }
        }
    }
    pub fn clone_tx(&self) -> Sender {
        self.tx.clone()
    }
}
