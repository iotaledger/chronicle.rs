pub mod client;
mod listener;
mod websocket;
// work in progress
use crate::{
    cluster::supervisor,
    connection::cql::Address,
    dashboard::websocket::SocketMsg,
};
use chronicle_common::{
    actor,
    traits::{
        dashboard::{
            AppStatus,
            DashboardTx,
        },
        launcher::LauncherTx,
        shutdown::ShutdownTx,
    },
};
use futures::{
    future::AbortHandle,
    stream::SplitSink,
    SinkExt,
};
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

#[derive(Clone)]
pub struct Sender(pub mpsc::UnboundedSender<Event>);
impl DashboardTx for Sender {
    fn starting_app(&mut self, app_name: std::string::String) {
        let event = Event::Launcher(Launcher::App(AppStatus::Starting(app_name)));
        let _ = self.0.send(event);
    }
    fn started_app(&mut self, app_name: std::string::String) {
        let event = Event::Launcher(Launcher::App(AppStatus::Running(app_name)));
        let _ = self.0.send(event);
    }
    fn restarted_app(&mut self, app_name: std::string::String) {
        let event = Event::Launcher(Launcher::App(AppStatus::Restarting(app_name)));
        let _ = self.0.send(event);
    }
    fn shutdown_app(&mut self, app_name: std::string::String) {
        let event = Event::Launcher(Launcher::App(AppStatus::Shutdown(app_name)));
        let _ = self.0.send(event);
    }
    fn apps_status(&mut self, apps_status: HashMap<String, AppStatus>) {
        let event = Event::Launcher(Launcher::Apps(apps_status));
        let _ = self.0.send(event);
    }
}
pub struct Shutdown(Sender);
pub type Receiver = mpsc::UnboundedReceiver<Event>;
type WsTx = SplitSink<WebSocketStream<TcpStream>, Message>;

#[allow(unused_must_use)]
impl ShutdownTx for Shutdown {
    fn shutdown(self: Box<Self>) {
        (self.0).0.send(Event::Shutdown);
    }
}
// event
pub enum Event {
    Session(Session),
    Toplogy(Toplogy),
    Result(Result),
    Launcher(Launcher),
    Shutdown,
}
pub enum Session {
    // todo auth events
    Socket { peer: SocketAddr, ws_tx: WsTx },
    Close(SocketAddr),
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

pub enum Launcher {
    App(AppStatus),
    Apps(HashMap<String, AppStatus>),
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
            listener: None,
            sockets: HashMap::new(),
            tx: Sender(tx),
            rx,
        }
    }
}

pub struct Dashboard {
    launcher_tx: Box<dyn LauncherTx>,
    listener: Option<AbortHandle>,
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
        // make listner abortable
        let (abortable_listener, abort_handle) = listener.make_abortable();
        // register listener abort_handle
        self.listener = Some(abort_handle);
        // spawn dashboard listener
        tokio::spawn(listener::Listener::run(abortable_listener, self.clone_tx()));
        // register storage/dashboard app in launcher
        self.launcher_tx
            .register_app("storage".to_string(), Box::new(Shutdown(self.clone_tx())));
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
                        Session::Close(peer) => {
                            self.sockets.remove(&peer);
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
                            println!("Dashboard: AddNode: Ok({})", address);
                            // NOTE: for now we tell all the active sockets
                            for (_, socket) in &mut self.sockets {
                                let msg = SocketMsg::Ok(address.clone());
                                let j = serde_json::to_string(&msg).unwrap();
                                let m = Message::text(j);
                                let _ = socket.send(m).await;
                            }
                        }
                        Result::Err(address) => {
                            println!("Dashboard: AddNode: Err({})", address);
                            // NOTE: for now we tell all the active sockets
                            for (_, socket) in &mut self.sockets {
                                let msg = SocketMsg::Err(address.clone());
                                let j = serde_json::to_string(&msg).unwrap();
                                let m = Message::text(j);
                                let _ = socket.send(m).await;
                            }
                        }
                        Result::TryBuild(built) => {
                            println!("Dashboard: Built Ring: {}", built);
                            // NOTE: for now we tell all the active sockets
                            for (_, socket) in &mut self.sockets {
                                let msg = SocketMsg::BuiltRing(built);
                                let j = serde_json::to_string(&msg).unwrap();
                                let m = Message::text(j);
                                let _ = socket.send(m).await;
                            }
                        }
                    }
                } /* todo handle websocket decoded msgs (add node, remove node, build,
                   * get status, get dashboard log, import dump file, etc) */
                Event::Launcher(_launcher_status) => {
                    //TODO do something with app/apps_status
                }
                Event::Shutdown => {
                    // storage app shutdown including the dashboard/cluster/listener.
                    // - async shutdown cluster
                    cluster_tx.send(supervisor::Event::Shutdown).unwrap();
                    // - abort listener
                    self.listener.unwrap().abort();
                    // shutdown self
                    break;
                }
            }
        }
        // now is safe to aknowledge_shutdown to launcher,
        // note: it's still possible that some reporters are still active draining remaining requests.
        self.launcher_tx.aknowledge_shutdown("storage".to_string());
    }
    pub fn clone_tx(&self) -> Sender {
        self.tx.clone()
    }
}
