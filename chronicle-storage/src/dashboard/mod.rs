// Copyright 2020 IOTA Stiftung
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and limitations under the License.

//! This dashboard module handles the events sent from/to the dashboard, which can controls the topology of
//! the ScyllaDB cluster and control the life cyle of applications.

pub mod client;
mod listener;
mod websocket;
// work in progress
use crate::{cluster::supervisor, connection::cql::Address, dashboard::websocket::SocketMsg};
use chronicle_common::{
    actor,
    traits::{
        dashboard::{AppStatus, DashboardTx},
        launcher::LauncherTx,
        shutdown::ShutdownTx,
    },
};
use futures::{future::AbortHandle, stream::SplitSink, SinkExt};
use log::*;
use std::{collections::HashMap, net::SocketAddr};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::mpsc,
};
use tokio_tungstenite::{tungstenite::Message, WebSocketStream};

#[derive(Clone)]
/// The sender structure for dashboard events.
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
/// The shutdown structure for registration the dashboard in launcher.
pub struct Shutdown(Sender);
/// The receiver structure for dashboard event.
pub type Receiver = mpsc::UnboundedReceiver<Event>;
type WsTx = SplitSink<WebSocketStream<TcpStream>, Message>;

#[allow(unused_must_use)]
impl ShutdownTx for Shutdown {
    fn shutdown(self: Box<Self>) {
        (self.0).0.send(Event::Shutdown);
    }
}
/// Dashboard events.
pub enum Event {
    /// The dashboard session.
    Session(Session),
    /// The ScyllaDB node topology event enum.
    Toplogy(Toplogy), // TODO: fix typo
    /// The dashboard result event enum.
    Result(Result),
    /// The launcher event enum.
    Launcher(Launcher),
    /// The shutdown event.
    Shutdown,
}

/// The dashboard session.
pub enum Session {
    // TODO: auth events
    /// The socket address and websocket structure.
    Socket {
        /// The peer socket address.
        peer: SocketAddr,
        /// The transmission part of the websocket stream pair
        ws_tx: WsTx,
    },
    /// Close the socket address
    Close(SocketAddr),
}

/// The ScyllaDB culster toplogy events.
pub enum Toplogy {
    /// Add a new ScyllaDB node with this address.
    AddNode(Address),
    /// Remove the node ScyllaDB node with this address.
    RemoveNode(Address),
    /// Try to build the ScyllaDB ring with the given replication factor.
    TryBuild(usize),
}

/// The dashboard results events.
pub enum Result {
    /// The node with this address is successfully spawned.
    Ok(Address),
    /// The process in adding this node with this address has error.
    Err(Address),
    /// The pass or fail of trying to build the ring.
    TryBuild(bool),
}

/// The launcher events which can be shown in the dashboard.
pub enum Launcher {
    /// The status of this app.
    App(AppStatus),
    /// The status of apps.
    Apps(HashMap<String, AppStatus>),
}

actor!(
    DashboardBuilder {
        tcp_listener: TcpListener,
        launcher_tx: Box<dyn LauncherTx>
});

impl DashboardBuilder {
    /// Build a dashboard.
    pub fn build(self) -> Dashboard {
        let (tx, rx) = mpsc::unbounded_channel::<Event>();
        Dashboard {
            launcher_tx: self.launcher_tx.unwrap(),
            tcp_listener: self.tcp_listener,
            listener: None,
            sockets: HashMap::new(),
            tx: Sender(tx),
            rx,
        }
    }
}

/// Dashboard structure, which give an interface to the user to watch the apps status and perform
/// 1) Add node 2) Remove node 3) Build the ring instructions.
pub struct Dashboard {
    launcher_tx: Box<dyn LauncherTx>,
    listener: Option<AbortHandle>,
    tcp_listener: Option<TcpListener>,
    sockets: HashMap<SocketAddr, WsTx>,
    tx: Sender,
    rx: Receiver,
}

impl Dashboard {
    /// Start to run the event loop of the dashboard.
    pub async fn run(mut self, cluster_tx: supervisor::Sender) {
        // build dashboard listener
        let listener = listener::ListenerBuilder::new()
            .tcp_listener(self.tcp_listener.take().unwrap())
            .dashboard_tx(self.tx.clone())
            .build();
        // make listner abortable
        let (abortable_listener, abort_handle) = listener.make_abortable();
        // register listener abort_handle
        self.listener = Some(abort_handle);
        // spawn dashboard listener
        tokio::spawn(listener::Listener::run(abortable_listener));
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
                            info!("Dashboard: AddNode: Ok({})", address);
                            // NOTE: for now we tell all the active sockets
                            for socket in self.sockets.values_mut() {
                                let msg = SocketMsg::Ok(address.clone());
                                let j = serde_json::to_string(&msg).unwrap();
                                let m = Message::text(j);
                                let _ = socket.send(m).await;
                            }
                        }
                        Result::Err(address) => {
                            error!("Dashboard: AddNode: Err({})", address);
                            // NOTE: for now we tell all the active sockets
                            for socket in self.sockets.values_mut() {
                                let msg = SocketMsg::Err(address.clone());
                                let j = serde_json::to_string(&msg).unwrap();
                                let m = Message::text(j);
                                let _ = socket.send(m).await;
                            }
                        }
                        Result::TryBuild(built) => {
                            if built {
                                info!("Dashboard: Built Ring: {}", built);
                            } else {
                                error!("Dashboard: Built Ring: {}", built);
                            }
                            // NOTE: for now we tell all the active sockets
                            for socket in self.sockets.values_mut() {
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
    /// Clone the dashboard transmission channel.
    pub fn clone_tx(&self) -> Sender {
        self.tx.clone()
    }
}
