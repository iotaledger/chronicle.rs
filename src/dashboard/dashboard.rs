// uses
use crate::connection::cql::Address;
use crate::ring::ring::DC;
use std::collections::HashMap;
use futures::stream::SplitSink;
use tokio::sync::mpsc;
use tokio::net::TcpStream;
use std::net::SocketAddr;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::Message;
use super::listener;
use crate::cluster::supervisor;
// types
pub type Sender = mpsc::UnboundedSender<Event>;
pub type Receiver = mpsc::UnboundedReceiver<Event>;
type WsTx = SplitSink<WebSocketStream<TcpStream>, Message>;

// event
pub enum Event {
    Session(Session),
    Toplogy(Toplogy),
    Result(Result),
}

pub enum Session {
    // todo auth events
    Socket{peer: SocketAddr, ws_tx: WsTx},
}

// todo remove the need to pass DC, by fetching durring connection establishment
pub enum Toplogy {
    AddNode(DC, Address),
    RemoveNode(DC, Address),
    TryBuild,
}

pub enum Result {
    Ok(Address),
    Err(Address),
    TryBuild(bool),
}

actor!(
    DashboardBuilder {
        listen_address: String,
        launcher_tx: mpsc::UnboundedSender<String>
});

impl DashboardBuilder {

    pub fn build(self) -> Dashboard {
        let (tx, rx) = mpsc::unbounded_channel::<Event>();
        Dashboard {
            launcher_tx: self.launcher_tx.unwrap(),
            listen_address: self.listen_address.unwrap(),
            sockets: HashMap::new(),
            tx,
            rx,
        }
    }

}

pub struct Dashboard {
    launcher_tx: mpsc::UnboundedSender<String>,
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
                        Session::Socket{peer, ws_tx} => {
                            // once we recv this event means authentication succeed
                            // therefore we add the peer to the sockets we are handling right now
                            self.sockets.insert(peer, ws_tx);
                        }
                    }
                }
                Event::Toplogy(toplogy) => {
                    match toplogy {
                        Toplogy::AddNode(dc, address) => {
                            let event = supervisor::Event::SpawnNode(dc, address);
                            cluster_tx.send(event);
                        }
                        Toplogy::RemoveNode(dc, address) => {
                            let event = supervisor::Event::ShutDownNode(dc, address);
                            cluster_tx.send(event);
                        }
                        Toplogy::TryBuild => {
                            let event = supervisor::Event::TryBuild;
                            cluster_tx.send(event);
                        }
                    }
                }
                Event::Result(result) => {
                    match result {
                        Result::Ok(address) => {
                            // successfully spawned the node
                            println!("addnode ok status: {}", address);
                        }
                        Result::Err(address) => {
                            // wasn't able to
                            println!("addnode err status: {}", address);
                        }
                        Result::TryBuild(built) => {
                            println!("built status: {}", built);
                        }

                    }
                }
                // todo handle websocket decoded msgs (add node, remove node, build,
                // get status, get dashboard log, import dump file, etc)
            }
        }
    }
    pub fn clone_tx(&self) -> Sender {
        self.tx.clone()
    }
}
