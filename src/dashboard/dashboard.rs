// uses
use super::listener;
use crate::cluster::supervisor;
use crate::connection::cql::Address;
use futures::stream::SplitSink;
use std::collections::HashMap;
use std::net::SocketAddr;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;
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
    Socket { peer: SocketAddr, ws_tx: WsTx },
}

pub enum Toplogy {
    AddNode(Address),
    RemoveNode(Address),
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
                    Toplogy::TryBuild => {
                        let event = supervisor::Event::TryBuild;
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
                } // todo handle websocket decoded msgs (add node, remove node, build,
                  // get status, get dashboard log, import dump file, etc)
            }
        }
    }
    pub fn clone_tx(&self) -> Sender {
        self.tx.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_dashboard_from_builder() {
        let (launcher_tx, _) = mpsc::unbounded_channel::<String>();
        let _ = DashboardBuilder::new()
            .launcher_tx(launcher_tx)
            .listen_address("0.0.0.0:9042".to_string())
            .build();
    }
}
