// uses
use super::dashboard;
use super::websocket;
use tokio::net::TcpListener;
use tokio_tungstenite::accept_async;

// types

actor!(ListenerBuilder {
    listen_address: String,
    dashboard_tx: dashboard::Sender
});

impl ListenerBuilder {
    pub fn build(self) -> Listener {
        Listener {
            listen_address: self.listen_address.unwrap(),
            dashboard_tx: self.dashboard_tx.unwrap(),
        }
    }
}

// listener state
pub struct Listener {
    listen_address: String,
    dashboard_tx: dashboard::Sender,
}

impl Listener {
    pub async fn run(self) {
        // try to bind and unwrap to panic() on error (this is what we want)
        let mut listener = TcpListener::bind(&self.listen_address).await.unwrap();
        // accept dashboard connections
        while let stream = listener.accept().await {
            match stream {
                Ok((socket, _)) => {
                    // convert socket to websocketstream
                    let peer = socket
                        .peer_addr()
                        .expect("connected streams should have a peer address");
                    if let Ok(ws_stream) = accept_async(socket).await {
                        // build websocket
                        let websocket = websocket::WebsocketdBuilder::new()
                            .peer(peer)
                            .stream(ws_stream)
                            .dashboard_tx(self.dashboard_tx.clone())
                            .build();
                        // spawn websocket
                        tokio::spawn(websocket.run());
                    };
                }
                Err(_) => {
                    // todo error handling
                    continue;
                }
            }
        }
    }
}
