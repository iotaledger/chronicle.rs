// uses
use futures::{
    future::{abortable, AbortHandle, Abortable},
    Future,
};

use crate::dashboard::{self, websocket};
use chronicle_common::actor;
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
    pub async fn run(listener: Abortable<impl Future>, _dashboard_tx: dashboard::Sender) {
        // await abortable_listener
        let _aborted_or_ok = listener.await;
        // aknowledge shutdown
        // dashboard_tx.0.send();
    }
    pub fn make_abortable(self) -> (Abortable<impl Future>, AbortHandle) {
        // make abortable_listener
        abortable(self.listener())
    }
    async fn listener(self) {
        if let Ok(mut listener) = TcpListener::bind(&self.listen_address).await {
            while let Ok((socket, _)) = listener.accept().await {
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
                }
            }
        }
    }
}
