// websocket get spwaned by listener , then it will do the following:
// - split the connection to two halfs (read, write)
// - pass the write-half to dashboard (or maybe to its own async web task)
// - block on read-half to recv headless packets from clients(admins)

// uses
use tokio::net::TcpStream;
use futures::StreamExt;
use futures::stream::{SplitSink, SplitStream};
use std::net::SocketAddr;
use tokio_tungstenite::WebSocketStream;
use super::dashboard;
use tokio_tungstenite::{tungstenite::Message, tungstenite::Result};


actor!(
    WebsocketdBuilder {
        peer: SocketAddr,
        stream: WebSocketStream<TcpStream>,
        dashboard_tx: dashboard::Sender
});

impl WebsocketdBuilder {

    pub fn build(self) -> Websocket {
        // split the websocket stream
        let (ws_tx, ws_rx) = self.stream.unwrap().split();
        Websocket {
            peer: self.peer.unwrap(),
            ws_rx: ws_rx,
            ws_tx: Some(ws_tx),
            dashboard_tx: self.dashboard_tx.unwrap(),
        }
    }

}

pub struct Websocket {
    peer: SocketAddr,
    ws_rx: SplitStream<WebSocketStream<TcpStream>>,
    ws_tx: Option<SplitSink<WebSocketStream<TcpStream>, Message>>,
    dashboard_tx: dashboard::Sender,
}

impl Websocket {
    pub async fn run(mut self) -> Result<()> {
        if self.authenticate().await {
            // create login session
            let session = dashboard::Session::Socket{
                peer: self.peer,
                ws_tx: self.ws_tx.take().unwrap(),
            };
            // pass session to dashboard
            let _ = self.dashboard_tx.send(dashboard::Event::Session(session));
            // event loop for websocket
            while let Some(res) = self.ws_rx.next().await {
                let msg = res?;
                match msg {
                    // handle websockets msgs (binary, text, ping, pong) and then encode them into
                    // dashboard events
                    _ => {

                    }
                }
            }
        }
        Ok(())
    }
    async fn authenticate(&mut self) -> bool {
        // authentication through self.dashboard_tx login session, it should login
        // unimplemented!()
        true
    }
}
