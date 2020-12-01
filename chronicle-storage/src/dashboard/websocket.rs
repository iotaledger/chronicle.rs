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

//! The websocket is spwaned by listener and do the following:
//! - split the connection to two halfs (read, write)
//! - pass the write-half to dashboard (or maybe to its own async web task)
//! - block on read-half to recv headless packets from clients (admins)

// uses
use crate::dashboard;
use chronicle_common::actor;
use futures::{
    stream::{SplitSink, SplitStream},
    StreamExt,
};
use log::*;
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use tokio::net::TcpStream;
use tokio_tungstenite::{
    tungstenite::{Message, Result},
    WebSocketStream,
};

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
            ws_rx,
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
#[derive(Deserialize, Serialize, Debug)]
pub enum SocketMsg {
    AddNode(String),
    RemoveNode(String),
    TryBuild(u8),
    Ok(String),
    Err(String),
    BuiltRing(bool),
}

impl Websocket {
    pub async fn run(mut self) -> Result<()> {
        if self.authenticate().await {
            // create login session
            let session = dashboard::Session::Socket {
                peer: self.peer,
                ws_tx: self.ws_tx.take().unwrap(),
            };
            // pass session to dashboard
            let _ = self.dashboard_tx.0.send(dashboard::Event::Session(session));
            // event loop for websocket
            while let Some(res) = self.ws_rx.next().await {
                let msg = res?;
                if msg.is_text() {
                    let event: SocketMsg = serde_json::from_str(msg.to_text().unwrap()).expect("invalid SocketMsg");
                    match event {
                        SocketMsg::AddNode(address) => {
                            let _ = self
                                .dashboard_tx
                                .0
                                .send(dashboard::Event::Toplogy(dashboard::Toplogy::AddNode(address)));
                        }
                        SocketMsg::RemoveNode(address) => {
                            let _ = self
                                .dashboard_tx
                                .0
                                .send(dashboard::Event::Toplogy(dashboard::Toplogy::RemoveNode(address)));
                        }
                        SocketMsg::TryBuild(uniform_rf) => {
                            let _ = self
                                .dashboard_tx
                                .0
                                .send(dashboard::Event::Toplogy(dashboard::Toplogy::TryBuild(uniform_rf as usize)));
                        }
                        _ => warn!("unexpected SocketMsg from {}", self.peer),
                    }
                }
                if msg.is_close() {
                    let _ = self
                        .dashboard_tx
                        .0
                        .send(dashboard::Event::Session(dashboard::Session::Close(self.peer)));
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    use tokio::{net::TcpStream, sync::mpsc};
    use tokio_tungstenite::accept_async;

    #[tokio::test]
    #[ignore]
    async fn create_websocket_from_builder() {
        let peer = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080);
        let socket = TcpStream::connect(peer).await.unwrap();
        let ws_stream = accept_async(socket).await.unwrap();
        let (dashboard_tx, _) = mpsc::unbounded_channel::<dashboard::Event>();
        let _ = WebsocketdBuilder::new()
            .peer(peer)
            .stream(ws_stream)
            .dashboard_tx(dashboard::Sender(dashboard_tx.clone()))
            .build();
    }
}
