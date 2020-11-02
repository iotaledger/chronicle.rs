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

//! A mdoules implements the dashboard listener, which has a dashboard transaction channel, used to send events those
//! can be shown in the dashboard, and a tcp listener for websocket usage.

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
    tcp_listener: TcpListener,
    dashboard_tx: dashboard::Sender
});

impl ListenerBuilder {
    pub fn build(self) -> Listener {
        Listener {
            tcp_listener: self.tcp_listener.unwrap(),
            dashboard_tx: self.dashboard_tx.unwrap(),
        }
    }
}

// listener state
pub struct Listener {
    tcp_listener: TcpListener,
    dashboard_tx: dashboard::Sender,
}

impl Listener {
    pub async fn run(listener: Abortable<impl Future>) {
        // await abortable_listener
        let _aborted_or_ok = listener.await;
    }
    pub fn make_abortable(self) -> (Abortable<impl Future>, AbortHandle) {
        // make abortable_listener
        abortable(self.listener())
    }
    async fn listener(mut self) {
        loop {
            if let Ok((socket, peer)) = self.tcp_listener.accept().await {
                let peer = socket.peer_addr().unwrap_or(peer);
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
