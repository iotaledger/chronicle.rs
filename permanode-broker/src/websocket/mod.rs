// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use crate::application::*;
use futures::{
    stream::{
        SplitSink,
        SplitStream,
    },
    StreamExt,
};

use std::net::SocketAddr;
use tokio::net::TcpStream;
pub(crate) use tokio_tungstenite::{
    tungstenite::Message,
    WebSocketStream,
};
mod event_loop;
mod init;
mod terminating;

builder!(
    WebsocketdBuilder {
        peer: SocketAddr,
        stream: WebSocketStream<TcpStream>
});
/// The writehalf of the webssocket
pub type WsTx = SplitSink<WebSocketStream<TcpStream>, Message>;
/// The readhalf of the webssocket
pub type WsRx = SplitStream<WebSocketStream<TcpStream>>;

/// Client Websocket struct
pub struct Websocket {
    service: Service,
    peer: SocketAddr,
    ws_rx: WsRx,
    opt_ws_tx: Option<WsTx>,
}

impl<H: PermanodeBrokerScope> ActorBuilder<BrokerHandle<H>> for WebsocketdBuilder {}

impl Builder for WebsocketdBuilder {
    type State = Websocket;
    fn build(self) -> Self::State {
        // split the websocket stream
        let (ws_tx, ws_rx) = self.stream.unwrap().split();
        Websocket {
            service: Service::new(),
            peer: self.peer.unwrap(),
            ws_rx,
            opt_ws_tx: Some(ws_tx),
        }
        .set_name()
    }
}

/// impl name of the Websocket
impl Name for Websocket {
    fn set_name(mut self) -> Self {
        // TODO make sure the name is unique
        let name: String = self.peer.to_string();
        self.service.update_name(name);
        self
    }
    fn get_name(&self) -> String {
        self.service.get_name()
    }
}

#[async_trait::async_trait]
impl<H: PermanodeBrokerScope> AknShutdown<Websocket> for BrokerHandle<H> {
    async fn aknowledge_shutdown(mut self, mut _state: Websocket, _status: Result<(), Need>) {
        _state.service.update_status(ServiceStatus::Stopped);
        let event = BrokerEvent::Children(BrokerChild::Websocket(_state.service.clone(), None));
        let _ = self.send(event);
    }
}
