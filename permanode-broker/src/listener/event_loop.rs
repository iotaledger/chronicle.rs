// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::websocket::WebsocketdBuilder;
use tokio_tungstenite::accept_async;

#[async_trait::async_trait]
impl<H: BrokerScope> EventLoop<BrokerHandle<H>> for Listener {
    async fn event_loop(
        &mut self,
        _status: Result<(), Need>,
        supervisor: &mut Option<BrokerHandle<H>>,
    ) -> Result<(), Need> {
        self.service.update_status(ServiceStatus::Running);
        let event = BrokerEvent::Children(BrokerChild::Listener(self.service.clone()));
        let my_sup = supervisor.as_mut().unwrap();
        let _ = my_sup.send(event);
        loop {
            if let Ok((socket, peer)) = self.tcp_listener.accept().await {
                let peer = socket.peer_addr().unwrap_or(peer);
                if let Ok(ws_stream) = accept_async(socket).await {
                    // build websocket
                    let websocket = WebsocketdBuilder::new().peer(peer).stream(ws_stream).build();
                    // spawn websocket
                    tokio::spawn(websocket.start(supervisor.clone()));
                }
            }
        }
    }
}
