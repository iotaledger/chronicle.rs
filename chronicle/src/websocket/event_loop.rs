// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0
use super::*;
use chronicle::{
    ConfigCommand,
    SocketMsg,
};
use futures::{
    stream::SplitSink,
    SinkExt,
    StreamExt,
};
use log::error;
use scylla::application::Topology;
use serde_json::Value;
use tokio_tungstenite::connect_async;
use url::Url;
use warp::{
    ws::{
        Message,
        WebSocket,
        Ws,
    },
    Filter,
};

#[async_trait]
impl<H: WebsocketScope> EventLoop<H> for Websocket<H> {
    async fn event_loop(&mut self, _status: Result<(), Need>, supervisor: &mut Option<H>) -> Result<(), Need> {
        self.service.update_status(ServiceStatus::Running);
        if let Some(ref mut supervisor) = supervisor {
            supervisor.status_change(self.service.clone());

            let websocket_address = get_config_async().await.websocket_address;

            let route = warp::any()
                .and(warp::ws())
                .map(|ws: Ws| ws.on_upgrade(manage_connection));

            let handle = tokio::spawn(warp::serve(route).run(websocket_address));

            while let Some(evt) = self.inbox.recv().await {
                match evt {
                    WebsocketEvent::Passthrough(passthrough_events) => match passthrough_events.try_get_my_event() {
                        Ok(my_event) => match my_event {
                            WebsocketThrough::Shutdown => {
                                if !self.service.is_stopping() {
                                    supervisor.shutdown_app(&self.get_name());
                                    handle.abort();
                                    self.sender.take();
                                }
                            }
                        },
                        Err(other_app_event) => {
                            supervisor.passthrough(other_app_event, self.get_name());
                        }
                    },
                }
            }
        }

        Ok(())
    }
}

async fn manage_connection(ws: WebSocket) {
    let (mut tx, mut rx) = ws.split();

    while let Some(Ok(msg)) = rx.next().await {
        if let Err(e) = handle_message(msg, &mut tx).await {
            error!("{}", e);
        }
    }
}

async fn handle_message(msg: Message, tx: &mut SplitSink<WebSocket, Message>) -> anyhow::Result<()> {
    if let Ok(txt) = msg.to_str() {
        if let Ok(target) = serde_json::from_str::<SocketMsg<Value>>(txt) {
            if let SocketMsg::General(v) = target.clone() {
                if let Ok(command) = serde_json::from_value::<ConfigCommand>(v) {
                    match command {
                        ConfigCommand::Rollback => {
                            get_history_mut_async().await.rollback();
                            return Ok(());
                        }
                    }
                }
            }
            match target.to_outgoing() {
                Ok(s) => {
                    // Connect to the appropriate websocket
                    let config = get_config_async().await;
                    if let Ok((mut stream, _)) = connect_async(Url::parse(&format!(
                        "ws://{}/",
                        match target {
                            SocketMsg::General(_) => todo!(),
                            SocketMsg::API(_) => todo!(),
                            SocketMsg::Broker(_) => config.broker_config.websocket_address,
                            SocketMsg::Scylla(_) => config.storage_config.listen_address,
                        }
                    ))?)
                    .await
                    {
                        let message = tokio_tungstenite::tungstenite::Message::text(s);
                        // Pass along the message we received
                        stream.send(message).await?;
                        if let SocketMsg::Scylla(v) = target {
                            if let Ok(through) = serde_json::value::from_value::<ScyllaThrough>(v) {
                                match through {
                                    ScyllaThrough::Topology(topo) => {
                                        let mut config = get_config_async().await;
                                        match topo {
                                            Topology::AddNode(address) => {
                                                if config.storage_config.nodes.insert(address) {
                                                    get_history_mut_async().await.update(config.into());
                                                }
                                            }
                                            Topology::RemoveNode(address) => {
                                                if config.storage_config.nodes.remove(&address) {
                                                    get_history_mut_async().await.update(config.into());
                                                }
                                            }
                                            Topology::BuildRing(_) => (),
                                        }
                                    }
                                    ScyllaThrough::Shutdown => (),
                                }
                            }
                        }
                        // Get the response if there is one
                        if let Some(res) = stream.next().await {
                            // Send the response back to the original peer
                            match res.map(|msg| msg.to_text().map(String::from)).and_then(|r| r) {
                                Ok(msg) => {
                                    tx.send(warp::ws::Message::text(msg)).await.ok();
                                }
                                Err(e) => {
                                    tx.send(warp::ws::Message::text(e.to_string())).await.ok();
                                }
                            }
                        }
                    }
                }
                Err(e) => {
                    tx.send(warp::ws::Message::text(e)).await.ok();
                }
            }
        }
    }
    Ok(())
}
