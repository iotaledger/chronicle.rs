use super::*;
use futures::{
    future::{
        AbortHandle,
        AbortRegistration,
        Abortable,
    },
    stream::SplitSink,
    Future,
    SinkExt,
    StreamExt,
};
use std::{
    borrow::Cow,
    collections::HashMap,
    convert::TryInto,
    net::SocketAddr,
    str::FromStr,
    sync::Arc,
    time::Duration,
};
use tokio::sync::Mutex;
use warp::{
    ws::{
        Message,
        WebSocket,
        Ws,
    },
    Filter,
};

#[async_trait]
impl<H: PermanodeAPIScope> EventLoop<PermanodeAPISender<H>> for Websocket {
    async fn event_loop(
        &mut self,
        _status: Result<(), Need>,
        supervisor: &mut Option<PermanodeAPISender<H>>,
    ) -> Result<(), Need> {
        self.service.update_status(ServiceStatus::Running);
        if let Some(ref mut supervisor) = supervisor {
            supervisor
                .send(PermanodeAPIEvent::Children(PermanodeAPIChild::Listener(
                    self.service.clone(),
                )))
                .map_err(|_| Need::Abort)?;
        }
        let route = warp::path("ws")
            .and(warp::ws())
            .map(|ws: Ws| ws.on_upgrade(|socket| manage_connection(socket)));

        let address = std::env::var("WEBSOCKET_ADDRESS").unwrap_or("127.0.0.1".to_string());
        let port = std::env::var("WEBSOCKET_PORT").unwrap_or("8081".to_string());

        warp::serve(route)
            .run(SocketAddr::from_str(&format!("{}:{}", address, port)).unwrap())
            .await;
        Ok(())
    }
}

async fn manage_connection(ws: WebSocket) {
    let (tx, mut rx) = ws.split();

    let sender = Arc::new(Mutex::new(tx));

    let mut registered_topics = HashMap::new();

    while let Some(message) = rx.next().await {
        match message {
            Ok(msg) => {
                if !msg.is_binary() {
                    return;
                }

                let bytes = msg.as_bytes();

                if bytes.len() < 2 {
                    return;
                }

                let command: commands::WsCommand = match bytes[0].try_into() {
                    Ok(command) => command,
                    Err(e) => {
                        error!("Unknown websocket command: {}.", e);
                        return;
                    }
                };
                let topic: topics::WsTopic = match bytes[1].try_into() {
                    Ok(topic) => topic,
                    Err(e) => {
                        error!("Unknown websocket topic: {}.", e);
                        return;
                    }
                };

                info!("Received Command: {:?} for Topic: {:?}", command, topic);

                match command {
                    commands::WsCommand::Register => {
                        let (abort_handle, abort_registration) = AbortHandle::new_pair();
                        let abortable = register_topic(abort_registration, topic.clone(), sender.clone());
                        if let Ok(abortable) = abortable {
                            registered_topics.insert(topic, abort_handle);
                            tokio::spawn(start_abortable(abortable));
                        }
                    }
                    commands::WsCommand::Unregister => {
                        registered_topics.remove(&topic).map(|handle| handle.abort());
                    }
                }
            }
            Err(e) => {
                error!("{}", e);
            }
        }
    }
}

async fn start_abortable(abortable: Abortable<impl Future>) {
    abortable.await.ok();
}

fn register_topic(
    handle: AbortRegistration,
    topic: topics::WsTopic,
    tx: Arc<Mutex<SplitSink<WebSocket, Message>>>,
) -> Result<Abortable<impl Future>, Cow<'static, str>> {
    Ok(Abortable::new(
        match topic {
            // ***topics::WsTopic::SyncStatus => {}
            topics::WsTopic::PublicNodeStatus => node_status(tx),
            // topics::WsTopic::NodeStatus => {}
            // ***topics::WsTopic::MPSMetrics => {}
            // topics::WsTopic::TipSelectionMetrics => {}
            // ***topics::WsTopic::Milestone => {}
            // topics::WsTopic::PeerMetrics => {}
            // ***topics::WsTopic::ConfirmedMilestoneMetrics => {}
            // ***topics::WsTopic::Vertex => {}
            // ***topics::WsTopic::SolidInfo => {}
            // ***topics::WsTopic::ConfirmedInfo => {}
            // ***topics::WsTopic::MilestoneInfo => {}
            // ***topics::WsTopic::TipInfo => {}
            // topics::WsTopic::DatabaseSizeMetrics => {}
            // topics::WsTopic::DatabaseCleanupEvent => {}
            // topics::WsTopic::SpamMetrics => {}
            // topics::WsTopic::AverageSpamMetrics => {}
            _ => Err("No handler for this topic!")?,
        },
        handle,
    ))
}

async fn node_status(tx: Arc<Mutex<SplitSink<WebSocket, Message>>>) {
    let mut interval = tokio::time::interval(Duration::from_secs(1));

    loop {
        interval.tick().await;
        let public_node_status = event::PublicNodeStatus {
            snapshot_index: 0,
            pruning_index: 0,
            is_healthy: true,
            is_synced: true,
        };

        send_ws_response(public_node_status.into(), &tx).await;
    }
}

async fn send_ws_response(event: event::WsEvent, tx: &Arc<Mutex<SplitSink<WebSocket, Message>>>) {
    match serde_json::to_string(&event) {
        Ok(s) => {
            tx.lock().await.send(Message::text(s)).await;
        }
        Err(e) => (),
    }
}
