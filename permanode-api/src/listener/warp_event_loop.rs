use super::*;
use mpsc::unbounded_channel;
use permanode_storage::{
    access::{
        Bee,
        GetSelectRequest,
        MessageId,
        MessageMetadata,
        SelectRequest,
    },
    keyspaces::Mainnet,
};
use serde::Serialize;
use std::{
    borrow::Cow,
    net::SocketAddr,
    str::FromStr,
};
use tokio::sync::mpsc;
use warp::{
    reject::Reject,
    reply::{
        json,
        Json,
    },
    Filter,
    Rejection,
};

#[async_trait]
impl<H: LauncherSender<PermanodeAPIBuilder<H>>> EventLoop<PermanodeAPISender<H>> for Listener<WarpListener> {
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
        let get_message = warp::path!("messages" / String).and_then(get_message);
        let get_metadata = warp::path!("messages" / String / "metadata").and_then(get_message_metadata);
        let routes = get_message.or(get_metadata);

        let address = std::env::var("WARP_ADDRESS").unwrap_or("127.0.0.1".to_string());
        let port = std::env::var("WARP_PORT").unwrap_or("8000".to_string());

        warp::serve(routes)
            .run(SocketAddr::from_str(&format!("{}:{}", address, port)).unwrap())
            .await;
        Ok(())
    }
}

#[derive(Debug)]
pub struct EndpointError {
    msg: Cow<'static, str>,
}

impl From<&'static str> for EndpointError {
    fn from(msg: &'static str) -> Self {
        Self { msg: msg.into() }
    }
}

impl From<String> for EndpointError {
    fn from(msg: String) -> Self {
        Self { msg: msg.into() }
    }
}

impl Reject for EndpointError {}

async fn query<'a, V: Serialize, S: Select<'a, K, V>, K>(
    request: SelectRequest<'a, S, K, V>,
) -> Result<Json, EndpointError> {
    let (sender, mut inbox) = unbounded_channel::<Event>();
    let worker = Box::new(DecoderWorker(sender));

    let decoder = request.send_local(worker);

    while let Some(event) = inbox.recv().await {
        match event {
            Event::Response { giveload } => {
                let res = decoder.decode(giveload);
                if let Ok(Some(message)) = res {
                    return Ok(json(&message));
                }
            }
            Event::Error { kind } => return Err(kind.to_string().into()),
        }
    }

    Err("Failed to receive response!".into())
}

async fn get_message(message_id: String) -> Result<Json, Rejection> {
    let request = Mainnet.select::<Bee<Message>>(&MessageId::from_str(&message_id).unwrap().into());
    query(request).await.map_err(|e| warp::reject::custom(e))
}

//#[get("/messages/<message_id>/metadata")]
async fn get_message_metadata(message_id: String) -> Result<Json, Rejection> {
    let request = Mainnet.select::<Bee<MessageMetadata>>(&MessageId::from_str(&message_id).unwrap().into());
    query(request).await.map_err(|e| warp::reject::custom(e))
}

//#[get("/messages/<message_id>/children")]
async fn get_message_children(message_id: String) -> Result<Json, Rejection> {
    todo!()
}

//#[get("/messages?<index>")]
async fn get_message_by_index(index: String) -> Result<Json, Rejection> {
    todo!()
}
