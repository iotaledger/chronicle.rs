use std::{
    borrow::Cow,
    str::FromStr,
};

use super::*;
use mpsc::unbounded_channel;
use permanode_storage::{
    access::{
        MessageId,
        MessageMetadata,
        ReporterEvent,
        RowsDecoder,
        SelectQuery,
    },
    keyspaces::Mainnet,
};
use rocket::{
    get,
    response::content::Json,
};
use scylla::ring::Ring;
use scylla_cql::{
    CqlError,
    Query,
};
use serde::Serialize;
use tokio::sync::mpsc;

#[async_trait]
impl<H: LauncherSender<PermanodeBuilder<H>>> EventLoop<PermanodeSender<H>> for Listener {
    async fn event_loop(
        &mut self,
        status: Result<(), Need>,
        supervisor: &mut Option<PermanodeSender<H>>,
    ) -> Result<(), Need> {
        std::env::set_var("ROCKET_PORT", "3000");
        rocket::ignite()
            .mount(
                "/",
                routes![
                    get_message,
                    get_message_metadata,
                    get_message_children,
                    get_message_by_index
                ],
            )
            .launch()
            .await
            .map_err(|e| Need::Abort)
    }
}

async fn query<S: RowsDecoder<V>, V: Serialize>(keyspace: S, query: Query) -> Result<Json<String>, Cow<'static, str>> {
    let (sender, mut inbox) = unbounded_channel::<Event>();
    let worker = Box::new(DecoderWorker(sender));

    let request = ReporterEvent::Request {
        worker,
        payload: query.0,
    };

    Ring::send_local_random_replica(rand::random::<i64>(), request);

    while let Some(event) = inbox.recv().await {
        match event {
            Event::Response { giveload } => {
                let res: Result<Option<V>, CqlError> = S::try_decode(giveload.into());
                if let Ok(Some(message)) = res {
                    return Ok(Json(serde_json::to_string(&message).unwrap()));
                }
            }
            Event::Error { kind } => return Err(kind.to_string().into()),
        }
    }

    Err("Failed to receive response!".into())
}

#[get("/messages/<message_id>")]
async fn get_message(message_id: String) -> Result<Json<String>, Cow<'static, str>> {
    let keyspace = Mainnet;
    let select_query: SelectQuery<MessageId, Message> = keyspace.select(&MessageId::from_str(&message_id).unwrap());
    query::<_, Message>(keyspace, select_query.into_inner()).await
}

#[get("/messages/<message_id>/metadata")]
async fn get_message_metadata(message_id: String) -> Result<Json<String>, Cow<'static, str>> {
    let keyspace = Mainnet;
    let select_query: SelectQuery<MessageId, MessageMetadata> =
        keyspace.select(&MessageId::from_str(&message_id).unwrap());
    query::<_, Message>(keyspace, select_query.into_inner()).await
}

#[get("/messages/<message_id>/children")]
async fn get_message_children(message_id: String) -> Result<Json<String>, Cow<'static, str>> {
    todo!()
}

#[get("/messages?<index>")]
async fn get_message_by_index(index: String) -> Result<Json<String>, Cow<'static, str>> {
    todo!()
}
