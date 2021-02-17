use std::{
    borrow::Cow,
    str::FromStr,
};

use super::*;
use bee_common::packable::Packable;
use mpsc::unbounded_channel;
use permanode_storage::{
    access::{
        MessageId,
        ReporterEvent,
        RowsDecoder,
        SelectQuery,
    },
    keyspaces::Mainnet,
};
use rocket::get;
use scylla::ring::Ring;
use scylla_cql::CqlError;
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

#[get("/messages/<message_id>")]
async fn get_message(message_id: String) -> Result<Vec<u8>, Cow<'static, str>> {
    let (sender, mut inbox) = unbounded_channel::<Event>();
    let worker = Box::new(DecoderWorker(sender));

    let keyspace = Mainnet;
    let query: SelectQuery<MessageId, Message> = keyspace.select(&MessageId::from_str(&message_id).unwrap());

    let request = ReporterEvent::Request {
        worker,
        payload: query.into_bytes(),
    };

    Ring::send_local_random_replica(rand::random::<i64>(), request);

    while let Some(event) = inbox.recv().await {
        match event {
            Event::Response { giveload } => {
                let res: Result<Option<Message>, CqlError> = Mainnet::try_decode(giveload.into());
                if let Ok(Some(message)) = res {
                    let mut bytes = Vec::<u8>::new();
                    message.pack(&mut bytes).unwrap();
                    return Ok(bytes);
                }
            }
            Event::Error { kind } => return Err(kind.to_string().into()),
        }
    }

    Err("Failed to receive response!".into())
}

#[get("/messages/<message_id>/metadata")]
async fn get_message_metadata(message_id: u32) -> String {
    "okay".to_string()
}

#[get("/messages/<message_id>/children")]
async fn get_message_children(message_id: u32) -> String {
    "okay".to_string()
}

#[get("/messages?<index>")]
async fn get_message_by_index(index: u32) -> String {
    "okay".to_string()
}
