use super::*;
use mpsc::unbounded_channel;
use permanode_storage::{
    access::{
        Ed25519Address,
        GetSelectRequest,
        HashedIndex,
        IndexMessages,
        IteratorSerializer,
        MessageChildren,
        MessageId,
        MessageMetadata,
        Milestone,
        MilestoneIndex,
        NeedsSerialize,
        Output,
        OutputId,
        Outputs,
        SelectRequest,
        HASHED_INDEX_LENGTH,
    },
    keyspaces::Mainnet,
    types::Bee,
};
use rocket::{
    get,
    response::content::Json,
};
use scylla_cql::TryInto;
use serde::Serialize;
use std::{
    borrow::Cow,
    str::FromStr,
};
use tokio::sync::mpsc;

#[async_trait]
impl<H: LauncherSender<PermanodeBuilder<H>>> EventLoop<PermanodeSender<H>> for Listener<RocketListener> {
    async fn event_loop(
        &mut self,
        _status: Result<(), Need>,
        supervisor: &mut Option<PermanodeSender<H>>,
    ) -> Result<(), Need> {
        self.service.update_status(ServiceStatus::Running);
        if let Some(ref mut supervisor) = supervisor {
            supervisor
                .send(PermanodeEvent::Children(PermanodeChild::Listener(self.service.clone())))
                .map_err(|_| Need::Abort)?;
        }
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
            .map_err(|_| Need::Abort)
    }
}

async fn query<'a, V: Serialize, S: Select<'a, K, V>, K>(
    request: SelectRequest<'a, S, K, V>,
) -> Result<Json<String>, Cow<'static, str>> {
    let (sender, mut inbox) = unbounded_channel::<Event>();
    let worker = Box::new(DecoderWorker(sender));

    let decoder = request.send_local(worker);

    while let Some(event) = inbox.recv().await {
        match event {
            Event::Response { giveload } => {
                let res = decoder.decode(giveload);
                if let Ok(Some(message)) = res {
                    return Ok(Json(serde_json::to_string(&message).unwrap()));
                }
            }
            Event::Error { kind } => return Err(kind.to_string().into()),
        }
    }

    Err("Failed to receive response!".into())
}

async fn query_iter<'a, V, S, K>(request: SelectRequest<'a, S, K, V>) -> Result<Json<String>, Cow<'static, str>>
where
    V: Iterator,
    V::Item: Serialize,
    S: Select<'a, K, V>,
{
    let (sender, mut inbox) = unbounded_channel::<Event>();
    let worker = Box::new(DecoderWorker(sender));

    let decoder = request.send_local(worker);

    while let Some(event) = inbox.recv().await {
        match event {
            Event::Response { giveload } => {
                let res = decoder.decode(giveload);
                if let Ok(Some(message)) = res {
                    return Ok(Json(serde_json::to_string(&IteratorSerializer::new(message)).unwrap()));
                }
            }
            Event::Error { kind } => return Err(kind.to_string().into()),
        }
    }

    Err("Failed to receive response!".into())
}

#[get("/messages/<message_id>")]
async fn get_message(message_id: String) -> Result<Json<String>, Cow<'static, str>> {
    let request = Mainnet.select::<Bee<Message>>(&MessageId::from_str(&message_id).unwrap().into());
    query(request).await
}

#[get("/messages/<message_id>/metadata")]
async fn get_message_metadata(message_id: String) -> Result<Json<String>, Cow<'static, str>> {
    let request = Mainnet.select::<Bee<MessageMetadata>>(&MessageId::from_str(&message_id).unwrap().into());
    query(request).await
}

#[get("/messages/<message_id>/children")]
async fn get_message_children(message_id: String) -> Result<Json<String>, Cow<'static, str>> {
    let request = Mainnet.select::<MessageChildren>(&MessageId::from_str(&message_id).unwrap().into());
    query_iter(request).await
}

#[get("/messages?<index>")]
async fn get_message_by_index(index: String) -> Result<Json<String>, Cow<'static, str>> {
    let bytes: [u8; HASHED_INDEX_LENGTH] = hex::decode(index)
        .map_err(|_| Cow::from("Invalid Hex character in index!"))?
        .try_into()
        .map_err(|_| Cow::from("Invalid index length!"))?;

    let request = Mainnet.select::<IndexMessages>(&HashedIndex::from(bytes).into());
    query_iter(request).await
}

#[get("/outputs/<output_id>")]
async fn get_output(output_id: String) -> Result<Json<String>, Cow<'static, str>> {
    let request = Mainnet.select::<Bee<Output>>(&OutputId::from_str(&output_id).unwrap().into());
    query(request).await
}

#[get("/addresses/ed25519/<address>/outputs")]
async fn get_ed25519_outputs(address: String) -> Result<Json<String>, Cow<'static, str>> {
    let request = Mainnet.select::<Outputs>(&Ed25519Address::from_str(&address).unwrap().into());
    query_iter(request).await
}

#[get("/milestones/<index>")]
async fn get_milestone(index: u32) -> Result<Json<String>, Cow<'static, str>> {
    let request = Mainnet.select::<NeedsSerialize<Milestone>>(&MilestoneIndex::from(index).into());
    query(request).await
}
