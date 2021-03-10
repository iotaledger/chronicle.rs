use super::*;
use bee_rest_api::handlers::{
    info::InfoResponse,
    message::MessageResponse,
    message_children::MessageChildrenResponse,
    messages_find::MessagesForIndexResponse,
    milestone::MilestoneResponse,
    output::OutputResponse,
    outputs_ed25519::OutputsForAddressResponse,
};
use mpsc::unbounded_channel;
use permanode_storage::{
    access::{
        CreatedOutput,
        Ed25519Address,
        GetSelectRequest,
        HashedIndex,
        IndexMessages,
        MessageChildren,
        MessageId,
        MessageMetadata,
        Milestone,
        MilestoneIndex,
        OutputId,
        OutputIds,
        Outputs,
        TransactionData,
        HASHED_INDEX_LENGTH,
    },
    keyspaces::PermanodeKeyspace,
};
use rocket::{
    fairing::{
        Fairing,
        Info,
        Kind,
    },
    get,
    Request,
    Response,
};
use rocket_contrib::json::Json;
use scylla_cql::{
    Consistency,
    TryInto,
};
use std::{
    borrow::Cow,
    path::PathBuf,
    str::FromStr,
};
use tokio::sync::mpsc;

#[async_trait]
impl<H: PermanodeAPIScope> EventLoop<PermanodeAPISender<H>> for Listener<RocketListener> {
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

        construct_rocket(self.data.rocket.take().ok_or(Need::Abort)?)
            .launch()
            .await
            .map_err(|_| Need::Abort)
    }
}

fn construct_rocket(rocket: Rocket) -> Rocket {
    rocket
        .mount(
            "/api",
            routes![
                options,
                info,
                get_message,
                get_message_metadata,
                get_message_children,
                get_message_by_index,
                get_output,
                get_ed25519_outputs,
                get_milestone
            ],
        )
        .attach(CORS)
}

struct CORS;

#[rocket::async_trait]
impl Fairing for CORS {
    fn info(&self) -> rocket::fairing::Info {
        Info {
            name: "Add CORS Headers",
            kind: Kind::Response,
        }
    }

    async fn on_response<'r>(&self, _request: &'r Request<'_>, response: &mut Response<'r>) {
        response.set_raw_header("Access-Control-Allow-Origin", "*");
        response.set_raw_header("Access-Control-Allow-Methods", "GET, OPTIONS");
        response.set_raw_header("Access-Control-Allow-Headers", "*");
        response.set_raw_header("Access-Control-Allow-Credentials", "true");
    }
}

#[options("/<_path..>")]
async fn options(_path: PathBuf) {}

#[get("/info")]
async fn info() -> Result<Json<InfoResponse>, Cow<'static, str>> {
    Ok(Json(InfoResponse {
        name: "Permanode".into(),
        version: "1.0".into(),
        is_healthy: true,
        network_id: "network id".into(),
        bech32_hrp: "bech32 hrp".into(),
        latest_milestone_index: 0,
        solid_milestone_index: 0,
        pruning_index: 0,
        features: vec![],
        min_pow_score: 0.0,
    }))
}

async fn query<V, S, K>(keyspace: S, key: K) -> Result<V, Cow<'static, str>>
where
    S: 'static + Select<K, V> + std::fmt::Debug,
    K: 'static + Send + std::fmt::Debug + Clone,
    V: 'static + Send + std::fmt::Debug + Clone,
{
    let request = keyspace.select::<V>(&key).consistency(Consistency::One).build();

    let (sender, mut inbox) = unbounded_channel::<Event>();
    let worker = Box::new(DecoderWorker {
        sender,
        keyspace: keyspace,
        key,
        value: PhantomData,
    });

    let decoder = request.send_local(worker);

    while let Some(event) = inbox.recv().await {
        match event {
            Event::Response { giveload } => {
                let res = decoder.decode(giveload);
                match res {
                    Ok(v) => return v.ok_or("No results returned!".into()),
                    Err(cql_error) => return Err(format!("{:?}", cql_error).into()),
                }
            }
            Event::Error { kind } => return Err(kind.to_string().into()),
        }
    }

    Err("Failed to receive response!".into())
}

#[get("/<keyspace>/messages/<message_id>")]
pub async fn get_message(
    keyspace: String,
    message_id: String,
) -> Result<Json<SuccessBody<MessageResponse>>, Cow<'static, str>> {
    let keyspace = PermanodeKeyspace::new(keyspace);
    query::<Message, _, _>(keyspace, MessageId::from_str(&message_id).unwrap())
        .await
        .and_then(|ref message| {
            message
                .try_into()
                .map(|dto| Json(MessageResponse(dto).into()))
                .map_err(|e| e.into())
        })
}

#[get("/<keyspace>/messages/<message_id>/metadata")]
pub async fn get_message_metadata(
    keyspace: String,
    message_id: String,
) -> Result<Json<SuccessBody<MessageMetadata>>, Cow<'static, str>> {
    let keyspace = PermanodeKeyspace::new(keyspace);
    let message_id = MessageId::from_str(&message_id).unwrap();
    Ok(Json(query::<MessageMetadata, _, _>(keyspace, message_id).await?.into()))
}

#[get("/<keyspace>/messages/<message_id>/children")]
pub async fn get_message_children(
    keyspace: String,
    message_id: String,
) -> Result<Json<SuccessBody<MessageChildrenResponse>>, Cow<'static, str>> {
    let keyspace = PermanodeKeyspace::new(keyspace);

    // TODO: Paging
    let children = query::<MessageChildren, _, _>(keyspace, MessageId::from_str(&message_id).unwrap())
        .await?
        .children;
    let count = children.len();
    let max_results = 1000;

    Ok(Json(
        MessageChildrenResponse {
            message_id,
            max_results,
            count,
            children_message_ids: children.iter().take(max_results).map(|id| id.to_string()).collect(),
        }
        .into(),
    ))
}

#[get("/<keyspace>/messages?<index>")]
pub async fn get_message_by_index(
    keyspace: String,
    index: String,
) -> Result<Json<SuccessBody<MessagesForIndexResponse>>, Cow<'static, str>> {
    let mut bytes_vec = vec![0; HASHED_INDEX_LENGTH];
    let bytes = hex::decode(index.clone()).map_err(|_| "Invalid Hex character in index!")?;
    bytes.iter().enumerate().for_each(|(i, &b)| bytes_vec[i] = b);

    let keyspace = PermanodeKeyspace::new(keyspace);

    // TODO: Paging
    let messages = query::<IndexMessages, _, _>(keyspace, HashedIndex::new(bytes_vec.as_slice().try_into().unwrap()))
        .await?
        .messages;
    let count = messages.len();
    let max_results = 1000;

    Ok(Json(
        MessagesForIndexResponse {
            index,
            max_results,
            count,
            message_ids: messages.iter().take(max_results).map(|id| id.to_string()).collect(),
        }
        .into(),
    ))
}

#[get("/<keyspace>/outputs/<output_id>")]
pub async fn get_output(
    keyspace: String,
    output_id: String,
) -> Result<Json<SuccessBody<OutputResponse>>, Cow<'static, str>> {
    let output_id = OutputId::from_str(&output_id).unwrap();
    let keyspace = PermanodeKeyspace::new(keyspace);

    let mut outputs = query::<Outputs, _, _>(keyspace, output_id).await?.outputs;
    let (output, is_spent) = {
        let mut output = None;
        let mut is_spent = false;
        for (message_id, data) in outputs.drain(..) {
            match data {
                TransactionData::Input(_) => {}
                TransactionData::Output(o) => {
                    output = Some(CreatedOutput::new(message_id, o));
                }
                TransactionData::Unlock(_) => {
                    is_spent = true;
                }
            }
        }
        (
            output.ok_or(Cow::from(format!("No output found for id {}", output_id)))?,
            is_spent,
        )
    };
    Ok(Json(
        OutputResponse {
            message_id: output.message_id().to_string(),
            transaction_id: output_id.transaction_id().to_string(),
            output_index: output_id.index(),
            is_spent,
            output: output.inner().try_into().map_err(|e| Cow::from(e))?,
        }
        .into(),
    ))
}

#[get("/<keyspace>/addresses/ed25519/<address>/outputs")]
pub async fn get_ed25519_outputs(
    keyspace: String,
    address: String,
) -> Result<Json<SuccessBody<OutputsForAddressResponse>>, Cow<'static, str>> {
    let keyspace = PermanodeKeyspace::new(keyspace);

    // TODO: Paging
    let outputs = query::<OutputIds, _, _>(keyspace, Ed25519Address::from_str(&address).unwrap())
        .await?
        .ids;
    let count = outputs.len();
    let max_results = 1000;

    Ok(Json(
        OutputsForAddressResponse {
            address_type: 1,
            address,
            max_results,
            count,
            output_ids: outputs.iter().take(max_results).map(|id| id.to_string()).collect(),
        }
        .into(),
    ))
}

#[get("/<keyspace>/milestones/<index>")]
pub async fn get_milestone(
    keyspace: String,
    index: u32,
) -> Result<Json<SuccessBody<MilestoneResponse>>, Cow<'static, str>> {
    let keyspace = PermanodeKeyspace::new(keyspace);

    query::<Milestone, _, _>(keyspace, MilestoneIndex::from(index))
        .await
        .map(|milestone| {
            Json(
                MilestoneResponse {
                    milestone_index: index,
                    message_id: milestone.message_id().to_string(),
                    timestamp: milestone.timestamp(),
                }
                .into(),
            )
        })
        .map_err(|_| Cow::from(format!("No milestone found for index {}", index)))
}

#[cfg(test)]
mod tests {
    use super::construct_rocket;
    use bee_rest_api::handlers::info::InfoResponse;
    use rocket::{
        http::{
            ContentType,
            Header,
            Status,
        },
        local::asynchronous::{
            Client,
            LocalResponse,
        },
    };

    fn check_cors_headers(res: &LocalResponse) {
        assert_eq!(
            res.headers().get_one("Access-Control-Allow-Origin"),
            Some(Header::new("Access-Control-Allow-Origin", "*").value())
        );
        assert_eq!(
            res.headers().get_one("Access-Control-Allow-Methods"),
            Some(Header::new("Access-Control-Allow-Methods", "GET, OPTIONS").value())
        );
        assert_eq!(
            res.headers().get_one("Access-Control-Allow-Headers"),
            Some(Header::new("Access-Control-Allow-Headers", "*").value())
        );
        assert_eq!(
            res.headers().get_one("Access-Control-Allow-Credentials"),
            Some(Header::new("Access-Control-Allow-Credentials", "true").value())
        );
    }

    #[rocket::async_test]
    async fn options() {
        let rocket = construct_rocket(rocket::ignite());
        let client = Client::tracked(rocket).await.expect("Invalid rocket instance!");

        let res = client.options("/api/anything").dispatch().await;
        assert_eq!(res.status(), Status::Ok);
        assert_eq!(res.content_type(), None);
        check_cors_headers(&res);
        assert!(res.into_string().await.is_none());
    }

    #[rocket::async_test]
    async fn info() {
        let rocket = construct_rocket(rocket::ignite());
        let client = Client::tracked(rocket).await.expect("Invalid rocket instance!");

        let res = client.get("/api/info").dispatch().await;
        assert_eq!(res.status(), Status::Ok);
        assert_eq!(res.content_type(), Some(ContentType::JSON));
        check_cors_headers(&res);
        let _body: InfoResponse = serde_json::from_str(&res.into_string().await.expect("No body returned!"))
            .expect("Failed to deserialize Info Response!");
    }

    #[rocket::async_test]
    async fn get_message() {
        let rocket = construct_rocket(rocket::ignite());
        let client = Client::tracked(rocket).await.expect("Invalid rocket instance!");

        let res = client
            .get("/api/permanode/messages/91515c13d2025f79ded3758abe5dc640591c3b6d58b1c52cd51d1fa0585774bc")
            .dispatch()
            .await;
        assert_eq!(res.status(), Status::Ok);
        assert_eq!(res.content_type(), Some(ContentType::Plain));
        check_cors_headers(&res);
        assert_eq!(res.into_string().await, Some("Worker NoRing".to_owned()));
    }
}
