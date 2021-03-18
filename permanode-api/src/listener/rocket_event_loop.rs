use super::*;
use bee_rest_api::types::responses::{
    InfoResponse,
    MessageChildrenResponse,
    MessageResponse,
    MessagesForIndexResponse,
    MilestoneResponse,
    OutputResponse,
    OutputsForAddressResponse,
};
use mpsc::unbounded_channel;
use permanode_storage::{
    access::{
        Ed25519Address,
        GetSelectRequest,
        MessageId,
        MessageMetadata,
        Milestone,
        MilestoneIndex,
        OutputData,
        OutputId,
        PartitionId,
        Partitioned,
    },
    keyspaces::PermanodeKeyspace,
    PartitionConfig,
};
use rocket::{
    fairing::{
        Fairing,
        Info,
        Kind,
    },
    get,
    http::{
        Cookie,
        CookieJar,
    },
    Request,
    Response,
    State,
};
use rocket_contrib::json::Json;
use scylla_cql::{
    Consistency,
    TryInto,
};
use std::{
    borrow::Cow,
    collections::{
        HashMap,
        HashSet,
    },
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

        construct_rocket(
            self.data
                .rocket
                .take()
                .ok_or(Need::Abort)?
                .manage(self.storage_config.partition_config.clone()),
        )
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
        confirmed_milestone_index: 0,
        pruning_index: 0,
        features: vec![],
        min_pow_score: 0.0,
    }))
}

async fn query<V, S, K>(
    keyspace: S,
    key: K,
    page_size: Option<i32>,
    paging_state: Option<Vec<u8>>,
) -> Result<V, Cow<'static, str>>
where
    S: 'static + Select<K, V>,
    K: 'static + Send + Clone,
    V: 'static + Send + Clone,
{
    let request = keyspace.select::<V>(&key).consistency(Consistency::One);
    let request = if let Some(page_size) = page_size {
        request.page_size(page_size).paging_state(&paging_state)
    } else {
        request.paging_state(&paging_state)
    }
    .build();

    let (sender, mut inbox) = unbounded_channel::<Result<Option<V>, WorkerError>>();
    let worker = ValueWorker::boxed(sender, keyspace, key, PhantomData);

    request.send_local(worker);

    while let Some(event) = inbox.recv().await {
        match event {
            Ok(res) => return res.ok_or("No results returned!".into()),
            Err(worker_error) => return Err(format!("{:?}", worker_error).into()),
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
    query::<Message, _, _>(keyspace, MessageId::from_str(&message_id).unwrap(), None, None)
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
    Ok(Json(
        query::<MessageMetadata, _, _>(keyspace, message_id, None, None)
            .await?
            .into(),
    ))
}

#[get("/<keyspace>/messages/<message_id>/children?<page_size>")]
pub async fn get_message_children(
    keyspace: String,
    message_id: String,
    page_size: usize,
    cookies: &CookieJar<'_>,
    partition_config: State<'_, PartitionConfig>,
) -> Result<Json<SuccessBody<MessageChildrenResponse>>, Cow<'static, str>> {
    let milestone_chunk = partition_config.milestone_chunk_size as usize;

    let paging_state = cookies.get("paging_state").map(|c| c.value());
    let mut paging_states = paging_state
        .map(|h| bincode::deserialize::<HashMap<PartitionId, Cow<[u8]>>>(hex::decode(h).unwrap().as_slice()).unwrap())
        .unwrap_or_default();

    let keyspace = PermanodeKeyspace::new(keyspace);
    let message_id = MessageId::from_str(&message_id).unwrap();
    let mut partition_ids = query::<Vec<(MilestoneIndex, PartitionId)>, _, _>(
        keyspace.clone(),
        Hint::parent(message_id.to_string()),
        None,
        None,
    )
    .await?;
    let max_results = partition_ids.len() * page_size;
    let latest_milestone = partition_ids.first().ok_or("No records found!")?.0;
    let res = futures::future::join_all(partition_ids.iter().map(|(_, partition_id)| {
        query::<Paged<Vec<(MessageId, MilestoneIndex)>>, _, _>(
            keyspace.clone(),
            Partitioned::new(message_id, *partition_id),
            Some(page_size as i32),
            paging_states.get(partition_id).cloned().map(|state| state.into_owned()),
        )
    }))
    .await;
    let mut res = partition_ids.iter().map(|v| v.1).zip(res).collect::<HashMap<_, _>>();
    paging_states = res
        .iter()
        .filter_map(|(&partition_id, paged)| {
            paged.as_ref().ok().and_then(|paged| {
                paged
                    .paging_state
                    .as_ref()
                    .map(|state| (partition_id, Cow::from(state.to_owned())))
            })
        })
        .collect();

    let mut messages = Vec::new();
    while !partition_ids.is_empty() && messages.len() < page_size {
        let partition_id = partition_ids[(messages.len() / milestone_chunk) % partition_ids.len()].1;
        let list = res
            .get_mut(&partition_id)
            .unwrap()
            .as_mut()
            .expect("Failed to retrieve records from a partition!");
        if list.is_empty() {
            let partition_id = partition_ids
                .remove((messages.len() / milestone_chunk) % partition_ids.len())
                .1;
            paging_states.remove(&partition_id);
            continue;
        }
        while messages.len() < page_size
            && list.first().is_some()
            && (latest_milestone.0 < milestone_chunk as u32
                || list.first().unwrap().1 .0 > latest_milestone.0 - milestone_chunk as u32)
        {
            messages.push(list.remove(0).0);
        }
    }

    if !paging_states.is_empty() {
        let new_paging_states = bincode::serialize(&paging_states).unwrap();

        cookies.add(Cookie::new("paging_state", hex::encode(new_paging_states)));
    }

    Ok(Json(
        MessageChildrenResponse {
            message_id: message_id.to_string(),
            max_results,
            count: messages.len(),
            children_message_ids: messages.iter().map(|id| id.to_string()).collect(),
        }
        .into(),
    ))
}

#[get("/<keyspace>/messages?<index>&<page_size>")]
pub async fn get_message_by_index(
    keyspace: String,
    index: String,
    page_size: usize,
    cookies: &CookieJar<'_>,
    partition_config: State<'_, PartitionConfig>,
) -> Result<Json<SuccessBody<MessagesForIndexResponse>>, Cow<'static, str>> {
    if index.len() > 64 {
        return Err("Provided index is too large! (Max 64 characters)".into());
    }
    let milestone_chunk = partition_config.milestone_chunk_size as usize;

    let paging_state = cookies.get("paging_state").map(|c| c.value());
    let mut paging_states = paging_state
        .map(|h| bincode::deserialize::<HashMap<PartitionId, Cow<[u8]>>>(hex::decode(h).unwrap().as_slice()).unwrap())
        .unwrap_or_default();

    let keyspace = PermanodeKeyspace::new(keyspace);
    let mut partition_ids = query(keyspace.clone(), Hint::index(index.clone()), None, None).await?;
    let max_results = partition_ids.len() * page_size;
    let latest_milestone = partition_ids.first().ok_or("No records found!")?.0;
    let res = futures::future::join_all(partition_ids.iter().map(|(_, partition_id)| {
        query::<Paged<Vec<(MessageId, MilestoneIndex)>>, _, _>(
            keyspace.clone(),
            Partitioned::new(Indexation(index.clone()), *partition_id),
            Some(page_size as i32),
            paging_states.get(partition_id).cloned().map(|state| state.into_owned()),
        )
    }))
    .await;
    let mut res = partition_ids.iter().map(|v| v.1).zip(res).collect::<HashMap<_, _>>();
    paging_states = res
        .iter()
        .filter_map(|(&partition_id, paged)| {
            paged.as_ref().ok().and_then(|paged| {
                paged
                    .paging_state
                    .as_ref()
                    .map(|state| (partition_id, Cow::from(state.to_owned())))
            })
        })
        .collect();

    let mut messages = Vec::new();
    while !partition_ids.is_empty() && messages.len() < page_size {
        let partition_id = partition_ids[(messages.len() / milestone_chunk) % partition_ids.len()].1;
        let list = res
            .get_mut(&partition_id)
            .unwrap()
            .as_mut()
            .expect("Failed to retrieve records from a partition!");
        if list.is_empty() {
            let partition_id = partition_ids
                .remove((messages.len() / milestone_chunk) % partition_ids.len())
                .1;
            paging_states.remove(&partition_id);
            continue;
        }
        while messages.len() < page_size
            && list.first().is_some()
            && (latest_milestone.0 < milestone_chunk as u32
                || list.first().unwrap().1 .0 > latest_milestone.0 - milestone_chunk as u32)
        {
            messages.push(list.remove(0).0);
        }
    }

    if !paging_states.is_empty() {
        let new_paging_states = bincode::serialize(&paging_states).unwrap();

        cookies.add(Cookie::new("paging_state", hex::encode(new_paging_states)));
    }

    Ok(Json(
        MessagesForIndexResponse {
            index,
            max_results,
            count: messages.len(),
            message_ids: messages.iter().map(|id| id.to_string()).collect(),
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

    let output_data =
        query::<OutputData, _, _>(PermanodeKeyspace::new(keyspace.clone()), output_id, None, None).await?;
    let is_spent = if output_data.unlock_blocks.is_empty() {
        false
    } else {
        let mut is_spent = false;
        let mut query_message_ids = HashSet::new();
        for UnlockRes {
            message_id,
            block: _,
            inclusion_state,
        } in output_data.unlock_blocks.iter()
        {
            if *inclusion_state == Some(LedgerInclusionState::Included) {
                is_spent = true;
                break;
            } else {
                query_message_ids.insert(message_id);
            }
        }
        if !query_message_ids.is_empty() {
            let queries = query_message_ids.drain().map(|&message_id| {
                query::<MessageMetadata, _, _>(PermanodeKeyspace::new(keyspace.clone()), message_id.clone(), None, None)
            });
            is_spent = futures::future::join_all(queries)
                .await
                .drain(..)
                .filter_map(|res| res.ok())
                .any(|metadata| metadata.ledger_inclusion_state == Some(LedgerInclusionState::Included));
        }
        is_spent
    };
    Ok(Json(
        OutputResponse {
            message_id: output_data.output.message_id().to_string(),
            transaction_id: output_id.transaction_id().to_string(),
            output_index: output_id.index(),
            is_spent,
            output: output_data.output.inner().try_into().map_err(|e| Cow::from(e))?,
        }
        .into(),
    ))
}

#[get("/<keyspace>/addresses/ed25519/<address>/outputs?<page_size>")]
pub async fn get_ed25519_outputs(
    keyspace: String,
    address: String,
    page_size: usize,
    cookies: &CookieJar<'_>,
    partition_config: State<'_, PartitionConfig>,
) -> Result<Json<SuccessBody<OutputsForAddressResponse>>, Cow<'static, str>> {
    let milestone_chunk = partition_config.milestone_chunk_size as usize;

    let paging_state = cookies.get("paging_state").map(|c| c.value());
    let mut paging_states = paging_state
        .map(|h| bincode::deserialize::<HashMap<PartitionId, Cow<[u8]>>>(hex::decode(h).unwrap().as_slice()).unwrap())
        .unwrap_or_default();

    let keyspace = PermanodeKeyspace::new(keyspace);

    let ed25519_address = Ed25519Address::from_str(&address).unwrap();
    let mut partition_ids = query(keyspace.clone(), Hint::address(ed25519_address.to_string()), None, None).await?;
    let max_results = partition_ids.len() * page_size;
    let latest_milestone = partition_ids.first().ok_or("No records found!")?.0;
    let res = futures::future::join_all(partition_ids.iter().map(|(_, partition_id)| {
        query::<Paged<Vec<(OutputId, MilestoneIndex)>>, _, _>(
            keyspace.clone(),
            Partitioned::new(ed25519_address, *partition_id),
            Some(page_size as i32),
            paging_states.get(partition_id).cloned().map(|state| state.into_owned()),
        )
    }))
    .await;
    let mut res = partition_ids.iter().map(|v| v.1).zip(res).collect::<HashMap<_, _>>();
    paging_states = res
        .iter()
        .filter_map(|(&partition_id, paged)| {
            paged.as_ref().ok().and_then(|paged| {
                paged
                    .paging_state
                    .as_ref()
                    .map(|state| (partition_id, Cow::from(state.to_owned())))
            })
        })
        .collect();

    let mut outputs = Vec::new();
    while !partition_ids.is_empty() && outputs.len() < page_size {
        let partition_id = partition_ids[(outputs.len() / milestone_chunk) % partition_ids.len()].1;
        let list = res
            .get_mut(&partition_id)
            .unwrap()
            .as_mut()
            .expect("Failed to retrieve records from a partition!");
        if list.is_empty() {
            let partition_id = partition_ids
                .remove((outputs.len() / milestone_chunk) % partition_ids.len())
                .1;
            paging_states.remove(&partition_id);
            continue;
        }
        while outputs.len() < page_size
            && list.first().is_some()
            && (latest_milestone.0 < milestone_chunk as u32
                || list.first().unwrap().1 .0 > latest_milestone.0 - milestone_chunk as u32)
        {
            outputs.push(list.remove(0).0);
        }
    }

    if !paging_states.is_empty() {
        let new_paging_states = bincode::serialize(&paging_states).unwrap();

        cookies.add(Cookie::new("paging_state", hex::encode(new_paging_states)));
    }

    Ok(Json(
        OutputsForAddressResponse {
            address_type: 1,
            address,
            max_results,
            count: outputs.len(),
            output_ids: outputs.iter().map(|id| id.to_string()).collect(),
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

    query::<Milestone, _, _>(keyspace, MilestoneIndex::from(index), None, None)
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
    use bee_rest_api::types::responses::InfoResponse;
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
