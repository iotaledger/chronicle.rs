use super::*;
use crate::responses::*;
use mpsc::unbounded_channel;
use permanode_storage::{
    access::{
        Ed25519Address,
        GetSelectRequest,
        MessageId,
        MessageMetadata,
        Milestone,
        MilestoneIndex,
        OutputId,
        OutputRes,
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
    borrow::{
        Borrow,
        Cow,
    },
    collections::{
        HashMap,
        HashSet,
        VecDeque,
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

async fn page<K, V>(
    keyspace: String,
    hint: Hint,
    page_size: usize,
    cookies: &CookieJar<'_>,
    cookie_path: String,
    partition_config: &PartitionConfig,
    key: K,
) -> Result<Vec<Partitioned<V>>, Cow<'static, str>>
where
    K: 'static + Send + Clone,
    V: 'static + Send + Clone,
    PermanodeKeyspace: Select<Partitioned<K>, Paged<VecDeque<Partitioned<V>>>>,
{
    let total_start_time = std::time::Instant::now();
    let mut start_time = total_start_time;
    // The milestone chunk, i.e. how many sequential milestones go on a partition at a time
    let milestone_chunk = partition_config.milestone_chunk_size as usize;

    // The last partition id that we got results from. This is sent back and forth between
    // the requestor to keep track of pages.
    let last_partition_id = cookies
        .get("last_partition_id")
        .map(|c| c.value().parse::<u16>().ok())
        .flatten();
    // The last milestone index we got results from.
    let last_milestone_index = cookies
        .get("last_milestone_index")
        .map(|c| c.value().parse::<u32>().ok())
        .flatten();
    let paging_state = cookies
        .get("paging_state")
        .map(|c| hex::decode(c.value()).ok())
        .flatten();

    let keyspace = PermanodeKeyspace::new(keyspace);
    // Get the list of partitions which contain records for this request
    let mut partition_ids =
        query::<Vec<(MilestoneIndex, PartitionId)>, _, _>(keyspace.clone(), hint, None, None).await?;

    if partition_ids.is_empty() {
        return Err("No results returned!".into());
    }

    debug!(
        "Setup time: {} ms",
        (std::time::Instant::now() - start_time).as_millis()
    );
    start_time = std::time::Instant::now();

    // Either use the provided partition / milestone index or the first hint record
    let (first_partition_id, latest_milestone) =
        if let (Some(last_partition_id), Some(last_milestone_index)) = (last_partition_id, last_milestone_index) {
            (last_partition_id, last_milestone_index)
        } else {
            partition_ids
                .iter()
                .max_by_key(|(index, _)| index)
                .map(|(index, id)| (*id, index.0))
                .unwrap()
        };

    // Reorder the partitions list so we start with the correct partition id
    let i = partition_ids
        .iter()
        .position(|&(_, partition_id)| first_partition_id == partition_id);
    if let Some(i) = i {
        partition_ids = partition_ids[i..]
            .iter()
            .chain(partition_ids[..i].iter())
            .cloned()
            .collect();
    }

    debug!(
        "Reorder time: {} ms",
        (std::time::Instant::now() - start_time).as_millis()
    );

    // This will hold lists of results keyed by partition id
    let mut list_map = HashMap::new();

    // The number of queries we will dispatch at a time.
    // Two queries seems to cover most cases. In extreme circumstances we can fetch more as needed.
    let fetch_size = 2;
    // The resulting list
    let mut results = Vec::new();
    let mut depleted_partitions = HashSet::new();
    let mut last_index_map = HashMap::new();
    last_index_map.insert(partition_ids[0].1, last_milestone_index.unwrap_or(latest_milestone));
    let mut loop_timings = HashMap::new();
    for (partition_ind, (index, partition_id)) in partition_ids.iter().enumerate().cycle() {
        if !last_index_map.contains_key(partition_id) {
            last_index_map.insert(*partition_id, index.0);
        }
        debug!("Gathering results from partition {}", partition_id);
        // Make sure we stop iterating if all of our partitions are depleted.
        if depleted_partitions.len() == partition_ids.len() {
            // Remove the cookies so the client knows it's done
            cookies.remove(Cookie::named("last_partition_id"));
            cookies.remove(Cookie::named("last_milestone_index"));
            cookies.remove(Cookie::named("paging_state"));
            break;
        }
        // Skip depleted partitions
        if depleted_partitions.contains(partition_id) {
            debug!("Skipping partition");
            continue;
        }

        // Fetch a chunk of results if we need them to fill the page size
        if !list_map.contains_key(partition_id) {
            start_time = std::time::Instant::now();
            let fetch_ids =
                (partition_ind..partition_ind + fetch_size).filter_map(|ind| partition_ids.get(ind).map(|v| v.1));
            let res = futures::future::join_all(fetch_ids.clone().map(|partition_id| {
                debug!(
                    "Fetching results for partition id: {}, milestone: {}, with paging state: {:?}",
                    partition_id,
                    latest_milestone,
                    last_partition_id.map(|id| partition_id == id)
                );
                query::<Paged<VecDeque<Partitioned<V>>>, _, _>(
                    keyspace.clone(),
                    Partitioned::new(key.clone(), partition_id, latest_milestone),
                    Some(page_size as i32),
                    last_partition_id.and_then(|id| if partition_id == id { paging_state.clone() } else { None }),
                )
            }))
            .await;
            debug!(
                "Fetch time: {} ms",
                (std::time::Instant::now() - start_time).as_millis()
            );
            for (partition_id, list) in fetch_ids.zip(res) {
                list_map.insert(partition_id, list);
            }
        }
        // Get the list from the map
        let list = match list_map.get_mut(&partition_id).unwrap().as_mut() {
            Ok(list) => list,
            Err(msg) => return Err(msg.to_owned()),
        };

        // Iterate the list, pulling records from the front until we hit
        // a milestone in the next chunk or run out
        loop {
            let loop_start_time = std::time::Instant::now();
            if !list.is_empty() {
                // If we're still looking at the same chunk
                if list[0].milestone_index() / milestone_chunk as u32
                    == last_index_map[partition_id] / milestone_chunk as u32
                {
                    // And we exceeded the page size
                    if results.len() >= page_size {
                        // Add more anyway if the milestone index is the same,
                        // because we won't be able to recover lost records
                        // with a paging state
                        if last_index_map[partition_id] == list[0].milestone_index() {
                            // debug!("Adding extra records past page_size");
                            results.push(list.pop_front().unwrap());
                            *loop_timings.entry("Adding additional").or_insert(0) +=
                                (std::time::Instant::now() - loop_start_time).as_nanos();
                        // Otherwise we can stop here and set our cookies
                        } else {
                            debug!("Finished a milestone");
                            cookies.add(
                                Cookie::build("last_partition_id", partition_id.to_string())
                                    .path(cookie_path.clone())
                                    .finish(),
                            );
                            cookies.add(
                                Cookie::build("last_milestone_index", list[0].milestone_index().to_string())
                                    .path(cookie_path.clone())
                                    .finish(),
                            );
                            cookies.remove(Cookie::named("paging_state"));
                            *loop_timings.entry("Finish Adding Additional").or_insert(0) +=
                                (std::time::Instant::now() - loop_start_time).as_nanos();
                            debug!(
                                "{:#?}",
                                loop_timings
                                    .iter()
                                    .map(|(k, v)| (k, format!("{} ms", *v as f32 / 1000000.0)))
                                    .collect::<HashMap<_, _>>()
                            );
                            debug!(
                                "Total time: {} ms",
                                (std::time::Instant::now() - total_start_time).as_millis()
                            );
                            return Ok(results);
                        }
                    // Otherwise, business as usual
                    } else {
                        let partitioned_value = list.pop_front().unwrap();
                        // debug!("Adding result normally");
                        last_index_map.insert(*partition_id, partitioned_value.milestone_index());
                        results.push(partitioned_value);
                        *loop_timings.entry("Adding normally").or_insert(0) +=
                            (std::time::Instant::now() - loop_start_time).as_nanos();
                    }
                // We hit a new chunk, so we want to look at the next partition now
                } else {
                    debug!("Hit a chunk boundary");
                    last_index_map.insert(*partition_id, list[0].milestone_index());
                    // Remove our paging state because it's useless now
                    cookies.remove(Cookie::named("paging_state"));
                    *loop_timings.entry("Chunk Boundary").or_insert(0) +=
                        (std::time::Instant::now() - loop_start_time).as_nanos();
                    break;
                }
            // The list is empty, but that doesn't necessarily mean there aren't more valid records on this partition.
            // So we will get the next page_size records by re-running the same query with the paging state
            // or just give it to the client if we already have enough records.
            } else {
                debug!("Results list is empty");
                if results.len() >= page_size {
                    debug!("...but we already have enough results so returning the paging state");
                    if let Some(ref paging_state) = list.paging_state {
                        cookies.add(
                            Cookie::build("paging_state", hex::encode(paging_state))
                                .path(cookie_path.clone())
                                .finish(),
                        );
                    } else {
                        cookies.remove(Cookie::named("paging_state"));
                    }
                    cookies.add(
                        Cookie::build("last_partition_id", partition_id.to_string())
                            .path(cookie_path.clone())
                            .finish(),
                    );
                    cookies.add(
                        Cookie::build("last_milestone_index", latest_milestone.to_string())
                            .path(cookie_path.clone())
                            .finish(),
                    );
                    *loop_timings.entry("Returning page_state").or_insert(0) +=
                        (std::time::Instant::now() - loop_start_time).as_nanos();
                    debug!(
                        "{:#?}",
                        loop_timings
                            .iter()
                            .map(|(k, v)| (k, format!("{} ms", *v as f32 / 1000000.0)))
                            .collect::<HashMap<_, _>>()
                    );
                    debug!(
                        "Total time: {} ms",
                        (std::time::Instant::now() - total_start_time).as_millis()
                    );
                    return Ok(results);
                } else {
                    debug!("...and we need more results");
                    if list.paging_state.is_some() {
                        debug!("......so we're querying for them");
                        *list = query::<Paged<VecDeque<Partitioned<V>>>, _, _>(
                            keyspace.clone(),
                            Partitioned::new(key.clone(), *partition_id, latest_milestone),
                            Some(page_size as i32),
                            paging_state.clone(),
                        )
                        .await
                        .unwrap();
                        *loop_timings.entry("Requery").or_insert(0) +=
                            (std::time::Instant::now() - loop_start_time).as_nanos();
                    // Unless it didn't have one, in which case we mark it as a depleted partition and
                    // move on to the next one.
                    } else {
                        debug!("......but there's no paging state");
                        depleted_partitions.insert(*partition_id);
                        *loop_timings.entry("Depleted partition").or_insert(0) +=
                            (std::time::Instant::now() - loop_start_time).as_nanos();
                        break;
                    }
                }
            }
        }
    }

    debug!(
        "{:#?}",
        loop_timings
            .iter()
            .map(|(k, v)| (k, format!("{} ms", *v as f32 / 1000000.0)))
            .collect::<HashMap<_, _>>()
    );

    debug!(
        "Total time: {} ms",
        (std::time::Instant::now() - total_start_time).as_millis()
    );

    Ok(results)
}

#[get("/<keyspace>/messages/<message_id>")]
async fn get_message(
    keyspace: String,
    message_id: String,
) -> Result<Json<SuccessBody<MessageResponse>>, Cow<'static, str>> {
    let keyspace = PermanodeKeyspace::new(keyspace);
    query::<Message, _, _>(keyspace, MessageId::from_str(&message_id).unwrap(), None, None)
        .await
        .and_then(|message| {
            message
                .try_into()
                .map(|res: MessageResponse| Json(res.into()))
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
async fn get_message_children(
    keyspace: String,
    message_id: String,
    page_size: Option<usize>,
    cookies: &CookieJar<'_>,
    partition_config: State<'_, PartitionConfig>,
) -> Result<Json<SuccessBody<MessageChildrenResponse>>, Cow<'static, str>> {
    let message_id = MessageId::from_str(&message_id).unwrap();
    let page_size = page_size.unwrap_or(100);

    let mut messages = page(
        keyspace.clone(),
        Hint::parent(message_id.to_string()),
        page_size,
        cookies,
        format!("/api/{}/messages/{}/children", keyspace, message_id.to_string()),
        partition_config.borrow(),
        message_id,
    )
    .await?;

    Ok(Json(
        MessageChildrenResponse {
            message_id: message_id.to_string(),
            max_results: 2 * page_size,
            count: messages.len(),
            children_message_ids: messages.drain(..).map(|record| record.into()).collect(),
        }
        .into(),
    ))
}

#[get("/<keyspace>/messages?<index>&<page_size>")]
async fn get_message_by_index(
    keyspace: String,
    index: String,
    page_size: Option<usize>,
    cookies: &CookieJar<'_>,
    partition_config: State<'_, PartitionConfig>,
) -> Result<Json<SuccessBody<MessagesForIndexResponse>>, Cow<'static, str>> {
    if index.len() > 64 {
        return Err("Provided index is too large! (Max 64 characters)".into());
    }
    let indexation = Indexation(index.clone());
    let page_size = page_size.unwrap_or(1000);

    let mut messages = page(
        keyspace.clone(),
        Hint::index(index.clone()),
        page_size,
        cookies,
        format!("/api/{}/messages", keyspace),
        partition_config.borrow(),
        indexation,
    )
    .await?;

    Ok(Json(
        MessagesForIndexResponse {
            index,
            max_results: 2 * page_size,
            count: messages.len(),
            message_ids: messages.drain(..).map(|record| record.into()).collect(),
        }
        .into(),
    ))
}

#[get("/<keyspace>/addresses/ed25519/<address>/outputs?<page_size>")]
async fn get_ed25519_outputs(
    keyspace: String,
    address: String,
    page_size: Option<usize>,
    cookies: &CookieJar<'_>,
    partition_config: State<'_, PartitionConfig>,
) -> Result<Json<SuccessBody<OutputsForAddressResponse>>, Cow<'static, str>> {
    let ed25519_address = Ed25519Address::from_str(&address).unwrap();
    let page_size = page_size.unwrap_or(100);

    let mut outputs = page(
        keyspace.clone(),
        Hint::address(ed25519_address.to_string()),
        page_size,
        cookies,
        format!("api/{}/addresses/ed25519/{}/outputs", keyspace, address),
        partition_config.borrow(),
        ed25519_address,
    )
    .await?;

    Ok(Json(
        OutputsForAddressResponse {
            address_type: 1,
            address,
            max_results: 2 * page_size,
            count: outputs.len(),
            output_ids: outputs.drain(..).map(|record| record.into()).collect(),
        }
        .into(),
    ))
}

#[get("/<keyspace>/outputs/<output_id>")]
async fn get_output(
    keyspace: String,
    output_id: String,
) -> Result<Json<SuccessBody<OutputResponse>>, Cow<'static, str>> {
    let output_id = OutputId::from_str(&output_id).unwrap();

    let output_data = query::<OutputRes, _, _>(PermanodeKeyspace::new(keyspace.clone()), output_id, None, None).await?;
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

#[get("/<keyspace>/milestones/<index>")]
async fn get_milestone(
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
