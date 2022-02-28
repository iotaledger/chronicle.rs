// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::responses::*;
use ::rocket::{
    fairing::{
        Fairing,
        Info,
        Kind,
    },
    get,
    http::ContentType,
    response::{
        content,
        Responder,
    },
    serde::json::Json,
    Build,
    Data,
    Request,
    Response,
};
use anyhow::anyhow;
use bee_message::{
    address::{
        Address,
        Ed25519Address,
    },
    milestone::{
        Milestone,
        MilestoneIndex,
    },
    output::OutputId,
    payload::{
        transaction::TransactionId,
        Payload,
    },
    Message,
    MessageId,
};
use bee_rest_api::types::responses::MessageMetadataResponse;
use chronicle_common::{
    metrics::{
        prometheus::{
            self,
            Encoder,
            TextEncoder,
        },
        INCOMING_REQUESTS,
        REGISTRY,
        RESPONSE_CODE_COLLECTOR,
        RESPONSE_TIME_COLLECTOR,
    },
    SyncRange,
};
use chronicle_storage::{
    access::OutputRes,
    keyspaces::ChronicleKeyspace,
};
use chrono::NaiveDateTime;
use futures::{
    StreamExt,
    TryStreamExt,
};
use hex::FromHex;
use rand::Rng;
use rocket_dyn_templates::Template;
use std::{
    borrow::Borrow,
    collections::{
        HashSet,
        VecDeque,
    },
    convert::TryInto,
    fmt::Debug,
    io::Cursor,
    ops::Range,
    path::PathBuf,
    str::FromStr,
    time::SystemTime,
};

#[allow(missing_docs)]
pub fn construct_rocket() -> Rocket<Build> {
    ::rocket::build()
        .mount(
            "/api",
            routes![
                options,
                info,
                metrics,
                service,
                sync,
                get_message,
                get_message_metadata,
                get_message_children,
                get_message_by_index,
                get_output_by_transaction_id,
                get_output,
                get_outputs_by_address,
                get_transactions_for_address,
                get_transaction_history_for_address,
                get_transaction_for_message,
                get_transaction_included_message,
                get_milestone,
                get_analytics,
                active_addresses_graph
            ],
        )
        .attach(Template::fairing())
        .attach(CORS)
        .attach(RequestTimer)
        .register("/", catchers![internal_error, not_found])
}

struct CORS;

#[::rocket::async_trait]
impl Fairing for CORS {
    fn info(&self) -> ::rocket::fairing::Info {
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

pub struct RequestTimer;

#[derive(Copy, Clone)]
struct TimerStart(Option<SystemTime>);

#[::rocket::async_trait]
impl Fairing for RequestTimer {
    fn info(&self) -> Info {
        Info {
            name: "Request Timer",
            kind: Kind::Request | Kind::Response,
        }
    }

    /// Stores the start time of the request in request-local state.
    async fn on_request(&self, request: &mut Request<'_>, _: &mut Data<'_>) {
        // Store a `TimerStart` instead of directly storing a `SystemTime`
        // to ensure that this usage doesn't conflict with anything else
        // that might store a `SystemTime` in request-local cache.
        request.local_cache(|| TimerStart(Some(SystemTime::now())));
        INCOMING_REQUESTS.inc();
    }

    /// Adds a header to the response indicating how long the server took to
    /// process the request.
    async fn on_response<'r>(&self, req: &'r Request<'_>, res: &mut Response<'r>) {
        let start_timestamp = req.local_cache(|| TimerStart(None));
        if let Some(Ok(duration)) = start_timestamp.0.map(|st| st.elapsed()) {
            let ms = (duration.as_secs() * 1000 + duration.subsec_millis() as u64) as f64;
            RESPONSE_TIME_COLLECTOR
                .with_label_values(&[&format!("{} {}", req.method(), req.uri())])
                .observe(ms)
        }
        match res.status().code {
            500..=599 => RESPONSE_CODE_COLLECTOR
                .with_label_values(&[&res.status().code.to_string(), "500"])
                .inc(),
            400..=499 => RESPONSE_CODE_COLLECTOR
                .with_label_values(&[&res.status().code.to_string(), "400"])
                .inc(),
            300..=399 => RESPONSE_CODE_COLLECTOR
                .with_label_values(&[&res.status().code.to_string(), "300"])
                .inc(),
            200..=299 => RESPONSE_CODE_COLLECTOR
                .with_label_values(&[&res.status().code.to_string(), "200"])
                .inc(),
            100..=199 => RESPONSE_CODE_COLLECTOR
                .with_label_values(&[&res.status().code.to_string(), "100"])
                .inc(),
            _ => (),
        }
    }
}

impl<'r> Responder<'r, 'static> for ListenerError {
    fn respond_to(self, _req: &'r Request<'_>) -> ::rocket::response::Result<'static> {
        let err = ErrorBody::from(self);
        let string = serde_json::to_string(&err).map_err(|e| {
            error!("JSON failed to serialize: {:?}", e);
            Status::InternalServerError
        })?;

        Response::build()
            .sized_body(None, Cursor::new(string))
            .status(err.status)
            .header(ContentType::JSON)
            .ok()
    }
}

impl<'r> Responder<'r, 'static> for ListenerResponseV1 {
    fn respond_to(self, req: &'r Request<'_>) -> ::rocket::response::Result<'static> {
        let success = SuccessBody::from(self);
        let string = serde_json::to_string(&success).map_err(|e| {
            error!("JSON failed to serialize: {:?}", e);
            Status::InternalServerError
        })?;

        content::Json(string).respond_to(req)
    }
}

type ListenerResult = Result<ListenerResponseV1, ListenerError>;

pub const MAX_PAGE_SIZE: usize = 200_000;

#[options("/<_path..>")]
async fn options(_path: PathBuf) {}

#[get("/info")]
async fn info() -> ListenerResult {
    let version = std::env!("CARGO_PKG_VERSION").to_string();
    let service = Scope::lookup::<Service>(0)
        .await
        .ok_or_else(|| ListenerError::NotFound)?;
    let is_healthy = !std::iter::once(&service)
        .chain(service.microservices.values())
        .any(|service| !service.is_running());
    Ok(ListenerResponseV1::Info {
        name: "Chronicle".into(),
        version,
        is_healthy,
    })
}

#[get("/metrics")]
async fn metrics() -> Result<String, ListenerError> {
    let encoder = TextEncoder::new();
    let mut buffer = Vec::new();
    encoder
        .encode(&REGISTRY.gather(), &mut buffer)
        .map_err(|e| ListenerError::Other(e.into()))?;

    let res_custom = String::from_utf8(std::mem::take(&mut buffer)).map_err(|e| ListenerError::Other(e.into()))?;

    encoder
        .encode(&prometheus::gather(), &mut buffer)
        .map_err(|e| ListenerError::Other(e.into()))?;

    let res_default = String::from_utf8(buffer).map_err(|e| ListenerError::Other(e.into()))?;

    Ok(format!("{}{}", res_custom, res_default))
}

#[get("/service")]
async fn service() -> Result<Json<Service>, ListenerError> {
    let service = Scope::lookup::<Service>(0)
        .await
        .ok_or_else(|| ListenerError::NotFound)?;
    Ok(Json(service))
}

#[get("/<keyspace>/sync")]
async fn sync(keyspace: String) -> Result<Json<SyncData>, ListenerError> {
    let keyspace = ChronicleKeyspace::new(keyspace);
    SyncData::try_fetch(&keyspace, SyncRange::default(), 3)
        .await
        .map(|s| Json(s))
        .map_err(|e| ListenerError::Other(e.into()))
}

async fn query<O, K, V, S>(
    keyspace: S,
    key: K,
    variables: V,
    page_size: Option<i32>,
    paging_state: Option<Vec<u8>>,
) -> Result<O, ListenerError>
where
    S: 'static + Select<K, V, O>,
    K: 'static + Send + Sync + Clone + TokenEncoder,
    V: 'static + Send + Sync + Clone,
    O: 'static + Send + Sync + Clone + Debug + RowsDecoder,
{
    let request = keyspace.select::<O>(&key, &variables).consistency(Consistency::One);
    if let Some(page_size) = page_size {
        request.page_size(page_size).paging_state(&paging_state)
    } else {
        request.paging_state(&paging_state)
    }
    .build()?
    .worker()
    .with_retries(3)
    .get_local()
    .await
    .map_err(|e| e.into())
    .and_then(|res| res.ok_or_else(|| ListenerError::NoResults))
}
async fn page<K, O>(
    keyspace: String,
    hint: Hint,
    page_size: usize,
    state: &mut Option<StateData>,
    range: Option<Range<NaiveDateTime>>,
    key: K,
) -> Result<Vec<O>, ListenerError>
where
    K: 'static + Send + Sync + Clone + TokenEncoder,
    O: 'static + Send + Sync + Clone + Debug,
    ChronicleKeyspace: Select<(K, MsRangeId), Range<NaiveDateTime>, Paged<Vec<O>>>,
    Paged<Vec<O>>: RowsDecoder,
{
    let total_start_timestamp = std::time::Instant::now();
    let mut start_timestamp = total_start_timestamp;

    let keyspace = ChronicleKeyspace::new(keyspace);
    let range = range.unwrap_or_else(|| chrono::naive::MIN_DATETIME..chrono::naive::MAX_DATETIME);
    // Get the list of partitions which contain records for this request.
    // These may have been passed in by the client, in which case we do not need
    // to query for them.
    match state {
        Some(state) => {
            if state.ms_range_ids.is_empty() {
                return Err(ListenerError::InvalidState);
            }
        }
        None => {
            let ms_range_ids = match hint {
                Hint::Address(address_hint) => {
                    query::<Iter<MsRangeId>, _, _, _>(keyspace.clone(), address_hint, (), None, None).await?
                }

                Hint::Tag(tag_hint) => {
                    query::<Iter<MsRangeId>, _, _, _>(keyspace.clone(), tag_hint, (), None, None).await?
                }
            };
            if ms_range_ids.is_empty() {
                return Err(ListenerError::NoResults);
            }
            state.replace((None, ms_range_ids.into_iter().collect::<VecDeque<_>>()).into());
        }
    };

    let StateData {
        ms_range_ids,
        paging_state,
    } = &mut state.as_mut().unwrap();

    debug!(
        "Setup time: {} ms",
        (std::time::Instant::now() - start_timestamp).as_millis()
    );

    // The resulting list
    let mut results = Vec::new();
    while let Some(&ms_range_id) = ms_range_ids.front() {
        debug!("Gathering results from partition {}", ms_range_id);

        if results.len() < page_size {
            // Fetch a chunk of results if we need them to fill the page size
            start_timestamp = std::time::Instant::now();
            debug!(
                "Fetching results for milestone range id: {}, num_requested: {}",
                ms_range_id,
                page_size - results.len()
            );
            let mut res = query::<Paged<Vec<O>>, _, _, _>(
                keyspace.clone(),
                (key.clone(), ms_range_id),
                range.clone(),
                Some((page_size - results.len()) as i32),
                std::mem::take(paging_state),
            )
            .await?;
            debug!(
                "Fetch time: {} ms",
                (std::time::Instant::now() - start_timestamp).as_millis()
            );
            if let Some(ps) = std::mem::take(&mut res.paging_state) {
                paging_state.replace(ps);
            } else {
                ms_range_ids.pop_front();
            }
            results.extend(res.into_iter());
        }
    }

    debug!(
        "Total time: {} ms",
        (std::time::Instant::now() - total_start_timestamp).as_millis()
    );

    Ok(results)
}

#[get("/<keyspace>/messages/<message_id>")]
async fn get_message(keyspace: String, message_id: String) -> ListenerResult {
    let keyspace = ChronicleKeyspace::new(keyspace);
    let message_id = Bee(MessageId::from_str(&message_id).map_err(|e| ListenerError::BadParse(e.into()))?);
    query::<Bee<Message>, _, _, _>(keyspace, message_id, (), None, None)
        .await
        .and_then(|message| {
            message
                .into_inner()
                .try_into()
                .map_err(|e: Cow<'static, str>| anyhow!(e).into())
        })
}

#[get("/<keyspace>/messages/<message_id>/metadata")]
async fn get_message_metadata(keyspace: String, message_id: String) -> ListenerResult {
    let keyspace = ChronicleKeyspace::new(keyspace);
    let message_id = Bee(MessageId::from_str(&message_id).map_err(|e| ListenerError::BadParse(e.into()))?);
    query::<MessageRecord, _, _, _>(keyspace, message_id, (), None, None)
        .await
        .map_err(|e| match e {
            ListenerError::NoResults => ListenerError::NotFound,
            _ => e,
        })
        .map(|res| {
            ListenerResponseV1::MessageMetadata(MessageMetadataResponse {
                message_id: res.message_id.to_string(),
                parent_message_ids: res.message.parents().iter().map(|id| id.to_string()).collect(),
                is_solid: res.inclusion_state.is_some(),
                referenced_by_milestone_index: res.inclusion_state.and(res.milestone_index.as_ref().map(|m| m.0)),
                milestone_index: res.inclusion_state.and(res.milestone_index.as_ref().map(|m| m.0)),
                should_promote: Some(res.inclusion_state.is_none()),
                should_reattach: Some(res.inclusion_state.is_none()),
                ledger_inclusion_state: res.inclusion_state.map(Into::into),
                conflict_reason: res.conflict_reason().map(|c| *c as u8),
            })
        })
}

#[get("/<keyspace>/messages/<message_id>/children?<page_size>&<expanded>&<paging_state>")]
async fn get_message_children(
    keyspace: String,
    message_id: String,
    page_size: Option<usize>,
    expanded: Option<bool>,
    paging_state: Option<String>,
) -> ListenerResult {
    let message_id = Bee(MessageId::from_str(&message_id).map_err(|e| ListenerError::BadParse(e.into()))?);
    let page_size = page_size.unwrap_or(100);

    let mut paging_state = paging_state
        .map(|state| hex::decode(state).map_err(|_| ListenerError::InvalidState))
        .transpose()?;

    let keyspace = ChronicleKeyspace::new(keyspace);

    let mut mut_page_size = page_size;
    let mut messages = Vec::new();
    while mut_page_size > MAX_PAGE_SIZE {
        let mut res = query::<Paged<Iter<ParentRecord>>, _, _, _>(
            keyspace.clone(),
            message_id,
            (),
            Some(MAX_PAGE_SIZE as i32),
            paging_state.take(),
        )
        .await?;
        paging_state = res.paging_state.take();
        messages.extend(res.into_iter());
        mut_page_size -= MAX_PAGE_SIZE;
    }
    if mut_page_size > 0 {
        let mut res = query::<Paged<Iter<ParentRecord>>, _, _, _>(
            keyspace.clone(),
            message_id,
            (),
            Some(MAX_PAGE_SIZE as i32),
            paging_state.take(),
        )
        .await?;
        paging_state = res.paging_state.take();
        messages.extend(res.into_iter());
    }

    let paging_state = paging_state.map(|ref state| hex::encode(state));

    if let Some(true) = expanded {
        Ok(ListenerResponseV1::MessageChildrenExpanded {
            message_id: message_id.into_inner(),
            max_results: 2 * page_size,
            count: messages.len(),
            children_message_ids: messages.drain(..).map(|record| record.into()).collect(),
            paging_state,
        })
    } else {
        Ok(ListenerResponseV1::MessageChildren {
            message_id: message_id.into_inner(),
            max_results: 2 * page_size,
            count: messages.len(),
            children_message_ids: messages.drain(..).map(|record| record.message_id).collect(),
            paging_state,
        })
    }
}

#[get("/<keyspace>/messages?<index>&<page_size>&<utf8>&<expanded>&<state>&<start_timestamp>&<end_timestamp>")]
async fn get_message_by_index(
    keyspace: String,
    mut index: String,
    page_size: Option<usize>,
    utf8: Option<bool>,
    expanded: Option<bool>,
    start_timestamp: Option<u64>,
    end_timestamp: Option<u64>,
    state: Option<String>,
) -> ListenerResult {
    if let Some(true) = utf8 {
        index = hex::encode(index);
    }
    if Vec::<u8>::from_hex(index.clone())
        .map_err(|_| ListenerError::InvalidHex)?
        .len()
        > 64
    {
        return Err(ListenerError::IndexTooLarge);
    }

    let (start_timestamp, end_timestamp) = (
        start_timestamp
            .map(|t| NaiveDateTime::from_timestamp(t as i64, 0))
            .unwrap_or(chrono::naive::MIN_DATETIME),
        end_timestamp
            .map(|t| NaiveDateTime::from_timestamp(t as i64, 0))
            .unwrap_or(chrono::naive::MAX_DATETIME),
    );
    if end_timestamp < start_timestamp {
        return Err(ListenerError::Other(anyhow!("Invalid time range")));
    }

    let mut state = state
        .map(|state| {
            hex::decode(state)
                .map_err(|_| ListenerError::InvalidState)
                .and_then(|v| bincode::deserialize::<StateData>(&v).map_err(|_| ListenerError::InvalidState))
        })
        .transpose()?;

    let page_size = page_size.unwrap_or(1000);

    let mut messages = page(
        keyspace.clone(),
        Hint::tags(index.clone()),
        page_size,
        &mut state,
        Some(start_timestamp..end_timestamp),
        index.clone(),
    )
    .await?;

    let state = state
        .map(|state| bincode::serialize(&state).map(|v| hex::encode(v)))
        .transpose()
        .map_err(|e| anyhow!(e))?;

    if let Some(true) = expanded {
        Ok(ListenerResponseV1::MessagesForIndexExpanded {
            index,
            max_results: 2 * page_size,
            count: messages.len(),
            message_ids: messages.drain(..).map(|record| record.into()).collect(),
            state,
        })
    } else {
        Ok(ListenerResponseV1::MessagesForIndex {
            index,
            max_results: 2 * page_size,
            count: messages.len(),
            message_ids: messages.drain(..).map(|record| record.message_id).collect(),
            state,
        })
    }
}

#[get(
    "/<keyspace>/addresses/<address>/outputs?<included>&<expanded>&<page_size>&<state>&<start_timestamp>&<end_timestamp>"
)]
async fn get_outputs_by_address(
    keyspace: String,
    address: String,
    expanded: Option<bool>,
    included: Option<bool>,
    page_size: Option<usize>,
    start_timestamp: Option<u64>,
    end_timestamp: Option<u64>,
    state: Option<String>,
) -> ListenerResult {
    let mut state = state
        .map(|state| {
            hex::decode(state)
                .map_err(|_| ListenerError::InvalidState)
                .and_then(|v| bincode::deserialize::<StateData>(&v).map_err(|_| ListenerError::InvalidState))
        })
        .transpose()?;

    let address = Bee(Address::try_from_bech32(&address).map_err(|e| ListenerError::BadParse(e.into()))?);
    let page_size = page_size.unwrap_or(100);

    let (start_timestamp, end_timestamp) = (
        start_timestamp
            .map(|t| NaiveDateTime::from_timestamp(t as i64, 0))
            .unwrap_or(chrono::naive::MIN_DATETIME),
        end_timestamp
            .map(|t| NaiveDateTime::from_timestamp(t as i64, 0))
            .unwrap_or(chrono::naive::MAX_DATETIME),
    );
    if end_timestamp < start_timestamp {
        return Err(ListenerError::Other(anyhow!("Invalid time range")));
    }

    let mut outputs = page(
        keyspace.clone(),
        Hint::legacy_outputs_by_address(address.0),
        page_size,
        &mut state,
        Some(start_timestamp..end_timestamp),
        address,
    )
    .await?;

    if included.unwrap_or(true) {
        outputs.retain(|record| matches!(record.inclusion_state, Some(LedgerInclusionState::Included)))
    }

    let state = state
        .map(|state| bincode::serialize(&state).map(|v| hex::encode(v)))
        .transpose()
        .map_err(|e| anyhow!(e))?;

    if let Some(true) = expanded {
        Ok(ListenerResponseV1::OutputsForAddressExpanded {
            address_type: 1,
            address: address.into_inner(),
            max_results: 2 * page_size,
            count: outputs.len(),
            output_ids: outputs
                .drain(..)
                .map(|record| Ok(record.try_into()?))
                .filter_map(|r: anyhow::Result<responses::Record>| r.ok())
                .collect(),
            state,
        })
    } else {
        Ok(ListenerResponseV1::OutputsForAddress {
            address_type: 1,
            address: address.into_inner(),
            max_results: 2 * page_size,
            count: outputs.len(),
            output_ids: outputs.drain(..).map(|record| record.output_id).collect(),
            state,
        })
    }
}

#[get("/<keyspace>/outputs/<transaction_id>/<idx>")]
async fn get_output_by_transaction_id(keyspace: String, transaction_id: String, idx: u16) -> ListenerResult {
    get_output(
        keyspace,
        TransactionId::from_str(&transaction_id)
            .and_then(|t| OutputId::new(t, idx))
            .map_err(|e| ListenerError::BadParse(e.into()))?
            .to_string(),
    )
    .await
}

#[get("/<keyspace>/outputs/<output_id>")]
async fn get_output(keyspace: String, output_id: String) -> ListenerResult {
    let (transaction_id, index) = OutputId::from_str(&output_id)
        .map_err(|e| ListenerError::BadParse(e.into()))?
        .split();

    let output_data = query::<OutputRes, _, _, _>(
        ChronicleKeyspace::new(keyspace.clone()),
        Bee(transaction_id.clone()),
        index,
        None,
        None,
    )
    .await?;
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
                query::<MessageRecord, _, _, _>(
                    ChronicleKeyspace::new(keyspace.clone()),
                    Bee(message_id.clone()),
                    (),
                    None,
                    None,
                )
            });
            is_spent = futures::future::join_all(queries)
                .await
                .drain(..)
                .filter_map(|res| res.ok())
                .any(|rec| rec.inclusion_state == Some(LedgerInclusionState::Included));
        }
        is_spent
    };
    Ok(ListenerResponseV1::Output {
        message_id: output_data.message_id,
        transaction_id: transaction_id,
        output_index: index,
        is_spent,
        output: output_data.output.borrow().into(),
    })
}

#[get("/<keyspace>/transactions/<address>?<page_size>&<state>&<start_timestamp>&<end_timestamp>")]
async fn get_transactions_for_address(
    keyspace: String,
    address: String,
    page_size: Option<usize>,
    start_timestamp: Option<u64>,
    end_timestamp: Option<u64>,
    state: Option<String>,
) -> ListenerResult {
    todo!("Deprecate or fix this endpoint");
    let mut state = state
        .map(|state| {
            hex::decode(state)
                .map_err(|_| ListenerError::InvalidState)
                .and_then(|v| bincode::deserialize::<StateData>(&v).map_err(|_| ListenerError::InvalidState))
        })
        .transpose()?;

    let address = Bee(Address::try_from_bech32(&address).map_err(|e| ListenerError::BadParse(e.into()))?);
    let page_size = page_size.unwrap_or(100);

    let (start_timestamp, end_timestamp) = (
        start_timestamp
            .map(|t| NaiveDateTime::from_timestamp(t as i64, 0))
            .unwrap_or(chrono::naive::MIN_DATETIME),
        end_timestamp
            .map(|t| NaiveDateTime::from_timestamp(t as i64, 0))
            .unwrap_or(chrono::naive::MAX_DATETIME),
    );
    if end_timestamp < start_timestamp {
        return Err(ListenerError::Other(anyhow!("Invalid time range")));
    }

    let outputs = page(
        keyspace.clone(),
        Hint::legacy_outputs_by_address(address.0),
        page_size,
        &mut state,
        Some(start_timestamp..end_timestamp),
        address,
    )
    .await?;

    let transactions = futures::stream::iter(outputs)
        .map(|o| (o, keyspace.clone()))
        .then(|(o, keyspace)| async move {
            query::<TransactionRes, _, _, _>(
                ChronicleKeyspace::new(keyspace),
                Bee(*o.output_id.transaction_id()),
                (),
                None,
                None,
            )
            .await
            .map(Into::into)
        })
        .try_collect()
        .await?;

    let state = state
        .map(|state| bincode::serialize(&state).map(|v| hex::encode(v)))
        .transpose()
        .map_err(|e| anyhow!(e))?;

    Ok(ListenerResponseV1::Transactions { transactions, state })
}

#[get("/<keyspace>/transaction_history/<address>?<page_size>&<state>&<start_timestamp>&<end_timestamp>")]
async fn get_transaction_history_for_address(
    keyspace: String,
    address: String,
    page_size: Option<usize>,
    start_timestamp: Option<u64>,
    end_timestamp: Option<u64>,
    state: Option<String>,
) -> ListenerResult {
    let mut state = state
        .map(|state| {
            hex::decode(state)
                .map_err(|_| ListenerError::InvalidState)
                .and_then(|v| bincode::deserialize::<StateData>(&v).map_err(|_| ListenerError::InvalidState))
        })
        .transpose()?;

    let address = Bee(Address::try_from_bech32(&address).map_err(|e| ListenerError::BadParse(e.into()))?);
    let page_size = page_size.unwrap_or(100);

    let (start_timestamp, end_timestamp) = (
        start_timestamp
            .map(|t| NaiveDateTime::from_timestamp(t as i64, 0))
            .unwrap_or(chrono::naive::MIN_DATETIME),
        end_timestamp
            .map(|t| NaiveDateTime::from_timestamp(t as i64, 0))
            .unwrap_or(chrono::naive::MAX_DATETIME),
    );
    if end_timestamp < start_timestamp {
        return Err(ListenerError::Other(anyhow!("Invalid time range")));
    }

    let outputs = page(
        keyspace.clone(),
        Hint::legacy_outputs_by_address(address.0),
        page_size,
        &mut state,
        Some(start_timestamp..end_timestamp),
        address,
    )
    .await?;

    let state = state
        .map(|state| bincode::serialize(&state).map(|v| hex::encode(v)))
        .transpose()
        .map_err(|e| anyhow!(e))?;

    Ok(ListenerResponseV1::TransactionHistory {
        transactions: outputs.into_iter().map(Into::into).collect(),
        state,
    })
}

#[get("/<keyspace>/transactions/<message_id>")]
async fn get_transaction_for_message(keyspace: String, message_id: String) -> ListenerResult {
    let keyspace = ChronicleKeyspace::new(keyspace);
    let message_id = Bee(MessageId::from_str(&message_id).map_err(|e| ListenerError::BadParse(e.into()))?);
    let message = query::<Bee<Message>, _, _, _>(keyspace.clone(), message_id, (), None, None).await?;
    let transaction_id = if let Some(payload) = message.payload() {
        match payload {
            Payload::Transaction(p) => p.id(),
            _ => return Err(ListenerError::NoResults),
        }
    } else {
        return Err(ListenerError::NoResults);
    };
    let transaction = query::<TransactionRes, _, _, _>(keyspace, Bee(transaction_id), (), None, None).await?;
    Ok(ListenerResponseV1::Transaction(transaction.into()))
}

#[get("/<keyspace>/transactions/<transaction_id>/included-message")]
async fn get_transaction_included_message(keyspace: String, transaction_id: String) -> ListenerResult {
    let keyspace = ChronicleKeyspace::new(keyspace);

    let transaction_id = Bee(TransactionId::from_str(&transaction_id).map_err(|e| ListenerError::Other(anyhow!(e)))?);

    let message_id = query::<Option<Bee<MessageId>>, _, _, _>(
        keyspace.clone(),
        transaction_id,
        LedgerInclusionState::Included,
        None,
        None,
    )
    .await?
    .ok_or_else(|| ListenerError::NoResults)?;
    query::<Bee<Message>, _, _, _>(keyspace, message_id, (), None, None)
        .await
        .and_then(|message| {
            message
                .into_inner()
                .try_into()
                .map_err(|e: Cow<'static, str>| anyhow!(e).into())
        })
}

#[get("/<keyspace>/milestones/<index>")]
async fn get_milestone(keyspace: String, index: u32) -> ListenerResult {
    let keyspace = ChronicleKeyspace::new(keyspace);
    let milestone_index = MilestoneIndex::from(index);

    query::<Bee<Milestone>, _, _, _>(keyspace, Bee(milestone_index), (), None, None)
        .await
        .map(|milestone| ListenerResponseV1::Milestone {
            milestone_index,
            message_id: milestone.message_id().to_string(),
            timestamp: milestone.timestamp(),
        })
}

#[get("/<keyspace>/analytics?<start>&<end>")]
async fn get_analytics(keyspace: String, start: Option<u32>, end: Option<u32>) -> ListenerResult {
    let keyspace = ChronicleKeyspace::new(keyspace);
    todo!()

    // let range = start.unwrap_or(1)..end.unwrap_or(i32::MAX as u32);
    // let range = SyncRange::try_from(range).map_err(|e| ListenerError::BadParse(e))?;
    // let ranges = AnalyticsData::try_fetch(&keyspace, &range, 1, 5000).await?.analytics;
    // Ok(ListenerResponseV1::Analytics { ranges })
}

#[derive(Serialize)]
struct AddressContext {
    data: Vec<AddressData>,
}

#[derive(Serialize)]
struct AddressData {
    date: String,
    total_addresses: usize,
    recv_addresses: usize,
    send_addresses: usize,
}

#[get("/graph/addresses")]
async fn active_addresses_graph() -> Template {
    let mut data = Vec::new();
    let start_date = (chrono::Utc::today() - chrono::Duration::days(365)).naive_utc();
    let mut rng = rand::thread_rng();
    let mut recv_addresses = 0;
    let mut send_addresses = 0;
    for date in start_date.iter_days().take(365) {
        let recv_addresses_delta: i32 = rng.gen_range(-24..32);
        let send_addresses_delta: i32 = rng.gen_range(-24..32);
        recv_addresses = i32::max(0, recv_addresses + recv_addresses_delta);
        send_addresses = i32::max(0, send_addresses + send_addresses_delta);
        data.push(AddressData {
            date: date.format("%Y-%m-%d").to_string(),
            total_addresses: recv_addresses as usize + send_addresses as usize,
            recv_addresses: recv_addresses as usize,
            send_addresses: send_addresses as usize,
        });
    }
    let context = AddressContext { data };
    Template::render("graph", &context)
}

#[catch(500)]
fn internal_error() -> ListenerError {
    ListenerError::Other(anyhow!("Internal server error!"))
}

#[catch(404)]
fn not_found() -> ListenerError {
    ListenerError::NotFound
}

#[cfg(test)]
mod tests {
    use super::*;
    use ::rocket::{
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
    use serde_json::Value;

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

    async fn construct_client() -> Client {
        let mut keyspaces = HashSet::new();
        keyspaces.insert("permanode".to_string());
        let rocket = construct_rocket().manage(keyspaces);
        Client::tracked(rocket).await.expect("Invalid rocket instance!")
    }

    #[::rocket::async_test]
    async fn options() {
        let client = construct_client().await;

        let res = client.options("/api/anything").dispatch().await;
        assert_eq!(res.status(), Status::Ok);
        assert_eq!(res.content_type(), None);
        check_cors_headers(&res);
        assert!(res.into_string().await.is_none());
    }

    #[::rocket::async_test]
    async fn info() {
        let client = construct_client().await;

        let res = client.get("/api/permanode/info").dispatch().await;
        assert_eq!(res.status(), Status::Ok);
        assert_eq!(res.content_type(), Some(ContentType::JSON));
        check_cors_headers(&res);
        let body: SuccessBody<ListenerResponseV1> =
            serde_json::from_str(&res.into_string().await.expect("No body returned!"))
                .expect("Failed to deserialize Info Response!");
        match *body {
            ListenerResponseV1::Info { .. } => (),
            _ => panic!("Did not receive an info response!"),
        }
    }

    #[::rocket::async_test]
    async fn service() {
        let client = construct_client().await;

        let res = client.get("/api/service").dispatch().await;
        assert_eq!(res.status(), Status::Ok);
        assert_eq!(res.content_type(), Some(ContentType::JSON));
        check_cors_headers(&res);
        // let _body: Service = serde_json::from_str(&res.into_string().await.expect("No body returned!"))
        //    .expect("Failed to deserialize Service Tree Response!");
    }

    #[::rocket::async_test]
    async fn get_message() {
        let client = construct_client().await;

        let res = client
            .get("/api/permanode/messages/91515c13d2025f79ded3758abe5dc640591c3b6d58b1c52cd51d1fa0585774bc")
            .dispatch()
            .await;
        assert_eq!(res.status(), Status::InternalServerError);
        assert_eq!(res.content_type(), Some(ContentType::JSON));
        check_cors_headers(&res);
        let body: Value = serde_json::from_str(&res.into_string().await.expect("No body returned!"))
            .expect("Failed to deserialize response!");
        assert_eq!(body.get("message").and_then(Value::as_str), Some("Worker NoRing"));
    }
}
