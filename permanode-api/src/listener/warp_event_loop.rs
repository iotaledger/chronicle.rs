use super::*;
use bee_rest_api::handlers::{
    info::InfoResponse,
    message::MessageResponse,
    message_children::MessageChildrenResponse,
    messages_find::MessagesForIndexResponse,
    milestone::MilestoneResponse,
    output::OutputResponse,
    outputs_ed25519::OutputsForAddressResponse,
    SuccessBody,
};
use mpsc::unbounded_channel;
use permanode_storage::{
    access::{
        Bee,
        CreatedOutput,
        Ed25519Address,
        GetSelectRequest,
        HashedIndex,
        IndexMessages,
        MessageChildren,
        MessageId,
        MessageRow,
        MilestoneIndex,
        OutputId,
        OutputIds,
        Outputs,
        SelectRequest,
        SingleMilestone,
        TransactionData,
        HASHED_INDEX_LENGTH,
    },
    keyspaces::PermanodeKeyspace,
};
use serde::Deserialize;
use std::{
    borrow::Cow,
    convert::TryInto,
    net::SocketAddr,
    ops::Deref,
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

use crate::listener::rocket_event_loop::message_row_to_response;

#[async_trait]
impl<H: PermanodeAPIScope> EventLoop<PermanodeAPISender<H>> for Listener<WarpListener> {
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

        let cors = warp::cors()
            .allow_any_origin()
            .allow_methods(vec!["GET", "OPTIONS"])
            .allow_header("content-type")
            .allow_credentials(true);

        let options = warp::options().map(warp::reply);

        let api = warp::path("api").and(warp::get());

        let routes = api
            .and(warp::path("info"))
            .and_then(info)
            .or(warp::path!(String / "messages")
                .and(warp::query::<IndexParam>())
                .and_then(get_message_by_index)
                .or(warp::path!(String / "messages" / String).and_then(get_message))
                .or(warp::path!(String / "messages" / String / "metadata").and_then(get_message_metadata))
                .or(warp::path!(String / "messages" / String / "children").and_then(get_message_children))
                .or(warp::path!(String / "output" / String).and_then(get_output))
                .or(warp::path!(String / "addresses" / "ed25519" / String / "outputs").and_then(get_ed25519_outputs))
                .or(warp::path!(String / "milestones" / u32).and_then(get_milestone)))
            .or(options)
            .with(cors);

        let address = std::env::var("WARP_ADDRESS").unwrap_or("127.0.0.1".to_string());
        let port = std::env::var("WARP_PORT").unwrap_or("7000".to_string());

        warp::serve(routes)
            .run(SocketAddr::from_str(&format!("{}:{}", address, port)).unwrap())
            .await;
        Ok(())
    }
}

#[derive(Deserialize)]
struct IndexParam {
    pub index: String,
}

async fn info() -> Result<Json, Rejection> {
    Ok(json(&InfoResponse {
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

async fn query<'a, V, S: Select<K, V>, K>(request: SelectRequest<'a, S, K, V>) -> Result<V, EndpointError> {
    let (sender, mut inbox) = unbounded_channel::<Event>();
    let worker = Box::new(DecoderWorker(sender));

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

async fn get_message(keyspace: String, message_id: String) -> Result<Json, Rejection> {
    let keyspace = PermanodeKeyspace::new(keyspace);
    let request = keyspace.select::<Bee<Message>>(&MessageId::from_str(&message_id).unwrap().into());
    query(request)
        .await?
        .deref()
        .try_into()
        .map(|dto| json(&SuccessBody::new(MessageResponse(dto))))
        .map_err(|e| EndpointError::from(e).into())
}

async fn get_message_metadata(keyspace: String, message_id: String) -> Result<Json, Rejection> {
    let keyspace = PermanodeKeyspace::new(keyspace);
    let request = keyspace.select::<MessageRow>(&MessageId::from_str(&message_id).unwrap().into());
    message_row_to_response(query(request).await?)
        .map(|res| json(&SuccessBody::new(res)))
        .map_err(|e| EndpointError::from(e.to_string()).into())
}

async fn get_message_children(keyspace: String, message_id: String) -> Result<Json, Rejection> {
    let keyspace = PermanodeKeyspace::new(keyspace);
    let request = keyspace.select::<MessageChildren>(&MessageId::from_str(&message_id).unwrap().into());
    // TODO: Paging
    let children = query(request).await?;
    let count = children.rows_count();
    let max_results = 1000;

    Ok(json(&SuccessBody::new(MessageChildrenResponse {
        message_id,
        max_results,
        count,
        children_message_ids: children.take(max_results).map(|id| id.to_string()).collect(),
    })))
}

async fn get_message_by_index(keyspace: String, index: IndexParam) -> Result<Json, Rejection> {
    let mut bytes_vec = vec![0; HASHED_INDEX_LENGTH];
    let bytes = hex::decode(index.index.clone())
        .map_err(|_| Rejection::from(EndpointError::from("Invalid Hex character in index!")))?;
    bytes.iter().enumerate().for_each(|(i, &b)| bytes_vec[i] = b);

    info!("Getting message for index: {}", String::from_utf8_lossy(&bytes));

    let keyspace = PermanodeKeyspace::new(keyspace);
    let request = keyspace.select::<IndexMessages>(&HashedIndex::new(bytes_vec.as_slice().try_into().unwrap()).into());
    // TODO: Paging
    let messages = query(request).await?;
    let count = messages.rows_count();
    let max_results = 1000;

    Ok(json(&SuccessBody::new(MessagesForIndexResponse {
        index: index.index,
        max_results,
        count,
        message_ids: messages.take(max_results).map(|id| id.to_string()).collect(),
    })))
}

async fn get_output(keyspace: String, output_id: String) -> Result<Json, Rejection> {
    let output_id = OutputId::from_str(&output_id).unwrap();
    let keyspace = PermanodeKeyspace::new(keyspace);
    let request = keyspace.select::<Outputs>(&output_id.into());
    let outputs = query(request).await?;
    let (output, is_spent) = {
        let mut output = None;
        let mut is_spent = false;
        for row in outputs {
            match row.data {
                TransactionData::Input(_) => {}
                TransactionData::Output(o) => {
                    output = Some(CreatedOutput::new(row.message_id.into_inner(), o));
                }
                TransactionData::Unlock(_) => {
                    is_spent = true;
                }
            }
        }
        (
            output.ok_or(Rejection::from(EndpointError::from(format!(
                "No output found for id {}",
                output_id
            ))))?,
            is_spent,
        )
    };
    Ok(json(&SuccessBody::new(OutputResponse {
        message_id: output.message_id().to_string(),
        transaction_id: output_id.transaction_id().to_string(),
        output_index: output_id.index(),
        is_spent,
        output: output.inner().try_into().map_err(|e| EndpointError::from(e))?,
    })))
}

async fn get_ed25519_outputs(keyspace: String, address: String) -> Result<Json, Rejection> {
    let keyspace = PermanodeKeyspace::new(keyspace);
    let request = keyspace.select::<OutputIds>(&Ed25519Address::from_str(&address).unwrap().into());

    // TODO: Paging
    let outputs = query(request).await?;
    let count = outputs.rows_count();
    let max_results = 1000;

    Ok(json(&SuccessBody::new(OutputsForAddressResponse {
        address_type: 1,
        address,
        max_results,
        count,
        output_ids: outputs.take(max_results).map(|id| id.to_string()).collect(),
    })))
}

async fn get_milestone(keyspace: String, index: u32) -> Result<Json, Rejection> {
    let keyspace = PermanodeKeyspace::new(keyspace);
    let request = keyspace.select::<SingleMilestone>(&MilestoneIndex::from(index).into());
    query(request)
        .await?
        .get()
        .map(|milestone| {
            json(&SuccessBody::new(MilestoneResponse {
                milestone_index: index,
                message_id: milestone.message_id().to_string(),
                timestamp: milestone.timestamp(),
            }))
        })
        .ok_or(EndpointError::from(format!("No milestone found for index {}", index)).into())
}
