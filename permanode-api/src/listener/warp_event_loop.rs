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
    keyspaces::Mainnet,
    TangleNetwork,
};
use serde::Deserialize;
use std::{
    borrow::Cow,
    convert::TryInto,
    net::SocketAddr,
    ops::Deref,
    str::FromStr,
    sync::Arc,
};
use tokio::sync::{
    mpsc,
    Mutex,
};
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

macro_rules! routes {
    [$($($path:tt)/ * $(? $query:ty)? => $callback:path $(| ($($state:expr),*))?),* $(,)?] => {
        vec![
            $(warp::path!($($path)/ *)$(.and(warp::query::<$query>()))?$($(.and($state))*)?.and_then($callback).boxed()),*
        ]
    };
}

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

        let mut routes_vec = Vec::new();

        for network in self.config.keyspaces.keys() {
            match network {
                TangleNetwork::Mainnet => {
                    let mainnet = Arc::new(Mutex::new(Mainnet::new()));
                    routes_vec.extend(
                        routes![
                            "mainnet" / "info" => info,
                            "mainnet" / "messages" ? IndexParam => mainnet::get_message_by_index | (mainnet::with_mainnet(mainnet.clone())),
                            "mainnet" / "messages" / String => mainnet::get_message | (mainnet::with_mainnet(mainnet.clone())),
                            "mainnet" / "messages" / String / "metadata" => mainnet::get_message_metadata | (mainnet::with_mainnet(mainnet.clone())),
                            "mainnet" / "messages" / String / "children" => mainnet::get_message_children | (mainnet::with_mainnet(mainnet.clone())),
                            "mainnet" / "output" / String => mainnet::get_output | (mainnet::with_mainnet(mainnet.clone())),
                            "mainnet" / "addresses" / "ed25519" / String / "outputs" => mainnet::get_ed25519_outputs | (mainnet::with_mainnet(mainnet.clone())),
                            "mainnet" / "milestones" / u32 => mainnet::get_milestone | (mainnet::with_mainnet(mainnet.clone())),
                        ]
                        .drain(..),
                    );
                }
                TangleNetwork::Devnet => {
                    routes_vec.extend(
                        routes![
                            "devnet" / "info" => info,
                            "devnet" / "messages" ? IndexParam => devnet::get_message_by_index,
                            "devnet" / "messages" / String => devnet::get_message,
                            "devnet" / "messages" / String / "metadata" => devnet::get_message_metadata,
                            "devnet" / "messages" / String / "children" => devnet::get_message_children,
                            "devnet" / "output" / String => devnet::get_output,
                            "devnet" / "addresses" / "ed25519" / String / "outputs" => devnet::get_ed25519_outputs,
                            "devnet" / "milestones" / u32 => devnet::get_milestone,
                        ]
                        .drain(..),
                    );
                }
            }
        }

        let first_route = routes_vec.pop().ok_or(Need::Abort)?;
        let routes = routes_vec
            .drain(..)
            .fold(first_route, |routes, route| routes.or(route).unify().boxed());

        let routes = routes.or(options).with(cors);

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
mod mainnet {
    use super::*;

    pub(super) async fn get_message(message_id: String, mainnet: Arc<Mutex<Mainnet>>) -> Result<Json, Rejection> {
        let mainnet = mainnet.lock().await;
        let request = mainnet.select::<Bee<Message>>(&MessageId::from_str(&message_id).unwrap().into());
        query(request)
            .await?
            .deref()
            .try_into()
            .map(|dto| json(&SuccessBody::new(MessageResponse(dto))))
            .map_err(|e| EndpointError::from(e).into())
    }

    pub(super) async fn get_message_metadata(
        message_id: String,
        mainnet: Arc<Mutex<Mainnet>>,
    ) -> Result<Json, Rejection> {
        let mainnet = mainnet.lock().await;
        let request = mainnet.select::<MessageRow>(&MessageId::from_str(&message_id).unwrap().into());
        message_row_to_response(query(request).await?)
            .map(|res| json(&SuccessBody::new(res)))
            .map_err(|e| EndpointError::from(e.to_string()).into())
    }

    pub(super) async fn get_message_children(
        message_id: String,
        mainnet: Arc<Mutex<Mainnet>>,
    ) -> Result<Json, Rejection> {
        let mainnet = mainnet.lock().await;
        let request = mainnet.select::<MessageChildren>(&MessageId::from_str(&message_id).unwrap().into());
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

    pub(super) async fn get_message_by_index(
        index: IndexParam,
        mainnet: Arc<Mutex<Mainnet>>,
    ) -> Result<Json, Rejection> {
        let mainnet = mainnet.lock().await;
        let mut bytes_vec = vec![0; HASHED_INDEX_LENGTH];
        let bytes = hex::decode(index.index.clone())
            .map_err(|_| Rejection::from(EndpointError::from("Invalid Hex character in index!")))?;
        bytes.iter().enumerate().for_each(|(i, &b)| bytes_vec[i] = b);

        info!("Getting message for index: {}", String::from_utf8_lossy(&bytes));

        let request =
            mainnet.select::<IndexMessages>(&HashedIndex::new(bytes_vec.as_slice().try_into().unwrap()).into());
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

    pub(super) async fn get_output(output_id: String, mainnet: Arc<Mutex<Mainnet>>) -> Result<Json, Rejection> {
        let mainnet = mainnet.lock().await;
        let output_id = OutputId::from_str(&output_id).unwrap();
        let request = mainnet.select::<Outputs>(&output_id.into());
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

    pub(super) async fn get_ed25519_outputs(address: String, mainnet: Arc<Mutex<Mainnet>>) -> Result<Json, Rejection> {
        let mainnet = mainnet.lock().await;
        let request = mainnet.select::<OutputIds>(&Ed25519Address::from_str(&address).unwrap().into());

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

    pub(super) async fn get_milestone(index: u32, mainnet: Arc<Mutex<Mainnet>>) -> Result<Json, Rejection> {
        let mainnet = mainnet.lock().await;
        let request = mainnet.select::<SingleMilestone>(&MilestoneIndex::from(index).into());
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

    pub(super) fn with_mainnet(
        mainnet: Arc<Mutex<Mainnet>>,
    ) -> impl Filter<Extract = (Arc<Mutex<Mainnet>>,), Error = std::convert::Infallible> + Clone {
        warp::any().map(move || mainnet.clone())
    }
}

mod devnet {
    use super::*;

    pub(super) async fn get_message(message_id: String) -> Result<Json, Rejection> {
        todo!();
    }

    pub(super) async fn get_message_metadata(message_id: String) -> Result<Json, Rejection> {
        todo!();
    }

    pub(super) async fn get_message_children(message_id: String) -> Result<Json, Rejection> {
        todo!();
    }

    pub(super) async fn get_message_by_index(index: IndexParam) -> Result<Json, Rejection> {
        todo!();
    }

    pub(super) async fn get_output(output_id: String) -> Result<Json, Rejection> {
        todo!();
    }

    pub(super) async fn get_ed25519_outputs(address: String) -> Result<Json, Rejection> {
        todo!();
    }

    pub(super) async fn get_milestone(index: u32) -> Result<Json, Rejection> {
        todo!();
    }
}
