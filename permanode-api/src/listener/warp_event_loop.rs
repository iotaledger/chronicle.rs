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
        MessageId,
        MessageMetadata,
        Milestone,
        MilestoneIndex,
        OutputId,
        Outputs,
        TransactionData,
        HASHED_INDEX_LENGTH,
    },
    keyspaces::PermanodeKeyspace,
};
use scylla_cql::Consistency;
use serde::Deserialize;
use std::{
    borrow::Cow,
    convert::TryInto,
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

        let routes = construct_routes();

        let address = std::env::var("WARP_ADDRESS").unwrap_or("127.0.0.1".to_string());
        let port = std::env::var("WARP_PORT").unwrap_or("7000".to_string());

        warp::serve(routes)
            .run(SocketAddr::from_str(&format!("{}:{}", address, port)).unwrap())
            .await;
        Ok(())
    }
}

fn construct_routes() -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    let cors = warp::cors()
        .allow_any_origin()
        .allow_methods(vec!["GET", "OPTIONS"])
        .allow_header("content-type")
        .allow_credentials(true);

    let options = warp::options().map(warp::reply);

    let api = warp::path("api").and(warp::get());

    api.and(
        warp::path("info")
            .and_then(info)
            .or(warp::path!(String / "messages")
                .and(warp::query::<IndexParam>())
                .and_then(get_message_by_index))
            .or(warp::path!(String / "messages" / String).and_then(get_message))
            .or(warp::path!(String / "messages" / String / "metadata").and_then(get_message_metadata))
            .or(warp::path!(String / "messages" / String / "children").and_then(get_message_children))
            .or(warp::path!(String / "output" / String).and_then(get_output))
            .or(warp::path!(String / "addresses" / "ed25519" / String / "outputs").and_then(get_ed25519_outputs))
            .or(warp::path!(String / "milestones" / u32).and_then(get_milestone)),
    )
    .or(options)
    .with(cors)
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

async fn query<V, S, K>(keyspace: S, key: K) -> Result<V, EndpointError>
where
    S: 'static + Select<K, V> + std::fmt::Debug,
    K: 'static + Send + std::fmt::Debug + Clone,
    V: 'static + Send + std::fmt::Debug + Clone,
{
    let request = keyspace.select::<V>(&key).consistency(Consistency::One).build();

    let (sender, mut inbox) = unbounded_channel::<Result<Option<V>, WorkerError>>();
    let worker = ValueWorker::boxed(sender, keyspace, key, PhantomData);

    let decoder = request.send_local(worker);

    while let Some(event) = inbox.recv().await {
        match event {
            Ok(res) => {
                res.ok_or("No results returned!".into());
            }
            Err(worker_error) => return Err(format!("{:?}", worker_error).into()),
        }
    }

    Err("Failed to receive response!".into())
}

async fn get_message(keyspace: String, message_id: String) -> Result<Json, Rejection> {
    let keyspace = PermanodeKeyspace::new(keyspace);
    query::<Message, _, _>(keyspace, MessageId::from_str(&message_id).unwrap())
        .await
        .and_then(|ref message| {
            message
                .try_into()
                .map(|dto| json(&SuccessBody::new(MessageResponse(dto))))
                .map_err(|e| EndpointError::from(e))
        })
        .map_err(|e| e.into())
}

async fn get_message_metadata(keyspace: String, message_id: String) -> Result<Json, Rejection> {
    let keyspace = PermanodeKeyspace::new(keyspace);
    let message_id = MessageId::from_str(&message_id).unwrap();
    query::<MessageMetadata, _, _>(keyspace, message_id)
        .await
        .map(|metadata| json(&SuccessBody::new(metadata)))
        .map_err(|e| e.into())
}

async fn get_message_children(keyspace: String, message_id: String) -> Result<Json, Rejection> {
    let keyspace = PermanodeKeyspace::new(keyspace);
    // TODO: Paging
    query::<MessageChildren, _, _>(keyspace, MessageId::from_str(&message_id).unwrap())
        .await
        .map(|message_children| {
            let children = message_children.children;

            let count = children.len();
            let max_results = 1000;

            json(&SuccessBody::new(MessageChildrenResponse {
                message_id,
                max_results,
                count,
                children_message_ids: children.iter().take(max_results).map(|id| id.to_string()).collect(),
            }))
        })
        .map_err(|e| e.into())
}

async fn get_message_by_index(keyspace: String, IndexParam { index }: IndexParam) -> Result<Json, Rejection> {
    let mut bytes_vec = vec![0; HASHED_INDEX_LENGTH];
    let bytes = hex::decode(index.clone())
        .map_err(|_| Rejection::from(EndpointError::from("Invalid Hex character in index!")))?;
    bytes.iter().enumerate().for_each(|(i, &b)| bytes_vec[i] = b);

    let keyspace = PermanodeKeyspace::new(keyspace);
    query::<IndexMessages, _, _>(keyspace, HashedIndex::new(bytes_vec.as_slice().try_into().unwrap()))
        .await
        .map(|index_messages| {
            let messages = index_messages.messages;
            let count = messages.len();
            let max_results = 1000;

            json(&SuccessBody::new(MessagesForIndexResponse {
                index,
                max_results,
                count,
                message_ids: messages.iter().take(max_results).map(|id| id.to_string()).collect(),
            }))
        })
        .map_err(|e| e.into())
}

async fn get_output(keyspace: String, output_id: String) -> Result<Json, Rejection> {
    let output_id = OutputId::from_str(&output_id).unwrap();
    let keyspace = PermanodeKeyspace::new(keyspace);

    let mut outputs = query::<Outputs, _, _>(keyspace, output_id)
        .await
        .map_err(|e| Rejection::from(e))?
        .outputs;
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
            output.ok_or(EndpointError::from(format!("No output found for id {}", output_id)))?,
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

    // TODO: Paging
    let outputs = query::<OutputIds, _, _>(keyspace, Ed25519Address::from_str(&address).unwrap())
        .await?
        .ids;
    let count = outputs.len();
    let max_results = 1000;

    Ok(json(&SuccessBody::new(OutputsForAddressResponse {
        address_type: 1,
        address,
        max_results,
        count,
        output_ids: outputs.iter().take(max_results).map(|id| id.to_string()).collect(),
    })))
}

async fn get_milestone(keyspace: String, index: u32) -> Result<Json, Rejection> {
    let keyspace = PermanodeKeyspace::new(keyspace);

    query::<Milestone, _, _>(keyspace, MilestoneIndex::from(index))
        .await
        .map(|milestone| {
            json(&SuccessBody::new(MilestoneResponse {
                milestone_index: index,
                message_id: milestone.message_id().to_string(),
                timestamp: milestone.timestamp(),
            }))
        })
        .map_err(|_| EndpointError::from(format!("No milestone found for index {}", index)).into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::{
        header::*,
        Response,
        StatusCode,
    };
    use rocket::http::hyper::Bytes;

    fn check_cors_headers(res: &Response<Bytes>) {
        println!("{:?}", res.headers());
        let mut methods = res.headers()[ACCESS_CONTROL_ALLOW_METHODS]
            .to_str()
            .unwrap()
            .split(",")
            .map(|s| s.trim())
            .collect::<Vec<_>>();
        methods.sort_unstable();
        assert_eq!(res.headers()[ACCESS_CONTROL_ALLOW_ORIGIN], "warp");
        assert_eq!(methods.join(", "), "GET, OPTIONS");
        assert_eq!(res.headers()[ACCESS_CONTROL_ALLOW_HEADERS], "content-type");
        assert_eq!(res.headers()[ACCESS_CONTROL_ALLOW_CREDENTIALS], "true");
    }

    #[tokio::test]
    async fn options() {
        let routes = construct_routes();
        let res = warp::test::request()
            .method("OPTIONS")
            .header(ORIGIN, "warp")
            .header(ACCESS_CONTROL_REQUEST_METHOD, "GET")
            .path("/api/anything")
            .reply(&routes)
            .await;
        assert_eq!(res.status(), StatusCode::OK);
        assert_eq!(res.headers().get(CONTENT_TYPE), None);
        check_cors_headers(&res);
        assert_eq!(res.body(), "");
    }

    #[tokio::test]
    async fn info() {
        let routes = construct_routes();
        let res = warp::test::request()
            .method("OPTIONS")
            .header(ORIGIN, "warp")
            .header(ACCESS_CONTROL_REQUEST_METHOD, "GET")
            .path("/api/info")
            .reply(&routes)
            .await;
        assert_eq!(res.status(), StatusCode::OK);
        check_cors_headers(&res);
        let res = warp::test::request()
            .header(ORIGIN, "warp")
            .path("/api/info")
            .reply(&routes)
            .await;
        assert_eq!(res.status(), StatusCode::OK);
        assert_eq!(res.headers()[CONTENT_TYPE], "application/json");
        let _body: InfoResponse = serde_json::from_slice(res.body()).expect("Failed to deserialize Info Response!");
    }

    #[tokio::test]
    async fn get_message() {
        let routes = construct_routes();
        let res = warp::test::request()
            .method("OPTIONS")
            .header(ORIGIN, "warp")
            .header(ACCESS_CONTROL_REQUEST_METHOD, "GET")
            .path("/api/permanode/messages/91515c13d2025f79ded3758abe5dc640591c3b6d58b1c52cd51d1fa0585774bc")
            .reply(&routes)
            .await;
        assert_eq!(res.status(), StatusCode::OK);
        check_cors_headers(&res);
        let res = warp::test::request()
            .header(ORIGIN, "warp")
            .path("/api/permanode/messages/91515c13d2025f79ded3758abe5dc640591c3b6d58b1c52cd51d1fa0585774bc")
            .reply(&routes)
            .await;
        println!("{:?}", res.headers());
        assert_eq!(res.status(), StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(res.headers()[CONTENT_TYPE], "text/plain; charset=utf-8");
        assert_eq!(
            res.body(),
            "Unhandled rejection: EndpointError { msg: \"Worker NoRing\" }"
        );
    }

    #[tokio::test]
    async fn get_message_by_index() {
        let routes = construct_routes();
        let hex_index = hex::encode("test_index");
        let res = warp::test::request()
            .method("OPTIONS")
            .header(ORIGIN, "warp")
            .header(ACCESS_CONTROL_REQUEST_METHOD, "GET")
            .path(&format!("/api/permanode/messages?index={}", hex_index))
            .reply(&routes)
            .await;
        assert_eq!(res.status(), StatusCode::OK);
        check_cors_headers(&res);
        let res = warp::test::request()
            .header(ORIGIN, "warp")
            .path(&format!("/api/permanode/messages?index={}", hex_index))
            .reply(&routes)
            .await;
        println!("{:?}", res.headers());
        assert_eq!(res.status(), StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(res.headers()[CONTENT_TYPE], "text/plain; charset=utf-8");
        assert_eq!(
            res.body(),
            "Unhandled rejection: EndpointError { msg: \"Worker NoRing\" }"
        );
    }
}
