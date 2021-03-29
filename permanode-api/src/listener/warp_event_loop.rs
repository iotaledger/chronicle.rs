use super::*;
use crate::{
    listener::rocket_event_loop::{
        page,
        query,
    },
    responses::*,
};
use http::HeaderMap;
use permanode_storage::{
    access::{
        Ed25519Address,
        MessageId,
        MessageMetadata,
        Milestone,
        MilestoneIndex,
        OutputId,
        OutputRes,
    },
    keyspaces::PermanodeKeyspace,
    PartitionConfig,
};
use rocket::http::{
    private::cookie::Key,
    Cookie,
    CookieJar,
};
use serde::Deserialize;
use std::{
    borrow::Cow,
    collections::HashSet,
    convert::TryInto,
    net::SocketAddr,
    str::FromStr,
};
use warp::{
    reject::Reject,
    reply::{
        json,
        Json,
        Response,
    },
    Filter,
    Rejection,
    Reply,
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

        let routes = construct_routes(&self.storage_config.partition_config);

        let address = std::env::var("WARP_ADDRESS").unwrap_or("127.0.0.1".to_string());
        let port = std::env::var("WARP_PORT").unwrap_or("7000".to_string());

        let server = warp::serve(routes);
        let abort_handle = self.data.abort_handle.take().unwrap();
        server
            .bind_with_graceful_shutdown(SocketAddr::from_str(&format!("{}:{}", address, port)).unwrap(), async {
                abort_handle.await.ok();
            })
            .1
            .await;
        Ok(())
    }
}

fn construct_routes(
    partition_config: &PartitionConfig,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    let cors = warp::cors()
        .allow_any_origin()
        .allow_methods(vec!["GET", "OPTIONS"])
        .allow_header("content-type")
        .allow_credentials(true);

    let options = warp::options().map(warp::reply);

    let api = warp::path("api").and(warp::get());

    let cookies = CookieJar::new(&Key);

    let milestone_cookie = warp::cookie::optional("last_milestone_index");
    let partition_cookie = warp::cookie::optional("last_partition_id");
    let paging_state_cookie = warp::cookie::optional("paging_state");

    api.and(
        warp::path("info")
            .and_then(info)
            .or(warp::path!(String / "messages")
                .and(milestone_cookie)
                .and(partition_cookie)
                .and(paging_state_cookie)
                .and(with_cookies(cookies.clone()))
                .and(with_partition_config(partition_config.clone()))
                .and(warp::query::<IndexParams>())
                .and_then(get_message_by_index))
            .or(warp::path!(String / "messages" / String).and_then(get_message))
            .or(warp::path!(String / "messages" / String / "metadata").and_then(get_message_metadata))
            .or(warp::path!(String / "messages" / String / "children")
                .and(milestone_cookie)
                .and(partition_cookie)
                .and(paging_state_cookie)
                .and(with_cookies(cookies.clone()))
                .and(with_partition_config(partition_config.clone()))
                .and(warp::query::<PageParam>())
                .and_then(get_message_children))
            .or(warp::path!(String / "output" / String).and_then(get_output))
            .or(warp::path!(String / "addresses" / "ed25519" / String / "outputs")
                .and(milestone_cookie)
                .and(partition_cookie)
                .and(paging_state_cookie)
                .and(with_cookies(cookies))
                .and(with_partition_config(partition_config.clone()))
                .and(warp::query::<PageParam>())
                .and_then(get_ed25519_outputs))
            .or(warp::path!(String / "milestones" / u32).and_then(get_milestone)),
    )
    .or(options)
    .with(cors)
}

fn with_cookies(
    cookies: CookieJar<'_>,
) -> impl Filter<Extract = (CookieJar<'_>,), Error = std::convert::Infallible> + Clone {
    warp::any().map(move || cookies.clone())
}

fn with_partition_config(
    config: PartitionConfig,
) -> impl Filter<Extract = (PartitionConfig,), Error = std::convert::Infallible> + Clone {
    warp::any().map(move || config.clone())
}

#[derive(Deserialize)]
struct IndexParams {
    pub index: String,
    pub page_size: Option<usize>,
}

#[derive(Deserialize)]
struct PageParam {
    pub page_size: Option<usize>,
}

async fn info() -> Result<Json, Rejection> {
    Ok(json(&InfoResponse {
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

#[derive(Debug)]
pub struct ErrorBody {
    msg: Cow<'static, str>,
}

impl From<&'static str> for ErrorBody {
    fn from(msg: &'static str) -> Self {
        Self { msg: msg.into() }
    }
}

impl From<String> for ErrorBody {
    fn from(msg: String) -> Self {
        Self { msg: msg.into() }
    }
}

impl From<Cow<'_, str>> for ErrorBody {
    fn from(msg: Cow<'_, str>) -> Self {
        Self {
            msg: msg.into_owned().into(),
        }
    }
}

impl Reject for ErrorBody {}

async fn get_message(keyspace: String, message_id: String) -> Result<Json, Rejection> {
    let keyspace = PermanodeKeyspace::new(keyspace);
    query::<Message, _, _>(keyspace, MessageId::from_str(&message_id).unwrap(), None, None)
        .await
        .and_then(|message| {
            message
                .try_into()
                .map(|res: MessageResponse| json(&SuccessBody::new(res)))
        })
        .map_err(|e| ErrorBody::from(e).into())
}

async fn get_message_metadata(keyspace: String, message_id: String) -> Result<Json, Rejection> {
    let keyspace = PermanodeKeyspace::new(keyspace);
    let message_id = MessageId::from_str(&message_id).unwrap();
    query::<MessageMetadata, _, _>(keyspace, message_id, None, None)
        .await
        .map(|metadata| json(&SuccessBody::new(metadata)))
        .map_err(|e| ErrorBody::from(e).into())
}

async fn get_message_children(
    keyspace: String,
    message_id: String,
    last_milestone_cookie: Option<String>,
    last_partition_cookie: Option<String>,
    paging_state_cookie: Option<String>,
    mut cookies: CookieJar<'_>,
    partition_config: PartitionConfig,
    PageParam { page_size }: PageParam,
) -> Result<Response, Rejection> {
    let message_id = MessageId::from_str(&message_id).unwrap();
    let page_size = page_size.unwrap_or(100);

    if let Some(last_milestone_cookie) = last_milestone_cookie {
        cookies.add_original(Cookie::new("last_milestone_index", last_milestone_cookie));
    }
    if let Some(last_partition_cookie) = last_partition_cookie {
        cookies.add_original(Cookie::new("last_partition_id", last_partition_cookie));
    }
    if let Some(paging_state_cookie) = paging_state_cookie {
        cookies.add_original(Cookie::new("paging_state", paging_state_cookie));
    }

    let mut messages = page(
        keyspace.clone(),
        Hint::parent(message_id.to_string()),
        page_size,
        &cookies,
        format!("/api/{}/messages/{}/children", keyspace, message_id.to_string()),
        &partition_config,
        message_id,
    )
    .await
    .map_err(|e| warp::reject::custom(ErrorBody::from(e)))?;

    let mut headers = HeaderMap::new();

    if let Some(last_milestone_cookie) = cookies.get_pending("last_milestone_index") {
        headers.append(
            http::header::SET_COOKIE,
            last_milestone_cookie.to_string().parse().unwrap(),
        );
    }

    if let Some(last_partition_cookie) = cookies.get_pending("last_partition_id") {
        headers.append(
            http::header::SET_COOKIE,
            last_partition_cookie.to_string().parse().unwrap(),
        );
    }

    if let Some(paging_state_cookie) = cookies.get_pending("paging_state") {
        headers.append(
            http::header::SET_COOKIE,
            paging_state_cookie.to_string().parse().unwrap(),
        );
    }

    let mut res = json(&SuccessBody::new(MessageChildrenResponse {
        message_id: message_id.to_string(),
        max_results: 2 * page_size,
        count: messages.len(),
        children_message_ids: messages.drain(..).map(|record| record.into()).collect(),
    }))
    .into_response();

    res.headers_mut().extend(headers);

    Ok(res)
}

async fn get_message_by_index(
    keyspace: String,
    last_milestone_cookie: Option<String>,
    last_partition_cookie: Option<String>,
    paging_state_cookie: Option<String>,
    mut cookies: CookieJar<'_>,
    partition_config: PartitionConfig,
    IndexParams { index, page_size }: IndexParams,
) -> Result<Response, Rejection> {
    if index.len() > 64 {
        return Err(ErrorBody::from("Provided index is too large! (Max 64 characters)").into());
    }
    let indexation = Indexation(index.clone());
    let page_size = page_size.unwrap_or(1000);

    if let Some(last_milestone_cookie) = last_milestone_cookie {
        cookies.add_original(Cookie::new("last_milestone_index", last_milestone_cookie));
    }
    if let Some(last_partition_cookie) = last_partition_cookie {
        cookies.add_original(Cookie::new("last_partition_id", last_partition_cookie));
    }
    if let Some(paging_state_cookie) = paging_state_cookie {
        cookies.add_original(Cookie::new("paging_state", paging_state_cookie));
    }

    let mut messages = page(
        keyspace.clone(),
        Hint::index(index.clone()),
        page_size,
        &cookies,
        format!("/api/{}/messages", keyspace),
        &partition_config,
        indexation,
    )
    .await
    .map_err(|e| warp::reject::custom(ErrorBody::from(e)))?;

    let mut headers = HeaderMap::new();

    if let Some(last_milestone_cookie) = cookies.get_pending("last_milestone_index") {
        headers.append(
            http::header::SET_COOKIE,
            last_milestone_cookie.to_string().parse().unwrap(),
        );
    }

    if let Some(last_partition_cookie) = cookies.get_pending("last_partition_id") {
        headers.append(
            http::header::SET_COOKIE,
            last_partition_cookie.to_string().parse().unwrap(),
        );
    }

    if let Some(paging_state_cookie) = cookies.get_pending("paging_state") {
        headers.append(
            http::header::SET_COOKIE,
            paging_state_cookie.to_string().parse().unwrap(),
        );
    }

    let mut res = json(&SuccessBody::new(MessagesForIndexResponse {
        index,
        max_results: 2 * page_size,
        count: messages.len(),
        message_ids: messages.drain(..).map(|record| record.into()).collect(),
    }))
    .into_response();
    res.headers_mut().extend(headers);

    Ok(res)
}

async fn get_ed25519_outputs(
    keyspace: String,
    address: String,
    last_milestone_cookie: Option<String>,
    last_partition_cookie: Option<String>,
    paging_state_cookie: Option<String>,
    mut cookies: CookieJar<'_>,
    partition_config: PartitionConfig,
    PageParam { page_size }: PageParam,
) -> Result<Response, Rejection> {
    let ed25519_address = Ed25519Address::from_str(&address).unwrap();
    let page_size = page_size.unwrap_or(100);

    if let Some(last_milestone_cookie) = last_milestone_cookie {
        cookies.add_original(Cookie::new("last_milestone_index", last_milestone_cookie));
    }
    if let Some(last_partition_cookie) = last_partition_cookie {
        cookies.add_original(Cookie::new("last_partition_id", last_partition_cookie));
    }
    if let Some(paging_state_cookie) = paging_state_cookie {
        cookies.add_original(Cookie::new("paging_state", paging_state_cookie));
    }

    let mut outputs = page(
        keyspace.clone(),
        Hint::address(ed25519_address.to_string()),
        page_size,
        &cookies,
        format!("api/{}/addresses/ed25519/{}/outputs", keyspace, address),
        &partition_config,
        ed25519_address,
    )
    .await
    .map_err(|e| warp::reject::custom(ErrorBody::from(e)))?;

    let mut headers = HeaderMap::new();

    if let Some(last_milestone_cookie) = cookies.get_pending("last_milestone_index") {
        headers.append(
            http::header::SET_COOKIE,
            last_milestone_cookie.to_string().parse().unwrap(),
        );
    }

    if let Some(last_partition_cookie) = cookies.get_pending("last_partition_id") {
        headers.append(
            http::header::SET_COOKIE,
            last_partition_cookie.to_string().parse().unwrap(),
        );
    }

    if let Some(paging_state_cookie) = cookies.get_pending("paging_state") {
        headers.append(
            http::header::SET_COOKIE,
            paging_state_cookie.to_string().parse().unwrap(),
        );
    }

    let mut res = json(&SuccessBody::new(OutputsForAddressResponse {
        address_type: 1,
        address,
        max_results: 2 * page_size,
        count: outputs.len(),
        output_ids: outputs.drain(..).map(|record| record.into()).collect(),
    }))
    .into_response();

    res.headers_mut().extend(headers);

    Ok(res)
}

async fn get_output(keyspace: String, output_id: String) -> Result<Json, Rejection> {
    let output_id = OutputId::from_str(&output_id).unwrap();

    let output_data = query::<OutputRes, _, _>(PermanodeKeyspace::new(keyspace.clone()), output_id, None, None)
        .await
        .map_err(|e| warp::reject::custom(ErrorBody::from(e)))?;
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
    Ok(json(&SuccessBody::new(OutputResponse {
        message_id: output_data.output.message_id().to_string(),
        transaction_id: output_id.transaction_id().to_string(),
        output_index: output_id.index(),
        is_spent,
        output: output_data
            .output
            .inner()
            .try_into()
            .map_err(|e| warp::reject::custom(ErrorBody::from(e)))?,
    })))
}

async fn get_milestone(keyspace: String, index: u32) -> Result<Json, Rejection> {
    let keyspace = PermanodeKeyspace::new(keyspace);

    query::<Milestone, _, _>(keyspace, MilestoneIndex::from(index), None, None)
        .await
        .map(|milestone| {
            json(&SuccessBody::new(MilestoneResponse {
                milestone_index: index,
                message_id: milestone.message_id().to_string(),
                timestamp: milestone.timestamp(),
            }))
        })
        .map_err(|_| ErrorBody::from(format!("No milestone found for index {}", index)).into())
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
        let routes = construct_routes(&Default::default());
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
        let routes = construct_routes(&Default::default());
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
        let routes = construct_routes(&Default::default());
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
        assert_eq!(res.body(), "Unhandled rejection: ErrorBody { msg: \"NoRing\" }");
    }

    #[tokio::test]
    async fn get_message_by_index() {
        let routes = construct_routes(&Default::default());
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
        assert_eq!(res.body(), "Unhandled rejection: ErrorBody { msg: \"NoRing\" }");
    }
}
