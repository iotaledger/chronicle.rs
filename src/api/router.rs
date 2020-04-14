use serde::{Deserialize, Serialize};
use serde_json::Value;
use hyper::body::Buf;
use hyper::{body::aggregate, Body, Request, Response, Method};
use std::convert::Infallible;
use super::gettrytes::GetTrytesBuilder;

#[derive(Debug, Deserialize, Serialize, Clone)]
struct ReqBody {
    command: String,
    hashes: Option<Vec<String>>,
    bundle: Option<Vec<String>>,
    address: Option<Vec<String>>,
    tag: Option<Vec<String>>,
    approvee: Option<Vec<String>>,
}

pub async fn handle(req: Request<Body>) -> Result<Response<Body>, Infallible> {
    let (parts, stream) = req.into_parts();
    match (parts.method,parts.uri.path(),parts.headers.get("content-length"), parts.headers.get("content-type")) {
        (
            Method::POST,
            "/api",
            Some(length),
            Some(application_json)
        )   if application_json == "application/json" => {
            if let Ok(length_str) =  length.to_str() {
                if let Ok(length_u32) = length_str.parse::<u32>() {
                    if length_u32 <= 16384 {
                        if let Ok(buffer) = aggregate(stream).await {
                            if let Ok(request) = serde_json::from_slice::<ReqBody>(buffer.bytes()) {
                                Ok(route(request).await)
                            } else {
                                Ok(response!(status: BAD_REQUEST, body: r#"{"error":"invalid request, check the api reference"}"#))
                            }
                        } else {
                            Ok(response!(status: BAD_REQUEST, body: r#"{"error":"invalid request"}"#))
                        }
                    } else {
                        // PAYLOAD_TOO_LARGE
                        Ok(response!(status: PAYLOAD_TOO_LARGE, body: r#"{"error":"request entity too large"}"#))
                    }
                } else {
                    // content-length is invalid
                    Ok(response!(status: BAD_REQUEST, body: r#"{"error":"content-length is invalid"}"#))
                }
            } else {
                // content-length is invalid
                Ok(response!(status: BAD_REQUEST, body: r#"{"error":"content-length is invalid"}"#))
            }
        }
        _ => {
            Ok(response!(
                status: BAD_REQUEST,
                body: r#"{"error":"can only POST application/json to /api where content-length <= 16384-bytes"}"#
            ))
        }
    }
}


async fn route(request: ReqBody) -> Response<Body> {
    match &request.command[..] {
        "getTrytes" => {
            if let Some(hashes) = request.hashes {
                if let Value::Array(hashes) = serde_json::to_value(hashes).unwrap() {
                    if !hashes.is_empty() {
                        GetTrytesBuilder::new()
                        .hashes(hashes)
                        .build().run().await
                    } else {
                        response!(status: BAD_REQUEST, body: r#"{"error":"No Hashes"}"#)
                    }
                } else {
                    unreachable!()
                }
            } else {
                response!(status: BAD_REQUEST, body: r#"{"error":"No Hashes"}"#)
            }
        }
        _ => {
            response!(status: BAD_REQUEST, body: r#"{"error":"Invalid Request Command"}"#)
        }
    }
}
