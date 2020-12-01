// Copyright 2020 IOTA Stiftung
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and limitations under the License.

//! This module implements the router for API calls.

use super::{
    findtransactions::{hints::Hint, FindTransactionsBuilder},
    gettrytes::GetTrytesBuilder,
    types::{Trytes27, Trytes81},
};
use hyper::{body::to_bytes, Body, Method, Request, Response};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::convert::Infallible;

/// This static constant is the maximum supported bytes for the user's request.
pub static mut CONTENT_LENGTH: u32 = 65535;

#[derive(Deserialize, Serialize)]
struct ReqBody {
    command: String,
    milestones: Option<Vec<Option<u64>>>,
    hashes: Option<Vec<Trytes81>>,
    bundles: Option<Vec<Trytes81>>,
    addresses: Option<Vec<Trytes81>>,
    tags: Option<Vec<Trytes27>>,
    hints: Option<Vec<Hint>>,
    approvees: Option<Vec<Trytes81>>,
}

/// Handle the user's request.
pub async fn handle(req: Request<Body>) -> Result<Response<Body>, Infallible> {
    let (parts, stream) = req.into_parts();
    match (
        parts.method,
        parts.uri.path(),
        parts.headers.get("content-length"),
        parts.headers.get("content-type"),
    ) {
        (Method::POST, "/api", Some(length), Some(application_json)) if application_json == "application/json" => {
            if let Ok(length_str) = length.to_str() {
                if let Ok(length_u32) = length_str.parse::<u32>() {
                    if length_u32 <= unsafe { CONTENT_LENGTH } {
                        if let Ok(buffer) = to_bytes(stream).await {
                            if let Ok(request) = serde_json::from_slice::<ReqBody>(&buffer) {
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
            let body = format!(
                "{{\"error\":\"can only POST application/json to /api where content-length <= {}-bytes\"}}",
                unsafe { CONTENT_LENGTH }
            );
            Ok(response!(status: BAD_REQUEST, body: body))
        }
    }
}

async fn route(request: ReqBody) -> Response<Body> {
    match &request.command[..] {
        "getTrytes" => {
            if let Some(hashes) = request.hashes {
                if let Value::Array(hashes) = serde_json::to_value(hashes).unwrap() {
                    if !hashes.is_empty() {
                        GetTrytesBuilder::new().hashes(hashes).build().run().await
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
        "findTransactions" => {
            FindTransactionsBuilder::new()
                .addresses(request.addresses)
                .tags(request.tags)
                .approvees(request.approvees)
                .bundles(request.bundles)
                .hints(request.hints)
                .build()
                .run()
                .await
        }
        _ => response!(status: BAD_REQUEST, body: r#"{"error":"Invalid Request Command"}"#),
    }
}
