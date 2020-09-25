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

//! This macro builds response with user provided body (w/ status code) and predefined header.

macro_rules! response {
    (body: $body:expr) => {
        hyper::Response::builder()
            .header("Content-Type", "application/json")
            .body(Body::from($body))
            .unwrap()
    };
    (status: $status:tt, body: $body:expr) => {
        hyper::Response::builder()
            .header("Content-Type", "application/json")
            .status(hyper::StatusCode::$status)
            .body(Body::from($body))
            .unwrap()
    };
}

pub mod endpoint;
pub mod findtransactions;
pub mod gettrytes;
pub mod router;
pub mod types;

use chronicle_common::app;
use endpoint::EndpointBuilder;
app!(ApiBuilder {
    listen_address: String,
    content_length: Option<u32>
});

impl ApiBuilder {
    pub fn build(self) -> Api {
        // set content_length if provided
        if let Some(content_length) = self.content_length.unwrap() {
            unsafe { router::CONTENT_LENGTH = content_length };
        };
        Api {
            listen_address: self.listen_address.unwrap(),
            launcher_tx: self.launcher_tx.unwrap(),
        }
    }
}

pub struct Api {
    listen_address: String,
    launcher_tx: Box<dyn LauncherTx>,
}

impl Api {
    pub async fn run(self) {
        let server = EndpointBuilder::new()
            .listen_address(self.listen_address)
            .launcher_tx(self.launcher_tx)
            .build();
        tokio::spawn(server.run());
    }
}
