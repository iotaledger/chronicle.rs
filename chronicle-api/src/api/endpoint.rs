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

//! This module defines the endpoint used by API calls.

use super::router::handle;
use chronicle_common::{
    actor,
    traits::{launcher::LauncherTx, shutdown::ShutdownTx},
};
use hyper::{
    server::Server,
    service::{make_service_fn, service_fn},
};
use log::*;
use std::{convert::Infallible, net::SocketAddr};

/// This structure provides oneshot channel to shut down the API endpoint.
pub struct Shutdown(tokio::sync::oneshot::Sender<()>);
actor!(EndpointBuilder { listen_address: String, launcher_tx: Box<dyn LauncherTx> });

impl EndpointBuilder {
    /// Endpoint builder.
    pub fn build(self) -> Endpoint {
        let addr: SocketAddr = self.listen_address.unwrap().parse().unwrap();
        let launcher_tx: Box<dyn LauncherTx> = self.launcher_tx.unwrap();
        Endpoint { addr, launcher_tx }
    }
}

/// API Endpoint structure.
pub struct Endpoint {
    /// The API socket address.
    addr: SocketAddr,
    /// The launcher (the one launches API application) sender channel.
    launcher_tx: Box<dyn LauncherTx>,
}
impl ShutdownTx for Shutdown {
    fn shutdown(self: Box<Self>) {
        self.0.send(()).unwrap();
    }
}
impl Endpoint {
    /// Run method of the endpoint, which will notify its launcher when this endpoint is shutted down.
    /// When the endpoint is running, `handle` function in the router mod will handle the request/reply.
    pub async fn run(mut self) {
        let service = make_service_fn(|_conn| async { Ok::<_, Infallible>(service_fn(handle)) });
        let server = Server::bind(&self.addr).serve(service);
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        // register api app with launcher
        self.launcher_tx.register_app("api".to_string(), Box::new(Shutdown(tx)));
        let graceful = server.with_graceful_shutdown(async {
            rx.await.ok();
        });
        if let Err(e) = graceful.await {
            error!("error: {}, endpoint: {}", e, self.addr);
        }
        // aknowledge_shutdown
        self.launcher_tx.aknowledge_shutdown("api".to_string());
    }
}
