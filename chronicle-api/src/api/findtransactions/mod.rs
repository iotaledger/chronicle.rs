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

//! This module defines the methods/structures for API findtransactions.

mod addresses;
mod approvees;
mod bundles;
pub mod hints;
mod tags;

use crate::api::{
    findtransactions::{
        addresses::Rows as AddressesRows,
        approvees::Rows as ApproveesRows,
        bundles::Rows as BundlesRows,
        hints::{Hint, Rows as HintsRows},
        tags::Rows as TagsRows,
    },
    types::{Milestones, Trytes27, Trytes81},
};
use chronicle_common::actor;
use chronicle_cql::{
    compression::MyCompression,
    frame::decoder::{Decoder, Frame},
};
use chronicle_storage::{
    ring::Ring,
    stage::reporter,
    worker::{Error, Worker},
};
use hyper::{Body, Response};
use log::*;
use serde::Serialize;
use std::collections::VecDeque;
use tokio::sync::mpsc;
type Sender = mpsc::UnboundedSender<Event>;
#[derive(Debug)]
/// The sender of FindTransactions API.
pub struct FindTransactionsId(Sender);

actor!(FindTransactionsBuilder {
    addresses: Option<Vec<Trytes81>>,
    bundles: Option<Vec<Trytes81>>,
    approvees: Option<Vec<Trytes81>>,
    tags: Option<Vec<Trytes27>>,
    hints: Option<Vec<Hint>>
});

impl FindTransactionsBuilder {
    /// Build the structure for FindTransactions API.
    pub fn build(self) -> FindTransactions {
        let (tx, rx) = mpsc::unbounded_channel::<Event>();
        let worker = Box::new(FindTransactionsId(tx));
        FindTransactions {
            addresses: self.addresses.unwrap(),
            bundles: self.bundles.unwrap(),
            approvees: self.approvees.unwrap(),
            tags: self.tags.unwrap(),
            hints: self.hints.unwrap(),
            worker: Some(worker),
            rx,
        }
    }
}

/// Struct for FindTransactions API usage, currently supports `addresses`/`bundle`/`approvees`/`tags`
/// with `hints`.
pub struct FindTransactions {
    addresses: Option<Vec<Trytes81>>,
    bundles: Option<Vec<Trytes81>>,
    approvees: Option<Vec<Trytes81>>,
    tags: Option<Vec<Trytes27>>,
    hints: Option<Vec<Hint>>,
    worker: Option<Box<FindTransactionsId>>,
    rx: mpsc::UnboundedReceiver<Event>,
}

#[derive(Serialize, Default)]
/// The responsed transaction strcture to get more transactions by hints.
pub struct ResTransactions {
    hashes: Vec<Trytes81>,
    milestones: Milestones,
    values: Vec<i64>,
    timestamps: Vec<u64>,
    hints: Option<Vec<Hint>>,
}

impl ResTransactions {
    fn new(hints: Vec<Hint>) -> Self {
        let mut res_txs = ResTransactions::default();
        res_txs.hints.replace(hints);
        res_txs
    }
}

impl FindTransactions {
    /// Start running the process function.
    pub async fn run(self) -> Response<Body> {
        match self.process().await {
            Ok(response) => return response,
            Err(response) => return response,
        }
    }
    async fn process(mut self) -> Result<Response<Body>, Response<Body>> {
        self.process_bundles().await?;
        self.process_approvees().await?;
        self.process_addresses().await?;
        self.process_tags().await?;
        // take whatever hints we did found
        let mut hints = self.hints.take().unwrap();
        let res_txs = ResTransactions::new(Vec::new());
        // move the hints to be processed and return empty_hints and the computed res_txs
        let (mut empty_hints_buffer, mut res_txs) = self.process_hints(hints, res_txs).await?;
        // force min hashes length to be returned to the user.
        while (res_txs.hashes.len() < 1000) && !res_txs.hints.as_ref().unwrap().is_empty() {
            // take hints
            hints = res_txs.hints.take().unwrap();
            // put an empty buffer
            res_txs.hints.replace(empty_hints_buffer);
            // process the hints
            let tuple_result = self.process_hints(hints, res_txs).await?;
            // return the buffer
            empty_hints_buffer = tuple_result.0;
            // return the res_txs
            res_txs = tuple_result.1;
        }
        Ok(response!(body: serde_json::to_string(&res_txs).unwrap()))
    }
    async fn process_bundles(&mut self) -> Result<(), Response<Body>> {
        // take worker
        let mut worker = self.worker.take().unwrap();
        // take hints
        let mut hints = self.hints.take().unwrap_or_default();
        // process bundle by bundle (if any)
        if let Some(bundles) = self.bundles.take() {
            for bundle in bundles {
                // create request
                let payload = bundles::query(&bundle);
                let request = reporter::Event::Request { payload, worker };
                // send request using ring, todo use shard-awareness algo
                Ring::send_local_random_replica(rand::random::<i64>(), request);
                // loop till we aknoweledge response for the bundle
                loop {
                    match self.rx.recv().await.unwrap() {
                        Event::Response { giveload, pid } => {
                            // return the ownership of the pid.
                            worker = pid;
                            let decoder = Decoder::new(giveload, MyCompression::get());
                            if decoder.is_rows() {
                                hints = bundles::Hints::new(decoder, bundle, 0, 0, VecDeque::new(), hints)
                                    .decode()
                                    .finalize();
                                break;
                            } else {
                                error!("bundle: {:?}", decoder.get_error());
                                // it's for future impl to be used with execute
                                if decoder.is_unprepared() {
                                    // retry using normal query
                                    todo!();
                                } else {
                                    return Err(
                                        response!(status: INTERNAL_SERVER_ERROR, body: r#"{"error":"scylla error while processing bundle"}"#),
                                    );
                                }
                            }
                        }
                        Event::Error {
                            kind: _error,
                            pid: _pid,
                        } => {
                            return Err(
                                response!(status: INTERNAL_SERVER_ERROR, body: r#"{"error":"internal error while processing bundle"}"#),
                            );
                        }
                    }
                }
            }
        }
        // update hints
        self.hints.replace(hints);
        // update worker
        self.worker.replace(worker);
        // return ok
        Ok(())
    }

    async fn process_tags(&mut self) -> Result<(), Response<Body>> {
        // take worker
        let mut worker = self.worker.take().unwrap();
        // take hints
        let mut hints = self.hints.take().unwrap_or_default();
        // process tag by tag (if any)
        if let Some(tags) = self.tags.take() {
            for tag in tags {
                // create request
                let payload = tags::query(&tag);
                let request = reporter::Event::Request { payload, worker };
                // send request using ring, todo use shard-awareness algo
                Ring::send_local_random_replica(rand::random::<i64>(), request);
                loop {
                    match self.rx.recv().await.unwrap() {
                        Event::Response { giveload, pid } => {
                            // return the ownership of the pid.
                            worker = pid;
                            let decoder = Decoder::new(giveload, MyCompression::get());
                            if decoder.is_rows() {
                                hints = tags::Hints::new(decoder, tag, 0, 0, VecDeque::new(), hints)
                                    .decode()
                                    .finalize();
                                break;
                            } else {
                                error!("tag: {:?}", decoder.get_error());
                                // it's for future impl to be used with execute
                                if decoder.is_unprepared() {
                                    // retry using normal query
                                    todo!();
                                } else {
                                    return Err(
                                        response!(status: INTERNAL_SERVER_ERROR, body: r#"{"error":"scylla error while processing tag"}"#),
                                    );
                                }
                            }
                        }
                        Event::Error { .. } => {
                            return Err(
                                response!(status: INTERNAL_SERVER_ERROR, body: r#"{"error":"internal error while processing tag"}"#),
                            );
                        }
                    }
                }
            }
        }
        // update hints
        self.hints.replace(hints);
        // update worker
        self.worker.replace(worker);
        // return ok
        Ok(())
    }

    async fn process_approvees(&mut self) -> Result<(), Response<Body>> {
        // take worker
        let mut worker = self.worker.take().unwrap();
        // take hints
        let mut hints = self.hints.take().unwrap_or_default();
        // process approvee by approvee (if any)
        if let Some(approvees) = self.approvees.take() {
            for approvee in approvees {
                // create request
                let payload = approvees::query(&approvee);
                let request = reporter::Event::Request { payload, worker };
                // send request using ring, todo use shard-awareness algo
                Ring::send_local_random_replica(rand::random::<i64>(), request);
                loop {
                    match self.rx.recv().await.unwrap() {
                        Event::Response { giveload, pid } => {
                            // return the ownership of the pid.
                            worker = pid;
                            let decoder = Decoder::new(giveload, MyCompression::get());
                            if decoder.is_rows() {
                                hints = approvees::Hints::new(decoder, approvee, 0, 0, VecDeque::new(), hints)
                                    .decode()
                                    .finalize();
                                break;
                            } else {
                                error!("approvee: {:?}", decoder.get_error());
                                // it's for future impl to be used with execute
                                if decoder.is_unprepared() {
                                    // retry using normal query
                                    todo!();
                                } else {
                                    return Err(
                                        response!(status: INTERNAL_SERVER_ERROR, body: r#"{"error":"scylla error while processing approvee"}"#),
                                    );
                                }
                            }
                        }
                        Event::Error { .. } => {
                            return Err(
                                response!(status: INTERNAL_SERVER_ERROR, body: r#"{"error":"internal error while processing approvee"}"#),
                            );
                        }
                    }
                }
            }
        }
        // update hints
        self.hints.replace(hints);
        // update worker
        self.worker.replace(worker);
        // return ok
        Ok(())
    }

    async fn process_addresses(&mut self) -> Result<(), Response<Body>> {
        // take worker
        let mut worker = self.worker.take().unwrap();
        // take hints
        let mut hints = self.hints.take().unwrap_or_default();
        // process address by addresss (if any)
        if let Some(addresses) = self.addresses.take() {
            for address in addresses {
                // create request
                let payload = addresses::query(&address);
                let request = reporter::Event::Request { payload, worker };
                // send request using ring, todo use shard-awareness algo
                Ring::send_local_random_replica(rand::random::<i64>(), request);
                loop {
                    match self.rx.recv().await.unwrap() {
                        Event::Response { giveload, pid } => {
                            // return the ownership of the pid.
                            worker = pid;
                            let decoder = Decoder::new(giveload, MyCompression::get());
                            if decoder.is_rows() {
                                hints = addresses::Hints::new(decoder, address, 0, 0, VecDeque::new(), hints)
                                    .decode()
                                    .finalize();
                                break;
                            } else {
                                error!("address: {:?}", decoder.get_error());
                                // it's for future impl to be used with execute
                                if decoder.is_unprepared() {
                                    // retry using normal query
                                    todo!();
                                } else {
                                    return Err(
                                        response!(status: INTERNAL_SERVER_ERROR, body: r#"{"error":"scylla error while processing address"}"#),
                                    );
                                }
                            }
                        }
                        Event::Error { .. } => {
                            return Err(
                                response!(status: INTERNAL_SERVER_ERROR, body: r#"{"error":"internal error while processing address"}"#),
                            );
                        }
                    }
                }
            }
        }
        // update hints
        self.hints.replace(hints);
        // update worker
        self.worker.replace(worker);
        // return ok
        Ok(())
    }

    async fn process_hints(
        &mut self,
        mut hints: Vec<Hint>,
        mut res_txs: ResTransactions,
    ) -> Result<(Vec<Hint>, ResTransactions), Response<Body>> {
        // take worker
        let mut worker = self.worker.take().unwrap();
        // process hint by hint (if any)
        for mut hint in hints.drain(..) {
            // create request
            if let Some(payload) = hints::query(&mut hint) {
                let request = reporter::Event::Request { payload, worker };
                // send request using ring, todo use shard-awareness algo
                Ring::send_local_random_replica(rand::random::<i64>(), request);
                loop {
                    match self.rx.recv().await.unwrap() {
                        Event::Response { giveload, pid } => {
                            // return the ownership of the pid.
                            worker = pid;
                            let decoder = Decoder::new(giveload, MyCompression::get());
                            if decoder.is_rows() {
                                res_txs = hints::ResTxs::new(decoder, hint, res_txs).decode().finalize();
                                break;
                            } else {
                                error!("hint: {:?}", decoder.get_error());
                                // it's for future impl to be used with execute
                                if decoder.is_unprepared() {
                                    // retry using normal query
                                    todo!();
                                } else {
                                    return Err(
                                        response!(status: INTERNAL_SERVER_ERROR, body: r#"{"error":"scylla error while processing hint"}"#),
                                    );
                                }
                            }
                        }
                        Event::Error { .. } => {
                            return Err(
                                response!(status: INTERNAL_SERVER_ERROR, body: r#"{"error":"internal error while processing hint"}"#),
                            );
                        }
                    }
                }
            } else {
                // hint with empty timeline
                // this can be used for future filtering support where the user send hint
                // in advance without knowning the timeline
                // we skip this hint for the meantime.
            }
        }
        // update worker
        self.worker.replace(worker);
        // return empty hints to be used as buffer in case we didn't satisify the min page size.
        Ok((hints, res_txs))
    }
}
/// The event enum for FindTransactions API usage.
pub enum Event {
    /// Event of getting response payload.
    Response {
        /// THe payload.
        giveload: Vec<u8>,
        /// The process ID.
        pid: Box<FindTransactionsId>,
    },
    /// Error when getting FindTransactions API response.
    Error {
        /// The Erorr kind.
        kind: Error,
        /// The process ID.
        pid: Box<FindTransactionsId>,
    },
}

// implementation!
impl Worker for FindTransactionsId {
    fn send_response(self: Box<Self>, _: &Option<reporter::Sender>, giveload: Vec<u8>) {
        // to enable reusable self(Sender), we will do unsafe trick
        unsafe {
            // convert box into raw
            let raw = Box::into_raw(self);
            // convert back to box from raw
            let pid = Box::from_raw(raw);
            let event = Event::Response { giveload, pid };
            // now we can use raw to send self through itself.
            let _ = (*raw).0.send(event);
        }
    }
    fn send_error(self: Box<Self>, kind: Error) {
        unsafe {
            // convert box into raw
            let raw = Box::into_raw(self);
            // convert back to box from raw
            let pid = Box::from_raw(raw);
            let event = Event::Error { kind, pid };
            // now we can use raw to send itself through itself.
            let _ = (*raw).0.send(event);
        }
    }
}
