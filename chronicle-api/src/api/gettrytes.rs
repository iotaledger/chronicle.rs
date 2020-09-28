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

//! This module implements the GetTrytes API call.

use super::types::Milestones;
use chronicle_common::actor;
use chronicle_cql::{
    compression::MyCompression,
    frame::{
        consistency::Consistency,
        decoder::{Decoder, Frame},
        header::Header,
        query::Query,
        queryflags,
    },
    rows,
};
use chronicle_storage::{
    ring::Ring,
    stage::reporter,
    worker::{Error, Worker},
};
use hyper::{Body, Response};
use log::*;
use serde::Serialize;
use serde_json::Value as JsonValue;
use tokio::sync::mpsc;
type Sender = mpsc::UnboundedSender<Event>;
type Receiver = mpsc::UnboundedReceiver<Event>;
#[derive(Debug)]
/// Struct of GetTrytes API.
pub struct GetTrytesId(Sender);

actor!(GetTrytesBuilder {
    hashes: Vec<JsonValue>
});

impl GetTrytesBuilder {
    /// Build the struct of GetTrytes by builder pattern.
    pub fn build(self) -> GetTrytes {
        GetTrytes {
            hashes: self.hashes.unwrap(),
        }
    }
}

/// GetTrytes structure, currently supports `hashes`.
pub struct GetTrytes {
    hashes: Vec<JsonValue>,
}

#[derive(Serialize)]
struct ResTrytes {
    trytes: Vec<JsonValue>,
    milestones: Milestones,
}

impl GetTrytes {
    /// Start running the GetTrytes process.
    pub async fn run(mut self) -> Response<Body> {
        let (tx, mut rx) = mpsc::unbounded_channel::<Event>();
        let mut worker = Box::new(GetTrytesId(tx));
        let hashes = self.hashes.iter_mut();
        let mut milestones: Milestones = Vec::new();
        for value in hashes {
            worker = Self::process(value, &mut milestones, worker, &mut rx).await;
        }
        let res_trytes = ResTrytes {
            trytes: self.hashes,
            milestones,
        };
        response!(body: serde_json::to_string(&res_trytes).unwrap())
    }

    async fn process(
        value: &mut JsonValue,
        milestones: &mut Milestones,
        worker: Box<GetTrytesId>,
        rx: &mut Receiver,
    ) -> Box<GetTrytesId> {
        // by taking the value we are leaving behind null.
        // now we try to query and get the result
        if let JsonValue::String(hash) = value.take() {
            let request = reporter::Event::Request {
                payload: Self::query(hash),
                worker,
            };
            // use random token till murmur3 hash function algo impl is ready
            // send_local_random_replica will select random replica for token.
            Ring::send_local_random_replica(rand::random::<i64>(), request);
            match rx.recv().await.unwrap() {
                Event::Response { giveload, pid } => {
                    // create decoder
                    let decoder = Decoder::new(giveload, MyCompression::get());
                    if decoder.is_rows() {
                        if let Some((trytes, milestone)) = Trytes::new(decoder, None).decode().finalize() {
                            *value = serde_json::value::Value::String(trytes);
                            milestones.push(milestone)
                        } else {
                            milestones.push(None);
                        };
                    } else {
                        error!("GetTrytes: {:?}", decoder.get_error());
                    }
                    pid
                }
                Event::Error { pid, .. } => {
                    // do nothing to the value as it's already null,
                    // still we have to push none to milestones.
                    milestones.push(None);
                    // still we can apply other retry strategies
                    pid
                }
            }
        } else {
            unreachable!()
        }
    }
    fn query(hash: String) -> Vec<u8> {
        let Query(payload) = Query::new()
            .version()
            .flags(MyCompression::flag())
            .stream(0)
            .opcode()
            .length()
            .statement(SELECT_TX_QUERY)
            .consistency(Consistency::One)
            .query_flags(queryflags::SKIP_METADATA | queryflags::VALUES)
            .value_count(1) // the total value count
            .value(hash)
            .build(MyCompression::get());
        payload
    }
}

/// The event enum for GetTrytes API.
pub enum Event {
    /// The response structure for the API call.
    Response {
        /// The payload of the ScyllaDB response.
        giveload: Vec<u8>,
        /// The process ID of this API call.
        pid: Box<GetTrytesId>,
    },
    /// The Error structure for the API call.
    Error {
        /// The error kind of the API call.
        kind: Error,
        /// The process ID of this API call.
        pid: Box<GetTrytesId>,
    },
}

// implementation!
impl Worker for GetTrytesId {
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

rows!(
    rows: Trytes {milestone: Option<u64>},
    row: Row(
        Payload,
        Address,
        Value,
        ObsoleteTag,
        Timestamp,
        CurrentIndex,
        LastIndex,
        Bundle,
        Trunk,
        Branch,
        Tag,
        AttachmentTimestamp,
        AttachmentTimestampLower,
        AttachmentTimestampUpper,
        Nonce,
        Milestone
    ),
    column_decoder: TrytesDecoder
);

// define decoder trait as you wish
trait Rows {
    // to decode the rows
    fn decode(self) -> Self;
    // to finalize it as the expected result (trytes or none)
    fn finalize(self) -> Option<(String, Option<u64>)>;
}

impl Rows for Trytes {
    fn decode(mut self) -> Self {
        // each next() call will decode one row
        self.next();
        // return
        self
    }
    fn finalize(mut self) -> Option<(String, Option<u64>)> {
        // check if result was not empty
        if self.rows_count == 1 {
            // the buffer is ready to be converted to string trytes
            self.buffer().truncate(2673);
            Some((String::from_utf8(self.decoder.into_buffer()).unwrap(), self.milestone))
        } else {
            // we didn't have any transaction row for the provided hash.
            None
        }
    }
}
// implementation to decoder the columns in order to form the trytes eventually
impl TrytesDecoder for Payload {
    fn decode_column(start: usize, length: i32, acc: &mut Trytes) {
        // Payload trytes offest is 0..2187, note: assuming length != -1(indicate empty column).
        // copy_within so a buffer[0..2187] will = buffer[start..length]
        acc.buffer().copy_within(start..(start + length as usize), 0)
    }
    fn handle_null(_: &mut Trytes) {
        unreachable!()
    }
}
impl TrytesDecoder for Address {
    fn decode_column(start: usize, length: i32, acc: &mut Trytes) {
        // Address trytes offest is 2187..2268, note: we assume the length value is also correct
        acc.buffer().copy_within(start..(start + length as usize), 2187)
    }
    fn handle_null(_: &mut Trytes) {
        unreachable!()
    }
}
impl TrytesDecoder for Value {
    fn decode_column(start: usize, length: i32, acc: &mut Trytes) {
        // Value tryte offset is 2268..2295
        acc.buffer().copy_within(start..(start + length as usize), 2268);
    }
    fn handle_null(_: &mut Trytes) {
        unreachable!()
    }
}
impl TrytesDecoder for ObsoleteTag {
    fn decode_column(start: usize, length: i32, acc: &mut Trytes) {
        // ObsoleteTag tryte offset is 2295..2322
        acc.buffer().copy_within(start..(start + length as usize), 2295)
    }
    fn handle_null(_: &mut Trytes) {
        unreachable!()
    }
}
impl TrytesDecoder for Timestamp {
    fn decode_column(start: usize, length: i32, acc: &mut Trytes) {
        // Timestamp tryte offset is 2322..2331
        acc.buffer().copy_within(start..(start + length as usize), 2322);
    }
    fn handle_null(_: &mut Trytes) {
        unreachable!()
    }
}
impl TrytesDecoder for CurrentIndex {
    fn decode_column(start: usize, length: i32, acc: &mut Trytes) {
        // CurrentIndex tryte offset is 2331..2340
        acc.buffer().copy_within(start..(start + length as usize), 2331);
    }
    fn handle_null(_: &mut Trytes) {
        unreachable!()
    }
}
impl TrytesDecoder for LastIndex {
    fn decode_column(start: usize, length: i32, acc: &mut Trytes) {
        // LastIndex tryte offset is 2340..2349
        acc.buffer().copy_within(start..(start + length as usize), 2340);
    }
    fn handle_null(_: &mut Trytes) {
        unreachable!()
    }
}
impl TrytesDecoder for Bundle {
    fn decode_column(start: usize, length: i32, acc: &mut Trytes) {
        // Bundle tryte offset is 2349..2430
        acc.buffer().copy_within(start..(start + length as usize), 2349)
    }
    fn handle_null(_: &mut Trytes) {
        unreachable!()
    }
}
impl TrytesDecoder for Trunk {
    fn decode_column(start: usize, length: i32, acc: &mut Trytes) {
        // Trunk tryte offset is 2430..2511
        acc.buffer().copy_within(start..(start + length as usize), 2430)
    }
    fn handle_null(_: &mut Trytes) {
        unreachable!()
    }
}
impl TrytesDecoder for Branch {
    fn decode_column(start: usize, length: i32, acc: &mut Trytes) {
        // Branch tryte offset is 2511..2592
        acc.buffer().copy_within(start..(start + length as usize), 2511)
    }
    fn handle_null(_: &mut Trytes) {
        unreachable!()
    }
}
impl TrytesDecoder for Tag {
    fn decode_column(start: usize, length: i32, acc: &mut Trytes) {
        // Tag tryte offset is 2592..2619
        acc.buffer().copy_within(start..(start + length as usize), 2592)
    }
    fn handle_null(_: &mut Trytes) {
        unreachable!()
    }
}
impl TrytesDecoder for AttachmentTimestamp {
    fn decode_column(start: usize, length: i32, acc: &mut Trytes) {
        // AttachmentTimestamp tryte offset is 2619..2628
        acc.buffer().copy_within(start..(start + length as usize), 2619);
    }
    fn handle_null(_: &mut Trytes) {
        unreachable!()
    }
}
impl TrytesDecoder for AttachmentTimestampLower {
    fn decode_column(start: usize, length: i32, acc: &mut Trytes) {
        // AttachmentTimestampLower tryte offset is 2628..2637
        acc.buffer().copy_within(start..(start + length as usize), 2628);
    }
    fn handle_null(_: &mut Trytes) {
        unreachable!()
    }
}
impl TrytesDecoder for AttachmentTimestampUpper {
    fn decode_column(start: usize, length: i32, acc: &mut Trytes) {
        // AttachmentTimestampUpper tryte offset is 2637..2646
        acc.buffer().copy_within(start..(start + length as usize), 2637);
    }
    fn handle_null(_: &mut Trytes) {
        unreachable!()
    }
}
impl TrytesDecoder for Nonce {
    fn decode_column(start: usize, length: i32, acc: &mut Trytes) {
        // Tag tryte offset is 2646..2673
        acc.buffer().copy_within(start..(start + length as usize), 2646)
    }
    fn handle_null(_: &mut Trytes) {
        unreachable!()
    }
}
impl TrytesDecoder for Milestone {
    fn decode_column(start: usize, length: i32, acc: &mut Trytes) {
        acc.milestone = Some(u64::from_be_bytes(
            acc.buffer()[start..(start + length as usize)].try_into().unwrap(),
        ));
    }
    fn handle_null(_: &mut Trytes) {
        // do nothing acc.milestone is already none.
    }
}

const SELECT_TX_QUERY: &str = {
    #[cfg(feature = "mainnet")]
    let cql = r#"
SELECT
payload,
address,
value,
obsolete_tag,
timestamp,
current_index,
last_index,
bundle,
trunk,
branch,
tag,
attachment_timestamp,
attachment_timestamp_lower,
attachment_timestamp_upper,
nonce,
milestone
FROM mainnet.transaction
WHERE hash = ?;
"#;
    #[cfg(feature = "devnet")]
    #[cfg(not(feature = "mainnet"))]
    #[cfg(not(feature = "comnet"))]
    let cql = r#"
SELECT
payload,
address,
value,
obsolete_tag,
timestamp,
current_index,
last_index,
bundle,
trunk,
branch,
tag,
attachment_timestamp,
attachment_timestamp_lower,
attachment_timestamp_upper,
nonce,
milestone
FROM devnet.transaction
WHERE hash = ?;
"#;
    #[cfg(feature = "comnet")]
    #[cfg(not(feature = "mainnet"))]
    #[cfg(not(feature = "devnet"))]
    let cql = r#"
SELECT
payload,
address,
value,
obsolete_tag,
timestamp,
current_index,
last_index,
bundle,
trunk,
branch,
tag,
attachment_timestamp,
attachment_timestamp_lower,
attachment_timestamp_upper,
nonce,
milestone
FROM comnet.transaction
WHERE hash = ?;
"#;
    cql
};
