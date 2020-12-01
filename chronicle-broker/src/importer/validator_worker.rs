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

//! This module implements low-level mqtt event handling methods.

use crate::importer::{trytes::Trytes, *};
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
use tokio::sync::mpsc;

use chrono::{Datelike, NaiveDateTime};
/// ValidatorWorker Sender type
pub type Sender = mpsc::UnboundedSender<Event>;
type Receiver = mpsc::UnboundedReceiver<Event>;

#[derive(Debug)]
/// ValidatorWorker process id
pub struct ValidatorWorkerId(Sender, u8, usize);
impl ValidatorWorkerId {
    fn query_id(mut self: Box<Self>, query_id: u8) -> Box<Self> {
        self.1 = query_id;
        self
    }
    fn get_query_id(&self) -> u8 {
        self.1
    }
    fn is_select(&self) -> bool {
        self.1 == 255
    }
}

/// The Worker event.
pub enum Event {
    Tip(String, u32, Sender),
    Tx {
        decoder: Decoder,
        pid: Box<ValidatorWorkerId>,
    },
    /// The Worker ID.
    Void {
        /// The process ID.
        pid: Box<ValidatorWorkerId>,
    },
    /// The CQL Error.
    Error {
        /// The error kind.
        kind: Error,
        /// The process ID.
        pid: Box<ValidatorWorkerId>,
    },
    /// shutdown variant
    Shutdown,
}

use chronicle_common::actor;

actor!(ValidatorWorkerBuilder {
    id: usize,
    force_correctness: bool,
    max_retries: usize
});

impl ValidatorWorkerBuilder {
    pub fn build(self) -> ValidatorWorker {
        let (tx, rx) = mpsc::unbounded_channel::<Event>();
        let mut pids = Vec::new();
        let max_retries = self.max_retries.unwrap();
        // create some pids in advance to enable us to send concurrent queries without the cost for heap-reallocation
        for _ in 0..50 {
            pids.push(Box::new(ValidatorWorkerId(tx.clone(), 0, max_retries)));
        }
        ValidatorWorker {
            current_ms: 0,
            id: self.id.unwrap(),
            tip_tx: None,
            force_correctness: self.force_correctness.unwrap_or(false),
            tx: Some(tx),
            rx,
            pending: 0,
            pids,
            max_retries,
            current_hash: String::new(),
        }
    }
}

pub struct ValidatorWorker {
    id: usize,
    tip_tx: Option<Sender>,
    force_correctness: bool,
    /// ValidatorWorker Sender handle
    pub tx: Option<Sender>,
    rx: Receiver,
    current_ms: u32,
    pids: Vec<Box<ValidatorWorkerId>>,
    pending: usize,
    max_retries: usize,
    current_hash: String,
}

impl ValidatorWorker {
    pub async fn run(mut self, supervisor_tx: mpsc::UnboundedSender<validator::Event>) {
        while let Some(event) = self.rx.recv().await {
            match event {
                Event::Tip(hash, ms, tip_tx) => {
                    self.tip_tx.replace(tip_tx);
                    // make sure the current_ms is identical to the current tip
                    self.current_ms = ms;
                    self.current_hash = hash;
                    // send query to get the transaction
                    let request = reporter::Event::Request {
                        payload: get_tx_query(&self.current_hash[..]),
                        worker: self
                            .pids
                            .pop()
                            .unwrap_or(Box::new(ValidatorWorkerId(
                                self.tx.as_ref().unwrap().clone(),
                                255,
                                self.max_retries,
                            )))
                            .query_id(255),
                    };
                    self.pending += 1;
                    Ring::send_local_random_replica(rand::random::<i64>(), request);
                }
                Event::Void { pid } => {
                    // decrement
                    self.pending -= 1;
                    // return pid
                    self.pids.push(pid);
                }
                Event::Tx { decoder, pid } => {
                    self.pending -= 1;
                    // return pid
                    self.pids.push(pid);
                    if let Some((trytes, milestone)) = Tx::new(decoder, None).decode().finalize() {
                        let trytes = Trytes::new(&trytes);
                        let trunk = trytes.trunk();
                        let branch = trytes.branch();
                        if let Some(record_ms) = milestone {
                            if record_ms < self.current_ms {
                                // send None as no further is required to proceed
                                let _ = supervisor_tx.send(validator::Event::Tip(None, self.id, self.tip_tx.take().unwrap()));
                                // stop to not force_correctness;
                                continue;
                            } else {
                                // send trunk and branch
                                let _ = supervisor_tx.send(validator::Event::Tip(
                                    Some([trunk.to_string(), branch.to_string()]),
                                    self.id,
                                    self.tip_tx.take().unwrap(),
                                ));
                            }
                        } else {
                            // send trunk and branch
                            let _ = supervisor_tx.send(validator::Event::Tip(
                                Some([trunk.to_string(), branch.to_string()]),
                                self.id,
                                self.tip_tx.take().unwrap(),
                            ));
                        }
                        // presist the transaction
                        if self.force_correctness {
                            let timestamp_ms: i64;
                            if let Some(m) = milestone {
                                if m == self.current_ms {
                                    // no need to reinsert we continue
                                    continue;
                                } else if m > 337_541 {
                                    timestamp_ms = pick_timestamp_ms(trytes.atch_timestamp(), trytes.timestamp());
                                } else {
                                    // timestamp only
                                    let timestamp = trytes_to_i64(trytes.timestamp());
                                    timestamp_ms = get_timestamp_ms(timestamp);
                                }
                            } else {
                                timestamp_ms = pick_timestamp_ms(trytes.atch_timestamp(), trytes.timestamp());
                            }
                            info!(
                                "correcting transaction: hash: {}, indirectly confirmed by: {:?} to directly confirmed by: {}",
                                self.current_hash, milestone, self.current_ms
                            );
                            self.persist_transaction(trytes, self.current_ms.into(), timestamp_ms);
                        }
                    } else {
                        warn!(
                            "validator worker found gap: {}, in milestone: {}",
                            &self.current_hash, self.current_ms
                        );
                        // tell validator about a gap
                        let _ = supervisor_tx.send(validator::Event::Gap(
                            self.current_hash.clone(),
                            self.id,
                            self.tip_tx.take().unwrap(),
                        ));
                    }
                }
                Event::Shutdown => {
                    if self.pending == 0 {
                        // drop all pids including self tx
                        self.pids.clear();
                        self.tx.take();
                    } else {
                        // send shutdown event // TODO add delay for 1 second or so;
                        let _ = self.tx.as_mut().unwrap().send(Event::Shutdown);
                    }
                }

                Event::Error { kind, pid } => {
                    // TODO add error handling
                    println!("{}", kind);
                }
            }
        }
    }
    // presist transaction
    fn persist_transaction<'a>(&mut self, trytes: Trytes<'a>, milestone: i64, timestamp_ms: i64) {
        let naive = NaiveDateTime::from_timestamp(timestamp_ms / 1000, 0);
        let year = naive.year() as u16;
        let month = naive.month() as u8;
        // ----------- transaction table query ---------------
        let payload = insert_to_tx_table(&self.current_hash[..], &trytes, milestone);
        let request = reporter::Event::Request {
            payload,
            worker: self
                .pids
                .pop()
                .unwrap_or(Box::new(ValidatorWorkerId(self.tx.as_ref().unwrap().clone(), 1, self.max_retries)))
                .query_id(1),
        };
        Ring::send_local_random_replica(rand::random::<i64>(), request);
        // extract the transaction value
        let value = trytes_to_i64(trytes.value());
        let address_kind;
        if value < 0 {
            address_kind = INPUT
        } else {
            address_kind = OUTPUT
        }
        let itr: [(&str, &str, &str, u8); 4] = [
            (trytes.trunk(), APPROVEE, TRUNK, 4),
            (trytes.branch(), APPROVEE, BRANCH, 6),
            (trytes.bundle(), BUNDLE, BUNDLE, 8),
            (trytes.tag(), TAG, TAG, 10),
        ];
        // presist by address
        let payload = insert_to_hint_table(trytes.address(), ADDRESS, year, month, milestone);
        let request = reporter::Event::Request {
            payload,
            worker: self
                .pids
                .pop()
                .unwrap_or(Box::new(ValidatorWorkerId(self.tx.as_ref().unwrap().clone(), 0, self.max_retries)))
                .query_id(2),
        };
        Ring::send_local_random_replica(rand::random::<i64>(), request);
        let payload = insert_to_data_table(
            trytes.address(),
            year,
            month,
            address_kind,
            timestamp_ms,
            &self.current_hash[..],
            value,
            milestone,
        );
        let request = reporter::Event::Request {
            payload,
            worker: self
                .pids
                .pop()
                .unwrap_or(Box::new(ValidatorWorkerId(self.tx.as_ref().unwrap().clone(), 0, self.max_retries)))
                .query_id(3),
        };
        Ring::send_local_random_replica(rand::random::<i64>(), request);
        // presist remaining queries
        for (vertex, hint_kind, data_kind, query_id) in &itr {
            // create payload and then put it inside reporter request
            let payload = insert_to_hint_table(vertex, hint_kind, year, month, milestone);
            let request = reporter::Event::Request {
                payload,
                worker: self
                    .pids
                    .pop()
                    .unwrap_or(Box::new(ValidatorWorkerId(self.tx.as_ref().unwrap().clone(), 0, self.max_retries)))
                    .query_id(*query_id),
            };
            // send request
            Ring::send_local_random_replica(rand::random::<i64>(), request);
            let payload = insert_to_data_table(
                vertex,
                year,
                month,
                data_kind,
                timestamp_ms,
                &self.current_hash[..],
                value,
                milestone,
            );
            // create another request
            let request = reporter::Event::Request {
                payload,
                worker: self
                    .pids
                    .pop()
                    .unwrap_or(Box::new(ValidatorWorkerId(self.tx.as_ref().unwrap().clone(), 0, self.max_retries)))
                    .query_id(query_id + 1),
            };
            // send it
            Ring::send_local_random_replica(rand::random::<i64>(), request);
        }
        self.pending += 11;
    }
}

impl Worker for ValidatorWorkerId {
    fn send_response(self: Box<Self>, _: &Option<reporter::Sender>, giveload: Vec<u8>) {
        let decoder = Decoder::new(giveload, MyCompression::get());
        unsafe {
            let raw = Box::into_raw(self);
            let pid = Box::from_raw(raw);
            let event;
            if decoder.is_error() {
                let error = decoder.get_error();
                event = Event::Error {
                    kind: Error::Cql(error),
                    pid,
                }
            } else {
                // if is insert
                if pid.is_select() {
                    event = Event::Tx { decoder, pid };
                } else {
                    event = Event::Void { pid };
                }
            }
            let _ = (*raw).0.send(event);
        }
    }
    fn send_error(self: Box<Self>, kind: Error) {
        unsafe {
            let raw = Box::into_raw(self);
            let pid = Box::from_raw(raw);
            let event = Event::Error { kind, pid };
            let _ = (*raw).0.send(event);
        }
    }
}

fn get_tx_query(hash: &str) -> Vec<u8> {
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

rows!(
    rows: Tx{milestone: Option<u32>},
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
    column_decoder: TxDecoder
);

// define decoder trait as you wish
trait Rows {
    // to decode the rows
    fn decode(self) -> Self;
    // to finalize it as the expected result (Tx or none)
    fn finalize(self) -> Option<(String, Option<u32>)>;
}

impl Rows for Tx {
    fn decode(mut self) -> Self {
        // each next() call will decode one row
        self.next();
        // return
        self
    }
    fn finalize(mut self) -> Option<(String, Option<u32>)> {
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
impl TxDecoder for Payload {
    fn decode_column(start: usize, length: i32, acc: &mut Tx) {
        // Payload trytes offest is 0..2187, note: assuming length != -1(indicate empty column).
        // copy_within so a buffer[0..2187] will = buffer[start..length]
        acc.buffer().copy_within(start..(start + length as usize), 0)
    }
    fn handle_null(_: &mut Tx) {
        unreachable!()
    }
}
impl TxDecoder for Address {
    fn decode_column(start: usize, length: i32, acc: &mut Tx) {
        // Address trytes offest is 2187..2268, note: we assume the length value is also correct
        acc.buffer().copy_within(start..(start + length as usize), 2187)
    }
    fn handle_null(_: &mut Tx) {
        unreachable!()
    }
}
impl TxDecoder for Value {
    fn decode_column(start: usize, length: i32, acc: &mut Tx) {
        // Value tryte offset is 2268..2295
        acc.buffer().copy_within(start..(start + length as usize), 2268);
    }
    fn handle_null(_: &mut Tx) {
        unreachable!()
    }
}
impl TxDecoder for ObsoleteTag {
    fn decode_column(start: usize, length: i32, acc: &mut Tx) {
        // ObsoleteTag tryte offset is 2295..2322
        acc.buffer().copy_within(start..(start + length as usize), 2295)
    }
    fn handle_null(_: &mut Tx) {
        unreachable!()
    }
}
impl TxDecoder for Timestamp {
    fn decode_column(start: usize, length: i32, acc: &mut Tx) {
        // Timestamp tryte offset is 2322..2331
        acc.buffer().copy_within(start..(start + length as usize), 2322);
    }
    fn handle_null(_: &mut Tx) {
        unreachable!()
    }
}
impl TxDecoder for CurrentIndex {
    fn decode_column(start: usize, length: i32, acc: &mut Tx) {
        // CurrentIndex tryte offset is 2331..2340
        acc.buffer().copy_within(start..(start + length as usize), 2331);
    }
    fn handle_null(_: &mut Tx) {
        unreachable!()
    }
}
impl TxDecoder for LastIndex {
    fn decode_column(start: usize, length: i32, acc: &mut Tx) {
        // LastIndex tryte offset is 2340..2349
        acc.buffer().copy_within(start..(start + length as usize), 2340);
    }
    fn handle_null(_: &mut Tx) {
        unreachable!()
    }
}
impl TxDecoder for Bundle {
    fn decode_column(start: usize, length: i32, acc: &mut Tx) {
        // Bundle tryte offset is 2349..2430
        acc.buffer().copy_within(start..(start + length as usize), 2349)
    }
    fn handle_null(_: &mut Tx) {
        unreachable!()
    }
}
impl TxDecoder for Trunk {
    fn decode_column(start: usize, length: i32, acc: &mut Tx) {
        // Trunk tryte offset is 2430..2511
        acc.buffer().copy_within(start..(start + length as usize), 2430)
    }
    fn handle_null(_: &mut Tx) {
        unreachable!()
    }
}
impl TxDecoder for Branch {
    fn decode_column(start: usize, length: i32, acc: &mut Tx) {
        // Branch tryte offset is 2511..2592
        acc.buffer().copy_within(start..(start + length as usize), 2511)
    }
    fn handle_null(_: &mut Tx) {
        unreachable!()
    }
}
impl TxDecoder for Tag {
    fn decode_column(start: usize, length: i32, acc: &mut Tx) {
        // Tag tryte offset is 2592..2619
        acc.buffer().copy_within(start..(start + length as usize), 2592)
    }
    fn handle_null(_: &mut Tx) {
        unreachable!()
    }
}
impl TxDecoder for AttachmentTimestamp {
    fn decode_column(start: usize, length: i32, acc: &mut Tx) {
        // AttachmentTimestamp tryte offset is 2619..2628
        acc.buffer().copy_within(start..(start + length as usize), 2619);
    }
    fn handle_null(_: &mut Tx) {
        unreachable!()
    }
}
impl TxDecoder for AttachmentTimestampLower {
    fn decode_column(start: usize, length: i32, acc: &mut Tx) {
        // AttachmentTimestampLower tryte offset is 2628..2637
        acc.buffer().copy_within(start..(start + length as usize), 2628);
    }
    fn handle_null(_: &mut Tx) {
        unreachable!()
    }
}
impl TxDecoder for AttachmentTimestampUpper {
    fn decode_column(start: usize, length: i32, acc: &mut Tx) {
        // AttachmentTimestampUpper tryte offset is 2637..2646
        acc.buffer().copy_within(start..(start + length as usize), 2637);
    }
    fn handle_null(_: &mut Tx) {
        unreachable!()
    }
}
impl TxDecoder for Nonce {
    fn decode_column(start: usize, length: i32, acc: &mut Tx) {
        // Tag tryte offset is 2646..2673
        acc.buffer().copy_within(start..(start + length as usize), 2646)
    }
    fn handle_null(_: &mut Tx) {
        unreachable!()
    }
}
impl TxDecoder for Milestone {
    fn decode_column(start: usize, length: i32, acc: &mut Tx) {
        acc.milestone = Some(i64::from_be_bytes(acc.buffer()[start..(start + length as usize)].try_into().unwrap()) as u32);
    }
    fn handle_null(_: &mut Tx) {
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
