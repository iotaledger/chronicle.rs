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

//! This module defines the `hints` for query in ScyllaDB, which enables query data with year/month, to
//! avoid returning large data with the same column.

use super::{ResTransactions, VecDeque};
use crate::api::types::{Trytes27, Trytes81};
use chronicle_cql::{
    compression::MyCompression,
    frame::{
        consistency::Consistency,
        decoder::{ColumnDecoder, Decoder, Frame},
        header::Header,
        query::Query,
        queryflags::{PAGE_SIZE, PAGING_STATE, SKIP_METADATA, VALUES},
    },
    rows,
};
use serde::{Deserialize, Serialize};
#[derive(Deserialize, Serialize, Clone)]
/// The year and month in the data model used for the `hints`.
pub struct YearMonth {
    year: u16,
    month: u8,
}

impl YearMonth {
    /// Create a new `YearMonth` structure.
    pub fn new(year: u16, month: u8) -> Self {
        Self { year, month }
    }
    /// Assign the year value.
    fn year(&self) -> u16 {
        self.year
    }
    /// Assign the month value.
    fn month(&self) -> u8 {
        self.month
    }
}

/// The purpose of `Hint` design is to divide a big table into smaller ones, which prevents overflow faliure
/// as well as improves the query efficiency.
///
/// In general database design (Cassandra for example), if too many rows share the same partition key, then
/// overflow may occur. Even if there is no overflow, the query time is long in a big table.
///
/// An example follows. When there are one billion rows share the same `Tag`, we will not use the tag as the
/// partion key directly. Instead, the `Hint` assocated with the `Tag` also constitutes the partition key, which
/// prevents the overflow failure and resists attack vectors (1 billion transactions with the same `Tag`.)
/// Furthermore, dividing a big table into smaller ones reduces the key searching time, which improves the query
/// performance.
#[derive(Deserialize, Serialize, Clone)]
#[serde(untagged)]
pub enum Hint {
    /// The address structure for `Hint` API.
    Address {
        /// The requested 81-tryte address.
        address: Trytes81,
        /// The year and month vector for the requested address.
        timeline: VecDeque<YearMonth>,
        /// The optional paging state of current request in scyllaDB.
        paging_state: Option<Vec<u8>>,
        /// The page size of of current request for scyllaDB.
        page_size: Option<u16>,
    },
    /// The tag structure for `Hint` API.
    Tag {
        /// The requested 27-tryte tag.
        tag: Trytes27,
        /// The year and month vector for the requested address.
        timeline: VecDeque<YearMonth>,
        /// The optional paging state of current request in scyllaDB.
        paging_state: Option<Vec<u8>>,
        /// The page size of of current request for scyllaDB.
        page_size: Option<u16>,
    },
    /// The bundle structure for `Hint` API.
    Bundle {
        /// The requested 81-tryte bundle hash.
        bundle: Trytes81,
        /// The year and month vector for the requested address.
        timeline: VecDeque<YearMonth>,
        /// The optional paging state of current request in scyllaDB.
        paging_state: Option<Vec<u8>>,
        /// The page size of of current request for scyllaDB.
        page_size: Option<u16>,
    },
    /// The approvee structure for `Hint` API.
    Approvee {
        /// The requested 81-tryte approvee.
        approvee: Trytes81,
        /// The year and month vector for the requested address.
        timeline: VecDeque<YearMonth>,
        /// The optional paging state of current request in scyllaDB.
        paging_state: Option<Vec<u8>>,
        /// The page size of of current request for scyllaDB.
        page_size: Option<u16>,
    },
}

impl Hint {
    /// Create a new address hint structure.
    pub fn new_address_hint(address: Trytes81, timeline: VecDeque<YearMonth>) -> Self {
        Self::Address {
            address,
            timeline,
            paging_state: None,
            page_size: None,
        }
    }
    /// Create a new bundle hint structure.
    pub fn new_bundle_hint(bundle: Trytes81, timeline: VecDeque<YearMonth>) -> Self {
        Self::Bundle {
            bundle,
            timeline,
            paging_state: None,
            page_size: None,
        }
    }
    /// Create a new approvee hint structure.
    pub fn new_approvee_hint(approvee: Trytes81, timeline: VecDeque<YearMonth>) -> Self {
        Self::Approvee {
            approvee,
            timeline,
            paging_state: None,
            page_size: None,
        }
    }
    /// Create a new tag hint structure.
    pub fn new_tag_hint(tag: Trytes27, timeline: VecDeque<YearMonth>) -> Self {
        Self::Tag {
            tag,
            timeline,
            paging_state: None,
            page_size: None,
        }
    }
    /// Get the timeline vecture from a given hint strcture.
    pub fn get_mut_timeline(&mut self) -> &mut VecDeque<YearMonth> {
        match self {
            Hint::Address { timeline, .. } => timeline,
            Hint::Tag { timeline, .. } => timeline,
            Hint::Approvee { timeline, .. } => timeline,
            Hint::Bundle { timeline, .. } => timeline,
        }
    }
    /// Update the paging state in the `Hint` structure.
    pub fn replace_paging_state(&mut self, pg_state: Vec<u8>) {
        match self {
            Hint::Address { paging_state, .. } => {
                paging_state.replace(pg_state);
            }
            Hint::Tag { paging_state, .. } => {
                paging_state.replace(pg_state);
            }
            Hint::Approvee { paging_state, .. } => {
                paging_state.replace(pg_state);
            }
            Hint::Bundle { paging_state, .. } => {
                paging_state.replace(pg_state);
            }
        }
    }
}

// ----------- decoding scope -----------

rows!(
    rows: ResTxs {
        hint: Hint,
        res_txs: ResTransactions
    },
    row: Row(
        Timestamp,
        Tx,
        Value,
        Milestone
    ),
    column_decoder: HintsDecoder
);

/// This trait defines the `decode` and `finalize` behavior for the responsed transactions.
pub trait Rows {
    /// Decode each row by `next` method.
    fn decode(self) -> Self;
    /// Create the transactions to response depends on the pagins state.
    fn finalize(self) -> ResTransactions;
}

impl Rows for ResTxs {
    fn decode(mut self) -> Self {
        while let Some(_) = self.next() {}
        self
    }
    fn finalize(mut self) -> ResTransactions {
        // if there is paging_state then we return hint to the user, to be used for further API calls.
        if let Some(paging_state) = self.metadata.take_paging_state() {
            self.hint.replace_paging_state(paging_state);
            // push hint
            self.res_txs.hints.as_mut().unwrap().push(self.hint);
        } else {
            // no paging_state indicates the need to consume(front_pop) from the timeline
            let timeline = self.hint.get_mut_timeline();
            timeline.pop_front();
            // if timeline become empty we consider it consumed and not return it in res_txs result
            if !timeline.is_empty() {
                // hint is not consumed yet, therefore we return it to res_txs
                self.res_txs.hints.as_mut().unwrap().push(self.hint);
            }
        }
        self.res_txs
    }
}

impl HintsDecoder for Timestamp {
    fn decode_column(start: usize, length: i32, acc: &mut ResTxs) {
        // decode timestamp
        let timestamp = u64::decode(&acc.buffer()[start..], length as usize);
        acc.res_txs.timestamps.push(timestamp);
    }
    fn handle_null(_: &mut ResTxs) {
        unreachable!()
    }
}

impl HintsDecoder for Tx {
    fn decode_column(start: usize, length: i32, acc: &mut ResTxs) {
        // decode transaction hash
        let hash = Trytes81::decode(&acc.buffer()[start..], length as usize);
        acc.res_txs.hashes.push(hash);
    }
    fn handle_null(_: &mut ResTxs) {
        unreachable!()
    }
}

impl HintsDecoder for Value {
    fn decode_column(start: usize, length: i32, acc: &mut ResTxs) {
        // decode value
        let value = i64::decode(&acc.buffer()[start..], length as usize);
        acc.res_txs.values.push(value);
    }
    fn handle_null(_: &mut ResTxs) {
        unreachable!()
    }
}

impl HintsDecoder for Milestone {
    fn decode_column(start: usize, length: i32, acc: &mut ResTxs) {
        // decode milestone
        let milestone = u64::decode(&acc.buffer()[start..], length as usize);
        acc.res_txs.milestones.push(Some(milestone));
    }
    fn handle_null(acc: &mut ResTxs) {
        acc.res_txs.milestones.push(None);
    }
}

// ----------- encoding scope -----------
/// Query the ScyllaDB with `Hint`.
pub fn query(hint: &mut Hint) -> Option<Vec<u8>> {
    let mut query_flags = SKIP_METADATA | VALUES | PAGE_SIZE;
    let query = Query::new().version().flags(MyCompression::flag()).stream(0).opcode().length();
    match hint {
        Hint::Address {
            address,
            timeline,
            paging_state,
            page_size,
        } => {
            if paging_state.is_some() {
                query_flags |= PAGING_STATE;
            }
            if let Some(year_month) = timeline.get(0) {
                // timeline still active or new;
                let Query(payload) = query
                    .statement(SELECT_BY_ADDRESS_DATA_QUERY)
                    .consistency(Consistency::One)
                    .query_flags(query_flags)
                    .value_count(3)
                    .value(address)
                    .value(year_month.year())
                    .value(year_month.month())
                    .page_size(page_size.unwrap_or(5000) as i32)
                    .paging_state(&paging_state.take())
                    .build(MyCompression::get());
                Some(payload)
            } else {
                // empty timeline or consumed
                return None;
            }
        }
        Hint::Tag {
            tag,
            timeline,
            paging_state,
            page_size,
        } => {
            if paging_state.is_some() {
                query_flags |= PAGING_STATE;
            }
            if let Some(year_month) = timeline.get(0) {
                // timeline still active or new;
                let Query(payload) = query
                    .statement(SELECT_BY_TAG_DATA_QUERY)
                    .consistency(Consistency::One)
                    .query_flags(query_flags)
                    .value_count(3)
                    .value(tag)
                    .value(year_month.year())
                    .value(year_month.month())
                    .page_size(page_size.unwrap_or(5000) as i32)
                    .paging_state(&paging_state.take())
                    .build(MyCompression::get());
                Some(payload)
            } else {
                // empty timeline or consumed
                return None;
            }
        }
        Hint::Bundle {
            bundle,
            timeline,
            paging_state,
            page_size,
        } => {
            if paging_state.is_some() {
                query_flags |= PAGING_STATE;
            }
            if let Some(year_month) = timeline.get(0) {
                // timeline still active or new;
                let Query(payload) = query
                    .statement(SELECT_BY_BUNDLE_DATA_QUERY)
                    .consistency(Consistency::One)
                    .query_flags(query_flags)
                    .value_count(3)
                    .value(bundle)
                    .value(year_month.year())
                    .value(year_month.month())
                    .page_size(page_size.unwrap_or(5000) as i32)
                    .paging_state(&paging_state.take())
                    .build(MyCompression::get());
                Some(payload)
            } else {
                // empty timeline or consumed
                return None;
            }
        }
        Hint::Approvee {
            approvee,
            timeline,
            paging_state,
            page_size,
        } => {
            if paging_state.is_some() {
                query_flags |= PAGING_STATE;
            }
            if let Some(year_month) = timeline.get(0) {
                // timeline still active or new;
                let Query(payload) = query
                    .statement(SELECT_BY_APPROVEE_DATA_QUERY)
                    .consistency(Consistency::One)
                    .query_flags(query_flags)
                    .value_count(3)
                    .value(approvee) // it might be tag or address
                    .value(year_month.year())
                    .value(year_month.month())
                    .page_size(page_size.unwrap_or(5000) as i32)
                    .paging_state(&paging_state.take())
                    .build(MyCompression::get());
                Some(payload)
            } else {
                // empty timeline or consumed
                return None;
            }
        }
    }
}

//
const SELECT_BY_APPROVEE_DATA_QUERY: &str = {
    #[cfg(feature = "mainnet")]
let cql = "SELECT timestamp, tx, value, milestone FROM mainnet.data WHERE vertex = ? AND year = ? AND month = ? AND kind in ('trunk','branch')";
    #[cfg(feature = "devnet")]
#[cfg(not(feature = "mainnet"))]
#[cfg(not(feature = "comnet"))]
let cql = "SELECT timestamp, tx, value, milestone FROM devnet.data WHERE vertex = ? AND year = ? AND month = ? AND kind in ('trunk','branch')";
    #[cfg(feature = "comnet")]
#[cfg(not(feature = "mainnet"))]
#[cfg(not(feature = "devnet"))]
let cql = "SELECT timestamp, tx, value, milestone FROM comnet.data WHERE vertex = ? AND year = ? AND month = ? AND kind in ('trunk','branch')";
    cql
};
//
const SELECT_BY_BUNDLE_DATA_QUERY: &str = {
    #[cfg(feature = "mainnet")]
    let cql = "SELECT timestamp, tx, value, milestone FROM mainnet.data WHERE vertex = ? AND year = ? AND month = ? AND kind = 'bundle'";
    #[cfg(feature = "devnet")]
    #[cfg(not(feature = "mainnet"))]
    #[cfg(not(feature = "comnet"))]
    let cql = "SELECT timestamp, tx, value, milestone FROM devnet.data WHERE vertex = ? AND year = ? AND month = ? AND kind = 'bundle'";
    #[cfg(feature = "comnet")]
    #[cfg(not(feature = "mainnet"))]
    #[cfg(not(feature = "devnet"))]
    let cql = "SELECT timestamp, tx, value, milestone FROM comnet.data WHERE vertex = ? AND year = ? AND month = ? AND kind = 'bundle'";
    cql
};
//
const SELECT_BY_TAG_DATA_QUERY: &str = {
    #[cfg(feature = "mainnet")]
    let cql = "SELECT timestamp, tx, value, milestone FROM mainnet.data WHERE vertex = ? AND year = ? AND month = ? AND kind = 'tag'";
    #[cfg(feature = "devnet")]
    #[cfg(not(feature = "mainnet"))]
    #[cfg(not(feature = "comnet"))]
    let cql = "SELECT timestamp, tx, value, milestone FROM devnet.data WHERE vertex = ? AND year = ? AND month = ? AND kind = 'tag'";
    #[cfg(feature = "comnet")]
    #[cfg(not(feature = "mainnet"))]
    #[cfg(not(feature = "devnet"))]
    let cql = "SELECT timestamp, tx, value, milestone FROM comnet.data WHERE vertex = ? AND year = ? AND month = ? AND kind = 'tag'";
    cql
};
//
const SELECT_BY_ADDRESS_DATA_QUERY: &str = {
    #[cfg(feature = "mainnet")]
let cql = "SELECT timestamp, tx, value, milestone FROM mainnet.data WHERE vertex = ? AND year = ? AND month = ? AND kind in ('input','output')";
    #[cfg(feature = "devnet")]
#[cfg(not(feature = "mainnet"))]
#[cfg(not(feature = "comnet"))]
let cql = "SELECT timestamp, tx, value, milestone FROM devnet.data WHERE vertex = ? AND year = ? AND month = ? AND kind in ('input','output')";
    #[cfg(feature = "comnet")]
#[cfg(not(feature = "mainnet"))]
#[cfg(not(feature = "devnet"))]
let cql = "SELECT timestamp, tx, value, milestone FROM comnet.data WHERE vertex = ? AND year = ? AND month = ? AND kind in ('input','output')";
    cql
};
//
