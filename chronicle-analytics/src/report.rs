// Copyright 2022 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0
//! This module defines the report data structures.

use chronicle_storage::access::Address;

use chrono::NaiveDate;
use serde::Serialize;
use std::collections::{
    HashMap,
    HashSet,
};

/// The type for (milestone begin, milestone end, path).
pub type MilestoneRangePath = (u32, u32, std::path::PathBuf);

/// The report data of a given period.
#[derive(Clone, Debug, Default)]
pub struct ReportData {
    /// The address set of receiving and sending tokens.
    pub total_addresses: HashSet<Address>,
    /// The address set of receiving tokens.
    pub recv_addresses: HashSet<Address>,
    /// The address set of sending tokens.
    pub send_addresses: HashSet<Address>,
    /// The outputs map.
    pub outputs: HashMap<Address, usize>,
    /// The message count.
    pub message_count: u64,
    /// The included transaction count.
    pub included_transaction_count: u64,
    /// The conflicting transaction count.
    pub conflicting_transaction_count: u64,
    /// The total transaction count.
    pub total_transaction_count: u64,
    /// The total transferred tokens.
    pub transferred_tokens: u128,
}

/// The report format for each row.
#[derive(Clone, Debug, Serialize)]
pub struct ReportRow {
    /// The data of the report.
    pub date: NaiveDate,
    /// The total address count.
    pub total_addresses: usize,
    /// The receiving address count.
    pub recv_addresses: usize,
    /// The sending address count.
    pub send_addresses: usize,
    /// The average output count of each address.
    pub avg_outputs: f32,
    /// The maximum output count of an address.
    pub max_outputs: usize,
    /// Tht total message count.
    pub message_count: u64,
    /// The included transaction count.
    pub included_transaction_count: u64,
    /// The conflicting transaction count.
    pub conflicting_transaction_count: u64,
    /// The total transaction count.
    pub total_transaction_count: u64,
    /// The total number of transferred tokens.
    pub transferred_tokens: u128,
}

impl ReportData {
    /// Merge different report data.
    pub fn merge(&mut self, other: Self) {
        self.total_addresses.extend(other.total_addresses);
        self.recv_addresses.extend(other.recv_addresses);
        self.send_addresses.extend(other.send_addresses);
        for (addr, count) in other.outputs {
            *self.outputs.entry(addr).or_default() += count;
        }
        self.message_count += other.message_count;
        self.included_transaction_count += other.included_transaction_count;
        self.conflicting_transaction_count += other.conflicting_transaction_count;
        self.total_transaction_count += other.total_transaction_count;
        self.transferred_tokens += other.transferred_tokens;
    }
}

impl From<(NaiveDate, ReportData)> for ReportRow {
    /// Converse the report data of the date to a report row.
    fn from((t, d): (NaiveDate, ReportData)) -> Self {
        Self {
            date: t,
            total_addresses: d.total_addresses.len(),
            recv_addresses: d.recv_addresses.len(),
            send_addresses: d.send_addresses.len(),
            avg_outputs: d.outputs.values().sum::<usize>() as f32 / d.outputs.len() as f32,
            max_outputs: *d.outputs.values().max().unwrap_or(&0),
            message_count: d.message_count,
            included_transaction_count: d.included_transaction_count,
            conflicting_transaction_count: d.conflicting_transaction_count,
            total_transaction_count: d.total_transaction_count,
            transferred_tokens: d.transferred_tokens,
        }
    }
}
