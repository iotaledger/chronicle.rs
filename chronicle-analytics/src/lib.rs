// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0
//! The chronicle-analytics crates provides methods to generate data reports.

#![warn(missing_docs)]
pub mod error;
#[allow(dead_code)]
pub mod helper;
#[allow(dead_code)]
pub mod offline;
#[allow(dead_code)]
pub mod online;
#[allow(dead_code)]
pub mod report;

pub use report::MilestoneRangePath;
