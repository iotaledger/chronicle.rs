// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

#![warn(missing_docs)]
//! # Chronicle Storage
//! This crate provides the Chronicle storage types

pub mod access;
pub use access::*;

pub use mongodb;
