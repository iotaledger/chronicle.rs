// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0
use bee_message::prelude::*;
use std::borrow::Cow;

mod address;
mod indexes;

pub struct FilterResponse {
    /// The keyspace in which this message should be stored
    pub keyspace: Cow<'static, str>,
    /// The record's time-to-live
    pub ttl: Option<usize>,
}

#[allow(unused_variables)]
pub fn filter_messages(message: &mut Message) -> Option<FilterResponse> {
    indexes::filter_messages(message)
}
