// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0
use bee_message::Message;
use std::borrow::Cow;

pub struct FilterResponse {
    /// The keyspace in which this message should be stored
    pub keyspace: Cow<'static, str>,
    /// The record's time-to-live
    pub ttl: Option<usize>,
}

#[allow(unused_variables)]
pub async fn filter_messages(message: &mut Message) -> FilterResponse {
    todo!()
}
