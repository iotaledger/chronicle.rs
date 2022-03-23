// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

mod cpt2;
mod shimmer;

use super::Message;
use mongodb::bson::Bson;

impl Into<Bson> for &Message {
    fn into(self) -> Bson {
        match self {
            Message::Chrysalis(m) => cpt2::message_to_bson(m),
            Message::Shimmer(m) => shimmer::message_to_bson(m),
        }
    }
}
