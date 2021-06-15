// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0
use super::*;

#[allow(unused)]
pub fn filter_messages(message: &mut Message) -> Option<FilterResponse> {
    message.payload().as_ref().and_then(|payload| match payload {
        Payload::Indexation(i) => {
            if String::from_utf8_lossy(i.index()) == "HORNET Spammer" {
                Some(FilterResponse {
                    keyspace: "chronicle".into(),
                    ttl: None,
                })
            } else {
                None
            }
        }
        _ => None,
    })
}
