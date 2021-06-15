// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0
use super::*;

#[allow(unused)]
pub fn filter_messages(message: &mut Message) -> Option<FilterResponse> {
    message.payload().as_ref().and_then(|payload| match payload {
        Payload::Transaction(t) => match t.essence() {
            Essence::Regular(r) => {
                for output in r.outputs() {
                    if let Some(res) = match output {
                        Output::SignatureLockedSingle(s) => Some(s.address()),
                        Output::SignatureLockedDustAllowance(s) => Some(s.address()),
                        _ => None,
                    }
                    .and_then(|address| match address {
                        // TODO: Load desired addresses / keyspaces from file
                        Address::Ed25519(a) => {
                            if a.to_string() == "0d8aa7e036934de3a90f69aab746e22f1ef4f8466537fc4fac700168e38f3d6d" {
                                Some(FilterResponse {
                                    keyspace: "permanode".into(),
                                    ttl: None,
                                })
                            } else {
                                Some(FilterResponse {
                                    keyspace: "chronicle".into(),
                                    ttl: None,
                                })
                            }
                        }
                    }) {
                        return Some(res);
                    }
                }
                None
            }
        },
        _ => None,
    })
}
