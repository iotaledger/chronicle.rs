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

//! The prepare structure that implements worker trait.
//! Please note: preparer is not a real actor instead is a resumption actor
//! (without its own mailbox/channel).
// TODO: Remove the preparer.

use super::{Error, Worker};
use crate::stage::reporter::{Event, Sender};

#[derive(Debug)]
/// The query reference unit stucture.
pub struct QueryRef {}

impl QueryRef {
    fn new() -> Self {
        QueryRef {}
    }
}

#[derive(Debug)]
/// The Preparer has the `QueryRef` field and implements the worker trait.
pub struct Preparer {
    query: QueryRef,
}

impl Worker for Preparer {
    fn send_response(self: Box<Self>, _tx: &Option<Sender>, _giveload: Vec<u8>) {}
    fn send_error(self: Box<Self>, _error: Error) {}
}

/// Try to prepare the payload if the payload is not prepared yet.
pub fn try_prepare(prepare_payload: &[u8], tx: &Option<Sender>, giveload: &[u8]) {
    // check if the giveload is unprepared_error.
    if check_unprepared(giveload) {
        // create preparer
        let preparer = Preparer { query: QueryRef::new() };
        // create event query
        let event = Event::Request {
            payload: prepare_payload.to_vec(),
            worker: Box::new(preparer),
        };
        // send to reporter(self as this function is invoked inside reporter)
        if let Some(tx) = tx {
            tx.send(event).unwrap();
        };
    }
}

fn check_unprepared(giveload: &[u8]) -> bool {
    giveload[4] == 0 && giveload[9..13] == [0, 0, 37, 0] // cql specs
}
