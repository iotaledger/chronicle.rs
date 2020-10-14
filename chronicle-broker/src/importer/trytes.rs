// TODO compute token to enable shard_awareness.
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

//! This module defines the `Trytes` strcture which is used by the MQTT mod and importer mod.

/// The Trytes structure.
pub struct Trytes<'a>(&'a str);

impl<'a> Trytes<'a> {
    /// Create a Tryte structure.
    pub fn new(trytes: &'a str) -> Self {
        Trytes(trytes)
    }
    /// Get the trytes string.
    pub fn trytes(&self) -> &str {
        &self.0
    }
    /// Get the `payload` string.
    pub fn payload(&self) -> &str {
        &self.0[..2187]
    }
    /// Get the `address` string.
    pub fn address(&self) -> &str {
        &self.0[2187..2268]
    }
    /// Get the `value` string.
    pub fn value(&self) -> &str {
        &self.0[2268..2295]
    }
    /// Get the `obsolete tag` string.
    pub fn obsolete_tag(&self) -> &str {
        &self.0[2295..2322]
    }
    /// Get the `timestamp` string.
    pub fn timestamp(&self) -> &str {
        &self.0[2322..2331]
    }
    /// Get the `current index` string.
    pub fn current_index(&self) -> &str {
        &self.0[2331..2340]
    }
    /// Get the `last index` string.
    pub fn last_index(&self) -> &str {
        &self.0[2340..2349]
    }
    /// Get the `bundle` string.
    pub fn bundle(&self) -> &str {
        &self.0[2349..2430]
    }
    /// Get the `trunk transaction` string.
    pub fn trunk(&self) -> &str {
        &self.0[2430..2511]
    }
    /// Get the `branch transaction` string.
    pub fn branch(&self) -> &str {
        &self.0[2511..2592]
    }
    /// Get the `tag` string.
    pub fn tag(&self) -> &str {
        &self.0[2592..2619]
    }
    /// Get the `attachment timestamp` string.
    pub fn atch_timestamp(&self) -> &str {
        &self.0[2619..2628]
    }
    /// Get the `attachment timestamp lower bound` string.
    pub fn atch_timestamp_lower(&self) -> &str {
        &self.0[2628..2637]
    }
    /// Get the `attachment timestamp upper bound` string.
    pub fn atch_timestamp_upper(&self) -> &str {
        &self.0[2637..2646]
    }
    /// Get the `nonce` string.
    pub fn nonce(&self) -> &str {
        &self.0[2646..2673]
    }
}
use crate::broker::mqtt::MqttMsg;

/// The compatible traits for the received trytes and hashes of MQTT message.
pub trait Compatible {
    /// Get the Trytes.
    fn trytes(&self) -> Trytes;
    /// Get the hash string.
    fn hash(&self) -> &str;
}

impl<'a> From<&'a MqttMsg> for Trytes<'a> {
    fn from(msg: &'a MqttMsg) -> Self {
        let trytes = unsafe { std::mem::transmute::<&[u8], &str>(&msg.msg.payload()[104..2777]) };
        Trytes::new(trytes)
    }
}
