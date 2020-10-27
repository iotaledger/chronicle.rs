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

//! This module implements the structure used in autentication process.

use super::decoder::{string, Decoder, Frame};

/// The `Authenticate` sturcutre with the autenticator name.
pub struct Authenticate {
    /// The autenticator name.
    pub authenticator: String,
}

impl Authenticate {
    /// Create a new autenticator from the frame decoder.
    pub fn new(decoder: &Decoder) -> Self {
        Self::from(decoder.body())
    }
    /// Get the autenticator name.
    pub fn authenticator(&self) -> &str {
        &self.authenticator[..]
    }
}

impl From<&[u8]> for Authenticate {
    fn from(slice: &[u8]) -> Self {
        let authenticator = string(slice);
        Self { authenticator }
    }
}
