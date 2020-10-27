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

//! This module implements the needed structure and traits used for successful autentication.

use super::decoder::{bytes, Decoder, Frame};

/// The structure for successful autentication.
pub struct AuthSuccess {
    token: Option<Vec<u8>>,
}

impl AuthSuccess {
    /// Create a new `AuthSuccess` structure from frame decoder.
    pub fn new(decoder: &Decoder) -> Self {
        Self::from(decoder.body())
    }
    /// Get the autentication token.
    pub fn token(&self) -> Option<&Vec<u8>> {
        self.token.as_ref()
    }
}

impl From<&[u8]> for AuthSuccess {
    fn from(slice: &[u8]) -> Self {
        let token = bytes(slice);
        Self { token }
    }
}
