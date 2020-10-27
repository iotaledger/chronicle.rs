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

//! This module implements the Supported frame.

use super::decoder::{string_multimap, Decoder, Frame};
use std::collections::HashMap;

/// The supported frame with options field.
pub struct Supported {
    options: HashMap<String, Vec<String>>,
}

impl Supported {
    /// Create a Supported frame from frame decoder.
    pub fn new(decoder: &Decoder) -> Self {
        let options = string_multimap(decoder.body());
        Self { options }
    }
    /// Get the options in the Supported frame.
    pub fn get_options(&self) -> &HashMap<String, Vec<String>> {
        &self.options
    }
}
