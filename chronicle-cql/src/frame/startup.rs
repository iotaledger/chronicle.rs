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

//! This module implements the Startup frame.

use super::{encoder::BE_0_BYTES_LEN, header::Header, opcode::STARTUP};
use std::collections::HashMap;

/// The Startup frame.
pub struct Startup(pub Vec<u8>);

impl Header for Startup {
    fn new() -> Self {
        Startup(Vec::new())
    }
    fn with_capacity(capacity: usize) -> Self {
        Startup(Vec::with_capacity(capacity))
    }
    fn version(mut self) -> Self {
        self.0.push(4);
        self
    }
    fn flags(mut self, flags: u8) -> Self {
        self.0.push(flags);
        self
    }
    fn stream(mut self, stream: i16) -> Self {
        self.0.extend(&i16::to_be_bytes(stream));
        self
    }
    fn opcode(mut self) -> Self {
        self.0.push(STARTUP);
        self
    }
    fn length(mut self) -> Self {
        self.0.extend(&BE_0_BYTES_LEN);
        self
    }
}

impl Startup {
    /// Update the options in Startup frame.
    pub fn options(mut self, map: &HashMap<String, String>) -> Self {
        self.0.extend(&u16::to_be_bytes(map.keys().len() as u16));
        for (k, v) in map {
            self.0.extend(&u16::to_be_bytes(k.len() as u16));
            self.0.extend(k.bytes());
            self.0.extend(&u16::to_be_bytes(v.len() as u16));
            self.0.extend(v.bytes());
        }
        let body_length = i32::to_be_bytes((self.0.len() as i32) - 9);
        self.0[5..9].copy_from_slice(&body_length);
        self
    }
    /// Build the Startup frame.
    pub fn build(self) -> Self {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::frame::header;

    #[test]
    // note: junk data
    fn simple_startup_builder_test() {
        let mut options = HashMap::new();
        options.insert("CQL_VERSION".to_string(), "3.0.0".to_string());

        let Startup(_payload) = Startup::new()
            .version()
            .flags(header::IGNORE)
            .stream(0)
            .opcode()
            .length()
            .options(&options)
            .build(); // build uncompressed
    }
}
