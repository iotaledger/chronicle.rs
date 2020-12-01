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

//! This module implements the Options frame.

use super::{encoder::BE_0_BYTES_LEN, header::Header, opcode::OPTIONS};

/// The Options frame structure.
pub struct Options(pub Vec<u8>);

impl Header for Options {
    fn new() -> Self {
        Options(Vec::new())
    }
    fn with_capacity(capacity: usize) -> Self {
        Options(Vec::with_capacity(capacity))
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
        self.0.push(OPTIONS);
        self
    }
    fn length(mut self) -> Self {
        self.0.extend(&BE_0_BYTES_LEN);
        self
    }
}

impl Options {
    /// Build a new Options frame.
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
    fn simple_options_builder_test() {
        let Options(_payload) = Options::new().version().flags(header::IGNORE).stream(0).opcode().length().build(); // build uncompressed
    }
}
