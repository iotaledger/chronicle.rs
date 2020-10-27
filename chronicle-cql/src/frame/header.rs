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

//! This module defines the header trait.

/// The ignore flag.
pub const IGNORE: u8 = 0x00;
/// The compression flag.
pub const COMPRESSION: u8 = 0x01;
/// The tracing flag.
pub const TRACING: u8 = 0x02;
/// The custoem payload flag.
pub const CUSTOM_PAYLOAD: u8 = 0x04;
/// The warning flag.
pub const WARNING: u8 = 0x08;

/// The header trait, which must be implemented by frame structures.
pub trait Header {
    /// Create a new header.
    fn new() -> Self;
    /// The capacity size.
    fn with_capacity(capacity: usize) -> Self;
    /// The frame version.
    fn version(self) -> Self;
    /// The header flags.
    fn flags(self, flags: u8) -> Self;
    /// The header stream.
    fn stream(self, stream: i16) -> Self;
    /// The header opcode.
    fn opcode(self) -> Self;
    /// The length of the header body.
    fn length(self) -> Self;
}
