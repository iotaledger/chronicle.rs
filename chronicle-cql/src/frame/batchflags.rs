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

//! This module defines the batch flag.

/// The batch flag indicates that there is no flags.
pub const NOFLAGS: u8 = 0x00;
/// The batch flag indicates whether to use serial consistency.
pub const SERIAL_CONSISTENCY: u8 = 0x10;
/// The batch flag indicates whether to use the default timestamp.
pub const TIMESTAMP: u8 = 0x20;
