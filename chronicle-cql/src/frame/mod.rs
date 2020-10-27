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

//! This crate implements decoder/encoder for a Cassandra frame and the associated protocol.
//! See `https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec` for more details.

pub mod auth_challenge;
pub mod auth_response;
pub mod auth_success;
pub mod authenticate;
pub mod batch;
pub mod batchflags;
pub mod consistency;
pub mod decoder;
pub mod encoder;
pub mod error;
pub mod execute;
pub mod header;
mod opcode;
pub mod options;
pub mod prepare;
pub mod query;
pub mod queryflags;
mod result;
pub mod rows;
pub mod startup;
pub mod supported;
