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

//! This module defines the opcode.

#![allow(unused)]
pub const ERROR: u8 = 0x00;
pub const STARTUP: u8 = 0x01;
pub const READY: u8 = 0x02;
pub const AUTHENTICATE: u8 = 0x03;
pub const OPTIONS: u8 = 0x05;
pub const SUPPORTED: u8 = 0x06;
pub const QUERY: u8 = 0x07;
pub const RESULT: u8 = 0x08;
pub const PREPARE: u8 = 0x09;
pub const EXECUTE: u8 = 0x0A;
pub const REGISTER: u8 = 0x0B;
pub const EVENT: u8 = 0x0C;
pub const BATCH: u8 = 0x0D;
pub const AUTH_CHALLENGE: u8 = 0x0E;
pub const AUTH_RESPONSE: u8 = 0x0F;
pub const AUTH_SUCCESS: u8 = 0x10;
