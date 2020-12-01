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

//! This module implements the cql error decoder.

use super::{
    consistency::Consistency,
    decoder::{self, Decoder, Frame},
};
use std::{
    convert::{From, TryInto},
    mem::transmute,
};

#[derive(Debug)]
/// The CQL error structure.
pub struct CqlError {
    /// The Error code.
    pub code: ErrorCodes,
    /// The message string.
    pub message: String,
    /// The additional Error information.
    pub additional: Option<Additional>,
}

impl CqlError {
    /// Get the CQL error from the frame decoder.
    pub fn new(decoder: &Decoder) -> Self {
        Self::from(decoder.body())
    }
}

impl From<&[u8]> for CqlError {
    fn from(slice: &[u8]) -> Self {
        let code = ErrorCodes::from(slice);
        let message = decoder::string(&slice[4..]);
        let additional: Option<Additional>;
        match code {
            ErrorCodes::UnavailableException => {
                additional = Some(Additional::UnavailableException(UnavailableException::from(
                    &slice[(6 + message.len()..)],
                )))
            }
            ErrorCodes::WriteTimeout => additional = Some(Additional::WriteTimeout(WriteTimeout::from(&slice[(6 + message.len()..)]))),
            ErrorCodes::ReadTimeout => additional = Some(Additional::ReadTimeout(ReadTimeout::from(&slice[(6 + message.len()..)]))),
            ErrorCodes::ReadFailure => additional = Some(Additional::ReadFailure(ReadFailure::from(&slice[(6 + message.len()..)]))),
            ErrorCodes::FunctionFailure => {
                additional = Some(Additional::FunctionFailure(FunctionFailure::from(&slice[(6 + message.len()..)])))
            }
            ErrorCodes::WriteFailure => additional = Some(Additional::WriteFailure(WriteFailure::from(&slice[(6 + message.len()..)]))),
            ErrorCodes::AlreadyExists => additional = Some(Additional::AlreadyExists(AlreadyExists::from(&slice[(6 + message.len()..)]))),
            ErrorCodes::Unprepared => additional = Some(Additional::Unprepared(Unprepared::from(&slice[(6 + message.len()..)]))),
            _ => {
                additional = None;
            }
        }
        CqlError { code, message, additional }
    }
}

// ErrorCodes as consts
/// The Error code of `SERVER_ERROR`.
pub const SERVER_ERROR: i32 = 0x0000;
/// The Error code of `PROTOCOL_ERROR`.
pub const PROTOCOL_ERROR: i32 = 0x000A;
/// The Error code of `AUTHENTICATION_ERROR`.
pub const AUTHENTICATION_ERROR: i32 = 0x0100;
/// The Error code of `UNAVAILABLE_EXCEPTION`.
pub const UNAVAILABLE_EXCEPTION: i32 = 0x1000;
/// The Error code of `OVERLOADED`.
pub const OVERLOADED: i32 = 0x1001;
/// The Error code of `IS_BOOSTRAPPING`.
pub const IS_BOOSTRAPPING: i32 = 0x1002;
/// The Error code of `TRUNCATE_ERROR`.
pub const TRUNCATE_ERROR: i32 = 0x1003;
/// The Error code of `WRITE_TIMEOUT`.
pub const WRITE_TIMEOUT: i32 = 0x1100;
/// The Error code of `READ_TIMEOUT`.
pub const READ_TIMEOUT: i32 = 0x1200;
/// The Error code of `READ_FAILURE`.
pub const READ_FAILURE: i32 = 0x1300;
/// The Error code of `FUNCTION_FAILURE`.
pub const FUNCTION_FAILURE: i32 = 0x1400;
/// The Error code of `WRITE_FAILURE`.
pub const WRITE_FAILURE: i32 = 0x1500;
/// The Error code of `SYNTAX_ERROR`.
pub const SYNTAX_ERROR: i32 = 0x2000;
/// The Error code of `UNAUTHORIZED`.
pub const UNAUTHORIZED: i32 = 0x2100;
/// The Error code of `INVALID`.
pub const INVALID: i32 = 0x2200;
/// The Error code of `CONFIGURE_ERROR`.
pub const CONFIGURE_ERROR: i32 = 0x2300;
/// The Error code of `ALREADY_EXISTS`.
pub const ALREADY_EXISTS: i32 = 0x2400;
/// The Error code of `UNPREPARED`.
pub const UNPREPARED: i32 = 0x2500;
#[derive(Debug)]
#[repr(i32)]
/// The Error code enum.
pub enum ErrorCodes {
    /// The Error code is `SERVER_ERROR`.
    ServerError = 0x0000,
    /// The Error code is `PROTOCOL_ERROR`.
    ProtocolError = 0x000A,
    /// The Error code is `AUTHENTICATION_ERROR`.
    AuthenticationError = 0x0100,
    /// The Error code is `UNAVAILABLE_EXCEPTION`.
    UnavailableException = 0x1000,
    /// The Error code is `OVERLOADED`.
    Overloaded = 0x1001,
    /// The Error code is `IS_BOOSTRAPPING`.
    IsBoostrapping = 0x1002,
    /// The Error code is `TRUNCATE_ERROR`.
    TruncateError = 0x1003,
    /// The Error code is `WRITE_TIMEOUT`.
    WriteTimeout = 0x1100,
    /// The Error code is `READ_TIMEOUT`.
    ReadTimeout = 0x1200,
    /// The Error code is `READ_FAILURE`.
    ReadFailure = 0x1300,
    /// The Error code is `FUNCTION_FAILURE`.
    FunctionFailure = 0x1400,
    /// The Error code is `WRITE_FAILURE`.
    WriteFailure = 0x1500,
    /// The Error code is `SYNTAX_ERROR`.
    SyntaxError = 0x2000,
    /// The Error code is `UNAUTHORIZED`.
    Unauthorized = 0x2100,
    /// The Error code is `INVALID`.
    Invalid = 0x2200,
    /// The Error code is `CONFIGURE_ERROR`.
    ConfigureError = 0x2300,
    /// The Error code is `ALREADY_EXISTS`.
    AlreadyExists = 0x2400,
    /// The Error code is `UNPREPARED`.
    Unprepared = 0x2500,
}
#[derive(Debug)]
/// The additional error information enum.
pub enum Additional {
    /// The additional error information is `UnavailableException`.
    UnavailableException(UnavailableException),
    /// The additional error information is `WriteTimeout`.
    WriteTimeout(WriteTimeout),
    /// The additional error information is `ReadTimeout`.
    ReadTimeout(ReadTimeout),
    /// The additional error information is `ReadFailure`.
    ReadFailure(ReadFailure),
    /// The additional error information is `FunctionFailure`.
    FunctionFailure(FunctionFailure),
    /// The additional error information is `WriteFailure`.
    WriteFailure(WriteFailure),
    /// The additional error information is `AlreadyExists`.
    AlreadyExists(AlreadyExists),
    /// The additional error information is `Unprepared`.
    Unprepared(Unprepared),
}
#[derive(Debug)]
/// The unavailable exception structure.
pub struct UnavailableException {
    /// The consistency level.
    pub cl: Consistency,
    /// The number of nodes that should be alive to respect the consistency levels.
    pub required: i32,
    /// The number of replicas that were known to be alive when the request had been processed.
    pub alive: i32,
}
impl From<&[u8]> for UnavailableException {
    fn from(slice: &[u8]) -> Self {
        let cl = Consistency::from(slice);
        let required = i32::from_be_bytes(slice[2..6].try_into().unwrap());
        let alive = i32::from_be_bytes(slice[6..10].try_into().unwrap());
        Self { cl, required, alive }
    }
}
#[derive(Debug)]
/// The addtional error information, `WriteTimeout`, stucture.
pub struct WriteTimeout {
    /// The consistency level of the query having triggered the exception.
    pub cl: Consistency,
    /// Representing the number of nodes having acknowledged the request.
    pub received: i32,
    /// Representing the number of replicas whose acknowledgement is required to achieve `cl`.
    pub blockfor: i32,
    /// That describe the type of the write that timed out.
    pub writetype: WriteType,
}
impl From<&[u8]> for WriteTimeout {
    fn from(slice: &[u8]) -> Self {
        let cl = Consistency::from(slice);
        let received = i32::from_be_bytes(slice[2..6].try_into().unwrap());
        let blockfor = i32::from_be_bytes(slice[6..10].try_into().unwrap());
        let writetype = WriteType::from(&slice[10..]);
        Self {
            cl,
            received,
            blockfor,
            writetype,
        }
    }
}
#[derive(Debug)]
/// The addtional error information, `ReadTimeout`, stucture.
pub struct ReadTimeout {
    /// The consistency level of the query having triggered the exception.
    pub cl: Consistency,
    /// Representing the number of nodes having answered the request.
    pub received: i32,
    /// Representing the number of replicas whose response is required to achieve `cl`.
    pub blockfor: i32,
    /// If its value is 0, it means the replica that was asked for data has not responded.
    /// Otherwise, the value is != 0.
    pub data_present: u8,
}
impl ReadTimeout {
    /// Check whether the the replica that was asked for data had not responded.
    pub fn replica_had_not_responded(&self) -> bool {
        self.data_present == 0
    }
}
impl From<&[u8]> for ReadTimeout {
    fn from(slice: &[u8]) -> Self {
        let cl = Consistency::from(slice);
        let received = i32::from_be_bytes(slice[2..6].try_into().unwrap());
        let blockfor = i32::from_be_bytes(slice[6..10].try_into().unwrap());
        let data_present = slice[10];
        Self {
            cl,
            received,
            blockfor,
            data_present,
        }
    }
}
#[derive(Debug)]
/// The addtional error information, `ReadFailure`, stucture.
pub struct ReadFailure {
    /// The consistency level of the query having triggered the exception.
    pub cl: Consistency,
    /// Representing the number of nodes having answered the request.
    pub received: i32,
    /// Representing the number of replicas whose acknowledgement is required to
    /// achieve <cl>.
    pub blockfor: i32,
    /// The number of nodes that experience a failure while executing the request.
    pub num_failures: i32,
    /// If its value is 0, it means the replica that was asked for data had not
    /// responded. Otherwise, the value is != 0.
    pub data_present: u8,
}
impl ReadFailure {
    /// Check whether the the replica that was asked for data had not responded.
    pub fn replica_had_not_responded(&self) -> bool {
        self.data_present == 0
    }
}
impl From<&[u8]> for ReadFailure {
    fn from(slice: &[u8]) -> Self {
        let cl = Consistency::from(slice);
        let received = i32::from_be_bytes(slice[2..6].try_into().unwrap());
        let blockfor = i32::from_be_bytes(slice[6..10].try_into().unwrap());
        let num_failures = i32::from_be_bytes(slice[10..14].try_into().unwrap());
        let data_present = slice[14];
        Self {
            cl,
            received,
            blockfor,
            num_failures,
            data_present,
        }
    }
}
#[derive(Debug)]
/// The addtional error information, `FunctionFailure`, stucture.
pub struct FunctionFailure {
    /// The keyspace of the failed function.
    pub keyspace: String,
    /// The name of the failed function.
    pub function: String,
    /// One string for each argument type (as CQL type) of the failed function.
    pub arg_types: Vec<String>,
}

impl From<&[u8]> for FunctionFailure {
    fn from(slice: &[u8]) -> Self {
        let keyspace = decoder::string(slice);
        let function = decoder::string(&slice[2 + keyspace.len()..]);
        let arg_types = decoder::string_list(&slice[4 + keyspace.len() + function.len()..]);
        Self {
            keyspace,
            function,
            arg_types,
        }
    }
}
#[derive(Debug)]
/// The addtional error information, `WriteFailure`, stucture.
pub struct WriteFailure {
    /// The consistency level of the query having triggered the exception.
    pub cl: Consistency,
    /// Representing the number of nodes having answered the request.
    pub received: i32,
    /// Representing the number of replicas whose acknowledgement is required to achieve `cl`.
    pub blockfor: i32,
    /// Representing the number of nodes that experience a failure while executing the request.
    pub num_failures: i32,
    /// Describes the type of the write that timed out.
    pub writetype: WriteType,
}

impl From<&[u8]> for WriteFailure {
    fn from(slice: &[u8]) -> Self {
        let cl = Consistency::from(slice);
        let received = i32::from_be_bytes(slice[2..6].try_into().unwrap());
        let blockfor = i32::from_be_bytes(slice[6..10].try_into().unwrap());
        let num_failures = i32::from_be_bytes(slice[10..14].try_into().unwrap());
        let writetype = WriteType::from(&slice[14..]);
        Self {
            cl,
            received,
            blockfor,
            num_failures,
            writetype,
        }
    }
}
#[derive(Debug)]
/// The addtional error information, `AlreadyExists`, stucture.
pub struct AlreadyExists {
    /// Representing either the keyspace that already exists, or the keyspace in which the table that
    /// already exists is.
    pub ks: String,
    /// Representing the name of the table that already exists. If the query was attempting to create a
    /// keyspace, <table> will be present but will be the empty string.
    pub table: String,
}

impl From<&[u8]> for AlreadyExists {
    fn from(slice: &[u8]) -> Self {
        let ks = decoder::string(slice);
        let table = decoder::string(slice[2 + ks.len()..].try_into().unwrap());
        Self { ks, table }
    }
}
#[derive(Debug)]
/// The addtional error information, `Unprepared`, stucture.
pub struct Unprepared {
    /// The unprepared id.
    pub id: String,
}

impl From<&[u8]> for Unprepared {
    fn from(slice: &[u8]) -> Self {
        let id = decoder::string(slice);
        Self { id }
    }
}
#[derive(Debug)]
/// The type of the write that timed out.
pub enum WriteType {
    /// Simple write type.
    Simple,
    /// Batch write type.
    Batch,
    /// UnloggedBatch write type.
    UnloggedBatch,
    /// Counter write type.
    Counter,
    /// BatchLog write type.
    BatchLog,
    /// Cas write type.
    Cas,
    /// View write type.
    View,
    /// Cdc write type.
    Cdc,
}

impl From<&[u8]> for WriteType {
    fn from(slice: &[u8]) -> Self {
        match decoder::str(slice) {
            "SIMPLE" => WriteType::Simple,
            "BATCH" => WriteType::Batch,
            "UNLOGGED_BATCH" => WriteType::UnloggedBatch,
            "COUNTER" => WriteType::Counter,
            "BATCH_LOG" => WriteType::BatchLog,
            "CAS" => WriteType::Cas,
            "VIEW" => WriteType::View,
            "CDC" => WriteType::Cdc,
            _ => {
                panic!("unexpected writetype error");
            }
        }
    }
}

impl From<&[u8]> for ErrorCodes {
    fn from(slice: &[u8]) -> ErrorCodes {
        unsafe { transmute(i32::from_be_bytes(slice[0..4].try_into().unwrap())) }
    }
}
