// work in progress
use std::convert::From;
use std::mem::transmute;
use super::decoder;
use super::consistency::Consistency;
use std::convert::TryInto;

pub struct CqlError {
    pub code: ErrorCodes,
    pub message: String,
    pub additional: Option<Additional>,
}

impl From<&[u8]> for CqlError {
    fn from(slice: &[u8]) -> Self {
        let code = ErrorCodes::from(slice);
        let message = decoder::string(&slice[4..]);
        let additional: Option<Additional>;
        match code {
            ErrorCodes::UnavailableException => {
                additional = Some(
                    Additional::UnavailableException(
                        UnavailableException::from(&slice[(6+message.len()..)])
                    )
                )
            },
            ErrorCodes::WriteTimeout => {
                additional = Some(
                    Additional::WriteTimeout(
                        WriteTimeout::from(&slice[(6+message.len()..)])
                    )
                )
            },
            ErrorCodes::ReadTimeout => {
                additional = Some(
                    Additional::ReadTimeout(
                        ReadTimeout::from(&slice[(6+message.len()..)])
                    )
                )
            },
            ErrorCodes::ReadFailure => {
                additional = Some(
                    Additional::ReadFailure(
                        ReadFailure::from(&slice[(6+message.len()..)])
                    )
                )
            },
            ErrorCodes::FunctionFailure => {
                additional = Some(
                    Additional::FunctionFailure(
                        FunctionFailure::from(&slice[(6+message.len()..)])
                    )
                )
            },
            ErrorCodes::WriteFailure => {
                additional = Some(
                    Additional::WriteFailure(
                        WriteFailure::from(&slice[(6+message.len()..)])
                    )
                )
            },
            ErrorCodes::AlreadyExists => {
                additional = Some(
                    Additional::AlreadyExists(
                        AlreadyExists::from(&slice[(6+message.len()..)])
                    )
                )
            },
            ErrorCodes::Unprepared => {
                additional = Some(
                    Additional::Unprepared(
                        Unprepared::from(&slice[(6+message.len()..)])
                    )
                )
            },
            _ => {
                additional = None;
            }
        }
        CqlError {
            code,
            message,
            additional,
        }
    }
}

// ErrorCodes as consts
pub const SERVER_ERROR: i32 = 0x0000;
pub const PROTOCOL_ERROR: i32 = 0x000A;
pub const AUTHENTICATION_ERROR: i32 = 0x0100;
pub const UNAVAILABLE_EXCEPTION: i32 = 0x1000;
pub const OVERLOADED: i32 = 0x1001;
pub const IS_BOOSTRAPPING: i32 = 0x1002;
pub const TRUNCATE_ERROR: i32 = 0x1003;
pub const WRITE_TIMEOUT: i32 = 0x1100;
pub const READ_TIMEOUT: i32 = 0x1200;
pub const READ_FAILURE: i32 = 0x1300;
pub const FUNCTION_FAILURE: i32 = 0x1400;
pub const WRITE_FAILURE: i32 = 0x1500;
pub const SYNTAX_ERROR: i32 = 0x2000;
pub const UNAUTHORIZED: i32 = 0x2100;
pub const INVALID: i32 = 0x2200;
pub const CONFIGURE_ERROR: i32 = 0x2300;
pub const ALREADY_EXISTS: i32 = 0x2400;
pub const UNPREPARED: i32 = 0x2500;

#[repr(i32)]
pub enum ErrorCodes {
    ServerError = 0x0000,
    ProtocolError = 0x000A,
    AuthenticationError = 0x0100,
    UnavailableException = 0x1000,
    Overloaded = 0x1001,
    IsBoostrapping = 0x1002,
    TruncateError = 0x1003,
    WriteTimeout = 0x1100,
    ReadTimeout = 0x1200,
    ReadFailure = 0x1300,
    FunctionFailure = 0x1400,
    WriteFailure = 0x1500,
    SyntaxError = 0x2000,
    Unauthorized = 0x2100,
    Invalid = 0x2200,
    ConfigureError = 0x2300,
    AlreadyExists = 0x2400,
    Unprepared = 0x2500,
}

pub enum Additional {
    UnavailableException(UnavailableException),
    WriteTimeout(WriteTimeout),
    ReadTimeout(ReadTimeout),
    ReadFailure(ReadFailure),
    FunctionFailure(FunctionFailure),
    WriteFailure(WriteFailure),
    AlreadyExists(AlreadyExists),
    Unprepared(Unprepared),
}

pub struct UnavailableException {
    pub cl: Consistency,
    pub required: i32,
    pub alive: i32,
}
impl From<&[u8]> for UnavailableException {
    fn from(slice: &[u8]) -> Self {
        let cl = Consistency::from(slice);
        let required = i32::from_be_bytes(slice[2..6].try_into().unwrap());
        let alive = i32::from_be_bytes(slice[6..10].try_into().unwrap());
        Self {
            cl,
            required,
            alive,
        }
    }
}
pub struct WriteTimeout {
    pub cl: Consistency,
    pub received: i32,
    pub blockfor: i32,
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

pub struct ReadTimeout {
    pub cl: Consistency,
    pub received: i32,
    pub blockfor: i32,
    pub data_present: u8,
}
impl ReadTimeout {
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

pub struct ReadFailure {
    pub cl: Consistency,
    pub received: i32,
    pub blockfor: i32,
    pub num_failures: i32,
    pub data_present: u8,
}
impl ReadFailure {
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
pub struct FunctionFailure {
    pub keyspace: String,
    pub function: String,
    pub arg_types: Vec<String>,
}

impl From<&[u8]> for FunctionFailure {
    fn from(slice: &[u8]) -> Self {
        let keyspace = decoder::string(slice);
        let function = decoder::string(&slice[2+keyspace.len()..]);
        let arg_types = decoder::string_list(&slice[4+keyspace.len()+function.len()..]);
        Self {
            keyspace,
            function,
            arg_types,
        }
    }
}

pub struct WriteFailure {
    pub cl: Consistency,
    pub received: i32,
    pub blockfor: i32,
    pub num_failures: i32,
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

pub struct AlreadyExists {
    pub ks: String,
    pub table: String,
}

impl From<&[u8]> for AlreadyExists {
    fn from(slice: &[u8]) -> Self {
        let ks = decoder::string(slice);
        let table = decoder::string(slice[2+ks.len()..].try_into().unwrap());
        Self {
            ks,
            table,
        }
    }
}

pub struct Unprepared {
    pub id: String,
}

impl From<&[u8]> for Unprepared {
    fn from(slice: &[u8]) -> Self {
        let id = decoder::string(slice);
        Self {
            id,
        }
    }
}
pub enum WriteType {
    Simple,
    Batch,
    UnloggedBatch,
    Counter,
    BatchLog,
    Cas,
    View,
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
        unsafe {
            transmute(i32::from_be_bytes(slice[0..4].try_into().unwrap()))
        }
    }
}
