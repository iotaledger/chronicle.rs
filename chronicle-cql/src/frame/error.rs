// work in progress
use std::convert::From;
use std::mem::transmute;

pub struct CqlError {
    code: ErrorCodes,
    message: String,
    additional: Option<Additional>,
}

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

}

impl From<i32> for ErrorCodes {
    fn from(error_code: i32) -> ErrorCodes {
        unsafe { transmute(error_code) }
    }
}
