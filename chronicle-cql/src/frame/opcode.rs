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
