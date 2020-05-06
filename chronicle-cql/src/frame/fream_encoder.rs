use super::types::{
    Flags,
    Opcode,
    Version,
};
type Short = u16; // A 2 bytes unsigned integer

// https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec
//
// CQL Frame Structure
//
// 0         8        16        24        32         40
// +---------+---------+---------+---------+---------+
// | version |  flags  |      stream       | opcode  |
// +---------+---------+---------+---------+---------+
// |                length                 |
// +---------+---------+---------+---------+
// |                                       |
// .            ...  body ...              .
// .                                       .
// .                                       .
// +----------------------------------------

pub struct Frame {
    pub version: Version,
    pub flags: Flags,
    pub opcode: Opcode,
    pub stream: Short,
    pub body: Vec<u8>,
}

pub trait FrameEncoder {
    fn create_startup_frame(&self) -> Vec<u8>;
    fn create_auth_response_frame(&self) -> Vec<u8>;
    fn create_options_frame(&self) -> Vec<u8>;
    fn create_query_frame(&self) -> Vec<u8>;
    fn create_prepare_frame(&self) -> Vec<u8>;
    fn create_execute_frame(&self) -> Vec<u8>;
    fn create_batch_frame(&self) -> Vec<u8>;
    fn create_register_frame(&self) -> Vec<u8>;
}

impl FrameEncoder for Frame {
    fn create_startup_frame(&self) -> Vec<u8> {
        todo!()
    }

    fn create_auth_response_frame(&self) -> Vec<u8> {
        todo!()
    }
    fn create_options_frame(&self) -> Vec<u8> {
        todo!()
    }
    fn create_query_frame(&self) -> Vec<u8> {
        todo!()
    }
    fn create_prepare_frame(&self) -> Vec<u8> {
        todo!()
    }
    fn create_execute_frame(&self) -> Vec<u8> {
        todo!()
    }
    fn create_batch_frame(&self) -> Vec<u8> {
        todo!()
    }
    fn create_register_frame(&self) -> Vec<u8> {
        todo!()
    }
}
