use super::rows::{Flags, ColumnsCount, PagingState, Metadata};
use super::flags;
use super::opcode;
use super::result;
use crate::compression::Compression;
use std::convert::TryInto;

pub trait Frame {
    fn version(&self) -> u8;
    fn flags(&self) -> &HeaderFlags;
    fn stream(&self) -> i16;
    fn opcode(&self) -> u8;
    fn length(&self) -> usize;
    fn body(&self) -> &[u8];
    fn body_start(&self,padding: usize) -> usize;
    fn is_void(&self) -> bool;
    fn is_rows(&self) -> bool;
    fn rows_flags(&self) -> Flags;
    fn columns_count(&self) -> ColumnsCount;
    fn paging_state(&self, has_more_pages: bool) -> PagingState;
    fn metadata(&self) -> Metadata;
}
pub struct Decoder {
    buffer: Vec<u8>,
    header_flags: HeaderFlags,
}
impl Decoder {
    pub fn new(mut buffer: Vec<u8>, decompressor: impl Compression) -> Self {
        let header_flags = HeaderFlags::new(&mut buffer, decompressor);
        Decoder {
            buffer,
            header_flags,
        }
    }
    pub fn buffer_as_ref(&self) -> &Vec<u8> {
        &self.buffer
    }
    pub fn buffer_as_mut(&mut self) -> &mut Vec<u8> {
        &mut self.buffer
    }
    pub fn into_buffer(self) -> Vec<u8> {
        self.buffer
    }
}

pub struct HeaderFlags {
    compression: bool,
    tracing: Option<[u8;16]>,
    custom_payload: bool,
    warnings: Option<Vec<String>>,
    // this not a flag, but it indicates the body start in the buffer.
    body_start: usize,
}

impl HeaderFlags {
    pub fn new(buffer: &mut Vec<u8>,decompressor: impl Compression) -> Self {
        let mut body_start = 9;
        let flags = buffer[1];
        let compression = flags & flags::COMPRESSION == flags::COMPRESSION;
        if compression {
            decompressor.decompress(buffer);
        }
        let tracing;
        if flags & flags::TRACING == flags::TRACING {
            let mut tracing_id = [0;16];
            tracing_id.copy_from_slice(&buffer[9..25]);
            tracing = Some(tracing_id);
            // add tracing_id length = 16
            body_start += 16;
        } else {
            tracing = None;
        }
        let warnings;
        if flags & flags::WARNING == flags::WARNING {
            let string_list = string_list(&buffer[body_start..]);
            // add all [short] length to the body_start
            body_start += 2*(string_list.len()+1);
            // add the warning length
            for warning in &string_list {
                // add the warning.len to the body_start
                body_start += warning.len();
            }
            warnings = Some(string_list);
        } else {
            warnings = None;
        }
        let custom_payload = flags & flags::CUSTOM_PAYLOAD == flags::CUSTOM_PAYLOAD;
        Self {
            compression,
            tracing,
            warnings,
            custom_payload,
            body_start,
        }
    }
    pub fn compression(&self) -> bool {
        self.compression
    }
    pub fn take_tracing_id(&mut self) -> Option<[u8;16]>{
        self.tracing.take()
    }
    fn take_warnings(&mut self) -> Option<Vec<String>> {
        self.warnings.take()
    }
}

impl Frame for Decoder {
    fn version(&self) -> u8 {
        self.buffer_as_ref()[0]
    }
    fn flags(&self) -> &HeaderFlags {
        &self.header_flags
    }
    fn stream(&self) -> i16 {
        todo!()
    }
    fn opcode(&self) -> u8 {
        self.buffer_as_ref()[4]
    }
    fn length(&self) -> usize {
        i32::from_be_bytes(self.buffer_as_ref()[5..9].try_into().unwrap()) as usize
    }
    fn body(&self) -> &[u8] {
        let body_start = self.header_flags.body_start;
        &self.buffer_as_ref()[body_start..self.length()]
    }
    fn body_start(&self, padding: usize) -> usize {
        self.header_flags.body_start+padding
    }
    fn is_void(&self) -> bool {
        let body_kind = i32::from_be_bytes(
            self.body()[0..4].try_into().unwrap()
        );
        (self.opcode() == opcode::RESULT) && (body_kind == result::VOID)
    }
    fn is_rows(&self) -> bool {
        let body_kind = i32::from_be_bytes(
            self.body()[0..4].try_into().unwrap()
        );
        (self.opcode() == opcode::RESULT) && (body_kind == result::ROWS)
    }
    fn rows_flags(&self) -> Flags {
        // cql rows specs, flags is [int] and protocol is big-endian
        let flags = i32::from_be_bytes(
            self.buffer_as_ref()[self.body_start(4)..self.body_start(8)].try_into().unwrap()
        );
        Flags::from_i32(flags)
    }
    fn columns_count(&self) -> ColumnsCount {
        // column count located right after flags, therefore
        i32::from_be_bytes(
            self.buffer_as_ref()[self.body_start(8)..self.body_start(12)].try_into().unwrap()
        )
    }
    fn paging_state(&self, has_more_pages: bool) -> PagingState {
        let paging_state_bytes_start = self.body_start(12);
        if has_more_pages {
            // decode PagingState
            let paging_state_value_start = paging_state_bytes_start+4;
            let paging_state_len = i32::from_be_bytes(
                self.buffer_as_ref()[paging_state_bytes_start..paging_state_value_start].try_into().unwrap());
            if paging_state_len == -1 {
                PagingState::new(None, paging_state_value_start)
            } else {
                let paging_state_end: usize = paging_state_value_start+(paging_state_len as usize);
                PagingState::new(Some((self.buffer_as_ref()[paging_state_value_start..paging_state_end]).to_vec()), paging_state_end)
            }
        } else {
            PagingState::new(None, paging_state_bytes_start)
        }
    }
    fn metadata(&self) -> Metadata {
        let flags = self.rows_flags();
        let columns_count = self.columns_count();
        let paging_state = self.paging_state(flags.has_more_pages());
        Metadata::new(flags,columns_count,paging_state)
    }
}

// helper types decoder functions
pub fn string_list(buffer: &[u8]) -> Vec<String> {
    let list_len = u16::from_be_bytes(buffer[0..2].try_into().unwrap()) as usize;
    let mut list: Vec<String> = Vec::with_capacity(list_len);
    // current_string_start
    let mut s = 2;
    for _ in 0..list_len {
        // ie first string length is buffer[2..4]
        let string_len = u16::from_be_bytes(buffer[s..(s+2)].try_into().unwrap()) as usize;
        s += 2;
        let e = s + string_len;
        let string = String::from_utf8_lossy(&buffer[s..e]);
        list.push(string.to_string());
        s = e;
    }
    list
}

// helper traits
