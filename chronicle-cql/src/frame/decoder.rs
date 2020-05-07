use super::rows::{Flags, ColumnsCount, PagingState, Metadata};
use super::header;
use crate::compression::decompressor::Decompressor;
use std::convert::TryInto;

pub trait Frame {
    fn version(&self) -> u8;
    fn flags(&mut self, decompressor: Option<impl Decompressor>) -> HeaderFlags;
    fn stream(&self) -> i16;
    fn opcode(&self) -> u8;
    fn length(&self) -> usize;
    fn is_rows(&self) -> bool;
    fn rows_flags(&self, header_flags: &HeaderFlags) -> Flags;
    fn columns_count(&self, header_flags: &HeaderFlags) -> ColumnsCount;
    fn paging_state(&self,header_flags: &HeaderFlags, has_more_pages: bool) -> PagingState;
    fn metadata(&self, header_flags: &HeaderFlags) -> Metadata;
}

pub struct HeaderFlags {
    compression: bool,
    tracing: Option<[u8;16]>,
    custom_payload: bool,
    warnings: Option<Vec<String>>,
    // this not a flag, but it indicates the body start in the buffer.
    body_start: usize,
}

impl Frame for Vec<u8> {
    fn version(&self) -> u8 {
        self[0]
    }
    fn flags(&mut self, decompressor: Option<impl Decompressor>) -> HeaderFlags {
        let mut body_start = 9;
        let flags = self[1];
        let compression = flags & header::COMPRESSION == header::COMPRESSION;
        if compression {
            decompressor.as_ref().unwrap().decompress(self);
        }
        let tracing;
        if flags & header::TRACING == header::TRACING {
            let mut tracing_id = [0;16];
            tracing_id.copy_from_slice(&self[9..25]);
            tracing = Some(tracing_id);
            // add tracing_id length = 16
            body_start += 16;
        } else {
            tracing = None;
        }
        let warnings;
        if flags & header::WARNING == header::WARNING {
            let string_list = string_list(&self[body_start..]);
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
        let custom_payload = flags & header::CUSTOM_PAYLOAD == header::CUSTOM_PAYLOAD;
        HeaderFlags {
            compression,
            tracing,
            warnings,
            custom_payload,
            body_start,
        }
    }
    fn stream(&self) -> i16 {
        todo!()
    }
    fn opcode(&self) -> u8 {
        self[4]
    }
    fn length(&self) -> usize {
        todo!()
    }
    fn is_rows(&self) -> bool {
        todo!()
    }
    fn rows_flags(&self, header_flags: &HeaderFlags) -> Flags {
        // cql rows specs, flags is [int] and protocol is big-endian
        let flags = i32::from_be_bytes(
            self[(header_flags.body_start+4)..(header_flags.body_start+8)].try_into().unwrap()
        );
        Flags::from_i32(flags)
    }
    fn columns_count(&self, header_flags: &HeaderFlags) -> ColumnsCount {
        // column count located right after flags, therefore
        i32::from_be_bytes(
            self[(header_flags.body_start+8)..(header_flags.body_start+12)].try_into().unwrap()
        )
    }
    fn paging_state(&self, header_flags: &HeaderFlags, has_more_pages: bool) -> PagingState {
        if has_more_pages {
            // decode PagingState
            let paging_state_bytes_start = header_flags.body_start+12;
            let paging_state_value_start = paging_state_bytes_start+4;
            let paging_state_len = i32::from_be_bytes(
                self[paging_state_bytes_start..paging_state_value_start].try_into().unwrap());
            if paging_state_len == -1 {
                PagingState::new(None, paging_state_value_start)
            } else {
                let paging_state_end: usize = paging_state_value_start+(paging_state_len as usize);
                PagingState::new(Some((&self[paging_state_value_start..paging_state_end]).to_vec()), paging_state_end)
            }
        } else {
            PagingState::new(None, header_flags.body_start+12)
        }
    }
    fn metadata(&self, header_flags: &HeaderFlags) -> Metadata {
        let flags = self.rows_flags(header_flags);
        let columns_count = self.columns_count(header_flags);
        let paging_state = self.paging_state(header_flags, flags.has_more_pages());
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
