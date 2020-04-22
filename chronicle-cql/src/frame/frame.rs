use super::rows::{Flags, ColumnsCount, PagingState, Metadata};
use std::convert::TryInto;

pub trait Frame {
    fn version(&self) -> u8;
    fn flags(&self) -> u8;
    fn stream(&self) -> i16;
    fn opcode(&self) -> u8;
    fn length(&self) -> usize;
    fn body(&self) -> &[u8];
    fn is_rows(&self) -> bool;
    fn rows_flags(&self) -> Flags;
    fn columns_count(&self) -> ColumnsCount;
    fn paging_state(&self, has_more_pages: bool) -> PagingState;
    fn metadata(&self) -> Metadata;
}

impl Frame for Vec<u8> {
    fn version(&self) -> u8 {
        self[0]
    }
    fn flags(&self) -> u8 {
        self[1]
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
    fn body(&self) -> &[u8] {
        &self[9..self.length()]
    }
    fn is_rows(&self) -> bool {
        todo!()
    }
    fn rows_flags(&self) -> Flags {
        // cql rows specs, flags is [int] and protocol is big-endian
        let flags = i32::from_be_bytes(self[13..17].try_into().unwrap());
        Flags::from_i32(flags)
    }
    fn columns_count(&self) -> ColumnsCount {
        // column count located right after flags, therefore
        i32::from_be_bytes(self[17..21].try_into().unwrap())
    }
    fn paging_state(&self, has_more_pages: bool) -> PagingState {
        if has_more_pages {
            // decode PagingState
            let paging_state_len = i32::from_be_bytes(self[21..25].try_into().unwrap());
            if paging_state_len == -1 {
                PagingState::new(None, 29)
            } else {
                let paging_state_end: usize = (25+paging_state_len) as usize;
                PagingState::new(Some((&self[25..paging_state_end]).to_vec()), paging_state_end)
            }
        } else {
            PagingState::new(None, 25)
        }
    }
    fn metadata(&self) -> Metadata {
        let flags = self.rows_flags();
        let columns_count = self.columns_count();
        let paging_state = self.paging_state(flags.has_more_pages());
        Metadata::new(flags,columns_count,paging_state)
    }
}
