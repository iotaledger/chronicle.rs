use crate::api::types::{
    Trytes27,
    Trytes81,
};
use chronicle_cql::{
    compression::MyCompression,
    frame::{
        consistency::Consistency,
        decoder::{
            ColumnDecoder,
            Decoder,
            Frame,
        },
        header::Header,
        query::Query,
        queryflags::{
            PAGE_SIZE,
            PAGING_STATE,
            SKIP_METADATA,
            VALUES,
        },
    },
    rows,
};
use serde::{
    Deserialize,
    Serialize,
};

#[derive(Deserialize, Serialize, Clone)]
#[serde(untagged)]
pub enum Hint {
    Address {
        address: Trytes81,
        year: u16,
        month: u8,
        paging_state: Option<Vec<u8>>,
    },
    Tag {
        tag: Trytes27,
        year: u16,
        month: u8,
        paging_state: Option<Vec<u8>>,
    },
}

impl Hint {
    pub fn new_address_hint(address: Trytes81, paging_state: Option<Vec<u8>>, year: u16, month: u8) -> Self {
        Self::Address {
            address,
            paging_state,
            year,
            month,
        }
    }
    pub fn get_vertex(&self) -> &[u8] {
        match self {
            Self::Address { address, .. } => &address.0,
            Self::Tag { tag, .. } => &tag.0,
        }
    }
    pub fn take_paging_state(&mut self) -> Option<Vec<u8>> {
        match self {
            Self::Address { paging_state, .. } => paging_state.take(),
            Self::Tag { paging_state, .. } => paging_state.take(),
        }
    }
    pub fn replace_paging_state(&mut self, pgs: Vec<u8>) {
        match self {
            Self::Address { paging_state, .. } => {
                paging_state.replace(pgs);
            }
            Self::Tag { paging_state, .. } => {
                paging_state.replace(pgs);
            }
        }
    }
    pub fn is_paging_state_some(&self) -> bool {
        match self {
            Self::Address { paging_state, .. } => paging_state.is_some(),
            Self::Tag { paging_state, .. } => paging_state.is_some(),
        }
    }
    pub fn year(&self) -> u16 {
        match self {
            Self::Address { year, .. } => *year,
            Self::Tag { year, .. } => *year,
        }
    }
    pub fn month(&self) -> u8 {
        match self {
            Self::Address { month, .. } => *month,
            Self::Tag { month, .. } => *month,
        }
    }
}

// ----------- decoding scope -----------

rows!(
    rows: Hashes {
        hashes: Vec<Trytes81>,
        hints: Vec<Hint>,
        hint: Hint
    },
    row: Row(
        Tx
    ),
    column_decoder: HintsDecoder
);

pub trait Rows {
    fn decode(self) -> Self;
    fn finalize(self) -> (Vec<Trytes81>, Vec<Hint>);
}

impl Rows for Hashes {
    fn decode(mut self) -> Self {
        while let Some(_) = self.next() {}
        self
    }
    fn finalize(mut self) -> (Vec<Trytes81>, Vec<Hint>) {
        // if there is paging_state then we return hint to the user, to used for further API calls
        if let Some(paging_state) = self.metadata.take_paging_state() {
            self.hint.replace_paging_state(paging_state);
            self.hints.push(self.hint);
        }
        (self.hashes, self.hints)
    }
}

impl HintsDecoder for Tx {
    fn decode_column(start: usize, length: i32, acc: &mut Hashes) {
        // decode transaction hash
        let hash = Trytes81::decode(&acc.buffer()[start..], length as usize);
        acc.hashes.push(hash);
    }
    fn handle_null(_: &mut Hashes) {
        unreachable!()
    }
}

// ----------- encoding scope -----------

pub fn query(hint: &mut Hint) -> Vec<u8> {
    let mut query_flags = SKIP_METADATA | VALUES | PAGE_SIZE;
    if hint.is_paging_state_some() {
        query_flags |= PAGING_STATE;
    }
    let Query(payload) = Query::new()
        .version()
        .flags(MyCompression::flag())
        .stream(0)
        .opcode()
        .length()
        .statement(
            "SELECT tx FROM tangle.data WHERE vertex = ? AND year = ? AND month = ? AND kind in ('address','tag')",
        )
        .consistency(Consistency::One)
        .query_flags(query_flags)
        .value_count(3)
        .value(hint.get_vertex()) // it might be tag or address
        .value(hint.year())
        .value(hint.month())
        .page_size(255 as i32)
        .paging_state(&hint.take_paging_state())
        .build(MyCompression::get());
    payload
}
