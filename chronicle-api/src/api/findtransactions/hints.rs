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
            SKIP_METADATA,
            VALUES,
            PAGE_SIZE,
            PAGING_STATE,
        },
    },
    rows,
};
use serde::{
    Deserialize,
    Serialize,
};

#[derive(Deserialize, Serialize, Clone)]
pub struct Hint {
    address: Option<Trytes81>,
    tag: Option<Trytes27>,
    paging_state: Option<Vec<u8>>,
    year: u16,
    month: u8,
}

impl Hint {
    pub fn new_address_hint(address: Trytes81, paging_state: Option<Vec<u8>>, year: u16, month: u8) -> Self {
        Self {
            address: Some(address),
            tag: None,
            paging_state,
            year,
            month,
        }
    }
    pub fn get_vertex(&self) -> &[u8] {
        if self.address.is_some() {
            &self.address.as_ref().unwrap().0
        } else if self.tag.is_some() {
            &self.tag.as_ref().unwrap().0
        } else {
            unreachable!("get_vertex in hint struct")
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
            self.hint.paging_state.replace(paging_state);
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
    if hint.paging_state.is_some() {
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
        .value(hint.year)
        .value(hint.month)
        .page_size(255 as i32)
        .paging_state(&hint.paging_state.take())
        .build(MyCompression::get());
    payload
}
