use super::hints::Hint;
use crate::api::types::Trytes81;
use chronicle_cql::{
    compression::compression::MyCompression,
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
        },
    },
    rows,
};

// ----------- decoding scope -----------

rows!(
    rows: Hashes {
        hashes: Vec<Trytes81>,
        hints: Vec<Hint>,
        is_hint: bool,
        address: Trytes81
    },
    row: Row(
        Hash,
        Extra
    ),
    column_decoder: AddressesDecoder
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
    fn finalize(self) -> (Vec<Trytes81>, Vec<Hint>) {
        (self.hashes, self.hints)
    }
}
// implementation to decode the columns in order to form the hashes & hints eventually
impl AddressesDecoder for Hash {
    fn decode_column(start: usize, length: i32, acc: &mut Hashes) {
        // check if the current row is a hint by checking the length
        if length == 1 {
            // it means the hash is "0",
            acc.is_hint = true
        } else {
            // decode transaction hash
            let hash = Trytes81::decode(&acc.buffer()[start..], length as usize);
            acc.hashes.push(hash);
        }
    }
    fn handle_null(_: &mut Hashes) {
        unreachable!()
    }
}

impl AddressesDecoder for Extra {
    fn decode_column(start: usize, _length: i32, acc: &mut Hashes) {
        if acc.is_hint {
            // create a hint and push it to hints
            let end = start + 2;
            let year = u16::from_be_bytes(acc.buffer()[start..end].try_into().unwrap());
            let month = acc.buffer()[end];
            let hint = Hint::new_address_hint(acc.address, None, year, month);
            acc.hints.push(hint);
            // reset is_hint to be false for next() rows if any.
            acc.is_hint = false;
        }
    }
    fn handle_null(_: &mut Hashes) {}
}

// ----------- encoding scope -----------

pub fn query(address: &Trytes81) -> Vec<u8> {
    let Query(payload) = Query::new()
        .version()
        .flags(MyCompression::flag())
        .stream(0)
        .opcode()
        .length()
        .statement("SELECT tx, extra FROM tangle.edge WHERE vertex = ? AND kind in ('input','output','hint')")
        .consistency(Consistency::One)
        .query_flags(SKIP_METADATA | VALUES)
        .value_count(1)
        .value(address)
        .build(MyCompression::get());
    payload
}
