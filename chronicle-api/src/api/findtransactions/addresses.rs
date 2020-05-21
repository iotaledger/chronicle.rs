use chronicle_cql::{
    frame::decoder::{
        Decoder,
        ColumnDecoder,
        Frame,
    },
    frame::query::Query,
    frame::header::{self, Header},
    frame::consistency::Consistency,
    frame::queryflags::{SKIP_METADATA, VALUES},
    compression::compression::UNCOMPRESSED,
    rows,
};
use super::hints::Hint;

// ----------- decoding scope -----------

rows!(
    rows: Hashes {
        hashes: Vec<String>,
        hints: Option<Vec<Hint>>,
        is_hint: bool,
        address: Option<String>
    },
    row: Row(
        Hash,
        Extra
    ),
    column_decoder: AddressesDecoder
);

pub trait Rows {
    fn decode(self) -> Self;
    fn finalize(self) -> (Vec<String>, Option<Vec<Hint>>);
}

impl Rows for Hashes {
    fn decode(mut self) -> Self {
        while let Some(_) = self.next() {};
        self
    }
    fn finalize(self) -> (Vec<String>, Option<Vec<Hint>>) {
        (self.hashes, self.hints)
    }
}
// implementation to decode the columns in order to form the hashes & hints eventually
impl AddressesDecoder for Hash {
    fn decode_column(start: usize, length: i32, acc: &mut Hashes) {
        // check if the current row is a hint by checking the length
        if length == 1 { // it means the hash is "0",
            acc.is_hint = true
        } else {
            // decode transaction hash
            let hash = String::decode(
                &acc.buffer()[start..], length as usize
            );
            // insert hash into hashset
            acc.hashes.push(hash);
        }
    }
}

impl AddressesDecoder for Extra {
    fn decode_column(start: usize, _length: i32, acc: &mut Hashes) {
        if acc.is_hint == true {
            // create a hint
            let end = start+2;
            let year = u16::from_be_bytes(acc.buffer()[start..end].try_into().unwrap());
            let month = acc.buffer()[end];
            let address = acc.address.take().unwrap();
            Hint::new_address_hint(address, None, year, month);
            // reset is_hint to be false for next() rows if any.
            acc.is_hint = false;
        }
    }
}
// ----------- encoding scope -----------

pub fn query(address: &str) -> Vec<u8> {
    let Query(payload) = Query::new()
        .version()
        .flags(header::IGNORE)
        .stream(0)
        .opcode()
        .length()
        .statement(
            "SELECT tx FROM tangle.edge WHERE vertex = ? AND kind in ['input','output','hint']"
        )
        .consistency(Consistency::One)
        .query_flags(SKIP_METADATA | VALUES)
        .value_count(1)
        .value(address)
        .build(UNCOMPRESSED);
    payload
}
