use crate::api::types::Trytes81;
use chronicle_cql::{
    compression::compression::UNCOMPRESSED,
    frame::{
        consistency::Consistency,
        decoder::{
            ColumnDecoder,
            Decoder,
            Frame,
        },
        header::{
            self,
            Header,
        },
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
    rows: Hashes {hashes: Vec<Trytes81>},
    row: Row(
        Hash
    ),
    column_decoder: ApproveesDecoder
);

pub trait Rows {
    fn decode(self) -> Self;
    fn finalize(self) -> Vec<Trytes81>;
}

impl Rows for Hashes {
    fn decode(mut self) -> Self {
        while let Some(_) = self.next() {}
        self
    }
    fn finalize(self) -> Vec<Trytes81> {
        self.hashes
    }
}
// implementation to decode the columns in order to form the hash eventually
impl ApproveesDecoder for Hash {
    fn decode_column(start: usize, length: i32, acc: &mut Hashes) {
        // decode transaction hash
        let hash = Trytes81::decode(&acc.buffer()[start..], length as usize);
        acc.hashes.push(hash);
    }
}

// ----------- encoding scope -----------

/// Create a query frame to lookup for tx-hashes in the edge table using an approve
pub fn query(approve: &Trytes81) -> Vec<u8> {
    let Query(payload) = Query::new()
        .version()
        .flags(header::IGNORE)
        .stream(0)
        .opcode()
        .length()
        .statement("SELECT tx FROM tangle.edge WHERE vertex = ? AND kind in ['trunk','branch']")
        .consistency(Consistency::One)
        .query_flags(SKIP_METADATA | VALUES)
        .value_count(1)
        .value(approve)
        .build(UNCOMPRESSED);
    payload
}
