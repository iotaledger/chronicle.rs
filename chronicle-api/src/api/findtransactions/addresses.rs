use crate::api::types::Trytes81;
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
        },
    },
    rows,
};

use super::{
    hints::{
        Hint,
        YearMonth,
    },
    VecDeque,
};

// ----------- decoding scope -----------
rows!(
    rows: Hints {address: Trytes81, year: u16, month: u8, timeline: VecDeque<YearMonth>, hints: Vec<Hint>},
    row: Row(
        Year,
        Month
    ),
    column_decoder: AddressesDecoder
);

pub trait Rows {
    fn decode(self) -> Self;
    fn finalize(self) -> Vec<Hint>;
}

impl Rows for Hints {
    fn decode(mut self) -> Self {
        while let Some(_) = self.next() {
            // after each row we create the hint
            let year_month = YearMonth::new(self.year, self.month);
            // push_back the year_month to the timeline of the address hint
            self.timeline.push_back(year_month);
        }
        self
    }
    fn finalize(mut self) -> Vec<Hint> {
        if self.rows_count != 0 {
            // create hint for the given address
            let hint = Hint::new_address_hint(self.address, self.timeline);
            // push hint to the hints
            self.hints.push(hint);
        }
        // return hints of the address
        self.hints
    }
}

impl AddressesDecoder for Year {
    fn decode_column(start: usize, length: i32, acc: &mut Hints) {
        // decode year
        acc.year = u16::decode(&acc.buffer()[start..], length as usize);
    }
    fn handle_null(_: &mut Hints) {
        unreachable!()
    }
}

impl AddressesDecoder for Month {
    fn decode_column(start: usize, length: i32, acc: &mut Hints) {
        // decode month
        acc.month = u8::decode(&acc.buffer()[start..], length as usize);
    }
    fn handle_null(_: &mut Hints) {
        unreachable!()
    }
}

// ----------- encoding scope -----------

pub fn query(address: &Trytes81) -> Vec<u8> {
    let Query(payload) = Query::new()
        .version()
        .flags(MyCompression::flag())
        .stream(0)
        .opcode()
        .length()
        .statement(SELECT_BY_ADDRESS_QUERY)
        .consistency(Consistency::One)
        .query_flags(SKIP_METADATA | VALUES)
        .value_count(1)
        .value(address)
        .build(MyCompression::get());
    payload
}

#[cfg(feature = "mainnet")]
const SELECT_BY_ADDRESS_QUERY: &str = "SELECT year, month FROM mainnet.hint WHERE vertex = ? AND kind = 'address'";
#[cfg(feature = "devnet")]
#[cfg(not(feature = "mainnet"))]
#[cfg(not(feature = "comnet"))]
const SELECT_BY_ADDRESS_QUERY: &str = "SELECT year, month FROM devnet.hint WHERE vertex = ? AND kind = 'address'";
#[cfg(feature = "comnet")]
#[cfg(not(feature = "mainnet"))]
#[cfg(not(feature = "devnet"))]
const SELECT_BY_ADDRESS_QUERY: &str = "SELECT year, month FROM comnet.hint WHERE vertex = ? AND kind = 'address'";
