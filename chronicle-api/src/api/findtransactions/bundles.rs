use super::{
    hints::{
        Hint,
        YearMonth,
    },
    VecDeque,
};
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

// ----------- decoding scope -----------
rows!(
    rows: Hints {bundle: Trytes81, year: u16, month: u8, timeline: VecDeque<YearMonth>, hints: Vec<Hint>},
    row: Row(
        Year,
        Month
    ),
    column_decoder: BundlesDecoder
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
            // push the year_month to the timeline of the bundle hint
            self.timeline.push_back(year_month);
        }
        self
    }
    fn finalize(mut self) -> Vec<Hint> {
        if self.rows_count != 0 {
            // create hint for the given bundle
            let hint = Hint::new_bundle_hint(self.bundle, self.timeline);
            // push hint to the hints
            self.hints.push(hint);
        }
        // return hints of the bundle
        self.hints
    }
}

impl BundlesDecoder for Year {
    fn decode_column(start: usize, length: i32, acc: &mut Hints) {
        // decode year
        acc.year = u16::decode(&acc.buffer()[start..], length as usize);
    }
    fn handle_null(_: &mut Hints) {
        unreachable!()
    }
}

impl BundlesDecoder for Month {
    fn decode_column(start: usize, length: i32, acc: &mut Hints) {
        // decode month
        acc.month = u8::decode(&acc.buffer()[start..], length as usize);
    }
    fn handle_null(_: &mut Hints) {
        unreachable!()
    }
}

// ----------- encoding scope -----------

/// Create a query frame to lookup for tx-hashes in the edge table using a bundle
pub fn query(bundle: &Trytes81) -> Vec<u8> {
    let Query(payload) = Query::new()
        .version()
        .flags(MyCompression::flag())
        .stream(0)
        .opcode()
        .length()
        .statement(SELECT_BY_BUNDLE_QUERY)
        .consistency(Consistency::One)
        .query_flags(SKIP_METADATA | VALUES)
        .value_count(1)
        .value(bundle)
        .build(MyCompression::get());
    payload
}

const SELECT_BY_BUNDLE_QUERY: &str = {
#[cfg(feature = "mainnet")]
let cql = "SELECT year, month FROM mainnet.hint WHERE vertex = ? AND kind = 'bundle'";
#[cfg(feature = "devnet")]
#[cfg(not(feature = "mainnet"))]
#[cfg(not(feature = "comnet"))]
let cql = "SELECT year, month FROM devnet.hint WHERE vertex = ? AND kind = 'bundle'";
#[cfg(feature = "comnet")]
#[cfg(not(feature = "mainnet"))]
#[cfg(not(feature = "devnet"))]
let cql = "SELECT year, month FROM comnet.hint WHERE vertex = ? AND kind = 'bundle'";
cql
};
