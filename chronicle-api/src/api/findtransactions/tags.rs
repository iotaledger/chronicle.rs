use crate::api::types::Trytes27;
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
    rows: Hints {tag: Trytes27, year: u16, month: u8, timeline: VecDeque<YearMonth>, hints: Vec<Hint>},
    row: Row(
        Year,
        Month
    ),
    column_decoder: TagsDecoder
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
            // push_back the year_month to the timeline of the tag hint
            self.timeline.push_back(year_month);
        }
        self
    }
    fn finalize(mut self) -> Vec<Hint> {
        if self.rows_count != 0 {
            // create hint for the given tag
            let hint = Hint::new_tag_hint(self.tag, self.timeline);
            // push hint to the hints
            self.hints.push(hint);
        }
        // return hints of the tag
        self.hints
    }
}

impl TagsDecoder for Year {
    fn decode_column(start: usize, length: i32, acc: &mut Hints) {
        // decode year
        acc.year = u16::decode(&acc.buffer()[start..], length as usize);
    }
    fn handle_null(_: &mut Hints) {
        unreachable!()
    }
}

impl TagsDecoder for Month {
    fn decode_column(start: usize, length: i32, acc: &mut Hints) {
        // decode month
        acc.month = u8::decode(&acc.buffer()[start..], length as usize);
    }
    fn handle_null(_: &mut Hints) {
        unreachable!()
    }
}

// ----------- encoding scope -----------

pub fn query(tag: &Trytes27) -> Vec<u8> {
    let Query(payload) = Query::new()
        .version()
        .flags(MyCompression::flag())
        .stream(0)
        .opcode()
        .length()
        .statement(SELECT_BY_TAG_QUERY)
        .consistency(Consistency::One)
        .query_flags(SKIP_METADATA | VALUES)
        .value_count(1)
        .value(tag)
        .build(MyCompression::get());
    payload
}

const SELECT_BY_TAG_QUERY: &str = {
#[cfg(feature = "mainnet")]
let cql = "SELECT year, month FROM mainnet.hint WHERE vertex = ? AND kind = 'tag'";
#[cfg(feature = "devnet")]
#[cfg(not(feature = "mainnet"))]
#[cfg(not(feature = "comnet"))]
let cql = "SELECT year, month FROM devnet.hint WHERE vertex = ? AND kind = 'tag'";
#[cfg(feature = "comnet")]
#[cfg(not(feature = "mainnet"))]
#[cfg(not(feature = "devnet"))]
let cql = "SELECT year, month FROM comnet.hint WHERE vertex = ? AND kind = 'tag'";
cql
};
