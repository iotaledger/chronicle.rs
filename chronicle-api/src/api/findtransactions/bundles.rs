use chronicle_cql::{
    frame::decoder::{
        Decoder,
        Frame,
    },
    rows,
};
use std::collections::HashSet;
rows!(
    rows: Hashes {hashes: HashSet<String>},
    row: Row(
        Hash
    ),
    column_decoder: BundlesDecoder
);

// define decoder trait as you wish
trait Rows {
    // to decode the rows
    fn decode(self) -> Self;
    // to finalize it as the expected result (trytes or none)
    fn finalize(self) -> Option<HashSet<String>>;
}

impl Rows for Hashes {
    fn decode(mut self) -> Self {
        // each next() call will decode one row
        self.next();
        // return
        self
    }
    fn finalize(mut self) -> Option<HashSet<String>> {
        // check if result was not empty
        if self.rows_count == 1 {
            self.hashes
                .insert(String::from_utf8(self.decoder.into_buffer()).unwrap());
            Some(self.hashes)
        } else {
            // we didn't have any transaction row for the provided hash.
            None
        }
    }
}
// implementation to decoder the columns in order to form the trytes eventually
impl BundlesDecoder for Hash {
    fn decode_column(start: usize, length: i32, acc: &mut Hashes) {
        acc.buffer().copy_within(start..(start + length as usize), 81)
    }
}
