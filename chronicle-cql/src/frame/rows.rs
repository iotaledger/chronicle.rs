pub type ColumnsCount = i32;
#[derive(Debug)]
#[allow(dead_code)]
pub struct Flags {
    global_table_spec: bool,
    has_more_pages: bool,
    no_metadata: bool,
}

impl Flags {
    pub fn from_i32(flags: i32) -> Self {
        Flags {
            global_table_spec: (flags & 1) == 1,
            has_more_pages: (flags & 2) == 2,
            no_metadata: (flags & 4) == 4,
        }
    }
    pub fn has_more_pages(&self) -> bool {
        self.has_more_pages
    }
}
#[derive(Debug)]
pub struct PagingState {
    paging_state: Option<Vec<u8>>,
    end: usize,
}
impl PagingState {
    pub fn new(paging_state: Option<Vec<u8>>, end: usize) -> Self {
        PagingState { paging_state, end }
    }
}
#[derive(Debug)]
#[allow(unused)]
pub struct Metadata {
    flags: Flags,
    columns_count: ColumnsCount,
    paging_state: PagingState,
}

impl Metadata {
    pub fn new(flags: Flags, columns_count: ColumnsCount, paging_state: PagingState) -> Self {
        Metadata {
            flags,
            columns_count,
            paging_state,
        }
    }
    pub fn rows_start(&self) -> usize {
        self.paging_state.end
    }
    pub fn take_paging_state(&mut self) -> Option<Vec<u8>> {
        self.paging_state.paging_state.take()
    }
}

#[macro_export]
macro_rules! rows {
    (rows: $rows:ident {$( $field:ident:$type:ty ),*}, row: $row:ident($( $col_type:tt ),*), column_decoder: $decoder:ident ) => {
        use std::convert::TryInto;
        use chronicle_cql::compression::Compression;
        trait $decoder {
            fn decode_column(start: usize, length: i32, acc: &mut $rows);
            fn handle_null(acc: &mut $rows);
        }
        pub struct $rows {
            decoder: Decoder,
            rows_count: usize,
            remaining_rows_count: usize,
            metadata: chronicle_cql::frame::rows::Metadata,
            column_start: usize,
            $(
                $field: $type,
            )*
        }
        struct $row(
            $( $col_type, )*
        );

        $(
            struct $col_type;
        )*
        impl Iterator for $rows {
            type Item = usize;
            fn next(&mut self) -> Option<<Self as Iterator>::Item> {
                if self.remaining_rows_count > 0 {
                    self.remaining_rows_count -= 1;
                    $(
                        let length = i32::from_be_bytes(
                            self.decoder.buffer_as_ref()[self.column_start..(self.column_start+4)].try_into().unwrap()
                        );
                        self.column_start += 4; // now it become the column_value start, or next column_start if length < 0
                        if length > 0 {
                            $col_type::decode_column(self.column_start, length, self);
                            // update the next column_start to start from next column
                            self.column_start += (length as usize);
                        } else {
                            $col_type::handle_null(self);
                        }
                    )*
                    Some(self.remaining_rows_count)
                } else {
                    None
                }
            }
        }

        impl $rows {
            pub fn new(mut decoder: Decoder, $($field: $type,)*) -> Self {
                let metadata = decoder.metadata();
                let rows_start = metadata.rows_start();
                let column_start = rows_start+4;
                let rows_count = i32::from_be_bytes(decoder.buffer_as_ref()[rows_start..column_start].try_into().unwrap());
                Self{
                    decoder,
                    metadata,
                    rows_count: rows_count as usize,
                    remaining_rows_count: rows_count as usize,
                    column_start,
                    $(
                        $field,
                    )*
                }
            }
            pub fn buffer(&mut self) -> &mut Vec<u8> {
                self.decoder.buffer_as_mut()
            }
        }
    };
}
