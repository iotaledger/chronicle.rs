use super::frame::Frame;

pub type ColumnsCount = i32;
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

pub struct PagingState {
    paging_state: Option<Vec<u8>>,
    end: usize,
}
impl PagingState {
    pub fn new(paging_state: Option<Vec<u8>>, end: usize) -> Self {
        PagingState {
            paging_state,
            end,
        }
    }
}
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
}

#[macro_export]
macro_rules! rows {
    (rows: $rows:ident {$( $field:ident:$type:ty ),*}, row: $row:ident($( $col_type:tt ),*), column_decoder: $decoder:ident ) => {
        use std::convert::TryInto;
        trait $decoder {
            fn decode_column(start: usize, length: i32, acc: &mut $rows);
        }
        pub struct $rows {
            buffer: Vec<u8>,
            rows_count: usize,
            remaining_rows_count: usize,
            metadata: chronicle_cql::frame::rows::Metadata,
            row: $row,
            column_start: usize,
            $(
                $field: Option<$type>,
            )*
        }
        #[derive(Default)]
        struct $row(
            $( $col_type, )*
        );

        $(
            #[derive(Default)]
            struct $col_type;
        )*
        impl Iterator for $rows {
            type Item = usize;
            fn next(&mut self) -> Option<<Self as Iterator>::Item> {
                if self.remaining_rows_count > 0 {
                    self.remaining_rows_count -= 1;
                    $(
                        let length = i32::from_be_bytes(        self.buffer[self.column_start..(self.column_start+4)].try_into().unwrap());
                        self.column_start += 4; // now it become the column_value start.
                        $col_type::decode_column(self.column_start, length, self);
                        // update the next column_start to start from next column
                        if length > 0 {
                            self.column_start += (length as usize);
                        }
                    )*
                    Some(self.remaining_rows_count)
                } else {
                    None
                }
            }
        }

        impl $rows {
            pub fn new(buffer: Vec<u8>) -> Self {
                let metadata = buffer.metadata();
                let column_start = metadata.rows_start();
                let rows_count = i32::from_be_bytes(buffer[column_start..(column_start+4)].try_into().unwrap());
                let row = $row::default();
                Self{
                    buffer: buffer,
                    metadata,
                    rows_count: rows_count as usize,
                    remaining_rows_count: rows_count as usize,
                    row,
                    column_start,
                    $(
                        $field: None,
                    )*
                }
            }
            $(
                pub fn $field(mut self, $field: $type) -> Self {
                    self.$field.replace($field);
                    self
                }
            )*
        }
    };
}
