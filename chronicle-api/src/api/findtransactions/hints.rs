use serde::{
    Deserialize,
    Serialize,
};

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Hint {
    address: Option<String>,
    tag: Option<String>,
    paging_state: Option<String>,
    year: u16,
    month: u8,
}
