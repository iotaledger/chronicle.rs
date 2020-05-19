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

impl Hint {
    pub fn new_address_hint(
        address: String,
        paging_state: Option<String>,
        year: u16, month: u8) -> Self {
        Self {
            address: Some(address),
            tag: None,
            paging_state,
            year,
            month
        }
    }
}
