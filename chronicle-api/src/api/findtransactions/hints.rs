use serde::{
    Deserialize,
    Serialize,
};
use crate::api::types::Trytes81;

#[derive(Deserialize, Serialize, Clone)]
pub struct Hint {
    address: Option<Trytes81>,
    tag: Option<String>,
    paging_state: Option<String>,
    year: u16,
    month: u8,
}

impl Hint {
    pub fn new_address_hint(
        address: Trytes81,
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
