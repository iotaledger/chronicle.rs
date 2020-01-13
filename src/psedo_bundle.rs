use cdrs::{
  query::QueryValues,
  query_values,
  types::{from_cdrs::FromCDRSByName, prelude::*},
};
use time::Timespec;

#[derive(Debug, TryFromRow)]
pub struct PseudoBundle {
  pub bundle: String,
  pub time: Timespec,
  pub info: String,
}

impl PseudoBundle {
  pub fn into_query_values(self) -> QueryValues {
    query_values!("bundle" => self.bundle, "time" => self.time, "info" => self.info)
  }
}
