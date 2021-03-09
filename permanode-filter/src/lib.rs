use permanode_storage::access::Message;
use std::borrow::Cow;

pub struct FilterResponse {
    /// The keyspace in which this message should be stored
    pub keyspace: Cow<'static, str>,
    /// The record's time-to-live
    pub ttl: Option<usize>,
}

pub async fn filter_messages(message: &mut Message) -> FilterResponse {
    todo!()
}
