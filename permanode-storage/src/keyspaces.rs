use scylla::access::keyspace::Keyspace;
use std::borrow::Cow;
pub struct Mainnet;

impl Keyspace for Mainnet {
    type Error = Cow<'static, str>;

    const NAME: &'static str = "mainnet";
}
