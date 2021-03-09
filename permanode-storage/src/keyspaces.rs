pub use scylla::access::Keyspace;
use std::borrow::Cow;

/// The Mainnet keyspace, which will organize its tables to pull data from the Mainnet tangle network
#[derive(Default, Clone)]
pub struct PermanodeKeyspace {
    name: Cow<'static, str>,
}

impl PermanodeKeyspace {
    /// Create a new instance of the Mainnet keyspace
    pub fn new(name: String) -> Self {
        Self { name: name.into() }
    }
}

impl Keyspace for PermanodeKeyspace {
    fn name(&self) -> &Cow<'static, str> {
        &self.name
    }
}
