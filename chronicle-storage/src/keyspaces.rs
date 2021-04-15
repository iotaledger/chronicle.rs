pub use scylla::access::Keyspace;
use std::borrow::Cow;

/// The Permanode keyspace
#[derive(Default, Clone, Debug)]
pub struct PermanodeKeyspace {
    name: Cow<'static, str>,
}

impl PermanodeKeyspace {
    /// Create a new instance of the keyspace
    pub fn new(name: String) -> Self {
        Self { name: name.into() }
    }
}

impl Keyspace for PermanodeKeyspace {
    fn name(&self) -> &Cow<'static, str> {
        &self.name
    }
}
