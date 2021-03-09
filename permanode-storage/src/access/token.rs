use super::{
    ComputeToken,
    PermanodeKeyspace,
};

impl<K> ComputeToken<K> for PermanodeKeyspace {
    fn token(_key: &K) -> i64 {
        0
    }
}
