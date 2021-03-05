use super::{
    ComputeToken,
    Mainnet,
};

impl<K> ComputeToken<K> for Mainnet {
    fn token(_key: &K) -> i64 {
        0
    }
}
