use super::{
    ChronicleKeyspace,
    ComputeToken,
};

impl<K> ComputeToken<K> for ChronicleKeyspace {
    fn token(_key: &K) -> i64 {
        rand::random()
    }
}
