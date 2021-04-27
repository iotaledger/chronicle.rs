use chronicle_storage::{
    access::Keyspace,
    keyspaces::ChronicleKeyspace,
};

#[test]
pub fn test_create_keyspace() {
    let key_space = ChronicleKeyspace::new("chroincle_test".to_owned());
    assert_eq!(key_space.name(), "chroincle_test");
}
