use super::*;

macro_rules! impl_delete {
    ($keyspace:ty: <$key:ty> -> $block:block) => {
        impl Delete<$key> for $keyspace {
            fn delete(&self, key: &$key) -> DeleteQuery<$key>
            $block
        }
    };
}

impl_delete!(Mainnet: <MessageId> -> { todo!() });
// impl_delete!(Mainnet: <(MessageId, MessageId)> -> { todo!() });
// impl_delete!(Mainnet: <(HashedIndex, MessageId)> -> { todo!() });
// impl_delete!(Mainnet: <OutputId> -> { todo!() });
// impl_delete!(Mainnet: <Unspent> -> { todo!() });
// impl_delete!(Mainnet: <(Ed25519Address, OutputId)> -> { todo!() });
// impl_delete!(Mainnet: <MilestoneIndex> -> { todo!() });
// impl_delete!(Mainnet: <SolidEntryPoint> -> { todo!() });
// impl_delete!(Mainnet: <Address> -> { todo!() });
// impl_delete!(Mainnet: <(MilestoneIndex, UnconfirmedMessage)> -> { todo!() });
