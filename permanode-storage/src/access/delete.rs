use super::*;

macro_rules! impl_delete {
    ($keyspace:ty: <$key:ty, $val:ty> -> $block:block) => {
        impl<'a> Delete<'a, $key, $val> for $keyspace {
            fn get_request(&self, key: &$key) -> DeleteRequest<Self, $key, $val>
            $block
        }
    };
}

impl_delete!(Mainnet: <Message, MessageId> -> { todo!() });
// impl_delete!(Mainnet: <(MessageId, MessageId)> -> { todo!() });
// impl_delete!(Mainnet: <(HashedIndex, MessageId)> -> { todo!() });
// impl_delete!(Mainnet: <OutputId> -> { todo!() });
// impl_delete!(Mainnet: <Unspent> -> { todo!() });
// impl_delete!(Mainnet: <(Ed25519Address, OutputId)> -> { todo!() });
// impl_delete!(Mainnet: <MilestoneIndex> -> { todo!() });
// impl_delete!(Mainnet: <SolidEntryPoint> -> { todo!() });
// impl_delete!(Mainnet: <Address> -> { todo!() });
// impl_delete!(Mainnet: <(MilestoneIndex, UnconfirmedMessage)> -> { todo!() });
