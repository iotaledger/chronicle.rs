use super::*;

macro_rules! impl_update {
    ($keyspace:ty: <$key:ty, $val:ty> -> $block:block) => {
        impl<'a> Update<'a, $key, $val> for $keyspace {
            fn get_request(&self, key: &$key, value: &$val) -> UpdateRequest<Self, $key, $val>
            $block
        }
    };
}

impl_update!(Mainnet: <MessageId, Message> -> { todo!() });
// impl_update!(Mainnet: <MessageId, MessageMetadata> -> { todo!() });
// impl_update!(Mainnet: <(MessageId, MessageId), ()> -> { todo!() });
// impl_update!(Mainnet: <(HashedIndex, MessageId), ()> -> { todo!() });
// impl_update!(Mainnet: <OutputId, CreatedOutput> -> { todo!() });
// impl_update!(Mainnet: <OutputId, ConsumedOutput> -> { todo!() });
// impl_update!(Mainnet: <Unspent, ()> -> { todo!() });
// impl_update!(Mainnet: <(Ed25519Address, OutputId), ()> -> { todo!() });
// impl_update!(Mainnet: <(), LedgerIndex> -> { todo!() });
// impl_update!(Mainnet: <MilestoneIndex, Milestone> -> { todo!() });
// impl_update!(Mainnet: <(), SnapshotInfo> -> { todo!() });
// impl_update!(Mainnet: <SolidEntryPoint, MilestoneIndex> -> { todo!() });
// impl_update!(Mainnet: <MilestoneIndex, OutputDiff> -> { todo!() });
// impl_update!(Mainnet: <Address, Balance> -> { todo!() });
// impl_update!(Mainnet: <(MilestoneIndex, UnconfirmedMessage), ()> -> { todo!() });
