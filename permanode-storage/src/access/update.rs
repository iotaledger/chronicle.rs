use super::*;

impl<'a> Update<'a, Bee<MessageId>, Bee<Message>> for Mainnet {
    fn update_statement() -> std::borrow::Cow<'static, str> {
        todo!()
    }

    fn get_request(
        &'a self,
        key: &Bee<MessageId>,
        value: &Bee<Message>,
    ) -> UpdateRequest<'a, Self, Bee<MessageId>, Bee<Message>>
    where
        Self: Update<'a, Bee<MessageId>, Bee<Message>>,
    {
        todo!()
    }
}

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
