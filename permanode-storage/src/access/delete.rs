use super::*;

impl<'a> Delete<'a, Bee<Message>, Bee<MessageId>> for Mainnet {
    fn statement(&'a self) -> std::borrow::Cow<'static, str> {
        todo!()
    }

    fn get_request(&'a self, key: &Bee<Message>) -> DeleteRequest<'a, Self, Bee<Message>, Bee<MessageId>>
    where
        Self: Delete<'a, Bee<Message>, Bee<MessageId>>,
    {
        todo!()
    }
}
// impl_delete!(Mainnet: <(MessageId, MessageId)> -> { todo!() });
// impl_delete!(Mainnet: <(HashedIndex, MessageId)> -> { todo!() });
// impl_delete!(Mainnet: <OutputId> -> { todo!() });
// impl_delete!(Mainnet: <Unspent> -> { todo!() });
// impl_delete!(Mainnet: <(Ed25519Address, OutputId)> -> { todo!() });
// impl_delete!(Mainnet: <MilestoneIndex> -> { todo!() });
// impl_delete!(Mainnet: <SolidEntryPoint> -> { todo!() });
// impl_delete!(Mainnet: <Address> -> { todo!() });
// impl_delete!(Mainnet: <(MilestoneIndex, UnconfirmedMessage)> -> { todo!() });
