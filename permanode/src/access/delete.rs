use super::*;
use bee_message::{
    Message,
    MessageId,
};
use scylla::access::delete::Delete;

#[async_trait]
impl Delete<MessageId, Message> for Mainnet {
    async fn delete<T>(&self, worker: T, key: &MessageId)
    where
        T: scylla_cql::VoidDecoder<MessageId, Message> + scylla::Worker,
    {
        todo!()
    }
}
