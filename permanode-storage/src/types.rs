use bee_message::{
    Message,
    MessageId,
};
use bee_tangle::metadata::MessageMetadata;

pub struct MessageTable {
    pub id: MessageId,
    pub message: Option<Message>,
    pub metadata: Option<MessageMetadata>,
}
