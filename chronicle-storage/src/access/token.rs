// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;

impl ComputeToken<MessageId> for ChronicleKeyspace {
    fn token(key: &MessageId) -> i64 {
        key.to_string().get_token()
    }
}

impl ComputeToken<Partitioned<MessageId>> for ChronicleKeyspace {
    fn token(key: &Partitioned<MessageId>) -> i64 {
        key.to_string().chain_token(&key.partition_id()).finish()
    }
}

impl ComputeToken<Partitioned<Indexation>> for ChronicleKeyspace {
    fn token(key: &Partitioned<Indexation>) -> i64 {
        key.0.chain_token(&key.partition_id()).finish()
    }
}

impl ComputeToken<Partitioned<Ed25519Address>> for ChronicleKeyspace {
    fn token(key: &Partitioned<Ed25519Address>) -> i64 {
        key.to_string().chain_token(&key.partition_id()).finish()
    }
}

impl ComputeToken<OutputId> for ChronicleKeyspace {
    fn token(key: &OutputId) -> i64 {
        key.transaction_id().to_string().chain_token(&key.index()).finish()
    }
}

impl ComputeToken<TransactionId> for ChronicleKeyspace {
    fn token(key: &TransactionId) -> i64 {
        key.to_string().get_token()
    }
}

impl ComputeToken<MilestoneIndex> for ChronicleKeyspace {
    fn token(key: &MilestoneIndex) -> i64 {
        key.0.get_token()
    }
}

impl ComputeToken<Hint> for ChronicleKeyspace {
    fn token(key: &Hint) -> i64 {
        key.hint.chain_token(&key.variant.to_string()).finish()
    }
}

impl ComputeToken<SyncRange> for ChronicleKeyspace {
    fn token(key: &SyncRange) -> i64 {
        "permanode".get_token()
    }
}

impl ComputeToken<Synckey> for ChronicleKeyspace {
    fn token(key: &Synckey) -> i64 {
        "permanode".get_token()
    }
}

impl ComputeToken<(TransactionId, Index)> for ChronicleKeyspace {
    fn token(key: &(TransactionId, Index)) -> i64 {
        key.0.to_string().chain_token(&key.1).finish()
    }
}

impl ComputeToken<Ed25519AddressPK> for ChronicleKeyspace {
    fn token(key: &Ed25519AddressPK) -> i64 {
        key.address.to_string().chain_token(&key.partition_id).finish()
    }
}

impl ComputeToken<IndexationPK> for ChronicleKeyspace {
    fn token(key: &IndexationPK) -> i64 {
        key.indexation.0.chain_token(&key.partition_id).finish()
    }
}

impl ComputeToken<ParentPK> for ChronicleKeyspace {
    fn token(key: &ParentPK) -> i64 {
        key.parent_id.to_string().chain_token(&key.partition_id).finish()
    }
}
