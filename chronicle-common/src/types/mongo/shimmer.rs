// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0
// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use mongodb::bson::{
    to_bson,
    Bson,
    DateTime,
    Document,
};

pub fn message_to_bson(message: &crate::shimmer::Message) -> Bson {
    let mut doc = Document::new();
    doc.insert("protocol_version", message.protocol_version() as i32);
    doc.insert(
        "parents",
        message.parents().iter().map(|p| p.to_string()).collect::<Vec<_>>(),
    );
    doc.insert("payload", message.payload().map(|p| payload_to_bson(p)));
    doc.insert("nonce", message.nonce().to_string());
    Bson::Document(doc)
}

fn payload_to_bson(payload: &crate::shimmer::payload::Payload) -> Bson {
    use crate::shimmer::payload::Payload;
    match payload {
        Payload::Transaction(t) => transaction_payload_to_bson(&**t),
        Payload::Milestone(m) => milestone_payload_to_bson(&**m),
        Payload::Receipt(r) => receipt_payload_to_bson(&**r),
        Payload::TreasuryTransaction(t) => treasury_payload_to_bson(&**t),
        Payload::TaggedData(t) => tag_payload_to_bson(&**t),
    }
}

fn transaction_payload_to_bson(payload: &crate::shimmer::payload::transaction::TransactionPayload) -> Bson {
    use crate::shimmer::payload::transaction::TransactionPayload;
    let mut doc = Document::new();
    doc.insert("kind", TransactionPayload::KIND as i32);
    doc.insert("transaction_id", payload.id().to_string());
    doc.insert("essence", transaction_essence_to_bson(&payload.essence()));
    doc.insert(
        "unlock_blocks",
        payload
            .unlock_blocks()
            .iter()
            .map(|u| unlock_block_to_bson(u))
            .collect::<Vec<_>>(),
    );
    Bson::Document(doc)
}

fn milestone_payload_to_bson(payload: &crate::shimmer::payload::milestone::MilestonePayload) -> Bson {
    use crate::shimmer::payload::milestone::MilestonePayload;
    let mut doc = Document::new();
    doc.insert("kind", MilestonePayload::KIND as i32);
    doc.insert("milestone_id", payload.id().to_string());
    doc.insert("essence", milestone_essence_to_bson(&payload.essence()));
    doc.insert(
        "signatures",
        payload.signatures().map(|s| hex::encode(s)).collect::<Vec<_>>(),
    );
    Bson::Document(doc)
}

fn tag_payload_to_bson(payload: &crate::shimmer::payload::tagged_data::TaggedDataPayload) -> Bson {
    use crate::shimmer::payload::tagged_data::TaggedDataPayload;
    let mut doc = Document::new();
    doc.insert("kind", TaggedDataPayload::KIND as i32);
    doc.insert("tag", hex::encode(payload.tag()));
    doc.insert("data", hex::encode(payload.data()));
    Bson::Document(doc)
}

fn receipt_payload_to_bson(payload: &crate::shimmer::payload::receipt::ReceiptPayload) -> Bson {
    use crate::shimmer::payload::receipt::ReceiptPayload;
    let mut doc = Document::new();
    doc.insert("kind", ReceiptPayload::KIND as i32);
    doc.insert("migrated_at", payload.migrated_at().0 as i32);
    doc.insert("last", payload.last());
    doc.insert(
        "funds",
        payload
            .funds()
            .iter()
            .map(|f| {
                let mut doc = Document::new();
                doc.insert("tail_transaction_hash", hex::encode(f.tail_transaction_hash()));
                doc.insert("address", address_to_bson(f.address()));
                doc
            })
            .collect::<Vec<_>>(),
    );
    doc.insert("transaction", payload_to_bson(payload.transaction()));
    doc.insert("amount", payload.amount() as i64);
    Bson::Document(doc)
}

fn treasury_payload_to_bson(
    payload: &crate::shimmer::payload::treasury_transaction::TreasuryTransactionPayload,
) -> Bson {
    use crate::shimmer::payload::treasury_transaction::TreasuryTransactionPayload;
    let mut doc = Document::new();
    doc.insert("kind", TreasuryTransactionPayload::KIND as i32);
    doc.insert("input", input_to_bson(payload.input()));
    doc.insert("output", output_to_bson(payload.output()));
    Bson::Document(doc)
}

fn transaction_essence_to_bson(essence: &crate::shimmer::payload::transaction::TransactionEssence) -> Bson {
    use crate::shimmer::payload::transaction::TransactionEssence;
    let mut doc = Document::new();
    match essence {
        TransactionEssence::Regular(r) => {
            doc.insert("network_id", r.network_id() as i64);
            doc.insert(
                "inputs",
                r.inputs().iter().map(|i| input_to_bson(i)).collect::<Vec<_>>(),
            );
            doc.insert("inputs_commitment", hex::encode(r.inputs_commitment()));
            doc.insert(
                "outputs",
                r.outputs().iter().map(|o| output_to_bson(o)).collect::<Vec<_>>(),
            );
            doc.insert("payload", r.payload().as_ref().map(|p| payload_to_bson(p)));
        }
    }
    Bson::Document(doc)
}

fn milestone_essence_to_bson(essence: &crate::shimmer::payload::milestone::MilestoneEssence) -> Bson {
    let mut doc = Document::new();
    doc.insert("index", essence.index().0 as i32);
    doc.insert("timestamp", DateTime::from_millis(essence.timestamp() as i64));
    doc.insert(
        "parents",
        essence.parents().iter().map(|p| p.to_string()).collect::<Vec<_>>(),
    );
    doc.insert("merkle_proof", hex::encode(essence.merkle_proof()));
    doc.insert("next_pow_score", essence.next_pow_score() as i64);
    doc.insert(
        "next_pow_score_milestone_index",
        essence.next_pow_score_milestone_index() as i32,
    );
    doc.insert(
        "public_keys",
        essence.public_keys().iter().map(|p| hex::encode(p)).collect::<Vec<_>>(),
    );
    doc.insert("receipt", essence.receipt().map(|p| payload_to_bson(p)));
    Bson::Document(doc)
}

fn unlock_block_to_bson(unlock_block: &crate::shimmer::unlock_block::UnlockBlock) -> Bson {
    use crate::shimmer::{
        signature::Signature,
        unlock_block::{
            AliasUnlockBlock,
            NftUnlockBlock,
            ReferenceUnlockBlock,
            SignatureUnlockBlock,
            UnlockBlock,
        },
    };
    let mut doc = Document::new();
    match unlock_block {
        UnlockBlock::Signature(s) => match s.signature() {
            Signature::Ed25519(s) => {
                doc.insert("kind", SignatureUnlockBlock::KIND as i32);
                doc.insert("public_key", hex::encode(s.public_key()));
                doc.insert("signature", hex::encode(s.signature()));
            }
        },
        UnlockBlock::Reference(r) => {
            doc.insert("kind", ReferenceUnlockBlock::KIND as i32);
            doc.insert("index", r.index() as i32);
        }
        UnlockBlock::Alias(a) => {
            doc.insert("kind", AliasUnlockBlock::KIND as i32);
            doc.insert("index", a.index() as i32);
        }
        UnlockBlock::Nft(n) => {
            doc.insert("kind", NftUnlockBlock::KIND as i32);
            doc.insert("index", n.index() as i32);
        }
    }
    Bson::Document(doc)
}

fn input_to_bson(input: &crate::shimmer::input::Input) -> Bson {
    use crate::shimmer::input::{
        Input,
        TreasuryInput,
        UtxoInput,
    };
    let mut doc = Document::new();
    match input {
        Input::Utxo(u) => {
            doc.insert("kind", UtxoInput::KIND as i32);
            doc.insert("transaction_id", u.output_id().transaction_id().to_string());
            doc.insert("index", u.output_id().index() as i32);
        }
        Input::Treasury(t) => {
            doc.insert("kind", TreasuryInput::KIND as i32);
            doc.insert("milestone_id", t.milestone_id().to_string());
        }
    }
    Bson::Document(doc)
}

fn output_to_bson(output: &crate::shimmer::output::Output) -> Bson {
    use crate::shimmer::output::{
        AliasOutput,
        BasicOutput,
        FoundryOutput,
        NftOutput,
        Output,
        TreasuryOutput,
    };
    let mut doc = Document::new();
    match output {
        Output::Treasury(t) => {
            doc.insert("kind", TreasuryOutput::KIND as i32);
            doc.insert("amount", t.amount() as i64);
        }
        Output::Basic(b) => {
            doc.insert("kind", BasicOutput::KIND as i32);
            doc.insert("amount", b.amount() as i64);
            doc.insert("native_tokens", to_bson(b.native_tokens()).unwrap());
            doc.insert("unlock_conditions", to_bson(b.unlock_conditions()).unwrap());
            doc.insert("feature_blocks", to_bson(b.feature_blocks()).unwrap());
        }
        Output::Alias(a) => {
            doc.insert("kind", AliasOutput::KIND as i32);
            doc.insert("amount", a.amount() as i64);
            doc.insert("native_tokens", to_bson(a.native_tokens()).unwrap());
            doc.insert("alias_id", a.alias_id().to_string());
            doc.insert("state_index", a.state_index() as i32);
            doc.insert("state_metadata", hex::encode(a.state_metadata()));
            doc.insert("foundry_counter", a.foundry_counter() as i32);
            doc.insert("unlock_conditions", to_bson(a.unlock_conditions()).unwrap());
            doc.insert("feature_blocks", to_bson(a.feature_blocks()).unwrap());
            doc.insert(
                "immutable_feature_blocks",
                to_bson(a.immutable_feature_blocks()).unwrap(),
            );
        }
        Output::Foundry(f) => {
            doc.insert("kind", FoundryOutput::KIND as i32);
            doc.insert("amount", f.amount() as i64);
            doc.insert("native_tokens", to_bson(f.native_tokens()).unwrap());
            doc.insert("serial_number", f.serial_number() as i32);
            doc.insert("token_tag", f.token_tag().to_string());
            doc.insert("minted_tokens", f.minted_tokens().to_string());
            doc.insert("melted_tokens", f.melted_tokens().to_string());
            doc.insert("maximum_supply", f.maximum_supply().to_string());
            doc.insert("token_scheme", f.token_scheme() as u8 as i32);
            doc.insert("unlock_conditions", to_bson(f.unlock_conditions()).unwrap());
            doc.insert("feature_blocks", to_bson(f.feature_blocks()).unwrap());
            doc.insert(
                "immutable_feature_blocks",
                to_bson(f.immutable_feature_blocks()).unwrap(),
            );
        }
        Output::Nft(n) => {
            doc.insert("kind", NftOutput::KIND as i32);
            doc.insert("amount", n.amount() as i64);
            doc.insert("native_tokens", to_bson(n.native_tokens()).unwrap());
            doc.insert("nft_id", n.nft_id().to_string());
            doc.insert("unlock_conditions", to_bson(n.unlock_conditions()).unwrap());
            doc.insert("feature_blocks", to_bson(n.feature_blocks()).unwrap());
            doc.insert(
                "immutable_feature_blocks",
                to_bson(n.immutable_feature_blocks()).unwrap(),
            );
        }
    }
    Bson::Document(doc)
}

fn address_to_bson(address: &crate::shimmer::address::Address) -> Bson {
    use crate::shimmer::address::{
        Address,
        AliasAddress,
        Ed25519Address,
        NftAddress,
    };
    let mut doc = Document::new();
    match address {
        Address::Ed25519(a) => {
            doc.insert("kind", Ed25519Address::KIND as i32);
            doc.insert("data", a.to_string());
        }
        Address::Alias(a) => {
            doc.insert("kind", AliasAddress::KIND as i32);
            doc.insert("data", a.alias_id().to_string());
        }
        Address::Nft(n) => {
            doc.insert("kind", NftAddress::KIND as i32);
            doc.insert("data", n.nft_id().to_string());
        }
    }
    Bson::Document(doc)
}

// fn unlock_condition_to_bson(unlock_condition: &crate::shimmer::output::unlock_condition::UnlockCondition) -> Bson {
//     use crate::shimmer::output::unlock_condition::UnlockCondition;
//     let mut doc = Document::new();
//     match unlock_condition {
//         UnlockCondition::Address(_) => todo!(),
//         UnlockCondition::StorageDepositReturn(_) => todo!(),
//         UnlockCondition::Timelock(_) => todo!(),
//         UnlockCondition::Expiration(_) => todo!(),
//         UnlockCondition::StateControllerAddress(_) => todo!(),
//         UnlockCondition::GovernorAddress(_) => todo!(),
//         UnlockCondition::ImmutableAliasAddress(_) => todo!(),
//     }
//     Bson::Document(doc)
// }
//
// fn feature_block_to_bson(feature_block: &crate::shimmer::output::feature_block::FeatureBlock) -> Bson {
//     use crate::shimmer::output::feature_block::FeatureBlock;
//     let mut doc = Document::new();
//     match feature_block {
//         FeatureBlock::Sender(_) => todo!(),
//         FeatureBlock::Issuer(_) => todo!(),
//         FeatureBlock::Metadata(_) => todo!(),
//         FeatureBlock::Tag(_) => todo!(),
//     }
//     Bson::Document(doc)
// }
