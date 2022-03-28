// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0
// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::{
    BsonExt,
    DocExt,
};
use crate::shimmer::{
    address::{
        Address,
        AliasAddress,
        Ed25519Address,
        NftAddress,
    },
    input::{
        Input,
        TreasuryInput,
        UtxoInput,
    },
    milestone::MilestoneIndex,
    output::{
        AliasId,
        AliasOutput,
        BasicOutput,
        FeatureBlocks,
        FoundryOutput,
        NativeTokens,
        NftId,
        NftOutput,
        Output,
        TokenTag,
        TreasuryOutput,
        UnlockConditions,
    },
    parent::Parents,
    payload::{
        milestone::{
            MilestoneEssence,
            MilestoneId,
            MilestonePayload,
        },
        receipt::{
            MigratedFundsEntry,
            ReceiptPayload,
            TailTransactionHash,
        },
        tagged_data::TaggedDataPayload,
        transaction::{
            RegularTransactionEssence,
            TransactionEssence,
            TransactionId,
            TransactionPayload,
        },
        treasury_transaction::TreasuryTransactionPayload,
        Payload,
    },
    signature::{
        Ed25519Signature,
        Signature,
    },
    unlock_block::{
        AliasUnlockBlock,
        NftUnlockBlock,
        ReferenceUnlockBlock,
        SignatureUnlockBlock,
        UnlockBlock,
        UnlockBlocks,
    },
    Message,
    MessageBuilder,
    MessageId,
};
use anyhow::{
    anyhow,
    bail,
};
use mongodb::bson::{
    from_bson,
    to_bson,
    Bson,
    DateTime,
    Document,
};
use std::str::FromStr;

pub fn message_to_bson(message: &Message) -> Bson {
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

pub fn message_from_bson(bson: Bson) -> anyhow::Result<Message> {
    let doc = bson.to_document()?;
    message_from_doc(doc)
}

pub fn message_from_doc(mut doc: Document) -> anyhow::Result<Message> {
    let mut builder = MessageBuilder::new(Parents::new(
        doc.take_array("parents")?
            .into_iter()
            .map(|p| message_id_from_bson(p))
            .collect::<Result<Vec<_>, _>>()?,
    )?)
    .with_protocol_version(doc.get_i32("protocol_version")? as u8)
    .with_nonce_provider(doc.get_str("nonce")?.parse::<u64>()?, 0.0);
    if let Some(payload) = doc.take("payload").ok().map(|r| payload_from_bson(r)).transpose()? {
        builder = builder.with_payload(payload);
    }
    Ok(builder.finish()?)
}

fn payload_to_bson(payload: &Payload) -> Bson {
    match payload {
        Payload::Transaction(t) => transaction_payload_to_bson(&**t),
        Payload::Milestone(m) => milestone_payload_to_bson(&**m),
        Payload::Receipt(r) => receipt_payload_to_bson(&**r),
        Payload::TreasuryTransaction(t) => treasury_payload_to_bson(&**t),
        Payload::TaggedData(t) => tag_payload_to_bson(&**t),
    }
}

pub fn payload_from_bson(bson: Bson) -> anyhow::Result<Payload> {
    let kind = bson
        .as_document()
        .ok_or_else(|| anyhow!("Invalid payload"))?
        .get_i32("kind")? as u32;
    Ok(match kind {
        TransactionPayload::KIND => Payload::Transaction(Box::new(transaction_payload_from_bson(bson)?)),
        MilestonePayload::KIND => Payload::Milestone(Box::new(milestone_payload_from_bson(bson)?)),
        TaggedDataPayload::KIND => Payload::TaggedData(Box::new(tag_payload_from_bson(bson)?)),
        ReceiptPayload::KIND => Payload::Receipt(Box::new(receipt_payload_from_bson(bson)?)),
        TreasuryTransactionPayload::KIND => Payload::TreasuryTransaction(Box::new(treasury_payload_from_bson(bson)?)),
        _ => bail!("Unknown payload kind: {}", kind),
    })
}

fn transaction_payload_to_bson(payload: &TransactionPayload) -> Bson {
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

fn transaction_payload_from_bson(bson: Bson) -> anyhow::Result<TransactionPayload> {
    let mut doc = bson.to_document()?;
    Ok(TransactionPayload::new(
        transaction_essence_from_bson(doc.take("essence")?)?,
        UnlockBlocks::new(
            doc.take_array("unlock_blocks")?
                .into_iter()
                .map(|u| unlock_block_from_bson(u))
                .collect::<Result<Vec<_>, _>>()?,
        )?,
    )?)
}

fn milestone_payload_to_bson(payload: &MilestonePayload) -> Bson {
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

fn milestone_payload_from_bson(bson: Bson) -> anyhow::Result<MilestonePayload> {
    let mut doc = bson.to_document()?;
    Ok(MilestonePayload::new(
        milestone_essence_from_bson(doc.take("essence")?)?,
        doc.take_array("signatures")?
            .into_iter()
            .map(|u| bytes_from_bson(u).and_then(|v| v.as_slice().try_into().map_err(|e| anyhow!("{}", e))))
            .collect::<Result<Vec<_>, _>>()?,
    )?)
}

fn tag_payload_to_bson(payload: &TaggedDataPayload) -> Bson {
    let mut doc = Document::new();
    doc.insert("kind", TaggedDataPayload::KIND as i32);
    doc.insert("tag", hex::encode(payload.tag()));
    doc.insert("data", hex::encode(payload.data()));
    Bson::Document(doc)
}

fn tag_payload_from_bson(bson: Bson) -> anyhow::Result<TaggedDataPayload> {
    let doc = bson.to_document()?;
    Ok(TaggedDataPayload::new(
        hex::decode(doc.get_str("tag")?)?,
        hex::decode(doc.get_str("data")?)?,
    )?)
}

fn receipt_payload_to_bson(payload: &ReceiptPayload) -> Bson {
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
                doc.insert("amount", f.amount() as i64);
                doc
            })
            .collect::<Vec<_>>(),
    );
    doc.insert("transaction", treasury_payload_to_bson(payload.transaction()));
    doc.insert("amount", payload.amount() as i64);
    Bson::Document(doc)
}

fn receipt_payload_from_bson(bson: Bson) -> anyhow::Result<ReceiptPayload> {
    let mut doc = bson.to_document()?;
    Ok(ReceiptPayload::new(
        MilestoneIndex(doc.get_i32("migrated_at")? as u32),
        doc.get_bool("last")?,
        doc.take_array("funds")?
            .into_iter()
            .map(|f| migrated_funds_entry_from_bson(f))
            .collect::<Result<Vec<_>, _>>()?,
        treasury_payload_from_bson(doc.take("transaction")?)?,
    )?)
}

fn treasury_payload_to_bson(payload: &TreasuryTransactionPayload) -> Bson {
    let mut doc = Document::new();
    doc.insert("kind", TreasuryTransactionPayload::KIND as i32);
    doc.insert("input", treasury_input_to_bson(payload.input()));
    doc.insert("output", treasury_output_to_bson(payload.output()));
    Bson::Document(doc)
}

fn treasury_payload_from_bson(bson: Bson) -> anyhow::Result<TreasuryTransactionPayload> {
    let mut doc = bson.to_document()?;
    Ok(TreasuryTransactionPayload::new(
        treasury_input_from_bson(doc.take("input")?)?,
        treasury_output_from_bson(doc.take("output")?)?,
    )?)
}

fn transaction_essence_to_bson(essence: &TransactionEssence) -> Bson {
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

fn transaction_essence_from_bson(bson: Bson) -> anyhow::Result<TransactionEssence> {
    let mut doc = bson.to_document()?;
    let mut builder = RegularTransactionEssence::builder(
        doc.get_i64("network_id")? as u64,
        hex::decode(doc.get_str("inputs_commitment")?)?.as_slice().try_into()?,
    )
    .with_inputs(
        doc.take_array("inputs")?
            .into_iter()
            .map(|i| input_from_bson(i))
            .collect::<Result<Vec<_>, _>>()?,
    )
    .with_outputs(
        doc.take_array("outputs")?
            .into_iter()
            .map(|o| output_from_bson(o))
            .collect::<Result<Vec<_>, _>>()?,
    );
    if let Some(payload) = doc.take("payload").ok().map(|r| payload_from_bson(r)).transpose()? {
        builder = builder.with_payload(payload);
    }
    Ok(builder.finish()?.into())
}

fn milestone_essence_to_bson(essence: &MilestoneEssence) -> Bson {
    let mut doc = Document::new();
    doc.insert("index", essence.index().0 as i32);
    doc.insert("timestamp", DateTime::from_millis(essence.timestamp() as i64 * 1000));
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

fn milestone_essence_from_bson(bson: Bson) -> anyhow::Result<MilestoneEssence> {
    let mut doc = bson.to_document()?;
    Ok(MilestoneEssence::new(
        MilestoneIndex(doc.get_i32("index")? as u32),
        (doc.get_datetime("timestamp")?.timestamp_millis() as u64) / 1000,
        Parents::new(
            doc.take_array("parents")?
                .into_iter()
                .map(|p| message_id_from_bson(p))
                .collect::<Result<Vec<_>, _>>()?,
        )?,
        hex::decode(doc.get_str("merkle_proof")?)?.as_slice().try_into()?,
        doc.get_i64("next_pow_score")? as u32,
        doc.get_i32("next_pow_score_milestone_index")? as u32,
        doc.take_array("public_keys")?
            .into_iter()
            .map(|p| bytes_from_bson(p).and_then(|v| v.as_slice().try_into().map_err(|e| anyhow!("{}", e))))
            .collect::<Result<Vec<_>, _>>()?,
        doc.take("receipt").ok().map(|r| payload_from_bson(r)).transpose()?,
    )?)
}

fn unlock_block_to_bson(unlock_block: &UnlockBlock) -> Bson {
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

fn unlock_block_from_bson(bson: Bson) -> anyhow::Result<UnlockBlock> {
    let doc = bson.to_document()?;
    let kind = doc.get_i32("kind")? as u8;
    Ok(match kind {
        SignatureUnlockBlock::KIND => {
            UnlockBlock::Signature(SignatureUnlockBlock::new(Signature::Ed25519(Ed25519Signature::new(
                hex::decode(doc.get_str("public_key")?)?.as_slice().try_into()?,
                hex::decode(doc.get_str("signature")?)?.as_slice().try_into()?,
            ))))
        }
        ReferenceUnlockBlock::KIND => UnlockBlock::Reference(ReferenceUnlockBlock::new(doc.get_i32("index")? as u16)?),
        AliasUnlockBlock::KIND => UnlockBlock::Alias(AliasUnlockBlock::new(doc.get_i32("index")? as u16)?),
        NftUnlockBlock::KIND => UnlockBlock::Nft(NftUnlockBlock::new(doc.get_i32("index")? as u16)?),
        _ => bail!("Invalid unlock block"),
    })
}

fn input_to_bson(input: &Input) -> Bson {
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

fn input_from_bson(bson: Bson) -> anyhow::Result<Input> {
    let doc = bson.to_document()?;
    let kind = doc.get_i32("kind")? as u8;
    Ok(match kind {
        UtxoInput::KIND => Input::Utxo(UtxoInput::new(
            TransactionId::from_str(doc.get_str("transaction_id")?)?,
            doc.get_i32("index")? as u16,
        )?),
        TreasuryInput::KIND => {
            Input::Treasury(TreasuryInput::new(MilestoneId::from_str(doc.get_str("milestone_id")?)?))
        }
        _ => bail!("Invalid input"),
    })
}

fn treasury_input_to_bson(input: &TreasuryInput) -> Bson {
    let mut doc = Document::new();
    doc.insert("kind", TreasuryInput::KIND as i32);
    doc.insert("milestone_id", input.milestone_id().to_string());
    Bson::Document(doc)
}

fn treasury_input_from_bson(bson: Bson) -> anyhow::Result<TreasuryInput> {
    let doc = bson.to_document()?;
    Ok(TreasuryInput::new(MilestoneId::from_str(doc.get_str("milestone_id")?)?))
}

fn output_to_bson(output: &Output) -> Bson {
    let mut doc = Document::new();
    match output {
        Output::Treasury(t) => {
            doc.insert("kind", TreasuryOutput::KIND as i32);
            doc.insert("amount", t.amount() as i64);
        }
        Output::Basic(b) => {
            doc.insert("kind", BasicOutput::KIND as i32);
            doc.insert("amount", b.amount() as i64);
            doc.insert("address", address_to_bson(b.address()));
            doc.insert("native_tokens", to_bson(b.native_tokens()).unwrap());
            doc.insert("unlock_conditions", to_bson(b.unlock_conditions()).unwrap());
            doc.insert("feature_blocks", to_bson(b.feature_blocks()).unwrap());
        }
        Output::Alias(a) => {
            doc.insert("kind", AliasOutput::KIND as i32);
            doc.insert("alias_id", a.alias_id().to_string());
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
            doc.insert("foundry_id", f.id().to_string());
            doc.insert("amount", f.amount() as i64);
            doc.insert("native_tokens", to_bson(f.native_tokens()).unwrap());
            doc.insert("serial_number", f.serial_number() as i32);
            doc.insert("token_tag", f.token_tag().to_string());
            doc.insert("token_scheme", to_bson(f.token_scheme()).unwrap());
            doc.insert("unlock_conditions", to_bson(f.unlock_conditions()).unwrap());
            doc.insert("feature_blocks", to_bson(f.feature_blocks()).unwrap());
            doc.insert(
                "immutable_feature_blocks",
                to_bson(f.immutable_feature_blocks()).unwrap(),
            );
        }
        Output::Nft(n) => {
            doc.insert("kind", NftOutput::KIND as i32);
            doc.insert("nft_id", n.nft_id().to_string());
            doc.insert("amount", n.amount() as i64);
            doc.insert("address", address_to_bson(n.address()));
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

fn output_from_bson(bson: Bson) -> anyhow::Result<Output> {
    let doc = bson.to_document()?;
    let kind = doc.get_i32("kind")? as u8;
    Ok(match kind {
        TreasuryOutput::KIND => Output::Treasury(TreasuryOutput::new(doc.get_i64("amount")? as u64)?),
        BasicOutput::KIND => Output::Basic(
            BasicOutput::build(doc.get_i64("amount")? as u64)?
                .with_native_tokens(from_bson::<NativeTokens>(
                    doc.get("native_tokens")
                        .ok_or_else(|| anyhow!("Missing native tokens"))?
                        .clone(),
                )?)
                .with_unlock_conditions(from_bson::<UnlockConditions>(
                    doc.get("unlock_conditions")
                        .ok_or_else(|| anyhow!("Missing unlock conditions"))?
                        .clone(),
                )?)
                .with_feature_blocks(from_bson::<FeatureBlocks>(
                    doc.get("feature_blocks")
                        .ok_or_else(|| anyhow!("Missing feature blocks"))?
                        .clone(),
                )?)
                .finish()?,
        ),
        AliasOutput::KIND => Output::Alias(
            AliasOutput::build(
                doc.get_i64("amount")? as u64,
                AliasId::from_str(doc.get_str("alias_id")?)?,
            )?
            .with_native_tokens(from_bson::<NativeTokens>(
                doc.get("native_tokens")
                    .ok_or_else(|| anyhow!("Missing native tokens"))?
                    .clone(),
            )?)
            .with_state_index(doc.get_i32("state_index")? as u32)
            .with_state_metadata(hex::decode(doc.get_str("state_metadata")?)?)
            .with_foundry_counter(doc.get_i32("foundry_counter")? as u32)
            .with_unlock_conditions(from_bson::<UnlockConditions>(
                doc.get("unlock_conditions")
                    .ok_or_else(|| anyhow!("Missing unlock conditions"))?
                    .clone(),
            )?)
            .with_feature_blocks(from_bson::<FeatureBlocks>(
                doc.get("feature_blocks")
                    .ok_or_else(|| anyhow!("Missing feature blocks"))?
                    .clone(),
            )?)
            .with_immutable_feature_blocks(from_bson::<FeatureBlocks>(
                doc.get("immutable_feature_blocks")
                    .ok_or_else(|| anyhow!("Missing immutable feature blocks"))?
                    .clone(),
            )?)
            .finish()?,
        ),
        FoundryOutput::KIND => Output::Foundry(
            FoundryOutput::build(
                doc.get_i64("amount")? as u64,
                doc.get_i32("serial_number")? as u32,
                TokenTag::from_str(doc.get_str("token_tag")?)?,
                from_bson(
                    doc.get("token_scheme")
                        .ok_or_else(|| anyhow!("Missing token scheme"))?
                        .clone(),
                )?,
            )?
            .with_native_tokens(from_bson::<NativeTokens>(
                doc.get("native_tokens")
                    .ok_or_else(|| anyhow!("Missing native tokens"))?
                    .clone(),
            )?)
            .with_unlock_conditions(from_bson::<UnlockConditions>(
                doc.get("unlock_conditions")
                    .ok_or_else(|| anyhow!("Missing unlock conditions"))?
                    .clone(),
            )?)
            .with_feature_blocks(from_bson::<FeatureBlocks>(
                doc.get("feature_blocks")
                    .ok_or_else(|| anyhow!("Missing feature blocks"))?
                    .clone(),
            )?)
            .with_immutable_feature_blocks(from_bson::<FeatureBlocks>(
                doc.get("immutable_feature_blocks")
                    .ok_or_else(|| anyhow!("Missing immutable feature blocks"))?
                    .clone(),
            )?)
            .finish()?,
        ),
        NftOutput::KIND => Output::Nft(
            NftOutput::build(doc.get_i64("amount")? as u64, NftId::from_str(doc.get_str("nft_id")?)?)?
                .with_native_tokens(from_bson::<NativeTokens>(
                    doc.get("native_tokens")
                        .ok_or_else(|| anyhow!("Missing native tokens"))?
                        .clone(),
                )?)
                .with_unlock_conditions(from_bson::<UnlockConditions>(
                    doc.get("unlock_conditions")
                        .ok_or_else(|| anyhow!("Missing unlock conditions"))?
                        .clone(),
                )?)
                .with_feature_blocks(from_bson::<FeatureBlocks>(
                    doc.get("feature_blocks")
                        .ok_or_else(|| anyhow!("Missing feature blocks"))?
                        .clone(),
                )?)
                .with_immutable_feature_blocks(from_bson::<FeatureBlocks>(
                    doc.get("immutable_feature_blocks")
                        .ok_or_else(|| anyhow!("Missing immutable feature blocks"))?
                        .clone(),
                )?)
                .finish()?,
        ),
        _ => bail!("Invalid output"),
    })
}

fn treasury_output_to_bson(o: &TreasuryOutput) -> Bson {
    let mut doc = Document::new();
    doc.insert("kind", TreasuryOutput::KIND as i32);
    doc.insert("amount", o.amount() as i64);
    Bson::Document(doc)
}

fn treasury_output_from_bson(bson: Bson) -> anyhow::Result<TreasuryOutput> {
    let doc = bson.to_document()?;
    let amount = doc.get_i64("amount")?;
    Ok(TreasuryOutput::new(amount as u64)?)
}

fn address_to_bson(address: &Address) -> Bson {
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

fn address_from_bson(bson: Bson) -> anyhow::Result<Address> {
    let doc = bson.to_document()?;
    let kind = doc.get_i32("kind")? as u8;
    Ok(match kind {
        Ed25519Address::KIND => Address::Ed25519(Ed25519Address::from_str(doc.get_str("data")?)?),
        AliasAddress::KIND => Address::Alias(AliasAddress::from_str(doc.get_str("data")?)?),
        NftAddress::KIND => Address::Nft(NftAddress::from_str(doc.get_str("data")?)?),
        _ => bail!("Invalid address"),
    })
}

fn bytes_from_bson(bson: Bson) -> anyhow::Result<Vec<u8>> {
    let hex = bson.as_str().ok_or_else(|| anyhow!("Invalid bytes hex"))?;
    Ok(hex::decode(hex)?)
}

fn message_id_from_bson(bson: Bson) -> anyhow::Result<MessageId> {
    let s = bson.as_str().ok_or_else(|| anyhow!("Invalid message id"))?;
    Ok(MessageId::from_str(s)?)
}

fn migrated_funds_entry_from_bson(bson: Bson) -> anyhow::Result<MigratedFundsEntry> {
    let mut doc = bson.to_document()?;
    Ok(MigratedFundsEntry::new(
        TailTransactionHash::new(
            hex::decode(doc.get_str("tail_transaction_hash")?)?
                .as_slice()
                .try_into()?,
        )?,
        address_from_bson(doc.take("address")?)?,
        doc.get_i64("amount")? as u64,
    )?)
}
