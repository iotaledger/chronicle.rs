// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use crate::cpt2::{
    prelude::*,
    Message,
};
use anyhow::{
    anyhow,
    bail,
};
use mongodb::bson::{
    doc,
    Bson,
    DateTime,
    Document,
};
use std::str::FromStr;

pub fn message_to_bson(message: &Message) -> Bson {
    let mut doc = Document::new();
    doc.insert("protocol_version", 0);
    doc.insert("network_id", message.network_id() as i64);
    doc.insert(
        "parents",
        message.parents().iter().map(|p| p.to_string()).collect::<Vec<_>>(),
    );
    doc.insert("payload", message.payload().as_ref().map(|p| payload_to_bson(p)));
    doc.insert("nonce", message.nonce().to_string());
    Bson::Document(doc)
}

pub fn message_from_doc(doc: &Document) -> anyhow::Result<Message> {
    let mut builder = MessageBuilder::default()
        .with_network_id(doc.get_i64("network_id")? as u64)
        .with_parents(Parents::new(
            doc.get_array("parents")?
                .iter()
                .map(|p| message_id_from_bson(p))
                .collect::<Result<Vec<_>, _>>()?,
        )?)
        .with_nonce_provider(doc.get_str("nonce")?.parse::<u64>()?, 0.0);
    if let Some(payload) = doc
        .get_document("payload")
        .ok()
        .map(|r| payload_from_doc(&r))
        .transpose()?
    {
        builder = builder.with_payload(payload);
    }
    Ok(builder.finish()?)
}

fn payload_to_bson(payload: &Payload) -> Bson {
    match payload {
        Payload::Transaction(t) => transaction_payload_to_bson(&**t),
        Payload::Milestone(m) => milestone_payload_to_bson(&**m),
        Payload::Indexation(i) => indexation_payload_to_bson(&**i),
        Payload::Receipt(r) => receipt_payload_to_bson(&**r),
        Payload::TreasuryTransaction(t) => treasury_payload_to_bson(&**t),
    }
}

pub fn payload_from_doc(doc: &Document) -> anyhow::Result<Payload> {
    let kind = doc.get_i32("kind")? as u32;
    Ok(match kind {
        TransactionPayload::KIND => Payload::Transaction(Box::new(transaction_payload_from_doc(doc)?)),
        MilestonePayload::KIND => Payload::Milestone(Box::new(milestone_payload_from_doc(doc)?)),
        IndexationPayload::KIND => Payload::Indexation(Box::new(indexation_payload_from_doc(doc)?)),
        ReceiptPayload::KIND => Payload::Receipt(Box::new(receipt_payload_from_doc(doc)?)),
        TreasuryTransactionPayload::KIND => Payload::TreasuryTransaction(Box::new(treasury_payload_from_doc(doc)?)),
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

fn transaction_payload_from_doc(doc: &Document) -> anyhow::Result<TransactionPayload> {
    Ok(TransactionPayload::builder()
        .with_essence(transaction_essence_from_doc(doc.get_document("essence")?)?)
        .with_unlock_blocks(UnlockBlocks::new(
            doc.get_array("unlock_blocks")?
                .iter()
                .map(|u| unlock_block_from_bson(u))
                .collect::<Result<Vec<_>, _>>()?,
        )?)
        .finish()?)
}

fn milestone_payload_to_bson(payload: &MilestonePayload) -> Bson {
    let mut doc = Document::new();
    doc.insert("kind", MilestonePayload::KIND as i32);
    doc.insert("milestone_id", payload.id().to_string());
    doc.insert("essence", milestone_essence_to_bson(&payload.essence()));
    doc.insert(
        "signatures",
        payload.signatures().iter().map(|s| hex::encode(s)).collect::<Vec<_>>(),
    );
    Bson::Document(doc)
}

fn milestone_payload_from_doc(doc: &Document) -> anyhow::Result<MilestonePayload> {
    Ok(MilestonePayload::new(
        milestone_essence_from_doc(doc.get_document("essence")?)?,
        doc.get_array("signatures")?
            .iter()
            .map(|u| bytes_from_bson(u).and_then(|v| v.as_slice().try_into().map_err(|e| anyhow!("{}", e))))
            .collect::<Result<Vec<_>, _>>()?,
    )?)
}

fn indexation_payload_to_bson(payload: &IndexationPayload) -> Bson {
    let mut doc = Document::new();
    doc.insert("kind", IndexationPayload::KIND as i32);
    doc.insert("index", hex::encode(payload.index()));
    doc.insert("data", hex::encode(payload.data()));
    Bson::Document(doc)
}

fn indexation_payload_from_doc(doc: &Document) -> anyhow::Result<IndexationPayload> {
    Ok(IndexationPayload::new(
        hex::decode(doc.get_str("index")?)?.as_slice(),
        hex::decode(doc.get_str("data")?)?.as_slice(),
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
                doc.insert(
                    "output",
                    doc! {
                        "kind": SignatureLockedSingleOutput::KIND as i32,
                        "address": address_to_bson(f.output().address()),
                        "amount": f.output().amount() as i64,
                    },
                );
                doc
            })
            .collect::<Vec<_>>(),
    );
    doc.insert("transaction", payload_to_bson(payload.transaction()));
    doc.insert("amount", payload.amount() as i64);
    Bson::Document(doc)
}

fn receipt_payload_from_doc(doc: &Document) -> anyhow::Result<ReceiptPayload> {
    Ok(ReceiptPayload::new(
        MilestoneIndex(doc.get_i32("migrated_at")? as u32),
        doc.get_bool("last")?,
        doc.get_array("funds")?
            .iter()
            .map(|f| migrated_funds_entry_from_bson(f))
            .collect::<Result<Vec<_>, _>>()?,
        payload_from_doc(doc.get_document("transaction")?)?,
    )?)
}

fn treasury_payload_to_bson(payload: &TreasuryTransactionPayload) -> Bson {
    let mut doc = Document::new();
    doc.insert("kind", TreasuryTransactionPayload::KIND as i32);
    doc.insert("input", input_to_bson(payload.input()));
    doc.insert("output", output_to_bson(payload.output()));
    Bson::Document(doc)
}

fn treasury_payload_from_doc(doc: &Document) -> anyhow::Result<TreasuryTransactionPayload> {
    Ok(TreasuryTransactionPayload::new(
        input_from_doc(doc.get_document("input")?)?,
        output_from_doc(doc.get_document("output")?)?,
    )?)
}

fn transaction_essence_to_bson(essence: &Essence) -> Bson {
    let mut doc = Document::new();
    match essence {
        Essence::Regular(r) => {
            doc.insert(
                "inputs",
                r.inputs().iter().map(|i| input_to_bson(i)).collect::<Vec<_>>(),
            );
            doc.insert(
                "outputs",
                r.outputs().iter().map(|o| output_to_bson(o)).collect::<Vec<_>>(),
            );
            doc.insert("payload", r.payload().as_ref().map(|p| payload_to_bson(p)));
        }
    }
    Bson::Document(doc)
}

fn transaction_essence_from_doc(doc: &Document) -> anyhow::Result<Essence> {
    let mut builder = RegularEssence::builder()
        .with_inputs(
            doc.get_array("inputs")?
                .iter()
                .map(|i| input_from_bson(i))
                .collect::<Result<Vec<_>, _>>()?,
        )
        .with_outputs(
            doc.get_array("outputs")?
                .iter()
                .map(|o| output_from_bson(o))
                .collect::<Result<Vec<_>, _>>()?,
        );
    if let Some(payload) = doc
        .get_document("payload")
        .ok()
        .map(|r| payload_from_doc(&r))
        .transpose()?
    {
        builder = builder.with_payload(payload);
    }
    Ok(builder.finish()?.into())
}

fn milestone_essence_to_bson(essence: &MilestonePayloadEssence) -> Bson {
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

fn milestone_essence_from_doc(doc: &Document) -> anyhow::Result<MilestonePayloadEssence> {
    Ok(MilestonePayloadEssence::new(
        MilestoneIndex(doc.get_i32("index")? as u32),
        doc.get_datetime("timestamp")?.timestamp_millis() as u64,
        Parents::new(
            doc.get_array("parents")?
                .iter()
                .map(|p| message_id_from_bson(p))
                .collect::<Result<Vec<_>, _>>()?,
        )?,
        hex::decode(doc.get_str("merkle_proof")?)?.as_slice().try_into()?,
        doc.get_i64("next_pow_score")? as u32,
        doc.get_i32("next_pow_score_milestone_index")? as u32,
        doc.get_array("public_keys")?
            .iter()
            .map(|p| bytes_from_bson(p).and_then(|v| v.as_slice().try_into().map_err(|e| anyhow!("{}", e))))
            .collect::<Result<Vec<_>, _>>()?,
        doc.get_document("receipt")
            .ok()
            .map(|r| payload_from_doc(&r))
            .transpose()?,
    )?)
}

fn unlock_block_to_bson(unlock_block: &UnlockBlock) -> Bson {
    let mut doc = Document::new();
    match unlock_block {
        UnlockBlock::Signature(s) => match s {
            SignatureUnlock::Ed25519(s) => {
                doc.insert("kind", SignatureUnlock::KIND as i32);
                doc.insert("public_key", hex::encode(s.public_key()));
                doc.insert("signature", hex::encode(s.signature()));
            }
        },
        UnlockBlock::Reference(r) => {
            doc.insert("kind", ReferenceUnlock::KIND as i32);
            doc.insert("index", r.index() as i32);
        }
    }
    Bson::Document(doc)
}

fn unlock_block_from_bson(bson: &Bson) -> anyhow::Result<UnlockBlock> {
    let doc = bson.as_document().ok_or_else(|| anyhow!("Invalid unlock block"))?;
    let kind = doc.get_i32("kind")? as u8;
    Ok(match kind {
        SignatureUnlock::KIND => UnlockBlock::Signature(SignatureUnlock::Ed25519(Ed25519Signature::new(
            hex::decode(doc.get_str("public_key")?)?.as_slice().try_into()?,
            hex::decode(doc.get_str("signature")?)?.as_slice().try_into()?,
        ))),
        ReferenceUnlock::KIND => UnlockBlock::Reference(ReferenceUnlock::new(doc.get_i32("index")? as u16)?),
        _ => bail!("Invalid unlock block"),
    })
}

fn input_to_bson(input: &crate::cpt2::input::Input) -> Bson {
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

fn input_from_bson(bson: &Bson) -> anyhow::Result<crate::cpt2::input::Input> {
    input_from_doc(bson.as_document().ok_or_else(|| anyhow!("Invalid input"))?)
}

fn input_from_doc(doc: &Document) -> anyhow::Result<Input> {
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

fn output_to_bson(output: &Output) -> Bson {
    let mut doc = Document::new();
    match output {
        Output::SignatureLockedSingle(s) => {
            doc.insert("kind", SignatureLockedSingleOutput::KIND as i32);
            doc.insert("address", address_to_bson(s.address()));
            doc.insert("amount", s.amount() as i64);
        }
        Output::SignatureLockedDustAllowance(d) => {
            doc.insert("kind", SignatureLockedDustAllowanceOutput::KIND as i32);
            doc.insert("address", address_to_bson(d.address()));
            doc.insert("amount", d.amount() as i64);
        }
        Output::Treasury(t) => {
            doc.insert("kind", TreasuryOutput::KIND as i32);
            doc.insert("amount", t.amount() as i64);
        }
    }
    Bson::Document(doc)
}

fn output_from_bson(bson: &Bson) -> anyhow::Result<Output> {
    output_from_doc(bson.as_document().ok_or_else(|| anyhow!("Invalid output"))?)
}

fn output_from_doc(doc: &Document) -> anyhow::Result<Output> {
    let kind = doc.get_i32("kind")? as u8;
    Ok(match kind {
        SignatureLockedSingleOutput::KIND => Output::SignatureLockedSingle(SignatureLockedSingleOutput::new(
            address_from_doc(doc.get_document("address")?)?,
            doc.get_i64("amount")? as u64,
        )?),
        SignatureLockedDustAllowanceOutput::KIND => {
            Output::SignatureLockedDustAllowance(SignatureLockedDustAllowanceOutput::new(
                address_from_doc(doc.get_document("address")?)?,
                doc.get_i64("amount")? as u64,
            )?)
        }
        TreasuryOutput::KIND => Output::Treasury(TreasuryOutput::new(doc.get_i64("amount")? as u64)?),
        _ => bail!("Invalid output"),
    })
}

fn address_to_bson(address: &Address) -> Bson {
    let mut doc = Document::new();
    match address {
        Address::Ed25519(a) => {
            doc.insert("kind", Ed25519Address::KIND as i32);
            doc.insert("data", a.to_string());
        }
    }
    Bson::Document(doc)
}

fn address_from_bson(bson: &Bson) -> anyhow::Result<Address> {
    address_from_doc(bson.as_document().ok_or_else(|| anyhow!("Invalid address"))?)
}

fn address_from_doc(doc: &Document) -> anyhow::Result<Address> {
    let kind = doc.get_i32("kind")? as u8;
    Ok(match kind {
        Ed25519Address::KIND => Address::Ed25519(Ed25519Address::from_str(doc.get_str("data")?)?),
        _ => bail!("Invalid address"),
    })
}

fn bytes_from_bson(bson: &Bson) -> anyhow::Result<Vec<u8>> {
    let hex = bson.as_str().ok_or_else(|| anyhow!("Invalid bytes hex"))?;
    Ok(hex::decode(hex)?)
}

fn message_id_from_bson(bson: &Bson) -> anyhow::Result<MessageId> {
    let s = bson.as_str().ok_or_else(|| anyhow!("Invalid message id"))?;
    Ok(MessageId::from_str(s)?)
}

fn migrated_funds_entry_from_bson(bson: &Bson) -> anyhow::Result<MigratedFundsEntry> {
    let doc = bson.as_document().ok_or_else(|| anyhow!("Invalid funds"))?;
    let output = doc.get_document("output")?;
    Ok(MigratedFundsEntry::new(
        TailTransactionHash::new(
            hex::decode(doc.get_str("tail_transaction_hash")?)?
                .as_slice()
                .try_into()?,
        )?,
        SignatureLockedSingleOutput::new(
            address_from_bson(output.get("address").ok_or_else(|| anyhow!("Missing address"))?)?,
            output.get_i64("amount")? as u64,
        )?,
    )?)
}
