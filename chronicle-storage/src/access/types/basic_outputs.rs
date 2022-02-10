// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;

/// A `Basic Output` table row
#[allow(missing_docs)]
#[derive(Clone, Debug)]
pub struct BasicOutputRecord {
    pub output_id: OutputId,
    pub partition_data: PartitionData,
    pub inclusion_state: Option<LedgerInclusionState>,
    pub address: Address,
    pub sender: Option<Address>,
    pub tag: Option<String>,
    pub data: BasicOutput,
}

impl BasicOutputRecord {
    pub fn new(
        output_id: OutputId,
        partition_data: PartitionData,
        inclusion_state: Option<LedgerInclusionState>,
        data: BasicOutput,
    ) -> Self {
        Self {
            output_id,
            partition_data,
            inclusion_state,
            address: *data.address(),
            sender: data
                .feature_blocks()
                .binary_search_by_key(&SenderFeatureBlock::KIND, FeatureBlock::kind)
                .ok()
                .and_then(|idx| {
                    if let FeatureBlock::Sender(fb) = &data.feature_blocks()[idx] {
                        Some(*fb.address())
                    } else {
                        None
                    }
                }),
            tag: data
                .feature_blocks()
                .binary_search_by_key(&TagFeatureBlock::KIND, FeatureBlock::kind)
                .ok()
                .and_then(|idx| {
                    if let FeatureBlock::Tag(fb) = &data.feature_blocks()[idx] {
                        Some(String::from_utf8_lossy(fb.tag()).into_owned())
                    } else {
                        None
                    }
                }),
            data,
        }
    }
}

impl Partitioned for BasicOutputRecord {
    const MS_CHUNK_SIZE: u32 = 250_000;
}

impl Row for BasicOutputRecord {
    fn try_decode_row<R: Rows + ColumnValue>(rows: &mut R) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        Ok(Self {
            output_id: rows.column_value::<Bee<OutputId>>()?.into_inner(),
            partition_data: PartitionData::new(
                rows.column_value()?,
                rows.column_value::<Bee<MilestoneIndex>>()?.into_inner(),
                rows.column_value()?,
            ),
            inclusion_state: rows.column_value()?,
            address: rows.column_value::<Bee<Address>>()?.into_inner(),
            sender: rows.column_value::<Option<Bee<Address>>>()?.map(|a| a.into_inner()),
            tag: rows.column_value()?,
            data: rows.column_value::<Bee<BasicOutput>>()?.into_inner(),
        })
    }
}

impl TokenEncoder for BasicOutputRecord {
    fn encode_token(&self) -> TokenEncodeChain {
        (&Bee(&self.output_id)).into()
    }
}

impl<B: Binder> Bindable<B> for BasicOutputRecord {
    fn bind(&self, binder: B) -> B {
        binder
            .value(Bee(self.output_id))
            .bind(self.partition_data)
            .value(self.inclusion_state.as_ref().map(|l| *l as u8))
            .value(Bee(self.address))
            .value(self.sender.as_ref().map(|a| Bee(a)))
            .value(&self.tag)
            .value(Bee(&self.data))
    }
}
