// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;

/// An `Alias Output` table row
#[allow(missing_docs)]
#[derive(Clone, Debug)]
pub struct AliasOutputRecord {
    pub alias_id: AliasId,
    pub partition_data: PartitionData,
    pub inclusion_state: Option<LedgerInclusionState>,
    pub sender: Option<Address>,
    pub issuer: Option<Address>,
    pub state_controller: Address,
    pub governor: Address,
    pub data: AliasOutput,
}

impl AliasOutputRecord {
    pub fn new(
        alias_id: AliasId,
        partition_data: PartitionData,
        inclusion_state: Option<LedgerInclusionState>,
        data: AliasOutput,
    ) -> Self {
        Self {
            alias_id,
            partition_data,
            inclusion_state,
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
            issuer: data
                .feature_blocks()
                .binary_search_by_key(&IssuerFeatureBlock::KIND, FeatureBlock::kind)
                .ok()
                .and_then(|idx| {
                    if let FeatureBlock::Issuer(fb) = &data.feature_blocks()[idx] {
                        Some(*fb.address())
                    } else {
                        None
                    }
                }),
            state_controller: *data.state_controller(),
            governor: *data.governor(),
            data,
        }
    }
}

impl Partitioned for AliasOutputRecord {
    const MS_CHUNK_SIZE: u32 = BasicOutputRecord::MS_CHUNK_SIZE;
}

impl Row for AliasOutputRecord {
    fn try_decode_row<R: Rows + ColumnValue>(rows: &mut R) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        Ok(Self {
            alias_id: rows.column_value::<Bee<AliasId>>()?.into_inner(),
            partition_data: PartitionData::new(
                rows.column_value()?,
                rows.column_value::<Bee<MilestoneIndex>>()?.into_inner(),
                rows.column_value()?,
            ),
            inclusion_state: rows.column_value()?,
            sender: rows.column_value::<Option<Bee<Address>>>()?.map(|a| a.into_inner()),
            issuer: rows.column_value::<Option<Bee<Address>>>()?.map(|a| a.into_inner()),
            state_controller: rows.column_value::<Bee<Address>>()?.into_inner(),
            governor: rows.column_value::<Bee<Address>>()?.into_inner(),
            data: rows.column_value::<Bee<AliasOutput>>()?.into_inner(),
        })
    }
}

impl TokenEncoder for AliasOutputRecord {
    fn encode_token(&self) -> TokenEncodeChain {
        (&Bee(&self.alias_id)).into()
    }
}

impl<B: Binder> Bindable<B> for AliasOutputRecord {
    fn bind(&self, binder: B) -> B {
        binder
            .value(Bee(self.alias_id))
            .bind(self.partition_data)
            .value(self.inclusion_state.as_ref().map(|l| *l as u8))
            .value(self.sender.as_ref().map(Bee))
            .value(self.issuer.as_ref().map(Bee))
            .value(Bee(self.state_controller))
            .value(Bee(self.governor))
            .value(Bee(&self.data))
    }
}
