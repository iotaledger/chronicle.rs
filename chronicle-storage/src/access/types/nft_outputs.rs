// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;

/// An `NFT Output` table record
#[allow(missing_docs)]
#[derive(Clone, Debug)]
pub struct NftOutputRecord {
    pub nft_id: NftId,
    pub partition_data: PartitionData,
    pub inclusion_state: Option<LedgerInclusionState>,
    pub address: Address,
    pub dust_return_address: Option<Address>,
    pub sender: Option<Address>,
    pub issuer: Option<Address>,
    pub tag: Option<String>,
    pub data: NftOutput,
}

impl NftOutputRecord {
    pub fn new(
        nft_id: NftId,
        partition_data: PartitionData,
        inclusion_state: Option<LedgerInclusionState>,
        data: NftOutput,
    ) -> Self {
        Self {
            nft_id,
            partition_data,
            inclusion_state,
            address: *data.address(),
            dust_return_address: data
                .unlock_conditions()
                .binary_search_by_key(&StorageDepositReturnUnlockCondition::KIND, UnlockCondition::kind)
                .ok()
                .and_then(|idx| {
                    if let UnlockCondition::StorageDepositReturn(c) = &data.unlock_conditions()[idx] {
                        Some(*c.return_address())
                    } else {
                        None
                    }
                }),
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

impl Partitioned for NftOutputRecord {
    const MS_CHUNK_SIZE: u32 = BasicOutputRecord::MS_CHUNK_SIZE;
}

impl Row for NftOutputRecord {
    fn try_decode_row<R: Rows + ColumnValue>(rows: &mut R) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        Ok(Self {
            nft_id: rows.column_value::<Bee<NftId>>()?.into_inner(),
            partition_data: PartitionData::new(
                rows.column_value()?,
                rows.column_value::<Bee<MilestoneIndex>>()?.into_inner(),
                rows.column_value()?,
            ),
            inclusion_state: rows.column_value()?,
            address: rows.column_value::<Bee<Address>>()?.into_inner(),
            dust_return_address: rows.column_value::<Option<Bee<Address>>>()?.map(|a| a.into_inner()),
            sender: rows.column_value::<Option<Bee<Address>>>()?.map(|a| a.into_inner()),
            issuer: rows.column_value::<Option<Bee<Address>>>()?.map(|a| a.into_inner()),
            tag: rows.column_value()?,
            data: rows.column_value::<Bee<NftOutput>>()?.into_inner(),
        })
    }
}

impl TokenEncoder for NftOutputRecord {
    fn encode_token(&self) -> TokenEncodeChain {
        (&Bee(&self.nft_id)).into()
    }
}

impl<B: Binder> Bindable<B> for NftOutputRecord {
    fn bind(&self, binder: B) -> B {
        binder
            .value(Bee(self.nft_id))
            .bind(self.partition_data)
            .value(self.inclusion_state.as_ref().map(|l| *l as u8))
            .value(Bee(self.address))
            .value(self.dust_return_address.as_ref().map(Bee))
            .value(self.sender.as_ref().map(Bee))
            .value(self.issuer.as_ref().map(Bee))
            .value(&self.tag)
            .value(Bee(&self.data))
    }
}
