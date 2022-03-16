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
                .storage_deposit_return()
                .map(|u| *u.return_address()),
            sender: data.feature_blocks().sender().map(|fb| *fb.address()),
            issuer: data.feature_blocks().issuer().map(|fb| *fb.address()),
            tag: data
                .feature_blocks()
                .tag()
                .map(|fb| String::from_utf8_lossy(fb.tag()).into_owned()),
            data,
        }
    }
}
