// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;

/// Address hint, used to lookup in the `addresses hints` table
#[derive(Clone, Debug)]
pub struct AddressHint {
    /// The address
    pub address: Address,
    pub output_kind: OutputVariant,
    /// The tag hint variant. Can be 'Regular', 'ExtOutput', or 'NftOutput'.
    pub variant: AddressHintVariant,
}

impl AddressHint {
    /// Creates address hint
    pub fn new(address: Address, output_kind: OutputVariant, variant: AddressHintVariant) -> Self {
        Self {
            address,
            output_kind,
            variant,
        }
    }
    /// Get the address
    pub fn address(&self) -> &Address {
        &self.address
    }
    /// Get the address variant
    pub fn variant(&self) -> &AddressHintVariant {
        &self.variant
    }
    pub fn output_kind(&self) -> &OutputVariant {
        &self.output_kind
    }
}

impl TokenEncoder for AddressHint {
    fn encode_token(&self) -> TokenEncodeChain {
        (&Bee(&self.address)).into()
    }
}

impl<B: Binder> Bindable<B> for AddressHint {
    fn bind(&self, binder: B) -> B {
        binder
            .value(Bee(self.address))
            .value(&self.output_kind)
            .value(&self.variant)
    }
}

/// Hint variants
#[derive(Clone, Debug)]
pub enum AddressHintVariant {
    Address,
    Sender,
    Issuer,
    StateController,
    Governor,
}

impl std::fmt::Display for AddressHintVariant {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                Self::Address => "Address",
                Self::Sender => "Sender",
                Self::Issuer => "Issuer",
                Self::StateController => "StateController",
                Self::Governor => "Governor",
            }
        )
    }
}

impl ColumnEncoder for AddressHintVariant {
    fn encode(&self, buffer: &mut Vec<u8>) {
        self.to_string().encode(buffer)
    }
}

#[derive(Clone, Debug)]
pub enum OutputVariant {
    Legacy,
    Extended,
    Alias,
    Foundry,
    Nft,
}

impl std::fmt::Display for OutputVariant {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                Self::Legacy => "Legacy",
                Self::Extended => "Extended",
                Self::Alias => "Alias",
                Self::Nft => "Nft",
                Self::Foundry => "Foundry",
            }
        )
    }
}

impl ColumnEncoder for OutputVariant {
    fn encode(&self, buffer: &mut Vec<u8>) {
        self.to_string().encode(buffer)
    }
}
