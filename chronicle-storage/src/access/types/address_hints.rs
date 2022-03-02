// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;
use std::fmt::Display;

/// Address hint, used to lookup in the `addresses hints` table
#[derive(Clone, Debug)]
pub struct AddressHint {
    /// The address
    pub address: Address,
    pub output_table: OutputTable,
    /// The tag hint variant. Can be 'Regular', 'ExtOutput', or 'NftOutput'.
    pub variant: AddressHintVariant,
}

impl AddressHint {
    /// Creates address hint
    pub fn new(address: Address, output_kind: OutputTable, variant: AddressHintVariant) -> Self {
        Self {
            address,
            output_table: output_kind,
            variant,
        }
    }
    pub fn legacy_outputs_by_address(address: Address) -> Self {
        Self::new(address, OutputTable::Legacy, AddressHintVariant::Address)
    }
    pub fn basic_outputs_by_address(address: Address) -> Self {
        Self::new(address, OutputTable::Basic, AddressHintVariant::Address)
    }
    pub fn basic_outputs_by_sender(address: Address) -> Self {
        Self::new(address, OutputTable::Basic, AddressHintVariant::Sender)
    }
    pub fn alias_outputs_by_sender(address: Address) -> Self {
        Self::new(address, OutputTable::Alias, AddressHintVariant::Sender)
    }
    pub fn alias_outputs_by_issuer(address: Address) -> Self {
        Self::new(address, OutputTable::Alias, AddressHintVariant::Issuer)
    }
    pub fn alias_outputs_by_state_controller(address: Address) -> Self {
        Self::new(address, OutputTable::Alias, AddressHintVariant::StateController)
    }
    pub fn alias_outputs_by_governor(address: Address) -> Self {
        Self::new(address, OutputTable::Alias, AddressHintVariant::Governor)
    }
    pub fn foundry_outputs_by_address(address: Address) -> Self {
        Self::new(address, OutputTable::Foundry, AddressHintVariant::Address)
    }
    pub fn nft_outputs_by_address(address: Address) -> Self {
        Self::new(address, OutputTable::Nft, AddressHintVariant::Address)
    }
    pub fn nft_outputs_by_dust_return_address(address: Address) -> Self {
        Self::new(address, OutputTable::Nft, AddressHintVariant::DustReturnAddress)
    }
    pub fn nft_outputs_by_sender(address: Address) -> Self {
        Self::new(address, OutputTable::Nft, AddressHintVariant::Sender)
    }
    pub fn nft_outputs_by_issuer(address: Address) -> Self {
        Self::new(address, OutputTable::Nft, AddressHintVariant::Issuer)
    }
    /// Get the address
    pub fn address(&self) -> &Address {
        &self.address
    }
    /// Get the address variant
    pub fn variant(&self) -> &AddressHintVariant {
        &self.variant
    }
    pub fn output_kind(&self) -> &OutputTable {
        &self.output_table
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
            .value(&self.output_table)
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
    DustReturnAddress,
}

impl Display for AddressHintVariant {
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
                Self::DustReturnAddress => "DustReturnAddress",
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
pub enum OutputTable {
    Legacy,
    Basic,
    Alias,
    Foundry,
    Nft,
}

impl Display for OutputTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                Self::Legacy => "Legacy",
                Self::Basic => "Basic",
                Self::Alias => "Alias",
                Self::Nft => "Nft",
                Self::Foundry => "Foundry",
            }
        )
    }
}

impl ColumnEncoder for OutputTable {
    fn encode(&self, buffer: &mut Vec<u8>) {
        self.to_string().encode(buffer)
    }
}
