// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;

/// A tag hint, used to lookup in the `tags hints` table
#[derive(Clone, Debug)]
pub struct TagHint {
    /// The tag string
    pub tag: String,
    /// The tag hint variant. Can be 'Regular', 'ExtOutput', or 'NftOutput'.
    pub table_kind: TagHintVariant,
}

impl TagHint {
    /// Creates a new tagged or indexation hint
    pub fn regular(tag: String) -> Self {
        Self {
            tag,
            table_kind: TagHintVariant::Regular,
        }
    }
    /// Creates a new tag hint derived from feature block inside extended output
    pub fn extended_output(tag: String) -> Self {
        Self {
            tag,
            table_kind: TagHintVariant::ExtOutput,
        }
    }
    /// Creates a new tag hint derived from feature block inside nft output
    pub fn nft_output(tag: String) -> Self {
        Self {
            tag,
            table_kind: TagHintVariant::NftOutput,
        }
    }
    /// Get the tag string
    pub fn tag(&self) -> &String {
        &self.tag
    }
    /// Get the tag hint variant
    pub fn table_kind(&self) -> &TagHintVariant {
        &self.table_kind
    }
}

impl TokenEncoder for TagHint {
    fn encode_token(&self) -> TokenEncodeChain {
        self.tag().into()
    }
}

impl<B: Binder> Bindable<B> for TagHint {
    fn bind(&self, binder: B) -> B {
        binder.value(&self.tag).value(&self.table_kind)
    }
}

/// Hint variants
#[derive(Clone, Debug)]
pub enum TagHintVariant {
    /// An unhashed indexation key or tagged data
    Regular,
    /// A feature block for extended output
    ExtOutput,
    /// A feature block for nft output
    NftOutput,
}

impl std::fmt::Display for TagHintVariant {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                TagHintVariant::Regular => "Regular",
                TagHintVariant::ExtOutput => "ExtOutput",
                TagHintVariant::NftOutput => "NftOutput",
            }
        )
    }
}

impl ColumnEncoder for TagHintVariant {
    fn encode(&self, buffer: &mut Vec<u8>) {
        self.to_string().encode(buffer)
    }
}
