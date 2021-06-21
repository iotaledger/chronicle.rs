// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use rust_embed::RustEmbed;

// TODO: Use Chronicle native frontend
#[derive(RustEmbed)]
#[folder = "$CARGO_MANIFEST_DIR/src/dashboard/frontend/build/"]
pub(crate) struct Asset;
