// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::{
    ChronicleKeyspace,
    ComputeToken,
};

impl<K> ComputeToken<K> for ChronicleKeyspace {
    fn token(_key: &K) -> i64 {
        rand::random()
    }
}
