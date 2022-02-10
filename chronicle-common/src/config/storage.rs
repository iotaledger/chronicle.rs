// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;

/// The partition config. Defaults to using 1000 partitions and a chunk size of 60480.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
pub struct PartitionConfig {
    /// The total number of partitions to use
    pub partition_count: u16,
    /// The number of sequential milestones to store side-by-side in a partition
    pub milestone_chunk_size: u32,
}

impl Default for PartitionConfig {
    fn default() -> Self {
        PartitionConfig {
            partition_count: 1000,
            milestone_chunk_size: 8640,
        }
    }
}

impl PartitionConfig {
    /// Calculate partition id from a milestone index.
    /// The formula is:<br>
    ///    `(I / C) % P`<br>
    ///    where:<br>
    ///          `I` = milestone index<br>
    ///          `C` = configured milestone chunk size<br>
    ///          `P` = configured partition count<br>
    pub fn partition_id(&self, milestone_index: u32) -> u16 {
        ((milestone_index / self.milestone_chunk_size) % (self.partition_count as u32)) as u16
    }
}
