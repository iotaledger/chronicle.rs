use serde::{
    Deserialize,
    Serialize,
};

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Copy, Clone)]
pub struct Partitioner {
    milestone_chunk_size: u32,
    partition_count: u16,
}

impl Partitioner {
    /// Create new Partitioner
    pub fn new(milestone_chunk_size: u32, partition_count: u16) -> Self {
        Self {
            milestone_chunk_size,
            partition_count,
        }
    }
    /// Get the partition id
    pub fn partition_id(&self, milestone_index: u32) -> u16 {
        ((milestone_index / self.milestone_chunk_size) % (self.partition_count as u32)) as u16
    }
}

impl std::default::Default for Partitioner {
    fn default() -> Self {
        // Assuming one milestone every 10 seconds = ~60480 milestones per week
        // so we move to new partition every week
        Self {
            milestone_chunk_size: 60480,
            partition_count: 1000,
        }
    }
}
