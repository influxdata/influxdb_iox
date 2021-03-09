//! Module contains a representation of chunk metadata
use snafu::Snafu;
use std::{
    convert::{TryFrom, TryInto},
    sync::Arc,
};

use generated_types::influxdata::iox::management::v1 as management;
use serde::{Deserialize, Serialize};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unknown value for ChunkStorage: {}", v))]
    UnknownChunkStorage { v: i32 },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Which storage system is a chunk located in?
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Serialize, Deserialize)]
pub enum ChunkStorage {
    /// The chunk is still open for writes new writes, in the Mutable Buffer
    OpenMutableBuffer,

    /// The chunk is no longer open for writes, in the Mutable Buffer
    ClosedMutableBuffer,

    /// The chunk is in the Read Buffer (where it can not be mutated)
    ReadBuffer,

    /// The chunk is stored in Object Storage (where it can not be mutated)
    ObjectStore,
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Serialize, Deserialize)]
/// Represents metadata about a chunkin a database.
/// A chunk can contain one or more tables.
pub struct ChunkSummary {
    /// The partitition key of this chunk
    pub partition_key: Arc<String>,

    /// The id of this chunk
    pub id: u32,

    /// How is this chunk stored?
    pub storage: ChunkStorage,

    /// The total estimated size of this chunk, in bytes
    pub estimated_bytes: usize,
}

/// Conversion code to management API chunk structure
impl From<ChunkSummary> for management::Chunk {
    fn from(summary: ChunkSummary) -> Self {
        let ChunkSummary {
            partition_key,
            id,
            storage,
            estimated_bytes,
        } = summary;

        let storage = storage.into();
        let estimated_bytes = estimated_bytes as u64;

        let partition_key = match Arc::try_unwrap(partition_key) {
            // no one else has a reference so take the string
            Ok(partition_key) => partition_key,
            // some other refernece exists to this string, so clone it
            Err(partition_key) => partition_key.as_ref().clone(),
        };

        Self {
            partition_key,
            id,
            storage,
            estimated_bytes,
        }
    }
}

impl From<ChunkStorage> for i32 {
    fn from(storage: ChunkStorage) -> Self {
        match storage {
            ChunkStorage::OpenMutableBuffer => management::ChunkStorage::OpenMutableBuffer as Self,
            ChunkStorage::ClosedMutableBuffer => {
                management::ChunkStorage::ClosedMutableBuffer as Self
            }
            ChunkStorage::ReadBuffer => management::ChunkStorage::ReadBuffer as Self,
            ChunkStorage::ObjectStore => management::ChunkStorage::ObjectStore as Self,
        }
    }
}

/// Conversion code to management API chunk structure
impl TryFrom<management::Chunk> for ChunkSummary {
    type Error = Error;

    fn try_from(chunk: management::Chunk) -> Result<Self, Self::Error> {
        let management::Chunk {
            partition_key,
            id,
            storage,
            estimated_bytes,
        } = chunk;

        let storage = storage.try_into()?;
        let estimated_bytes = estimated_bytes as usize;
        let partition_key = Arc::new(partition_key);

        Ok(Self {
            partition_key,
            id,
            storage,
            estimated_bytes,
        })
    }
}

impl TryFrom<i32> for ChunkStorage {
    type Error = Error;

    fn try_from(v: i32) -> Result<Self, Self::Error> {
        // There must be a better way to do this...
        if v == management::ChunkStorage::OpenMutableBuffer as i32 {
            Ok(Self::OpenMutableBuffer)
        } else if v == management::ChunkStorage::ClosedMutableBuffer as i32 {
            Ok(Self::ClosedMutableBuffer)
        } else if v == management::ChunkStorage::ReadBuffer as i32 {
            Ok(Self::ReadBuffer)
        } else if v == management::ChunkStorage::ObjectStore as i32 {
            Ok(Self::ObjectStore)
        } else {
            UnknownChunkStorage { v }.fail()
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn valid_chunk_storage() {
        let storage = ChunkStorage::try_from(3).expect("conversion successful");
        assert_eq!(storage, ChunkStorage::ReadBuffer);
    }

    #[test]
    fn invalid_chunk_storage() {
        let err = ChunkStorage::try_from(33).expect_err("Unknown value for ChunkStorage: 33");
        assert_eq!(err.to_string(), "Unknown value for ChunkStorage: 33");
    }
}
