//! Module contains a representation of chunk metadata
use std::{
    convert::{TryFrom, TryInto},
    sync::Arc,
};

use crate::field_validation::FromField;
use chrono::{DateTime, Utc};
use generated_types::{google::FieldViolation, influxdata::iox::management::v1 as management};
use serde::{Deserialize, Serialize};

/// Which storage system is a chunk located in?
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Copy, Serialize, Deserialize)]
pub enum ChunkStorage {
    /// The chunk is still open for new writes, in the Mutable Buffer
    OpenMutableBuffer,

    /// The chunk is no longer open for writes, in the Mutable Buffer
    ClosedMutableBuffer,

    /// The chunk is in the Read Buffer (where it can not be mutated)
    ReadBuffer,

    /// The chunk is both in ReadBuffer and Object Store
    ReadBufferAndObjectStore,

    /// The chunk is stored in Object Storage (where it can not be mutated)
    ObjectStoreOnly,
}

impl ChunkStorage {
    /// Return a str representation of this storage state
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::OpenMutableBuffer => "OpenMutableBuffer",
            Self::ClosedMutableBuffer => "ClosedMutableBuffer",
            Self::ReadBuffer => "ReadBuffer",
            Self::ReadBufferAndObjectStore => "ReadBufferAndObjectStore",
            Self::ObjectStoreOnly => "ObjectStoreOnly",
        }
    }
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Serialize, Deserialize)]
/// Represents metadata about a chunk in a database.
/// A chunk can contain one or more tables.
pub struct ChunkSummary {
    /// The partition key of this chunk
    pub partition_key: Arc<String>,

    /// The table of this chunk
    pub table_name: Arc<String>,

    /// The id of this chunk
    pub id: u32,

    /// How is this chunk stored?
    pub storage: ChunkStorage,

    /// The total estimated size of this chunk, in bytes
    pub estimated_bytes: usize,

    /// The total number of rows in this chunk
    pub row_count: usize,

    /// Time at which the first data was written into this chunk. Note
    /// this is not the same as the timestamps on the data itself
    pub time_of_first_write: Option<DateTime<Utc>>,

    /// Most recent time at which data write was initiated into this
    /// chunk. Note this is not the same as the timestamps on the data
    /// itself
    pub time_of_last_write: Option<DateTime<Utc>>,

    /// Time at which this chunk was marked as closing. Note this is
    /// not the same as the timestamps on the data itself
    pub time_closing: Option<DateTime<Utc>>,
}

impl ChunkSummary {
    /// Construct a ChunkSummary that has None for all timestamps
    pub fn new_without_timestamps(
        partition_key: Arc<String>,
        table_name: Arc<String>,
        id: u32,
        storage: ChunkStorage,
        estimated_bytes: usize,
        row_count: usize,
    ) -> Self {
        Self {
            partition_key,
            table_name,
            id,
            storage,
            estimated_bytes,
            row_count,
            time_of_first_write: None,
            time_of_last_write: None,
            time_closing: None,
        }
    }
}

/// Conversion code to management API chunk structure
impl From<ChunkSummary> for management::Chunk {
    fn from(summary: ChunkSummary) -> Self {
        let ChunkSummary {
            partition_key,
            table_name,
            id,
            storage,
            estimated_bytes,
            row_count,
            time_of_first_write,
            time_of_last_write,
            time_closing,
        } = summary;

        let storage: management::ChunkStorage = storage.into();
        let storage = storage.into(); // convert to i32

        let estimated_bytes = estimated_bytes as u64;
        let row_count = row_count as u64;

        let partition_key = match Arc::try_unwrap(partition_key) {
            // no one else has a reference so take the string
            Ok(partition_key) => partition_key,
            // some other reference exists to this string, so clone it
            Err(partition_key) => partition_key.as_ref().clone(),
        };
        let table_name = match Arc::try_unwrap(table_name) {
            // no one else has a reference so take the string
            Ok(table_name) => table_name,
            // some other reference exists to this string, so clone it
            Err(table_name) => table_name.as_ref().clone(),
        };

        let time_of_first_write = time_of_first_write.map(|t| t.into());
        let time_of_last_write = time_of_last_write.map(|t| t.into());
        let time_closing = time_closing.map(|t| t.into());

        Self {
            partition_key,
            table_name,
            id,
            storage,
            estimated_bytes,
            row_count,
            time_of_first_write,
            time_of_last_write,
            time_closing,
        }
    }
}

impl From<ChunkStorage> for management::ChunkStorage {
    fn from(storage: ChunkStorage) -> Self {
        match storage {
            ChunkStorage::OpenMutableBuffer => Self::OpenMutableBuffer,
            ChunkStorage::ClosedMutableBuffer => Self::ClosedMutableBuffer,
            ChunkStorage::ReadBuffer => Self::ReadBuffer,
            ChunkStorage::ReadBufferAndObjectStore => Self::ReadBufferAndObjectStore,
            ChunkStorage::ObjectStoreOnly => Self::ObjectStoreOnly,
        }
    }
}

/// Conversion code from management API chunk structure
impl TryFrom<management::Chunk> for ChunkSummary {
    type Error = FieldViolation;

    fn try_from(proto: management::Chunk) -> Result<Self, Self::Error> {
        // Use prost enum conversion
        let storage = proto.storage().scope("storage")?;

        let time_of_first_write = proto
            .time_of_first_write
            .map(TryInto::try_into)
            .transpose()
            .map_err(|_| FieldViolation {
                field: "time_of_first_write".to_string(),
                description: "Timestamp must be positive".to_string(),
            })?;

        let time_of_last_write = proto
            .time_of_last_write
            .map(TryInto::try_into)
            .transpose()
            .map_err(|_| FieldViolation {
                field: "time_of_last_write".to_string(),
                description: "Timestamp must be positive".to_string(),
            })?;

        let time_closing = proto
            .time_closing
            .map(TryInto::try_into)
            .transpose()
            .map_err(|_| FieldViolation {
                field: "time_closing".to_string(),
                description: "Timestamp must be positive".to_string(),
            })?;

        let management::Chunk {
            partition_key,
            table_name,
            id,
            estimated_bytes,
            row_count,
            ..
        } = proto;

        let estimated_bytes = estimated_bytes as usize;
        let row_count = row_count as usize;
        let partition_key = Arc::new(partition_key);
        let table_name = Arc::new(table_name);

        Ok(Self {
            partition_key,
            table_name,
            id,
            storage,
            estimated_bytes,
            row_count,
            time_of_first_write,
            time_of_last_write,
            time_closing,
        })
    }
}

impl TryFrom<management::ChunkStorage> for ChunkStorage {
    type Error = FieldViolation;

    fn try_from(proto: management::ChunkStorage) -> Result<Self, Self::Error> {
        match proto {
            management::ChunkStorage::OpenMutableBuffer => Ok(Self::OpenMutableBuffer),
            management::ChunkStorage::ClosedMutableBuffer => Ok(Self::ClosedMutableBuffer),
            management::ChunkStorage::ReadBuffer => Ok(Self::ReadBuffer),
            management::ChunkStorage::ReadBufferAndObjectStore => {
                Ok(Self::ReadBufferAndObjectStore)
            }
            management::ChunkStorage::ObjectStoreOnly => Ok(Self::ObjectStoreOnly),
            management::ChunkStorage::Unspecified => Err(FieldViolation::required("")),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn valid_proto_to_summary() {
        let proto = management::Chunk {
            partition_key: "foo".to_string(),
            table_name: "bar".to_string(),
            id: 42,
            estimated_bytes: 1234,
            row_count: 321,
            storage: management::ChunkStorage::ObjectStoreOnly.into(),
            time_of_first_write: None,
            time_of_last_write: None,
            time_closing: None,
        };

        let summary = ChunkSummary::try_from(proto).expect("conversion successful");
        let expected = ChunkSummary {
            partition_key: Arc::new("foo".to_string()),
            table_name: Arc::new("bar".to_string()),
            id: 42,
            estimated_bytes: 1234,
            row_count: 321,
            storage: ChunkStorage::ObjectStoreOnly,
            time_of_first_write: None,
            time_of_last_write: None,
            time_closing: None,
        };

        assert_eq!(
            summary, expected,
            "Actual:\n\n{:?}\n\nExpected:\n\n{:?}\n\n",
            summary, expected
        );
    }

    #[test]
    fn valid_summary_to_proto() {
        let summary = ChunkSummary {
            partition_key: Arc::new("foo".to_string()),
            table_name: Arc::new("bar".to_string()),
            id: 42,
            estimated_bytes: 1234,
            row_count: 321,
            storage: ChunkStorage::ObjectStoreOnly,
            time_of_first_write: None,
            time_of_last_write: None,
            time_closing: None,
        };

        let proto = management::Chunk::try_from(summary).expect("conversion successful");

        let expected = management::Chunk {
            partition_key: "foo".to_string(),
            table_name: "bar".to_string(),
            id: 42,
            estimated_bytes: 1234,
            row_count: 321,
            storage: management::ChunkStorage::ObjectStoreOnly.into(),
            time_of_first_write: None,
            time_of_last_write: None,
            time_closing: None,
        };

        assert_eq!(
            proto, expected,
            "Actual:\n\n{:?}\n\nExpected:\n\n{:?}\n\n",
            proto, expected
        );
    }
}
