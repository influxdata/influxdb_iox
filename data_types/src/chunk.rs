//! Module contains a representation of chunk metadata
use std::sync::Arc;

use chrono::{DateTime, Utc};
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

/// Represents metadata about the physical storage of a chunk in a
/// database.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Serialize, Deserialize)]
pub struct ChunkSummary {
    /// The partition key of this chunk
    pub partition_key: Arc<str>,

    /// The table of this chunk
    pub table_name: Arc<str>,

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

    /// Time at which this chunk was marked as closed. Note this is
    /// not the same as the timestamps on the data itself
    pub time_closed: Option<DateTime<Utc>>,
}

/// Represents metadata about the physical storage of a column in a chunk
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct ChunkColumnSummary {
    /// Column name
    pub name: Arc<String>,

    /// Estimated size, in bytes, consumed by this column.
    pub estimated_bytes: usize,
}

/// Contains additional per-column details about physical storage of a chunk
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct DetailedChunkSummary {
    /// Overall chunk statistic
    pub inner: ChunkSummary,

    /// Per column breakdown
    pub columns: Vec<ChunkColumnSummary>,
}

impl DetailedChunkSummary {
    /// aggregates any duplicate entries in `columns`
    pub fn coalesce(&mut self) {
        self.columns.sort_by(|c1, c2| c1.name.cmp(&c2.name));
        let has_dupes = self
            .columns
            .iter()
            .zip(self.columns.iter().skip(1))
            .any(|(c1, c2)| c1.name == c2.name);

        if has_dupes {
            let mut t = Vec::new();
            std::mem::swap(&mut self.columns, &mut t);
            let mut deduplicated = t
                .into_iter()
                .fold(std::collections::BTreeMap::new(), |mut map, c| {
                    let entry = map.entry(c.name).or_insert(0);
                    *entry += c.estimated_bytes;
                    map
                })
                // now we have a hash map with the aggregated values
                .into_iter()
                .map(|(name, estimated_bytes)| ChunkColumnSummary {
                    name,
                    estimated_bytes,
                })
                .collect::<Vec<_>>();
            std::mem::swap(&mut self.columns, &mut deduplicated);
        }
    }
}

impl ChunkSummary {
    /// Construct a ChunkSummary that has None for all timestamps
    pub fn new_without_timestamps(
        partition_key: Arc<str>,
        table_name: Arc<str>,
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
            time_closed: None,
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn coalesce_summary() {
        let mut detailed_summary = DetailedChunkSummary {
            inner: ChunkSummary {
                partition_key: Arc::new("foo".to_string()),
                table_name: Arc::new("bar".to_string()),
                id: 42,
                estimated_bytes: 1234,
                row_count: 321,
                storage: ChunkStorage::ObjectStoreOnly,
                time_of_first_write: None,
                time_of_last_write: None,
                time_closed: None,
            },
            columns: vec![
                ChunkColumnSummary {
                    name: Arc::new("c3".to_string()),
                    estimated_bytes: 1000,
                },
                ChunkColumnSummary {
                    name: Arc::new("c1".to_string()),
                    estimated_bytes: 11,
                },
                ChunkColumnSummary {
                    name: Arc::new("c2".to_string()),
                    estimated_bytes: 100,
                },
                ChunkColumnSummary {
                    name: Arc::new("c2".to_string()),
                    estimated_bytes: 200,
                },
                ChunkColumnSummary {
                    name: Arc::new("c1".to_string()),
                    estimated_bytes: 200,
                },
            ],
        };

        let expected = DetailedChunkSummary {
            inner: detailed_summary.inner.clone(),
            columns: vec![
                ChunkColumnSummary {
                    name: Arc::new("c1".to_string()),
                    estimated_bytes: 211,
                },
                ChunkColumnSummary {
                    name: Arc::new("c2".to_string()),
                    estimated_bytes: 300,
                },
                ChunkColumnSummary {
                    name: Arc::new("c3".to_string()),
                    estimated_bytes: 1000,
                },
            ],
        };

        assert_ne!(&detailed_summary, &expected);

        detailed_summary.coalesce();
        assert_eq!(&detailed_summary, &expected);
    }
}
