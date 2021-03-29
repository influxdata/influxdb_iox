//! Holds one or more Chunks.

use generated_types::wal as wb;
use std::{collections::BTreeMap, sync::Arc, time::Instant};

use crate::chunk::{Chunk, Error as ChunkError};

use data_types::partition_metadata::TableSummary;
use snafu::{ResultExt, Snafu};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Error writing to open chunk of partition with key '{}' in mutable buffer: {}",
        partition_key,
        source
    ))]
    WritingChunkData {
        partition_key: String,
        source: ChunkError,
    },

    #[snafu(display(
        "Can not drop open chunk '{}' of partition with key '{}' in mutable buffer",
        chunk_id,
        partition_key,
    ))]
    DropOpenChunk {
        partition_key: String,
        chunk_id: u32,
    },

    #[snafu(display(
        "Unknown chunk '{}' of partition with key '{}' in mutable buffer",
        chunk_id,
        partition_key,
    ))]
    UnknownChunk {
        partition_key: String,
        chunk_id: u32,
    },

    #[snafu(display(
        "Can not drop unknown chunk '{}' of partition with key '{}' in mutable buffer. Valid chunk ids: {:?}",
        chunk_id,
        partition_key,
        valid_chunk_ids,
    ))]
    DropUnknownChunk {
        partition_key: String,
        chunk_id: u32,
        valid_chunk_ids: Vec<u32>,
    },

    #[snafu(display("Chunk error {}", source))]
    SummariesChunkError { source: ChunkError },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug)]
pub struct Partition {
    /// The partition key that is shared by all Chunks in this Partition
    key: String,

    /// The currently active, open Chunk; All new writes go to this chunk
    open_chunk: Chunk,

    /// Closed chunks which can no longer be written
    /// key: chunk_id, value: Chunk
    ///
    /// List of chunks, ordered by chunk id (and thus creation time).
    /// The ordereing is achieved with a BTreeMap. The ordering is
    /// used when `iter()` is used to iterate over chunks in their
    /// creation order
    closed_chunks: BTreeMap<u32, Arc<Chunk>>,

    /// Responsible for assigning ids to chunks. Eventually, this might
    /// need to start at a number other than 0.
    id_generator: u32,

    /// the instant time this partition was created
    pub created_at: Instant,

    /// the last instant time a write was made to this partition. Partition::new
    /// initializes this to now.
    pub last_write_at: Instant,
}

impl Partition {
    pub fn new(key: impl Into<String>) -> Self {
        // TODO: for existing partitions, does this need to pick up at preexisting ID?
        let mut id_generator = 0;

        let key: String = key.into();
        let open_chunk = Chunk::new(id_generator);
        id_generator += 1;

        let now = Instant::now();
        Self {
            key,
            open_chunk,
            closed_chunks: BTreeMap::new(),
            id_generator,
            created_at: now,
            last_write_at: now,
        }
    }

    /// returns the id of the current open chunk in this partition
    pub(crate) fn open_chunk_id(&self) -> u32 {
        self.open_chunk.id()
    }

    /// write data to the open chunk
    pub fn write_entry(&mut self, entry: &wb::WriteBufferEntry<'_>) -> Result<()> {
        assert_eq!(
            entry
                .partition_key()
                .expect("partition key should be present"),
            self.key
        );
        self.open_chunk
            .write_entry(entry)
            .with_context(|| WritingChunkData {
                partition_key: entry.partition_key().unwrap(),
            })?;
        self.last_write_at = Instant::now();

        Ok(())
    }

    /// Return the list of chunks, in order of id, in this
    /// partition). A Snapshot of the currently active chunk is
    /// returned. The snapshot will not be affected by future inserts
    pub fn chunks(&self) -> Vec<Arc<Chunk>> {
        let mut chunks: Vec<_> = self
            .closed_chunks
            .iter()
            .map(|(_, chunk)| Arc::clone(&chunk))
            .collect::<Vec<_>>();

        chunks.push(self.open_chunk_snapshot());
        chunks
    }

    /// return the chunk by id. If the requested chunk is still open,
    /// returns a snapshot of that chunk which will not be affected by
    /// subsequent writes.
    pub fn get_chunk(&self, chunk_id: u32) -> Result<Arc<Chunk>> {
        if let Some(chunk) = self.closed_chunks.get(&chunk_id) {
            Ok(Arc::clone(&chunk))
        } else if chunk_id == self.open_chunk.id {
            Ok(self.open_chunk_snapshot())
        } else {
            UnknownChunk {
                partition_key: &self.key,
                chunk_id,
            }
            .fail()
        }
    }

    /// Get a snapshot of the currently open chunk (that can be queried)
    fn open_chunk_snapshot(&self) -> Arc<Chunk> {
        // TODO the performance if cloning the chunk is terrible
        // Proper performance is tracked in
        // https://github.com/influxdata/influxdb_iox/issues/635
        let open_chunk_snapshot = self.open_chunk.clone();
        Arc::new(open_chunk_snapshot)
    }

    /// Close the currently open chunk and create a new open
    /// chunk. The newly closed chunk is adding to the list of closed
    /// chunks if it had data, and is returned.
    ///
    /// Any new writes to this partition will go to a new chunk.
    ///
    /// Queries will continue to see data in the specified chunk until
    /// it is dropped.
    ///
    /// Returns the previously open (now closed) Chunk
    pub fn rollover_chunk(&mut self) -> Arc<Chunk> {
        let chunk_id = self.id_generator;
        self.id_generator += 1;
        let mut chunk = Chunk::new(chunk_id);
        std::mem::swap(&mut chunk, &mut self.open_chunk);
        chunk.mark_closed();
        let chunk = Arc::new(chunk);
        if !chunk.is_empty() {
            let existing_value = self.closed_chunks.insert(chunk.id(), Arc::clone(&chunk));
            assert!(existing_value.is_none());
        }
        chunk
    }

    /// Drop the specified chunk for the partition, returning a reference to the
    /// chunk
    pub fn drop_chunk(&mut self, chunk_id: u32) -> Result<Arc<Chunk>> {
        self.closed_chunks.remove(&chunk_id).ok_or_else(|| {
            let partition_key = self.key.clone();
            if self.open_chunk.id() == chunk_id {
                Error::DropOpenChunk {
                    partition_key,
                    chunk_id,
                }
            } else {
                let valid_chunk_ids: Vec<_> = self.iter().map(|c| c.id()).collect();
                Error::DropUnknownChunk {
                    partition_key,
                    chunk_id,
                    valid_chunk_ids,
                }
            }
        })
    }

    /// Return the partition key shared by all data stored in this
    /// partition
    pub fn key(&self) -> &str {
        &self.key
    }

    /// in Return an iterator over each Chunk in this partition
    pub fn iter(&self) -> ChunkIter<'_> {
        ChunkIter::new(self)
    }

    /// Return the estimated size in bytes of the partition
    pub fn size(&self) -> usize {
        self.closed_chunks
            .values()
            .fold(0, |acc, val| acc + val.size())
            + self.open_chunk.size()
    }

    /// Return the table summaries from all chunks. A table will have a summary
    /// for each chunk it exists in. Use PartitionSummary::from_table_summaries
    /// to construct a PartitionSummary.
    #[allow(dead_code)] // TODO: remove once this gets used
    pub fn chunk_table_summaries(&self) -> Result<Vec<TableSummary>> {
        let mut summaries = self.open_chunk.table_stats().context(SummariesChunkError)?;

        for chunk in self.closed_chunks.values() {
            let mut other = chunk.table_stats().context(SummariesChunkError)?;
            summaries.append(&mut other);
        }

        Ok(summaries)
    }
}

/// information on chunks for this partition
#[derive(Debug, Default, PartialEq)]
pub struct PartitionChunkInfo {
    pub num_closed_chunks: usize,
}

/// Iterates over chunks in a partition. Always iterates over chunks
/// in their creation (id) order: Closed chunks first, followed by the
/// open chunk, if any. This allows data to be read out in the same order it
/// was written in
#[derive(Debug)]
pub struct ChunkIter<'a> {
    partition: &'a Partition,
    visited_open: bool,
    closed_iter: std::collections::btree_map::Iter<'a, u32, Arc<Chunk>>,
}

impl<'a> ChunkIter<'a> {
    fn new(partition: &'a Partition) -> Self {
        let closed_iter = partition.closed_chunks.iter();
        Self {
            partition,
            visited_open: false,
            closed_iter,
        }
    }
}

impl<'a> Iterator for ChunkIter<'a> {
    type Item = &'a Chunk;

    fn next(&mut self) -> Option<Self::Item> {
        let partition = self.partition;

        self.closed_iter
            .next()
            .map(|(_k, v)| v.as_ref())
            .or_else(|| {
                if !self.visited_open {
                    self.visited_open = true;
                    Some(&partition.open_chunk)
                } else {
                    None
                }
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use data_types::partition_metadata::PartitionSummary;
    use internal_types::{data::split_lines_into_write_entry_partitions, selection::Selection};

    use arrow_deps::{
        arrow::record_batch::RecordBatch, assert_table_eq, test_util::sort_record_batch,
    };
    use influxdb_line_protocol::parse_lines;

    #[tokio::test]
    async fn test_chunk_timestamps() {
        let start = Utc::now();
        let mut partition = Partition::new("a_key");
        let after_partition_creation = Utc::now();

        // Given data loaded into two chunks
        load_data(
            &mut partition,
            &[
                "h2o,state=MA,city=Boston temp=70.4 100",
                "o2,state=MA,city=Boston temp=71.4 100",
                "o2,state=MA,city=Boston temp=72.4 200",
            ],
        )
        .await;
        let after_data_load = Utc::now();

        // When the chunk is rolled over
        let chunk = partition.rollover_chunk();
        let after_rollover = Utc::now();

        println!("start: {:?}, after_partition_creation: {:?}, after_data_load: {:?}, after_rollover: {:?}",
                 start, after_partition_creation, after_data_load, after_rollover);
        println!("Chunk: {:#?}", chunk);

        // then the chunk creation and rollover times are as expected
        assert!(start < chunk.time_of_first_write.unwrap());
        assert!(after_partition_creation < chunk.time_of_first_write.unwrap());
        assert!(chunk.time_of_first_write.unwrap() < after_data_load);
        assert!(chunk.time_of_first_write.unwrap() == chunk.time_of_last_write.unwrap());
        assert!(after_data_load < chunk.time_closed.unwrap());
        assert!(chunk.time_closed.unwrap() < after_rollover);
    }

    #[tokio::test]
    async fn test_chunk_timestamps_last_write() {
        let mut partition = Partition::new("a_key");

        // Given data loaded into two chunks
        load_data(&mut partition, &["o2,state=MA,city=Boston temp=71.4 100"]).await;
        let after_data_load_1 = Utc::now();

        load_data(&mut partition, &["o2,state=MA,city=Boston temp=72.4 200"]).await;
        let after_data_load_2 = Utc::now();
        let chunk = partition.rollover_chunk();

        assert!(chunk.time_of_first_write.unwrap() < after_data_load_1);
        assert!(chunk.time_of_first_write.unwrap() < chunk.time_of_last_write.unwrap());
        assert!(chunk.time_of_last_write.unwrap() < after_data_load_2);
    }

    #[tokio::test]
    async fn test_chunk_timestamps_empty() {
        let mut partition = Partition::new("a_key");
        let after_partition_creation = Utc::now();

        let chunk = partition.rollover_chunk();
        let after_rollover = Utc::now();
        assert!(chunk.time_of_first_write.is_none());
        assert!(chunk.time_of_last_write.is_none());
        assert!(after_partition_creation < chunk.time_closed.unwrap());
        assert!(chunk.time_closed.unwrap() < after_rollover);
    }

    #[tokio::test]
    async fn test_chunk_timestamps_empty_write() {
        let mut partition = Partition::new("a_key");
        let after_partition_creation = Utc::now();

        // Call load data but don't write any actual data (aka it was an empty write)
        load_data(&mut partition, &[""]).await;

        let chunk = partition.rollover_chunk();
        let after_rollover = Utc::now();

        assert!(chunk.time_of_first_write.is_none());
        assert!(chunk.time_of_last_write.is_none());
        assert!(after_partition_creation < chunk.time_closed.unwrap());
        assert!(chunk.time_closed.unwrap() < after_rollover);
    }

    #[tokio::test]
    async fn chunk_summaries_return_open_and_closed() {
        let mut partition = Partition::new("a_key");

        load_data(
            &mut partition,
            &["cpu,host=a,region=west usage=21.1,system=55.5,num=10 10"],
        )
        .await;
        partition.rollover_chunk();
        load_data(
            &mut partition,
            &[
                "cpu,foo=bar val=5 1",
                "mem,host=a,region=east bytes=23423 21",
            ],
        )
        .await;

        let summaries = partition.chunk_table_summaries().unwrap();
        assert_eq!(3, summaries.len());

        let summary = PartitionSummary::from_table_summaries(&partition.key, summaries);
        assert_eq!(2, summary.tables.len());
        assert!(summary.table("cpu").is_some());
        assert!(summary.table("mem").is_some());
    }

    fn row_count(table_name: &str, chunk: &Chunk) -> u32 {
        let stats = chunk.table_stats().unwrap();
        for s in &stats {
            if s.name == table_name {
                return s.columns[0].count();
            }
        }
        0
    }

    /// Load the specified rows of line protocol data into this partition
    async fn load_data(partition: &mut Partition, lp_data: &[&str]) {
        let lp_string = lp_data.to_vec().join("\n");

        let lines: Vec<_> = parse_lines(&lp_string).map(|l| l.unwrap()).collect();
        let data = split_lines_into_write_entry_partitions(|_| partition.key().into(), &lines);

        let batch = flatbuffers::root::<wb::WriteBufferBatch<'_>>(&data).unwrap();

        let entries = batch.entries().unwrap();
        for entry in entries {
            let key = entry
                .partition_key()
                .expect("partition key should have been inserted");
            assert_eq!(key, partition.key());

            partition.write_entry(&entry).unwrap()
        }
    }

    fn dump_table(partition: &Partition, table_name: &str) -> Vec<RecordBatch> {
        let mut dst = vec![];
        for chunk in partition.chunks() {
            let selection = Selection::All;
            chunk
                .table_to_arrow(&mut dst, table_name, selection)
                .unwrap();
        }

        // Now, sort dest
        dst.into_iter().map(sort_record_batch).collect()
    }

    fn dump_chunk_table(chunk: &Chunk, table_name: &str) -> Vec<RecordBatch> {
        let mut dst = vec![];
        let selection = Selection::All;
        chunk
            .table_to_arrow(&mut dst, table_name, selection)
            .unwrap();
        dst.into_iter().map(sort_record_batch).collect()
    }

    /// returns a list of all chunk ids in partition that are not empty
    fn all_ids_with_data(partition: &Partition) -> Vec<u32> {
        partition
            .iter()
            .filter_map(|c| if c.is_empty() { None } else { Some(c.id()) })
            .collect()
    }

    /// Lists all chunk ids the partition by calling `chunks()`
    fn chunk_ids(partition: &Partition) -> Vec<u32> {
        partition
            .chunks()
            .iter()
            .map(|c| c.id())
            .collect::<Vec<_>>()
    }
}
