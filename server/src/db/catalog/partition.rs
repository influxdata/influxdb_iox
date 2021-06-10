//! The catalog representation of a Partition

use std::{
    collections::{btree_map::Entry, BTreeMap},
    sync::Arc,
    time::{Duration, Instant},
};

use chrono::{DateTime, Utc};

use data_types::partition_metadata::PartitionSummary;
use tracker::RwLock;

use crate::db::catalog::metrics::PartitionMetrics;

use super::chunk::{Chunk, ChunkStage};
use data_types::chunk_metadata::ChunkSummary;

/// IOx Catalog Partition
///
/// A partition contains multiple Chunks for a given table
#[derive(Debug)]
pub struct Partition {
    /// The partition key
    partition_key: Arc<str>,

    /// The table name
    table_name: Arc<str>,

    /// The chunks that make up this partition, indexed by id
    chunks: BTreeMap<u32, Arc<RwLock<Chunk>>>,

    /// When this partition was created
    created_at: DateTime<Utc>,

    /// the last time at which write was made to this
    /// partition. Partition::new initializes this to now.
    last_write_at: DateTime<Utc>,

    /// What the next chunk id is
    next_chunk_id: u32,

    /// Partition metrics
    metrics: PartitionMetrics,
}

impl Partition {
    /// Create a new partition catalog object.
    ///
    /// This function is not pub because `Partition`s should be
    /// created using the interfaces on [`Catalog`](crate::db::catalog::Catalog) and not
    /// instantiated directly.
    pub(crate) fn new(
        partition_key: Arc<str>,
        table_name: Arc<str>,
        metrics: PartitionMetrics,
    ) -> Self {
        let now = Utc::now();
        Self {
            partition_key,
            table_name,
            chunks: Default::default(),
            created_at: now,
            last_write_at: now,
            next_chunk_id: 0,
            metrics,
        }
    }

    /// Return the partition_key of this Partition
    pub fn key(&self) -> &str {
        &self.partition_key
    }

    /// Return the table name of this partition
    pub fn table_name(&self) -> &str {
        &self.table_name
    }

    /// Update the last write time to now
    pub fn update_last_write_at(&mut self) {
        self.last_write_at = Utc::now();
    }

    /// Return the time at which this partition was created
    pub fn created_at(&self) -> DateTime<Utc> {
        self.created_at
    }

    /// Return the time at which the last write was written to this partititon
    pub fn last_write_at(&self) -> DateTime<Utc> {
        self.last_write_at
    }

    /// Create a new Chunk in the open state.
    ///
    /// This will add a new chunk to the catalog and increases the chunk ID counter for that table-partition
    /// combination.
    ///
    /// Returns an error if the chunk is empty.
    pub fn create_open_chunk(&mut self, chunk: mutable_buffer::chunk::Chunk) -> Arc<RwLock<Chunk>> {
        assert_eq!(chunk.table_name().as_ref(), self.table_name.as_ref());

        let chunk_id = self.next_chunk_id;
        // Technically this only causes an issue on the next upsert but
        // the MUB treats u32::MAX as a sentinel value
        assert_ne!(self.next_chunk_id, u32::MAX, "Chunk ID Overflow");

        self.next_chunk_id += 1;

        let chunk = Arc::new(self.metrics.new_chunk_lock(Chunk::new_open(
            chunk_id,
            &self.partition_key,
            chunk,
            self.metrics.new_chunk_metrics(),
        )));

        if self.chunks.insert(chunk_id, Arc::clone(&chunk)).is_some() {
            // A fundamental invariant has been violated - abort
            panic!("chunk already existed with id {}", chunk_id)
        }

        chunk
    }

    /// Create new chunk that is only in object store (= parquet file).
    ///
    /// The table-specific chunk ID counter will be set to
    /// `max(current, chunk_id + 1)`.
    ///
    /// Returns the previous chunk with the given chunk_id if any
    pub fn insert_object_store_only_chunk(
        &mut self,
        chunk_id: u32,
        chunk: Arc<parquet_file::chunk::Chunk>,
    ) -> Arc<RwLock<Chunk>> {
        assert_eq!(chunk.table_name(), self.table_name.as_ref());

        let chunk = Arc::new(self.metrics.new_chunk_lock(Chunk::new_object_store_only(
            chunk_id,
            &self.partition_key,
            chunk,
            self.metrics.new_chunk_metrics(),
        )));

        self.next_chunk_id = self.next_chunk_id.max(chunk_id + 1);
        match self.chunks.entry(chunk_id) {
            Entry::Vacant(vacant) => Arc::clone(vacant.insert(chunk)),
            Entry::Occupied(_) => panic!("chunk with id {} already exists", chunk_id),
        }
    }

    /// Drop the specified chunk
    pub fn drop_chunk(&mut self, chunk_id: u32) -> Option<Arc<RwLock<Chunk>>> {
        self.chunks.remove(&chunk_id)
    }

    /// return the first currently open chunk, if any
    pub fn open_chunk(&self) -> Option<Arc<RwLock<Chunk>>> {
        self.chunks
            .values()
            .find(|chunk| {
                let chunk = chunk.read();
                matches!(chunk.stage(), ChunkStage::Open { .. })
            })
            .cloned()
    }

    /// Return an immutable chunk reference by chunk id
    pub fn chunk(&self, chunk_id: u32) -> Option<&Arc<RwLock<Chunk>>> {
        self.chunks.get(&chunk_id)
    }

    /// Return a iterator over chunks in this partition
    pub fn chunks(&self) -> impl Iterator<Item = &Arc<RwLock<Chunk>>> {
        self.chunks.values()
    }

    /// Return a PartitionSummary for this partition
    pub fn summary(&self) -> PartitionSummary {
        PartitionSummary::from_table_summaries(
            self.partition_key.to_string(),
            self.chunks
                .values()
                .map(|x| x.read().table_summary().as_ref().clone()),
        )
    }

    /// Return chunk summaries for all chunks in this partition
    pub fn chunk_summaries(&self) -> impl Iterator<Item = ChunkSummary> + '_ {
        self.chunks().map(|x| x.read().summary())
    }

    pub fn metrics(&self) -> &PartitionMetrics {
        &self.metrics
    }
}

const CLOSED_WINDOW_SECONDS: u8 = 10;
const SECONDS_IN_MINUTE: u8 = 60;

#[derive(Debug, Clone)]
pub struct PersistenceWindows {
    persistable: Option<Window>,
    closed: Vec<Window>,
    open: Option<Window>,
    late_arrival_period: Duration,
}

impl PersistenceWindows {
    pub fn new(late_arrival_minutes: u8) -> Self {
        let late_arrival_seconds = late_arrival_minutes * SECONDS_IN_MINUTE;
        let closed_window_count = late_arrival_seconds  / CLOSED_WINDOW_SECONDS;

        Self{
            persistable: None,
            closed: Vec::with_capacity(closed_window_count as usize),
            open: None,
            late_arrival_period: Duration::new(late_arrival_seconds as u64, 0),
        }
    }

    /// Updates the windows with the information from a batch of rows to the same partition. If a
    pub fn add_row(&mut self, sequencer_id: u32, sequence_number: u64, row_time: DateTime<Utc>, received_at: Instant) {
        match &mut self.open {
            Some(w) => {
                w.update(sequencer_id, sequence_number, row_time);
            },
            None => {
                    let mut sequencer_maximums = BTreeMap::new();
                    sequencer_maximums.insert(sequencer_id, sequence_number);
                    let mut sequencer_minimums = BTreeMap::new();
                    sequencer_minimums.insert(sequencer_id, sequence_number);

                    self.open = Some(Window{
                        created_at: received_at,
                        row_count: 1,
                        min_time: row_time.clone(),
                        max_time: row_time,
                        sequencer_maximums,
                        sequencer_minimums,
                    })
            }
        }
    }

    pub fn persistable_row_count(&self) -> usize {
        self.persistable.as_ref().map(|w| w.row_count).unwrap_or(0)
    }

    pub fn persistable_age(&self) -> Option<Instant> {
        self.persistable.as_ref().map(|w| w.created_at)
    }

    pub fn t_flush(&self) -> Option<DateTime<Utc>> {
        self.persistable.as_ref().map(|w| w.max_time)
    }

    pub fn flush_persistable(&mut self) {
        self.persistable = None;
    }
}

#[derive(Debug, Clone)]
struct Window {
    created_at: Instant,
    row_count: usize,
    min_time: DateTime<Utc>,
    max_time: DateTime<Utc>,
    sequencer_minimums: BTreeMap<u32, u64>,
    sequencer_maximums: BTreeMap<u32, u64>,
}

impl Window {
    // Updates the window the the passed in row information. This function assumes that sequence numbers
    // are always increasing.
    fn update(&mut self, sequencer_id: u32, sequence_number: u64, row_time: DateTime<Utc>) {
        self.row_count += 1;
        if self.min_time > row_time {
            self.min_time = row_time;
        }
        if self.max_time < row_time {
            self.max_time = row_time;
        }
        self.sequencer_minimums.entry(sequencer_id).or_insert(sequence_number);
        self.sequencer_maximums.insert(sequencer_id, sequence_number);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn starts_open_window() {
        let mut w = PersistenceWindows::new(1);
        assert_eq!(w.closed.capacity(), 6);

        let i = Instant::now();
        let start_time = Utc::now();
        let last_time = Utc::now();

        w.add_row(1, 2, start_time, i);
        w.add_row(1, 4, Utc::now(), Instant::now());
        w.add_row(1, 10, last_time, Instant::now());

        assert!(w.closed.is_empty());
        assert!(w.persistable.is_none());
        assert_eq!(w.open.unwrap().row_count, 3);
    }
}