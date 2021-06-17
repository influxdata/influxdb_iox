//! The catalog representation of a Partition

use std::{
    collections::{btree_map::Entry, BTreeMap},
    sync::Arc,
};

use chrono::{DateTime, Utc};

use data_types::partition_metadata::PartitionSummary;
use tracker::RwLock;

use crate::db::catalog::metrics::PartitionMetrics;

use super::chunk::{CatalogChunk, ChunkStage};
use data_types::chunk_metadata::{ChunkAddr, ChunkSummary};

/// IOx Catalog Partition
///
/// A partition contains multiple Chunks for a given table
#[derive(Debug)]
pub struct Partition {
    /// Database name
    db_name: Arc<str>,
    /// The partition key
    partition_key: Arc<str>,

    /// The table name
    table_name: Arc<str>,

    /// The chunks that make up this partition, indexed by id
    chunks: BTreeMap<u32, Arc<RwLock<CatalogChunk>>>,

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
    pub(super) fn new(
        db_name: Arc<str>,
        partition_key: Arc<str>,
        table_name: Arc<str>,
        metrics: PartitionMetrics,
    ) -> Self {
        let now = Utc::now();
        Self {
            db_name,
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
    pub fn create_open_chunk(
        &mut self,
        chunk: mutable_buffer::chunk::MBChunk,
    ) -> Arc<RwLock<CatalogChunk>> {
        assert_eq!(chunk.table_name().as_ref(), self.table_name.as_ref());

        let chunk_id = self.next_chunk_id;
        // Technically this only causes an issue on the next upsert but
        // the MUB treats u32::MAX as a sentinel value
        assert_ne!(self.next_chunk_id, u32::MAX, "Chunk ID Overflow");

        self.next_chunk_id += 1;

        let addr = ChunkAddr {
            db_name: Arc::clone(&self.db_name),
            table_name: Arc::clone(&self.table_name),
            partition_key: Arc::clone(&self.partition_key),
            chunk_id,
        };

        let chunk = Arc::new(self.metrics.new_chunk_lock(CatalogChunk::new_open(
            addr,
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
        chunk: Arc<parquet_file::chunk::ParquetChunk>,
    ) -> Arc<RwLock<CatalogChunk>> {
        assert_eq!(chunk.table_name(), self.table_name.as_ref());

        let addr = ChunkAddr {
            db_name: Arc::clone(&self.db_name),
            table_name: Arc::clone(&self.table_name),
            partition_key: Arc::clone(&self.partition_key),
            chunk_id,
        };

        let chunk = Arc::new(
            self.metrics
                .new_chunk_lock(CatalogChunk::new_object_store_only(
                    addr,
                    chunk,
                    self.metrics.new_chunk_metrics(),
                )),
        );

        self.next_chunk_id = self.next_chunk_id.max(chunk_id + 1);
        match self.chunks.entry(chunk_id) {
            Entry::Vacant(vacant) => Arc::clone(vacant.insert(chunk)),
            Entry::Occupied(_) => panic!("chunk with id {} already exists", chunk_id),
        }
    }

    /// Drop the specified chunk
    pub fn drop_chunk(&mut self, chunk_id: u32) -> Option<Arc<RwLock<CatalogChunk>>> {
        self.chunks.remove(&chunk_id)
    }

    /// return the first currently open chunk, if any
    pub fn open_chunk(&self) -> Option<Arc<RwLock<CatalogChunk>>> {
        self.chunks
            .values()
            .find(|chunk| {
                let chunk = chunk.read();
                matches!(chunk.stage(), ChunkStage::Open { .. })
            })
            .cloned()
    }

    /// Return an immutable chunk reference by chunk id
    pub fn chunk(&self, chunk_id: u32) -> Option<&Arc<RwLock<CatalogChunk>>> {
        self.chunks.get(&chunk_id)
    }

    /// Return a iterator over chunks in this partition
    pub fn chunks(&self) -> impl Iterator<Item = &Arc<RwLock<CatalogChunk>>> {
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
