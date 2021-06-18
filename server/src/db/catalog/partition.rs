//! The catalog representation of a Partition

use std::{
    collections::{btree_map::Entry, BTreeMap},
    sync::Arc,
};

use chrono::{DateTime, Utc};

use data_types::partition_metadata::PartitionSummary;
use tracker::RwLock;

use crate::db::catalog::metrics::{PartitionMetrics, MemoryMetrics};

use super::chunk::CatalogChunk;
use data_types::chunk_metadata::ChunkSummary;
use mutable_buffer::chunk::{MBChunk, ChunkMetrics};
use entry::{TableBatch, Sequence};

use snafu::{ResultExt, Snafu};
use metrics::{MetricRegistry, KeyValue};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("write error"))]
    WriteError{ source: mutable_buffer::chunk::Error },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;


/// IOx Catalog Partition
///
/// A partition contains multiple Chunks for a given table
#[derive(Debug)]
pub struct Partition {
    /// The partition key
    partition_key: Arc<str>,

    /// The table name
    table_name: Arc<str>,

    /// The write buffer
    buffer: Option<MBChunk>,

    /// The chunks that make up this partition, indexed by id
    chunks: BTreeMap<u32, Arc<RwLock<CatalogChunk>>>,

    /// When this partition was created
    created_at: DateTime<Utc>,

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
            buffer: None,
            chunks: Default::default(),
            created_at: now,
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

    // TODO: remove the passing of metric stuff to this. It's an anti-pattern, partition should have what it needs for this
    pub fn write_table_batch(&mut self, sequence: &Sequence, table_batch: TableBatch<'_>, metric_labels: &Vec<KeyValue>, metrics_registry: &Arc<MetricRegistry>, memory_metrics: &MemoryMetrics) -> Result<()> {
        let table_name = &self.table_name;

        let buf = self.buffer.get_or_insert_with(|| {
            let metrics = metrics_registry.register_domain_with_labels(
                "mutable_buffer",
                metric_labels.clone(),
            );
            let metrics = ChunkMetrics::new(&metrics, memory_metrics.mutable_buffer());
            MBChunk::new(table_name, metrics)
        });
        buf.write_table_batch(sequence.id, sequence.number, table_batch).context(WriteError)?;

        Ok(())
    }

    /// Return the time at which this partition was created
    pub fn created_at(&self) -> DateTime<Utc> {
        self.created_at
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

        let chunk = Arc::new(
            self.metrics
                .new_chunk_lock(CatalogChunk::new_object_store_only(
                    chunk_id,
                    &self.partition_key,
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

    pub fn buffer_snapshot(&self) -> Option<CatalogChunk> {
        self.buffer.as_ref().map(|b| {
            let snap = b.snapshot();
            CatalogChunk::new_from_snapshot(Arc::clone(&self.table_name), Arc::clone(&self.partition_key), self.next_chunk_id, snap)
        })
    }

    pub fn rollover_buffer(&mut self) -> Option<Arc<RwLock<CatalogChunk>>> {
        self.buffer.take().map(|b| {
            let snap = b.snapshot();
            let chunk = CatalogChunk::new_from_snapshot(Arc::clone(&self.table_name), Arc::clone(&self.partition_key), self.next_chunk_id, snap);
            let chunk = Arc::new(RwLock::new(chunk));
            self.chunks.insert(self.next_chunk_id, chunk.clone());
            self.next_chunk_id += 1;
            chunk
        })
    }

    /// Drop the specified chunk
    pub fn drop_chunk(&mut self, chunk_id: u32) -> Option<Arc<RwLock<CatalogChunk>>> {
        // if the id is the next_chunk_id, they're refering to the buffer. This is a bit
        // odd, but rollover the chunk so that it takes the id, and whatever is open
        // next will take the next ID. We'll probably want to revisit this and maybe refactor
        // in the tests. A buffer shouldn't be a chunk. A snapshot of the buffer can be one, but
        // it's generated for queries only. Dropping the buffer feels like it should be a separate
        // and explicit operation, but not sure yet.
        if self.next_chunk_id == chunk_id {
            self.rollover_buffer();
        }
        self.chunks.remove(&chunk_id)
    }

    /// Return an immutable chunk reference by chunk id
    pub fn chunk(&self, chunk_id: u32) -> Option<&Arc<RwLock<CatalogChunk>>> {
        self.chunks.get(&chunk_id)
    }

    /// Return the chunks in this partition, including the mutable buffer snapshot
    pub fn chunks(&self) -> Vec<Arc<RwLock<CatalogChunk>>> {
        let mut chunks: Vec<_> = self.chunks.values().cloned().collect();
        if let Some(snapshot) = self.buffer_snapshot() {
            chunks.push(Arc::new(RwLock::new(snapshot)));
        }
        chunks
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
        self.chunks().into_iter().map(|x| x.read().summary())
    }

    pub fn metrics(&self) -> &PartitionMetrics {
        &self.metrics
    }
}
