use std::fmt::Display;
use std::sync::Arc;
use std::time::Instant;

use chrono::{DateTime, TimeZone, Utc};

use ::lifecycle::LifecycleDb;
use data_types::chunk_metadata::{ChunkAddr, ChunkLifecycleAction, ChunkStorage};
use data_types::database_rules::LifecycleRules;
use data_types::error::ErrorLogger;
use data_types::job::Job;
use data_types::partition_metadata::{InfluxDbType, Statistics, TableSummary};
use data_types::DatabaseName;
use datafusion::physical_plan::SendableRecordBatchStream;
use hashbrown::HashMap;
use internal_types::schema::merge::SchemaMerger;
use internal_types::schema::sort::SortKey;
use internal_types::schema::{Schema, TIME_COLUMN_NAME};
use lifecycle::{
    LifecycleChunk, LifecyclePartition, LifecycleReadGuard, LifecycleWriteGuard, LockableChunk,
    LockablePartition,
};
use observability_deps::tracing::{info, trace};
use query::QueryChunkMeta;
use tracker::{RwLock, TaskTracker};

use crate::db::catalog::chunk::CatalogChunk;
use crate::db::catalog::partition::Partition;
use crate::Db;

pub(crate) use compact::compact_chunks;
pub(crate) use error::{Error, Result};
pub(crate) use move_chunk::move_chunk_to_read_buffer;
use persistence_windows::persistence_windows::FlushHandle;
pub(crate) use unload::unload_read_buffer_chunk;
pub(crate) use write::write_chunk_to_object_store;

use super::DbChunk;

mod compact;
mod error;
mod move_chunk;
mod persist;
mod unload;
mod write;

/// A newtype wrapper around `Arc<Db>` to workaround trait orphan rules
#[derive(Debug, Clone)]
pub struct ArcDb(pub(super) Arc<Db>);

impl std::ops::Deref for ArcDb {
    type Target = Db;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

///
/// A `LockableCatalogChunk` combines a `CatalogChunk` with its owning `Db`
///
/// This provides the `lifecycle::LockableChunk` trait which can be used to lock
/// the chunk, determine what to do, and then optionally trigger an action, all
/// without allowing concurrent modification
///
#[derive(Debug, Clone)]
pub struct LockableCatalogChunk {
    pub db: Arc<Db>,
    pub chunk: Arc<RwLock<CatalogChunk>>,
}

impl LockableChunk for LockableCatalogChunk {
    type Chunk = CatalogChunk;

    type Job = Job;

    type Error = Error;

    fn read(&self) -> LifecycleReadGuard<'_, Self::Chunk, Self> {
        LifecycleReadGuard::new(self.clone(), self.chunk.as_ref())
    }

    fn write(&self) -> LifecycleWriteGuard<'_, Self::Chunk, Self> {
        LifecycleWriteGuard::new(self.clone(), self.chunk.as_ref())
    }

    fn move_to_read_buffer(
        s: LifecycleWriteGuard<'_, Self::Chunk, Self>,
    ) -> Result<TaskTracker<Self::Job>, Self::Error> {
        info!(chunk=%s.addr(), "move to read buffer");
        let (tracker, fut) = move_chunk::move_chunk_to_read_buffer(s)?;
        let _ = tokio::spawn(async move { fut.await.log_if_error("move to read buffer") });
        Ok(tracker)
    }

    fn write_to_object_store(
        s: LifecycleWriteGuard<'_, Self::Chunk, Self>,
    ) -> Result<TaskTracker<Self::Job>, Self::Error> {
        info!(chunk=%s.addr(), "writing to object store");
        let (tracker, fut) = write::write_chunk_to_object_store(s)?;
        let _ = tokio::spawn(async move { fut.await.log_if_error("writing to object store") });
        Ok(tracker)
    }

    fn unload_read_buffer(
        s: LifecycleWriteGuard<'_, Self::Chunk, Self>,
    ) -> Result<(), Self::Error> {
        info!(chunk=%s.addr(), "unloading from readbuffer");

        let _ = self::unload::unload_read_buffer_chunk(s)?;
        Ok(())
    }
}

///
/// A `LockableCatalogPartition` combines a `Partition` with its owning `Db`
///
/// This provides the `lifecycle::LockablePartition` trait which can be used to lock
/// the chunk, determine what to do, and then optionally trigger an action, all
/// without allowing concurrent modification
///
#[derive(Debug, Clone)]
pub struct LockableCatalogPartition {
    pub db: Arc<Db>,
    pub partition: Arc<RwLock<Partition>>,
    /// Human readable description of what this CatalogPartiton is
    pub display_string: String,
}

impl LockableCatalogPartition {
    pub fn new(db: Arc<Db>, partition: Arc<RwLock<Partition>>) -> Self {
        let display_string = {
            partition
                .try_read()
                .map(|partition| partition.to_string())
                .unwrap_or_else(|| "UNKNOWN (could not get lock)".into())
        };

        Self {
            db,
            partition,
            display_string,
        }
    }
}

impl Display for LockableCatalogPartition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.display_string)
    }
}

impl LockablePartition for LockableCatalogPartition {
    type Partition = Partition;

    type Chunk = LockableCatalogChunk;

    type PersistHandle = FlushHandle;

    type Error = super::lifecycle::Error;

    fn read(&self) -> LifecycleReadGuard<'_, Partition, Self> {
        LifecycleReadGuard::new(self.clone(), self.partition.as_ref())
    }

    fn write(&self) -> LifecycleWriteGuard<'_, Partition, Self> {
        LifecycleWriteGuard::new(self.clone(), self.partition.as_ref())
    }

    fn chunk(s: &LifecycleReadGuard<'_, Partition, Self>, chunk_id: u32) -> Option<Self::Chunk> {
        s.chunk(chunk_id).map(|chunk| LockableCatalogChunk {
            db: Arc::clone(&s.data().db),
            chunk: Arc::clone(chunk),
        })
    }

    fn chunks(s: &LifecycleReadGuard<'_, Partition, Self>) -> Vec<(u32, Self::Chunk)> {
        s.keyed_chunks()
            .map(|(id, chunk)| {
                (
                    id,
                    LockableCatalogChunk {
                        db: Arc::clone(&s.data().db),
                        chunk: Arc::clone(chunk),
                    },
                )
            })
            .collect()
    }

    fn compact_chunks(
        partition: LifecycleWriteGuard<'_, Partition, Self>,
        chunks: Vec<LifecycleWriteGuard<'_, CatalogChunk, Self::Chunk>>,
    ) -> Result<TaskTracker<Job>, Self::Error> {
        info!(table=%partition.table_name(), partition=%partition.partition_key(), "compacting chunks");
        let (tracker, fut) = compact::compact_chunks(partition, chunks)?;
        let _ = tokio::spawn(async move { fut.await.log_if_error("compacting chunks") });
        Ok(tracker)
    }

    fn prepare_persist(
        partition: &mut LifecycleWriteGuard<'_, Self::Partition, Self>,
    ) -> Option<(Self::PersistHandle, DateTime<Utc>)> {
        let window = partition.persistence_windows_mut().unwrap();
        window.rotate(Instant::now());

        let max_persistable_timestamp = window.max_persistable_timestamp();
        let handle = window.flush_handle();
        trace!(?max_persistable_timestamp, ?handle, "preparing for persist");
        Some((handle?, max_persistable_timestamp?))
    }

    fn persist_chunks(
        partition: LifecycleWriteGuard<'_, Partition, Self>,
        chunks: Vec<LifecycleWriteGuard<'_, CatalogChunk, Self::Chunk>>,
        max_persistable_timestamp: DateTime<Utc>,
        handle: FlushHandle,
    ) -> Result<TaskTracker<Job>, Self::Error> {
        info!(table=%partition.table_name(), partition=%partition.partition_key(), "persisting chunks");
        let (tracker, fut) =
            persist::persist_chunks(partition, chunks, max_persistable_timestamp, handle)?;
        let _ = tokio::spawn(async move { fut.await.log_if_error("persisting chunks") });
        Ok(tracker)
    }

    fn drop_chunk(
        mut s: LifecycleWriteGuard<'_, Self::Partition, Self>,
        chunk_id: u32,
    ) -> Result<(), Self::Error> {
        s.drop_chunk(chunk_id)?;
        Ok(())
    }
}

impl LifecycleDb for ArcDb {
    type Chunk = LockableCatalogChunk;
    type Partition = LockableCatalogPartition;

    fn buffer_size(&self) -> usize {
        self.catalog.metrics().memory().total()
    }

    fn rules(&self) -> LifecycleRules {
        self.rules.read().lifecycle_rules.clone()
    }

    fn partitions(&self) -> Vec<Self::Partition> {
        self.catalog
            .partitions()
            .into_iter()
            .map(|partition| LockableCatalogPartition::new(Arc::clone(&self.0), partition))
            .collect()
    }

    fn name(&self) -> DatabaseName<'static> {
        self.rules.read().name.clone()
    }
}

impl LifecyclePartition for Partition {
    fn partition_key(&self) -> &str {
        self.key()
    }

    fn persistable_row_count(&self) -> usize {
        self.persistence_windows()
            .map(|w| w.persistable_row_count())
            .unwrap_or(0)
    }

    fn minimum_unpersisted_age(&self) -> Option<Instant> {
        self.persistence_windows()
            .and_then(|w| w.minimum_unpersisted_age())
    }
}

impl LifecycleChunk for CatalogChunk {
    fn lifecycle_action(&self) -> Option<&TaskTracker<ChunkLifecycleAction>> {
        self.lifecycle_action()
    }

    fn clear_lifecycle_action(&mut self) {
        self.clear_lifecycle_action()
            .expect("failed to clear lifecycle action")
    }

    fn time_of_first_write(&self) -> Option<DateTime<Utc>> {
        self.time_of_first_write()
    }

    fn time_of_last_write(&self) -> Option<DateTime<Utc>> {
        self.time_of_last_write()
    }

    fn addr(&self) -> &ChunkAddr {
        self.addr()
    }

    fn storage(&self) -> ChunkStorage {
        self.storage().1
    }

    fn row_count(&self) -> usize {
        self.storage().0
    }

    fn min_timestamp(&self) -> DateTime<Utc> {
        let table_summary = self.table_summary();
        let col = table_summary
            .columns
            .iter()
            .find(|x| x.name == TIME_COLUMN_NAME)
            .expect("time column expected");

        let min = match &col.stats {
            Statistics::I64(stats) => stats.min.expect("time column cannot be empty"),
            _ => panic!("unexpected time column type"),
        };

        Utc.timestamp_nanos(min)
    }
}

/// Compute a sort key that orders lower cardinality columns first
///
/// In the absence of more precise information, this should yield a
/// good ordering for RLE compression
fn compute_sort_key<'a>(summaries: impl Iterator<Item = &'a TableSummary>) -> SortKey<'a> {
    let mut cardinalities: HashMap<&str, u64> = Default::default();
    for summary in summaries {
        for column in &summary.columns {
            if column.influxdb_type != Some(InfluxDbType::Tag) {
                continue;
            }

            if let Some(count) = column.stats.distinct_count() {
                *cardinalities.entry(column.name.as_str()).or_default() += count.get()
            }
        }
    }

    let mut cardinalities: Vec<_> = cardinalities.into_iter().collect();
    cardinalities.sort_by_key(|x| x.1);

    let mut key = SortKey::with_capacity(cardinalities.len() + 1);
    for (col, _) in cardinalities {
        key.push(col, Default::default())
    }
    key.push(TIME_COLUMN_NAME, Default::default());
    key
}

/// Creates a new RUB chunk
fn new_rub_chunk(db: &Db, table_name: &str) -> read_buffer::RBChunk {
    // create a new read buffer chunk with memory tracking
    let metrics = db
        .metrics_registry
        .register_domain_with_labels("read_buffer", db.metric_labels.clone());

    read_buffer::RBChunk::new(
        table_name,
        read_buffer::ChunkMetrics::new(&metrics, db.catalog.metrics().memory().read_buffer()),
    )
}

/// Executes a plan and collects the results into a read buffer chunk
async fn collect_rub(
    mut stream: SendableRecordBatchStream,
    chunk: &mut read_buffer::RBChunk,
) -> Result<()> {
    use futures::StreamExt;

    while let Some(batch) = stream.next().await {
        let batch = batch?;
        if batch.num_rows() > 0 {
            chunk.upsert_table(batch)
        }
    }
    Ok(())
}

/// Return the merged schema for the chunks that are being
/// reorganized.
///
/// This is infallable because the schemas of chunks within a
/// partition are assumed to be compatible because that schema was
/// enforced as part of writing into the partition
fn merge_schemas(chunks: &[Arc<DbChunk>]) -> Arc<Schema> {
    let mut merger = SchemaMerger::new();
    for db_chunk in chunks {
        merger = merger
            .merge(&db_chunk.schema())
            .expect("schemas compatible");
    }
    Arc::new(merger.build())
}
