use std::sync::Arc;

use chrono::{DateTime, Utc};
use snafu::Snafu;

use data_types::{
    chunk_metadata::{ChunkColumnSummary, ChunkStorage, ChunkSummary, DetailedChunkSummary},
    partition_metadata::TableSummary,
};
use internal_types::schema::Schema;
use lifecycle::ChunkLifecycleAction;
use metrics::{Counter, Histogram, KeyValue};
use mutable_buffer::chunk::{snapshot::ChunkSnapshot as MBChunkSnapshot};
use parquet_file::chunk::ParquetChunk;
use read_buffer::RBChunk;
use tracker::{TaskRegistration, TaskTracker};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Internal Error: unexpected chunk state for {}:{}:{}  during {}. Expected {}, got {}",
        partition_key,
        table_name,
        chunk_id,
        operation,
        expected,
        actual
    ))]
    InternalChunkState {
        partition_key: String,
        table_name: String,
        chunk_id: u32,
        operation: String,
        expected: String,
        actual: String,
    },

    #[snafu(display(
        "Internal Error: A lifecycle action '{}' is already in progress for  {}:{}:{}",
        lifecycle_action,
        partition_key,
        table_name,
        chunk_id,
    ))]
    LifecycleActionAlreadyInProgress {
        partition_key: String,
        table_name: String,
        chunk_id: u32,
        lifecycle_action: String,
    },

    #[snafu(display(
        "Internal Error: Unexpected chunk state for {}:{}:{}. Expected {}, got {}",
        partition_key,
        table_name,
        chunk_id,
        expected,
        actual
    ))]
    UnexpectedLifecycleAction {
        partition_key: String,
        table_name: String,
        chunk_id: u32,
        expected: String,
        actual: String,
    },

    #[snafu(display(
        "Internal Error: Cannot clear a lifecycle action '{}' for chunk {}:{} that is still running",
        action,
        partition_key,
        chunk_id
    ))]
    IncompleteLifecycleAction {
        partition_key: String,
        chunk_id: u32,
        action: String,
    },
}
pub type Result<T, E = Error> = std::result::Result<T, E>;

// Closed chunks have cached information about their schema and statistics
#[derive(Debug, Clone)]
pub struct ChunkMetadata {
    /// The TableSummary, including statistics, for the table in this
    /// Chunk
    pub table_summary: Arc<TableSummary>,

    /// The schema for the table in this Chunk
    pub schema: Arc<Schema>,
}

/// Different memory representations of a frozen chunk.
#[derive(Debug)]
pub enum ChunkStageFrozenRepr {
    /// Snapshot from the Mutable Buffer, freshly created from the former _open_ chunk. Not ideal for memory consumption
    /// but good enough for the frozen stage. Should ideally be converted into the
    /// [`ReadBuffer`](ChunkStageFrozenRepr::ReadBuffer) rather quickly.
    MutableBufferSnapshot(Arc<MBChunkSnapshot>),

    /// Read Buffer that is optimized for in-memory data processing.
    ReadBuffer(Arc<RBChunk>),
}

/// Represents the current lifecycle stage a chunk is in.
///
/// # Stages
/// - **Open:** A chunk can receive new data. It is not persisted.
/// - **Frozen:** A chunk cannot receive new data. It is not persisted.
/// - **Persisted:** A chunk cannot receive new data. It is persisted.
///
/// # Stage Transitions
/// State changes look like this:
///
/// ```text
///      new           compact         restore
///       │             ▲   │             │
///       │             │   │             │
/// ┌─────▼─────┐   ┌───┴───▼───┐   ┌─────▼─────┐
/// │           │   │           │   │           │
/// │   Open    ├───►  Frozen   ├──►│ Persisted │
/// │           │   │           │   │           │
/// └─────┬─────┘   └─────┬─────┘   └─────┬─────┘
///       │               │               │
///       │               ▼               │
///       └────────────►drop◄─────────────┘
/// ```
///
/// A chunk stage lifecycle is linear, i.e. it can never go back. Also note that the _peristed_ stage is the only one
/// that can be restored on node startup (from the persisted catalog). Furthermore, multiple _frozen_ chunks can be
/// compacted into a single one. Nodes at any stage can be dropped via API calls and according to lifecycle policies.
///
/// A chunk can be in-transit when there is a lifecycle job active. A lifecycle job can change the stage once finished
/// (according to the diagram shown above). The chunk stage is considered unchanged as long as the job is running.
#[derive(Debug)]
pub enum ChunkStage {
    /// A chunk in an _frozen stage.
    ///
    /// Chunks in this stage cannot be modified but are not yet persisted. They can however be compacted which will take two
    /// or more chunks and creates a single new frozen chunk.
    Frozen {
        /// Metadata (statistics, schema) about this chunk
        meta: Arc<ChunkMetadata>,

        /// Internal memory representation of the frozen chunk.
        representation: ChunkStageFrozenRepr,
    },

    /// Chunk in _persisted_ stage.
    ///
    /// Chunk cannot receive new data. It is persisted.
    Persisted {
        /// Metadata (statistics, schema) about this chunk
        meta: Arc<ChunkMetadata>,

        /// Parquet chunk that lives immutable within the object store.
        parquet: Arc<ParquetChunk>,

        /// In-memory version of the parquet data.
        read_buffer: Option<Arc<RBChunk>>,
    },
}

impl ChunkStage {
    pub fn name(&self) -> &'static str {
        match self {
            Self::Frozen { .. } => "Frozen",
            Self::Persisted { .. } => "Persisted",
        }
    }
}

/// The catalog representation of a Chunk in IOx. Note that a chunk
/// may exist in several physical locations at any given time (e.g. in
/// mutable buffer and in read buffer)
///
/// # State Handling
/// The actual chunk _state_ consistest of multiple parts. First there is the [lifecycle _stage_](ChunkStage)
/// which captures the grant movement of a chunk from a unoptimized mutable object to an optimized immutable one. Within
/// these stages there are multiple ways to represent or cache data. This fact is captured by the _stage_-specific chunk
/// _representation_ (e.g. a persisted chunk may have data cached in-memory).
#[derive(Debug)]
pub struct CatalogChunk {
    /// What partition does the chunk belong to?
    partition_key: Arc<str>,

    /// What table does the chunk belong to?
    table_name: Arc<str>,

    /// The ID of the chunk
    id: u32,

    /// The lifecycle stage this chunk is in.
    stage: ChunkStage,

    /// The active lifecycle task if any
    ///
    /// This is stored as a TaskTracker to allow monitoring the progress of the
    /// action, detecting if the task failed, waiting for the task to complete
    /// or even triggering graceful termination of it
    lifecycle_action: Option<TaskTracker<ChunkLifecycleAction>>,

    /// The metrics for this chunk
    metrics: ChunkMetrics,

    /// Time at which the first data was written into this chunk. Note
    /// this is not the same as the timestamps on the data itself
    time_of_first_write: Option<DateTime<Utc>>,

    /// Most recent time at which data write was initiated into this
    /// chunk. Note this is not the same as the timestamps on the data
    /// itself
    time_of_last_write: Option<DateTime<Utc>>,

    /// Time at which this chunk was maked as closed. Note this is
    /// not the same as the timestamps on the data itself
    time_closed: Option<DateTime<Utc>>,
}

macro_rules! unexpected_state {
    ($SELF: expr, $OP: expr, $EXPECTED: expr, $STATE: expr) => {
        InternalChunkState {
            partition_key: $SELF.partition_key.as_ref(),
            table_name: $SELF.table_name.as_ref(),
            chunk_id: $SELF.id,
            operation: $OP,
            expected: $EXPECTED,
            actual: $STATE.name(),
        }
        .fail()
    };
}

#[derive(Debug)]
pub struct ChunkMetrics {
    pub(super) state: Counter,
    pub(super) immutable_chunk_size: Histogram,
}

impl ChunkMetrics {
    /// Creates an instance of ChunkMetrics that isn't registered with a central
    /// metrics registry. Observations made to instruments on this ChunkMetrics instance
    /// will therefore not be visible to other ChunkMetrics instances or metric instruments
    /// created on a metrics domain, and vice versa
    pub fn new_unregistered() -> Self {
        Self {
            state: Counter::new_unregistered(),
            immutable_chunk_size: Histogram::new_unregistered(),
        }
    }
}

impl CatalogChunk {
    pub(crate) fn new_from_snapshot(
        table_name: Arc<str>,
        partition_key: Arc<str>,
        chunk_id: u32,
        snapshot: Arc<MBChunkSnapshot>,
    ) -> Self {
        let metrics = ChunkMetrics::new_unregistered();
        metrics.state.inc_with_labels(&[KeyValue::new("state", "closed")]);
        metrics.immutable_chunk_size.observe_with_labels(
            snapshot.size() as f64,
            &[KeyValue::new("state", "closed")],
        );

        let meta = Arc::new(ChunkMetadata{
            table_summary: Arc::new(snapshot.table_summary()),
            schema: snapshot.full_schema(),
        });
        let time_of_first_write = Some(snapshot.first_write_at());
        let time_of_last_write = Some(snapshot.last_write_at());
        let frozen = ChunkStageFrozenRepr::MutableBufferSnapshot(snapshot);

        Self {
            partition_key,
            table_name,
            id: chunk_id,
            stage: ChunkStage::Frozen { meta, representation: frozen},
            lifecycle_action: None,
            metrics,
            time_of_first_write,
            time_of_last_write,
            time_closed: None,
        }
    }

    /// Creates a new chunk that is only registered via an object store reference (= only exists in parquet).
    ///
    /// Apart from [`new_open`](Self::new_open) this is the only way to create new chunks.
    pub(crate) fn new_object_store_only(
        chunk_id: u32,
        partition_key: impl AsRef<str>,
        chunk: Arc<parquet_file::chunk::ParquetChunk>,
        metrics: ChunkMetrics,
    ) -> Self {
        let table_name = Arc::from(chunk.table_name());

        // Cache table summary + schema
        let meta = Arc::new(ChunkMetadata {
            table_summary: Arc::clone(chunk.table_summary()),
            schema: chunk.full_schema(),
        });

        let stage = ChunkStage::Persisted {
            parquet: chunk,
            read_buffer: None,
            meta,
        };

        Self {
            partition_key: Arc::from(partition_key.as_ref()),
            table_name,
            id: chunk_id,
            stage,
            lifecycle_action: None,
            metrics,
            time_of_first_write: None,
            time_of_last_write: None,
            time_closed: None,
        }
    }

    pub fn id(&self) -> u32 {
        self.id
    }

    pub fn key(&self) -> &str {
        self.partition_key.as_ref()
    }

    pub fn table_name(&self) -> Arc<str> {
        Arc::clone(&self.table_name)
    }

    pub fn stage(&self) -> &ChunkStage {
        &self.stage
    }

    pub fn lifecycle_action(&self) -> Option<&TaskTracker<ChunkLifecycleAction>> {
        self.lifecycle_action.as_ref()
    }

    pub fn time_of_first_write(&self) -> Option<DateTime<Utc>> {
        self.time_of_first_write
    }

    pub fn time_of_last_write(&self) -> Option<DateTime<Utc>> {
        self.time_of_last_write
    }

    pub fn time_closed(&self) -> Option<DateTime<Utc>> {
        self.time_closed
    }

    /// Update the write timestamps for this chunk
    pub fn record_write(&mut self) {
        let now = Utc::now();
        if self.time_of_first_write.is_none() {
            self.time_of_first_write = Some(now);
        }
        self.time_of_last_write = Some(now);
    }

    /// Returns the storage and the number of rows
    pub fn storage(&self) -> (usize, ChunkStorage) {
        match &self.stage {
            ChunkStage::Frozen { representation, .. } => match &representation {
                ChunkStageFrozenRepr::MutableBufferSnapshot(repr) => {
                    (repr.rows(), ChunkStorage::ClosedMutableBuffer)
                }
                ChunkStageFrozenRepr::ReadBuffer(repr) => {
                    (repr.rows() as usize, ChunkStorage::ReadBuffer)
                }
            },
            ChunkStage::Persisted {
                parquet,
                read_buffer,
                ..
            } => {
                let rows = parquet.rows() as usize;
                let storage = if read_buffer.is_some() {
                    ChunkStorage::ReadBufferAndObjectStore
                } else {
                    ChunkStorage::ObjectStoreOnly
                };
                (rows, storage)
            }
        }
    }

    /// Return ChunkSummary metadata for this chunk
    pub fn summary(&self) -> ChunkSummary {
        let (row_count, storage) = self.storage();

        ChunkSummary {
            partition_key: Arc::clone(&self.partition_key),
            table_name: Arc::clone(&self.table_name),
            id: self.id,
            storage,
            estimated_bytes: self.size(),
            row_count,
            time_of_first_write: self.time_of_first_write,
            time_of_last_write: self.time_of_last_write,
            time_closed: self.time_closed,
        }
    }

    /// Return information about the storage in this Chunk
    pub fn detailed_summary(&self) -> DetailedChunkSummary {
        let inner = self.summary();

        fn to_summary(v: (&str, usize)) -> ChunkColumnSummary {
            ChunkColumnSummary {
                name: v.0.into(),
                estimated_bytes: v.1,
            }
        }

        let columns: Vec<ChunkColumnSummary> = match &self.stage {
            ChunkStage::Frozen { representation, .. } => match &representation {
                ChunkStageFrozenRepr::MutableBufferSnapshot(repr) => {
                    repr.column_sizes().map(to_summary).collect()
                }
                ChunkStageFrozenRepr::ReadBuffer(repr) => repr.column_sizes(&self.table_name),
            },
            ChunkStage::Persisted { read_buffer, .. } => {
                if let Some(read_buffer) = &read_buffer {
                    read_buffer.column_sizes(&self.table_name)
                } else {
                    // TODO parquet statistics
                    vec![]
                }
            }
        };

        DetailedChunkSummary { inner, columns }
    }

    /// Return the summary information about the table stored in this Chunk
    pub fn table_summary(&self) -> Arc<TableSummary> {
        match &self.stage {
            ChunkStage::Frozen { meta, .. } => Arc::clone(&meta.table_summary),
            ChunkStage::Persisted { meta, .. } => Arc::clone(&meta.table_summary),
        }
    }

    /// Returns an approximation of the amount of process memory consumed by the
    /// chunk
    pub fn size(&self) -> usize {
        match &self.stage {
            ChunkStage::Frozen { representation, .. } => match &representation {
                ChunkStageFrozenRepr::MutableBufferSnapshot(repr) => repr.size(),
                ChunkStageFrozenRepr::ReadBuffer(repr) => repr.size(),
            },
            ChunkStage::Persisted {
                parquet,
                read_buffer,
                ..
            } => {
                let mut size = parquet.size();
                if let Some(read_buffer) = &read_buffer {
                    size += read_buffer.size();
                }
                size
            }
        }
    }

    /// Set the chunk to the Moving state, returning a handle to the underlying
    /// storage
    pub fn set_moving(&mut self, registration: &TaskRegistration) -> Result<Arc<MBChunkSnapshot>> {
        match &self.stage {
            ChunkStage::Frozen { representation, .. } => match &representation {
                ChunkStageFrozenRepr::MutableBufferSnapshot(repr) => {
                    let chunk = Arc::clone(repr);
                    self.set_lifecycle_action(ChunkLifecycleAction::Moving, registration)?;

                    self.metrics
                        .state
                        .inc_with_labels(&[KeyValue::new("state", "moving")]);

                    self.metrics.immutable_chunk_size.observe_with_labels(
                        chunk.size() as f64,
                        &[KeyValue::new("state", "moving")],
                    );

                    Ok(chunk)
                }
                ChunkStageFrozenRepr::ReadBuffer(_) => InternalChunkState {
                    partition_key: self.partition_key.as_ref(),
                    table_name: self.table_name.as_ref(),
                    chunk_id: self.id,
                    operation: "setting moving",
                    expected: "Frozen with MutableBufferSnapshot",
                    actual: "Frozen with ReadBuffer",
                }
                .fail(),
            },
            _ => {
                unexpected_state!(self, "setting closed", "Open or Closed", &self.stage)
            }
        }
    }

    /// Set the chunk in the Moved state, setting the underlying
    /// storage handle to db, and discarding the underlying mutable buffer
    /// storage.
    pub fn set_moved(&mut self, chunk: Arc<RBChunk>) -> Result<()> {
        match &mut self.stage {
            ChunkStage::Frozen { representation, .. } => match &representation {
                ChunkStageFrozenRepr::MutableBufferSnapshot(_) => {
                    self.metrics
                        .state
                        .inc_with_labels(&[KeyValue::new("state", "moved")]);

                    self.metrics.immutable_chunk_size.observe_with_labels(
                        chunk.size() as f64,
                        &[KeyValue::new("state", "moved")],
                    );

                    *representation = ChunkStageFrozenRepr::ReadBuffer(chunk);
                    self.finish_lifecycle_action(ChunkLifecycleAction::Moving)?;
                    Ok(())
                }
                ChunkStageFrozenRepr::ReadBuffer(_) => InternalChunkState {
                    partition_key: self.partition_key.as_ref(),
                    table_name: self.table_name.as_ref(),
                    chunk_id: self.id,
                    operation: "setting moved",
                    expected: "Frozen with MutableBufferSnapshot",
                    actual: "Frozen with ReadBuffer",
                }
                .fail(),
            },
            _ => {
                unexpected_state!(self, "setting moved", "Moving", self.stage)
            }
        }
    }

    /// Start lifecycle action that should move the chunk into the _persisted_ stage.
    pub fn set_writing_to_object_store(
        &mut self,
        registration: &TaskRegistration,
    ) -> Result<Arc<RBChunk>> {
        match &self.stage {
            ChunkStage::Frozen { representation, .. } => {
                match &representation {
                    ChunkStageFrozenRepr::MutableBufferSnapshot(_) => {
                        // TODO: ideally we would support all Frozen representations
                        InternalChunkState {
                            partition_key: self.partition_key.as_ref(),
                            table_name: self.table_name.as_ref(),
                            chunk_id: self.id,
                            operation: "setting object store",
                            expected: "Frozen with ReadBuffer",
                            actual: "Frozen with MutableBufferSnapshot",
                        }
                        .fail()
                    }
                    ChunkStageFrozenRepr::ReadBuffer(repr) => {
                        let db = Arc::clone(repr);
                        self.set_lifecycle_action(ChunkLifecycleAction::Persisting, registration)?;
                        self.metrics
                            .state
                            .inc_with_labels(&[KeyValue::new("state", "writing_os")]);
                        Ok(db)
                    }
                }
            }
            _ => {
                unexpected_state!(self, "setting object store", "Moved", self.stage)
            }
        }
    }

    /// Set the chunk to the MovedToObjectStore state, returning a handle to the
    /// underlying storage
    pub fn set_written_to_object_store(&mut self, chunk: Arc<ParquetChunk>) -> Result<()> {
        match &self.stage {
            ChunkStage::Frozen {
                representation,
                meta,
                ..
            } => {
                let meta = Arc::clone(&meta);
                match &representation {
                    ChunkStageFrozenRepr::MutableBufferSnapshot(_) => {
                        // TODO: ideally we would support all Frozen representations
                        InternalChunkState {
                            partition_key: self.partition_key.as_ref(),
                            table_name: self.table_name.as_ref(),
                            chunk_id: self.id,
                            operation: "setting object store",
                            expected: "Frozen with ReadBuffer",
                            actual: "Frozen with MutableBufferSnapshot",
                        }
                        .fail()
                    }
                    ChunkStageFrozenRepr::ReadBuffer(repr) => {
                        let db = Arc::clone(&repr);
                        self.finish_lifecycle_action(ChunkLifecycleAction::Persisting)?;

                        self.metrics
                            .state
                            .inc_with_labels(&[KeyValue::new("state", "rub_and_os")]);

                        self.metrics.immutable_chunk_size.observe_with_labels(
                            (chunk.size() + db.size()) as f64,
                            &[KeyValue::new("state", "rub_and_os")],
                        );

                        self.stage = ChunkStage::Persisted {
                            meta,
                            parquet: chunk,
                            read_buffer: Some(db),
                        };
                        Ok(())
                    }
                }
            }
            _ => {
                unexpected_state!(
                    self,
                    "setting object store",
                    "MovingToObjectStore",
                    self.stage
                )
            }
        }
    }

    pub fn set_unload_from_read_buffer(&mut self) -> Result<Arc<RBChunk>> {
        match &mut self.stage {
            ChunkStage::Persisted {
                parquet,
                read_buffer,
                ..
            } => {
                if let Some(read_buffer_inner) = &read_buffer {
                    self.metrics
                        .state
                        .inc_with_labels(&[KeyValue::new("state", "os")]);

                    self.metrics.immutable_chunk_size.observe_with_labels(
                        parquet.size() as f64,
                        &[KeyValue::new("state", "os")],
                    );

                    let rub_chunk = Arc::clone(read_buffer_inner);
                    *read_buffer = None;
                    Ok(rub_chunk)
                } else {
                    // TODO: do we really need to error here or should unloading an unloaded chunk be a no-op?
                    InternalChunkState {
                        partition_key: self.partition_key.as_ref(),
                        table_name: self.table_name.as_ref(),
                        chunk_id: self.id,
                        operation: "setting unload",
                        expected: "Persisted with ReadBuffer",
                        actual: "Persisted without ReadBuffer",
                    }
                    .fail()
                }
            }
            _ => {
                unexpected_state!(self, "setting unload", "WrittenToObjectStore", &self.stage)
            }
        }
    }

    /// Set the chunk's in progress lifecycle action or return an error if already in-progress
    fn set_lifecycle_action(
        &mut self,
        lifecycle_action: ChunkLifecycleAction,
        registration: &TaskRegistration,
    ) -> Result<()> {
        if let Some(lifecycle_action) = &self.lifecycle_action {
            return Err(Error::LifecycleActionAlreadyInProgress {
                partition_key: self.partition_key.to_string(),
                table_name: self.table_name.to_string(),
                chunk_id: self.id,
                lifecycle_action: lifecycle_action.metadata().name().to_string(),
            });
        }
        self.lifecycle_action = Some(registration.clone().into_tracker(lifecycle_action));
        Ok(())
    }

    /// Clear the chunk's lifecycle action or return an error if it doesn't match that provided
    fn finish_lifecycle_action(&mut self, lifecycle_action: ChunkLifecycleAction) -> Result<()> {
        match &self.lifecycle_action {
            Some(actual) if actual.metadata() == &lifecycle_action => {}
            actual => {
                return Err(Error::UnexpectedLifecycleAction {
                    partition_key: self.partition_key.to_string(),
                    table_name: self.table_name.to_string(),
                    chunk_id: self.id,
                    expected: lifecycle_action.name().to_string(),
                    actual: actual
                        .as_ref()
                        .map(|x| x.metadata().name())
                        .unwrap_or("None")
                        .to_string(),
                })
            }
        }
        self.lifecycle_action = None;
        Ok(())
    }

    /// Abort the current lifecycle action if any
    ///
    /// Returns an error if the lifecycle action is still running
    pub fn clear_lifecycle_action(&mut self) -> Result<()> {
        if let Some(tracker) = &self.lifecycle_action {
            if !tracker.is_complete() {
                return Err(Error::IncompleteLifecycleAction {
                    partition_key: self.partition_key.to_string(),
                    chunk_id: self.id,
                    action: tracker.metadata().name().to_string(),
                });
            }
            self.lifecycle_action = None
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use parquet_file::{
        chunk::ParquetChunk,
        test_utils::{make_chunk as make_parquet_chunk_with_store, make_object_store},
    };

    use super::*;

    #[tokio::test]
    async fn test_lifecycle_action() {
        let mut chunk = make_persisted_chunk().await;
        let registration = TaskRegistration::new();

        // no action to begin with
        assert!(chunk.lifecycle_action().is_none());

        // set some action
        chunk
            .set_lifecycle_action(ChunkLifecycleAction::Moving, &registration)
            .unwrap();
        assert_eq!(
            *chunk.lifecycle_action().unwrap().metadata(),
            ChunkLifecycleAction::Moving
        );

        // setting an action while there is one running fails
        assert_eq!(chunk.set_lifecycle_action(ChunkLifecycleAction::Moving, &registration).unwrap_err().to_string(), "Internal Error: A lifecycle action \'Moving to the Read Buffer\' is already in progress for  part1:table1:0");

        // finishing the wrong action fails
        assert_eq!(chunk.finish_lifecycle_action(ChunkLifecycleAction::Compacting).unwrap_err().to_string(), "Internal Error: Unexpected chunk state for part1:table1:0. Expected Compacting, got Moving to the Read Buffer");

        // finish some action
        chunk
            .finish_lifecycle_action(ChunkLifecycleAction::Moving)
            .unwrap();

        // finishing w/o any action in progress will fail
        assert_eq!(chunk.finish_lifecycle_action(ChunkLifecycleAction::Moving).unwrap_err().to_string(), "Internal Error: Unexpected chunk state for part1:table1:0. Expected Moving to the Read Buffer, got None");

        // now we can set another action
        chunk
            .set_lifecycle_action(ChunkLifecycleAction::Compacting, &registration)
            .unwrap();
        assert_eq!(
            *chunk.lifecycle_action().unwrap().metadata(),
            ChunkLifecycleAction::Compacting
        );
    }

    async fn make_parquet_chunk(chunk_id: u32) -> ParquetChunk {
        let object_store = make_object_store();
        make_parquet_chunk_with_store(object_store, "foo", chunk_id).await
    }

    async fn make_persisted_chunk() -> CatalogChunk {
        let partition_key = "part1";
        let chunk_id = 0;

        // assemble ParquetChunk
        let parquet_chunk = make_parquet_chunk(chunk_id).await;

        CatalogChunk::new_object_store_only(
            chunk_id,
            partition_key,
            Arc::new(parquet_chunk),
            ChunkMetrics::new_unregistered(),
        )
    }
}
