use snafu::{ResultExt, Snafu};
use std::{collections::BTreeSet, sync::Arc};

use chrono::{DateTime, Utc};
use crate::{metadata::{IoxMetadata, IoxParquetMetaData}, storage::Storage};
use data_types::{
    partition_metadata::{Statistics, TableSummary},
    timestamp::TimestampRange,
};
use datafusion::physical_plan::SendableRecordBatchStream;
use internal_types::{
    schema::{Schema, TIME_COLUMN_NAME},
    selection::Selection,
};
use object_store::{path::Path, ObjectStore};
use query::predicate::Predicate;

use metrics::GaugeValue;
use std::mem;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Table '{}' not found in chunk", table_name))]
    NamedTableNotFoundInChunk { table_name: String },

    #[snafu(display("Failed to read parquet: {}", source))]
    ReadParquet { source: crate::storage::Error },

    #[snafu(display("Failed to select columns: {}", source))]
    SelectColumns {
        source: internal_types::schema::Error,
    },

    #[snafu(
        display("Cannot read IOx metadata from {:?}: {}", path, source),
        visibility(pub)
    )]
    IoxMetadataReadFailed {
        source: crate::metadata::Error,
        path: Path,
    },

    #[snafu(
        display("Cannot read schema from {:?}: {}", path, source),
        visibility(pub)
    )]
    SchemaReadFailed {
        source: crate::metadata::Error,
        path: Path,
    },

    #[snafu(
        display("Cannot read statistics from {:?}: {}", path, source),
        visibility(pub)
    )]
    StatisticsReadFailed {
        source: crate::metadata::Error,
        path: Path,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug)]
pub struct ChunkMetrics {
    /// keep track of memory used by chunk
    memory_bytes: GaugeValue,
}

impl ChunkMetrics {
    /// Creates an instance of ChunkMetrics that isn't registered with a central
    /// metrics registry. Observations made to instruments on this ChunkMetrics instance
    /// will therefore not be visible to other ChunkMetrics instances or metric instruments
    /// created on a metrics domain, and vice versa
    pub fn new_unregistered() -> Self {
        Self {
            memory_bytes: GaugeValue::new_unregistered(),
        }
    }

    pub fn new(_metrics: &metrics::Domain, memory_bytes: GaugeValue) -> Self {
        Self { memory_bytes }
    }
}

#[derive(Debug)]
pub struct ParquetChunk {
    /// Partition this chunk belongs to
    partition_key: Arc<str>,

    /// Meta data of the table
    table_summary: Arc<TableSummary>,

    /// Schema that goes with this table's parquet file
    schema: Arc<Schema>,

    /// Timestamp range of this table's parquet file
    /// (extracted from TableSummary)
    timestamp_range: Option<TimestampRange>,

    /// Object store of the above relative path to open and read the file
    object_store: Arc<ObjectStore>,

    /// Path in the object store. Format:
    ///  <writer id>/<database>/data/<partition key>/<chunk
    /// id>/<tablename>.parquet
    object_store_path: Path,

    /// Parquet metadata that can be used checkpoint the catalog state.
    parquet_metadata: Arc<IoxParquetMetaData>,

    metrics: ChunkMetrics,

    time_of_first_write: DateTime<Utc>,
    time_of_last_write: DateTime<Utc>,
}

impl ParquetChunk {
    /// Creates new chunk from given parquet metadata.
    pub fn new(
        file_location: Path,
        store: Arc<ObjectStore>,
        parquet_metadata: Arc<IoxParquetMetaData>,
        metrics: ChunkMetrics,
    ) -> Result<Self> {
        let iox_md = parquet_metadata
            .read_iox_metadata()
            .context(IoxMetadataReadFailed {
                path: &file_location,
            })?;

        let IoxMetadata { table_name, time_of_first_write, time_of_last_write, partition_key, ..} = iox_md;

        let schema = parquet_metadata.read_schema().context(SchemaReadFailed {
            path: &file_location,
        })?;
        let table_summary = parquet_metadata
            .read_statistics(&schema, &table_name)
            .context(StatisticsReadFailed {
                path: &file_location,
            })?;

        Ok(Self::new_from_parts(
            partition_key,
            Arc::new(table_summary),
            schema,
            file_location,
            store,
            parquet_metadata,
            metrics,
            time_of_first_write,
            time_of_last_write,
        ))
    }

    /// Creates a new chunk from given parts w/o parsing anything from the provided parquet metadata.
    pub(crate) fn new_from_parts(
        partition_key: Arc<str>,
        table_summary: Arc<TableSummary>,
        schema: Arc<Schema>,
        file_location: Path,
        store: Arc<ObjectStore>,
        parquet_metadata: Arc<IoxParquetMetaData>,
        metrics: ChunkMetrics,
        time_of_first_write: DateTime<Utc>,
        time_of_last_write: DateTime<Utc>,
    ) -> Self {
        let timestamp_range = extract_range(&table_summary);

        let mut chunk = Self {
            partition_key,
            table_summary,
            schema,
            timestamp_range,
            object_store: store,
            object_store_path: file_location,
            parquet_metadata,
            metrics,
            time_of_first_write,
            time_of_last_write,
        };

        chunk.metrics.memory_bytes.set(chunk.size());
        chunk
    }

    /// Return the chunk's partition key
    pub fn partition_key(&self) -> &str {
        self.partition_key.as_ref()
    }

    /// Return object store path for this chunk
    pub fn path(&self) -> Path {
        self.object_store_path.clone()
    }

    /// Returns the summary statistics for this chunk
    pub fn table_summary(&self) -> &Arc<TableSummary> {
        &self.table_summary
    }

    /// Returns the name of the table this chunk holds
    pub fn table_name(&self) -> &str {
        &self.table_summary.name
    }

    /// Return the approximate memory size of the chunk, in bytes including the
    /// dictionary, tables, and their rows.
    pub fn size(&self) -> usize {
        mem::size_of::<Self>()
            + self.partition_key.len()
            + self.table_summary.size()
            + mem::size_of_val(&self.schema.as_ref())
            + mem::size_of_val(&self.object_store_path)
            + mem::size_of_val(&self.parquet_metadata)
    }

    /// Infallably return the full schema (for all columns) for this chunk
    pub fn schema(&self) -> Arc<Schema> {
        Arc::clone(&self.schema)
    }

    // Return true if this chunk contains values within the time range
    pub fn has_timerange(&self, timestamp_range: Option<&TimestampRange>) -> bool {
        match (self.timestamp_range, timestamp_range) {
            (Some(a), Some(b)) => !a.disjoint(b),
            (None, Some(_)) => false, /* If this chunk doesn't have a time column it can't match */
            // the predicate
            (_, None) => true,
        }
    }

    // Return the columns names that belong to the given column
    // selection
    pub fn column_names(&self, selection: Selection<'_>) -> Option<BTreeSet<String>> {
        let fields = self.schema.inner().fields().iter();

        Some(match selection {
            Selection::Some(cols) => fields
                .filter_map(|x| {
                    if cols.contains(&x.name().as_str()) {
                        Some(x.name().clone())
                    } else {
                        None
                    }
                })
                .collect(),
            Selection::All => fields.map(|x| x.name().clone()).collect(),
        })
    }

    /// Return stream of data read from parquet file
    pub fn read_filter(
        &self,
        predicate: &Predicate,
        selection: Selection<'_>,
    ) -> Result<SendableRecordBatchStream> {
        Storage::read_filter(
            predicate,
            selection,
            Arc::clone(&self.schema.as_arrow()),
            self.object_store_path.clone(),
            Arc::clone(&self.object_store),
        )
        .context(ReadParquet)
    }

    /// The total number of rows in all row groups in this chunk.
    pub fn rows(&self) -> usize {
        self.parquet_metadata.row_count()
    }

    /// Parquet metadata from the underlying file.
    pub fn parquet_metadata(&self) -> Arc<IoxParquetMetaData> {
        Arc::clone(&self.parquet_metadata)
    }

    pub fn time_of_first_write(&self) -> Option<DateTime<Utc>> { Some(self.time_of_first_write) }
    pub fn time_of_last_write(&self) -> Option<DateTime<Utc>> { Some(self.time_of_last_write) }
}

/// Extracts min/max values of the timestamp column, from the TableSummary, if possible
fn extract_range(table_summary: &TableSummary) -> Option<TimestampRange> {
    table_summary
        .column(TIME_COLUMN_NAME)
        .map(|c| {
            if let Statistics::I64(s) = &c.stats {
                if let (Some(min), Some(max)) = (s.min, s.max) {
                    return Some(TimestampRange::new(min, max));
                }
            }
            None
        })
        .flatten()
}
