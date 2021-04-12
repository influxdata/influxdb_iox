use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use std::task::{Context, Poll};

use futures::Stream;

use arrow_deps::arrow::datatypes::SchemaRef;
use arrow_deps::arrow::{error::Result as ArrowResult, record_batch::RecordBatch};
use arrow_deps::datafusion::physical_plan::{RecordBatchStream, SendableRecordBatchStream};
use data_types::timestamp::TimestampRange;
use internal_types::schema::{Schema, TIME_COLUMN_NAME};
use internal_types::selection::Selection;
use snafu::{OptionExt, ResultExt, Snafu};

use super::Chunk;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Table not found: {}", table_name))]
    TableNotFound { table_name: String },

    #[snafu(display("Failed to select columns: {}", source))]
    SelectColumns {
        source: internal_types::schema::Error,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// A queryable snapshot of a mutable buffer chunk
#[derive(Debug)]
pub struct ChunkSnapshot {
    id: u32,
    records: HashMap<String, TableSnapshot>,
    // TODO: Memory tracking
}

#[derive(Debug)]
struct TableSnapshot {
    schema: Schema,
    batch: RecordBatch,
    timestamp_range: Option<TimestampRange>,
}

impl TableSnapshot {
    fn matches_predicate(&self, timestamp_range: &Option<TimestampRange>) -> bool {
        match (self.timestamp_range, timestamp_range) {
            (Some(a), Some(b)) => !a.disjoint(b),
            (None, Some(_)) => false, /* If this chunk doesn't have a time column it can't match */
            // the predicate
            (_, None) => true,
        }
    }
}

impl ChunkSnapshot {
    pub fn new(chunk: &Chunk) -> Self {
        let mut records: HashMap<String, TableSnapshot> = Default::default();
        for (id, table) in &chunk.tables {
            let schema = table.schema(chunk, Selection::All).unwrap();
            let batch = table.to_arrow(chunk, Selection::All).unwrap();
            let name = chunk.dictionary.lookup_id(*id).unwrap();

            let timestamp_range = chunk
                .dictionary
                .lookup_value(TIME_COLUMN_NAME)
                .ok()
                .and_then(|column_id| {
                    table.column(column_id).ok().and_then(|column| {
                        // TimestampRange has an exclusive upper bound
                        column
                            .get_i64_stats()
                            .map(|x| TimestampRange::new(x.min, x.max + 1))
                    })
                });

            records.insert(
                name.to_string(),
                TableSnapshot {
                    batch,
                    schema,
                    timestamp_range,
                },
            );
        }

        Self {
            id: chunk.id,
            records,
        }
    }

    /// return the ID of this chunk
    pub fn id(&self) -> u32 {
        self.id
    }

    /// returns true if there is no data in this chunk
    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    /// Return true if this snapshot has the specified table name
    pub fn has_table(&self, table_name: &str) -> bool {
        self.records.get(table_name).is_some()
    }

    /// Return Schema for the specified table / columns
    pub fn table_schema(&self, table_name: &str, selection: Selection<'_>) -> Result<Schema> {
        let table = self
            .records
            .get(table_name)
            .context(TableNotFound { table_name })?;

        Ok(match selection {
            Selection::All => table.schema.clone(),
            Selection::Some(columns) => {
                let columns = table.schema.select(columns).context(SelectColumns)?;
                table.schema.project(&columns)
            }
        })
    }

    /// Returns a list of tables with writes matching the given timestamp_range
    pub fn table_names(
        &self,
        timestamp_range: Option<TimestampRange>,
    ) -> impl Iterator<Item = &String> + '_ {
        self.records
            .iter()
            .flat_map(move |(table_name, table_snapshot)| {
                match table_snapshot.matches_predicate(&timestamp_range) {
                    true => Some(table_name),
                    false => None,
                }
            })
    }

    /// Returns a SendableRecordBatchStream for use by DataFusion
    pub fn read_filter(
        &self,
        table_name: &str,
        selection: Selection<'_>,
    ) -> Result<SendableRecordBatchStream> {
        let table = self
            .records
            .get(table_name)
            .context(TableNotFound { table_name })?;

        let (schema, batch) = match selection {
            Selection::All => (table.schema.as_arrow(), table.batch.clone()),
            Selection::Some(columns) => {
                let projection = table.schema.select(columns).context(SelectColumns)?;
                let schema = table.schema.project(&projection).into();
                let columns = projection
                    .into_iter()
                    .map(|x| Arc::clone(table.batch.column(x)))
                    .collect();

                let batch = RecordBatch::try_new(Arc::clone(&schema), columns).unwrap();
                (schema, batch)
            }
        };

        Ok(Box::pin(ChunkSnapshotStream {
            schema,
            batch: Some(batch),
        }))
    }

    /// Returns a given selection of column names from a table
    pub fn column_names(
        &self,
        table_name: &str,
        selection: Selection<'_>,
    ) -> Option<BTreeSet<String>> {
        let table = self.records.get(table_name)?;
        let fields = table.schema.inner().fields().iter();

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
}

#[derive(Debug)]
struct ChunkSnapshotStream {
    schema: SchemaRef,
    batch: Option<RecordBatch>,
}

impl RecordBatchStream for ChunkSnapshotStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

impl Stream for ChunkSnapshotStream {
    type Item = ArrowResult<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        Poll::Ready(self.batch.take().map(Ok))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (1, Some(1))
    }
}
