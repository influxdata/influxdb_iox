use std::any::Any;
use std::convert::AsRef;
use std::iter::FromIterator;
use std::sync::Arc;

use chrono::{DateTime, Utc};

use arrow_deps::{
    arrow::{
        array::{
            Array, ArrayRef, StringArray, StringBuilder, Time64NanosecondArray,
            TimestampNanosecondArray, UInt32Array, UInt64Array, UInt64Builder,
        },
        datatypes::{Field, Schema},
        error::Result,
        record_batch::RecordBatch,
    },
    datafusion::{
        catalog::schema::SchemaProvider,
        datasource::{MemTable, TableProvider},
    },
};
use data_types::{
    chunk::ChunkSummary, error::ErrorLogger, job::Job, partition_metadata::PartitionSummary,
};
use tracker::TaskTracker;

use super::catalog::Catalog;
use crate::JobRegistry;

// The IOx system schema
pub const SYSTEM_SCHEMA: &str = "system";

const CHUNKS: &str = "chunks";
const COLUMNS: &str = "columns";
const OPERATIONS: &str = "operations";

#[derive(Debug)]
pub struct SystemSchemaProvider {
    db_name: String,
    catalog: Arc<Catalog>,
    jobs: Arc<JobRegistry>,
}

impl SystemSchemaProvider {
    pub fn new(db_name: impl Into<String>, catalog: Arc<Catalog>, jobs: Arc<JobRegistry>) -> Self {
        let db_name = db_name.into();
        Self {
            db_name,
            catalog,
            jobs,
        }
    }
}

impl SchemaProvider for SystemSchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self as &dyn Any
    }

    fn table_names(&self) -> Vec<String> {
        vec![
            CHUNKS.to_string(),
            COLUMNS.to_string(),
            OPERATIONS.to_string(),
        ]
    }

    fn table(&self, name: &str) -> Option<Arc<dyn TableProvider>> {
        // TODO: Use of a MemTable potentially results in materializing redundant data
        let batch = match name {
            CHUNKS => from_chunk_summaries(self.catalog.chunk_summaries())
                .log_if_error("chunks table")
                .ok()?,
            COLUMNS => from_partition_summaries(self.catalog.partition_summaries())
                .log_if_error("chunks table")
                .ok()?,
            OPERATIONS => from_task_trackers(&self.db_name, self.jobs.tracked())
                .log_if_error("operations table")
                .ok()?,
            _ => return None,
        };

        let table = MemTable::try_new(batch.schema(), vec![vec![batch]])
            .log_if_error("constructing chunks system table")
            .ok()?;

        Some(Arc::<MemTable>::new(table))
    }
}

// TODO: Use a custom proc macro or serde to reduce the boilerplate
fn time_to_ts(time: Option<DateTime<Utc>>) -> Option<i64> {
    time.map(|ts| ts.timestamp_nanos())
}

fn from_chunk_summaries(chunks: Vec<ChunkSummary>) -> Result<RecordBatch> {
    let id = UInt32Array::from_iter(chunks.iter().map(|c| Some(c.id)));
    let partition_key =
        StringArray::from_iter(chunks.iter().map(|c| Some(c.partition_key.as_ref())));
    let table_name = StringArray::from_iter(chunks.iter().map(|c| Some(c.table_name.as_ref())));
    let storage = StringArray::from_iter(chunks.iter().map(|c| Some(c.storage.as_str())));
    let estimated_bytes =
        UInt64Array::from_iter(chunks.iter().map(|c| Some(c.estimated_bytes as u64)));
    let row_counts = UInt64Array::from_iter(chunks.iter().map(|c| Some(c.row_count as u64)));
    let time_of_first_write = TimestampNanosecondArray::from_iter(
        chunks.iter().map(|c| c.time_of_first_write).map(time_to_ts),
    );
    let time_of_last_write = TimestampNanosecondArray::from_iter(
        chunks.iter().map(|c| c.time_of_last_write).map(time_to_ts),
    );
    let time_closing =
        TimestampNanosecondArray::from_iter(chunks.iter().map(|c| c.time_closing).map(time_to_ts));

    RecordBatch::try_from_iter(vec![
        ("id", Arc::new(id) as ArrayRef),
        ("partition_key", Arc::new(partition_key)),
        ("table_name", Arc::new(table_name)),
        ("storage", Arc::new(storage)),
        ("estimated_bytes", Arc::new(estimated_bytes)),
        ("row_count", Arc::new(row_counts)),
        ("time_of_first_write", Arc::new(time_of_first_write)),
        ("time_of_last_write", Arc::new(time_of_last_write)),
        ("time_closing", Arc::new(time_closing)),
    ])
}

fn from_partition_summaries(partitions: Vec<PartitionSummary>) -> Result<RecordBatch> {
    // Assume each partition has roughly 5 tables with 5 columns
    let row_estimate = partitions.len() * 25;

    let mut partition_key = StringBuilder::new(row_estimate);
    let mut table_name = StringBuilder::new(row_estimate);
    let mut column_name = StringBuilder::new(row_estimate);
    let mut count = UInt64Builder::new(row_estimate);

    // Note no rows are produced for partitions with no tabes, or
    // tables with no columns: There are other tables to list tables
    // and columns
    for partition in partitions {
        for table in partition.tables {
            for column in table.columns {
                partition_key.append_value(&partition.key)?;
                table_name.append_value(&table.name)?;
                column_name.append_value(&column.name)?;
                count.append_value(column.count())?;
            }
        }
    }

    let partition_key = partition_key.finish();
    let table_name = table_name.finish();
    let column_name = column_name.finish();
    let count = count.finish();

    let schema = Schema::new(vec![
        Field::new("partition_key", partition_key.data_type().clone(), false),
        Field::new("table_name", table_name.data_type().clone(), true),
        Field::new("column_name", column_name.data_type().clone(), true),
        Field::new("count", count.data_type().clone(), true),
    ]);

    RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(partition_key),
            Arc::new(table_name),
            Arc::new(column_name),
            Arc::new(count),
        ],
    )
}

fn from_task_trackers(db_name: &str, jobs: Vec<TaskTracker<Job>>) -> Result<RecordBatch> {
    let jobs = jobs
        .into_iter()
        .filter(|job| job.metadata().db_name() == Some(db_name))
        .collect::<Vec<_>>();

    let ids = StringArray::from_iter(jobs.iter().map(|job| Some(job.id().to_string())));
    let statuses = StringArray::from_iter(jobs.iter().map(|job| Some(job.get_status().name())));
    let cpu_time_used = Time64NanosecondArray::from_iter(
        jobs.iter()
            .map(|job| job.get_status().cpu_nanos().map(|n| n as i64)),
    );
    let wall_time_used = Time64NanosecondArray::from_iter(
        jobs.iter()
            .map(|job| job.get_status().wall_nanos().map(|n| n as i64)),
    );
    let partition_keys =
        StringArray::from_iter(jobs.iter().map(|job| job.metadata().partition_key()));
    let chunk_ids = UInt32Array::from_iter(jobs.iter().map(|job| job.metadata().chunk_id()));
    let descriptions =
        StringArray::from_iter(jobs.iter().map(|job| Some(job.metadata().description())));

    let schema = Schema::new(vec![
        Field::new("id", ids.data_type().clone(), false),
        Field::new("status", statuses.data_type().clone(), false),
        Field::new("cpu_time_used", cpu_time_used.data_type().clone(), true),
        Field::new("wall_time_used", wall_time_used.data_type().clone(), true),
        Field::new("partition_key", partition_keys.data_type().clone(), true),
        Field::new("chunk_id", chunk_ids.data_type().clone(), true),
        Field::new("description", descriptions.data_type().clone(), true),
    ]);

    RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(ids),
            Arc::new(statuses),
            Arc::new(cpu_time_used),
            Arc::new(wall_time_used),
            Arc::new(partition_keys),
            Arc::new(chunk_ids),
            Arc::new(descriptions),
        ],
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_deps::assert_table_eq;
    use chrono::NaiveDateTime;
    use data_types::chunk::ChunkStorage;
    use data_types::partition_metadata::{ColumnSummary, StatValues, Statistics, TableSummary};

    #[test]
    fn test_from_chunk_summaries() {
        let chunks = vec![
            ChunkSummary {
                partition_key: Arc::new("p1".to_string()),
                table_name: Arc::new("table1".to_string()),
                id: 0,
                storage: ChunkStorage::OpenMutableBuffer,
                estimated_bytes: 23754,
                row_count: 11,
                time_of_first_write: Some(DateTime::from_utc(
                    NaiveDateTime::from_timestamp(10, 0),
                    Utc,
                )),
                time_of_last_write: None,
                time_closing: None,
            },
            ChunkSummary {
                partition_key: Arc::new("p1".to_string()),
                table_name: Arc::new("table1".to_string()),
                id: 0,
                storage: ChunkStorage::OpenMutableBuffer,
                estimated_bytes: 23454,
                row_count: 22,
                time_of_first_write: None,
                time_of_last_write: Some(DateTime::from_utc(
                    NaiveDateTime::from_timestamp(80, 0),
                    Utc,
                )),
                time_closing: None,
            },
        ];

        let expected = vec![
            "+----+---------------+------------+-------------------+-----------------+-----------+---------------------+---------------------+--------------+",
            "| id | partition_key | table_name | storage           | estimated_bytes | row_count | time_of_first_write | time_of_last_write  | time_closing |",
            "+----+---------------+------------+-------------------+-----------------+-----------+---------------------+---------------------+--------------+",
            "| 0  | p1            | table1     | OpenMutableBuffer | 23754           | 11        | 1970-01-01 00:00:10 |                     |              |",
            "| 0  | p1            | table1     | OpenMutableBuffer | 23454           | 22        |                     | 1970-01-01 00:01:20 |              |",
            "+----+---------------+------------+-------------------+-----------------+-----------+---------------------+---------------------+--------------+",
        ];

        let batch = from_chunk_summaries(chunks).unwrap();
        assert_table_eq!(&expected, &[batch]);
    }

    #[test]
    fn test_from_partition_summaries() {
        let partitions = vec![
            PartitionSummary {
                key: "p1".to_string(),
                tables: vec![TableSummary {
                    name: "t1".to_string(),
                    columns: vec![
                        ColumnSummary {
                            name: "c1".to_string(),
                            stats: Statistics::I64(StatValues::new_with_value(23)),
                        },
                        ColumnSummary {
                            name: "c2".to_string(),
                            stats: Statistics::I64(StatValues::new_with_value(43)),
                        },
                    ],
                }],
            },
            PartitionSummary {
                key: "p2".to_string(),
                tables: vec![],
            },
            PartitionSummary {
                key: "p3".to_string(),
                tables: vec![TableSummary {
                    name: "t1".to_string(),
                    columns: vec![],
                }],
            },
        ];

        let expected = vec![
            "+---------------+------------+-------------+-------+",
            "| partition_key | table_name | column_name | count |",
            "+---------------+------------+-------------+-------+",
            "| p1            | t1         | c1          | 1     |",
            "| p1            | t1         | c2          | 1     |",
            "+---------------+------------+-------------+-------+",
        ];

        let batch = from_partition_summaries(partitions).unwrap();
        assert_table_eq!(&expected, &[batch]);
    }
}
