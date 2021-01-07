//! This module contains the main IOx Database object which has the
//! instances of the immutable buffer, read buffer, and object store
#![allow(unused_variables)]

use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

use async_trait::async_trait;
use data_types::{data::ReplicatedWrite, database_rules::DatabaseRules};
use mutable_buffer::MutableBufferDb;
use query::Database;
use read_buffer::Database as ReadBufferDb;
use serde::{Deserialize, Serialize};
use snafu::{OptionExt, ResultExt, Snafu};

use crate::buffer::Buffer;

mod chunk;
use chunk::DBChunk;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Cannot write to this database: no mutable buffer configured"))]
    DatatbaseNotWriteable {},

    #[snafu(display("Cannot read to this database: no mutable buffer configured"))]
    DatabaseNotReadable {},

    #[snafu(display("Error rolling partition: {}", source))]
    RollingPartition {
        source: mutable_buffer::database::Error,
    },

    #[snafu(display("Error querying mutable buffer: {}", source))]
    MutableBufferRead {
        source: mutable_buffer::database::Error,
    },

    #[snafu(display("Error writing to mutable buffer: {}", source))]
    MutableBufferWrite {
        source: mutable_buffer::database::Error,
    },
}
pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Serialize, Deserialize)]
/// This is the main IOx Database object. It is the root object of any
/// specific InfluxDB IOx instance
pub struct Db {
    #[serde(flatten)]
    pub rules: DatabaseRules,

    #[serde(skip)]
    /// The (optional) mutable buffer stores incoming writes. If a
    /// database does not have a mutable buffer it can not accept
    /// writes (it is a read replica)
    pub mutable_buffer: Option<Arc<MutableBufferDb>>,

    #[serde(skip)]
    /// The read buffer holds chunk data in an in-memory optimized
    /// format.
    pub read_buffer: Arc<ReadBufferDb>,

    #[serde(skip)]
    wal_buffer: Option<Buffer>,

    #[serde(skip)]
    /// Next write sequence number
    sequence: AtomicU64,
}
impl Db {
    pub fn new(
        rules: DatabaseRules,
        mutable_buffer: Option<Arc<MutableBufferDb>>,
        read_buffer: Arc<ReadBufferDb>,
        wal_buffer: Option<Buffer>,
        sequence: AtomicU64,
    ) -> Self {
        Self {
            rules,
            mutable_buffer,
            read_buffer,
            wal_buffer,
            sequence,
        }
    }

    /// Rolls over the currently active, open chunk in the database's
    /// specified partition to the closed state. Returns the chunk
    /// which was closed.
    pub async fn rollover_partition(&self, partition_key: &str) -> Result<Arc<DBChunk>> {
        if let Some(local_store) = self.mutable_buffer.as_ref() {
            local_store
                .rollover_partition(partition_key)
                .await
                .context(RollingPartition)
                .map(|c| Arc::new(DBChunk::MutableBuffer(c)))
        } else {
            DatatbaseNotWriteable {}.fail()
        }
    }

    /// Return the next write sequence number
    pub fn next_sequence(&self) -> u64 {
        self.sequence.fetch_add(1, Ordering::SeqCst)
    }

    // List Chunks available in a partition. Note this list may have
    // multiple chunks with the same chunk_id if the same data is in
    // multiple places (e.g. immutable_buffer and normal buffer);

    // List closed chunks in the mutable_buffer (that can potentially
    // be migrated into the read buffer or object store)
    pub async fn mutable_buffer_chunks(partition_key: &str) -> Vec<Arc<DBChunk>> {
        todo!();
    }

    // List chunks which are currently present in the read buffer
    pub async fn list_mutable_chunks(partition_key: &str) -> Vec<Arc<DBChunk>> {
        todo!();
    }

    /// migrates a chunk to the read buffer.
    ///
    /// If the chunk is present in the mutable_buffer then it is
    /// loaded from there. Otherwise, the chunk is fetched from the
    /// object store.
    ///
    /// TODO: what happens if there is no more memory available in the read
    /// buffer?
    ///
    /// This (async) function returns when this process is complete,
    /// but the process may take a long time
    pub async fn load_chunk_to_read_buffer(
        partition_key: &str,
        chunk_id: u64,
    ) -> Vec<Arc<DBChunk>> {
        todo!();
    }

    // TODO things like "migrate a chunk
}

impl PartialEq for Db {
    fn eq(&self, other: &Self) -> bool {
        self.rules == other.rules
    }
}
impl Eq for Db {}

#[async_trait]
impl Database for Db {
    type Error = Error;

    // Note that most of these functions will eventually be removed from
    // this trait. For now, pass them directly on to the local store

    async fn store_replicated_write(&self, write: &ReplicatedWrite) -> Result<(), Self::Error> {
        self.mutable_buffer
            .as_ref()
            .context(DatatbaseNotWriteable)?
            .store_replicated_write(write)
            .await
            .context(MutableBufferWrite)
    }

    async fn table_names(
        &self,
        predicate: query::predicate::Predicate,
    ) -> Result<query::exec::StringSetPlan, Self::Error> {
        self.mutable_buffer
            .as_ref()
            .context(DatabaseNotReadable)?
            .table_names(predicate)
            .await
            .context(MutableBufferRead)
    }

    async fn tag_column_names(
        &self,
        predicate: query::predicate::Predicate,
    ) -> Result<query::exec::StringSetPlan, Self::Error> {
        self.mutable_buffer
            .as_ref()
            .context(DatabaseNotReadable)?
            .tag_column_names(predicate)
            .await
            .context(MutableBufferRead)
    }

    async fn field_column_names(
        &self,
        predicate: query::predicate::Predicate,
    ) -> Result<query::exec::FieldListPlan, Self::Error> {
        self.mutable_buffer
            .as_ref()
            .context(DatabaseNotReadable)?
            .field_column_names(predicate)
            .await
            .context(MutableBufferRead)
    }

    async fn column_values(
        &self,
        column_name: &str,
        predicate: query::predicate::Predicate,
    ) -> Result<query::exec::StringSetPlan, Self::Error> {
        self.mutable_buffer
            .as_ref()
            .context(DatabaseNotReadable)?
            .column_values(column_name, predicate)
            .await
            .context(MutableBufferRead)
    }

    async fn query_series(
        &self,
        predicate: query::predicate::Predicate,
    ) -> Result<query::exec::SeriesSetPlans, Self::Error> {
        self.mutable_buffer
            .as_ref()
            .context(DatabaseNotReadable)?
            .query_series(predicate)
            .await
            .context(MutableBufferRead)
    }

    async fn query_groups(
        &self,
        predicate: query::predicate::Predicate,
        gby_agg: query::group_by::GroupByAndAggregate,
    ) -> Result<query::exec::SeriesSetPlans, Self::Error> {
        self.mutable_buffer
            .as_ref()
            .context(DatabaseNotReadable)?
            .query_groups(predicate, gby_agg)
            .await
            .context(MutableBufferRead)
    }

    async fn table_to_arrow(
        &self,
        table_name: &str,
        columns: &[&str],
    ) -> Result<Vec<arrow_deps::arrow::record_batch::RecordBatch>, Self::Error> {
        self.mutable_buffer
            .as_ref()
            .context(DatabaseNotReadable)?
            .table_to_arrow(table_name, columns)
            .await
            .context(MutableBufferRead)
    }

    async fn partition_keys(&self) -> Result<Vec<String>, Self::Error> {
        self.mutable_buffer
            .as_ref()
            .context(DatabaseNotReadable)?
            .partition_keys()
            .await
            .context(MutableBufferRead)
    }

    async fn table_names_for_partition(
        &self,
        partition_key: &str,
    ) -> Result<Vec<String>, Self::Error> {
        self.mutable_buffer
            .as_ref()
            .context(DatabaseNotReadable)?
            .table_names_for_partition(partition_key)
            .await
            .context(MutableBufferRead)
    }
}

#[cfg(test)]
mod tests {
    use arrow_deps::{
        arrow::record_batch::RecordBatch, assert_table_eq, datafusion::physical_plan::collect,
    };
    use query::{
        exec::Executor, frontend::sql::SQLQueryPlanner, test::TestLPWriter, PartitionChunk,
    };
    use test_helpers::assert_contains;

    use super::*;

    /// Create a Database with a local store
    fn make_db() -> Db {
        let name = "test_db";
        Db {
            rules: DatabaseRules::default(),
            mutable_buffer: Some(Arc::new(MutableBufferDb::new(name))),
            read_buffer: Arc::new(ReadBufferDb::new()),
            wal_buffer: None,
            sequence: AtomicU64::new(0),
        }
    }

    #[tokio::test]
    async fn write_no_mutable_buffer() {
        // Validate that writes are rejected if there is no mutable buffer
        let mutable_buffer = None;
        let db = make_db();
        let db = Db {
            mutable_buffer,
            ..db
        };

        let mut writer = TestLPWriter::default();
        let res = writer.write_lp_string(&db, "cpu bar=1 10").await;
        assert_contains!(
            res.unwrap_err().to_string(),
            "Cannot write to this database: no mutable buffer configured"
        );
    }

    #[tokio::test]
    async fn read_write() {
        let db = make_db();
        let mut writer = TestLPWriter::default();
        writer.write_lp_string(&db, "cpu bar=1 10").await.unwrap();

        let batches = run_query(&db, "select * from cpu").await;

        let expected = vec![
            "+-----+------+",
            "| bar | time |",
            "+-----+------+",
            "| 1   | 10   |",
            "+-----+------+",
        ];
        assert_table_eq!(expected, &batches);
    }

    #[tokio::test]
    async fn write_with_rollover() {
        let db = make_db();
        let mut writer = TestLPWriter::default();
        writer.write_lp_string(&db, "cpu bar=1 10").await.unwrap();
        assert_eq!(vec!["1970-01-01T00"], db.partition_keys().await.unwrap());

        let chunk = db.rollover_partition("1970-01-01T00").await.unwrap();
        assert_eq!(chunk.id(), 0);

        let expected = vec![
            "+-----+------+",
            "| bar | time |",
            "+-----+------+",
            "| 1   | 10   |",
            "+-----+------+",
        ];
        let batches = run_query(&db, "select * from cpu").await;
        assert_table_eq!(expected, &batches);

        // add new data
        writer.write_lp_string(&db, "cpu bar=2 20").await.unwrap();
        let expected = vec![
            "+-----+------+",
            "| bar | time |",
            "+-----+------+",
            "| 1   | 10   |",
            "| 2   | 20   |",
            "+-----+------+",
        ];
        let batches = run_query(&db, "select * from cpu").await;
        assert_table_eq!(expected, &batches);

        // And expect that we still get the same thing when data is rolled over again
        let chunk = db.rollover_partition("1970-01-01T00").await.unwrap();
        assert_eq!(chunk.id(), 1);

        let batches = run_query(&db, "select * from cpu").await;
        assert_table_eq!(expected, &batches);
    }

    // run a sql query against the database, returning the results as record batches
    async fn run_query(db: &Db, query: &str) -> Vec<RecordBatch> {
        let planner = SQLQueryPlanner::default();
        let executor = Executor::new();

        let physical_plan = planner
            .query(db, "select * from cpu", &executor)
            .await
            .unwrap();

        collect(physical_plan).await.unwrap()
    }
}
