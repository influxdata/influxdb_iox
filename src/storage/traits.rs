//! This module defines the traits by which the rest of Delorean
//! interacts with the storage system. This helps define a clear
//! interface as well as being able to test other parts of Delorean
//! using mockups that conform to these traits

use std::{fmt::Debug, sync::Arc};

use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use delorean_line_parser::ParsedLine;
use snafu::Snafu;

#[derive(Debug, Snafu)]
/// Opaque error type that holds an actual implementation's error
pub enum Error {
    #[snafu(display("Underlying Database Error: {:?}", source))]
    DatabaseError {
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

#[async_trait]
pub trait Database: Debug + Send + Sync {
    /// writes parsed lines into this database
    async fn write_lines(&self, lines: &[ParsedLine<'_>]) -> Result<(), Error>;

    /// Execute the specified query and return arrow record batches with the result
    async fn query(&self, query: &str) -> Result<Vec<RecordBatch>, Error>;

    /// Fetch the specified table names and columns as Arrow RecordBatches
    async fn table_to_arrow(
        &self,
        table_name: &str,
        columns: &[&str],
    ) -> Result<Vec<RecordBatch>, Error>;
}

#[async_trait]
pub trait DatabaseStore: Debug + Send + Sync {
    type Database: Database;

    /// Retrieve the database specified by the org and bucket name,
    /// returning None if no such database exists
    ///
    /// TODO: change this to take a single database name, and move the
    /// computation of org/bucket to the callers
    async fn db(&self, org: &str, bucket: &str) -> Option<Arc<Self::Database>>;

    /// Retrieve the database specified by the org and bucket name,
    /// creating it if it doesn't exist.
    ///
    /// TODO: change this to take a single database name, and move the computation of org/bucket
    /// to the callers
    async fn db_or_create(&self, org: &str, bucket: &str) -> Result<Arc<Self::Database>, Error>;
}

/// return the database name to use for the specified org and bucket name.
///
/// TODO move to somewhere else / change the traits to take the database name directly
pub fn org_and_bucket_to_database(org: &str, bucket: &str) -> String {
    org.to_owned() + "_" + bucket
}
