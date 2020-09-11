//! This module defines the traits by which the rest of Delorean
//! interacts with the storage system. This helps define a clear
//! interface as well as being able to test other parts of Delorean
//! using mockups that conform to these traits

use std::{fmt::Debug, sync::Arc};

use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use delorean_line_parser::ParsedLine;

#[async_trait]
/// A `Database` stores data and provides an interface to query that data.
pub trait Database: Debug + Send + Sync {
    type Error: std::error::Error + Send + Sync + 'static;

    /// writes parsed lines into this database
    async fn write_lines(&self, lines: &[ParsedLine<'_>]) -> Result<(), Self::Error>;

    /// Execute the specified query and return arrow record batches with the result
    async fn query(&self, query: &str) -> Result<Vec<RecordBatch>, Self::Error>;

    /// Fetch the specified table names and columns as Arrow RecordBatches
    async fn table_to_arrow(
        &self,
        table_name: &str,
        columns: &[&str],
    ) -> Result<Vec<RecordBatch>, Self::Error>;
}

#[async_trait]
/// Storage for `Databases` which can be retrieved by name
pub trait DatabaseStore: Debug + Send + Sync {
    /// The type of database that is stored by this DatabaseStore
    type Database: Database;

    /// The type of error this DataBase store generates
    type Error: std::error::Error + Send + Sync + 'static;

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
    async fn db_or_create(
        &self,
        org: &str,
        bucket: &str,
    ) -> Result<Arc<Self::Database>, Self::Error>;
}

/// return the database name to use for the specified org and bucket name.
///
/// TODO move to somewhere else / change the traits to take the database name directly
pub fn org_and_bucket_to_database(org: &str, bucket: &str) -> String {
    org.to_owned() + "_" + bucket
}
