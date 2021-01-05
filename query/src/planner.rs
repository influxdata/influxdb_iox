//! The main query planners of InfluxDB IOx

/// Plans queries agains the InfluxDB Line Protocol data model (`ParsedLine`
/// structures) and provides an interface to query that data. The query methods
/// on this trait such as `tag_columns are specific to this data model.
///
/// The IOx storage engine implements this trait to provide Timeseries
/// specific queries, but also provides more generic access to the
/// same underlying data via other interfaces (e.g. SQL).
///
/// The InfluxDB Timeseries data model can can be thought of as a
/// relational database table where each column has both a type as
/// well as one of the following categories:
///
/// * Tag (always String type)
/// * Field (Float64, Int64, UInt64, String, or Bool)
/// * Time (Int64)
///
/// While the underlying storage is the same for columns in different
/// categories with the same data type, columns of different
/// categories are treated differently in the different query types.
#[derive(Debug)]
pub struct TSQueryPlanner {
    // Example methods:
//
// async fn table_names(&self, database: impl Database, predicate: Predicate) ->
// Result<StringSetPlan>; async fn tag_column_names(&self, database: impl Database, predicate:
// Predicate) -> Result<StringSetPlan>; ...
}

/// Plans queries as SQL against databases
#[derive(Debug)]
pub struct SQLQueryPlanner {
    // Example methods:
//async fn query(&self, database: &impl Database, query: &str) -> Result<Vec<RecordBatch>>;
}
