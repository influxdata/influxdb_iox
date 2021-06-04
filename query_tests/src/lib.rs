//! This module contains "end-to-end" tests for the query layer.
//!
//! These tests consist of loading the same data in several
//! "scenarios" (different distributions across the Mutable Buffer,
//! Immutable Buffer, and (eventually) Parquet files, running queries
//! against it and verifying the same answer is produced in all scenarios

// Actual tests

#[cfg(test)]
pub mod influxrpc;
#[cfg(test)]
pub mod pruning;
#[cfg(test)]
pub mod scenarios;
#[cfg(test)]
pub mod sql;
#[cfg(test)]
pub mod table_schema;

/// Utility modules used by benchmarks (always compiled)
pub mod utils;
