//! This crate contains the data types that are shared between InfluxDB IOx
//! servers including replicated data, rules for how data is split up and
//! queried, and what gets stored in the write buffer database.

#![deny(rust_2018_idioms)]
#![warn(
    missing_debug_implementations,
    clippy::explicit_iter_loop,
    clippy::use_self,
    clippy::clone_on_ref_ptr
)]

pub use database_name::*;

/// The name of the timestamp column
//pub const TIME_COLUMN_NAME: &str = "time";

/// The name of the column containing table names returned by a call to
/// `table_names`.
pub const TABLE_NAMES_COLUMN_NAME: &str = "table";

/// The name of the column containing column names returned by a call to
/// `column_names`.
pub const COLUMN_NAMES_COLUMN_NAME: &str = "column";

pub mod chunk;
pub mod database_rules;
pub mod error;
pub mod http;
pub mod job;
pub mod names;
pub mod partition_metadata;
pub mod timestamp;
pub mod wal;

mod database_name;
pub(crate) mod field_validation;
