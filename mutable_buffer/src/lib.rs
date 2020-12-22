//! Contains an in memory mutable buffer that stores incoming data in
//! a structure that is designed to be quickly appended to as well as queried
//!
//! The mutable buffer is

#![deny(rust_2018_idioms)]
#![warn(
    missing_copy_implementations,
    missing_debug_implementations,
    clippy::explicit_iter_loop,
    clippy::use_self
)]

mod column;
mod database;
mod dictionary;
pub mod partition;
mod store;
mod table;

// Allow restore partitions to be used outside of this crate (for
// benchmarking)
pub use crate::database::MutableBufferDb;
pub use crate::partition::restore_partitions_from_wal;
pub use crate::store::MutableBufferDatabases;
