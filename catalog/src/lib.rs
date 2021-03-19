//! This module contains the implementation of the InfluxDB IOx Metadata catalog
#![deny(rust_2018_idioms)]
#![warn(
    missing_debug_implementations,
    clippy::explicit_iter_loop,
    clippy::use_self,
    clippy::clone_on_ref_ptr
)]
use std::collections::{btree_map::Entry, BTreeMap};

//use snafu::{OptionExt, ResultExt, Snafu};
use snafu::Snafu;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("unknown chunk {}:{}", partition_key, chunk_id))]
    NoSuchChunk {
        partition_key: String,
        chunk_id: u32,
    },
    #[snafu(display(
        "internal error: can not create pre-existing partition: {}",
        partition_key
    ))]
    InternalPartitionAlreadyExists { partition_key: String },
}
pub type Result<T, E = Error> = std::result::Result<T, E>;

pub mod chunk;
pub mod partition;

use chunk::Chunk;
use partition::Partition;

/// InfluxDB IOx Metadata Catalog
///
/// The Catalog stores information such as which chunks exist, what
/// state they are in, and what objects on object store are used, etc.
///
/// The catalog is also responsible for (eventually) persisting this
/// information as well as ensuring that references between different
/// objects remain valid (e.g. that the `partition_key` field of all
/// Chunk's refer to valid partitions).
#[derive(Debug, Default)]
pub struct Catalog {
    /// The set of chunks in this database. The key is the partition
    /// key, the values are the chunks for that partition
    chunks: BTreeMap<String, Vec<Chunk>>,

    /// key is id, value is Partition data
    partitions: BTreeMap<String, Partition>,
}

impl Catalog {
    pub fn new() -> Self {
        Self {
            ..Default::default()
        }
    }

    /// return an immutable chunk reference given the specified partition and
    /// chunk id
    pub fn chunk(&self, partition_key: impl AsRef<str>, chunk_id: u32) -> Option<&Chunk> {
        // todo: build some sort of index so we don't have to look at all chunks
        let partition_key = partition_key.as_ref();
        self.chunks
            .get(partition_key)
            .map(|chunks| chunks.iter().find(|c| c.id() == chunk_id))
            .unwrap_or(None)
    }

    /// return an mutable chunk reference given the specified partition and
    /// chunk id
    pub fn chunk_mut(
        &mut self,
        partition_key: impl AsRef<str>,
        chunk_id: u32,
    ) -> Option<&mut Chunk> {
        // todo: build some sort of index so we don't have to look at all chunks
        let partition_key = partition_key.as_ref();
        self.chunks
            .get_mut(partition_key)
            .map(|chunks| chunks.iter_mut().find(|c| c.id() == chunk_id))
            .unwrap_or(None)
    }

    /// Creates a new chunk with the catalog
    ///
    /// This function also validates 'referential integrity' - aka that the
    /// partition
    // referred to by this chunk exists in the catalog
    pub fn create_chunk(&mut self, partition_key: &str, id: u32) -> Result<()> {
        let chunks = self.chunks.get_mut(partition_key);

        // TODO return a proper error here if there is no partititon
        let chunks = chunks.expect("No such partition");

        // Ensure this chunk doesn't already exist
        if chunks.iter().find(|c| c.id() == id).is_some() {
            panic!("chunk already exists");
        }

        chunks.push(Chunk::new(partition_key, id));
        Ok(())
    }

    /// Removes the specified chunk from the catalog, returning the existing
    /// chunk, if any
    pub fn drop_chunk(&mut self, partition_key: &str, id: u32) -> Option<Chunk> {
        let chunks = self.chunks.get_mut(partition_key);

        // TODO return a proper error here if there is no partititon
        let chunks = chunks.expect("No such partition");

        let idx = chunks
            .iter()
            .enumerate()
            .filter(|(_, c)| c.id() == id)
            .map(|(i, _)| i)
            .next();
        if let Some(idx) = idx {
            Some(chunks.remove(idx))
        } else {
            None
        }
    }

    /// List all chunks in this database
    pub fn chunks(&self) -> impl Iterator<Item = &Chunk> {
        self.chunks.values().map(|chunks| chunks.iter()).flatten()
    }

    /// List all chunks in a particular partition
    pub fn partition_chunks(&self, key: &str) -> impl Iterator<Item = &Chunk> {
        self.chunks
            .get(key)
            .into_iter()
            .map(|chunks| chunks.iter())
            .flatten()
    }

    // List all partitions in this dataase
    pub fn partitions(&self) -> impl Iterator<Item = &Partition> {
        self.partitions.values()
    }

    // Get a specific partition by name
    pub fn partition(&self, key: impl AsRef<str>) -> Option<&Partition> {
        let key = key.as_ref();
        self.partitions.get(key)
    }

    // Create a new partition in the catalog, returning an error if it already
    // exists
    pub fn create_partition(&mut self, key: impl Into<String>) -> Result<()> {
        let key = key.into();

        let entry = self.partitions.entry(key);
        if matches!(entry, Entry::Occupied(_)) {
            return InternalPartitionAlreadyExists {
                partition_key: entry.key(),
            }
            .fail();
        }

        let chunks = self.chunks.insert(entry.key().to_string(), Vec::new());
        //TODO proper error handling (shouldn't had a previous chunks entry)
        assert!(chunks.is_none());

        let partition = Partition::new(entry.key());
        entry.or_insert(partition);

        Ok(())
    }
}

#[cfg(test)]
mod tests {}
