use std::convert::TryInto;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use parking_lot::{RwLock, RwLockUpgradableReadGuard};
use tracing::{error, info};

use data_types::{database_rules::LifecycleRules, error::ErrorLogger, job::Job};

use crate::tracker::Tracker;

use super::{
    catalog::chunk::{Chunk, ChunkState},
    Db,
};

/// Handles the lifecycle of chunks within a Db
pub struct LifecycleManager {
    db: Arc<Db>,
    move_task: Option<Tracker<Job>>,
}

impl LifecycleManager {
    pub fn new(db: Arc<Db>) -> Self {
        Self {
            db,
            move_task: None,
        }
    }

    /// Polls the lifecycle manager to do work
    ///
    /// Should be called periodically and should spawn any long-running
    /// work onto the tokio threadpool and return
    pub fn poll(&mut self) {
        ChunkMover::poll(self, Utc::now())
    }
}

/// A trait that encapsulates the core chunk lifecycle logic
///
/// This is to enable independent testing of the policy logic
trait ChunkMover {
    /// Returns the size of a chunk - overridden for testing
    fn chunk_size(chunk: &Chunk) -> usize {
        chunk.size()
    }

    /// Returns the lifecycle policy
    fn rules(&self) -> &LifecycleRules;

    /// Returns a list of chunks sorted in the order
    /// they should prioritised
    fn chunks(&self) -> Vec<Arc<RwLock<Chunk>>>;

    /// Returns a boolean indicating if a move is in progress
    fn is_move_active(&self) -> bool;

    /// Starts an operation to move a chunk to the read buffer
    fn move_to_read_buffer(&mut self, partition_key: String, chunk_id: u32);

    /// Drops a chunk from the database
    fn drop_chunk(&mut self, partition_key: String, chunk_id: u32);

    /// The core policy logic
    fn poll(&mut self, now: DateTime<Utc>) {
        let chunks = self.chunks();

        let mut buffer_size = 0;

        // Only want to start a new move task if there isn't one already in-flight
        //
        // Note: This does not take into account manually triggered tasks
        let mut move_active = self.is_move_active();

        // Iterate through the chunks to determine
        // - total memory consumption
        // - any chunks to move

        // TODO: Track size globally to avoid iterating through all chunks
        for chunk in &chunks {
            let chunk_guard = chunk.upgradable_read();
            buffer_size += Self::chunk_size(&*chunk_guard);

            if !move_active && can_move(self.rules(), &*chunk_guard, now) {
                match chunk_guard.state() {
                    ChunkState::Open(_) => {
                        let mut chunk_guard = RwLockUpgradableReadGuard::upgrade(chunk_guard);
                        chunk_guard.set_closing().expect("cannot close open chunk");

                        let partition_key = chunk_guard.key().to_string();
                        let chunk_id = chunk_guard.id();

                        std::mem::drop(chunk_guard);

                        move_active = true;
                        self.move_to_read_buffer(partition_key, chunk_id);
                    }
                    ChunkState::Closing(_) => {
                        let partition_key = chunk_guard.key().to_string();
                        let chunk_id = chunk_guard.id();

                        std::mem::drop(chunk_guard);

                        move_active = true;
                        self.move_to_read_buffer(partition_key, chunk_id);
                    }
                    _ => {}
                }
            }

            // TODO: Find and recover cancelled move jobs
        }

        if let Some(soft_limit) = self.rules().buffer_size_soft {
            let mut chunks = chunks.iter();

            while buffer_size > soft_limit.get() {
                match chunks.next() {
                    Some(chunk) => {
                        let chunk_guard = chunk.read();
                        if self.rules().drop_non_persisted
                            || matches!(chunk_guard.state(), ChunkState::Moved(_))
                        {
                            let partition_key = chunk_guard.key().to_string();
                            let chunk_id = chunk_guard.id();
                            buffer_size =
                                buffer_size.saturating_sub(Self::chunk_size(&*chunk_guard));

                            std::mem::drop(chunk_guard);

                            self.drop_chunk(partition_key, chunk_id)
                        }
                    }
                    None => {
                        error!("failed to find chunk to evict");
                        break;
                    }
                }
            }
        }
    }
}

impl ChunkMover for LifecycleManager {
    fn rules(&self) -> &LifecycleRules {
        &self.db.rules.lifecycle_rules
    }

    fn chunks(&self) -> Vec<Arc<RwLock<Chunk>>> {
        self.db.catalog.chunks_sorted_by(&self.rules().sort_order)
    }

    fn is_move_active(&self) -> bool {
        self.move_task
            .as_ref()
            .map(|x| !x.is_complete())
            .unwrap_or(false)
    }

    fn move_to_read_buffer(&mut self, partition_key: String, chunk_id: u32) {
        info!(%partition_key, %chunk_id, "moving chunk to read buffer");
        self.move_task = Some(
            self.db
                .load_chunk_to_read_buffer_in_background(partition_key, chunk_id),
        )
    }

    fn drop_chunk(&mut self, partition_key: String, chunk_id: u32) {
        info!(%partition_key, %chunk_id, "dropping chunk");
        let _ = self
            .db
            .drop_chunk(&partition_key, chunk_id)
            .log_if_error("dropping chunk to free up memory");
    }
}

/// Returns the number of seconds between two times
///
/// Computes a - b
fn elapsed_seconds(a: DateTime<Utc>, b: DateTime<Utc>) -> u32 {
    let seconds = (a - b).num_seconds();
    if seconds < 0 {
        0 // This can occur as DateTime is not monotonic
    } else {
        seconds.try_into().unwrap_or(u32::max_value())
    }
}

/// Returns if the chunk is sufficiently cold and old to move
///
/// Note: Does not check the chunk is the correct state
fn can_move(rules: &LifecycleRules, chunk: &Chunk, now: DateTime<Utc>) -> bool {
    match (rules.mutable_linger_seconds, chunk.time_of_last_write()) {
        (Some(linger), Some(last_write)) if elapsed_seconds(now, last_write) >= linger.get() => {
            match (
                rules.mutable_minimum_age_seconds,
                chunk.time_of_first_write(),
            ) {
                (Some(min_age), Some(first_write)) => {
                    // Chunk can be moved if it is old enough
                    elapsed_seconds(now, first_write) >= min_age.get()
                }
                // If no minimum age set - permit chunk movement
                (None, _) => true,
                (_, None) => unreachable!("chunk with last write and no first write"),
            }
        }

        // Disable movement if no mutable_linger set,
        // or the chunk is empty, or the linger hasn't expired
        _ => false,
    }
}

#[cfg(test)]
mod tests;
