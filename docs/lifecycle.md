# Lifecycle

The IOx lifecycle consists of 3 parts:

* Coordination - coordinate access to chunks
* Policy - determine when to perform certain actions on chunks
* Action - the actions to perform on chunks

## Coordination

The implementation of the coordination component can be found in the [server](../server) crate, in particular [db.rs](../server/src/db.rs) and [catalog](../server/src/db/catalog.rs).

The coordination component should:

* Maintain the data structure on which the lifecycle and query components of IOx operate
* Provide the necessary synchronisation for correct operation of the lifecycle and query components
* Provide correct ordering of chunks for the purposes of de-duplication and upsert resolution

The fundamental unit of data motion within IOx is the chunk, which is a collection of rows for a given partition within a table. 

A chunk can have one of a number of physical representations - e.g. MUB, RUB, LocalDiskParquet, etc... - see the [ChunkStorage](../data_types/src/chunk_metadata.rs) enumeration for the full list of backing representations. 

Various [lifecycle actions](#action) will act on one or more chunks at once, many following the pattern below:

1. Identify chunks within the catalog
2. Perform some semi-expensive operation
3. Swap the result(s) back into catalog, replacing previous chunks

This **must** be a single atomic operation.

The simplest mechanism to achieve this would be to use one or more mutexes to guarantee exclusive access to the chunks whilst the action is in-progress. However, this would block queries from reading these chunks until the operation completes.

An alternative mechanism would be to use a non-owning synchronisation primitive, such as a semaphore, to prevent concurrent lifecycle actions from manipulating a chunk (or partition). Queries wouldn't acquire this and so would be unaffected.

The approach taken is a combination of the two. RWLocks mediate access to the data within the catalog. However, instead of holding locks for the entire lifecycle action, the coordinator records that the chunks have a lifecycle action in progress, before dropping the locks. The coordinator will then refuse to spawn lifecycle actions on chunks with already in progress actions. This is broadly equivalent to using a semaphore, but has a couple of additional benefits:

* Provides information on what is being performed, not just that something is
* Must be explicitly cleared - most commonly by dropping and replacing the chunk

### Potential Improvements

The in-memory catalog currently has more RWLocks than is probably necessary. It currently contains:

* One around the CatalogState
* One around the list of tables
* One around each partition
* One around each chunk

The first two of these may be able to be merged as part of unifying the coordination mechanism for persistence and other lifecycle actions.

Given all queries need to at least check the metadata of the open chunk within a partition, and that lifecycle actions on non-open chunks drop locks as early as possible, it is unclear that chunk-level locks allow significantly greater parallelism than partition-level locks alone. They do, however, complicate the implementation of multi-chunk lifecycle actions. Having a single lock at the partition-level is not only easier to reason about, but avoids the complexities of lock-ordering, and the accompanying risk of deadlocks.

## Action

**The below assumes that the chunk-level write locks have been removed as described in the [section above](#potential-improvements).**

### Freeze

Takes an open MUB chunk and converts it to a frozen MUB chunk. This should be a quick operation and so can take place entirely under the partition write lock. 

### Compact

Takes one or more frozen, un-persisted chunks within a single partition and compacts them into a single chunk in the read buffer.

* With partition write lock:
    * Set lifecycle action on chunks
* Perform datafusion query to yield single, deduplicated stream
* Collect this into a ReadBuffer chunk
* With partition write lock:
    * Drop source chunks
    * Insert new ReadBuffer chunk

### Persist

Takes one or more frozen, un-persisted chunks within a single partition, and a flush timestamp. Yields two chunks in the read buffer, with one containing all the rows with timestamp less than or equal to the flush timestamp, and the other all the rows with a greater timestamp. Then persists the former.

* With partition write lock:
    * Set lifecycle action on chunks
* Perform datafusion query to yield two, deduplicated streams partitioned by the flush timestamp
* Collect the two streams into two ReadBuffer chunks
* With partition write lock:
    * Drop source chunks
    * Insert new ReadBuffer chunks
    * Set lifecycle action on chunk with lower timestamps
* Persist chunk with lower timestamps
* With partition write lock
    * Clear lifecycle action on persisted chunk

## Policy

The implementation of the lifecycle policy can be found in the [lifecycle](../lifecycle) crate.

This provides the automated mechanism for moving data through the various stages in the lifecycle.

**NB: this is not the only way to initiate lifecycle actions, but one of many inputs to the [coordination](#coordination) component outlined above.**

TBC: Flesh out policy section
