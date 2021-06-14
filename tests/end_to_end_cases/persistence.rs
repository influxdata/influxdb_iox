use arrow_util::assert_batches_eq;
use data_types::chunk_metadata::{ChunkStorage, ChunkSummary};
//use generated_types::influxdata::iox::management::v1::*;
use influxdb_iox_client::{management::generated_types::LifecycleRules, operations};

use super::scenario::{
    collect_query, create_quickly_persisting_database, create_readable_database, rand_name,
};
use crate::common::server_fixture::ServerFixture;
use std::{collections::HashMap, convert::TryInto};

#[tokio::test]
async fn test_chunk_is_persisted_automatically() {
    let fixture = ServerFixture::create_shared().await;
    let mut write_client = fixture.write_client();

    let db_name = rand_name();
    create_quickly_persisting_database(&db_name, fixture.grpc_channel()).await;

    // Stream in a write that should exceed the limit
    let lp_lines: Vec<_> = (0..1_000)
        .map(|i| format!("data,tag1=val{} x={} {}", i, i * 10, i))
        .collect();

    let num_lines_written = write_client
        .write(&db_name, lp_lines.join("\n"))
        .await
        .expect("successful write");
    assert_eq!(num_lines_written, 1000);

    wait_for_chunk_storage(
        &fixture,
        &db_name,
        ChunkStorage::ReadBufferAndObjectStore,
        std::time::Duration::from_secs(5),
    )
    .await;
}

#[tokio::test]
async fn test_chunk_are_removed_from_memory_when_soft_limit_is_hit() {
    let fixture = ServerFixture::create_shared().await;
    let mut write_client = fixture.write_client();

    let db_name = rand_name();
    create_quickly_persisting_database(&db_name, fixture.grpc_channel()).await;

    // write in more chunks that exceed the soft limit (512K) and
    // expect that at least one ends up on object store but not in memory

    // (as of time of writing, 10 chunks took up ~800K)
    let num_chunks = 10;
    for _ in 0..num_chunks {
        let lp_lines: Vec<_> = (0..1_000)
            .map(|i| format!("data,tag1=val{} x={} {}", i, i * 10, i))
            .collect();
        let num_lines_written = write_client
            .write(&db_name, lp_lines.join("\n"))
            .await
            .expect("successful write");
        assert_eq!(num_lines_written, 1000);
    }

    // make sure that at least one is in object store
    wait_for_chunk_storage(
        &fixture,
        &db_name,
        ChunkStorage::ObjectStoreOnly,
        std::time::Duration::from_secs(5),
    )
    .await;

    // Also ensure that all 10 chunks are still there
    let chunks = list_chunks(&fixture, &db_name).await;
    assert_eq!(
        num_chunks,
        chunks.len(),
        "expected {} chunks but had {}: {:#?}",
        num_chunks,
        chunks.len(),
        chunks
    );
}

#[tokio::test]
async fn test_chunks_are_removed_only_until_soft_limit() {
    // Ensure that when chunks are evicted, only enough are evicted to get under the limit
    let fixture = ServerFixture::create_shared().await;
    let mut management_client = fixture.management_client();
    let mut write_client = fixture.write_client();

    let db_name = rand_name();
    create_quickly_persisting_database(&db_name, fixture.grpc_channel()).await;

    // setup soft limit to 1M (so we can write in all chunks without hitting that)
    let mut lifecycle_rules = LifecycleRules {
        mutable_linger_seconds: 1,
        mutable_size_threshold: 100,
        buffer_size_soft: 1024 * 1024, // 1MB
        buffer_size_hard: 1024 * 1024, // 1MB
        persist: true,
        ..Default::default()
    };

    let mut rules = management_client.get_database(&db_name).await.unwrap();
    rules.lifecycle_rules = Some(lifecycle_rules.clone());
    management_client
        .update_database(rules.clone())
        .await
        .unwrap();

    // write in 10 chunks, and expect that some are persisted
    // (as of time of writing, 10 chunks took up ~800K)
    let num_chunks: usize = 10;
    for _ in 0..num_chunks {
        let lp_lines: Vec<_> = (0..1_000)
            .map(|i| format!("data,tag1=val{} x={} {}", i, i * 10, i))
            .collect();
        let num_lines_written = write_client
            .write(&db_name, lp_lines.join("\n"))
            .await
            .expect("successful write");
        assert_eq!(num_lines_written, 1000);
    }

    // Ensure that 10 chunks have been persisted
    let expected: HashMap<ChunkStorage, usize> = vec![(ChunkStorage::ReadBufferAndObjectStore, 10)]
        .into_iter()
        .collect();
    wait_for_counts(&fixture, &db_name, expected).await;

    // Now, crank down the lifecycle rules so that some (but not all) must be evicted
    lifecycle_rules.buffer_size_soft = 512 * 1024; // 512K
    rules.lifecycle_rules = Some(lifecycle_rules);
    management_client.update_database(rules).await.unwrap();

    // Ensure that only the chunks needed to hit the soft limit
    // were unloaded -- there should still be some in the read buffer.
    let expected: HashMap<ChunkStorage, usize> = vec![
        (ChunkStorage::ObjectStoreOnly, 4),
        (ChunkStorage::ReadBufferAndObjectStore, 6),
    ]
    .into_iter()
    .collect();

    wait_for_counts(&fixture, &db_name, expected).await;
}

async fn wait_for_counts(
    fixture: &ServerFixture,
    db_name: &str,
    expected_counts: HashMap<ChunkStorage, usize>,
) {
    wait_for_chunks(
        &fixture,
        &db_name,
        std::time::Duration::from_secs(5),
        |chunks| {
            let counts = chunks_to_counts(chunks);
            println!("Looking for\n{:#?}\nactual\n{:#?}", expected_counts, counts);
            counts == expected_counts
        },
    )
    .await
}

/// return a map from storage --> number of chunks in that state
fn chunks_to_counts(chunks: &[ChunkSummary]) -> HashMap<ChunkStorage, usize> {
    chunks.iter().fold(HashMap::new(), |mut counts, c| {
        let count = counts.entry(c.storage).or_default();
        *count += 1;
        counts
    })
}

#[tokio::test]
async fn test_query_chunk_after_restart() {
    // fixtures
    let fixture = ServerFixture::create_single_use().await;
    let server_id = 42;
    let db_name = rand_name();

    // set server ID
    let mut management_client = fixture.management_client();
    management_client
        .update_server_id(server_id)
        .await
        .expect("set ID failed");
    fixture.wait_server_initialized().await;

    // create DB and a RB chunk
    create_readable_database(&db_name, fixture.grpc_channel()).await;
    let chunk_id = create_readbuffer_chunk(&fixture, &db_name).await;

    // enable persistance
    let mut rules = management_client.get_database(&db_name).await.unwrap();
    rules.lifecycle_rules = Some({
        let mut lifecycle_rules = rules.lifecycle_rules.unwrap();
        lifecycle_rules.persist = true;
        lifecycle_rules
    });
    management_client.update_database(rules).await.unwrap();

    // wait for persistence
    wait_for_persisted_chunk(
        &fixture,
        &db_name,
        chunk_id,
        std::time::Duration::from_secs(10),
    )
    .await;

    // check before restart
    assert_chunk_query_works(&fixture, &db_name).await;

    // restart server
    let fixture = fixture.restart_server().await;
    fixture.wait_server_initialized().await;

    // query data after restart
    assert_chunk_query_works(&fixture, &db_name).await;
}

/// Create a closed read buffer chunk and return its id
async fn create_readbuffer_chunk(fixture: &ServerFixture, db_name: &str) -> u32 {
    use influxdb_iox_client::management::generated_types::operation_metadata::Job;

    let mut management_client = fixture.management_client();
    let mut write_client = fixture.write_client();
    let mut operations_client = fixture.operations_client();

    let partition_key = "cpu";
    let table_name = "cpu";
    let lp_lines = vec!["cpu,region=west user=23.2 100"];

    write_client
        .write(db_name, lp_lines.join("\n"))
        .await
        .expect("write succeded");

    let chunks = list_chunks(fixture, db_name).await;

    assert_eq!(chunks.len(), 1, "Chunks: {:#?}", chunks);
    let chunk_id = chunks[0].id;
    assert_eq!(chunks[0].storage, ChunkStorage::OpenMutableBuffer);

    // Move the chunk to read buffer
    let operation = management_client
        .close_partition_chunk(db_name, table_name, partition_key, 0)
        .await
        .expect("new partition chunk");

    println!("Operation response is {:?}", operation);
    let operation_id = operation.id();

    let meta = operations::ClientOperation::try_new(operation)
        .unwrap()
        .metadata();

    // ensure we got a legit job description back
    if let Some(Job::CloseChunk(close_chunk)) = meta.job {
        assert_eq!(close_chunk.db_name, db_name);
        assert_eq!(close_chunk.partition_key, partition_key);
        assert_eq!(close_chunk.chunk_id, 0);
    } else {
        panic!("unexpected job returned")
    };

    // wait for the job to be done
    operations_client
        .wait_operation(operation_id, Some(std::time::Duration::from_secs(1)))
        .await
        .expect("failed to wait operation");

    // And now the chunk  should be good
    let mut chunks = list_chunks(fixture, db_name).await;
    chunks.sort_by(|c1, c2| c1.id.cmp(&c2.id));

    assert_eq!(chunks.len(), 1, "Chunks: {:#?}", chunks);
    assert_eq!(chunks[0].id, chunk_id);
    assert_eq!(chunks[0].storage, ChunkStorage::ReadBuffer);

    chunk_id
}

// Wait for the specified chunk to be persisted to object store
async fn wait_for_persisted_chunk(
    fixture: &ServerFixture,
    db_name: &str,
    chunk_id: u32,
    wait_time: std::time::Duration,
) {
    let t_start = std::time::Instant::now();

    loop {
        let chunks = list_chunks(fixture, db_name).await;

        let chunk = chunks.iter().find(|chunk| chunk.id == chunk_id).unwrap();
        if (chunk.storage == ChunkStorage::ReadBufferAndObjectStore)
            || (chunk.storage == ChunkStorage::ObjectStoreOnly)
        {
            return;
        }

        assert!(t_start.elapsed() < wait_time);
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
}

// Wait for at least one chunk to be in the specified storage state
async fn wait_for_chunk_storage(
    fixture: &ServerFixture,
    db_name: &str,
    desired_storage: ChunkStorage,
    wait_time: std::time::Duration,
) {
    wait_for_chunks(fixture, db_name, wait_time, |chunks| {
        // Log the current status of the chunks
        for chunk in chunks {
            println!(
                "chunk {} partition {} storage:{:?}",
                chunk.id, chunk.partition_key, chunk.storage
            );
        }

        chunks.iter().any(|chunk| chunk.storage == desired_storage)
    })
    .await
}

// Wait until pred returns true or the wait time is exceeded
async fn wait_for_chunks<P>(
    fixture: &ServerFixture,
    db_name: &str,
    wait_time: std::time::Duration,
    pred: P,
) where
    P: Fn(&[ChunkSummary]) -> bool,
{
    let t_start = std::time::Instant::now();

    loop {
        let chunks = list_chunks(fixture, db_name).await;

        if pred(&chunks) {
            return;
        }

        assert!(
            t_start.elapsed() < wait_time,
            "Could not find chunk that mated predicate within {:?}. Chunks: {:#?}",
            wait_time,
            chunks
        );

        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
}

async fn assert_chunk_query_works(fixture: &ServerFixture, db_name: &str) {
    let mut client = fixture.flight_client();
    let sql_query = "select region, user, time from cpu";

    let query_results = client.perform_query(db_name, sql_query).await.unwrap();

    let batches = collect_query(query_results).await;
    let expected_read_data = vec![
        "+--------+------+-------------------------------+",
        "| region | user | time                          |",
        "+--------+------+-------------------------------+",
        "| west   | 23.2 | 1970-01-01 00:00:00.000000100 |",
        "+--------+------+-------------------------------+",
    ];

    assert_batches_eq!(expected_read_data, &batches);
}

/// Gets the list of ChunkSummaries from the server
async fn list_chunks(fixture: &ServerFixture, db_name: &str) -> Vec<ChunkSummary> {
    let mut management_client = fixture.management_client();
    let chunks = management_client.list_chunks(db_name).await.unwrap();

    chunks.into_iter().map(|c| c.try_into().unwrap()).collect()
}
