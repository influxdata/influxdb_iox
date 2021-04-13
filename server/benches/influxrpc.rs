use arrow_deps::datafusion::physical_plan::hash_join::PartitionMode::Partitioned;
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use data_types::database_rules::PartitionTemplate;
use influxdb_line_protocol::parse_lines;
use internal_types::data::{lines_to_replicated_write, replicated_write_from_file};
use tracker::MemRegistry;
use server::db::Db;
use server::query_tests::utils::make_db;
use query::Database;
use query::frontend::influxrpc::InfluxRPCPlanner;
use query::exec::Executor;


fn db(count: usize) -> Db {
    let partitioner = PartitionTemplate::default();
    let write =
        replicated_write_from_file("../tests/fixtures/lineproto/metrics.lp", &partitioner).unwrap();
    let db = make_db();

    for _ in 0..count {
        db.store_replicated_write(&write);
    }

    db
}

pub fn tag_column_values(c: &mut Criterion) {
    let mut group = c.benchmark_group("tag_column_values");
    for count in &[50, 100, 200, 300, 400, 500, 1000] {
        let db = db(*count as _);
        let size = db.chunk_summaries().unwrap().into_iter().map(|x| x.estimated_bytes as u64).sum();

        let runtime = tokio::runtime::Runtime::new().unwrap();

        group.throughput(Throughput::Bytes(size));
        group.bench_function(BenchmarkId::from_parameter(size), |b| {
            b.to_async(&runtime).iter(|| async {
                let planner = InfluxRPCPlanner::new();
                let executor = Executor::new();

                let plan = planner
                    .tag_keys(&db, Default::default())
                    .await
                    .expect("built plan successfully");

                let _ = executor.to_string_set(plan).await.expect("failed to get strings from plan");
            });
        });
    }
    group.finish();
}

pub fn tag_keys(c: &mut Criterion) {
    let mut group = c.benchmark_group("tag_keys");
    for count in &[50, 100, 200, 300, 400, 500, 1000] {
        let db = db(*count as _);
        let size = db.chunk_summaries().unwrap().into_iter().map(|x| x.estimated_bytes as u64).sum();

        let runtime = tokio::runtime::Runtime::new().unwrap();

        group.throughput(Throughput::Bytes(size));
        group.bench_function(BenchmarkId::from_parameter(size), |b| {
            b.to_async(&runtime).iter(|| async {
                let planner = InfluxRPCPlanner::new();
                let executor = Executor::new();

                let plan = planner
                    .tag_keys(&db, Default::default())
                    .await
                    .expect("built plan successfully");

                let _ = executor.to_string_set(plan).await.expect("failed to get strings from plan");
            });
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    tag_column_values,
    tag_keys
);
criterion_main!(benches);
