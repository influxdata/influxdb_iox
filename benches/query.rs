use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

use delorean_query_benchmarks::{generate_wide_high_cardinality_data, query};
use std::time::Duration;

fn query_select_where(c: &mut Criterion) {
    let batch_count = 1;
    let record_count = 1000000;
    let total_records = (batch_count * record_count) as u64;
    let (schema, data) = generate_wide_high_cardinality_data(batch_count, record_count);

    let mut group = c.benchmark_group("query_select_where");
    group.sample_size(10);
    group.measurement_time(Duration::new(10, 0));

    let queries = [
        "select count(i64_1) from foo",
        "select min(i64_1), max(i64_1) from foo",
        "select min(i64_1), max(f64_1) from foo",
        "select count(i64_1) from foo where ts > 99",
        "select count(i64_1) from foo where ts > 99 and i64_49 = 12345",
        "select count(i64_1) from foo where ts > 99 and ts < 120",
        "select count(i64_1) from foo where (ts > 99 and ts < 500) and i64_2 > 5 and i64_3 > 10 and i64_4 > 100",
    ];

    for q in &queries {
        group.throughput(Throughput::Elements(total_records));
        group.bench_with_input(BenchmarkId::from_parameter(q), q, |b, q| {
            b.iter(|| query(q, "foo", schema.clone(), data.clone()).unwrap());
        });
    }
    group.finish();
}

fn query_group_by(c: &mut Criterion) {
    let batch_count = 1;
    let record_count = 1000000;
    let total_records = (batch_count * record_count) as u64;
    let (schema, data) = generate_wide_high_cardinality_data(batch_count, record_count);

    let mut group = c.benchmark_group("query_group_by");
    group.sample_size(10);
    group.measurement_time(Duration::new(10, 0));

    let queries = [
        "select i64_1, count(i64_1) from foo group by i64_1",
        "select i64_1, count(i64_1) from foo where ts > 99 and ts < 50000 group by i64_10",
        "select i64_1, count(i64_1) from foo where ts > 99 and ts < 50000 group by i64_12",
        // "select i64_1, i64_2, i64_3, i64_4, i64_5, count(i64_1) from foo where ts > 99 and ts < 50000 group by i64_1, i64_2, i64_3, i64_4, i64_5"
    ];

    for q in &queries {
        group.throughput(Throughput::Elements(total_records));
        group.bench_with_input(BenchmarkId::from_parameter(q), q, |b, q| {
            b.iter(|| query(q, "foo", schema.clone(), data.clone()).unwrap());
        });
    }
    group.finish();
}

criterion_group!(benches, query_select_where, query_group_by);

criterion_main!(benches);
