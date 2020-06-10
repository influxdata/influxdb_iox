use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

use delorean_query_benchmarks::{generate_test_data, query, Column};
use std::time::Duration;

fn select_where(c: &mut Criterion) {
    let batch_count = 1;
    let record_count = 1000000;
    let total_records = (batch_count * record_count) as u64;
    let mut columns = vec![];

    for i in 1..50 as usize {
        let cardinality = i.pow(5);

        columns.push(Column::new_i64(
            format!("i64_{}", i).to_string(),
            Some(cardinality),
            false,
        ));
    }

    for i in 1..51 {
        columns.push(Column::new_f64(
            format!("f64_{}", i).to_string(),
            None,
            false,
        ))
    }

    columns.push(Column::new_i64("ts".to_string(), None, false));

    let (schema, data) = generate_test_data(batch_count, record_count, columns);

    let mut group = c.benchmark_group("select_where");
    group.sample_size(10);
    group.measurement_time(Duration::new(10, 0));
    for q in [
        "select count(i64_1) from foo",
        "select min(i64_1), max(i64_1) from foo",
        "select min(i64_1), max(f64_1) from foo",
        "select count(i64_1) from foo where ts > 99",
        "select count(i64_1) from foo where ts > 99 and ts < 120",
        "select count(i64_1) from foo where (ts > 99 and ts < 500) and i64_2 > 5 and i64_3 > 10 and i64_4 > 100",
    ].iter() {
        group.throughput(Throughput::Elements(total_records));
        group.bench_with_input(BenchmarkId::from_parameter(q), q, |b, q| {
            b.iter(|| query(q, "foo", schema.clone(), data.clone()).unwrap());
        });
    }
    group.finish();
}

fn group_by(c: &mut Criterion) {
    let batch_count = 1;
    let record_count = 1000000;
    let total_records = (batch_count * record_count) as u64;
    let mut columns = vec![];

    for i in 1..50 as usize {
        let cardinality = i.pow(5);

        columns.push(Column::new_i64(
            format!("i64_{}", i).to_string(),
            Some(cardinality),
            false,
        ));
    }

    for i in 1..51 {
        columns.push(Column::new_f64(
            format!("f64_{}", i).to_string(),
            None,
            false,
        ))
    }

    columns.push(Column::new_i64("ts".to_string(), None, false));

    let (schema, data) = generate_test_data(batch_count, record_count, columns);

    let mut group = c.benchmark_group("select_where");
    group.sample_size(10);
    group.measurement_time(Duration::new(10, 0));
    for q in [
        "select i64_1, count(i64_1) from foo group by i64_1",
        "select i64_1, count(i64_1) from foo where ts > 99 and ts < 50000 group by i64_10",
        "select i64_1, count(i64_1) from foo where ts > 99 and ts < 50000 group by i64_12",
        // "select i64_1, i64_2, i64_3, i64_4, i64_5, count(i64_1) from foo where ts > 99 and ts < 50000 group by i64_1, i64_2, i64_3, i64_4, i64_5"
    ]
    .iter()
    {
        group.throughput(Throughput::Elements(total_records));
        group.bench_with_input(BenchmarkId::from_parameter(q), q, |b, q| {
            b.iter(|| query(q, "foo", schema.clone(), data.clone()).unwrap());
        });
    }
    group.finish();
}

criterion_group!(benches, select_where, group_by);

criterion_main!(benches);
