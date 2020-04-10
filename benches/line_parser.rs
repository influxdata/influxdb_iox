use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use std::time::Duration;

use tremor_runtime::{
    codec::binflux::BInflux,
    preprocessor::{LengthPrefix, Preprocessor},
};

static LINES: &str = include_str!("line-protocol.txt");

fn delorean_line_parser(c: &mut Criterion) {
    let mut group = c.benchmark_group("delorean line parser");

    // group.throughput(Throughput::Elements(LINES.lines().count() as u64));
    group.throughput(Throughput::Bytes(LINES.len() as u64));
    group.measurement_time(Duration::from_secs(30));

    group.bench_function("delorean all lines", |b| {
        b.iter(|| {
            let lines = delorean::line_parser::parse(LINES).unwrap();
            assert_eq!(582, lines.len());
        })
    });

    group.finish();
}

fn tremor_line_parser(c: &mut Criterion) {
    let mut group = c.benchmark_group("tremor line parser");

    // group.throughput(Throughput::Elements(LINES.lines().count() as u64));
    group.throughput(Throughput::Bytes(LINES.len() as u64));
    group.measurement_time(Duration::from_secs(30));

    group.bench_function("tremor all lines", |b| {
        b.iter(|| {
            let values: Vec<_> = LINES
                .lines()
                .map(|line| {
                    tremor_influx::decode::<simd_json::BorrowedValue>(line, 0)
                        .unwrap()
                        .unwrap()
                })
                .collect();
            assert_eq!(554, values.len());
        })
    });

    group.finish();
}

static BINFLUX: &[u8] = include_bytes!("sample.binflux");

fn binflux_parser(c: &mut Criterion) {
    let mut group = c.benchmark_group("binflux parser");

    let mut pre_p = LengthPrefix::default();

    group.throughput(Throughput::Bytes(BINFLUX.len() as u64));
    group.measurement_time(Duration::from_secs(30));

    group.bench_function("all binflux", |b| {
        b.iter(|| {
            let values = pre_p.process(&mut 0, &BINFLUX).unwrap();
            let parsed_values: Vec<_> = values
                .iter()
                .map(|value| BInflux::decode(&value).unwrap())
                .collect();
            assert_eq!(554, parsed_values.len());
        })
    });

    group.finish();
}

criterion_group!(
    benches,
    delorean_line_parser,
    tremor_line_parser,
    binflux_parser
);
criterion_main!(benches);
