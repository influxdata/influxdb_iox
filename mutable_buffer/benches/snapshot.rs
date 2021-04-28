use std::{convert::TryFrom, io::Read};

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use data_types::server_id::ServerId;
use flate2::read::GzDecoder;
use internal_types::entry::{test_helpers::lp_to_entries, ClockValue};
use mutable_buffer::chunk::Chunk;
use tracker::MemRegistry;

#[inline]
fn snapshot_chunk(chunk: &Chunk) {
    let _ = chunk.snapshot();
}

fn chunk(count: usize) -> Chunk {
    let mut chunk = Chunk::new(0, &MemRegistry::new());

    let raw = include_bytes!("../../tests/fixtures/lineproto/tag_values.lp.gz");
    let mut gz = GzDecoder::new(&raw[..]);
    let mut lp = String::new();
    gz.read_to_string(&mut lp).unwrap();

    for _ in 0..count {
        for entry in lp_to_entries(&lp) {
            for write in entry.partition_writes().iter().flatten() {
                chunk
                    .write_table_batches(
                        ClockValue::try_from(5).unwrap(),
                        ServerId::try_from(1).unwrap(),
                        write.table_batches().as_slice(),
                    )
                    .unwrap();
            }
        }
    }

    chunk
}

pub fn snapshot_mb(c: &mut Criterion) {
    let mut group = c.benchmark_group("snapshot_mb");
    for count in &[1, 2, 3, 4, 5] {
        let chunk = chunk(*count as _);
        group.bench_function(BenchmarkId::from_parameter(count), |b| {
            b.iter(|| snapshot_chunk(&chunk));
        });
    }
    group.finish();
}

criterion_group!(benches, snapshot_mb);
criterion_main!(benches);
