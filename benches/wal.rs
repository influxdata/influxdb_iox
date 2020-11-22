use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use tempfile::TempDir;
use wal::{Wal, WalOptions};
use rand::{SeedableRng, Rng};

fn wal(c: &mut Criterion) {
    let mut group = c.benchmark_group("wal");

    let mut rng = rand::rngs::StdRng::seed_from_u64(1337);
    let bytes: Vec<u8> = (0..4096).map(|_| { rng.gen() }).collect();

    group.throughput(Throughput::Bytes(bytes.len() as u64));

    group.bench_function("append", |b| {
        let dir = TempDir::new().unwrap();
        let wal = Wal::with_options(
            dir.path().to_path_buf(),
            WalOptions::default().sync_writes(false).rollover_size(u64::MAX),
        )
        .unwrap();
        b.iter(|| wal.append(&bytes))
    });

    group.finish();
}

criterion_group!(benches, wal);

criterion_main!(benches);
