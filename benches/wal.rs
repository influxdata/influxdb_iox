use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use rand::{Rng, SeedableRng};
use std::fs::File;
use std::io::{BufWriter, Write};
use tempfile::TempDir;
use wal::{Wal, WalOptions};

fn wal(c: &mut Criterion) {
    let mut group = c.benchmark_group("wal");

    let mut rng = rand::rngs::StdRng::seed_from_u64(1337);
    let bytes: Vec<u8> = (0..8196).map(|_| rng.gen()).collect();

    group.throughput(Throughput::Bytes(bytes.len() as u64));

    group.bench_function("append", |b| {
        let dir = TempDir::new().unwrap();
        let wal = Wal::with_options(
            dir.path().to_path_buf(),
            WalOptions::default()
                .sync_writes(false)
                .rollover_size(u64::MAX),
        )
        .unwrap();
        b.iter(|| wal.append(&bytes).unwrap())
    });

    group.bench_function("buffered_write", |b| {
        let dir = TempDir::new().unwrap();
        let mut writer =
            BufWriter::new(File::create(dir.path().to_path_buf().join("test.data")).unwrap());
        b.iter(|| writer.write_all(&bytes).unwrap())
    });

    #[cfg(unix)]
    {
        group.bench_function("zero_overhead_write", |b| {
            use std::os::unix::fs::FileExt;
            let dir = TempDir::new().unwrap();
            let file = File::create(dir.path().to_path_buf().join("test.data")).unwrap();
            let mut offset = 0;
            b.iter(|| {
                file.write_all_at(&bytes, offset).unwrap();
                offset += bytes.len() as u64;
            })
        });
    }

    group.finish();
}

criterion_group!(benches, wal);

criterion_main!(benches);
