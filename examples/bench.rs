//! Milestone 13: benchmarks + analysis.
//!
//! Run with `cargo run --release --example bench`. Release mode matters
//! here, fsync-per-write (deliberate, see `wal.rs`) already dominates write
//! latency, an unoptimized build would just add noise on top of that.
//!
//! Caveat that shapes several numbers below: `FLUSH_THRESHOLD_BYTES` is 1KB,
//! chosen back in milestone 3 to make auto-flush easy to trigger in tests.
//! With ~100 byte values that means a new sstable roughly every 8-10 puts,
//! so a "sequential writes" run of any real size produces dozens to
//! hundreds of small sstables, not the handful of large ones a real engine
//! (with MB-scale thresholds) would produce. That's why sstable counts and
//! compaction's effect look more dramatic here than they would in practice,
//! the mechanism being demonstrated is real, the scale is exaggerated.

use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use rand::rng;
use rand::seq::SliceRandom;
use vellumdb::Db;

const VALUE_SIZE: usize = 100;

struct Stats {
    count: usize,
    total: Duration,
    min: Duration,
    p50: Duration,
    p95: Duration,
    p99: Duration,
    max: Duration,
}

fn stats(mut samples: Vec<Duration>) -> Stats {
    samples.sort();
    let count = samples.len();
    let total: Duration = samples.iter().sum();
    let at = |p: f64| samples[(((count - 1) as f64) * p).round() as usize];
    Stats { count, total, min: samples[0], p50: at(0.50), p95: at(0.95), p99: at(0.99), max: samples[count - 1] }
}

impl std::fmt::Display for Stats {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "n={:<5} total={:>8.2?}  min={:>8.2?}  p50={:>8.2?}  p95={:>8.2?}  p99={:>8.2?}  max={:>8.2?}",
            self.count, self.total, self.min, self.p50, self.p95, self.p99, self.max
        )
    }
}

fn header(title: &str) {
    println!("\n=== {title} ===");
}

fn fresh_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("vellumdb_bench_{name}"));
    let _ = fs::remove_dir_all(&dir);
    dir
}

fn dir_size(dir: &Path) -> u64 {
    fs::read_dir(dir)
        .map(|entries| {
            entries.filter_map(|e| e.ok()).filter_map(|e| e.metadata().ok()).map(|m| m.len()).sum()
        })
        .unwrap_or(0)
}

fn value(i: usize) -> Vec<u8> {
    format!("{:0width$}", i, width = VALUE_SIZE).into_bytes()
}

fn bench_sequential_writes(n: usize) -> Stats {
    let dir = fresh_dir("seq_writes");
    let mut db = Db::open(&dir).unwrap();
    let mut samples = Vec::with_capacity(n);
    for i in 0..n {
        let key = format!("key-{i:08}").into_bytes();
        let start = Instant::now();
        db.put(key, value(i)).unwrap();
        samples.push(start.elapsed());
    }
    let s = stats(samples);
    fs::remove_dir_all(&dir).ok();
    s
}

fn bench_random_writes(n: usize) -> Stats {
    let dir = fresh_dir("rand_writes");
    let mut db = Db::open(&dir).unwrap();
    let mut order: Vec<usize> = (0..n).collect();
    order.shuffle(&mut rng());

    let mut samples = Vec::with_capacity(n);
    for i in order {
        let key = format!("key-{i:08}").into_bytes();
        let start = Instant::now();
        db.put(key, value(i)).unwrap();
        samples.push(start.elapsed());
    }
    let s = stats(samples);
    fs::remove_dir_all(&dir).ok();
    s
}

/// Shared setup for the read benchmarks: a populated db, periodically
/// flushed so reads actually have to cross the memtable/sstable boundary
/// instead of everything conveniently sitting in memory.
fn populated_db(dir: &Path, n: usize) -> Db {
    let mut db = Db::open(dir).unwrap();
    for i in 0..n {
        db.put(format!("key-{i:08}").into_bytes(), value(i)).unwrap();
    }
    db
}

fn bench_existing_key_reads(n: usize, lookups: usize) -> Stats {
    let dir = fresh_dir("existing_reads");
    let db = populated_db(&dir, n);

    let mut keys: Vec<usize> = (0..n).collect();
    keys.shuffle(&mut rng());
    keys.truncate(lookups);

    let mut samples = Vec::with_capacity(lookups);
    for i in keys {
        let key = format!("key-{i:08}").into_bytes();
        let start = Instant::now();
        let found = db.get(&key).unwrap();
        samples.push(start.elapsed());
        assert!(found.is_some());
    }
    let s = stats(samples);
    fs::remove_dir_all(&dir).ok();
    s
}

/// The actual point of milestone 5: measure the same missing-key lookups
/// twice against the same populated db, once through the normal Bloom
/// filter path and once with it deliberately bypassed
/// (`get_without_bloom_filters`), so the difference is a real measurement
/// instead of an assumption.
fn bench_missing_key_reads(n: usize, lookups: usize) -> (Stats, Stats, usize) {
    let dir = fresh_dir("missing_reads");
    let db = populated_db(&dir, n);
    let sstable_count = db.sstable_count();

    let mut with_bloom = Vec::with_capacity(lookups);
    for i in 0..lookups {
        let key = format!("missing-{i:08}").into_bytes();
        let start = Instant::now();
        let found = db.get(&key).unwrap();
        with_bloom.push(start.elapsed());
        assert!(found.is_none());
    }

    let mut without_bloom = Vec::with_capacity(lookups);
    for i in 0..lookups {
        let key = format!("missing-{i:08}").into_bytes();
        let start = Instant::now();
        let found = db.get_without_bloom_filters(&key).unwrap();
        without_bloom.push(start.elapsed());
        assert!(found.is_none());
    }

    let result = (stats(with_bloom), stats(without_bloom), sstable_count);
    fs::remove_dir_all(&dir).ok();
    result
}

fn bench_range_scan(n: usize) -> Duration {
    let dir = fresh_dir("range_scan");
    let db = populated_db(&dir, n);

    let start = Instant::now();
    let count = db.range().unwrap().count();
    let elapsed = start.elapsed();
    assert_eq!(count, n);

    fs::remove_dir_all(&dir).ok();
    elapsed
}

/// Write amplification: how many bytes actually got written to sstable
/// files over the workload's lifetime (every flush, plus compaction
/// rewriting merged data) versus the logical size of the live data at the
/// end. Overwrites and periodic compaction are what make these diverge.
fn bench_write_amplification(n: usize, overwrite_rounds: usize) -> (u64, u64, u64) {
    let dir = fresh_dir("write_amp");
    let mut db = Db::open(&dir).unwrap();
    let mut bytes_written_to_sstables = 0u64;

    for round in 0..overwrite_rounds {
        for i in 0..n {
            db.put(format!("key-{i:08}").into_bytes(), value(i + round)).unwrap();
        }
        db.flush().unwrap();
        bytes_written_to_sstables = dir_size(&dir);
    }

    let before_compaction = dir_size(&dir);
    db.compact().unwrap();
    let after_compaction = dir_size(&dir);

    let logical_bytes: u64 =
        (0..n).map(|i| format!("key-{i:08}").len() as u64 + value(i).len() as u64).sum();

    fs::remove_dir_all(&dir).ok();
    (logical_bytes, before_compaction.max(bytes_written_to_sstables), after_compaction)
}

/// Recovery time with a small WAL (flushed regularly, milestone 10's
/// rotation keeps it near-empty) versus a large one (never flushed, so
/// replay has to walk every single write). This is the concrete payoff of
/// WAL rotation: bounded replay instead of "since the database was born".
fn bench_recovery_time(n: usize) -> (Duration, Duration) {
    let flushed_dir = fresh_dir("recovery_flushed");
    {
        let mut db = Db::open(&flushed_dir).unwrap();
        for i in 0..n {
            db.put(format!("key-{i:08}").into_bytes(), value(i)).unwrap();
            if i % 50 == 0 {
                db.flush().unwrap();
            }
        }
    }
    let start = Instant::now();
    drop(Db::open(&flushed_dir).unwrap());
    let flushed_recovery = start.elapsed();
    fs::remove_dir_all(&flushed_dir).ok();

    let unflushed_dir = fresh_dir("recovery_unflushed");
    {
        // Db::new (no dir) never flushes, so every write stays in a real
        // Db's WAL only, simulated here by opening once and never calling
        // flush explicitly. FLUSH_THRESHOLD_BYTES still auto-flushes past
        // 1KB, so to keep the wal genuinely large we write bigger values.
        let mut db = Db::open(&unflushed_dir).unwrap();
        let big_value = vec![b'x'; 2000];
        for i in 0..n {
            db.put(format!("key-{i:08}").into_bytes(), big_value.clone()).unwrap();
        }
    }
    let start = Instant::now();
    drop(Db::open(&unflushed_dir).unwrap());
    let unflushed_recovery = start.elapsed();
    fs::remove_dir_all(&unflushed_dir).ok();

    (flushed_recovery, unflushed_recovery)
}

fn main() {
    println!("vellumdb benchmarks (milestone 13)");
    println!("release mode: {}", !cfg!(debug_assertions));

    header("Sequential writes (n=1000)");
    println!("{}", bench_sequential_writes(1000));
    println!("note: fsync-per-write (no group commit) dominates this, see wal.rs");

    header("Random writes (n=1000)");
    println!("{}", bench_random_writes(1000));

    header("Existing-key reads (db size=1000, 500 lookups)");
    println!("{}", bench_existing_key_reads(1000, 500));

    header("Missing-key reads: with vs without bloom filters (db size=1000, 500 lookups)");
    let (with_bloom, without_bloom, sstables) = bench_missing_key_reads(1000, 500);
    println!("sstables on disk: {sstables}");
    println!("with bloom:    {with_bloom}");
    println!("without bloom: {without_bloom}");
    let speedup = without_bloom.total.as_secs_f64() / with_bloom.total.as_secs_f64();
    println!("bloom filter speedup: {speedup:.1}x total time over {sstables} sstables");

    header("Range scan (n=1000)");
    println!("full scan of 1000 entries took {:?}", bench_range_scan(1000));

    header("Write amplification (n=500 keys, 3 overwrite rounds + compact)");
    let (logical, before_compaction, after_compaction) = bench_write_amplification(500, 3);
    println!("logical live data size:          {logical} bytes");
    println!("on-disk size before compaction:  {before_compaction} bytes");
    println!("on-disk size after compaction:   {after_compaction} bytes");
    println!(
        "write amplification (before compaction / logical): {:.1}x",
        before_compaction as f64 / logical as f64
    );
    println!(
        "compaction reclaimed: {:.1}%",
        100.0 * (1.0 - after_compaction as f64 / before_compaction as f64)
    );

    header("Recovery time: flushed (bounded wal) vs never-flushed (unbounded wal)");
    let (flushed, unflushed) = bench_recovery_time(500);
    println!("open() after regular flushing:  {flushed:?}");
    println!("open() with everything in wal:  {unflushed:?}");
    println!(
        "wal rotation speedup: {:.1}x",
        unflushed.as_secs_f64() / flushed.as_secs_f64().max(0.000_001)
    );
}
