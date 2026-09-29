# VellumDB

Persistent LSM-tree key-value store in Rust — WAL, memtables, SSTables, Bloom filters, compaction, crash recovery.

An educational storage engine, built from scratch (no `rocksdb`/`sled`) to actually understand what PostgreSQL/RocksDB/LevelDB are doing underneath, rather than just calling one. Part of a personal [engineering lab](../../checklist.md) of systems-depth projects.

## What's here

- **Write-ahead log** — every write is `fsync`'d before it's applied in memory, survives a hard process kill with no data loss
- **Memtable** — an in-memory sorted (`BTreeMap`) buffer for recent writes
- **SSTables** — immutable, sorted files the memtable flushes to once it crosses a size threshold, each with a **Bloom filter** sidecar so a lookup for a missing key skips opening the file entirely
- **Manifest** — a durably, atomically installed record of which SSTables are actually part of the database, so a half-written flush or compaction is never mistaken for live data
- **Compaction** — merges SSTables together, drops overwritten values and tombstones once safe
- **Tombstones** — deletes are markers, not removals, so they survive being carried forward across a flush
- **Crash recovery hardening** — every WAL/SSTable record is checksummed; a torn write at the tail of the log is recovered from instead of bricking the database
- **WAL rotation** — the log is truncated after every successful flush, so recovery time is bounded by "writes since the last flush," not "writes since the database was created"
- **Atomic batches** — multiple keys can be written as one all-or-nothing unit
- **Snapshots** — a frozen, point-in-time read-only view of the database (see [Known limitations](#known-limitations))

## Architecture

### Write path

```
put(key, value)
     │
     ▼
┌─────────┐   fsync'd append,
│   WAL   │   durable before
└─────────┘   this call returns
     │
     ▼
┌─────────┐   in-memory, sorted,
│ Memtable│   this is what get()
└─────────┘   checks first
     │
     │  once memtable crosses
     │  FLUSH_THRESHOLD_BYTES
     ▼
┌──────────┐  ┌──────────┐
│ SSTable  │  │ Manifest │  new sstable file written,
│  file    │─▶│  commit  │  THEN manifest atomically
└──────────┘  └──────────┘  updated to reference it
                    │
                    ▼
              WAL rotated
              (truncated to empty)
```

### Read path

```
get(key)
   │
   ├─▶ in memtable?  ── yes ──▶ return (or "not found" if it's a tombstone)
   │
   ▼ no
   │
   for each sstable, newest → oldest:
   │
   ├─▶ Bloom filter says "definitely not here"? ── skip, never open the file
   │
   └─▶ maybe present → linear scan the file
           │
           ├─▶ found a value      → return it
           ├─▶ found a tombstone  → return "not found", stop (don't check older sstables)
           └─▶ not in this file   → check the next (older) sstable
```

### Compaction

```
sstable-0  sstable-1  sstable-2  ...  sstable-N
    │          │          │              │
    └──────────┴──────────┴──────────────┘
                    │
          merge all entries, keep only
          the newest copy of each key,
          drop tombstones entirely
                    │
                    ▼
            one new sstable
                    │
                    ▼
     manifest atomically swapped to
     reference only the new file
                    │
                    ▼
        old sstable files deleted
```

## On-disk layout

```
<data-dir>/
├── wal.log        write-ahead log, truncated after every flush
├── MANIFEST        durable record of which sstables are active
├── 000000.sst       an immutable sstable (key-sorted, checksummed records)
├── 000000.bloom     that sstable's Bloom filter
├── 000001.sst
├── 000001.bloom
└── ...
```

## Usage

```sh
cargo run -- <data-dir>       # opens (or creates) a database, starts a repl
```

```
vellum> put foo bar
ok
vellum> get foo
bar
vellum> delete foo
ok
vellum> scan               # full merged view: memtable + all sstables
vellum> flush               # force the memtable to disk (normally automatic)
vellum> compact             # merge all sstables, drop stale/deleted data
vellum> exit
```

As a library, the same operations are available directly:

```rust
let mut db = vellumdb::Db::open("data-dir")?;
db.put(b"foo".to_vec(), b"bar".to_vec())?;
db.write_batch(vec![
    vellumdb::WriteOp::Put(b"a".to_vec(), b"1".to_vec()),
    vellumdb::WriteOp::Delete(b"b".to_vec()),
])?;
let snap = db.snapshot();
for entry in db.range()? { /* ... */ }
```

## Benchmarks

```sh
cargo run --release --example bench
```

Covers sequential/random writes, existing/missing-key reads, range scans, write amplification, recovery time, and a head-to-head against a naive baseline (a plain `HashMap` that rewrites its entire dataset to disk on every write), with p50/p95/p99 latency throughout. Representative numbers from one run (yours will vary by machine and disk):

| Measurement | Result |
|---|---|
| Missing-key read, Bloom filter on vs off (100 sstables) | 165µs vs 14.5ms median — **38x** |
| Write amplification before/after compaction | 3.8x on-disk bloat, compaction reclaims **67%** |
| Recovery time, WAL rotated vs unrotated | 13ms vs 66ms — **5.1x** |
| Write latency (fsync per write, no group commit) | ~2ms median |
| Writes vs naive baseline (n=200, small on purpose, see caveat) | VellumDB **3-5x faster**, and the gap only grows with data size (naive's cost is O(n) per write) |
| Reads vs naive baseline | naive up to **400x faster** (everything's already in RAM, zero I/O cost) |

The naive comparison is deliberately two-sided: VellumDB already wins on writes even at this small scale, but the naive store wins dramatically on reads, because it holds everything in memory with no per-op disk cost at all. That's only possible because its write path is O(n) per write and its memory use is unbounded, VellumDB bounds both and pays a real, small read-side cost for it. A store that takes longer to write the millionth key than the first isn't viable at any real size, which is the actual argument for all the complexity above.

## Known limitations

Documented deliberately, not hidden:

- **Snapshots aren't safe across `compact()`** — a snapshot taken before a compaction that later deletes its SSTables fails with a real `io::Error` on read, rather than silently serving wrong data. Full safety would need reference-counted SSTables so compaction knows not to delete ones a live snapshot still needs.
- **Compaction is size-tiered only, no leveled compaction** — every compaction rewrites the entire dataset, this is the direct cause of the 3.8x write amplification measured above. Leveled compaction (bounded, overlap-aware merging) is a natural next step, deliberately deferred until there were real numbers to compare it against.
- **No group commit** — every write does its own `fsync`, batching multiple writes into one `fsync` would improve throughput at the cost of latency predictability.
- **Full scans only, no bounded range queries** — `range()` always walks the entire keyspace; there's no index yet to make seeking to a start key cheap.
- **Compaction loads everything into memory** rather than streaming a merge, fine at this project's scale, would need revisiting for large datasets.

## Testing

```sh
cargo test      # unit + integration tests across every module
cargo clippy    # lint
```
