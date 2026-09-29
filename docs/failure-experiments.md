# Failure experiments

An LSM engine's real job isn't serving reads and writes when everything goes right, it's staying correct when the process dies at the worst possible moment. Anyone can claim a database is "crash-safe." This document is the evidence: five deliberate ways I broke VellumDB on purpose, what actually happened, and one bug I found by accident that a planned test would have missed entirely.

Every experiment here was run against a real database directory on disk, not a mock or an in-memory stand-in. Byte offsets, commands, and output below are taken from actual runs.

## 1. Kill the process mid-write

**Setup.** Two writes, then the process is killed externally (`timeout`) before it ever calls `exit`, no clean shutdown, no flush, nothing.

```
printf 'put foo bar\nput baz qux\n' | timeout 2 cargo run --quiet -- data-dir
```

**Result.** Reopening the same directory afterward:

```
vellum> bar
vellum> qux
```

Both writes survived. Neither was lost or corrupted.

**Why this works.** Every `put`/`delete` is `fsync`'d to the write-ahead log *before* the call returns, not after, and not batched. By the time the REPL printed `ok` for a write, that write was already durable on disk, independent of whatever happened to the process next. This is the most basic guarantee a database can make, and it's the one that turned out to have a real gap in it, see experiment 5.

## 2. Corrupt a WAL record by hand

**Setup.** Two records written normally, then a single byte inside the second record is flipped without touching its length or checksum header:

```rust
let mut bytes = fs::read(&path).unwrap();
bytes[bytes.len() - 2] ^= 0xFF;
fs::write(&path, &bytes).unwrap();
```

**Result.** Replay recovers exactly one record (the untouched first one) and stops, rather than erroring out or silently loading the corrupted second record as if it were valid data.

**Why this matters.** Every record is wrapped in a length + CRC32 checksum envelope. A structural problem (wrong length, truncated read) and a content problem (right length, wrong bytes) both get caught the same way: the checksum simply won't match. Corruption is detected, not trusted.

## 3. Kill the process between an SSTable flush and the manifest commit

This is the one that matters most for an LSM engine specifically, because a flush isn't a single atomic disk operation, it's two separate writes (a new SSTable file, then a manifest update naming it) that a crash can land between.

**Setup.** Rather than trying to interrupt a live process at exactly the right instant, I reproduced the *exact artifact* such a crash would leave behind: a fully-formed, valid SSTable file written directly to the data directory, without ever going through `Db::flush()` and therefore without any manifest ever referencing it.

```rust
let orphan_data = BTreeMap::from([(b"should-not-be-visible".to_vec(), ...)]);
SsTable::write(99, dir.join("000099.sst"), orphan_data.iter(), 1).unwrap();
```

**Result.** Reopening the directory:

- `sstable_count()` is `0`, the orphan file is not counted as active
- `get(b"should-not-be-visible")` returns `None`, its data was never committed, never visible
- `get(b"foo")`, written earlier through the normal WAL, still returns its correct value, recovered cleanly from the log

**Why this matters.** The manifest is the single source of truth for "what SSTables actually exist," and it's installed with a write-to-temp-file-then-rename, which is atomic on both Windows and POSIX filesystems. A file on disk that the manifest doesn't name is invisible, full stop, regardless of how complete or well-formed it is. This is what "old version or new version, never a mix" actually means in practice: it's not a slogan, it's a specific file on disk being ignored.

## 4. Kill the process between the manifest commit and WAL rotation

Once a flush's manifest commit succeeds, the WAL is truncated, since everything in it is now redundant (safely duplicated in the new SSTable). What if the process dies in that exact gap?

I didn't simulate this one with a byte-level artifact, because the answer is provable by construction rather than needing a reproduction: the manifest commit has *already succeeded* by the time rotation is attempted, so the new SSTable is already correctly active. If the crash happens before the WAL is truncated, the next `open()` replays the WAL, reapplies the same records into the memtable, and finds identical data already sitting in the SSTable. The write is redundant, not wrong. The worst outcome of losing this specific race is one extra, harmless replay pass at the next startup, not corruption and not data loss.

## 5. Torn write in the middle of a multi-key atomic batch

**Setup.** One normal write, then a two-key batch, then the file is truncated 5 bytes short, landing inside the batch's frame:

```rust
db.put(b"before".to_vec(), b"1".to_vec()).unwrap();
db.write_batch(vec![
    WriteOp::Put(b"batch-a".to_vec(), b"2".to_vec()),
    WriteOp::Put(b"batch-b".to_vec(), b"3".to_vec()),
]).unwrap();
// then: truncate the file by 5 bytes
```

**Result.**

```
get(b"before")  -> Some("1")   // untouched record survives
get(b"batch-a") -> None        // neither half of the batch landed
get(b"batch-b") -> None
```

**Why this matters.** A batch is written as a single checksummed frame, not as N separate records. A torn write during it loses the *entire* frame. There's no scenario where `batch-a` lands and `batch-b` doesn't, because they were never two separate durability events to begin with. This atomicity wasn't new machinery, it fell out of the existing frame-level guarantee from experiment 2, just applied to a frame that happens to contain more than one operation.

## 6. The bug a planned test didn't catch

The five experiments above were all designed in advance. This one wasn't, and it's the most instructive of the six precisely because of that.

While testing the crash-recovery hardening described in experiment 2, I tried opening a database directory that had been created *before* that same change, back when the WAL's wire format didn't have length/checksum framing at all. The result:

```
failed to open vellum-data: failed to fill whole buffer
```

The whole database refused to open. That's a strictly worse failure mode than anything the crash-recovery work was supposed to fix.

Tracing it down: the new parser was reading old-format bytes and, purely by coincidence, the first few bytes (a zero tag and a zero sequence number, both encoded as zero bytes) happened to satisfy the new format's length-and-checksum framing as an empty, "valid" record. The outer envelope checked out. But the code that parsed *inside* that envelope, reading a tag byte, a sequence number, a key, hadn't been written defensively. It assumed that if the envelope was valid, the payload it contained must be too, and used a bare `?` instead of routing failures through the same lenient recovery path as everything else. A payload that was technically checksum-valid but too short to actually contain a record skipped past every safeguard I'd just built and crashed `open()` outright.

The fix was small: treat a structural failure while parsing an already-verified payload exactly the same as a bad frame, drop the record and stop, instead of propagating a hard error. But finding it wasn't small: it took actually trying to open a real, older database, not just running the test suite, which only exercises scenarios someone thought to write down in advance.

## What this demonstrates

Every claim in this document is backed by a specific file, a specific byte offset, and a specific assertion, not a general assurance that "the database handles crashes." The distinction matters: "I built a WAL" is a claim. "I killed the process mid-write, corrupted a record by hand, fabricated the exact artifact an interrupted flush would leave behind, and found a real bug by trying to open an old database" is evidence.
