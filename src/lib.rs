mod bloom;
mod encoding;
mod entry;
mod manifest;
mod merge;
mod sstable;
mod wal;

use std::collections::BTreeMap;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use entry::{Entry, EntryValue};
use manifest::Manifest;
use merge::{MergeIter, SourceIter};
use sstable::SsTable;
use wal::Wal;

/// One write within a batch passed to `Db::write_batch`.
pub use wal::WalOp as WriteOp;

/// Memtable flushes to a new SSTable once its approximate size crosses this.
/// Small on purpose so it's easy to trigger and observe without huge inputs.
const FLUSH_THRESHOLD_BYTES: usize = 1024;

/// Key-value store. `new()` gives a pure in-memory instance with no
/// persistence, used for the skeleton and for quick tests. `open()` backs it
/// with a data directory: a WAL for durability plus zero or more SSTables
/// that earlier memtable flushes produced.
///
/// Reads check the memtable first, then SSTables newest to oldest. Deletes
/// write a tombstone rather than just removing the key, so a flushed key
/// stays deleted even once the memtable that originally held the delete has
/// been cleared: the tombstone travels forward through future flushes until
/// compaction (milestone 7) can eventually drop it for good.
pub struct Db {
    data: BTreeMap<Vec<u8>, Entry>,
    memtable_size_bytes: usize,
    wal: Option<Wal>,
    dir: Option<PathBuf>,
    /// Oldest first. Reads walk this in reverse so the newest flush wins.
    sstables: Vec<SsTable>,
    next_sstable_id: u64,
    /// Monotonic counter, one shared value stamped onto every operation in
    /// a single `put`/`delete`/`write_batch` call, that sharing is what a
    /// batch's atomicity is actually built on, see `write_batch`. Not
    /// load-bearing for the recency decisions `get`/`range` make (that's
    /// "memtable wins, then newest sstable wins"). Recovered on open from
    /// the manifest's last committed value plus whatever the WAL replay
    /// pushes it forward by.
    next_seq: u64,
}

impl Default for Db {
    fn default() -> Self {
        Self::new()
    }
}

impl Db {
    pub fn new() -> Self {
        Db {
            data: BTreeMap::new(),
            memtable_size_bytes: 0,
            wal: None,
            dir: None,
            sstables: Vec::new(),
            next_sstable_id: 0,
            next_seq: 0,
        }
    }

    /// Open (or create) a database directory at `dir`. Only SSTables listed
    /// in the manifest are trusted, an `.sst` file sitting in the directory
    /// but not named there is an orphan from an interrupted flush (the data
    /// file got written but the process died before the manifest committed
    /// it) and is silently ignored rather than treated as live data. The WAL
    /// is then replayed on top to restore anything written since the last
    /// flush.
    pub fn open(dir: impl AsRef<Path>) -> io::Result<Self> {
        let dir = dir.as_ref();
        fs::create_dir_all(dir)?;

        let manifest = Manifest::load(dir)?;
        let next_sstable_id = manifest.sstable_ids.iter().max().map_or(0, |id| id + 1);
        let sstables: Vec<SsTable> = manifest
            .sstable_ids
            .iter()
            .map(|&id| SsTable::open(id, sstable_path(dir, id)))
            .collect::<io::Result<Vec<_>>>()?;

        // The manifest's next_seq is a floor as of the last committed
        // flush, replaying the WAL (only the tail written since then, the
        // rest was rotated away in flush()) pushes it forward to cover
        // anything written after. This is what lets open() skip scanning
        // every sstable's contents just to recover the counter, and what
        // keeps replay bounded by "writes since the last flush" rather than
        // "writes since the database was created".
        let mut next_seq = manifest.next_seq;
        let wal_path = dir.join("wal.log");
        let mut data = BTreeMap::new();
        for record in Wal::replay(&wal_path)? {
            next_seq = next_seq.max(record.seq + 1);
            for op in record.ops {
                let (key, entry) = op_to_entry(record.seq, op);
                data.insert(key, entry);
            }
        }
        let memtable_size_bytes = data.iter().map(|(k, e)| entry_size(k, e)).sum();

        let wal = Wal::open(&wal_path)?;
        Ok(Db {
            data,
            memtable_size_bytes,
            wal: Some(wal),
            dir: Some(dir.to_path_buf()),
            sstables,
            next_sstable_id,
            next_seq,
        })
    }

    pub fn put(&mut self, key: Vec<u8>, value: Vec<u8>) -> io::Result<()> {
        self.write_batch(vec![WriteOp::Put(key, value)])
    }

    pub fn get(&self, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        lookup(&self.data, &self.sstables, key)
    }

    /// Writes a tombstone for the key instead of just removing it, so the
    /// delete survives being carried forward across a flush: even once an
    /// older SSTable still holds a stale value for this key, the tombstone
    /// in front of it makes `get` report not-found instead of falling
    /// through to the stale copy.
    ///
    /// The return value reflects whether the key existed anywhere (memtable
    /// or any sstable), which costs a full lookup on every delete. That cost
    /// is exactly what Bloom filters (milestone 5) exist to cut down.
    pub fn delete(&mut self, key: &[u8]) -> io::Result<bool> {
        let existed = self.get(key)?.is_some();
        self.write_batch(vec![WriteOp::Delete(key.to_vec())])?;
        Ok(existed)
    }

    /// Applies every operation in `ops` as one atomic unit: either all of
    /// them are durable together, or (after a crash before this call
    /// returns) none of them are, a reader never sees a partial batch. A
    /// lone `put`/`delete` is just a batch of one under the hood, both go
    /// through this same path.
    ///
    /// This reuses the WAL's existing frame-level atomicity (milestone 9)
    /// rather than needing new machinery: the whole batch is written as one
    /// checksummed frame sharing a single sequence number, so a torn write
    /// during it loses the entire frame, not just part of it, exactly the
    /// same guarantee a single record already had.
    pub fn write_batch(&mut self, ops: Vec<WriteOp>) -> io::Result<()> {
        if ops.is_empty() {
            return Ok(());
        }

        let seq = self.next_seq;
        self.next_seq += 1;

        if let Some(wal) = &mut self.wal {
            wal.append(seq, &ops)?;
        }

        for op in ops {
            let (key, entry) = op_to_entry(seq, op);
            let old_size = self.data.get(&key).map_or(0, |old| entry_size(&key, old));
            let new_size = entry_size(&key, &entry);
            self.memtable_size_bytes = self.memtable_size_bytes.saturating_sub(old_size) + new_size;
            self.data.insert(key, entry);
        }

        self.maybe_flush()?;
        Ok(())
    }

    /// Freeze a point-in-time, read-only view of the database: a copy of
    /// the current memtable plus the current list of sstables, both taken
    /// right now. Safe to keep using after any later `put`/`delete`/`flush`
    /// on this `Db`, flush only ever adds new sstable files, it never
    /// removes existing ones, so the files this snapshot references stay
    /// exactly as they were.
    ///
    /// **Not** safe across `compact()`, which does delete old sstable files
    /// once they're merged away. Reading through a snapshot taken before a
    /// compaction that has since run surfaces a file-not-found `io::Error`
    /// rather than silently serving stale-but-valid data, seeing why is
    /// worth walking through: full snapshot isolation across compaction
    /// would need the sstables a live snapshot still needs to be reference
    /// counted so compaction knows not to delete them yet, real MVCC
    /// storage engines do exactly this. That's a legitimate further
    /// extension, not implemented here, this milestone's snapshots cover
    /// the more common case (a consistent read while writes continue)
    /// without it.
    pub fn snapshot(&self) -> Snapshot {
        Snapshot { data: self.data.clone(), sstables: self.sstables.clone() }
    }

    /// Number of entries in the memtable, including tombstones not yet
    /// flushed. Not the same as "number of live keys".
    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Every live key in the database in ascending order: memtable and all
    /// SSTables merged together with recency resolved correctly and
    /// tombstoned keys dropped entirely. This is a full scan, not a bounded
    /// range query, there's no index yet to make seeking to a start key
    /// cheap, and nothing needs bounded scans yet either, that's a natural,
    /// cheap extension to add once something actually calls for it.
    pub fn range(&self) -> io::Result<MergeIter<'_>> {
        build_range(&self.sstables, &self.data)
    }

    pub fn sstable_count(&self) -> usize {
        self.sstables.len()
    }

    fn maybe_flush(&mut self) -> io::Result<()> {
        if self.dir.is_some() && self.memtable_size_bytes >= FLUSH_THRESHOLD_BYTES {
            self.flush()?;
        }
        Ok(())
    }

    /// Write the current memtable (including any tombstones) to a new
    /// SSTable file, then atomically install a manifest that references it,
    /// only then is it actually part of the database. If the process dies
    /// between those two steps, the new `.sst` file is left on disk but the
    /// manifest still points at the old state, so the next `open()` ignores
    /// it as an orphan and recovers the pre-flush state from the WAL
    /// instead, never a mix of the two.
    ///
    /// Once the manifest commit succeeds, every record currently in the WAL
    /// is redundant (it's now durably reflected in this or an earlier
    /// sstable), so the WAL is rotated (truncated to empty) before
    /// returning. This is what keeps `open()`'s replay bounded by "writes
    /// since the last flush" instead of letting the WAL grow forever across
    /// the database's whole lifetime. No-op if this database has no backing
    /// directory (`Db::new()`).
    pub fn flush(&mut self) -> io::Result<()> {
        let Some(dir) = self.dir.clone() else {
            return Ok(());
        };
        if self.data.is_empty() {
            return Ok(());
        }

        let id = self.next_sstable_id;
        let sstable = SsTable::write(id, sstable_path(&dir, id), self.data.iter(), self.data.len())?;

        let manifest = Manifest {
            sstable_ids: self.sstables.iter().map(SsTable::id).chain([id]).collect(),
            next_seq: self.next_seq,
        };
        manifest.save(&dir)?;

        if let Some(wal) = &mut self.wal {
            wal.truncate()?;
        }

        self.sstables.push(sstable);
        self.next_sstable_id += 1;
        self.data.clear();
        self.memtable_size_bytes = 0;
        Ok(())
    }

    /// Merge every current SSTable into a single new one. Duplicate keys
    /// keep only the copy from the newest sstable, and tombstones are
    /// dropped entirely, safe here specifically because this merges *all*
    /// sstables at once, there's no older, not-yet-compacted file left that
    /// a tombstone would still need to shadow. A no-op below two sstables,
    /// there's nothing to merge, only the memtable is untouched by this,
    /// flush and compaction stay separate operations.
    ///
    /// Loads every sstable's contents into memory to de-duplicate rather
    /// than streaming a merge. `merge::MergeIter` (milestone 8) turned out
    /// not to be a drop-in replacement for this: it's shaped for reads, so
    /// it drops tombstones and discards sequence numbers, while compaction
    /// needs to keep `Entry` (seq included) intact for every surviving key.
    /// Streaming this properly would need a merge that yields full `Entry`
    /// values and leaves the tombstone/dedup decision to the caller, a
    /// reasonable future generalization if benchmarking (milestone 13)
    /// ever shows this eager approach is a real bottleneck.
    ///
    /// Installs the merged result the same way `flush` installs a new
    /// sstable: write the file, then atomically swap in a manifest that
    /// references only it. A crash between those two steps leaves the old
    /// sstables (and the old manifest pointing at them) untouched, and the
    /// half-written merge file is ignored as an orphan on reopen, this
    /// reuses the exact atomic-install path already proven crash-safe by
    /// milestone 6's orphan test, so it isn't re-tested here.
    pub fn compact(&mut self) -> io::Result<()> {
        let Some(dir) = self.dir.clone() else {
            return Ok(());
        };
        if self.sstables.len() < 2 {
            return Ok(());
        }

        let mut merged: BTreeMap<Vec<u8>, Entry> = BTreeMap::new();
        for sstable in &self.sstables {
            for item in sstable.iter()? {
                let (key, entry) = item?;
                merged.insert(key, entry);
            }
        }
        merged.retain(|_, entry| !matches!(entry.value, EntryValue::Tombstone));

        let new_id = self.next_sstable_id;
        let new_sstable =
            SsTable::write(new_id, sstable_path(&dir, new_id), merged.iter(), merged.len())?;

        let manifest = Manifest { sstable_ids: vec![new_id], next_seq: self.next_seq };
        manifest.save(&dir)?;

        // Only reached once the new manifest is durably installed, so it's
        // now safe to drop our reference to the old sstables and delete
        // their files. A crash before this point just leaves them as
        // harmless files a future compaction would overwrite; a crash after
        // partway through just leaks a few files, no orphan-sweeping exists
        // yet to reclaim them, that's a nice-to-have, not a correctness gap.
        let old_sstables = std::mem::replace(&mut self.sstables, vec![new_sstable]);
        self.next_sstable_id += 1;
        for old in old_sstables {
            old.remove_files()?;
        }

        Ok(())
    }
}

fn sstable_path(dir: &Path, id: u64) -> PathBuf {
    dir.join(format!("{id:06}.sst"))
}

fn entry_size(key: &[u8], entry: &Entry) -> usize {
    key.len()
        + match &entry.value {
            EntryValue::Value(v) => v.len(),
            EntryValue::Tombstone => 0,
        }
}

fn op_to_entry(seq: u64, op: WriteOp) -> (Vec<u8>, Entry) {
    match op {
        WriteOp::Put(key, value) => (key, Entry { seq, value: EntryValue::Value(value) }),
        WriteOp::Delete(key) => (key, Entry { seq, value: EntryValue::Tombstone }),
    }
}

/// Shared by `Db::get` and `Snapshot::get`: check the memtable first, then
/// sstables newest to oldest, a tombstone found anywhere stops the search
/// and reports not-found rather than letting an older copy shadow it.
fn lookup(data: &BTreeMap<Vec<u8>, Entry>, sstables: &[SsTable], key: &[u8]) -> io::Result<Option<Vec<u8>>> {
    if let Some(entry) = data.get(key) {
        return Ok(match &entry.value {
            EntryValue::Value(v) => Some(v.clone()),
            EntryValue::Tombstone => None,
        });
    }
    for sstable in sstables.iter().rev() {
        if let Some(entry) = sstable.get(key)? {
            return Ok(match entry.value {
                EntryValue::Value(v) => Some(v),
                EntryValue::Tombstone => None,
            });
        }
    }
    Ok(None)
}

/// Shared by `Db::range` and `Snapshot::range`.
fn build_range<'a>(
    sstables: &'a [SsTable],
    data: &'a BTreeMap<Vec<u8>, Entry>,
) -> io::Result<MergeIter<'a>> {
    let mut sources: Vec<SourceIter<'a>> = Vec::with_capacity(sstables.len() + 1);
    for sstable in sstables {
        sources.push(Box::new(sstable.iter()?));
    }
    sources.push(Box::new(data.iter().map(|(k, e)| Ok((k.clone(), e.clone())))));
    Ok(MergeIter::new(sources))
}

/// A frozen, point-in-time read-only view of the database, see
/// `Db::snapshot` for exactly what it is and isn't safe against.
pub struct Snapshot {
    data: BTreeMap<Vec<u8>, Entry>,
    sstables: Vec<SsTable>,
}

impl Snapshot {
    pub fn get(&self, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        lookup(&self.data, &self.sstables, key)
    }

    pub fn range(&self) -> io::Result<MergeIter<'_>> {
        build_range(&self.sstables, &self.data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn put_then_get_returns_value() {
        let mut db = Db::new();
        db.put(b"foo".to_vec(), b"bar".to_vec()).unwrap();
        assert_eq!(db.get(b"foo").unwrap(), Some(b"bar".to_vec()));
    }

    #[test]
    fn get_missing_key_returns_none() {
        let db = Db::new();
        assert_eq!(db.get(b"missing").unwrap(), None);
    }

    #[test]
    fn put_overwrites_existing_value() {
        let mut db = Db::new();
        db.put(b"foo".to_vec(), b"bar".to_vec()).unwrap();
        db.put(b"foo".to_vec(), b"baz".to_vec()).unwrap();
        assert_eq!(db.get(b"foo").unwrap(), Some(b"baz".to_vec()));
    }

    #[test]
    fn delete_removes_key() {
        let mut db = Db::new();
        db.put(b"foo".to_vec(), b"bar".to_vec()).unwrap();
        assert!(db.delete(b"foo").unwrap());
        assert_eq!(db.get(b"foo").unwrap(), None);
    }

    #[test]
    fn delete_missing_key_returns_false() {
        let mut db = Db::new();
        assert!(!db.delete(b"missing").unwrap());
    }

    fn collect_range(db: &Db) -> Vec<(Vec<u8>, Vec<u8>)> {
        db.range().unwrap().collect::<io::Result<Vec<_>>>().unwrap()
    }

    #[test]
    fn range_returns_entries_in_key_order() {
        let mut db = Db::new();
        db.put(b"charlie".to_vec(), b"3".to_vec()).unwrap();
        db.put(b"alpha".to_vec(), b"1".to_vec()).unwrap();
        db.put(b"bravo".to_vec(), b"2".to_vec()).unwrap();

        let keys: Vec<Vec<u8>> = collect_range(&db).into_iter().map(|(k, _)| k).collect();
        assert_eq!(keys, vec![b"alpha".to_vec(), b"bravo".to_vec(), b"charlie".to_vec()]);
    }

    #[test]
    fn range_skips_tombstones() {
        let mut db = Db::new();
        db.put(b"alpha".to_vec(), b"1".to_vec()).unwrap();
        db.put(b"bravo".to_vec(), b"2".to_vec()).unwrap();
        db.delete(b"alpha").unwrap();

        let keys: Vec<Vec<u8>> = collect_range(&db).into_iter().map(|(k, _)| k).collect();
        assert_eq!(keys, vec![b"bravo".to_vec()]);
    }

    /// The actual point of milestone 8: a full scan has to merge the
    /// memtable and every SSTable together, not just show whichever one
    /// happens to hold a given key.
    #[test]
    fn range_merges_memtable_and_sstables() {
        let dir = temp_db_dir("range_merge");
        let mut db = Db::open(&dir).unwrap();

        db.put(b"alpha".to_vec(), b"1".to_vec()).unwrap();
        db.flush().unwrap();
        db.put(b"bravo".to_vec(), b"2".to_vec()).unwrap();
        db.flush().unwrap();
        db.put(b"charlie".to_vec(), b"3".to_vec()).unwrap(); // stays in the memtable

        assert_eq!(
            collect_range(&db),
            vec![
                (b"alpha".to_vec(), b"1".to_vec()),
                (b"bravo".to_vec(), b"2".to_vec()),
                (b"charlie".to_vec(), b"3".to_vec()),
            ]
        );

        fs::remove_dir_all(&dir).unwrap();
    }

    /// Recency has to be resolved across sources too: a newer value in the
    /// memtable must win over a stale copy still sitting in an sstable, and
    /// a tombstone anywhere in the chain must suppress an older sstable's
    /// value for that key rather than both showing up.
    #[test]
    fn range_resolves_recency_across_sources() {
        let dir = temp_db_dir("range_recency");
        let mut db = Db::open(&dir).unwrap();

        db.put(b"foo".to_vec(), b"v1".to_vec()).unwrap();
        db.flush().unwrap();
        db.put(b"foo".to_vec(), b"v2".to_vec()).unwrap(); // newer, still in memtable

        db.put(b"bar".to_vec(), b"hello".to_vec()).unwrap();
        db.flush().unwrap();
        db.delete(b"bar").unwrap(); // tombstone, still in memtable

        assert_eq!(collect_range(&db), vec![(b"foo".to_vec(), b"v2".to_vec())]);

        fs::remove_dir_all(&dir).unwrap();
    }

    fn temp_db_dir(name: &str) -> PathBuf {
        let path =
            std::env::temp_dir().join(format!("vellumdb_test_{}_{}", std::process::id(), name));
        let _ = fs::remove_dir_all(&path);
        path
    }

    #[test]
    fn open_rebuilds_state_from_existing_wal() {
        let dir = temp_db_dir("rebuild");

        {
            let mut db = Db::open(&dir).unwrap();
            db.put(b"foo".to_vec(), b"bar".to_vec()).unwrap();
            db.put(b"baz".to_vec(), b"qux".to_vec()).unwrap();
            db.delete(b"foo").unwrap();
        }

        let db = Db::open(&dir).unwrap();
        assert_eq!(db.get(b"foo").unwrap(), None);
        assert_eq!(db.get(b"baz").unwrap(), Some(b"qux".to_vec()));

        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn open_on_missing_dir_starts_empty() {
        let dir = temp_db_dir("fresh");

        let db = Db::open(&dir).unwrap();
        assert!(db.is_empty());

        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn flush_writes_sstable_and_clears_memtable() {
        let dir = temp_db_dir("flush");
        let mut db = Db::open(&dir).unwrap();

        db.put(b"foo".to_vec(), b"bar".to_vec()).unwrap();
        assert_eq!(db.sstable_count(), 0);
        assert_eq!(db.len(), 1);

        db.flush().unwrap();

        assert_eq!(db.sstable_count(), 1);
        assert_eq!(db.len(), 0, "memtable should be empty right after flush");
        // still readable, now served from the sstable instead of the memtable
        assert_eq!(db.get(b"foo").unwrap(), Some(b"bar".to_vec()));

        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn auto_flush_triggers_past_size_threshold() {
        let dir = temp_db_dir("autoflush");
        let mut db = Db::open(&dir).unwrap();

        let big_value = vec![b'x'; FLUSH_THRESHOLD_BYTES];
        db.put(b"big".to_vec(), big_value.clone()).unwrap();

        assert_eq!(db.sstable_count(), 1, "put past the threshold should trigger a flush");
        assert_eq!(db.len(), 0);
        assert_eq!(db.get(b"big").unwrap(), Some(big_value));

        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn reopen_picks_up_existing_sstables() {
        let dir = temp_db_dir("reopen_sstables");

        {
            let mut db = Db::open(&dir).unwrap();
            db.put(b"foo".to_vec(), b"bar".to_vec()).unwrap();
            db.flush().unwrap();
        }

        let db = Db::open(&dir).unwrap();
        assert_eq!(db.sstable_count(), 1);
        assert_eq!(db.get(b"foo").unwrap(), Some(b"bar".to_vec()));

        fs::remove_dir_all(&dir).unwrap();
    }

    /// This is the fix for the gap milestone 3 documented and deliberately
    /// left open: deleting a key already flushed to an SSTable now works,
    /// because delete writes a tombstone into the (empty) memtable, and that
    /// tombstone shadows the stale value sitting in the older sstable.
    #[test]
    fn delete_after_flush_now_takes_effect() {
        let dir = temp_db_dir("delete_after_flush");
        let mut db = Db::open(&dir).unwrap();

        db.put(b"foo".to_vec(), b"bar".to_vec()).unwrap();
        db.flush().unwrap();

        let deleted = db.delete(b"foo").unwrap();
        assert!(deleted, "key exists in the sstable, so delete should report it existed");
        assert_eq!(db.get(b"foo").unwrap(), None, "tombstone in the memtable shadows the sstable");

        fs::remove_dir_all(&dir).unwrap();
    }

    /// The tombstone itself must also survive being flushed: once it's
    /// written out to a newer sstable, it still has to shadow the stale
    /// value sitting in an older one.
    #[test]
    fn tombstone_survives_flush_and_still_shadows_older_sstable() {
        let dir = temp_db_dir("tombstone_flush");
        let mut db = Db::open(&dir).unwrap();

        db.put(b"foo".to_vec(), b"bar".to_vec()).unwrap();
        db.flush().unwrap();

        db.delete(b"foo").unwrap();
        db.flush().unwrap();

        assert_eq!(db.sstable_count(), 2);
        assert_eq!(db.get(b"foo").unwrap(), None);

        fs::remove_dir_all(&dir).unwrap();
    }

    /// Proves the Bloom filter actually skips the data file for a
    /// definitely-missing key, rather than just trusting it does: delete the
    /// sstable's data file but leave its bloom sidecar in place. A missing
    /// key still correctly resolves to `None` (the filter said "definitely
    /// not here", so the file was never opened), while a key that might be
    /// present now surfaces an IO error, since the filter said "maybe" and
    /// the linear scan tried to open a file that's gone.
    #[test]
    fn bloom_filter_avoids_touching_disk_for_missing_keys() {
        let dir = temp_db_dir("bloom_skip");
        let mut db = Db::open(&dir).unwrap();

        db.put(b"foo".to_vec(), b"bar".to_vec()).unwrap();
        db.flush().unwrap();
        fs::remove_file(dir.join("000000.sst")).unwrap();

        assert_eq!(db.get(b"definitely-not-a-key").unwrap(), None);
        assert!(db.get(b"foo").is_err(), "bloom said maybe, so it should have tried the gone file");

        fs::remove_dir_all(&dir).unwrap();
    }

    /// This is the manifest's actual reason for existing: simulate a crash
    /// that happens between an sstable's data file being written and the
    /// manifest being updated to reference it, by fabricating a well-formed
    /// orphan sstable directly, bypassing `Db::flush` (and therefore the
    /// manifest) entirely. Reopening must ignore it and recover the
    /// pre-flush state purely from the WAL, never a mix of the two.
    #[test]
    fn orphaned_sstable_from_a_crashed_flush_is_ignored_on_reopen() {
        let dir = temp_db_dir("orphan_sstable");

        {
            let mut db = Db::open(&dir).unwrap();
            db.put(b"foo".to_vec(), b"bar".to_vec()).unwrap();
            // durable in the wal, nothing flushed yet
        }

        let orphan_key = b"should-not-be-visible".to_vec();
        let orphan_entry = Entry { seq: 999, value: EntryValue::Value(b"orphan".to_vec()) };
        let orphan_data = BTreeMap::from([(orphan_key.clone(), orphan_entry)]);
        SsTable::write(99, dir.join("000099.sst"), orphan_data.iter(), 1).unwrap();

        let db = Db::open(&dir).unwrap();
        assert_eq!(db.sstable_count(), 0, "orphan isn't in the manifest, so it's not active");
        assert_eq!(db.get(b"foo").unwrap(), Some(b"bar".to_vec()), "recovered via wal replay");
        assert_eq!(db.get(&orphan_key).unwrap(), None, "orphan's data was never committed");

        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn write_batch_applies_every_op_atomically_in_memory() {
        let mut db = Db::new();
        db.put(b"existing".to_vec(), b"old".to_vec()).unwrap();

        db.write_batch(vec![
            WriteOp::Put(b"alpha".to_vec(), b"1".to_vec()),
            WriteOp::Put(b"bravo".to_vec(), b"2".to_vec()),
            WriteOp::Delete(b"existing".to_vec()),
        ])
        .unwrap();

        assert_eq!(db.get(b"alpha").unwrap(), Some(b"1".to_vec()));
        assert_eq!(db.get(b"bravo").unwrap(), Some(b"2".to_vec()));
        assert_eq!(db.get(b"existing").unwrap(), None);
    }

    #[test]
    fn write_batch_survives_reopen_as_a_whole() {
        let dir = temp_db_dir("batch_reopen");

        {
            let mut db = Db::open(&dir).unwrap();
            db.write_batch(vec![
                WriteOp::Put(b"alpha".to_vec(), b"1".to_vec()),
                WriteOp::Put(b"bravo".to_vec(), b"2".to_vec()),
            ])
            .unwrap();
        }

        let db = Db::open(&dir).unwrap();
        assert_eq!(db.get(b"alpha").unwrap(), Some(b"1".to_vec()));
        assert_eq!(db.get(b"bravo").unwrap(), Some(b"2".to_vec()));

        fs::remove_dir_all(&dir).unwrap();
    }

    /// The actual payoff of unifying single writes and batches onto one WAL
    /// frame per call: a torn write during a batch can't leave some of its
    /// keys durable and others not, the whole frame is either recovered or
    /// it isn't. Proven here at the `Db` level (`wal.rs` proves the same
    /// thing at the record-parsing level directly).
    #[test]
    fn write_batch_is_all_or_nothing_across_a_crash() {
        let dir = temp_db_dir("batch_torn");

        {
            let mut db = Db::open(&dir).unwrap();
            db.put(b"before".to_vec(), b"1".to_vec()).unwrap();
            db.write_batch(vec![
                WriteOp::Put(b"batch-a".to_vec(), b"2".to_vec()),
                WriteOp::Put(b"batch-b".to_vec(), b"3".to_vec()),
            ])
            .unwrap();
        }

        let wal_path = dir.join("wal.log");
        let full_len = fs::metadata(&wal_path).unwrap().len();
        let file = fs::OpenOptions::new().write(true).open(&wal_path).unwrap();
        file.set_len(full_len - 5).unwrap();
        drop(file);

        let db = Db::open(&dir).unwrap();
        assert_eq!(db.get(b"before").unwrap(), Some(b"1".to_vec()), "untouched record survives");
        assert_eq!(db.get(b"batch-a").unwrap(), None, "neither half of the torn batch should land");
        assert_eq!(db.get(b"batch-b").unwrap(), None, "neither half of the torn batch should land");

        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn snapshot_is_unaffected_by_later_writes() {
        let mut db = Db::new();
        db.put(b"foo".to_vec(), b"v1".to_vec()).unwrap();

        let snap = db.snapshot();

        db.put(b"foo".to_vec(), b"v2".to_vec()).unwrap();
        db.put(b"new-key".to_vec(), b"new".to_vec()).unwrap();
        db.delete(b"foo").unwrap();

        assert_eq!(snap.get(b"foo").unwrap(), Some(b"v1".to_vec()), "snapshot sees the old value");
        assert_eq!(snap.get(b"new-key").unwrap(), None, "snapshot predates this key entirely");
        assert_eq!(db.get(b"foo").unwrap(), None, "live db reflects the delete");
        assert_eq!(db.get(b"new-key").unwrap(), Some(b"new".to_vec()));
    }

    #[test]
    fn snapshot_is_unaffected_by_a_later_flush() {
        let dir = temp_db_dir("snapshot_flush");
        let mut db = Db::open(&dir).unwrap();
        db.put(b"foo".to_vec(), b"bar".to_vec()).unwrap();

        let snap = db.snapshot();
        db.flush().unwrap();

        assert_eq!(db.len(), 0, "live db's memtable was cleared by the flush");
        assert_eq!(snap.get(b"foo").unwrap(), Some(b"bar".to_vec()), "snapshot's own copy is untouched");

        fs::remove_dir_all(&dir).unwrap();
    }

    /// Documents the known limitation, not a desired behavior: a snapshot
    /// taken before a compaction that later deletes the sstables it
    /// references fails loudly (a real io::Error) rather than silently
    /// returning wrong data. See `Db::snapshot`'s docs for why this isn't
    /// solved here.
    #[test]
    fn known_limitation_snapshot_read_fails_after_compaction_removes_its_sstables() {
        let dir = temp_db_dir("snapshot_compact");
        let mut db = Db::open(&dir).unwrap();

        db.put(b"foo".to_vec(), b"1".to_vec()).unwrap();
        db.flush().unwrap();
        db.put(b"bar".to_vec(), b"2".to_vec()).unwrap();
        db.flush().unwrap();

        let snap = db.snapshot();
        db.compact().unwrap();

        assert!(
            snap.get(b"foo").is_err(),
            "the sstable this snapshot references no longer exists on disk"
        );

        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn compact_below_two_sstables_is_a_noop() {
        let dir = temp_db_dir("compact_noop");
        let mut db = Db::open(&dir).unwrap();

        db.put(b"foo".to_vec(), b"bar".to_vec()).unwrap();
        db.flush().unwrap();
        db.compact().unwrap();

        assert_eq!(db.sstable_count(), 1, "nothing to merge with only one sstable");

        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn compact_merges_multiple_sstables_and_keeps_all_live_keys() {
        let dir = temp_db_dir("compact_merge");
        let mut db = Db::open(&dir).unwrap();

        db.put(b"alpha".to_vec(), b"1".to_vec()).unwrap();
        db.flush().unwrap();
        db.put(b"bravo".to_vec(), b"2".to_vec()).unwrap();
        db.flush().unwrap();
        db.put(b"charlie".to_vec(), b"3".to_vec()).unwrap();
        db.flush().unwrap();
        assert_eq!(db.sstable_count(), 3);

        db.compact().unwrap();

        assert_eq!(db.sstable_count(), 1);
        assert_eq!(db.get(b"alpha").unwrap(), Some(b"1".to_vec()));
        assert_eq!(db.get(b"bravo").unwrap(), Some(b"2".to_vec()));
        assert_eq!(db.get(b"charlie").unwrap(), Some(b"3".to_vec()));

        fs::remove_dir_all(&dir).unwrap();
    }

    /// Not just "get returns the right value", proves the stale copy is
    /// actually gone from disk after compaction, not merely shadowed.
    #[test]
    fn compact_drops_stale_overwritten_values() {
        let dir = temp_db_dir("compact_overwrite");
        let mut db = Db::open(&dir).unwrap();

        db.put(b"foo".to_vec(), b"v1".to_vec()).unwrap();
        db.flush().unwrap();
        db.put(b"foo".to_vec(), b"v2".to_vec()).unwrap();
        db.flush().unwrap();

        db.compact().unwrap();

        assert_eq!(db.sstable_count(), 1);
        assert_eq!(db.get(b"foo").unwrap(), Some(b"v2".to_vec()));

        let entries: Vec<_> =
            db.sstables[0].iter().unwrap().collect::<io::Result<Vec<_>>>().unwrap();
        assert_eq!(entries.len(), 1, "only the newest copy of foo should remain on disk");

        fs::remove_dir_all(&dir).unwrap();
    }

    /// Proves tombstones are actually dropped, not just correctly shadowed:
    /// after compacting everything, there should be no trace of the key
    /// left at all, not even a tombstone record.
    #[test]
    fn compact_drops_tombstones_entirely() {
        let dir = temp_db_dir("compact_tombstone");
        let mut db = Db::open(&dir).unwrap();

        db.put(b"foo".to_vec(), b"bar".to_vec()).unwrap();
        db.flush().unwrap();
        db.delete(b"foo").unwrap();
        db.flush().unwrap();

        db.compact().unwrap();

        assert_eq!(db.sstable_count(), 1);
        assert_eq!(db.get(b"foo").unwrap(), None);

        let entries: Vec<_> =
            db.sstables[0].iter().unwrap().collect::<io::Result<Vec<_>>>().unwrap();
        assert!(entries.is_empty(), "tombstone should have been dropped, not just shadowed");

        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn flush_rotates_the_wal_to_empty() {
        let dir = temp_db_dir("wal_rotate");
        let mut db = Db::open(&dir).unwrap();

        db.put(b"foo".to_vec(), b"bar".to_vec()).unwrap();
        assert!(fs::metadata(dir.join("wal.log")).unwrap().len() > 0);

        db.flush().unwrap();
        assert_eq!(
            fs::metadata(dir.join("wal.log")).unwrap().len(),
            0,
            "wal should be empty once its records are durably reflected in the new sstable"
        );

        fs::remove_dir_all(&dir).unwrap();
    }

    /// The actual point of rotation: without it the wal accumulates every
    /// record from every flush across the database's whole lifetime. With
    /// it, only writes since the *last* flush are ever sitting in the wal.
    #[test]
    fn wal_does_not_accumulate_records_across_multiple_flushes() {
        let dir = temp_db_dir("wal_bounded");
        let mut db = Db::open(&dir).unwrap();

        for i in 0..5 {
            db.put(format!("key-{i}").into_bytes(), b"v".to_vec()).unwrap();
            db.flush().unwrap();
            assert_eq!(
                fs::metadata(dir.join("wal.log")).unwrap().len(),
                0,
                "wal should be rotated away after every single flush, not just the first"
            );
        }

        // all 5 keys should still be correct, purely from the sstables now
        for i in 0..5 {
            assert_eq!(db.get(format!("key-{i}").as_bytes()).unwrap(), Some(b"v".to_vec()));
        }

        fs::remove_dir_all(&dir).unwrap();
    }

    /// Reopening after a flush should recover cleanly with nothing left for
    /// the (now-rotated, empty) wal to contribute.
    #[test]
    fn reopen_after_flush_needs_no_wal_replay() {
        let dir = temp_db_dir("reopen_after_flush");

        {
            let mut db = Db::open(&dir).unwrap();
            db.put(b"foo".to_vec(), b"bar".to_vec()).unwrap();
            db.flush().unwrap();
        }

        assert_eq!(fs::metadata(dir.join("wal.log")).unwrap().len(), 0);
        let db = Db::open(&dir).unwrap();
        assert_eq!(db.get(b"foo").unwrap(), Some(b"bar".to_vec()));

        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn compact_removes_old_sstable_files_from_disk() {
        let dir = temp_db_dir("compact_cleanup");
        let mut db = Db::open(&dir).unwrap();

        db.put(b"foo".to_vec(), b"1".to_vec()).unwrap();
        db.flush().unwrap();
        db.put(b"bar".to_vec(), b"2".to_vec()).unwrap();
        db.flush().unwrap();

        db.compact().unwrap();

        assert!(!dir.join("000000.sst").exists(), "old sstable data file should be deleted");
        assert!(!dir.join("000000.bloom").exists(), "old bloom sidecar should be deleted");
        assert!(!dir.join("000001.sst").exists(), "old sstable data file should be deleted");
        assert!(!dir.join("000001.bloom").exists(), "old bloom sidecar should be deleted");
        assert!(dir.join("000002.sst").exists(), "merged sstable should exist");

        fs::remove_dir_all(&dir).unwrap();
    }
}
