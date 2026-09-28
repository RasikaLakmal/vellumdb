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
    /// Monotonic counter stamped onto every entry. Not load-bearing for
    /// correctness yet, current recency is already handled by "memtable
    /// wins, then newest sstable wins". It becomes load-bearing once
    /// compaction needs to merge sstables with overlapping key ranges and
    /// once MVCC (milestone 11) needs point-in-time snapshots. Recovered on
    /// open from the manifest's last committed value plus whatever the WAL
    /// replay pushes it forward by.
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
        // flush, replaying the WAL (only the tail written since then, since
        // rotation doesn't exist until milestone 10) pushes it forward to
        // cover anything written after. This is what lets open() skip
        // scanning every sstable's contents just to recover the counter.
        let mut next_seq = manifest.next_seq;
        let wal_path = dir.join("wal.log");
        let mut data = BTreeMap::new();
        for record in Wal::replay(&wal_path)? {
            next_seq = next_seq.max(record.entry.seq + 1);
            data.insert(record.key, record.entry);
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
        let seq = self.next_seq;
        self.next_seq += 1;
        if let Some(wal) = &mut self.wal {
            wal.append_put(seq, &key, &value)?;
        }

        let entry = Entry { seq, value: EntryValue::Value(value) };
        let old_size = self.data.get(&key).map_or(0, |old| entry_size(&key, old));
        let new_size = entry_size(&key, &entry);
        self.memtable_size_bytes = self.memtable_size_bytes.saturating_sub(old_size) + new_size;
        self.data.insert(key, entry);

        self.maybe_flush()?;
        Ok(())
    }

    pub fn get(&self, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        if let Some(entry) = self.data.get(key) {
            return Ok(match &entry.value {
                EntryValue::Value(v) => Some(v.clone()),
                EntryValue::Tombstone => None,
            });
        }
        for sstable in self.sstables.iter().rev() {
            if let Some(entry) = sstable.get(key)? {
                return Ok(match entry.value {
                    EntryValue::Value(v) => Some(v),
                    EntryValue::Tombstone => None,
                });
            }
        }
        Ok(None)
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

        let seq = self.next_seq;
        self.next_seq += 1;
        if let Some(wal) = &mut self.wal {
            wal.append_tombstone(seq, key)?;
        }

        let entry = Entry { seq, value: EntryValue::Tombstone };
        let old_size = self.data.get(key).map_or(0, |old| entry_size(key, old));
        let new_size = entry_size(key, &entry);
        self.memtable_size_bytes = self.memtable_size_bytes.saturating_sub(old_size) + new_size;
        self.data.insert(key.to_vec(), entry);

        self.maybe_flush()?;
        Ok(existed)
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
        let mut sources: Vec<SourceIter<'_>> = Vec::with_capacity(self.sstables.len() + 1);
        for sstable in &self.sstables {
            sources.push(Box::new(sstable.iter()?));
        }
        sources.push(Box::new(self.data.iter().map(|(k, e)| Ok((k.clone(), e.clone())))));
        Ok(MergeIter::new(sources))
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
    /// instead, never a mix of the two. No-op if this database has no
    /// backing directory (`Db::new()`).
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
