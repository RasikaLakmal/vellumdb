mod bloom;
mod encoding;
mod entry;
mod sstable;
mod wal;

use std::collections::BTreeMap;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use entry::{Entry, EntryValue};
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
    /// once MVCC (milestone 11) needs point-in-time snapshots.
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

    /// Open (or create) a database directory at `dir`. Existing SSTables are
    /// picked up in order, then the WAL is replayed on top of them to
    /// restore anything written since the last flush. The next sequence
    /// number is recovered by scanning every sstable and the WAL for the
    /// highest one seen, this is an O(all data on disk) startup cost that
    /// milestone 6's manifest will remove by storing it durably instead.
    pub fn open(dir: impl AsRef<Path>) -> io::Result<Self> {
        let dir = dir.as_ref();
        fs::create_dir_all(dir)?;

        let mut sstable_files: Vec<(u64, PathBuf)> = fs::read_dir(dir)?
            .filter_map(|entry| entry.ok())
            .filter_map(|entry| {
                let path = entry.path();
                let id: u64 = path.file_stem()?.to_str()?.parse().ok()?;
                (path.extension()?.to_str()? == "sst").then_some((id, path))
            })
            .collect();
        sstable_files.sort_by_key(|(id, _)| *id);

        let next_sstable_id = sstable_files.last().map(|(id, _)| id + 1).unwrap_or(0);
        let sstables: Vec<SsTable> = sstable_files
            .into_iter()
            .map(|(_, path)| SsTable::open(path))
            .collect::<io::Result<Vec<_>>>()?;

        let mut max_seq: Option<u64> = None;
        for sstable in &sstables {
            for item in sstable.iter()? {
                let (_, entry) = item?;
                max_seq = Some(max_seq.map_or(entry.seq, |m| m.max(entry.seq)));
            }
        }

        let wal_path = dir.join("wal.log");
        let mut data = BTreeMap::new();
        for record in Wal::replay(&wal_path)? {
            max_seq = Some(max_seq.map_or(record.entry.seq, |m| m.max(record.entry.seq)));
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
            next_seq: max_seq.map_or(0, |m| m + 1),
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

    /// Iterate the memtable's live entries in key order, tombstones are
    /// filtered out since a scan should only show what's actually there.
    /// Only source is the memtable for now, this becomes a merge iterator
    /// across memtable + SSTables at milestone 8.
    pub fn iter(&self) -> impl Iterator<Item = (&Vec<u8>, &Vec<u8>)> {
        self.data.iter().filter_map(|(k, e)| match &e.value {
            EntryValue::Value(v) => Some((k, v)),
            EntryValue::Tombstone => None,
        })
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
    /// SSTable file and clear it. No-op if this database has no backing
    /// directory (`Db::new()`).
    pub fn flush(&mut self) -> io::Result<()> {
        let Some(dir) = &self.dir else {
            return Ok(());
        };
        if self.data.is_empty() {
            return Ok(());
        }

        let path = dir.join(format!("{:06}.sst", self.next_sstable_id));
        let sstable = SsTable::write(path, self.data.iter(), self.data.len())?;
        self.sstables.push(sstable);
        self.next_sstable_id += 1;

        self.data.clear();
        self.memtable_size_bytes = 0;
        Ok(())
    }
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

    #[test]
    fn iter_returns_entries_in_key_order() {
        let mut db = Db::new();
        db.put(b"charlie".to_vec(), b"3".to_vec()).unwrap();
        db.put(b"alpha".to_vec(), b"1".to_vec()).unwrap();
        db.put(b"bravo".to_vec(), b"2".to_vec()).unwrap();

        let keys: Vec<&[u8]> = db.iter().map(|(k, _)| k.as_slice()).collect();
        assert_eq!(keys, vec![b"alpha".as_slice(), b"bravo".as_slice(), b"charlie".as_slice()]);
    }

    #[test]
    fn iter_skips_tombstones() {
        let mut db = Db::new();
        db.put(b"alpha".to_vec(), b"1".to_vec()).unwrap();
        db.put(b"bravo".to_vec(), b"2".to_vec()).unwrap();
        db.delete(b"alpha").unwrap();

        let keys: Vec<&[u8]> = db.iter().map(|(k, _)| k.as_slice()).collect();
        assert_eq!(keys, vec![b"bravo".as_slice()]);
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
}
