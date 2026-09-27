mod encoding;
mod sstable;
mod wal;

use std::collections::BTreeMap;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use sstable::SsTable;
use wal::{Wal, WalRecord};

/// Memtable flushes to a new SSTable once its approximate size crosses this.
/// Small on purpose so it's easy to trigger and observe without huge inputs.
const FLUSH_THRESHOLD_BYTES: usize = 1024;

/// Key-value store. `new()` gives a pure in-memory instance with no
/// persistence, used for the skeleton and for quick tests. `open()` backs it
/// with a data directory: a WAL for durability plus zero or more SSTables
/// that earlier memtable flushes produced.
///
/// Reads check the memtable first, then SSTables newest to oldest, so a
/// flushed key is still found transparently through `get`.
pub struct Db {
    data: BTreeMap<Vec<u8>, Vec<u8>>,
    memtable_size_bytes: usize,
    wal: Option<Wal>,
    dir: Option<PathBuf>,
    /// Oldest first. Reads walk this in reverse so the newest flush wins.
    sstables: Vec<SsTable>,
    next_sstable_id: u64,
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
        }
    }

    /// Open (or create) a database directory at `dir`. Existing SSTables are
    /// picked up in order, then the WAL is replayed on top of them to
    /// restore anything written since the last flush.
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
        let sstables: Vec<SsTable> =
            sstable_files.into_iter().map(|(_, path)| SsTable::open(path)).collect();

        let wal_path = dir.join("wal.log");
        let mut data = BTreeMap::new();
        for record in Wal::replay(&wal_path)? {
            match record {
                WalRecord::Put { key, value } => {
                    data.insert(key, value);
                }
                WalRecord::Delete { key } => {
                    data.remove(&key);
                }
            }
        }
        let memtable_size_bytes = data.iter().map(|(k, v)| k.len() + v.len()).sum();

        let wal = Wal::open(&wal_path)?;
        Ok(Db {
            data,
            memtable_size_bytes,
            wal: Some(wal),
            dir: Some(dir.to_path_buf()),
            sstables,
            next_sstable_id,
        })
    }

    pub fn put(&mut self, key: Vec<u8>, value: Vec<u8>) -> io::Result<()> {
        if let Some(wal) = &mut self.wal {
            wal.append_put(&key, &value)?;
        }
        let old_size = self.data.get(&key).map_or(0, |old| key.len() + old.len());
        let new_size = key.len() + value.len();
        self.memtable_size_bytes = self.memtable_size_bytes - old_size + new_size;
        self.data.insert(key, value);
        self.maybe_flush()?;
        Ok(())
    }

    pub fn get(&self, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        if let Some(value) = self.data.get(key) {
            return Ok(Some(value.clone()));
        }
        for sstable in self.sstables.iter().rev() {
            if let Some(value) = sstable.get(key)? {
                return Ok(Some(value));
            }
        }
        Ok(None)
    }

    /// Deletes the key from the memtable. Known limitation: if the key was
    /// already flushed to an SSTable, this currently does nothing to that
    /// on-disk copy, so `get` will still find the old value there. Fixed by
    /// tombstones in milestone 4, see notes/vellumdb/04-tombstones.md.
    pub fn delete(&mut self, key: &[u8]) -> io::Result<bool> {
        if let Some(wal) = &mut self.wal {
            wal.append_delete(key)?;
        }
        match self.data.remove(key) {
            Some(old) => {
                self.memtable_size_bytes = self.memtable_size_bytes.saturating_sub(key.len() + old.len());
                Ok(true)
            }
            None => Ok(false),
        }
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Iterate the memtable's entries in key order. Only source is the
    /// memtable for now, this becomes a merge iterator across memtable +
    /// SSTables at milestone 8.
    pub fn iter(&self) -> impl Iterator<Item = (&Vec<u8>, &Vec<u8>)> {
        self.data.iter()
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

    /// Write the current memtable to a new SSTable file and clear it.
    /// No-op if this database has no backing directory (`Db::new()`).
    pub fn flush(&mut self) -> io::Result<()> {
        let Some(dir) = &self.dir else {
            return Ok(());
        };
        if self.data.is_empty() {
            return Ok(());
        }

        let path = dir.join(format!("{:06}.sst", self.next_sstable_id));
        let sstable = SsTable::write(path, self.data.iter())?;
        self.sstables.push(sstable);
        self.next_sstable_id += 1;

        self.data.clear();
        self.memtable_size_bytes = 0;
        Ok(())
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

    /// Documents a known gap, not a desired behavior: deleting a key that's
    /// already been flushed to an SSTable currently has no effect, because
    /// delete only touches the memtable. `get` still finds the stale value
    /// on disk. This gets fixed by tombstones in milestone 4.
    #[test]
    fn known_limitation_delete_after_flush_does_not_take_effect_yet() {
        let dir = temp_db_dir("delete_after_flush");
        let mut db = Db::open(&dir).unwrap();

        db.put(b"foo".to_vec(), b"bar".to_vec()).unwrap();
        db.flush().unwrap();

        let deleted = db.delete(b"foo").unwrap();
        assert!(!deleted, "delete reports not-found since the key isn't in the memtable");
        assert_eq!(
            db.get(b"foo").unwrap(),
            Some(b"bar".to_vec()),
            "stale value is still served from the sstable, this is the gap tombstones close"
        );

        fs::remove_dir_all(&dir).unwrap();
    }
}
