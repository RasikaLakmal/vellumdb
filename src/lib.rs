mod wal;

use std::collections::BTreeMap;
use std::io;
use std::path::Path;

use wal::{Wal, WalRecord};

/// Key-value store. `new()` gives a pure in-memory instance with no
/// persistence. `open()` backs it with a write-ahead log: every write is
/// appended to disk before it's applied in memory, and the log is replayed
/// to rebuild state on startup.
///
/// The in-memory data lives in a `BTreeMap` (the memtable) rather than a
/// `HashMap` so keys stay sorted, this is what will let flushes to SSTables
/// and range scans be ordered without a separate sort step later.
pub struct Db {
    data: BTreeMap<Vec<u8>, Vec<u8>>,
    wal: Option<Wal>,
}

impl Default for Db {
    fn default() -> Self {
        Self::new()
    }
}

impl Db {
    pub fn new() -> Self {
        Db { data: BTreeMap::new(), wal: None }
    }

    pub fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        let path = path.as_ref();
        let mut data = BTreeMap::new();
        for record in Wal::replay(path)? {
            match record {
                WalRecord::Put { key, value } => {
                    data.insert(key, value);
                }
                WalRecord::Delete { key } => {
                    data.remove(&key);
                }
            }
        }

        let wal = Wal::open(path)?;
        Ok(Db { data, wal: Some(wal) })
    }

    pub fn put(&mut self, key: Vec<u8>, value: Vec<u8>) -> io::Result<()> {
        if let Some(wal) = &mut self.wal {
            wal.append_put(&key, &value)?;
        }
        self.data.insert(key, value);
        Ok(())
    }

    pub fn get(&self, key: &[u8]) -> Option<&Vec<u8>> {
        self.data.get(key)
    }

    pub fn delete(&mut self, key: &[u8]) -> io::Result<bool> {
        if let Some(wal) = &mut self.wal {
            wal.append_delete(key)?;
        }
        Ok(self.data.remove(key).is_some())
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Iterate all entries in key order. Only source is the memtable for now,
    /// this becomes a merge iterator across memtable + SSTables at milestone 8.
    pub fn iter(&self) -> impl Iterator<Item = (&Vec<u8>, &Vec<u8>)> {
        self.data.iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn put_then_get_returns_value() {
        let mut db = Db::new();
        db.put(b"foo".to_vec(), b"bar".to_vec()).unwrap();
        assert_eq!(db.get(b"foo"), Some(&b"bar".to_vec()));
    }

    #[test]
    fn get_missing_key_returns_none() {
        let db = Db::new();
        assert_eq!(db.get(b"missing"), None);
    }

    #[test]
    fn put_overwrites_existing_value() {
        let mut db = Db::new();
        db.put(b"foo".to_vec(), b"bar".to_vec()).unwrap();
        db.put(b"foo".to_vec(), b"baz".to_vec()).unwrap();
        assert_eq!(db.get(b"foo"), Some(&b"baz".to_vec()));
    }

    #[test]
    fn delete_removes_key() {
        let mut db = Db::new();
        db.put(b"foo".to_vec(), b"bar".to_vec()).unwrap();
        assert!(db.delete(b"foo").unwrap());
        assert_eq!(db.get(b"foo"), None);
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

    fn temp_wal_path(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!("vellumdb_test_{}_{}.wal", std::process::id(), name))
    }

    #[test]
    fn open_rebuilds_state_from_existing_wal() {
        let path = temp_wal_path("rebuild");
        let _ = std::fs::remove_file(&path);

        {
            let mut db = Db::open(&path).unwrap();
            db.put(b"foo".to_vec(), b"bar".to_vec()).unwrap();
            db.put(b"baz".to_vec(), b"qux".to_vec()).unwrap();
            db.delete(b"foo").unwrap();
        }

        let db = Db::open(&path).unwrap();
        assert_eq!(db.get(b"foo"), None);
        assert_eq!(db.get(b"baz"), Some(&b"qux".to_vec()));

        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn open_on_missing_file_starts_empty() {
        let path = temp_wal_path("fresh");
        let _ = std::fs::remove_file(&path);

        let db = Db::open(&path).unwrap();
        assert!(db.is_empty());

        std::fs::remove_file(&path).unwrap();
    }
}
