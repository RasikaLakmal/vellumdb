use std::collections::HashMap;

/// In-memory key-value store. No persistence yet, this is the skeleton
/// milestone the rest of the engine (WAL, memtable, SSTables) will replace.
#[derive(Default)]
pub struct Db {
    data: HashMap<Vec<u8>, Vec<u8>>,
}

impl Db {
    pub fn new() -> Self {
        Db { data: HashMap::new() }
    }

    pub fn put(&mut self, key: Vec<u8>, value: Vec<u8>) {
        self.data.insert(key, value);
    }

    pub fn get(&self, key: &[u8]) -> Option<&Vec<u8>> {
        self.data.get(key)
    }

    pub fn delete(&mut self, key: &[u8]) -> bool {
        self.data.remove(key).is_some()
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn put_then_get_returns_value() {
        let mut db = Db::new();
        db.put(b"foo".to_vec(), b"bar".to_vec());
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
        db.put(b"foo".to_vec(), b"bar".to_vec());
        db.put(b"foo".to_vec(), b"baz".to_vec());
        assert_eq!(db.get(b"foo"), Some(&b"baz".to_vec()));
    }

    #[test]
    fn delete_removes_key() {
        let mut db = Db::new();
        db.put(b"foo".to_vec(), b"bar".to_vec());
        assert!(db.delete(b"foo"));
        assert_eq!(db.get(b"foo"), None);
    }

    #[test]
    fn delete_missing_key_returns_false() {
        let mut db = Db::new();
        assert!(!db.delete(b"missing"));
    }
}
