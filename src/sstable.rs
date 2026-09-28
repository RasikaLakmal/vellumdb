use std::fs::{self, File};
use std::io::{self, BufReader, BufWriter, Cursor, Read, Write};
use std::path::{Path, PathBuf};

use crate::bloom::BloomFilter;
use crate::encoding::{read_bytes, read_frame, write_bytes, write_frame, Frame};
use crate::entry::{Entry, EntryValue};

const TAG_PUT: u8 = 0;
const TAG_TOMBSTONE: u8 = 1;
const BLOOM_FALSE_POSITIVE_RATE: f64 = 0.01;

/// An immutable, sorted key-value file flushed from the memtable, plus a
/// Bloom filter sidecar (`<id>.bloom`) that lets `get` skip opening the data
/// file entirely for keys that definitely aren't in it. No other index yet,
/// a key that might be present still costs a linear scan.
#[derive(Clone)]
pub struct SsTable {
    id: u64,
    path: PathBuf,
    bloom: BloomFilter,
}

impl SsTable {
    /// Write `entries` (must already be sorted by key, as memtable iteration
    /// guarantees) to a new SSTable file at `path`, plus its Bloom filter
    /// sidecar. `expected_items` sizes the filter, it should be the number
    /// of entries about to be written. Note this alone doesn't make the
    /// sstable part of the database, that only happens once the manifest is
    /// updated to reference `id`, see `manifest.rs`.
    pub fn write<'a>(
        id: u64,
        path: impl AsRef<Path>,
        entries: impl Iterator<Item = (&'a Vec<u8>, &'a Entry)>,
        expected_items: usize,
    ) -> io::Result<Self> {
        let path = path.as_ref().to_path_buf();
        let mut bloom = BloomFilter::new(expected_items, BLOOM_FALSE_POSITIVE_RATE);
        let mut writer = BufWriter::new(File::create(&path)?);

        for (key, entry) in entries {
            bloom.insert(key);

            let mut payload = Vec::new();
            write_bytes(&mut payload, key)?;
            payload.extend_from_slice(&entry.seq.to_le_bytes());
            match &entry.value {
                EntryValue::Value(value) => {
                    payload.push(TAG_PUT);
                    write_bytes(&mut payload, value)?;
                }
                EntryValue::Tombstone => payload.push(TAG_TOMBSTONE),
            }
            write_frame(&mut writer, &payload)?;
        }
        writer.flush()?;
        bloom.write(bloom_path(&path))?;

        Ok(SsTable { id, path, bloom })
    }

    pub fn open(id: u64, path: impl AsRef<Path>) -> io::Result<Self> {
        let path = path.as_ref().to_path_buf();
        let bloom = BloomFilter::load(bloom_path(&path))?;
        Ok(SsTable { id, path, bloom })
    }

    pub fn id(&self) -> u64 {
        self.id
    }

    /// Deletes this sstable's data file and bloom sidecar from disk. Only
    /// safe to call once nothing (in particular, the manifest) references
    /// it anymore, otherwise a reopen would fail trying to load it.
    pub fn remove_files(&self) -> io::Result<()> {
        fs::remove_file(&self.path)?;
        fs::remove_file(bloom_path(&self.path))
    }

    /// Checks the Bloom filter first, a `false` there means the key is
    /// definitely absent and the data file is never opened. Otherwise falls
    /// back to a linear scan (an index makes this faster later, no
    /// milestone assigned yet).
    pub fn get(&self, key: &[u8]) -> io::Result<Option<Entry>> {
        if !self.bloom.might_contain(key) {
            return Ok(None);
        }
        for item in self.iter()? {
            let (k, entry) = item?;
            if k == key {
                return Ok(Some(entry));
            }
        }
        Ok(None)
    }

    /// Every entry in key order, the file is already sorted since it was
    /// written straight from the memtable.
    pub fn iter(&self) -> io::Result<SsTableIter> {
        Ok(SsTableIter { reader: BufReader::new(File::open(&self.path)?) })
    }
}

fn bloom_path(sstable_path: &Path) -> PathBuf {
    sstable_path.with_extension("bloom")
}

pub struct SsTableIter {
    reader: BufReader<File>,
}

impl Iterator for SsTableIter {
    type Item = io::Result<(Vec<u8>, Entry)>;

    /// Unlike WAL replay, a bad frame here is a hard error, not something
    /// to quietly stop at. An SSTable is written in one shot and only
    /// becomes visible once the manifest commits to it (see `manifest.rs`),
    /// so under normal operation there's no such thing as a partially
    /// written, already-committed sstable, a bad frame here means real,
    /// unexpected corruption, and silently truncating the read could hide
    /// perfectly good entries sitting further into the file.
    fn next(&mut self) -> Option<Self::Item> {
        let payload = match read_frame(&mut self.reader) {
            Ok(Frame::Ok(payload)) => payload,
            Ok(Frame::End) => return None,
            Ok(Frame::Bad) => {
                return Some(Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "corrupt or truncated sstable record",
                )));
            }
            Err(e) => return Some(Err(e)),
        };

        let mut cursor = Cursor::new(payload);
        let key = match read_bytes(&mut cursor) {
            Ok(k) => k,
            Err(e) => return Some(Err(e)),
        };

        let mut seq_buf = [0u8; 8];
        if let Err(e) = cursor.read_exact(&mut seq_buf) {
            return Some(Err(e));
        }
        let seq = u64::from_le_bytes(seq_buf);

        let mut tag = [0u8; 1];
        if let Err(e) = cursor.read_exact(&mut tag) {
            return Some(Err(e));
        }

        let value = match tag[0] {
            TAG_PUT => match read_bytes(&mut cursor) {
                Ok(v) => EntryValue::Value(v),
                Err(e) => return Some(Err(e)),
            },
            TAG_TOMBSTONE => EntryValue::Tombstone,
            other => {
                return Some(Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("unknown sstable entry tag: {other}"),
                )));
            }
        };

        Some(Ok((key, Entry { seq, value })))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn temp_path(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!("vellumdb_sstable_test_{}_{}.sst", std::process::id(), name))
    }

    #[test]
    fn write_then_iter_round_trips() {
        let path = temp_path("roundtrip");
        let data = BTreeMap::from([
            (b"alpha".to_vec(), Entry { seq: 0, value: EntryValue::Value(b"1".to_vec()) }),
            (b"bravo".to_vec(), Entry { seq: 1, value: EntryValue::Tombstone }),
        ]);

        let sstable = SsTable::write(0, &path, data.iter(), data.len()).unwrap();
        let entries: Vec<_> = sstable.iter().unwrap().collect::<io::Result<Vec<_>>>().unwrap();

        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].0, b"alpha");
        assert!(matches!(entries[1].1.value, EntryValue::Tombstone));

        sstable.remove_files().unwrap();
    }

    #[test]
    fn corrupted_record_is_a_hard_error_not_a_silent_stop() {
        let path = temp_path("corrupt");
        let data = BTreeMap::from([
            (b"alpha".to_vec(), Entry { seq: 0, value: EntryValue::Value(b"1".to_vec()) }),
            (b"bravo".to_vec(), Entry { seq: 1, value: EntryValue::Value(b"2".to_vec()) }),
        ]);
        let sstable = SsTable::write(0, &path, data.iter(), data.len()).unwrap();

        let mut bytes = fs::read(&path).unwrap();
        let last = bytes.len() - 1;
        bytes[last] ^= 0xFF;
        fs::write(&path, &bytes).unwrap();

        let result: io::Result<Vec<_>> = sstable.iter().unwrap().collect();
        assert!(result.is_err(), "corruption in a committed sstable must surface, not be swallowed");

        sstable.remove_files().unwrap();
    }
}
