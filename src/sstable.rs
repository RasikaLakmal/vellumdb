use std::fs::File;
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

use crate::bloom::BloomFilter;
use crate::encoding::{read_bytes, write_bytes};
use crate::entry::{Entry, EntryValue};

const TAG_PUT: u8 = 0;
const TAG_TOMBSTONE: u8 = 1;
const BLOOM_FALSE_POSITIVE_RATE: f64 = 0.01;

/// An immutable, sorted key-value file flushed from the memtable, plus a
/// Bloom filter sidecar (`<id>.bloom`) that lets `get` skip opening the data
/// file entirely for keys that definitely aren't in it. No other index yet,
/// a key that might be present still costs a linear scan.
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
            write_bytes(&mut writer, key)?;
            writer.write_all(&entry.seq.to_le_bytes())?;
            match &entry.value {
                EntryValue::Value(value) => {
                    writer.write_all(&[TAG_PUT])?;
                    write_bytes(&mut writer, value)?;
                }
                EntryValue::Tombstone => writer.write_all(&[TAG_TOMBSTONE])?,
            }
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

    fn next(&mut self) -> Option<Self::Item> {
        let key = match read_bytes(&mut self.reader) {
            Ok(key) => key,
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return None,
            Err(e) => return Some(Err(e)),
        };

        let mut seq_buf = [0u8; 8];
        if let Err(e) = self.reader.read_exact(&mut seq_buf) {
            return Some(Err(e));
        }
        let seq = u64::from_le_bytes(seq_buf);

        let mut tag = [0u8; 1];
        if let Err(e) = self.reader.read_exact(&mut tag) {
            return Some(Err(e));
        }

        let value = match tag[0] {
            TAG_PUT => match read_bytes(&mut self.reader) {
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
