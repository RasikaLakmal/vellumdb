use std::fs::File;
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

use crate::encoding::{read_bytes, write_bytes};
use crate::entry::{Entry, EntryValue};

const TAG_PUT: u8 = 0;
const TAG_TOMBSTONE: u8 = 1;

/// An immutable, sorted key-value file flushed from the memtable. No index or
/// Bloom filter yet (that's milestone 5), so lookups are a linear scan.
pub struct SsTable {
    path: PathBuf,
}

impl SsTable {
    /// Write `entries` (must already be sorted by key, as memtable iteration
    /// guarantees) to a new SSTable file at `path`.
    pub fn write<'a>(
        path: impl AsRef<Path>,
        entries: impl Iterator<Item = (&'a Vec<u8>, &'a Entry)>,
    ) -> io::Result<Self> {
        let path = path.as_ref().to_path_buf();
        let mut writer = BufWriter::new(File::create(&path)?);
        for (key, entry) in entries {
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
        Ok(SsTable { path })
    }

    pub fn open(path: impl AsRef<Path>) -> Self {
        SsTable { path: path.as_ref().to_path_buf() }
    }

    /// Linear scan lookup. Fine for now, an index/Bloom filter makes this
    /// fast without reading the whole file once milestone 5 lands.
    pub fn get(&self, key: &[u8]) -> io::Result<Option<Entry>> {
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
