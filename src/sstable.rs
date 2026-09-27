use std::fs::File;
use std::io::{self, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};

use crate::encoding::{read_bytes, write_bytes};

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
        entries: impl Iterator<Item = (&'a Vec<u8>, &'a Vec<u8>)>,
    ) -> io::Result<Self> {
        let path = path.as_ref().to_path_buf();
        let mut writer = BufWriter::new(File::create(&path)?);
        for (key, value) in entries {
            write_bytes(&mut writer, key)?;
            write_bytes(&mut writer, value)?;
        }
        writer.flush()?;
        Ok(SsTable { path })
    }

    pub fn open(path: impl AsRef<Path>) -> Self {
        SsTable { path: path.as_ref().to_path_buf() }
    }

    /// Linear scan lookup. Fine for now, an index/Bloom filter makes this
    /// fast without reading the whole file once milestone 5 lands.
    pub fn get(&self, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        for entry in self.iter()? {
            let (k, v) = entry?;
            if k == key {
                return Ok(Some(v));
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
    type Item = io::Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        match read_bytes(&mut self.reader) {
            Ok(key) => match read_bytes(&mut self.reader) {
                Ok(value) => Some(Ok((key, value))),
                Err(e) => Some(Err(e)),
            },
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => None,
            Err(e) => Some(Err(e)),
        }
    }
}
