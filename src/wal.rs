use std::fs::{File, OpenOptions};
use std::io::{self, BufReader, Read, Write};
use std::path::Path;

use crate::encoding::{read_bytes, write_bytes};
use crate::entry::{Entry, EntryValue};

const TAG_PUT: u8 = 0;
const TAG_TOMBSTONE: u8 = 1;

pub struct WalRecord {
    pub key: Vec<u8>,
    pub entry: Entry,
}

pub struct Wal {
    file: File,
}

impl Wal {
    pub fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        Ok(Wal { file })
    }

    pub fn append_put(&mut self, seq: u64, key: &[u8], value: &[u8]) -> io::Result<()> {
        self.file.write_all(&[TAG_PUT])?;
        self.file.write_all(&seq.to_le_bytes())?;
        write_bytes(&mut self.file, key)?;
        write_bytes(&mut self.file, value)?;
        self.file.sync_data()
    }

    pub fn append_tombstone(&mut self, seq: u64, key: &[u8]) -> io::Result<()> {
        self.file.write_all(&[TAG_TOMBSTONE])?;
        self.file.write_all(&seq.to_le_bytes())?;
        write_bytes(&mut self.file, key)?;
        self.file.sync_data()
    }

    /// Read every record currently in the log, in order, to rebuild state on startup.
    /// A record cut short by a crash mid-write (a torn write) surfaces as an error here.
    /// Recovering gracefully from that instead of just erroring is milestone 9's job.
    pub fn replay(path: impl AsRef<Path>) -> io::Result<Vec<WalRecord>> {
        let file = match File::open(&path) {
            Ok(f) => f,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(e),
        };
        let mut reader = BufReader::new(file);
        let mut records = Vec::new();

        loop {
            let mut tag = [0u8; 1];
            match reader.read_exact(&mut tag) {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e),
            }

            let mut seq_buf = [0u8; 8];
            reader.read_exact(&mut seq_buf)?;
            let seq = u64::from_le_bytes(seq_buf);

            let key = read_bytes(&mut reader)?;

            let value = match tag[0] {
                TAG_PUT => EntryValue::Value(read_bytes(&mut reader)?),
                TAG_TOMBSTONE => EntryValue::Tombstone,
                other => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("unknown wal record tag: {other}"),
                    ));
                }
            };

            records.push(WalRecord { key, entry: Entry { seq, value } });
        }

        Ok(records)
    }
}
