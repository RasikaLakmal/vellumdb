use std::fs::{File, OpenOptions};
use std::io::{self, BufReader, Read, Write};
use std::path::Path;

use crate::encoding::{read_bytes, write_bytes};

const TAG_PUT: u8 = 0;
const TAG_DELETE: u8 = 1;

pub enum WalRecord {
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
}

pub struct Wal {
    file: File,
}

impl Wal {
    pub fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        Ok(Wal { file })
    }

    pub fn append_put(&mut self, key: &[u8], value: &[u8]) -> io::Result<()> {
        self.file.write_all(&[TAG_PUT])?;
        write_bytes(&mut self.file, key)?;
        write_bytes(&mut self.file, value)?;
        self.file.sync_data()
    }

    pub fn append_delete(&mut self, key: &[u8]) -> io::Result<()> {
        self.file.write_all(&[TAG_DELETE])?;
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

            let key = read_bytes(&mut reader)?;

            match tag[0] {
                TAG_PUT => {
                    let value = read_bytes(&mut reader)?;
                    records.push(WalRecord::Put { key, value });
                }
                TAG_DELETE => records.push(WalRecord::Delete { key }),
                other => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("unknown wal record tag: {other}"),
                    ));
                }
            }
        }

        Ok(records)
    }
}
