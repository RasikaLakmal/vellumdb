use std::fs::{File, OpenOptions};
use std::io::{self, BufReader, Read, Write};
use std::path::Path;

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

fn write_bytes(w: &mut impl Write, bytes: &[u8]) -> io::Result<()> {
    w.write_all(&(bytes.len() as u32).to_le_bytes())?;
    w.write_all(bytes)
}

fn read_bytes(r: &mut impl Read) -> io::Result<Vec<u8>> {
    let mut len_buf = [0u8; 4];
    r.read_exact(&mut len_buf)?;
    let len = u32::from_le_bytes(len_buf) as usize;
    let mut buf = vec![0u8; len];
    r.read_exact(&mut buf)?;
    Ok(buf)
}
