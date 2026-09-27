use std::fs::{self, File};
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

/// Durable record of which SSTables currently make up the database, plus
/// enough bookkeeping (`next_seq`) to avoid re-scanning their contents on
/// every startup just to recover the sequence counter.
///
/// Installed atomically: a save writes to a temp file, fsyncs it, then
/// renames it over the real path. A rename either fully lands or doesn't
/// happen at all, there's no window where a reader sees a half-written
/// manifest. That's what makes flush (and later, compaction) crash-safe: an
/// sstable file only becomes part of the database once the manifest naming
/// it has been durably installed. A file that exists on disk but isn't
/// listed here is an orphan from an interrupted write and is ignored.
///
/// A real engine (LevelDB) appends small edits to a log instead of
/// rewriting a full snapshot on every save, cheaper for frequent commits.
/// A full-snapshot rewrite is simpler and entirely correct for how
/// infrequently this changes right now (once per flush), so that's what
/// this does.
#[derive(Clone)]
pub struct Manifest {
    pub sstable_ids: Vec<u64>,
    pub next_seq: u64,
}

impl Manifest {
    pub fn empty() -> Self {
        Manifest { sstable_ids: Vec::new(), next_seq: 0 }
    }

    /// Load `dir/MANIFEST`, or an empty manifest if this is a brand new
    /// database that has never flushed.
    pub fn load(dir: &Path) -> io::Result<Self> {
        let file = match File::open(manifest_path(dir)) {
            Ok(f) => f,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Self::empty()),
            Err(e) => return Err(e),
        };
        let mut reader = BufReader::new(file);

        let mut buf8 = [0u8; 8];
        reader.read_exact(&mut buf8)?;
        let next_seq = u64::from_le_bytes(buf8);

        reader.read_exact(&mut buf8)?;
        let count = u64::from_le_bytes(buf8) as usize;

        let mut sstable_ids = Vec::with_capacity(count);
        for _ in 0..count {
            reader.read_exact(&mut buf8)?;
            sstable_ids.push(u64::from_le_bytes(buf8));
        }

        Ok(Manifest { sstable_ids, next_seq })
    }

    pub fn save(&self, dir: &Path) -> io::Result<()> {
        let tmp_path = dir.join("MANIFEST.tmp");
        {
            let file = File::create(&tmp_path)?;
            let mut writer = BufWriter::new(&file);
            writer.write_all(&self.next_seq.to_le_bytes())?;
            writer.write_all(&(self.sstable_ids.len() as u64).to_le_bytes())?;
            for id in &self.sstable_ids {
                writer.write_all(&id.to_le_bytes())?;
            }
            writer.flush()?;
            file.sync_all()?;
        }
        fs::rename(&tmp_path, manifest_path(dir))
    }
}

fn manifest_path(dir: &Path) -> PathBuf {
    dir.join("MANIFEST")
}
