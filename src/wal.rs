use std::fs::{File, OpenOptions};
use std::io::{self, BufReader, Cursor, Read};
use std::path::{Path, PathBuf};

use crate::encoding::{read_bytes, read_frame, write_bytes, write_frame, Frame};
use crate::entry::{Entry, EntryValue};

const TAG_PUT: u8 = 0;
const TAG_TOMBSTONE: u8 = 1;

pub struct WalRecord {
    pub key: Vec<u8>,
    pub entry: Entry,
}

pub struct Wal {
    file: File,
    path: PathBuf,
}

impl Wal {
    pub fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        let path = path.as_ref().to_path_buf();
        let file = OpenOptions::new().create(true).append(true).open(&path)?;
        Ok(Wal { file, path })
    }

    pub fn append_put(&mut self, seq: u64, key: &[u8], value: &[u8]) -> io::Result<()> {
        let mut payload = Vec::new();
        payload.push(TAG_PUT);
        payload.extend_from_slice(&seq.to_le_bytes());
        write_bytes(&mut payload, key)?;
        write_bytes(&mut payload, value)?;
        write_frame(&mut self.file, &payload)?;
        self.file.sync_data()
    }

    pub fn append_tombstone(&mut self, seq: u64, key: &[u8]) -> io::Result<()> {
        let mut payload = Vec::new();
        payload.push(TAG_TOMBSTONE);
        payload.extend_from_slice(&seq.to_le_bytes());
        write_bytes(&mut payload, key)?;
        write_frame(&mut self.file, &payload)?;
        self.file.sync_data()
    }

    /// Truncates the log to empty. Only safe to call once every record
    /// currently in it corresponds to data that's now durably reflected in
    /// an sstable, i.e. right after a successful flush's manifest commit,
    /// never before it.
    ///
    /// Goes through a fresh handle rather than `self.file.set_len(0)`: on
    /// Windows, a handle opened in append-only mode (`FILE_APPEND_DATA`)
    /// doesn't carry the access right `SetEndOfFile` needs, so resizing it
    /// directly fails with a permission error, Unix's `O_APPEND` doesn't
    /// have this restriction, which is exactly the kind of platform gap
    /// that's easy to miss without actually running this on Windows.
    /// `File::create` opens with write access and truncates by construction,
    /// and since it's a second handle to the same underlying file, `self.
    /// file` sees the truncation too, appends after this still land at the
    /// new (zero) end of file with no explicit seek needed.
    ///
    /// No fsync here: if the process crashes between the manifest commit
    /// and this call, the WAL still holds records for data that's already
    /// safely on disk, so the next `open()` just redundantly replays them
    /// into an otherwise-empty memtable again, exactly what happened before
    /// rotation existed. That's wasted startup work, not data loss, so a
    /// crash losing this particular write costs nothing beyond delaying
    /// rotation by one more flush cycle.
    pub fn truncate(&mut self) -> io::Result<()> {
        File::create(&self.path)?;
        Ok(())
    }

    /// Read every record currently in the log, in order, to rebuild state on
    /// startup. Stops at the first bad frame rather than erroring, whether
    /// it's a torn write (the process died mid-append, which can only ever
    /// land at the tail of an append-only log) or a checksum mismatch, both
    /// are treated as "nothing trustworthy past this point", and everything
    /// read successfully before it is still used. This is the actual torn
    /// write handling this milestone is about: previously, a record cut
    /// short by a crash made `open()` fail outright, bricking the database
    /// on the very next startup after the crash it was supposed to survive.
    pub fn replay(path: impl AsRef<Path>) -> io::Result<Vec<WalRecord>> {
        let file = match File::open(&path) {
            Ok(f) => f,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(e),
        };
        let mut reader = BufReader::new(file);
        let mut records = Vec::new();

        while let Frame::Ok(payload) = read_frame(&mut reader)? {
            // The checksum only proves the payload's bytes weren't cut
            // short or corrupted, not that they actually decode into a
            // valid tag/seq/key/value record. A structurally-too-short or
            // otherwise malformed payload gets the same lenient treatment
            // as a bad frame instead of propagating as a hard error, that's
            // exactly what happens trying to replay a WAL written in an
            // older, incompatible format: this record (and anything after
            // it, which is equally untrustworthy at that point) is dropped
            // rather than bricking the whole open.
            match parse_payload(payload) {
                Ok(record) => records.push(record),
                Err(_) => break,
            }
        }

        Ok(records)
    }
}

fn parse_payload(payload: Vec<u8>) -> io::Result<WalRecord> {
    let mut cursor = Cursor::new(payload);
    let mut tag = [0u8; 1];
    cursor.read_exact(&mut tag)?;

    let mut seq_buf = [0u8; 8];
    cursor.read_exact(&mut seq_buf)?;
    let seq = u64::from_le_bytes(seq_buf);

    let key = read_bytes(&mut cursor)?;

    let value = match tag[0] {
        TAG_PUT => EntryValue::Value(read_bytes(&mut cursor)?),
        TAG_TOMBSTONE => EntryValue::Tombstone,
        other => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unknown wal record tag: {other}"),
            ));
        }
    };

    Ok(WalRecord { key, entry: Entry { seq, value } })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn temp_wal_path(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!("vellumdb_wal_test_{}_{}.wal", std::process::id(), name))
    }

    #[test]
    fn replay_recovers_complete_records_and_drops_a_torn_tail() {
        let path = temp_wal_path("torn");
        let _ = fs::remove_file(&path);

        {
            let mut wal = Wal::open(&path).unwrap();
            wal.append_put(0, b"foo", b"bar").unwrap();
            wal.append_put(1, b"baz", b"qux").unwrap();
        }

        // Simulate a crash mid-append: chop the last few bytes off the
        // file, landing inside the second record's payload rather than
        // between records.
        let full_len = fs::metadata(&path).unwrap().len();
        let file = fs::OpenOptions::new().write(true).open(&path).unwrap();
        file.set_len(full_len - 3).unwrap();
        drop(file);

        let records = Wal::replay(&path).unwrap();
        assert_eq!(records.len(), 1, "the torn second record should be dropped, not error");
        assert_eq!(records[0].key, b"foo");

        fs::remove_file(&path).unwrap();
    }

    #[test]
    fn replay_detects_corruption_and_stops_before_it() {
        let path = temp_wal_path("corrupt");
        let _ = fs::remove_file(&path);

        {
            let mut wal = Wal::open(&path).unwrap();
            wal.append_put(0, b"foo", b"bar").unwrap();
            wal.append_put(1, b"baz", b"qux").unwrap();
        }

        // Flip a byte inside the second record's payload without touching
        // its length/checksum header, so the checksum no longer matches.
        let mut bytes = fs::read(&path).unwrap();
        let corrupt_at = bytes.len() - 2;
        bytes[corrupt_at] ^= 0xFF;
        fs::write(&path, &bytes).unwrap();

        let records = Wal::replay(&path).unwrap();
        assert_eq!(records.len(), 1, "corrupted second record should be dropped, not error");
        assert_eq!(records[0].key, b"foo");

        fs::remove_file(&path).unwrap();
    }

    /// A payload can pass its own checksum (the bytes weren't cut short or
    /// corrupted) while still being too short to decode as a real record,
    /// this is exactly what happens reading a WAL written in an older,
    /// incompatible format. That must be handled the same as a bad frame,
    /// not propagate as a hard error out of `open`.
    #[test]
    fn replay_treats_a_checksum_valid_but_malformed_payload_as_a_bad_frame() {
        let path = temp_wal_path("malformed_payload");
        let _ = fs::remove_file(&path);

        {
            let mut wal = Wal::open(&path).unwrap();
            wal.append_put(0, b"foo", b"bar").unwrap();
        }

        // Append a second, well-framed record whose payload is empty, this
        // passes the length+checksum check on its own terms but is far too
        // short to contain even a tag byte.
        {
            let mut file = fs::OpenOptions::new().append(true).open(&path).unwrap();
            crate::encoding::write_frame(&mut file, &[]).unwrap();
        }

        let records = Wal::replay(&path).unwrap();
        assert_eq!(records.len(), 1, "the malformed record should be dropped, not error");
        assert_eq!(records[0].key, b"foo");

        fs::remove_file(&path).unwrap();
    }
}
