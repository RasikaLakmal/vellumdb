use std::io::{self, Read, Write};

/// Length-prefixed byte encoding: a u32 little-endian length followed by
/// that many raw bytes. Used for individual fields within a record.
pub fn write_bytes(w: &mut impl Write, bytes: &[u8]) -> io::Result<()> {
    w.write_all(&(bytes.len() as u32).to_le_bytes())?;
    w.write_all(bytes)
}

pub fn read_bytes(r: &mut impl Read) -> io::Result<Vec<u8>> {
    let mut len_buf = [0u8; 4];
    r.read_exact(&mut len_buf)?;
    let len = u32::from_le_bytes(len_buf) as usize;
    let mut buf = vec![0u8; len];
    r.read_exact(&mut buf)?;
    Ok(buf)
}

/// Wraps a whole record's bytes in a length + CRC32 checksum envelope. This
/// is what lets a reader tell a torn write (the process died mid-append) or
/// bit corruption apart from a genuine, complete record, instead of
/// silently parsing garbage or failing partway through a nested read.
/// The checksum covers only the payload, a corrupted length field almost
/// always still gets caught, since it makes the read land on the wrong
/// bytes entirely, which then fail the checksum (or hit EOF) anyway.
pub fn write_frame(w: &mut impl Write, payload: &[u8]) -> io::Result<()> {
    let checksum = crc32fast::hash(payload);
    w.write_all(&(payload.len() as u32).to_le_bytes())?;
    w.write_all(&checksum.to_le_bytes())?;
    w.write_all(payload)
}

pub enum Frame {
    /// A complete, checksum-verified record.
    Ok(Vec<u8>),
    /// The header was present but the payload was cut short, or the
    /// checksum didn't match. Kept distinct from `End` so callers can
    /// decide how alarmed to be: expected at a WAL's tail (a torn write
    /// from a crash), unexpected inside an already-committed SSTable.
    Bad,
    /// Nothing left to read, a clean end of stream before any frame header.
    End,
}

pub fn read_frame(r: &mut impl Read) -> io::Result<Frame> {
    let mut len_buf = [0u8; 4];
    match r.read_exact(&mut len_buf) {
        Ok(()) => {}
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(Frame::End),
        Err(e) => return Err(e),
    }
    let len = u32::from_le_bytes(len_buf) as usize;

    let mut checksum_buf = [0u8; 4];
    if r.read_exact(&mut checksum_buf).is_err() {
        return Ok(Frame::Bad);
    }
    let expected = u32::from_le_bytes(checksum_buf);

    let mut payload = vec![0u8; len];
    if r.read_exact(&mut payload).is_err() {
        return Ok(Frame::Bad);
    }

    if crc32fast::hash(&payload) != expected {
        return Ok(Frame::Bad);
    }

    Ok(Frame::Ok(payload))
}
