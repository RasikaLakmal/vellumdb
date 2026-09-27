/// A single versioned entry: either a live value or a tombstone marking a
/// delete, each stamped with the sequence number it was written at. The
/// memtable, WAL, and SSTables all store this now instead of raw bytes, so a
/// delete survives being carried forward across a flush instead of just
/// vanishing when the memtable is cleared.
pub struct Entry {
    pub seq: u64,
    pub value: EntryValue,
}

pub enum EntryValue {
    Value(Vec<u8>),
    Tombstone,
}
