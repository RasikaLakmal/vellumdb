use std::io;
use std::iter::Peekable;

use crate::entry::{Entry, EntryValue};

/// A sorted source of `(key, entry)` pairs, the memtable and each SSTable's
/// on-disk iterator both fit this, so the merge below doesn't need to know
/// which kind of source it's pulling from.
pub type SourceIter<'a> = Box<dyn Iterator<Item = io::Result<(Vec<u8>, Entry)>> + 'a>;

/// K-way merge across every source in ascending key order. Sources must be
/// given oldest to newest, with the memtable last, since when the same key
/// appears in more than one, the source that appears *later* in this list
/// wins, and every matching source is advanced past that key regardless of
/// which one wins, so a stale duplicate is never seen again. A key whose
/// winning entry is a tombstone is dropped from the output entirely rather
/// than yielded.
///
/// Finds the minimum key by scanning all sources on every step rather than
/// keeping them in a heap. That's O(sources) per step instead of
/// O(log sources), a deliberate simplicity-over-efficiency choice that's
/// fine at the sstable counts this engine produces before any tiering
/// exists; a heap would be the natural upgrade if that changes.
pub struct MergeIter<'a> {
    sources: Vec<Peekable<SourceIter<'a>>>,
}

impl<'a> MergeIter<'a> {
    pub fn new(sources: Vec<SourceIter<'a>>) -> Self {
        MergeIter { sources: sources.into_iter().map(Iterator::peekable).collect() }
    }
}

impl Iterator for MergeIter<'_> {
    type Item = io::Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            // A source with an error at its head means the whole merge is
            // in trouble, surface it immediately rather than trying to
            // route around it.
            for source in &mut self.sources {
                if matches!(source.peek(), Some(Err(_))) {
                    return match source.next() {
                        Some(Err(e)) => Some(Err(e)),
                        _ => unreachable!("peek just confirmed Some(Err(_))"),
                    };
                }
            }

            let winner_key = self
                .sources
                .iter_mut()
                .filter_map(|s| s.peek().map(|r| r.as_ref().unwrap().0.clone()))
                .min()?;

            // Every source currently at the winning key gets consumed, the
            // last one encountered in source order (i.e. the most recent,
            // since sources are oldest-to-newest with the memtable last)
            // is the entry that actually survives.
            let mut winning_entry: Option<Entry> = None;
            for source in &mut self.sources {
                let at_winner = matches!(source.peek(), Some(Ok((k, _))) if *k == winner_key);
                if at_winner {
                    winning_entry = Some(source.next().unwrap().unwrap().1);
                }
            }

            match winning_entry.expect("min key came from one of these sources").value {
                EntryValue::Value(v) => return Some(Ok((winner_key, v))),
                EntryValue::Tombstone => continue,
            }
        }
    }
}
