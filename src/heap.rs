//! The word heap (DESIGN.md 5.1): unique multi-character words in ordinal order.

use crate::error::{Error, Result};
use crate::varint;
use bytes::BytesMut;

/// Append-only store of words. A word's ordinal is its index in insertion order.
pub trait Heap {
    /// Appends `word` and returns its ordinal.
    fn push(&mut self, word: &str) -> Result<u32>;
    /// The word with the given ordinal.
    fn get(&self, ordinal: u32) -> Option<&str>;
    /// Number of words.
    fn len(&self) -> u32;
    /// Drops all words with an ordinal of `len` or more (used to roll back a failed insert).
    fn truncate(&mut self, len: u32);
    /// Size of the stored words in bytes, including length prefixes.
    fn byte_len(&self) -> u64;
    /// Whether the heap has no words.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// In-memory heap: words are stored as `[varint length][bytes]` back to back.
#[derive(Debug, Default)]
pub struct MemHeap {
    data: BytesMut,
    offsets: Vec<u32>,
}

impl MemHeap {
    /// An empty heap.
    pub fn new() -> Self {
        Self::default()
    }

    /// The raw stored bytes (`[varint length][bytes]` per word, in ordinal order).
    pub fn as_bytes(&self) -> &[u8] {
        &self.data
    }

    /// Size in bytes of the first `words` words (all of them if there are fewer).
    pub(crate) fn byte_len_of(&self, words: u32) -> u64 {
        self.offsets.get(words as usize).map_or(self.data.len(), |&start| start as usize) as u64
    }
}

impl Heap for MemHeap {
    fn push(&mut self, word: &str) -> Result<u32> {
        let ordinal = u32::try_from(self.offsets.len()).map_err(|_| Error::HeapFull)?;
        if ordinal == u32::MAX || self.data.len() + word.len() + 10 > u32::MAX as usize {
            return Err(Error::HeapFull);
        }
        let start = self.data.len() as u32;
        varint::put(&mut self.data, word.len() as u64);
        self.data.extend_from_slice(word.as_bytes());
        self.offsets.push(start);
        Ok(ordinal)
    }

    fn get(&self, ordinal: u32) -> Option<&str> {
        let data: &[u8] = &self.data;
        let mut pos = *self.offsets.get(ordinal as usize)? as usize;
        let len = varint::get(data, &mut pos).ok()? as usize;
        std::str::from_utf8(data.get(pos..pos.checked_add(len)?)?).ok()
    }

    fn len(&self) -> u32 {
        self.offsets.len() as u32
    }

    fn truncate(&mut self, len: u32) {
        if let Some(&start) = self.offsets.get(len as usize) {
            self.data.truncate(start as usize);
            self.offsets.truncate(len as usize);
        }
    }

    fn byte_len(&self) -> u64 {
        self.data.len() as u64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn push_get_truncate() {
        let mut heap = MemHeap::new();
        let words = ["alpha", "βeta", "x".repeat(300).leak() as &str];
        for (i, w) in words.iter().enumerate() {
            assert_eq!(heap.push(w).unwrap(), i as u32);
        }
        assert_eq!(heap.len(), 3);
        assert!(words.iter().enumerate().all(|(i, w)| heap.get(i as u32) == Some(*w)));
        assert_eq!(heap.get(3), None);
        heap.truncate(1);
        assert_eq!((heap.len(), heap.get(0), heap.get(1)), (1, Some("alpha"), None));
        assert_eq!(heap.byte_len(), 6);
        assert_eq!(heap.push("again").unwrap(), 1);
    }
}
