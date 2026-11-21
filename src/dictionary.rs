//! The dictionary (DESIGN.md 5.2): maps `hash(word, salt)` to a word ordinal.

use crate::error::{Error, Result};
use std::collections::HashMap;
use std::hash::{BuildHasherDefault, Hasher};

/// Assumed memory cost of one dictionary entry, in bytes (hash table slot, offset, stored hash).
pub const ENTRY_COST_BYTES: usize = 32;

/// Hash function used for words: `(bytes, salt) -> hash`.
pub type HashFn = fn(&[u8], u64) -> u64;

/// xxh3-64 with the salt as the seed.
pub fn xxh3(bytes: &[u8], salt: u64) -> u64 {
    xxhash_rust::xxh3::xxh3_64_with_seed(bytes, salt)
}

/// Passes an already computed 64-bit hash through unchanged.
#[derive(Default)]
struct IdentityHasher(u64);

impl Hasher for IdentityHasher {
    fn finish(&self) -> u64 {
        self.0
    }

    fn write(&mut self, bytes: &[u8]) {
        for &b in bytes {
            self.0 = self.0.rotate_left(8) ^ u64::from(b);
        }
    }

    fn write_u64(&mut self, n: u64) {
        self.0 = n;
    }
}

/// Hash to ordinal map with a memory limit.
pub struct Dictionary {
    map: HashMap<u64, u32, BuildHasherDefault<IdentityHasher>>,
    hash_fn: HashFn,
    max_entries: usize,
}

impl Dictionary {
    /// A dictionary limited to roughly `max_bytes` of memory (`max_bytes / ENTRY_COST_BYTES` entries).
    pub fn new(max_bytes: usize) -> Self {
        Self::with_hash_fn(max_bytes, xxh3)
    }

    /// Like [`Dictionary::new`] with a custom hash function (used to test collisions).
    pub fn with_hash_fn(max_bytes: usize, hash_fn: HashFn) -> Self {
        Self { map: HashMap::default(), hash_fn, max_entries: max_bytes / ENTRY_COST_BYTES }
    }

    /// Hash of `word` with `salt`.
    pub fn hash(&self, word: &[u8], salt: u64) -> u64 {
        (self.hash_fn)(word, salt)
    }

    /// The ordinal stored under `key`.
    pub fn get(&self, key: u64) -> Option<u32> {
        self.map.get(&key).copied()
    }

    /// Stores `ordinal` under `key`, or fails with [`Error::DictionaryFull`].
    pub fn insert(&mut self, key: u64, ordinal: u32) -> Result<()> {
        if self.is_full() {
            return Err(Error::DictionaryFull);
        }
        self.map.insert(key, ordinal);
        Ok(())
    }

    /// Removes `key` (used to roll back a failed insert).
    pub fn remove(&mut self, key: u64) {
        self.map.remove(&key);
    }

    /// Number of entries.
    pub fn len(&self) -> usize {
        self.map.len()
    }

    /// Whether the dictionary has no entries.
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    /// Whether no further entry can be added.
    pub fn is_full(&self) -> bool {
        self.map.len() >= self.max_entries
    }

    /// Estimated memory use in bytes.
    pub fn estimated_bytes(&self) -> usize {
        self.map.len() * ENTRY_COST_BYTES
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn limit_is_enforced_and_removal_frees_room() {
        let mut d = Dictionary::new(ENTRY_COST_BYTES * 2);
        d.insert(1, 0).unwrap();
        d.insert(2, 1).unwrap();
        assert!(matches!(d.insert(3, 2), Err(Error::DictionaryFull)));
        d.remove(2);
        d.insert(3, 2).unwrap();
        assert_eq!((d.get(1), d.get(2), d.get(3), d.estimated_bytes()), (Some(0), None, Some(2), 64));
    }
}
