//! The shared word store: heap + dictionary behind one lock (DESIGN.md 5.4).

use crate::dictionary::Dictionary;
use crate::error::Result;
use crate::heap::{Heap, MemHeap};
use std::sync::{Arc, RwLock};

/// Heap and dictionary together, so they can never get out of sync.
pub struct WordStore<H: Heap = MemHeap> {
    heap: H,
    dict: Dictionary,
    /// The dictionary key of every word, by ordinal (needed to roll back inserts).
    keys: Vec<u64>,
}

/// A point to roll a [`WordStore`] back to.
#[derive(Debug, Clone, Copy)]
pub struct Checkpoint(u32);

/// A word store shared between tokenizers and dataframes.
pub type SharedWords<H = MemHeap> = Arc<RwLock<WordStore<H>>>;

impl<H: Heap> WordStore<H> {
    /// A store over an empty heap.
    pub fn new(heap: H, dict: Dictionary) -> Self {
        debug_assert!(heap.is_empty());
        Self { heap, dict, keys: Vec::new() }
    }

    /// Rebuilds the dictionary of an existing heap by re-hashing every word in ordinal order.
    /// This reproduces exactly the salts used when the words were first inserted.
    pub fn from_heap(heap: H, dict: Dictionary) -> Result<Self> {
        let mut store = Self { heap, dict, keys: Vec::new() };
        for ordinal in 0..store.heap.len() {
            let word = store.heap.get(ordinal).ok_or(crate::Error::Corrupt("heap word is not valid UTF-8"))?;
            let mut salt = 0;
            let key = loop {
                let key = store.dict.hash(word.as_bytes(), salt);
                if store.dict.get(key).is_none() {
                    break key;
                }
                salt += 1;
            };
            store.dict.insert(key, ordinal)?;
            store.keys.push(key);
        }
        Ok(store)
    }

    /// Wraps the store for sharing.
    pub fn into_shared(self) -> SharedWords<H> {
        Arc::new(RwLock::new(self))
    }

    /// The ordinal of `word`, if it is stored.
    pub fn lookup(&self, word: &str) -> Option<u32> {
        let mut salt = 0;
        loop {
            let ordinal = self.dict.get(self.dict.hash(word.as_bytes(), salt))?;
            if self.heap.get(ordinal) == Some(word) {
                return Some(ordinal);
            }
            salt += 1; // another word took this hash: probe the next salt
        }
    }

    /// The ordinal of `word`, storing it first if it is new.
    pub fn intern(&mut self, word: &str) -> Result<u32> {
        let mut salt = 0;
        loop {
            let key = self.dict.hash(word.as_bytes(), salt);
            match self.dict.get(key) {
                None => {
                    if self.dict.is_full() {
                        return Err(crate::Error::DictionaryFull);
                    }
                    let ordinal = self.heap.push(word)?;
                    self.dict.insert(key, ordinal)?;
                    self.keys.push(key);
                    return Ok(ordinal);
                }
                Some(ordinal) if self.heap.get(ordinal) == Some(word) => return Ok(ordinal),
                Some(_) => salt += 1,
            }
        }
    }

    /// The word with the given ordinal.
    pub fn get(&self, ordinal: u32) -> Option<&str> {
        self.heap.get(ordinal)
    }

    /// Number of stored words.
    pub fn word_count(&self) -> u32 {
        self.heap.len()
    }

    /// The heap.
    pub fn heap(&self) -> &H {
        &self.heap
    }

    /// The dictionary.
    pub fn dictionary(&self) -> &Dictionary {
        &self.dict
    }

    /// Marks the current state, to undo later inserts with [`WordStore::rollback`].
    pub fn checkpoint(&self) -> Checkpoint {
        Checkpoint(self.heap.len())
    }

    /// Removes every word stored after `checkpoint`.
    pub fn rollback(&mut self, checkpoint: Checkpoint) {
        while self.keys.len() > checkpoint.0 as usize {
            if let Some(key) = self.keys.pop() {
                self.dict.remove(key);
            }
        }
        self.heap.truncate(checkpoint.0);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Error;
    use crate::dictionary::ENTRY_COST_BYTES;

    fn store(max_entries: usize) -> WordStore {
        WordStore::new(MemHeap::new(), Dictionary::new(max_entries * ENTRY_COST_BYTES))
    }

    #[test]
    fn interning_deduplicates() {
        let mut s = store(10);
        let (a, b) = (s.intern("alpha").unwrap(), s.intern("beta").unwrap());
        assert_eq!((s.intern("alpha").unwrap(), a, b), (a, 0, 1));
        assert_eq!((s.lookup("beta"), s.lookup("gamma"), s.get(1)), (Some(1), None, Some("beta")));
        assert_eq!(s.word_count(), 2);
    }

    #[test]
    fn colliding_hashes_are_resolved_by_salt() {
        // Every word hashes to the salt itself, so all words collide on salt 0.
        let dict = Dictionary::with_hash_fn(ENTRY_COST_BYTES * 10, |_, salt| salt);
        let mut s = WordStore::new(MemHeap::new(), dict);
        let ordinals: Vec<u32> = ["aa", "bb", "cc"].iter().map(|w| s.intern(w).unwrap()).collect();
        assert_eq!(ordinals, [0, 1, 2]);
        assert_eq!(s.lookup("cc"), Some(2));
        assert_eq!(s.intern("bb").unwrap(), 1);
        assert_eq!(s.lookup("dd"), None);
    }

    #[test]
    fn full_dictionary_rejects_new_words_but_not_known_ones() {
        let mut s = store(2);
        s.intern("aa").unwrap();
        s.intern("bb").unwrap();
        assert!(matches!(s.intern("cc"), Err(Error::DictionaryFull)));
        assert_eq!((s.intern("aa").unwrap(), s.word_count()), (0, 2));
    }

    #[test]
    fn rollback_removes_later_words() {
        let mut s = store(10);
        s.intern("keep").unwrap();
        let cp = s.checkpoint();
        s.intern("drop1").unwrap();
        s.intern("drop2").unwrap();
        s.rollback(cp);
        assert_eq!((s.word_count(), s.lookup("drop1"), s.lookup("keep")), (1, None, Some(0)));
        assert_eq!(s.intern("new").unwrap(), 1);
    }

    #[test]
    fn dictionary_is_rebuilt_from_the_heap_with_the_same_salts() {
        let make = || Dictionary::with_hash_fn(ENTRY_COST_BYTES * 10, |_, salt| salt);
        let mut s = WordStore::new(MemHeap::new(), make());
        for w in ["aa", "bb", "cc"] {
            s.intern(w).unwrap();
        }
        let mut heap = MemHeap::new();
        for i in 0..s.word_count() {
            heap.push(s.get(i).unwrap()).unwrap();
        }
        let rebuilt = WordStore::from_heap(heap, make()).unwrap();
        assert_eq!((rebuilt.lookup("aa"), rebuilt.lookup("bb"), rebuilt.lookup("cc")), (Some(0), Some(1), Some(2)));
    }
}
