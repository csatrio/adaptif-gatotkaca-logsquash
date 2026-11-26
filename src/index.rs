//! Search indexes (DESIGN.md 6.5): per-segment term indexes and the folder's sorted word table.
//!
//! A segment index file holds, for every indexed column, up to three **term indexes**, each a sorted directory
//! `key -> posting list` plus the posting lists (ascending row numbers within the segment, delta-varint encoded):
//!
//! | kind | key | built for |
//! |------|-----|-----------|
//! | word | word ordinal | `String` columns with `index_words` |
//! | fold | xxh3 of the case-folded word | `String` columns with `case_insensitive` |
//! | equal | the cell value (strings: xxh3 of the text) | columns with `index_equality` |
//!
//! ```text
//! "LSQIDX01" | u32 term index count | table: per term index [u32 column, u8 kind, u64 directory offset, u32 entries,
//! u64 postings offset, u64 postings length] | directories (20-byte entries: u64 key, u64 postings offset, u32 count)
//! | posting lists | u64 xxh3 checksum of everything before it
//! ```
//! Files are read through [`FileBytes`] (memory-mapped), so a lookup touches only the pages it needs. The checksum is
//! only checked by [`SegmentIndex::verify`], not on every open, to keep opening lazy.
//!
//! Optionally the whole file is stored zstd-compressed (`"LSQIDXZ1" | u32 raw length | zstd bytes`), which is roughly
//! 2.7 times smaller; such a file is decompressed into memory when it is opened instead of being mapped.

use crate::codec::{Reader, Writer};
use crate::dictionary::xxh3;
use crate::error::{Error, Result};
use crate::filebytes::FileBytes;
use crate::heap::Heap;
use crate::query::RowSource;
use crate::schema::Schema;
use crate::tokenizer::{Symbol, decode_with, symbols};
use crate::types::ColumnType;
use crate::varint;
use crate::words::WordStore;
use std::collections::{BTreeMap, HashMap};
use std::io::Write;
use std::path::Path;

pub(crate) const KIND_WORD: u8 = 0;
pub(crate) const KIND_FOLD: u8 = 1;
pub(crate) const KIND_EQUAL: u8 = 2;

const MAGIC: &[u8; 8] = b"LSQIDX01";
const MAGIC_COMPRESSED: &[u8; 8] = b"LSQIDXZ1";
const TABLE_ENTRY: usize = 4 + 1 + 8 + 4 + 8 + 8;
const DIR_ENTRY: usize = 8 + 8 + 4;

/// Hash of the case-folded word (full Unicode case folding).
pub(crate) fn fold_hash(word: &str) -> u64 {
    xxh3(caseless::default_case_fold_str(word).as_bytes(), 0)
}

/// Writes `bytes` to `<path>.tmp`, syncs it and renames it to `path`, so a crash never leaves a half-written `path`.
pub(crate) fn write_file_atomically(path: &Path, bytes: &[u8]) -> Result<()> {
    let mut tmp = path.as_os_str().to_owned();
    tmp.push(".tmp");
    let tmp = std::path::PathBuf::from(tmp);
    let _ = std::fs::remove_file(&tmp);
    write_new_file(&tmp, bytes)?;
    std::fs::rename(&tmp, path)?;
    Ok(())
}

/// Writes `bytes` to a new file and syncs it.
pub(crate) fn write_new_file(path: &Path, bytes: &[u8]) -> Result<()> {
    let mut file = std::fs::OpenOptions::new().write(true).create_new(true).open(path)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    Ok(())
}

/// What an index needs to know about one distinct string entry.
#[derive(Default)]
struct EntryInfo {
    ordinals: Vec<u32>,
    folds: Vec<u64>,
    text_hash: u64,
}

fn encode_postings(rows: &[u32], out: &mut Vec<u8>) {
    let mut previous = 0u32;
    for (i, &row) in rows.iter().enumerate() {
        varint::put(out, u64::from(if i == 0 { row } else { row - previous }));
        previous = row;
    }
}

/// Wraps a built index in the compressed file format.
pub(crate) fn compress_index(raw: &[u8], level: i32) -> Result<Vec<u8>> {
    let mut out = Vec::from(&MAGIC_COMPRESSED[..]);
    out.extend_from_slice(&(raw.len() as u32).to_le_bytes());
    out.extend_from_slice(&zstd::bulk::compress(raw, level)?);
    Ok(out)
}

/// Builds the index file of a segment from its rows, wherever they are (the buffer they are written from, or a written segment).
pub(crate) fn build_index<H: Heap>(src: &dyn RowSource, schema: &Schema, words: &WordStore<H>) -> Result<Vec<u8>> {
    let mut tables: Vec<(u32, u8, BTreeMap<u64, Vec<u32>>)> = Vec::new();
    let mut fold_of: HashMap<u32, u64> = HashMap::new();
    for (c, column) in schema.columns().iter().enumerate() {
        let flags = column.flags;
        let is_string = column.ty == ColumnType::String;
        let (want_words, want_fold, want_equal) =
            (flags.index_words && is_string, flags.case_insensitive && is_string, flags.index_equality);
        if !(want_words || want_fold || want_equal) {
            continue;
        }
        let (mut word_lists, mut fold_lists, mut equal_lists) = (BTreeMap::new(), BTreeMap::new(), BTreeMap::<u64, Vec<u32>>::new());
        let mut entries: HashMap<u64, EntryInfo> = HashMap::new();
        for row in 0..src.row_count() as usize {
            let (cell, null) = src.cell(row as u64, c)?;
            if null {
                continue;
            }
            if !is_string {
                equal_lists.entry(cell).or_default().push(row as u32);
                continue;
            }
            if let std::collections::hash_map::Entry::Vacant(slot) = entries.entry(cell) {
                let mut info = EntryInfo::default();
                src.with_entry(cell, &mut |tail| {
                    if want_words || want_fold {
                        info.ordinals =
                            symbols(tail)?.into_iter().filter_map(|s| if let Symbol::Word(o) = s { Some(o) } else { None }).collect();
                        info.ordinals.sort_unstable();
                        info.ordinals.dedup();
                    }
                    if want_fold {
                        for &ordinal in &info.ordinals {
                            let word = words.get(ordinal).ok_or(Error::Corrupt("unknown word ordinal"))?;
                            info.folds.push(*fold_of.entry(ordinal).or_insert_with(|| fold_hash(word)));
                        }
                        info.folds.sort_unstable();
                        info.folds.dedup();
                    }
                    if want_equal {
                        let mut text = Vec::new();
                        decode_with(words, tail, &mut text)?;
                        info.text_hash = xxh3(&text, 0);
                    }
                    Ok(())
                })?;
                slot.insert(info);
            }
            let info = &entries[&cell];
            if want_words {
                info.ordinals.iter().for_each(|&o| word_lists.entry(u64::from(o)).or_insert_with(Vec::new).push(row as u32));
            }
            if want_fold {
                info.folds.iter().for_each(|&h| fold_lists.entry(h).or_insert_with(Vec::new).push(row as u32));
            }
            if want_equal {
                equal_lists.entry(info.text_hash).or_default().push(row as u32);
            }
        }
        if want_words {
            tables.push((c as u32, KIND_WORD, word_lists));
        }
        if want_fold {
            tables.push((c as u32, KIND_FOLD, fold_lists));
        }
        if want_equal {
            tables.push((c as u32, KIND_EQUAL, equal_lists));
        }
    }

    let header_len = MAGIC.len() + 4 + TABLE_ENTRY * tables.len();
    let mut body = Vec::new();
    let mut table = Writer::new();
    for (column, kind, lists) in &tables {
        let (mut directory, mut postings) = (Writer::new(), Vec::new());
        for (key, rows) in lists {
            directory.u64(*key);
            directory.u64(postings.len() as u64);
            directory.u32(rows.len() as u32);
            encode_postings(rows, &mut postings);
        }
        let dir_offset = (header_len + body.len()) as u64;
        table.u32(*column);
        table.u8(*kind);
        table.u64(dir_offset);
        table.u32(lists.len() as u32);
        table.u64(dir_offset + directory.buf.len() as u64);
        table.u64(postings.len() as u64);
        body.extend_from_slice(&directory.buf);
        body.extend_from_slice(&postings);
    }
    let mut out = Vec::with_capacity(header_len + body.len() + 8);
    out.extend_from_slice(MAGIC);
    out.extend_from_slice(&(tables.len() as u32).to_le_bytes());
    out.extend_from_slice(&table.buf);
    out.extend_from_slice(&body);
    let checksum = xxh3(&out, 0);
    out.extend_from_slice(&checksum.to_le_bytes());
    Ok(out)
}

#[derive(Debug, Clone, Copy)]
struct Term {
    column: u32,
    kind: u8,
    dir_offset: usize,
    entries: usize,
    post_offset: usize,
    post_len: usize,
}

/// A segment's index file, opened for lookups.
pub(crate) struct SegmentIndex {
    bytes: FileBytes,
    terms: Vec<Term>,
}

impl SegmentIndex {
    /// Opens an index file and validates its structure (not its checksum).
    pub(crate) fn open(path: &Path) -> Result<Self> {
        let bytes = FileBytes::open(path)?;
        if bytes.starts_with(MAGIC_COMPRESSED) {
            let raw_len = bytes
                .get(8..12)
                .map(|b| u32::from_le_bytes(b.try_into().expect("4 bytes")) as usize)
                .ok_or(Error::Corrupt("segment index file"))?;
            let raw = zstd::bulk::decompress(&bytes[12..], raw_len).map_err(|_| Error::Corrupt("segment index does not decompress"))?;
            return Self::from_vec(raw);
        }
        Self::from_bytes(bytes)
    }

    /// An index held in memory (for rows that are not in a segment yet).
    pub(crate) fn from_vec(bytes: Vec<u8>) -> Result<Self> {
        Self::from_bytes(FileBytes::Owned(bytes))
    }

    fn from_bytes(bytes: FileBytes) -> Result<Self> {
        let corrupt = || Error::Corrupt("segment index file");
        if bytes.len() < MAGIC.len() + 4 + 8 || &bytes[..8] != MAGIC {
            return Err(corrupt());
        }
        let body_end = bytes.len() - 8;
        let mut r = Reader::new(&bytes[8..body_end]);
        let count = r.u32()? as usize;
        let mut terms = Vec::new();
        for _ in 0..count {
            let (column, kind) = (r.u32()?, r.u8()?);
            let to_usize = |v: u64| usize::try_from(v).map_err(|_| corrupt());
            let (dir_offset, entries) = (to_usize(r.u64()?)?, r.u32()? as usize);
            let (post_offset, post_len) = (to_usize(r.u64()?)?, to_usize(r.u64()?)?);
            let dir_end = dir_offset.checked_add(entries.checked_mul(DIR_ENTRY).ok_or_else(corrupt)?).ok_or_else(corrupt)?;
            if dir_end != post_offset || post_offset.checked_add(post_len).is_none_or(|end| end > body_end) || kind > KIND_EQUAL {
                return Err(corrupt());
            }
            terms.push(Term { column, kind, dir_offset, entries, post_offset, post_len });
        }
        Ok(Self { bytes, terms })
    }

    /// Checks the whole-file checksum (reads every page of the file).
    pub(crate) fn verify(&self) -> Result<()> {
        let (body, tail) = self.bytes.split_at(self.bytes.len() - 8);
        if u64::from_le_bytes(tail.try_into().expect("8 bytes")) != xxh3(body, 0) {
            return Err(Error::Corrupt("segment index checksum"));
        }
        Ok(())
    }

    fn term(&self, column: usize, kind: u8) -> Option<&Term> {
        self.terms.iter().find(|t| t.column as usize == column && t.kind == kind)
    }

    /// Whether the index has a term index of this kind for the column.
    pub(crate) fn has(&self, column: usize, kind: u8) -> bool {
        self.term(column, kind).is_some()
    }

    /// The ascending row numbers stored under `key`; `None` if the key is absent or the column has no such index.
    pub(crate) fn postings(&self, column: usize, kind: u8, key: u64) -> Result<Option<Vec<u32>>> {
        let Some(term) = self.term(column, kind) else { return Ok(None) };
        let entry = |i: usize| &self.bytes[term.dir_offset + i * DIR_ENTRY..term.dir_offset + (i + 1) * DIR_ENTRY];
        let key_at = |i: usize| u64::from_le_bytes(entry(i)[..8].try_into().expect("8 bytes"));
        let (mut lo, mut hi) = (0, term.entries);
        while lo < hi {
            let mid = (lo + hi) / 2;
            if key_at(mid) < key {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        if lo == term.entries || key_at(lo) != key {
            return Ok(None);
        }
        let mut r = Reader::new(entry(lo));
        let (_, offset, count) = (r.u64()?, r.u64()? as usize, r.u32()? as usize);
        let section = &self.bytes[term.post_offset..term.post_offset + term.post_len];
        let mut pos = offset;
        let mut rows = Vec::with_capacity(count);
        let mut previous = 0u32;
        for i in 0..count {
            let delta = u32::try_from(varint::get(section, &mut pos)?).map_err(|_| Error::Corrupt("posting list"))?;
            previous = if i == 0 { delta } else { previous.checked_add(delta).ok_or(Error::Corrupt("posting list"))? };
            rows.push(previous);
        }
        Ok(Some(rows))
    }
}

/// The folder's words sorted by their bytes (`words.idx`), for word lookup and prefix search without a dictionary.
pub(crate) struct WordTable {
    bytes: FileBytes,
    count: usize,
}

const WORD_MAGIC: &[u8; 8] = b"LSQWRD01";

impl WordTable {
    /// Writes the table for all words of `words`.
    pub(crate) fn write<H: Heap>(path: &Path, words: &WordStore<H>) -> Result<()> {
        let mut ordinals: Vec<u32> = (0..words.word_count()).collect();
        ordinals.sort_by(|&a, &b| words.get(a).map(str::as_bytes).cmp(&words.get(b).map(str::as_bytes)));
        let mut out = Vec::with_capacity(12 + 4 * ordinals.len() + 8);
        out.extend_from_slice(WORD_MAGIC);
        out.extend_from_slice(&(ordinals.len() as u32).to_le_bytes());
        ordinals.iter().for_each(|o| out.extend_from_slice(&o.to_le_bytes()));
        let checksum = xxh3(&out, 0);
        out.extend_from_slice(&checksum.to_le_bytes());
        write_new_file(path, &out)
    }

    pub(crate) fn open(path: &Path) -> Result<Self> {
        let bytes = FileBytes::open(path)?;
        let count = bytes.get(8..12).map(|b| u32::from_le_bytes(b.try_into().expect("4 bytes")) as usize);
        match count {
            Some(count) if bytes.starts_with(WORD_MAGIC) && bytes.len() == 12 + 4 * count + 8 => Ok(Self { bytes, count }),
            _ => Err(Error::Corrupt("word table file")),
        }
    }

    fn ordinal_at(&self, i: usize) -> u32 {
        u32::from_le_bytes(self.bytes[12 + 4 * i..16 + 4 * i].try_into().expect("4 bytes"))
    }

    /// Index of the first entry whose word is not below `key`.
    fn lower_bound<H: Heap>(&self, words: &WordStore<H>, key: &[u8]) -> usize {
        let (mut lo, mut hi) = (0, self.count);
        while lo < hi {
            let mid = (lo + hi) / 2;
            if words.get(self.ordinal_at(mid)).map(str::as_bytes).unwrap_or_default() < key {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        lo
    }

    /// The ordinal of `word`.
    pub(crate) fn lookup<H: Heap>(&self, words: &WordStore<H>, word: &str) -> Option<u32> {
        let i = self.lower_bound(words, word.as_bytes());
        (i < self.count).then(|| self.ordinal_at(i)).filter(|&o| words.get(o) == Some(word))
    }

    /// Up to `limit` words starting with `prefix`, in byte order.
    pub(crate) fn prefix<H: Heap>(&self, words: &WordStore<H>, prefix: &str, limit: usize) -> Vec<String> {
        let start = self.lower_bound(words, prefix.as_bytes());
        (start..self.count)
            .filter_map(|i| words.get(self.ordinal_at(i)))
            .take_while(|w| w.starts_with(prefix))
            .take(limit)
            .map(str::to_owned)
            .collect()
    }

    pub(crate) fn verify(&self) -> Result<()> {
        let (body, tail) = self.bytes.split_at(self.bytes.len() - 8);
        if u64::from_le_bytes(tail.try_into().expect("8 bytes")) != xxh3(body, 0) {
            return Err(Error::Corrupt("word table checksum"));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cell;
    use crate::dataframe::DataFrame;
    use crate::dictionary::Dictionary;
    use crate::heap::MemHeap;
    use crate::query::BufferSource;
    use crate::schema::Column;
    use crate::types::Value;
    use crate::words::SharedWords;

    fn shared() -> SharedWords {
        WordStore::new(MemHeap::new(), Dictionary::new(1 << 20)).into_shared()
    }

    fn schema() -> Schema {
        Schema::new(vec![
            Column::new("n", ColumnType::I64).index_equality(),
            Column::new("msg", ColumnType::String).index_words().case_insensitive().index_equality(),
            Column::new("plain", ColumnType::String),
        ])
        .unwrap()
    }

    fn frame(shared: &SharedWords) -> DataFrame {
        let frame = DataFrame::new(schema(), shared.clone());
        for i in 0..50i64 {
            let msg = format!("Error code {} in Module{}", i % 3, i % 4);
            let row = [Value::I64(i % 5), if i % 7 == 0 { Value::Null } else { Value::Str(&msg) }, Value::Str("x y z")];
            frame.push_row(&row).unwrap();
        }
        frame
    }

    fn built(frame: &DataFrame, shared: &SharedWords) -> SegmentIndex {
        let words = shared.read().unwrap();
        SegmentIndex::from_vec(frame.with_inner(|i| build_index(&BufferSource::all(i), frame.schema(), &words)).unwrap()).unwrap()
    }

    #[test]
    fn postings_match_a_brute_force_scan() {
        let shared = shared();
        let frame = frame(&shared);
        let index = built(&frame, &shared);
        index.verify().unwrap();
        assert!(index.has(0, KIND_EQUAL) && index.has(1, KIND_WORD) && index.has(1, KIND_FOLD) && index.has(1, KIND_EQUAL));
        assert!(!index.has(2, KIND_WORD) && !index.has(0, KIND_WORD));

        let expect = |pred: &dyn Fn(i64) -> bool| (0..50i64).filter(|&i| pred(i)).map(|i| i as u32).collect::<Vec<_>>();
        let words = shared.read().unwrap();
        let ordinal = |w: &str| u64::from(words.lookup(w).unwrap());
        assert_eq!(index.postings(1, KIND_WORD, ordinal("Error")).unwrap().unwrap(), expect(&|i| i % 7 != 0));
        assert_eq!(index.postings(1, KIND_WORD, ordinal("Module2")).unwrap().unwrap(), expect(&|i| i % 7 != 0 && i % 4 == 2));
        assert_eq!(index.postings(1, KIND_FOLD, fold_hash("module2")).unwrap().unwrap(), expect(&|i| i % 7 != 0 && i % 4 == 2));
        assert_eq!(index.postings(1, KIND_FOLD, fold_hash("ERROR")).unwrap().unwrap(), expect(&|i| i % 7 != 0));
        assert_eq!(index.postings(0, KIND_EQUAL, cell::from_signed(3)).unwrap().unwrap(), expect(&|i| i % 5 == 3));
        let text_hash = xxh3(b"Error code 1 in Module1", 0);
        assert_eq!(index.postings(1, KIND_EQUAL, text_hash).unwrap().unwrap(), expect(&|i| i % 7 != 0 && i % 3 == 1 && i % 4 == 1));
        assert_eq!(index.postings(1, KIND_WORD, 999_999).unwrap(), None);
        assert_eq!(index.postings(2, KIND_WORD, 0).unwrap(), None);
    }

    #[test]
    fn files_round_trip_and_damage_is_detected() {
        let shared = shared();
        let frame = frame(&shared);
        let words = shared.read().unwrap();
        let bytes = frame.with_inner(|i| build_index(&BufferSource::all(i), frame.schema(), &words)).unwrap();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("a.idx");
        write_new_file(&path, &bytes).unwrap();
        let index = SegmentIndex::open(&path).unwrap();
        index.verify().unwrap();
        assert!(index.postings(0, KIND_EQUAL, cell::from_signed(0)).unwrap().is_some());
        drop(index);

        let mut bad = bytes.clone();
        let middle = bad.len() - 20;
        bad[middle] ^= 0xff;
        std::fs::write(&path, &bad).unwrap();
        assert!(SegmentIndex::open(&path).unwrap().verify().is_err());
        for cut in [0, 5, 11, bytes.len() / 2, bytes.len() - 1] {
            std::fs::write(&path, &bytes[..cut]).unwrap();
            assert!(SegmentIndex::open(&path).is_err() || cut == bytes.len() - 1, "cut {cut}");
        }
        let mut wrong_magic = bytes.clone();
        wrong_magic[0] = b'X';
        std::fs::write(&path, &wrong_magic).unwrap();
        assert!(SegmentIndex::open(&path).is_err());
    }

    #[test]
    fn compressed_index_files_open_like_plain_ones() {
        let shared = shared();
        let frame = frame(&shared);
        let words = shared.read().unwrap();
        let raw = frame.with_inner(|i| build_index(&BufferSource::all(i), frame.schema(), &words)).unwrap();
        let packed = compress_index(&raw, 3).unwrap();
        assert!(packed.len() < raw.len());
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("z.idx");
        write_new_file(&path, &packed).unwrap();
        let index = SegmentIndex::open(&path).unwrap();
        index.verify().unwrap();
        let plain = SegmentIndex::from_vec(raw).unwrap();
        for key in 0..5 {
            let key = cell::from_signed(key);
            assert_eq!(index.postings(0, KIND_EQUAL, key).unwrap(), plain.postings(0, KIND_EQUAL, key).unwrap());
        }
        std::fs::write(&path, &packed[..packed.len() - 3]).unwrap();
        assert!(SegmentIndex::open(&path).is_err());
    }

    #[test]
    fn word_table_looks_up_words_and_prefixes() {
        let shared = shared();
        let mut store = shared.write().unwrap();
        for w in ["banana", "apple", "applesauce", "Apple", "cherry", "app", "zebra"] {
            store.intern(w).unwrap();
        }
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("words.idx");
        WordTable::write(&path, &store).unwrap();
        let table = WordTable::open(&path).unwrap();
        table.verify().unwrap();
        assert_eq!(table.lookup(&store, "cherry"), store.lookup("cherry"));
        assert_eq!(table.lookup(&store, "cherries"), None);
        assert_eq!(table.prefix(&store, "app", 10), ["app", "apple", "applesauce"]);
        assert_eq!(table.prefix(&store, "app", 2), ["app", "apple"]);
        assert_eq!(table.prefix(&store, "", 100).len(), 7);
        assert!(table.prefix(&store, "q", 5).is_empty());
        std::fs::write(&path, b"junk").unwrap();
        assert!(WordTable::open(&path).is_err());
    }
}
