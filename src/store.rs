//! Stores (DESIGN.md 7): a folder holding a shared word heap and the segments of one or more dataframes.
//!
//! ```text
//! <folder>/manifest          what is committed (atomically replaced; the commit point)
//! <folder>/heap.bin          append-only zstd blocks of words, shared by all dataframes
//! <folder>/f<id>-<n>.seg     immutable segment files (see the segment module)
//! <folder>/lock              held exclusively by the process that has the store open for writing
//! ```
//! Rows are buffered in memory until [`Store::commit`], which writes the new words, then one segment per dataframe
//! that has new rows, then the manifest. A crash before the manifest is replaced leaves the previous commit intact;
//! stray files are removed the next time the store is opened for writing.

use crate::codec::{Reader, Writer};
use crate::dataframe::{DataFrame, DataFrameOptions};
use crate::dictionary::{Dictionary, xxh3};
use crate::error::{Error, Result};
use crate::heap::{Heap, MemHeap};
use crate::index::{SegmentIndex, WordTable, build_index, compress_index, write_file_atomically};
use crate::query::{self, BufferSource, Page, PageResult, Prepared, Query, RowSource};
use crate::schema::{Column, ColumnFlags, Schema};
use crate::segment::{SegmentReader, WriteOptions, write_segment};
use crate::tokenizer::TokenizerOptions;
use crate::types::{ColumnType, OwnedValue, Value};
use crate::varint;
use crate::words::{SharedWords, WordStore};
use bytes::BytesMut;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};

const MANIFEST_MAGIC: &[u8; 8] = b"LSQMANI1";
const HEAP_MAGIC: &[u8; 8] = b"LSQHEAP1";
/// Version of the file formats written by this code.
const FORMAT_VERSION: u32 = 1;
/// Version of the splitting rules (default and JSON-escape splitters); a folder is only opened if it matches.
const SPLITTER_VERSION: u8 = 1;
const HEAP_RECORD_HEADER: usize = 4 + 4 + 4 + 8;

/// Settings of a [`Store`].
#[derive(Debug, Clone)]
pub struct StoreOptions {
    /// Tokenizer thresholds.
    pub tokenizer: TokenizerOptions,
    /// Limit of the dictionary's estimated memory in bytes (default 64 MiB).
    pub dictionary_bytes: usize,
    /// Entries of the whole-string dedup map of each dataframe buffer.
    pub dedup_entries: usize,
    /// zstd level of every block (default 19).
    pub zstd_level: i32,
    /// Rows at which [`Store::commit_if_full`] commits (default 100,000).
    pub segment_rows: usize,
    /// Rows per column block (default 4,096).
    pub block_rows: usize,
    /// Target size of a row-heap block in bytes (default 1 MiB; bigger compresses better but makes random row reads slower).
    pub row_heap_block_bytes: usize,
    /// Target size of a heap block in bytes (default 1 MiB).
    pub heap_block_bytes: usize,
    /// Bytes of decoded blocks each segment reader keeps for reuse (default 32 MiB).
    pub cache_bytes: usize,
    /// Write search index files when committing (default true). Without them, searches scan the rows; the indexes can
    /// be built later with [`Store::build_indexes`].
    pub indexing: bool,
    /// Store the search index files zstd-compressed (default true; about 2.7 times smaller, but read into memory
    /// instead of memory-mapped).
    pub compress_indexes: bool,
}

impl Default for StoreOptions {
    fn default() -> Self {
        Self {
            tokenizer: TokenizerOptions::default(),
            dictionary_bytes: 64 << 20,
            dedup_entries: 4_000_000,
            zstd_level: 19,
            segment_rows: 100_000,
            block_rows: 4096,
            row_heap_block_bytes: 1 << 20,
            heap_block_bytes: 1 << 20,
            cache_bytes: crate::segment::DEFAULT_CACHE_BYTES,
            indexing: true,
            compress_indexes: true,
        }
    }
}

// ---------------------------------------------------------------------------------------------------------------------
// Manifest

#[derive(Debug, Clone, PartialEq)]
struct SegmentMeta {
    file: String,
    rows: u64,
}

#[derive(Debug, Clone, PartialEq)]
struct FrameMeta {
    name: String,
    schema: Schema,
    segments: Vec<SegmentMeta>,
    next_segment: u32,
}

#[derive(Debug, Clone, PartialEq)]
struct Manifest {
    sealed: bool,
    /// Committed length of `heap.bin` in bytes.
    heap_bytes: u64,
    heap_words: u32,
    frames: Vec<FrameMeta>,
}

fn write_schema(w: &mut Writer, schema: &Schema) {
    w.varint(schema.len() as u64);
    for c in schema.columns() {
        w.str(&c.name);
        w.u8(c.ty.code());
        let f = c.flags;
        w.u8(u8::from(f.json_escapes) | u8::from(f.case_insensitive) << 1 | u8::from(f.index_equality) << 2 | u8::from(f.index_words) << 3);
        w.str(&c.text_format);
    }
}

fn read_schema(r: &mut Reader<'_>) -> Result<Schema> {
    let mut columns = Vec::new();
    for _ in 0..r.varint()? {
        let name = r.str()?;
        let ty = ColumnType::from_code(r.u8()?).ok_or(Error::Corrupt("unknown column type"))?;
        let bits = r.u8()?;
        let flags = ColumnFlags {
            json_escapes: bits & 1 != 0,
            case_insensitive: bits & 2 != 0,
            index_equality: bits & 4 != 0,
            index_words: bits & 8 != 0,
        };
        columns.push(Column { name, ty, flags, text_format: r.str()? });
    }
    Schema::new(columns).map_err(|_| Error::Corrupt("invalid schema in manifest"))
}

impl Manifest {
    fn encode_with_version(&self, version: u32) -> Vec<u8> {
        let mut w = Writer::new();
        w.buf.extend_from_slice(MANIFEST_MAGIC);
        w.u32(version);
        w.u8(SPLITTER_VERSION);
        w.u8(u8::from(self.sealed));
        w.u64(self.heap_bytes);
        w.u32(self.heap_words);
        w.varint(self.frames.len() as u64);
        for frame in &self.frames {
            w.str(&frame.name);
            write_schema(&mut w, &frame.schema);
            w.u32(frame.next_segment);
            w.varint(frame.segments.len() as u64);
            for segment in &frame.segments {
                w.str(&segment.file);
                w.u64(segment.rows);
            }
        }
        let checksum = xxh3(&w.buf, 0);
        w.u64(checksum);
        w.buf
    }

    fn encode(&self) -> Vec<u8> {
        self.encode_with_version(FORMAT_VERSION)
    }

    fn decode(bytes: &[u8]) -> Result<Self> {
        let body_len = bytes.len().checked_sub(8).ok_or(Error::Corrupt("manifest is too short"))?;
        let (body, tail) = bytes.split_at(body_len);
        if body.get(..8) != Some(MANIFEST_MAGIC.as_slice()) {
            return Err(Error::Corrupt("manifest magic"));
        }
        if u64::from_le_bytes(tail.try_into().expect("8 bytes")) != xxh3(body, 0) {
            return Err(Error::Corrupt("manifest checksum"));
        }
        let mut r = Reader::new(&body[8..]);
        if r.u32()? > FORMAT_VERSION || r.u8()? != SPLITTER_VERSION {
            return Err(Error::UnsupportedVersion);
        }
        let sealed = r.u8()? != 0;
        let (heap_bytes, heap_words) = (r.u64()?, r.u32()?);
        let mut frames = Vec::new();
        for _ in 0..r.varint()? {
            let name = r.str()?;
            let schema = read_schema(&mut r)?;
            let next_segment = r.u32()?;
            let mut segments = Vec::new();
            for _ in 0..r.varint()? {
                segments.push(SegmentMeta { file: r.str()?, rows: r.u64()? });
            }
            frames.push(FrameMeta { name, schema, segments, next_segment });
        }
        if !r.is_empty() {
            return Err(Error::Corrupt("manifest has trailing bytes"));
        }
        Ok(Self { sealed, heap_bytes, heap_words, frames })
    }
}

fn sync_dir(dir: &Path) {
    #[cfg(unix)]
    if let Ok(d) = File::open(dir) {
        let _ = d.sync_all();
    }
    #[cfg(not(unix))]
    let _ = dir;
}

/// Replaces the manifest atomically: write a temporary file, sync it, rename it over the old one.
fn write_manifest(dir: &Path, manifest: &Manifest) -> Result<()> {
    let tmp = dir.join("manifest.tmp");
    let mut file = File::create(&tmp)?;
    file.write_all(&manifest.encode())?;
    file.sync_all()?;
    drop(file);
    std::fs::rename(&tmp, dir.join("manifest"))?;
    sync_dir(dir);
    Ok(())
}

fn read_manifest(dir: &Path) -> Result<Manifest> {
    Manifest::decode(&std::fs::read(dir.join("manifest"))?)
}

// ---------------------------------------------------------------------------------------------------------------------
// heap.bin

/// Reads the committed part of `heap.bin` into an in-memory heap.
fn read_heap(path: &Path, committed_bytes: u64, committed_words: u32) -> Result<MemHeap> {
    let mut bytes = Vec::new();
    File::open(path)?.take(committed_bytes).read_to_end(&mut bytes)?;
    if bytes.len() as u64 != committed_bytes || bytes.get(..8) != Some(HEAP_MAGIC.as_slice()) {
        return Err(Error::Corrupt("heap file header or length"));
    }
    let mut heap = MemHeap::new();
    let mut r = Reader::new(&bytes[8..]);
    while !r.is_empty() {
        let (comp_len, raw_len, words, checksum) = (r.u32()?, r.u32()?, r.u32()?, r.u64()?);
        let comp = r.take(comp_len as usize)?;
        if xxh3(comp, 0) != checksum {
            return Err(Error::Corrupt("heap block checksum"));
        }
        let raw = zstd::bulk::decompress(comp, raw_len as usize).map_err(|_| Error::Corrupt("heap block does not decompress"))?;
        let mut pos = 0;
        for _ in 0..words {
            let len = varint::get(&raw, &mut pos)? as usize;
            let word = raw.get(pos..pos.checked_add(len).ok_or(Error::Corrupt("word length"))?).ok_or(Error::Corrupt("heap block"))?;
            heap.push(std::str::from_utf8(word).map_err(|_| Error::Corrupt("word is not UTF-8"))?)?;
            pos += len;
        }
        if pos != raw.len() {
            return Err(Error::Corrupt("heap block has trailing bytes"));
        }
    }
    if heap.len() != committed_words {
        return Err(Error::Corrupt("heap word count"));
    }
    Ok(heap)
}

/// Writes the words `from..to` as blocks after byte `at` of `heap.bin` (dropping anything after `at`); returns the new length.
fn append_heap(path: &Path, at: u64, words: &WordStore, from: u32, to: u32, block_bytes: usize, level: i32) -> Result<u64> {
    let mut file = OpenOptions::new().write(true).open(path)?;
    file.set_len(at)?;
    file.seek(SeekFrom::Start(at))?;
    let mut length = at;
    let mut ordinal = from;
    while ordinal < to {
        let mut raw = Writer::new();
        let mut count = 0u32;
        while ordinal < to && (count == 0 || raw.buf.len() < block_bytes) {
            raw.str(words.get(ordinal).ok_or(Error::Corrupt("missing word"))?);
            ordinal += 1;
            count += 1;
        }
        let comp = zstd::bulk::compress(&raw.buf, level)?;
        let mut record = Writer::new();
        record.u32(comp.len() as u32);
        record.u32(raw.buf.len() as u32);
        record.u32(count);
        record.u64(xxh3(&comp, 0));
        debug_assert_eq!(record.buf.len(), HEAP_RECORD_HEADER);
        file.write_all(&record.buf)?;
        file.write_all(&comp)?;
        length += (HEAP_RECORD_HEADER + comp.len()) as u64;
    }
    file.sync_all()?;
    Ok(length)
}

// ---------------------------------------------------------------------------------------------------------------------
// Frames

struct Segment {
    file: String,
    rows: u64,
    start: u64,
    reader: SegmentReader,
    index: Option<SegmentIndex>,
}

struct FrameState {
    active: DataFrame<MemHeap>,
    segments: Vec<Segment>,
    committed_rows: u64,
    next_segment: u32,
    /// Index of the buffered rows, valid while the buffer has this many rows.
    active_index: Mutex<Option<(u64, Arc<SegmentIndex>)>>,
}

/// One dataframe of a store: committed segments on disk plus the rows buffered since the last commit.
pub struct Frame {
    id: usize,
    name: String,
    schema: Schema,
    words: SharedWords,
    writable: Arc<AtomicBool>,
    /// The folder-wide write mutex of the store (see [`Store`]).
    write_mutex: Arc<Mutex<()>>,
    state: RwLock<FrameState>,
}

impl Frame {
    /// The name given when the dataframe was created.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The schema.
    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    /// Number of rows, committed and buffered.
    pub fn len(&self) -> u64 {
        let state = self.state.read().expect("frame lock poisoned");
        state.committed_rows + state.active.len()
    }

    /// Whether there are no rows.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Rows buffered in memory that the next commit will write.
    pub fn pending_rows(&self) -> u64 {
        self.state.read().expect("frame lock poisoned").active.len()
    }

    /// Appends a row and returns its row number. The row is only on disk after [`Store::commit`].
    pub fn push_row(&self, row: &[Value<'_>]) -> Result<u64> {
        if !self.writable.load(Ordering::Acquire) {
            return Err(Error::ReadOnly);
        }
        let _write = self.write_mutex.lock().expect("write mutex poisoned");
        let state = self.state.read().expect("frame lock poisoned");
        Ok(state.committed_rows + state.active.push_row(row)?)
    }

    /// Reads one row.
    pub fn row(&self, row: u64) -> Result<Vec<OwnedValue>> {
        let state = self.state.read().expect("frame lock poisoned");
        match locate(&state, row) {
            Some((segment, local)) => {
                let words = self.words.read().expect("word store lock poisoned");
                state.segments[segment].reader.row(&self.schema, &words, local)
            }
            None => state.active.row(row - state.committed_rows),
        }
    }

    /// Reads one value.
    pub fn value(&self, row: u64, column: usize) -> Result<OwnedValue> {
        let state = self.state.read().expect("frame lock poisoned");
        match locate(&state, row) {
            Some((segment, local)) => {
                let words = self.words.read().expect("word store lock poisoned");
                state.segments[segment].reader.value(&self.schema, &words, local, column)
            }
            None => state.active.value(row - state.committed_rows, column),
        }
    }

    /// Writes the text of a string cell into `out`; `false` for null.
    pub fn write_string(&self, row: u64, column: usize, out: &mut BytesMut) -> Result<bool> {
        let state = self.state.read().expect("frame lock poisoned");
        match locate(&state, row) {
            Some((segment, local)) => {
                if self.schema.columns().get(column).ok_or(Error::ColumnOutOfRange)?.ty != ColumnType::String {
                    return Err(Error::TypeMismatch { column: self.schema.columns()[column].name.clone() });
                }
                let words = self.words.read().expect("word store lock poisoned");
                state.segments[segment].reader.write_string(&words, local, column, out)
            }
            None => state.active.write_string(row - state.committed_rows, column, out),
        }
    }

    /// The row numbers that match `query`, in insertion order, evaluated one segment at a time. Rows buffered since the
    /// last commit are included. A commit while the iterator is being consumed can make it skip or repeat rows.
    pub fn search(&self, query: &Query) -> Result<RowIds<'_>> {
        let prepared = query::prepare(query, &self.schema, &self.words.read().expect("word store lock poisoned"))?;
        Ok(RowIds { frame: self, prepared, unit: 0, pending: Vec::new().into_iter(), done: false })
    }

    /// The number of rows that match `query`.
    pub fn count(&self, query: &Query) -> Result<u64> {
        let mut n = 0;
        for row in self.search(query)? {
            row?;
            n += 1;
        }
        Ok(n)
    }

    /// One page of the rows that match `query`, optionally sorted by a fixed-size column (nulls last).
    pub fn search_page(&self, query: &Query, page: &Page) -> Result<PageResult> {
        let mut rows: Vec<u64> = self.search(query)?.collect::<Result<_>>()?;
        let total = rows.len() as u64;
        if let Some(order) = page.order_by {
            let column = self.schema.columns().get(order.column).ok_or(Error::ColumnOutOfRange)?;
            if column.ty.is_variable() {
                return Err(Error::TypeMismatch { column: column.name.clone() });
            }
            let mut keyed: Vec<(Option<u64>, u64)> = Vec::with_capacity(rows.len());
            for &row in &rows {
                keyed.push((self.sort_key(row, order.column)?, row));
            }
            keyed.sort_by(|a, b| match (a.0, b.0) {
                (Some(x), Some(y)) => (if order.descending { y.cmp(&x) } else { x.cmp(&y) }).then(a.1.cmp(&b.1)),
                (Some(_), None) => std::cmp::Ordering::Less,
                (None, Some(_)) => std::cmp::Ordering::Greater,
                (None, None) => a.1.cmp(&b.1),
            });
            rows = keyed.into_iter().map(|(_, row)| row).collect();
        }
        Ok(PageResult { total, rows: rows.into_iter().skip(page.offset).take(page.limit).collect() })
    }

    fn sort_key(&self, row: u64, column: usize) -> Result<Option<u64>> {
        let state = self.state.read().expect("frame lock poisoned");
        let (cell, null) = match locate(&state, row) {
            Some((segment, local)) => state.segments[segment].reader.cell(local, column)?,
            None => state.active.source_cell(row.checked_sub(state.committed_rows).ok_or(Error::RowOutOfRange)?, column)?,
        };
        Ok((!null).then_some(cell))
    }

    /// The matches of `prepared` in one unit: segments in order, then the buffer. `None` after the last unit.
    fn search_unit(&self, prepared: &[Prepared], unit: usize) -> Result<Option<Vec<u64>>> {
        let state = self.state.read().expect("frame lock poisoned");
        if let Some(segment) = state.segments.get(unit) {
            let words = self.words.read().expect("word store lock poisoned");
            let rows = query::evaluate(&segment.reader, segment.index.as_ref(), &words, prepared)?;
            return Ok(Some(rows.into_iter().map(|r| segment.start + u64::from(r)).collect()));
        }
        if unit != state.segments.len() {
            return Ok(None);
        }
        // the buffer: its lock first, then the words (the order of push_row; see query::BufferSource)
        let base = state.committed_rows;
        let rows = state.active.with_inner(|inner| -> Result<Vec<u32>> {
            if inner.rows == 0 {
                return Ok(Vec::new());
            }
            let words = self.words.read().expect("word store lock poisoned");
            let source = BufferSource(inner);
            let index = self.active_index(&state, &source, &words)?;
            query::evaluate(&source, index.as_deref(), &words, prepared)
        })?;
        Ok(Some(rows.into_iter().map(|r| base + u64::from(r)).collect()))
    }

    /// The index of the buffered rows, rebuilt when the buffer has grown since it was built. The caller holds the buffer
    /// lock (`source`) and then the words.
    fn active_index(&self, state: &FrameState, source: &BufferSource<'_>, words: &WordStore) -> Result<Option<Arc<SegmentIndex>>> {
        let flags = |c: &Column| c.flags.index_words || c.flags.case_insensitive || c.flags.index_equality;
        if !self.schema.columns().iter().any(flags) {
            return Ok(None);
        }
        let rows = source.row_count();
        let mut cache = state.active_index.lock().expect("index cache lock poisoned");
        if let Some((cached_rows, index)) = &*cache {
            if *cached_rows == rows {
                return Ok(Some(Arc::clone(index)));
            }
        }
        let bytes = build_index(source, &self.schema, words)?;
        let index = Arc::new(SegmentIndex::from_vec(bytes)?);
        *cache = Some((rows, Arc::clone(&index)));
        Ok(Some(index))
    }

    /// An iterator over all rows in insertion order.
    pub fn rows(&self) -> FrameRows<'_> {
        FrameRows { frame: self, next: 0 }
    }
}

/// Lazy iterator over the row numbers that match a query; see [`Frame::search`].
pub struct RowIds<'a> {
    frame: &'a Frame,
    prepared: Vec<Prepared>,
    unit: usize,
    pending: std::vec::IntoIter<u64>,
    done: bool,
}

impl Iterator for RowIds<'_> {
    type Item = Result<u64>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(row) = self.pending.next() {
                return Some(Ok(row));
            }
            if self.done {
                return None;
            }
            match self.frame.search_unit(&self.prepared, self.unit) {
                Ok(Some(rows)) => {
                    self.unit += 1;
                    self.pending = rows.into_iter();
                }
                Ok(None) => self.done = true,
                Err(e) => {
                    self.done = true;
                    return Some(Err(e));
                }
            }
        }
    }
}

impl RowSource for SegmentReader {
    fn row_count(&self) -> u64 {
        self.rows()
    }

    fn cell(&self, row: u64, column: usize) -> Result<(u64, bool)> {
        SegmentReader::cell(self, row, column)
    }

    fn with_entry(&self, cell: u64, f: &mut dyn FnMut(&[u8]) -> Result<()>) -> Result<()> {
        SegmentReader::with_entry(self, cell, f)
    }

    fn range(&self, column: usize, lo: u64, hi: u64) -> Result<Vec<u32>> {
        SegmentReader::range(self, column, lo, hi)
    }
}

/// The committed segment holding `row` and the row's number in it; `None` if `row` is in the buffer or past the end.
fn locate(state: &FrameState, row: u64) -> Option<(usize, u64)> {
    if row >= state.committed_rows {
        return None;
    }
    let i = state.segments.partition_point(|s| s.start + s.rows <= row);
    Some((i, row - state.segments[i].start))
}

/// Iterator over the rows of a [`Frame`].
pub struct FrameRows<'a> {
    frame: &'a Frame,
    next: u64,
}

impl Iterator for FrameRows<'_> {
    type Item = Result<Vec<OwnedValue>>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.next >= self.frame.len() {
            return None;
        }
        self.next += 1;
        Some(self.frame.row(self.next - 1))
    }
}

// ---------------------------------------------------------------------------------------------------------------------
// Store

struct Committed {
    heap_bytes: u64,
    heap_words: u32,
    sealed: bool,
}

/// A folder with a shared word heap and dictionary and the dataframes that use them.
pub struct Store {
    dir: PathBuf,
    options: StoreOptions,
    words: SharedWords,
    writable: Arc<AtomicBool>,
    /// Serializes everything that writes: inserting rows, creating dataframes and committing. A folder has one writer at a time.
    write_mutex: Arc<Mutex<()>>,
    committed: Mutex<Committed>,
    frames: RwLock<Vec<Arc<Frame>>>,
    /// The sorted word table, present once the store has been sealed.
    word_table: RwLock<Option<WordTable>>,
    /// Held for as long as the store is open for writing.
    _lock: Option<File>,
}

fn acquire_writer_lock(dir: &Path) -> Result<File> {
    let file = OpenOptions::new().read(true).write(true).create(true).truncate(false).open(dir.join("lock"))?;
    match fs4::FileExt::try_lock(&file) {
        Ok(()) => Ok(file),
        Err(fs4::TryLockError::WouldBlock) => Err(Error::Locked),
        Err(fs4::TryLockError::Error(e)) => Err(e.into()),
    }
}

impl Store {
    /// Creates a new, empty store in `path` (the folder is created if needed and must not hold a store yet).
    pub fn create(path: impl AsRef<Path>, options: StoreOptions) -> Result<Self> {
        let dir = path.as_ref().to_path_buf();
        std::fs::create_dir_all(&dir)?;
        if dir.join("manifest").exists() {
            return Err(Error::Io(std::io::Error::new(std::io::ErrorKind::AlreadyExists, "a store exists in this folder")));
        }
        let lock = acquire_writer_lock(&dir)?;
        File::create(dir.join("heap.bin"))?.write_all(HEAP_MAGIC)?;
        let manifest = Manifest { sealed: false, heap_bytes: HEAP_MAGIC.len() as u64, heap_words: 0, frames: Vec::new() };
        write_manifest(&dir, &manifest)?;
        let words = WordStore::new(MemHeap::new(), Dictionary::new(options.dictionary_bytes)).into_shared();
        let committed = Committed { heap_bytes: manifest.heap_bytes, heap_words: 0, sealed: false };
        Ok(Self {
            dir,
            options,
            words,
            writable: Arc::new(AtomicBool::new(true)),
            write_mutex: Arc::new(Mutex::new(())),
            committed: Mutex::new(committed),
            frames: RwLock::new(Vec::new()),
            word_table: RwLock::new(None),
            _lock: Some(lock),
        })
    }

    /// Opens an existing store for reading and writing. Fails with [`Error::Locked`] if another process has it open for
    /// writing, and with [`Error::ReadOnly`] if it was sealed. Files left by an interrupted commit are removed.
    pub fn open(path: impl AsRef<Path>, options: StoreOptions) -> Result<Self> {
        Self::open_with(path.as_ref(), options, true)
    }

    /// Opens an existing store for reading only. It does not take the writer lock, so it works while another process
    /// is writing; it sees the data committed when it was opened.
    pub fn open_read_only(path: impl AsRef<Path>, options: StoreOptions) -> Result<Self> {
        Self::open_with(path.as_ref(), options, false)
    }

    fn open_with(dir: &Path, options: StoreOptions, writable: bool) -> Result<Self> {
        let lock = if writable { Some(acquire_writer_lock(dir)?) } else { None };
        let manifest = read_manifest(dir)?;
        if writable && manifest.sealed {
            return Err(Error::ReadOnly);
        }
        if writable {
            let known: Vec<String> = manifest
                .frames
                .iter()
                .flat_map(|f| f.segments.iter().flat_map(|s| [s.file.clone(), s.file.replace(".seg", ".idx")]))
                .collect();
            for entry in std::fs::read_dir(dir)?.flatten() {
                let name = entry.file_name().to_string_lossy().into_owned();
                let stray = (name.ends_with(".seg") || (name.ends_with(".idx") && name != "words.idx")) && !known.contains(&name);
                if name == "manifest.tmp" || name.ends_with(".idx.tmp") || stray {
                    std::fs::remove_file(entry.path())?;
                }
            }
        }
        let heap_path = dir.join("heap.bin");
        let heap = read_heap(&heap_path, manifest.heap_bytes, manifest.heap_words)?;
        if writable && std::fs::metadata(&heap_path)?.len() > manifest.heap_bytes {
            OpenOptions::new().write(true).open(&heap_path)?.set_len(manifest.heap_bytes)?;
        }
        let limit = if writable { options.dictionary_bytes } else { usize::MAX };
        let words = WordStore::from_heap(heap, Dictionary::new(limit))?.into_shared();
        let flag = Arc::new(AtomicBool::new(writable));
        let write_mutex = Arc::new(Mutex::new(()));

        let mut frames = Vec::new();
        for (id, meta) in manifest.frames.iter().enumerate() {
            let mut segments = Vec::new();
            let mut start = 0;
            for segment in &meta.segments {
                let reader = SegmentReader::open(&dir.join(&segment.file), meta.schema.len())?.with_cache_limit(options.cache_bytes);
                if reader.rows() != segment.rows {
                    return Err(Error::Corrupt("segment row count differs from the manifest"));
                }
                let index_path = dir.join(segment.file.replace(".seg", ".idx"));
                let index = if index_path.exists() { Some(SegmentIndex::open(&index_path)?) } else { None };
                segments.push(Segment { file: segment.file.clone(), rows: segment.rows, start, reader, index });
                start += segment.rows;
            }
            let state = FrameState {
                active: new_active(&meta.schema, &words, &options),
                segments,
                committed_rows: start,
                next_segment: meta.next_segment,
                active_index: Mutex::new(None),
            };
            frames.push(Arc::new(Frame {
                id,
                name: meta.name.clone(),
                schema: meta.schema.clone(),
                words: words.clone(),
                writable: flag.clone(),
                write_mutex: write_mutex.clone(),
                state: RwLock::new(state),
            }));
        }
        let committed = Committed { heap_bytes: manifest.heap_bytes, heap_words: manifest.heap_words, sealed: manifest.sealed };
        let table_path = dir.join("words.idx");
        let word_table = if manifest.sealed && table_path.exists() { Some(WordTable::open(&table_path)?) } else { None };
        Ok(Self {
            dir: dir.to_path_buf(),
            options,
            words,
            writable: flag,
            write_mutex: write_mutex.clone(),
            committed: Mutex::new(committed),
            frames: RwLock::new(frames),
            word_table: RwLock::new(word_table),
            _lock: lock,
        })
    }

    /// The word store shared by all dataframes.
    pub fn words(&self) -> &SharedWords {
        &self.words
    }

    /// The folder.
    pub fn path(&self) -> &Path {
        &self.dir
    }

    /// Whether rows can still be added.
    pub fn is_writable(&self) -> bool {
        self.writable.load(Ordering::Acquire)
    }

    /// Adds a dataframe. It is stored by the next commit.
    pub fn create_frame(&self, name: &str, schema: Schema) -> Result<Arc<Frame>> {
        if !self.is_writable() {
            return Err(Error::ReadOnly);
        }
        let _write = self.write_mutex.lock().expect("write mutex poisoned");
        let mut frames = self.frames.write().expect("frame list lock poisoned");
        if frames.iter().any(|f| f.name == name) {
            return Err(Error::FrameExists);
        }
        let state = FrameState {
            active: new_active(&schema, &self.words, &self.options),
            segments: Vec::new(),
            committed_rows: 0,
            next_segment: 0,
            active_index: Mutex::new(None),
        };
        let frame = Arc::new(Frame {
            id: frames.len(),
            name: name.to_owned(),
            schema,
            words: self.words.clone(),
            writable: self.writable.clone(),
            write_mutex: self.write_mutex.clone(),
            state: RwLock::new(state),
        });
        frames.push(frame.clone());
        Ok(frame)
    }

    /// The dataframe with the given name.
    pub fn frame(&self, name: &str) -> Result<Arc<Frame>> {
        let frames = self.frames.read().expect("frame list lock poisoned");
        frames.iter().find(|f| f.name == name).cloned().ok_or(Error::UnknownFrame)
    }

    /// Names of all dataframes.
    pub fn frame_names(&self) -> Vec<String> {
        self.frames.read().expect("frame list lock poisoned").iter().map(|f| f.name.clone()).collect()
    }

    /// Rows buffered in memory over all dataframes.
    pub fn pending_rows(&self) -> u64 {
        self.frames.read().expect("frame list lock poisoned").iter().map(|f| f.pending_rows()).sum()
    }

    /// Commits if any dataframe has buffered at least `segment_rows` rows. Returns whether it committed.
    pub fn commit_if_full(&self) -> Result<bool> {
        let full =
            self.frames.read().expect("frame list lock poisoned").iter().any(|f| f.pending_rows() >= self.options.segment_rows as u64);
        if full {
            self.commit()?;
        }
        Ok(full)
    }

    /// Writes everything buffered to disk: new words, one segment per dataframe with new rows, then the manifest.
    pub fn commit(&self) -> Result<()> {
        self.commit_inner(false)
    }

    /// Commits and closes the store for writing; it can only be opened read-only afterwards (log rotation).
    pub fn seal(&self) -> Result<()> {
        self.commit_inner(true)
    }

    fn commit_inner(&self, seal: bool) -> Result<()> {
        if !self.is_writable() {
            return Err(Error::ReadOnly);
        }
        let _write = self.write_mutex.lock().expect("write mutex poisoned");
        let mut committed = self.committed.lock().expect("commit lock poisoned");
        let frames = self.frames.read().expect("frame list lock poisoned");
        let mut guards: Vec<_> = frames.iter().map(|f| f.state.write().expect("frame lock poisoned")).collect();

        let (heap_bytes, heap_words) = {
            let words = self.words.read().expect("word store lock poisoned");
            let count = words.word_count();
            let o = &self.options;
            let bytes = append_heap(
                &self.dir.join("heap.bin"),
                committed.heap_bytes,
                &words,
                committed.heap_words,
                count,
                o.heap_block_bytes,
                o.zstd_level,
            )?;
            (bytes, count)
        };

        let write_options = WriteOptions { block_rows: self.options.block_rows, zstd_level: self.options.zstd_level };
        let mut written: Vec<(usize, String, u64)> = Vec::new();
        let mut failure = None;
        for (i, (frame, guard)) in frames.iter().zip(&guards).enumerate() {
            if guard.active.is_empty() {
                continue;
            }
            let file = format!("f{}-{:06}.seg", frame.id, guard.next_segment);
            let result = guard.active.with_inner(|inner| {
                write_segment(&self.dir.join(&file), inner, write_options)?;
                match self.options.indexing {
                    // the buffer lock is held: the words after it (see query::BufferSource)
                    true => {
                        let words = self.words.read().expect("word store lock poisoned");
                        write_index(&self.dir, &self.options, &frame.schema, &BufferSource(inner), &words, &file)
                    }
                    false => Ok(()),
                }
            });
            match result {
                Ok(()) => written.push((i, file, guard.active.len())),
                Err(e) => {
                    let _ = std::fs::remove_file(self.dir.join(&file));
                    let _ = std::fs::remove_file(self.dir.join(file.replace(".seg", ".idx")));
                    failure = Some(e);
                    break;
                }
            }
        }

        let manifest = Manifest {
            sealed: seal || committed.sealed,
            heap_bytes,
            heap_words,
            frames: frames
                .iter()
                .zip(&guards)
                .enumerate()
                .map(|(i, (frame, guard))| {
                    let mut segments: Vec<SegmentMeta> =
                        guard.segments.iter().map(|s| SegmentMeta { file: s.file.clone(), rows: s.rows }).collect();
                    let new = written.iter().find(|(index, _, _)| *index == i);
                    segments.extend(new.map(|(_, file, rows)| SegmentMeta { file: file.clone(), rows: *rows }));
                    FrameMeta {
                        name: frame.name.clone(),
                        schema: frame.schema.clone(),
                        segments,
                        next_segment: guard.next_segment + u32::from(new.is_some()),
                    }
                })
                .collect(),
        };
        let outcome = match failure {
            Some(e) => Err(e),
            None => write_manifest(&self.dir, &manifest),
        };
        if let Err(e) = outcome {
            for (_, file, _) in &written {
                let _ = std::fs::remove_file(self.dir.join(file));
                let _ = std::fs::remove_file(self.dir.join(file.replace(".seg", ".idx")));
            }
            return Err(e);
        }

        // The manifest is replaced: the commit happened. Now switch the in-memory state over.
        for (i, file, rows) in written {
            let guard = &mut guards[i];
            let reader = SegmentReader::open(&self.dir.join(&file), frames[i].schema.len())?.with_cache_limit(self.options.cache_bytes);
            let start = guard.committed_rows;
            let index_path = self.dir.join(file.replace(".seg", ".idx"));
            let index = if index_path.exists() { Some(SegmentIndex::open(&index_path)?) } else { None };
            guard.segments.push(Segment { file, rows, start, reader, index });
            guard.committed_rows += rows;
            guard.next_segment += 1;
            guard.active.clear();
            *guard.active_index.lock().expect("index cache lock poisoned") = None;
        }
        *committed = Committed { heap_bytes, heap_words, sealed: manifest.sealed };
        if seal {
            let path = self.dir.join("words.idx");
            let _ = std::fs::remove_file(&path);
            WordTable::write(&path, &self.words.read().expect("word store lock poisoned"))?;
            *self.word_table.write().expect("word table lock poisoned") = Some(WordTable::open(&path)?);
            self.writable.store(false, Ordering::Release);
        }
        Ok(())
    }

    /// Builds the search indexes that are missing in the store at `path`, from the stored segments and the heap: a segment
    /// without an index file gets one (for columns flagged in its schema), and a sealed store without `words.idx` gets
    /// one. Use it after committing with [`StoreOptions::indexing`] off, or on a store written before indexes existed. It
    /// takes the writer lock, so the store must not be open for writing elsewhere; it can be run on a sealed store.
    /// Stores that are already open do not notice new index files until they are reopened.
    pub fn build_indexes(path: impl AsRef<Path>, options: StoreOptions) -> Result<IndexReport> {
        let dir = path.as_ref();
        let _lock = acquire_writer_lock(dir)?;
        let manifest = read_manifest(dir)?;
        let heap = read_heap(&dir.join("heap.bin"), manifest.heap_bytes, manifest.heap_words)?;
        let words = WordStore::from_heap(heap, Dictionary::new(usize::MAX))?;
        let mut report = IndexReport { segments_indexed: 0, word_table_written: false };
        for frame in &manifest.frames {
            let flags = |c: &Column| c.flags.index_words || c.flags.case_insensitive || c.flags.index_equality;
            if !frame.schema.columns().iter().any(flags) {
                continue;
            }
            for segment in &frame.segments {
                if dir.join(segment.file.replace(".seg", ".idx")).exists() {
                    continue;
                }
                let reader = SegmentReader::open(&dir.join(&segment.file), frame.schema.len())?;
                write_index(dir, &options, &frame.schema, &reader, &words, &segment.file)?;
                report.segments_indexed += 1;
            }
        }
        let table = dir.join("words.idx");
        if manifest.sealed && !table.exists() {
            let tmp = dir.join("words.idx.tmp");
            let _ = std::fs::remove_file(&tmp);
            WordTable::write(&tmp, &words)?;
            std::fs::rename(&tmp, &table)?;
            report.word_table_written = true;
        }
        sync_dir(dir);
        Ok(report)
    }

    /// The ordinal of a word in the shared heap.
    pub fn lookup_word(&self, word: &str) -> Option<u32> {
        let words = self.words.read().expect("word store lock poisoned");
        match &*self.word_table.read().expect("word table lock poisoned") {
            Some(table) => table.lookup(&words, word),
            None => words.lookup(word),
        }
    }

    /// Up to `limit` words that start with `prefix`, in byte order (for suggestions). A sealed store answers from its
    /// memory-mapped word table; otherwise the heap is scanned.
    pub fn words_with_prefix(&self, prefix: &str, limit: usize) -> Vec<String> {
        let words = self.words.read().expect("word store lock poisoned");
        if let Some(table) = &*self.word_table.read().expect("word table lock poisoned") {
            return table.prefix(&words, prefix, limit);
        }
        let mut found: Vec<String> =
            (0..words.word_count()).filter_map(|o| words.get(o)).filter(|w| w.starts_with(prefix)).map(str::to_owned).collect();
        found.sort();
        found.truncate(limit);
        found
    }

    /// Checks every stored checksum (all blocks of the heap, the segment indexes and the word table). Reads every file.
    pub fn verify(&self) -> Result<()> {
        let manifest = read_manifest(&self.dir)?;
        read_heap(&self.dir.join("heap.bin"), manifest.heap_bytes, manifest.heap_words)?;
        for frame in self.frames.read().expect("frame list lock poisoned").iter() {
            let state = frame.state.read().expect("frame lock poisoned");
            for segment in &state.segments {
                segment.reader.verify()?;
                if let Some(index) = &segment.index {
                    index.verify()?;
                }
            }
        }
        if let Some(table) = &*self.word_table.read().expect("word table lock poisoned") {
            table.verify()?;
        }
        Ok(())
    }

    /// Size in bytes of the files of the store (manifest, heap and segments).
    pub fn disk_bytes(&self) -> Result<u64> {
        let mut total = 0;
        for entry in std::fs::read_dir(&self.dir)? {
            let entry = entry?;
            let name = entry.file_name().to_string_lossy().into_owned();
            if name == "manifest" || name == "heap.bin" || name.ends_with(".seg") || name.ends_with(".idx") {
                total += entry.metadata()?.len();
            }
        }
        Ok(total)
    }
}

/// Builds the index of a segment from `src` and writes it next to the segment file `segment_file` (`.seg` -> `.idx`).
fn write_index(
    dir: &Path,
    options: &StoreOptions,
    schema: &Schema,
    src: &dyn RowSource,
    words: &WordStore,
    segment_file: &str,
) -> Result<()> {
    let mut bytes = build_index(src, schema, words)?;
    if options.compress_indexes {
        bytes = compress_index(&bytes, options.zstd_level)?;
    }
    write_file_atomically(&dir.join(segment_file.replace(".seg", ".idx")), &bytes)
}

/// What [`Store::build_indexes`] did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IndexReport {
    /// Segments that got a new index file.
    pub segments_indexed: usize,
    /// Whether `words.idx` was written.
    pub word_table_written: bool,
}

fn new_active(schema: &Schema, words: &SharedWords, options: &StoreOptions) -> DataFrame<MemHeap> {
    let df_options = DataFrameOptions {
        tokenizer: options.tokenizer.clone(),
        dedup_entries: options.dedup_entries,
        row_heap_block_bytes: options.row_heap_block_bytes,
    };
    DataFrame::with_options(schema.clone(), words.clone(), df_options)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dataframe::decode_value;

    fn options() -> StoreOptions {
        StoreOptions { zstd_level: 3, block_rows: 16, row_heap_block_bytes: 256, heap_block_bytes: 512, ..StoreOptions::default() }
    }

    fn schema() -> Schema {
        Schema::new(vec![
            Column::new("id", ColumnType::U64),
            Column::new("msg", ColumnType::String),
            Column::new("data", ColumnType::String).json_escapes(),
            Column::new("blob", ColumnType::Blob),
            Column::new("score", ColumnType::F64),
        ])
        .unwrap()
    }

    fn push(frame: &Frame, id: u64) -> u64 {
        let msg = format!("request {} from host-{} finished with status {}", id % 13, id % 5, 200 + id % 3);
        let data = format!(r#"{{"id":{id},"text":"line one\nline {}","tags":["a","b"]}}"#, id % 9);
        let score = if id % 4 == 0 { Value::Null } else { Value::F64(id as f64 * 0.5) };
        frame.push_row(&[Value::U64(id), Value::Str(&msg), Value::Str(&data), Value::Blob(&[id as u8; 4]), score]).unwrap()
    }

    fn all_rows(frame: &Frame) -> Vec<Vec<OwnedValue>> {
        frame.rows().map(|r| r.unwrap()).collect()
    }

    /// A search over the buffered rows while another thread pushes rows with new words (and a third commits) must not
    /// deadlock: the buffer lock comes before the word store on every path (query::BufferSource).
    #[test]
    fn concurrent_push_search_and_commit_do_not_deadlock() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(Store::create(dir.path(), StoreOptions { segment_rows: 1 << 30, ..options() }).unwrap());
        let schema = Schema::new(vec![
            Column::new("run", ColumnType::U64).index_equality(),
            Column::new("id", ColumnType::U64),
            Column::new("text", ColumnType::String).index_words().case_insensitive(),
        ])
        .unwrap();
        store.create_frame("events", schema).unwrap();
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let (done_tx, done_rx) = std::sync::mpsc::channel::<&'static str>();
        let mut threads = Vec::new();
        {
            let (store, tx) = (store.clone(), done_tx.clone());
            threads.push(std::thread::spawn(move || {
                let frame = store.frame("events").unwrap();
                for id in 0..3000u64 {
                    // every row brings new words, so each push takes the word store for writing
                    let text = format!("tool output w{id} x{} y{}", id * 7, id * 13);
                    frame.push_row(&[Value::U64(id % 3), Value::U64(id), Value::Str(&text)]).unwrap();
                }
                tx.send("writer").unwrap();
            }));
        }
        for n in 0..3u64 {
            let (store, stop) = (store.clone(), stop.clone());
            threads.push(std::thread::spawn(move || {
                let frame = store.frame("events").unwrap();
                while !stop.load(Ordering::Relaxed) {
                    let q = Query::new().equals(0, Value::U64(n)).range(1, Some(Value::U64(0)), None);
                    for row in frame.search(&q).unwrap() {
                        frame.value(row.unwrap(), 2).unwrap();
                    }
                }
            }));
        }
        {
            let (store, stop) = (store.clone(), stop.clone());
            threads.push(std::thread::spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    std::thread::sleep(std::time::Duration::from_millis(3));
                    store.commit().unwrap();
                }
            }));
        }
        let r = done_rx.recv_timeout(std::time::Duration::from_secs(60));
        stop.store(true, Ordering::Relaxed);
        assert_eq!(r, Ok("writer"), "push, search and commit deadlocked");
        for t in threads {
            t.join().unwrap();
        }
        store.commit().unwrap();
        assert_eq!(store.frame("events").unwrap().len(), 3000);
    }

    #[test]
    fn commit_and_reopen_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let before;
        {
            let store = Store::create(dir.path(), options()).unwrap();
            let frame = store.create_frame("logs", schema()).unwrap();
            for id in 0..100 {
                assert_eq!(push(&frame, id), id);
            }
            store.commit().unwrap();
            for id in 100..150 {
                assert_eq!(push(&frame, id), id);
            }
            store.commit().unwrap();
            assert_eq!((frame.len(), frame.pending_rows()), (150, 0));
            before = all_rows(&frame);
        }
        let store = Store::open(dir.path(), options()).unwrap();
        let frame = store.frame("logs").unwrap();
        assert_eq!(frame.len(), 150);
        assert_eq!(all_rows(&frame), before);
        assert_eq!(frame.row(120).unwrap()[0], OwnedValue::U64(120));
        let mut out = BytesMut::new();
        assert!(frame.write_string(7, 1, &mut out).unwrap());
        assert_eq!(&out[..], b"request 7 from host-2 finished with status 201");
        assert_eq!(store.frame_names(), ["logs"]);
        assert!(matches!(store.frame("nope"), Err(Error::UnknownFrame)));
        assert!(matches!(frame.row(150), Err(Error::RowOutOfRange)));
    }

    #[test]
    fn uncommitted_rows_are_readable_but_lost_on_close() {
        let dir = tempfile::tempdir().unwrap();
        {
            let store = Store::create(dir.path(), options()).unwrap();
            let frame = store.create_frame("logs", schema()).unwrap();
            for id in 0..10 {
                push(&frame, id);
            }
            store.commit().unwrap();
            for id in 10..15 {
                push(&frame, id);
            }
            assert_eq!(frame.len(), 15);
            assert_eq!(frame.row(12).unwrap()[0], OwnedValue::U64(12)); // from the buffer
            assert_eq!(frame.row(3).unwrap()[0], OwnedValue::U64(3)); // from a segment
        }
        let store = Store::open(dir.path(), options()).unwrap();
        assert_eq!(store.frame("logs").unwrap().len(), 10);
    }

    #[test]
    fn the_dictionary_survives_a_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let count;
        {
            let store = Store::create(dir.path(), options()).unwrap();
            let frame = store.create_frame("logs", schema()).unwrap();
            for id in 0..50 {
                push(&frame, id);
            }
            store.commit().unwrap();
            count = store.words().read().unwrap().word_count();
        }
        let store = Store::open(dir.path(), options()).unwrap();
        let frame = store.frame("logs").unwrap();
        assert_eq!(store.words().read().unwrap().word_count(), count);
        for id in 0..50 {
            push(&frame, id); // the same words again
        }
        assert_eq!(store.words().read().unwrap().word_count(), count);
        store.commit().unwrap();
        drop(store);
        let store = Store::open(dir.path(), options()).unwrap();
        assert_eq!(store.frame("logs").unwrap().len(), 100);
        assert_eq!(store.frame("logs").unwrap().row(77).unwrap()[0], OwnedValue::U64(27));
    }

    #[test]
    fn two_dataframes_share_one_heap() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::create(dir.path(), options()).unwrap();
        let a = store.create_frame("a", schema()).unwrap();
        let b = store.create_frame("b", schema()).unwrap();
        assert!(matches!(store.create_frame("a", schema()), Err(Error::FrameExists)));
        push(&a, 1);
        let count = store.words().read().unwrap().word_count();
        push(&b, 1);
        assert_eq!(store.words().read().unwrap().word_count(), count);
        store.commit().unwrap();
        drop(store);
        let store = Store::open(dir.path(), options()).unwrap();
        assert_eq!(store.frame("b").unwrap().row(0).unwrap(), store.frame("a").unwrap().row(0).unwrap());
    }

    #[test]
    fn a_second_writer_is_refused_but_readers_see_committed_data() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::create(dir.path(), options()).unwrap();
        let frame = store.create_frame("logs", schema()).unwrap();
        for id in 0..20 {
            push(&frame, id);
        }
        store.commit().unwrap();
        for id in 20..30 {
            push(&frame, id); // not committed
        }
        assert!(matches!(Store::open(dir.path(), options()), Err(Error::Locked)));
        let reader = Store::open_read_only(dir.path(), options()).unwrap();
        let seen = reader.frame("logs").unwrap();
        assert_eq!(seen.len(), 20);
        assert_eq!(seen.row(19).unwrap()[0], OwnedValue::U64(19));
        assert!(matches!(seen.push_row(&vec![Value::Null; 5]), Err(Error::ReadOnly)));
        assert!(matches!(reader.commit(), Err(Error::ReadOnly)));
        assert!(matches!(reader.create_frame("x", schema()), Err(Error::ReadOnly)));
        store.commit().unwrap();
        assert_eq!(Store::open_read_only(dir.path(), options()).unwrap().frame("logs").unwrap().len(), 30);
    }

    #[test]
    fn sealing_ends_writing() {
        let dir = tempfile::tempdir().unwrap();
        {
            let store = Store::create(dir.path(), options()).unwrap();
            let frame = store.create_frame("logs", schema()).unwrap();
            for id in 0..5 {
                push(&frame, id);
            }
            store.seal().unwrap();
            assert!(!store.is_writable());
            assert!(matches!(frame.push_row(&vec![Value::Null; 5]), Err(Error::ReadOnly)));
            assert!(matches!(store.commit(), Err(Error::ReadOnly)));
        }
        assert!(matches!(Store::open(dir.path(), options()), Err(Error::ReadOnly)));
        assert_eq!(Store::open_read_only(dir.path(), options()).unwrap().frame("logs").unwrap().len(), 5);
    }

    #[test]
    fn leftovers_of_an_interrupted_commit_are_cleaned_up() {
        let dir = tempfile::tempdir().unwrap();
        {
            let store = Store::create(dir.path(), options()).unwrap();
            let frame = store.create_frame("logs", schema()).unwrap();
            for id in 0..40 {
                push(&frame, id);
            }
            store.commit().unwrap();
        }
        let committed = std::fs::metadata(dir.path().join("heap.bin")).unwrap().len();
        let mut heap = OpenOptions::new().append(true).open(dir.path().join("heap.bin")).unwrap();
        heap.write_all(b"half written words").unwrap();
        std::fs::write(dir.path().join("f0-000099.seg"), b"stray segment").unwrap();
        std::fs::write(dir.path().join("manifest.tmp"), b"stray manifest").unwrap();
        let store = Store::open(dir.path(), options()).unwrap();
        assert_eq!(store.frame("logs").unwrap().len(), 40);
        assert_eq!(std::fs::metadata(dir.path().join("heap.bin")).unwrap().len(), committed);
        assert!(!dir.path().join("f0-000099.seg").exists() && !dir.path().join("manifest.tmp").exists());
        push(&store.frame("logs").unwrap(), 40);
        store.commit().unwrap();
        assert_eq!(store.frame("logs").unwrap().len(), 41);
    }

    #[test]
    fn damaged_files_are_reported() {
        let dir = tempfile::tempdir().unwrap();
        {
            let store = Store::create(dir.path(), options()).unwrap();
            let frame = store.create_frame("logs", schema()).unwrap();
            for id in 0..40 {
                push(&frame, id);
            }
            store.commit().unwrap();
        }
        let flip = |name: &str, position: usize| {
            let path = dir.path().join(name);
            let mut bytes = std::fs::read(&path).unwrap();
            let at = position.min(bytes.len() - 1);
            bytes[at] ^= 0xff;
            std::fs::write(&path, bytes).unwrap();
        };
        let opens = || Store::open_read_only(dir.path(), options());
        assert!(opens().is_ok());
        flip("manifest", 12);
        assert!(matches!(opens(), Err(Error::Corrupt(_))));
        flip("manifest", 12);
        flip("heap.bin", 30);
        assert!(matches!(opens(), Err(Error::Corrupt(_))));
        flip("heap.bin", 30);
        assert!(opens().is_ok());
        std::fs::remove_file(dir.path().join("f0-000000.seg")).unwrap();
        assert!(matches!(opens(), Err(Error::Io(_))));
    }

    #[test]
    fn a_newer_format_version_is_rejected() {
        let dir = tempfile::tempdir().unwrap();
        drop(Store::create(dir.path(), options()).unwrap());
        let manifest = read_manifest(dir.path()).unwrap();
        std::fs::write(dir.path().join("manifest"), manifest.encode_with_version(FORMAT_VERSION + 1)).unwrap();
        assert!(matches!(Store::open_read_only(dir.path(), options()), Err(Error::UnsupportedVersion)));
        std::fs::write(dir.path().join("manifest"), manifest.encode()).unwrap();
        assert!(Store::open_read_only(dir.path(), options()).is_ok());
    }

    #[test]
    fn a_full_dictionary_loses_nothing_that_was_committed() {
        let dir = tempfile::tempdir().unwrap();
        let tiny = StoreOptions { dictionary_bytes: crate::dictionary::ENTRY_COST_BYTES * 60, ..options() };
        let store = Store::create(dir.path(), tiny.clone()).unwrap();
        let frame = store.create_frame("logs", schema()).unwrap();
        let mut stored = 0;
        for id in 0..1000 {
            let msg = format!("unique-word-{id} unique-token-{id}");
            match frame.push_row(&[Value::U64(id), Value::Str(&msg), Value::Null, Value::Null, Value::Null]) {
                Ok(_) => stored += 1,
                Err(Error::DictionaryFull) => break,
                Err(e) => panic!("{e}"),
            }
        }
        assert!(stored > 5 && stored < 1000);
        assert_eq!(frame.len(), stored);
        store.commit().unwrap();
        drop(store);
        let reopened = Store::open(dir.path(), tiny).unwrap();
        assert_eq!(reopened.frame("logs").unwrap().len(), stored);
        assert_eq!(all_rows(&reopened.frame("logs").unwrap()).len() as u64, stored);
    }

    #[test]
    fn commit_if_full_and_concurrent_writers() {
        let dir = tempfile::tempdir().unwrap();
        let small = StoreOptions { segment_rows: 50, ..options() };
        let store = Store::create(dir.path(), small.clone()).unwrap();
        let frame = store.create_frame("logs", schema()).unwrap();
        std::thread::scope(|scope| {
            for thread in 0..4u64 {
                let (frame, store) = (&frame, &store);
                scope.spawn(move || {
                    for i in 0..100u64 {
                        push(frame, thread * 1000 + i);
                        if i % 10 == 0 {
                            store.commit_if_full().unwrap();
                        }
                    }
                });
            }
        });
        store.commit().unwrap();
        assert_eq!(frame.len(), 400);
        drop(store);
        let store = Store::open(dir.path(), small).unwrap();
        let mut ids: Vec<u64> =
            all_rows(&store.frame("logs").unwrap()).iter().map(|r| if let OwnedValue::U64(id) = r[0] { id } else { panic!() }).collect();
        assert!(read_manifest(dir.path()).unwrap().frames[0].segments.len() > 1);
        ids.sort_unstable();
        ids.dedup();
        assert_eq!(ids.len(), 400);
    }

    // ---- search ----------------------------------------------------------------------------------------------------

    use crate::query::{OrderBy, Page};
    use crate::splitter::{DefaultSplitter, JsonEscapeSplitter, Splitter, split_words};

    fn search_schema(indexed: bool) -> Schema {
        let flag = |c: Column, on: bool, f: fn(Column) -> Column| if on { f(c) } else { c };
        Schema::new(vec![
            flag(Column::new("id", ColumnType::I64), indexed, Column::index_equality),
            flag(Column::new("level", ColumnType::String), indexed, Column::index_equality),
            flag(flag(Column::new("msg", ColumnType::String), indexed, Column::index_words), indexed, Column::case_insensitive),
            flag(Column::new("data", ColumnType::String).json_escapes(), indexed, Column::index_words),
            flag(Column::new("n", ColumnType::I64), indexed, Column::index_equality),
        ])
        .unwrap()
    }

    const LEVELS: [&str; 3] = ["INFO", "WARN", "ERROR"];
    const TAILS: [&str; 4] = ["ok", "failed: timeout", "retry later", "Done."];

    fn texts(i: u64) -> (String, String, String) {
        let level = LEVELS[(i % 3) as usize];
        let msg = format!("{level} request {} for user{} took {}ms {}", i % 13, i % 7, i % 50, TAILS[(i % 4) as usize]);
        let data = format!(r#"{{"k":{i},"text":"line a\nline b {}"}}"#, i % 5);
        (level.to_owned(), msg, data)
    }

    fn push_search_row(frame: &Frame, i: u64) {
        let (level, msg, data) = texts(i);
        let n = if i % 10 == 0 { Value::Null } else { Value::I64(((i * 7) % 100) as i64) };
        frame.push_row(&[Value::I64(i as i64), Value::Str(&level), Value::Str(&msg), Value::Str(&data), n]).unwrap();
    }

    fn has_word(text: &str, word: &str, ci: bool, splitter: &dyn Splitter) -> bool {
        let fold = |s: &str| caseless::default_case_fold_str(s);
        split_words(splitter, text).iter().any(|w| if ci { fold(w) == fold(word) } else { *w == word })
    }

    fn has_phrase(text: &str, phrase: &str, splitter: &dyn Splitter) -> bool {
        let (hay, needle) = (split_words(splitter, text), split_words(splitter, phrase));
        hay.windows(needle.len()).any(|w| w == needle.as_slice())
    }

    /// The queries to try and the ids each must return (computed from the generating rules, not from any index).
    fn cases(rows: u64) -> Vec<(String, Query, Vec<u64>)> {
        let truth = |pred: &dyn Fn(u64) -> bool| (0..rows).filter(|&i| pred(i)).collect::<Vec<_>>();
        let msg = |i: u64| texts(i).1;
        let data = |i: u64| texts(i).2;
        let n = |i: u64| if i % 10 == 0 { None } else { Some(((i * 7) % 100) as i64) };
        let d = &DefaultSplitter as &dyn Splitter;
        let j = &JsonEscapeSplitter as &dyn Splitter;
        vec![
            ("word request".into(), Query::new().word(2, "request"), truth(&|_| true)),
            ("word timeout".into(), Query::new().word(2, "timeout"), truth(&|i| i % 4 == 1)),
            ("word user3".into(), Query::new().word(2, "user3"), truth(&|i| i % 7 == 3)),
            ("word missing".into(), Query::new().word(2, "nonexistent"), vec![]),
            ("word ci ERROR".into(), Query::new().word_ci(2, "ERROR"), truth(&|i| has_word(&msg(i), "error", true, d))),
            ("word ci error".into(), Query::new().word_ci(2, "error"), truth(&|i| i % 3 == 2)),
            ("word cs error".into(), Query::new().word(2, "error"), vec![]),
            ("word ci missing".into(), Query::new().word_ci(2, "NONEXISTENT"), vec![]),
            ("single char colon".into(), Query::new().word(2, ":"), truth(&|i| has_word(&msg(i), ":", false, d))),
            ("single char dot".into(), Query::new().word(2, "."), truth(&|i| i % 4 == 3)),
            ("single char absent".into(), Query::new().word(2, "x"), vec![]),
            ("phrase failed: timeout".into(), Query::new().phrase(2, "failed: timeout"), truth(&|i| i % 4 == 1)),
            ("phrase request 3 for".into(), Query::new().phrase(2, "request 3 for"), truth(&|i| has_phrase(&msg(i), "request 3 for", d))),
            ("phrase wrong order".into(), Query::new().phrase(2, "for request"), vec![]),
            (
                "phrase json escape".into(),
                Query::new().phrase(3, "line a\\nline b 2"),
                truth(&|i| has_phrase(&data(i), "line a\\nline b 2", j)),
            ),
            ("json word".into(), Query::new().word(3, "text"), truth(&|_| true)),
            ("equals level".into(), Query::new().equals(1, Value::Str("WARN")), truth(&|i| i % 3 == 1)),
            ("equals level missing".into(), Query::new().equals(1, Value::Str("nope")), vec![]),
            ("equals id".into(), Query::new().equals(0, Value::I64(42)), truth(&|i| i == 42)),
            ("equals null".into(), Query::new().equals(4, Value::Null), truth(&|i| i % 10 == 0)),
            ("equals n".into(), Query::new().equals(4, Value::I64(49)), truth(&|i| n(i) == Some(49))),
            ("range id".into(), Query::new().range(0, Some(Value::I64(10)), Some(Value::I64(19))), truth(&|i| (10..=19).contains(&i))),
            ("range open".into(), Query::new().range(4, Some(Value::I64(90)), None), truth(&|i| n(i).is_some_and(|v| v >= 90))),
            ("range empty".into(), Query::new().range(0, Some(Value::I64(500)), None), vec![]),
            ("all rows".into(), Query::new(), truth(&|_| true)),
            (
                "combo".into(),
                Query::new().word(2, "timeout").equals(1, Value::Str("WARN")).range(0, Some(Value::I64(20)), Some(Value::I64(80))),
                truth(&|i| i % 4 == 1 && i % 3 == 1 && (20..=80).contains(&i)),
            ),
            (
                "combo words".into(),
                Query::new().word(2, "request").word_ci(2, "WARN").phrase(2, "took 3"),
                truth(&|i| has_word(&msg(i), "warn", true, d) && has_phrase(&msg(i), "took 3", d)),
            ),
        ]
    }

    fn check_search(frame: &Frame, label: &str) {
        let rows = frame.len();
        for (name, query, expected) in cases(rows) {
            let got: Vec<u64> = frame.search(&query).unwrap().map(|r| r.unwrap()).collect();
            assert_eq!(got, expected, "{label}: {name}");
            assert_eq!(frame.count(&query).unwrap(), expected.len() as u64, "{label}: {name} (count)");
        }
    }

    #[test]
    fn search_matches_the_ground_truth_in_every_state() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::create(dir.path(), options()).unwrap();
        let (indexed, plain) =
            (store.create_frame("indexed", search_schema(true)).unwrap(), store.create_frame("plain", search_schema(false)).unwrap());
        let push = |range: std::ops::Range<u64>| {
            for i in range {
                push_search_row(&indexed, i);
                push_search_row(&plain, i);
            }
        };
        push(0..60); // everything still in the buffer
        check_search(&indexed, "indexed, buffer");
        check_search(&plain, "plain, buffer");
        store.commit().unwrap();
        check_search(&indexed, "indexed, segment");
        check_search(&plain, "plain, segment");
        push(60..100); // a segment plus a buffer
        check_search(&indexed, "indexed, segment+buffer");
        check_search(&plain, "plain, segment+buffer");
        store.commit().unwrap();
        push(100..130);
        store.commit().unwrap();
        store.seal().unwrap();
        drop(store);
        let store = Store::open_read_only(dir.path(), options()).unwrap();
        check_search(&store.frame("indexed").unwrap(), "indexed, reopened, 3 segments");
        check_search(&store.frame("plain").unwrap(), "plain, reopened");
        store.verify().unwrap();
        assert!(dir.path().join("f0-000000.idx").exists() && dir.path().join("words.idx").exists());
    }

    #[test]
    fn the_buffer_index_is_refreshed_after_inserts_and_commits() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::create(dir.path(), options()).unwrap();
        let frame = store.create_frame("indexed", search_schema(true)).unwrap();
        let q = Query::new().word(2, "request");
        for i in 0..10 {
            push_search_row(&frame, i);
        }
        assert_eq!(frame.count(&q).unwrap(), 10);
        for i in 10..15 {
            push_search_row(&frame, i);
        }
        assert_eq!(frame.count(&q).unwrap(), 15);
        store.commit().unwrap();
        for i in 15..25 {
            push_search_row(&frame, i); // same buffer size as before the first commit's cached index
        }
        assert_eq!(frame.count(&q).unwrap(), 25);
        assert_eq!(frame.count(&Query::new().equals(0, Value::I64(24))).unwrap(), 1);
    }

    #[test]
    fn sorting_and_pagination() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::create(dir.path(), options()).unwrap();
        let frame = store.create_frame("indexed", search_schema(true)).unwrap();
        for i in 0..40 {
            push_search_row(&frame, i);
        }
        store.commit().unwrap();
        for i in 40..50 {
            push_search_row(&frame, i);
        }
        let all = Query::new();
        let by_n = |descending| Some(OrderBy { column: 4, descending });
        let page = |order_by, offset, limit| frame.search_page(&all, &Page { order_by, offset, limit }).unwrap();
        let plain = page(None, 5, 3);
        assert_eq!((plain.total, plain.rows), (50, vec![5, 6, 7]));
        let key = |row: u64| if row % 10 == 0 { None } else { Some(((row * 7) % 100) as i64) };
        let mut expected: Vec<u64> = (0..50).collect();
        expected.sort_by_key(|&r| (key(r).is_none(), key(r), r)); // nulls last, ties by row number
        assert_eq!(page(by_n(false), 0, 50).rows, expected);
        let mut descending: Vec<u64> = (0..50).filter(|&r| key(r).is_some()).collect();
        descending.sort_by_key(|&r| (std::cmp::Reverse(key(r)), r));
        let result = page(by_n(true), 0, 50).rows;
        assert_eq!(result[..descending.len()], descending[..]);
        assert!(result[descending.len()..].iter().all(|&r| r % 10 == 0));
        assert_eq!(page(by_n(false), 45, 10).rows, expected[45..]);
        let filtered = frame.search_page(&Query::new().word(2, "timeout"), &Page { order_by: by_n(true), offset: 0, limit: 2 }).unwrap();
        assert_eq!((filtered.total, filtered.rows.len()), (13, 2));
        let text_order = Page { order_by: Some(OrderBy { column: 1, descending: false }), offset: 0, limit: 1 };
        assert!(matches!(frame.search_page(&all, &text_order), Err(Error::TypeMismatch { .. })));
    }

    #[test]
    fn invalid_queries_are_errors() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::create(dir.path(), options()).unwrap();
        let frame = store.create_frame("f", search_schema(true)).unwrap();
        push_search_row(&frame, 1);
        assert!(matches!(frame.search(&Query::new().word(2, "")), Err(Error::InvalidQuery(_))));
        assert!(matches!(frame.search(&Query::new().phrase(2, "")), Err(Error::InvalidQuery(_))));
        assert!(matches!(frame.search(&Query::new().word(0, "x")), Err(Error::TypeMismatch { .. })));
        assert!(matches!(frame.search(&Query::new().word(9, "x")), Err(Error::ColumnOutOfRange)));
        assert!(matches!(frame.search(&Query::new().range(1, None, None)), Err(Error::TypeMismatch { .. })));
        assert!(matches!(frame.search(&Query::new().equals(0, Value::Str("x"))), Err(Error::TypeMismatch { .. })));
        assert!(matches!(
            frame.search(&Query::new().equals(0, Value::I64(1)).range(0, Some(Value::Str("a")), None)),
            Err(Error::TypeMismatch { .. })
        ));
    }

    #[test]
    fn inline_runs_are_not_words_but_are_found_by_equality() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::create(dir.path(), options()).unwrap();
        let schema = Schema::new(vec![
            Column::new("id", ColumnType::String).index_equality(),
            Column::new("text", ColumnType::String).index_words(),
        ])
        .unwrap();
        let frame = store.create_frame("f", schema).unwrap();
        let uuid = "28cb7d12-4175-485f-8b91-02be9c532039";
        let tool = "toolu_01UAgYH3kvbEkEaocV5KPN7N";
        frame.push_row(&[Value::Str(uuid), Value::Str(&format!("session {uuid} used {tool}"))]).unwrap();
        frame.push_row(&[Value::Str(tool), Value::Str("session none")]).unwrap();
        store.commit().unwrap();
        assert_eq!(frame.count(&Query::new().word(1, "session")).unwrap(), 2);
        assert_eq!(frame.count(&Query::new().word(1, "28cb7d12")).unwrap(), 0); // part of an inline UUID
        assert_eq!(frame.count(&Query::new().word(1, "01UAgYH3kvbEkEaocV5KPN7N")).unwrap(), 0); // inline opaque run
        assert_eq!(frame.count(&Query::new().equals(0, Value::Str(uuid))).unwrap(), 1);
        assert_eq!(frame.count(&Query::new().equals(0, Value::Str(tool))).unwrap(), 1);
    }

    #[test]
    fn word_table_gives_prefix_suggestions_before_and_after_sealing() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::create(dir.path(), options()).unwrap();
        let frame = store.create_frame("f", search_schema(true)).unwrap();
        for i in 0..30 {
            push_search_row(&frame, i);
        }
        let before = store.words_with_prefix("us", 100);
        assert_eq!(before, ["user0", "user1", "user2", "user3", "user4", "user5", "user6"]);
        assert!(store.lookup_word("timeout").is_some());
        store.seal().unwrap();
        assert_eq!(store.words_with_prefix("us", 3), ["user0", "user1", "user2"]);
        drop(store);
        let reopened = Store::open_read_only(dir.path(), options()).unwrap();
        assert_eq!(reopened.words_with_prefix("us", 100), before);
        assert!(reopened.words_with_prefix("zzz", 5).is_empty());
    }

    #[test]
    fn verify_detects_a_damaged_index() {
        for compress in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let opts = StoreOptions { compress_indexes: compress, ..options() };
            {
                let store = Store::create(dir.path(), opts.clone()).unwrap();
                let frame = store.create_frame("f", search_schema(true)).unwrap();
                for i in 0..30 {
                    push_search_row(&frame, i);
                }
                store.seal().unwrap();
            }
            let path = dir.path().join("f0-000000.idx");
            let good = std::fs::read(&path).unwrap();
            Store::open_read_only(dir.path(), opts.clone()).unwrap().verify().unwrap();
            let mut bad = good.clone();
            let at = bad.len() - 30;
            bad[at] ^= 0x01;
            std::fs::write(&path, &bad).unwrap();
            // a plain (memory-mapped) file is only checked by verify(); a compressed one is decoded when the store opens
            let damaged = Store::open_read_only(dir.path(), opts.clone()).and_then(|store| store.verify());
            assert!(damaged.is_err(), "compress = {compress}");
            std::fs::write(&path, &good).unwrap();
            Store::open_read_only(dir.path(), opts).unwrap().verify().unwrap();
        }
    }

    #[test]
    fn index_files_of_an_interrupted_commit_are_cleaned_up() {
        let dir = tempfile::tempdir().unwrap();
        {
            let store = Store::create(dir.path(), options()).unwrap();
            let frame = store.create_frame("f", search_schema(true)).unwrap();
            push_search_row(&frame, 1);
            store.commit().unwrap();
        }
        std::fs::write(dir.path().join("f0-000077.idx"), b"stray").unwrap();
        let store = Store::open(dir.path(), options()).unwrap();
        assert!(!dir.path().join("f0-000077.idx").exists() && dir.path().join("f0-000000.idx").exists());
        assert_eq!(store.frame("f").unwrap().count(&Query::new().word(2, "request")).unwrap(), 1);
    }

    #[test]
    fn indexing_can_be_off_and_built_later_from_the_stored_data() {
        let dir = tempfile::tempdir().unwrap();
        let off = StoreOptions { indexing: false, ..options() };
        {
            let store = Store::create(dir.path(), off.clone()).unwrap();
            let frame = store.create_frame("indexed", search_schema(true)).unwrap();
            for i in 0..70 {
                push_search_row(&frame, i);
            }
            store.commit().unwrap();
            for i in 70..100 {
                push_search_row(&frame, i);
            }
            store.seal().unwrap();
            check_search(&frame, "no index files yet"); // scans, still correct
        }
        let files =
            |ext: &str| std::fs::read_dir(dir.path()).unwrap().flatten().filter(|e| e.file_name().to_string_lossy().ends_with(ext)).count();
        assert_eq!((files(".seg"), files(".idx")), (2, 1)); // only words.idx, which sealing always writes
        std::fs::remove_file(dir.path().join("words.idx")).unwrap();

        let report = Store::build_indexes(dir.path(), options()).unwrap();
        assert_eq!(report, IndexReport { segments_indexed: 2, word_table_written: true });
        assert_eq!((files(".idx"), files(".tmp")), (3, 0));
        assert_eq!(Store::build_indexes(dir.path(), options()).unwrap(), IndexReport { segments_indexed: 0, word_table_written: false });

        let store = Store::open_read_only(dir.path(), options()).unwrap();
        check_search(&store.frame("indexed").unwrap(), "indexes built later");
        store.verify().unwrap();
        assert_eq!(store.words_with_prefix("us", 2), ["user0", "user1"]);
    }

    #[test]
    fn building_indexes_needs_the_writer_lock_and_leaves_no_partial_files() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::create(dir.path(), StoreOptions { indexing: false, ..options() }).unwrap();
        let frame = store.create_frame("indexed", search_schema(true)).unwrap();
        push_search_row(&frame, 1);
        store.commit().unwrap();
        assert!(matches!(Store::build_indexes(dir.path(), options()), Err(Error::Locked)));
        drop(store);
        std::fs::write(dir.path().join("f0-000000.idx.tmp"), b"half written").unwrap();
        Store::open(dir.path(), options()).unwrap(); // a writable open removes the leftover
        assert!(!dir.path().join("f0-000000.idx.tmp").exists());
        assert_eq!(Store::build_indexes(dir.path(), options()).unwrap().segments_indexed, 1);
    }

    #[test]
    fn writes_to_different_dataframes_are_serialized_with_commits() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::create(dir.path(), StoreOptions { segment_rows: 25, ..options() }).unwrap();
        let (a, b) = (store.create_frame("a", search_schema(true)).unwrap(), store.create_frame("b", search_schema(true)).unwrap());
        std::thread::scope(|scope| {
            for frame in [&a, &b] {
                for thread in 0..2u64 {
                    let store = &store;
                    scope.spawn(move || {
                        for i in 0..60u64 {
                            push_search_row(frame, thread * 100 + i);
                            if i % 5 == 0 {
                                store.commit_if_full().unwrap();
                            }
                        }
                    });
                }
            }
        });
        store.commit().unwrap();
        assert_eq!((a.len(), b.len()), (120, 120));
        drop(store);
        let store = Store::open(dir.path(), options()).unwrap();
        assert_eq!(store.frame("a").unwrap().count(&Query::new().word(2, "request")).unwrap(), 120);
        store.verify().unwrap();
    }

    #[test]
    fn date_and_time_columns_take_text_and_format_it_back() {
        let dir = tempfile::tempdir().unwrap();
        let schema = Schema::new(vec![
            Column::new("ts", ColumnType::TimestampMs).index_equality(),
            Column::new("at", ColumnType::TimestampTz).text_format("builtin:clf"),
            Column::new("day", ColumnType::Date).text_format("strftime:%d.%m.%Y"),
            Column::new("clock", ColumnType::TimeOfDay),
            Column::new("msg", ColumnType::String),
        ])
        .unwrap();
        let store = Store::create(dir.path(), options()).unwrap();
        let frame = store.create_frame("t", schema.clone()).unwrap();
        let rows = [
            ["2026-11-21T10:15:30.123Z", "21/Nov/2026:17:15:30 +0700", "21.11.2026", "10:15:30.5", "first"],
            ["2026-11-20T23:59:59.999+01:00", "20/Nov/2026:23:59:59 -0500", "20.11.2026", "23:59:59", "second"],
            ["2026-11-22 00:00:00.000", "22/Nov/2026:00:00:00 +0000", "22.11.2026", "00:00", "third"],
        ];
        for row in &rows {
            frame.push_row(&row.map(Value::Str)).unwrap();
        }
        let first = frame.row(0).unwrap();
        assert_eq!(first[0], OwnedValue::TimestampMs(1_795_256_130_123));
        assert_eq!(first[1], OwnedValue::TimestampTz { millis: 1_795_256_130_000, offset_minutes: 420 });
        assert_eq!((first[2].clone(), first[3].clone()), (OwnedValue::Date(20_778), OwnedValue::TimeOfDay(36_930_500_000_000)));
        let shown = |row: u64, column: usize| schema.columns()[column].format_value(&frame.row(row).unwrap()[column]).unwrap().unwrap();
        assert_eq!(shown(0, 0), "2026-11-21T10:15:30.123Z");
        assert_eq!(shown(1, 0), "2026-11-20T22:59:59.999Z"); // the offset is folded into the UTC instant
        assert_eq!(shown(1, 1), "20/Nov/2026:23:59:59 -0500"); // a TimestampTz keeps it
        assert_eq!(shown(0, 2), "21.11.2026");
        assert_eq!(shown(2, 3), "00:00:00.000000000");
        assert_eq!(schema.columns()[4].format_value(&OwnedValue::Null).unwrap(), None);

        // a bad value names its column and adds nothing, not even the new words of the other columns
        let words_before = store.words().read().unwrap().word_count();
        let bad = ["2026-11-21T10:15:30.123456Z", "21/Nov/2026:17:15:30 +0700", "21.11.2026", "10:15", "some new words"];
        match frame.push_row(&bad.map(Value::Str)) {
            Err(Error::DateParse { column, reason }) => assert_eq!((column.as_str(), reason.contains("precision")), ("ts", true)),
            other => panic!("{other:?}"),
        }
        assert!(matches!(
            frame.push_row(&[Value::TimestampSec(1), Value::Null, Value::Null, Value::Null, Value::Null]),
            Err(Error::TypeMismatch { .. })
        ));
        assert_eq!((frame.len(), store.words().read().unwrap().word_count()), (3, words_before));

        store.commit().unwrap();
        frame.push_row(&rows[0].map(Value::Str)).unwrap(); // the same instant again, now in the buffer
        for label in ["buffer + segment", "reopened"] {
            let reopened;
            let frame = if label == "reopened" {
                store.commit().unwrap();
                reopened = Store::open_read_only(dir.path(), options()).unwrap();
                reopened.frame("t").unwrap()
            } else {
                frame.clone()
            };
            // text bounds are parsed with the column's format
            let day_20 = Query::new().range(0, Some(Value::Str("2026-11-20T00:00:00Z")), Some(Value::Str("2026-11-20T23:59:59.999Z")));
            assert_eq!(frame.search(&day_20).unwrap().map(|r| r.unwrap()).collect::<Vec<_>>(), [1], "{label}");
            let from_21 = Query::new().range(0, Some(Value::Str("2026-11-21")), None);
            assert_eq!(frame.search(&from_21).unwrap().map(|r| r.unwrap()).collect::<Vec<_>>(), [0, 2, 3], "{label}");
            let same = Query::new().equals(0, Value::Str("2026-11-21T10:15:30.123Z"));
            assert_eq!(frame.search(&same).unwrap().map(|r| r.unwrap()).collect::<Vec<_>>(), [0, 3], "{label}");
            let by_time = Page { order_by: Some(OrderBy { column: 0, descending: true }), offset: 0, limit: 10 };
            assert_eq!(frame.search_page(&Query::new(), &by_time).unwrap().rows, [2, 0, 3, 1], "{label}");
            assert!(matches!(frame.search(&Query::new().range(0, Some(Value::Str("yesterday")), None)), Err(Error::DateParse { .. })));
        }
    }

    #[test]
    fn schemas_reject_bad_text_formats() {
        let bad = |ty, format: &str| matches!(Schema::new(vec![Column::new("c", ty).text_format(format)]), Err(Error::InvalidSchema(_)));
        assert!(
            bad(ColumnType::TimestampMs, "nonsense") && bad(ColumnType::TimestampMs, "strftime:%Q") && bad(ColumnType::Date, "builtin:clf")
        );
        assert!(bad(ColumnType::String, "builtin:iso8601") && bad(ColumnType::I64, "strftime:%Y"));
        assert!(Schema::new(vec![Column::new("c", ColumnType::TimestampSec).text_format("builtin:rfc2822")]).is_ok());
    }

    #[test]
    fn empty_store_and_decode_value_helper() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::create(dir.path(), options()).unwrap();
        store.create_frame("empty", schema()).unwrap();
        store.commit().unwrap();
        drop(store);
        let store = Store::open(dir.path(), options()).unwrap();
        assert!(store.frame("empty").unwrap().is_empty());
        assert!(store.disk_bytes().unwrap() > 0);
        let words = store.words().read().unwrap();
        assert_eq!(
            decode_value(ColumnType::U64, false, crate::cell::from_signed(0), &[], &words).unwrap(),
            OwnedValue::U64(crate::cell::from_signed(0))
        );
        assert!(matches!(Store::create(dir.path(), options()), Err(Error::Io(_))));
    }
}
