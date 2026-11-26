//! Stores (DESIGN.md 7 and 14): a folder holding a shared word heap and the segments of one or more dataframes, plus
//! the rows pushed since the last commit (the pending buffer).
//!
//! ```text
//! <folder>/manifest          what is committed (atomically replaced; the commit point)
//! <folder>/heap.bin          append-only zstd blocks of words, shared by all dataframes
//! <folder>/f<id>-<n>.seg     immutable segment files (see the segment module)
//! <folder>/lock              held exclusively by the process that has the store open for writing
//! <pending dir>/p-<id>-<epoch>.*   the mapped pending buffer (PendingMode::Mapped, see the pending module)
//! ```
//! Rows are pending until [`Store::commit`], which writes the new words, then one segment per dataframe that has new
//! rows, then the manifest. A crash before the manifest is replaced leaves the previous commit intact; stray files are
//! removed the next time the store is opened for writing. With [`PendingMode::Mapped`] (the default) the pending rows
//! live in memory-mapped files: they survive a crash of the writer process, and read-only stores in other processes
//! search them.

use crate::codec::{Reader, Writer};
use crate::dataframe::{DataFrame, DataFrameOptions, decode_value};
use crate::dictionary::{Dictionary, xxh3};
use crate::error::{Error, Result};
use crate::heap::{Heap, MemHeap};
use crate::index::{SegmentIndex, WordTable, build_index, compress_index, write_file_atomically};
use crate::pending::{ChunkSizes, Epoch, MappedFrame, SEALED, now_unix_ms, remove_epoch, remove_other_epochs};
use crate::query::{self, BufferSource, Page, PageResult, Prepared, Query, RowSource};
use crate::schema::{Column, ColumnFlags, Schema};
use crate::segment::{DEFAULT_MAX_OPEN_SEGMENTS, FilePool, SegmentReader, WriteOptions, write_segment};
use crate::tokenizer::{TokenizerOptions, decode_with};
use crate::types::{ColumnType, OwnedValue, Value};
use crate::varint;
use crate::words::{SharedWords, WordStore};
use bytes::BytesMut;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, RwLock, Weak};
use std::time::Duration;

const MANIFEST_MAGIC: &[u8; 8] = b"LSQMANI1";
const HEAP_MAGIC: &[u8; 8] = b"LSQHEAP1";
/// Version of the file formats written by this code (2: the manifest names the epoch of the pending buffer).
const FORMAT_VERSION: u32 = 2;
/// Version of the splitting rules (default and JSON-escape splitters); a folder is only opened if it matches.
const SPLITTER_VERSION: u8 = 1;
const HEAP_RECORD_HEADER: usize = 4 + 4 + 4 + 8;

/// Where a store keeps the rows pushed since the last commit (DESIGN.md 14).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PendingMode {
    /// In memory-mapped files (the default): pending rows survive a crash of the writer process (not a power loss or a
    /// kernel crash), and read-only stores in other processes see and search them. Without the `mmap` feature this
    /// behaves as [`PendingMode::Memory`].
    #[default]
    Mapped,
    /// In the writer's memory (the 0.1 behaviour): pending rows are lost if the process ends before a commit, and only
    /// the writer process sees them.
    Memory,
}

/// When [`Store::commit_if_due`] commits, besides `StoreOptions::segment_rows` rows in one dataframe (DESIGN.md 14.9).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitPolicy {
    /// Commit when the pending rows of the store take this many bytes (cells, row heaps and new words; default 256 MiB).
    /// Bounds the memory (or page cache) the pending buffer uses and the time a commit takes.
    pub max_bytes: u64,
    /// Commit when the oldest pending row is this old (default: no limit). The time is kept across restarts.
    pub max_age: Option<Duration>,
}

impl Default for CommitPolicy {
    fn default() -> Self {
        Self { max_bytes: 256 << 20, max_age: None }
    }
}

/// Why [`Store::commit_if_due`] committed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommitTrigger {
    /// A dataframe reached `StoreOptions::segment_rows` pending rows.
    Rows,
    /// The pending rows reached `CommitPolicy::max_bytes`.
    Bytes,
    /// The oldest pending row reached `CommitPolicy::max_age`.
    Age,
}

/// What opening a store found in its pending buffer (see [`Store::recovery`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct RecoveryReport {
    /// Pending rows found (rows pushed but not committed before the store was last closed or its process ended).
    pub pending_rows: u64,
    /// The manifest names a mapped pending buffer whose files are gone, for example a tmpfs `pending_dir` after a
    /// reboot. Its rows are lost.
    pub pending_files_missing: bool,
}

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
    /// Rows at which [`Store::commit_if_full`] and [`Store::commit_if_due`] commit (default 100,000).
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
    /// Where the rows since the last commit are kept (default [`PendingMode::Mapped`]).
    pub pending: PendingMode,
    /// Directory of the mapped pending buffer (default `None`: the store folder). A tmpfs such as `/dev/shm` avoids disk
    /// writes but loses pending rows on reboot. Readers find it through the manifest.
    pub pending_dir: Option<PathBuf>,
    /// Size of the first chunk file of each mapped region (default 1 MiB); later chunks double up to
    /// `pending_chunk_max_bytes`.
    pub pending_chunk_min_bytes: u64,
    /// Largest chunk file of a mapped region (default 64 MiB), except for single entries that are larger.
    pub pending_chunk_max_bytes: u64,
    /// When [`Store::commit_if_due`] commits.
    pub commit_policy: CommitPolicy,
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
            pending: PendingMode::Mapped,
            pending_dir: None,
            pending_chunk_min_bytes: 1 << 20,
            pending_chunk_max_bytes: 64 << 20,
            commit_policy: CommitPolicy::default(),
        }
    }
}

impl StoreOptions {
    fn mapped(&self) -> bool {
        cfg!(feature = "mmap") && self.pending == PendingMode::Mapped
    }

    fn chunk_sizes(&self) -> ChunkSizes {
        ChunkSizes { min: self.pending_chunk_min_bytes, max: self.pending_chunk_max_bytes }
    }

    fn dataframe_options(&self) -> DataFrameOptions {
        DataFrameOptions {
            tokenizer: self.tokenizer.clone(),
            dedup_entries: self.dedup_entries,
            row_heap_block_bytes: self.row_heap_block_bytes,
        }
    }

    /// The pending directory as recorded in the manifest: empty for the store folder, else an absolute path.
    fn pending_dir_text(&self) -> Result<String> {
        match &self.pending_dir {
            None => Ok(String::new()),
            Some(path) => std::path::absolute(path)?
                .to_str()
                .map(str::to_owned)
                .ok_or_else(|| Error::Io(std::io::Error::new(std::io::ErrorKind::InvalidInput, "pending_dir must be valid UTF-8"))),
        }
    }
}

fn pending_dir(dir: &Path, text: &str) -> PathBuf {
    if text.is_empty() { dir.to_path_buf() } else { PathBuf::from(text) }
}

/// A random-enough id for the pending files of a new store (they may share a directory with other stores).
fn new_store_id(dir: &Path) -> u128 {
    let nanos = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map_or(0, |d| d.as_nanos());
    let seed = format!("{}|{nanos}|{}|{:?}", dir.display(), std::process::id(), std::thread::current().id());
    let id = (u128::from(xxh3(seed.as_bytes(), 1)) << 64) | u128::from(xxh3(seed.as_bytes(), 2));
    id.max(1)
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
    /// The epoch of the pending buffer (incremented by every commit; 0 in a format 1 manifest).
    epoch: u64,
    /// Names the pending files (0 in a format 1 manifest: a new id is made at the next commit).
    store_id: u128,
    /// Whether the pending buffer of `epoch` is mapped.
    mapped: bool,
    /// Directory of the mapped pending buffer: empty for the store folder.
    pending_dir: String,
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
        if version >= 2 {
            w.u64(self.epoch);
            w.u64((self.store_id >> 64) as u64);
            w.u64(self.store_id as u64);
            w.u8(u8::from(self.mapped));
            w.str(&self.pending_dir);
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
        let version = r.u32()?;
        if version > FORMAT_VERSION || r.u8()? != SPLITTER_VERSION {
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
        let (epoch, store_id, mapped, pending_dir) = if version >= 2 {
            let epoch = r.u64()?;
            let store_id = (u128::from(r.u64()?) << 64) | u128::from(r.u64()?);
            (epoch, store_id, r.u8()? != 0, r.str()?)
        } else {
            (0, 0, false, String::new())
        };
        if !r.is_empty() {
            return Err(Error::Corrupt("manifest has trailing bytes"));
        }
        Ok(Self { sealed, heap_bytes, heap_words, frames, epoch, store_id, mapped, pending_dir })
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
// Pending buffers

// One per dataframe, behind an `Arc`: the size difference does not matter.
#[allow(clippy::large_enum_variant)]
enum Buffer {
    Memory(DataFrame<MemHeap>),
    Mapped(MappedFrame),
}

/// The rows of one dataframe pushed since the last commit. A commit replaces it with a new one, so a [`View`] that
/// holds the old one keeps a consistent snapshot.
struct Pending {
    buffer: Buffer,
    columns: usize,
    /// Index of the pending rows, valid while there are this many rows.
    index: Mutex<Option<(u64, Arc<SegmentIndex>)>>,
    /// Memory buffers: when the first row was pushed (Unix milliseconds), or 0.
    first_push_ms: AtomicU64,
}

impl Pending {
    fn memory(schema: &Schema, words: &SharedWords, options: &StoreOptions) -> Self {
        let buffer = Buffer::Memory(DataFrame::with_options(schema.clone(), words.clone(), options.dataframe_options()));
        Self { buffer, columns: schema.len(), index: Mutex::new(None), first_push_ms: AtomicU64::new(0) }
    }

    fn mapped(frame: MappedFrame, schema: &Schema) -> Self {
        Self { buffer: Buffer::Mapped(frame), columns: schema.len(), index: Mutex::new(None), first_push_ms: AtomicU64::new(0) }
    }

    /// The buffer of a frame: mapped from `epoch` when there is one (and its files exist), in memory otherwise.
    fn open(epoch: Option<&Arc<Epoch>>, id: usize, schema: &Schema, words: &SharedWords, options: &StoreOptions) -> Result<Self> {
        if let Some(epoch) = epoch {
            if let Some(frame) = epoch.open_frame(id, Arc::new(schema.clone()), words, &options.dataframe_options())? {
                return Ok(Self::mapped(frame, schema));
            }
        }
        Ok(Self::memory(schema, words, options))
    }

    /// Rows that can be read (all rows for the writer; the published rows for a reader).
    fn rows(&self) -> Result<u64> {
        match &self.buffer {
            Buffer::Memory(frame) => Ok(frame.len()),
            Buffer::Mapped(frame) => frame.published_rows(),
        }
    }

    fn push_row(&self, row: &[Value<'_>]) -> Result<u64> {
        match &self.buffer {
            Buffer::Memory(frame) => {
                let number = frame.push_row(row)?;
                if number == 0 {
                    self.first_push_ms.store(now_unix_ms().max(1), Ordering::Relaxed);
                }
                Ok(number)
            }
            Buffer::Mapped(frame) => frame.push_row(row),
        }
    }

    fn pending_bytes(&self) -> u64 {
        match &self.buffer {
            Buffer::Memory(frame) => frame.len() * self.columns as u64 * 8 + frame.row_heap_len(),
            Buffer::Mapped(frame) => frame.pending_bytes(),
        }
    }

    fn first_push_ms(&self) -> u64 {
        match &self.buffer {
            Buffer::Memory(_) => self.first_push_ms.load(Ordering::Relaxed),
            Buffer::Mapped(frame) => frame.first_push_ms(),
        }
    }

    /// Runs `f` on the first `rows` rows. A memory buffer is locked while `f` runs: take the word store inside `f`, not
    /// before (see `query::BufferSource`).
    fn with_source<R>(&self, rows: u64, f: impl FnOnce(&dyn RowSource) -> Result<R>) -> Result<R> {
        match &self.buffer {
            Buffer::Memory(frame) => frame.with_inner(|inner| f(&BufferSource::first(inner, rows))),
            Buffer::Mapped(frame) => f(&frame.source(rows)?),
        }
    }

    /// Writes every row as the segment `file` in `dir`, and its index when indexing is on.
    fn write_segment(&self, dir: &Path, file: &str, schema: &Schema, words: &SharedWords, options: &StoreOptions) -> Result<()> {
        let write_options = WriteOptions { block_rows: options.block_rows, zstd_level: options.zstd_level };
        let path = dir.join(file);
        match &self.buffer {
            Buffer::Memory(frame) => frame.with_inner(|inner| {
                write_segment(&path, inner, write_options)?;
                if options.indexing {
                    // the buffer lock is held: the words after it (see query::BufferSource)
                    let words = words.read().expect("word store lock poisoned");
                    write_index(dir, options, schema, &BufferSource::all(inner), &words, file)?;
                }
                Ok(())
            }),
            Buffer::Mapped(frame) => {
                let source = frame.segment_source()?;
                write_segment(&path, &source, write_options)?;
                if options.indexing {
                    let words = words.read().expect("word store lock poisoned");
                    write_index(dir, options, schema, &source, &words, file)?;
                }
                Ok(())
            }
        }
    }

    /// The index of the first `source.row_count()` rows, rebuilt when there are more rows than when it was built.
    fn index(&self, source: &dyn RowSource, schema: &Schema, words: &WordStore) -> Result<Option<Arc<SegmentIndex>>> {
        let flags = |c: &Column| c.flags.index_words || c.flags.case_insensitive || c.flags.index_equality;
        if !schema.columns().iter().any(flags) {
            return Ok(None);
        }
        let rows = source.row_count();
        let mut cache = self.index.lock().expect("index cache lock poisoned");
        if let Some((cached_rows, index)) = &*cache {
            if *cached_rows == rows {
                return Ok(Some(Arc::clone(index)));
            }
        }
        let bytes = build_index(source, schema, words)?;
        let index = Arc::new(SegmentIndex::from_vec(bytes)?);
        *cache = Some((rows, Arc::clone(&index)));
        Ok(Some(index))
    }
}

/// Decodes one value of a pending row.
fn source_value(source: &dyn RowSource, column: &Column, words: &WordStore, row: u64, index: usize) -> Result<OwnedValue> {
    let (cell, null) = source.cell(row, index)?;
    if !column.ty.is_variable() || null {
        return decode_value(column.ty, null, cell, &[], words);
    }
    let mut value = None;
    source.with_entry(cell, &mut |tail| {
        value = Some(decode_value(column.ty, false, cell, tail, words)?);
        Ok(())
    })?;
    value.ok_or(Error::Corrupt("row heap entry"))
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
    segments: Arc<Vec<Arc<Segment>>>,
    committed_rows: u64,
    next_segment: u32,
    pending: Arc<Pending>,
}

/// A consistent snapshot of a frame for one read or query: its segments, and its pending buffer up to a row count.
#[derive(Clone)]
struct View {
    segments: Arc<Vec<Arc<Segment>>>,
    committed_rows: u64,
    pending: Arc<Pending>,
    pending_rows: u64,
}

impl View {
    /// The committed segment holding `row` and the row's number in it; `None` if `row` is pending or past the end.
    fn locate(&self, row: u64) -> Option<(usize, u64)> {
        if row >= self.committed_rows {
            return None;
        }
        let i = self.segments.partition_point(|s| s.start + s.rows <= row);
        Some((i, row - self.segments[i].start))
    }

    /// The number of `row` in the pending buffer.
    fn pending_row(&self, row: u64) -> Result<u64> {
        row.checked_sub(self.committed_rows).filter(|&local| local < self.pending_rows).ok_or(Error::RowOutOfRange)
    }
}

/// One dataframe of a store: committed segments on disk plus the rows pushed since the last commit.
pub struct Frame {
    id: usize,
    name: String,
    schema: Schema,
    words: SharedWords,
    writable: Arc<AtomicBool>,
    /// The folder-wide write mutex of the store (see [`Store`]).
    write_mutex: Arc<Mutex<()>>,
    state: RwLock<FrameState>,
    /// The store, for the auto refresh of read-only stores (DESIGN.md 14.7).
    shared: OnceLock<Weak<Shared>>,
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

    /// A snapshot for one read or query. A read-only store first picks up new commits and pending rows of the writer.
    fn view(&self) -> Result<View> {
        let shared = self.shared.get().and_then(Weak::upgrade);
        if let Some(shared) = &shared {
            shared.refresh()?;
        }
        let view = {
            let state = self.state.read().expect("frame lock poisoned");
            let pending_rows = state.pending.rows()?;
            View { segments: state.segments.clone(), committed_rows: state.committed_rows, pending: state.pending.clone(), pending_rows }
        };
        // after the row count: the words of those rows are published before it
        if let Some(shared) = &shared {
            shared.load_pending_words()?;
        }
        Ok(view)
    }

    /// Number of rows, committed and pending.
    pub fn len(&self) -> u64 {
        self.view().map_or(0, |v| v.committed_rows + v.pending_rows)
    }

    /// Whether there are no rows.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Rows pushed since the last commit (the next commit writes them). A read-only store counts the rows the writer
    /// has published.
    pub fn pending_rows(&self) -> u64 {
        self.view().map_or(0, |v| v.pending_rows)
    }

    /// Bytes the pending rows of this dataframe take (cells and row heap).
    pub fn pending_bytes(&self) -> u64 {
        self.state.read().expect("frame lock poisoned").pending.pending_bytes()
    }

    /// Appends a row and returns its row number. With [`PendingMode::Mapped`] the row survives a crash of the process
    /// once this returns; it is in a segment after [`Store::commit`].
    pub fn push_row(&self, row: &[Value<'_>]) -> Result<u64> {
        if !self.writable.load(Ordering::Acquire) {
            return Err(Error::ReadOnly);
        }
        let _write = self.write_mutex.lock().expect("write mutex poisoned");
        let state = self.state.read().expect("frame lock poisoned");
        Ok(state.committed_rows + state.pending.push_row(row)?)
    }

    /// Reads one row.
    pub fn row(&self, row: u64) -> Result<Vec<OwnedValue>> {
        let view = self.view()?;
        match view.locate(row) {
            Some((segment, local)) => {
                let words = self.words.read().expect("word store lock poisoned");
                view.segments[segment].reader.row(&self.schema, &words, local)
            }
            None => {
                let local = view.pending_row(row)?;
                view.pending.with_source(view.pending_rows, |source| {
                    let words = self.words.read().expect("word store lock poisoned");
                    let columns = self.schema.columns().iter().enumerate();
                    columns.map(|(i, column)| source_value(source, column, &words, local, i)).collect()
                })
            }
        }
    }

    /// Reads one value.
    pub fn value(&self, row: u64, column: usize) -> Result<OwnedValue> {
        let view = self.view()?;
        match view.locate(row) {
            Some((segment, local)) => {
                let words = self.words.read().expect("word store lock poisoned");
                view.segments[segment].reader.value(&self.schema, &words, local, column)
            }
            None => {
                let local = view.pending_row(row)?;
                let col = self.schema.columns().get(column).ok_or(Error::ColumnOutOfRange)?;
                view.pending.with_source(view.pending_rows, |source| {
                    let words = self.words.read().expect("word store lock poisoned");
                    source_value(source, col, &words, local, column)
                })
            }
        }
    }

    /// Writes the text of a string cell into `out`; `false` for null.
    pub fn write_string(&self, row: u64, column: usize, out: &mut BytesMut) -> Result<bool> {
        let col = self.schema.columns().get(column).ok_or(Error::ColumnOutOfRange)?;
        if col.ty != ColumnType::String {
            return Err(Error::TypeMismatch { column: col.name.clone() });
        }
        let view = self.view()?;
        match view.locate(row) {
            Some((segment, local)) => {
                let words = self.words.read().expect("word store lock poisoned");
                view.segments[segment].reader.write_string(&words, local, column, out)
            }
            None => {
                let local = view.pending_row(row)?;
                view.pending.with_source(view.pending_rows, |source| {
                    let (cell, null) = source.cell(local, column)?;
                    if null {
                        return Ok(false);
                    }
                    let words = self.words.read().expect("word store lock poisoned");
                    source.with_entry(cell, &mut |tail| decode_with(&words, tail, out).map(|_| ()))?;
                    Ok(true)
                })
            }
        }
    }

    /// The row numbers that match `query`, in insertion order, evaluated one segment at a time and then over the pending
    /// rows. The search works on a snapshot taken when it starts: rows pushed or committed later are not seen.
    pub fn search(&self, query: &Query) -> Result<RowIds<'_>> {
        let view = self.view()?;
        self.search_in(view, query)
    }

    fn search_in(&self, view: View, query: &Query) -> Result<RowIds<'_>> {
        let prepared = query::prepare(query, &self.schema, &self.words.read().expect("word store lock poisoned"))?;
        Ok(RowIds { frame: self, view, prepared, unit: 0, pending: Vec::new().into_iter(), done: false })
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
        let view = self.view()?;
        let mut rows: Vec<u64> = self.search_in(view.clone(), query)?.collect::<Result<_>>()?;
        let total = rows.len() as u64;
        if let Some(order) = page.order_by {
            let column = self.schema.columns().get(order.column).ok_or(Error::ColumnOutOfRange)?;
            if column.ty.is_variable() {
                return Err(Error::TypeMismatch { column: column.name.clone() });
            }
            let mut keyed: Vec<(Option<u64>, u64)> = Vec::with_capacity(rows.len());
            for &row in &rows {
                keyed.push((self.sort_key(&view, row, order.column)?, row));
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

    fn sort_key(&self, view: &View, row: u64, column: usize) -> Result<Option<u64>> {
        let (cell, null) = match view.locate(row) {
            Some((segment, local)) => view.segments[segment].reader.cell(local, column)?,
            None => {
                let local = view.pending_row(row)?;
                view.pending.with_source(view.pending_rows, |source| source.cell(local, column))?
            }
        };
        Ok((!null).then_some(cell))
    }

    /// The matches of `prepared` in one unit of `view`: segments in order, then the pending rows. `None` after the last.
    fn search_unit(&self, view: &View, prepared: &[Prepared], unit: usize) -> Result<Option<Vec<u64>>> {
        if let Some(segment) = view.segments.get(unit) {
            let words = self.words.read().expect("word store lock poisoned");
            let rows = query::evaluate(&segment.reader, segment.index.as_ref(), &words, prepared)?;
            return Ok(Some(rows.into_iter().map(|r| segment.start + u64::from(r)).collect()));
        }
        if unit != view.segments.len() {
            return Ok(None);
        }
        if view.pending_rows == 0 {
            return Ok(Some(Vec::new()));
        }
        let base = view.committed_rows;
        let rows = view.pending.with_source(view.pending_rows, |source| {
            // a memory buffer is locked first, then the words (the order of push_row; see query::BufferSource)
            let words = self.words.read().expect("word store lock poisoned");
            let index = view.pending.index(source, &self.schema, &words)?;
            query::evaluate(source, index.as_deref(), &words, prepared)
        })?;
        Ok(Some(rows.into_iter().map(|r| base + u64::from(r)).collect()))
    }

    /// An iterator over all rows in insertion order.
    pub fn rows(&self) -> FrameRows<'_> {
        FrameRows { frame: self, next: 0 }
    }
}

/// Lazy iterator over the row numbers that match a query; see [`Frame::search`].
pub struct RowIds<'a> {
    frame: &'a Frame,
    view: View,
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
            match self.frame.search_unit(&self.view, &self.prepared, self.unit) {
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
    epoch: u64,
    store_id: u128,
    mapped: bool,
    pending_dir: String,
    /// Bytes of the words in the word store at the last commit (to measure the pending words).
    word_bytes: u64,
}

impl Committed {
    fn from_manifest(manifest: &Manifest, word_bytes: u64) -> Self {
        Self {
            heap_bytes: manifest.heap_bytes,
            heap_words: manifest.heap_words,
            sealed: manifest.sealed,
            epoch: manifest.epoch,
            store_id: manifest.store_id,
            mapped: manifest.mapped,
            pending_dir: manifest.pending_dir.clone(),
            word_bytes,
        }
    }
}

/// The state of a store, shared with its frames (which use it to refresh a read-only store).
struct Shared {
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
    /// The open segment files of all dataframes, at most [`Store::max_open_segments`] of them.
    segment_files: Arc<FilePool>,
    /// The current epoch of the mapped pending buffer.
    epoch: RwLock<Option<Arc<Epoch>>>,
    /// Serializes the reloads of a read-only store.
    reload_lock: Mutex<()>,
    recovery: RecoveryReport,
    /// A weak reference to this, for the frames a reload adds.
    me: OnceLock<Weak<Shared>>,
    /// Held for as long as the store is open for writing.
    _lock: Option<WriterLock>,
}

/// A folder with a shared word heap and dictionary and the dataframes that use them.
///
/// A store opened for writing holds the folder's writer lock. A read-only store ([`Store::open_read_only`]) of a store
/// whose writer keeps its pending rows mapped ([`PendingMode::Mapped`]) follows the writer: every read and query first
/// picks up new commits and new pending rows (DESIGN.md 14.7).
pub struct Store {
    shared: Arc<Shared>,
}

/// The writer lock of a folder (an exclusive advisory lock on its `lock` file), released when dropped.
struct WriterLock {
    file: File,
    /// The process that took the lock.
    owner: u32,
}

impl Drop for WriterLock {
    /// Unlocks explicitly before the file is closed. On Unix the lock belongs to the open file, which a child process
    /// forked by another thread shares until it executes: closing only our handle would leave the lock held by the
    /// child's copy, and an immediate reopen would fail with `Error::Locked`. An unlock through any copy releases it
    /// for all. Only the process that took the lock unlocks: a forked child that drops an inherited store must not
    /// release its parent's lock. On Windows the explicit unlock also makes the release immediate.
    fn drop(&mut self) {
        if self.owner == std::process::id() {
            let _ = fs4::FileExt::unlock(&self.file);
        }
    }
}

fn acquire_writer_lock(dir: &Path) -> Result<WriterLock> {
    let file = OpenOptions::new().read(true).write(true).create(true).truncate(false).open(dir.join("lock"))?;
    match fs4::FileExt::try_lock(&file) {
        Ok(()) => Ok(WriterLock { file, owner: std::process::id() }),
        Err(fs4::TryLockError::WouldBlock) => Err(Error::Locked),
        Err(fs4::TryLockError::Error(e)) => Err(e.into()),
    }
}

fn open_segment(
    dir: &Path,
    meta: &SegmentMeta,
    columns: usize,
    start: u64,
    options: &StoreOptions,
    pool: &Arc<FilePool>,
) -> Result<Segment> {
    let reader = SegmentReader::open(&dir.join(&meta.file), columns, pool)?.with_cache_limit(options.cache_bytes);
    if reader.rows() != meta.rows {
        return Err(Error::Corrupt("segment row count differs from the manifest"));
    }
    let index_path = dir.join(meta.file.replace(".seg", ".idx"));
    let index = if index_path.exists() { Some(SegmentIndex::open(&index_path)?) } else { None };
    Ok(Segment { file: meta.file.clone(), rows: meta.rows, start, reader, index })
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
        let store_id = new_store_id(&dir);
        let mapped = options.mapped();
        let pending_text = if mapped { options.pending_dir_text()? } else { String::new() };
        let epoch =
            if mapped { Some(Epoch::create(&pending_dir(&dir, &pending_text), store_id, 1, 0, options.chunk_sizes())?) } else { None };
        let manifest = Manifest {
            sealed: false,
            heap_bytes: HEAP_MAGIC.len() as u64,
            heap_words: 0,
            frames: Vec::new(),
            epoch: 1,
            store_id,
            mapped,
            pending_dir: pending_text,
        };
        write_manifest(&dir, &manifest)?;
        let words = WordStore::new(MemHeap::new(), Dictionary::new(options.dictionary_bytes)).into_shared();
        Ok(Self {
            shared: Arc::new(Shared {
                dir,
                options,
                words,
                writable: Arc::new(AtomicBool::new(true)),
                write_mutex: Arc::new(Mutex::new(())),
                committed: Mutex::new(Committed::from_manifest(&manifest, 0)),
                frames: RwLock::new(Vec::new()),
                word_table: RwLock::new(None),
                segment_files: FilePool::new(DEFAULT_MAX_OPEN_SEGMENTS),
                epoch: RwLock::new(epoch),
                reload_lock: Mutex::new(()),
                recovery: RecoveryReport::default(),
                me: OnceLock::new(),
                _lock: Some(lock),
            }),
        }
        .with_me())
    }

    fn with_me(self) -> Self {
        let _ = self.shared.me.set(Arc::downgrade(&self.shared));
        self
    }

    /// Opens an existing store for reading and writing. Fails with [`Error::Locked`] if another process has it open for
    /// writing, and with [`Error::ReadOnly`] if it was sealed. Files left by an interrupted commit are removed. Pending
    /// rows left by a process that ended without committing are recovered (see [`Store::recovery`]). If the options ask
    /// for another [`PendingMode`] or `pending_dir` than the store uses, the pending rows are committed and the store
    /// switches.
    pub fn open(path: impl AsRef<Path>, options: StoreOptions) -> Result<Self> {
        Self::open_with(path.as_ref(), options, true)
    }

    /// Opens an existing store for reading only. It does not take the writer lock, so it works while another process
    /// is writing. If that writer keeps its pending rows mapped, this store follows it: reads and queries see new
    /// commits and pending rows. Otherwise it sees the data committed when it was opened.
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
        let mut words = WordStore::from_heap(heap, Dictionary::new(limit))?;
        let word_bytes = words.heap().byte_len();

        let mut recovery = RecoveryReport::default();
        let pdir = pending_dir(dir, &manifest.pending_dir);
        let epoch = if manifest.mapped && !manifest.sealed {
            let sizes = options.chunk_sizes();
            match Epoch::open(&pdir, manifest.store_id, manifest.epoch, manifest.heap_words, sizes, writable)? {
                Some(epoch) => {
                    epoch.load_words(&mut words)?;
                    Some(epoch)
                }
                None => {
                    recovery.pending_files_missing = true;
                    match writable {
                        true => Some(Epoch::create(&pdir, manifest.store_id, manifest.epoch, manifest.heap_words, sizes)?),
                        false => None,
                    }
                }
            }
        } else {
            None
        };
        let words = words.into_shared();
        let flag = Arc::new(AtomicBool::new(writable));
        let write_mutex = Arc::new(Mutex::new(()));
        let segment_files = FilePool::new(DEFAULT_MAX_OPEN_SEGMENTS);

        let mut frames = Vec::new();
        for (id, meta) in manifest.frames.iter().enumerate() {
            let mut segments = Vec::new();
            let mut start = 0;
            for segment in &meta.segments {
                segments.push(Arc::new(open_segment(dir, segment, meta.schema.len(), start, &options, &segment_files)?));
                start += segment.rows;
            }
            let pending = Pending::open(epoch.as_ref(), id, &meta.schema, &words, &options)?;
            recovery.pending_rows += pending.rows()?;
            let state = FrameState {
                segments: Arc::new(segments),
                committed_rows: start,
                next_segment: meta.next_segment,
                pending: Arc::new(pending),
            };
            frames.push(Arc::new(Frame {
                id,
                name: meta.name.clone(),
                schema: meta.schema.clone(),
                words: words.clone(),
                writable: flag.clone(),
                write_mutex: write_mutex.clone(),
                state: RwLock::new(state),
                shared: OnceLock::new(),
            }));
        }
        let table_path = dir.join("words.idx");
        let word_table = if manifest.sealed && table_path.exists() { Some(WordTable::open(&table_path)?) } else { None };
        let shared = Arc::new(Shared {
            dir: dir.to_path_buf(),
            options,
            words,
            writable: flag,
            write_mutex,
            committed: Mutex::new(Committed::from_manifest(&manifest, word_bytes)),
            frames: RwLock::new(frames),
            word_table: RwLock::new(word_table),
            segment_files,
            epoch: RwLock::new(epoch.clone()),
            reload_lock: Mutex::new(()),
            recovery,
            me: OnceLock::new(),
            _lock: lock,
        });
        let _ = shared.me.set(Arc::downgrade(&shared));
        for frame in shared.frames.read().expect("frame list lock poisoned").iter() {
            let _ = frame.shared.set(Arc::downgrade(&shared));
        }
        let store = Self { shared };
        if writable {
            let s = &store.shared;
            if let Some(epoch) = &epoch {
                // older epochs left by an interrupted commit (and files Windows could not delete before)
                remove_other_epochs(epoch.dir(), manifest.store_id, Some(epoch.number()), epoch.number())?;
            }
            let wanted_dir = if s.options.mapped() { s.options.pending_dir_text()? } else { String::new() };
            let switch = manifest.store_id == 0 || manifest.mapped != s.options.mapped() || manifest.pending_dir != wanted_dir;
            if switch {
                store.commit()?;
            }
        }
        Ok(store)
    }

    /// The word store shared by all dataframes.
    pub fn words(&self) -> &SharedWords {
        &self.shared.words
    }

    /// The folder.
    pub fn path(&self) -> &Path {
        &self.shared.dir
    }

    /// What opening the store found in its pending buffer: rows recovered from a process that ended without committing,
    /// and whether the pending files were missing.
    pub fn recovery(&self) -> RecoveryReport {
        self.shared.recovery
    }

    /// The most segment files the store keeps open at once (default 64). Each committed segment is read from its own
    /// file; when more segments than this are read, the least recently used file is closed and opened again on its next
    /// read. Files that a read is using at that moment are closed when the read ends, so the count can briefly exceed
    /// the limit by the number of reads in progress.
    pub fn max_open_segments(&self) -> usize {
        self.shared.segment_files.limit()
    }

    /// Changes the most segment files the store keeps open (at least 1; see [`Store::max_open_segments`]). Lowering it
    /// closes the least recently used files at once. Read results do not depend on it, only how often a file is reopened.
    pub fn set_max_open_segments(&self, limit: usize) {
        self.shared.segment_files.set_limit(limit);
    }

    /// Segment files the store has open now.
    pub fn open_segment_files(&self) -> usize {
        self.shared.segment_files.open_files()
    }

    /// Whether rows can still be added.
    pub fn is_writable(&self) -> bool {
        self.shared.writable.load(Ordering::Acquire)
    }

    /// Adds a dataframe. It is recorded in the manifest at once, so read-only stores see it; its rows are pending until
    /// the next commit.
    pub fn create_frame(&self, name: &str, schema: Schema) -> Result<Arc<Frame>> {
        let s = &self.shared;
        if !self.is_writable() {
            return Err(Error::ReadOnly);
        }
        let _write = s.write_mutex.lock().expect("write mutex poisoned");
        let committed = s.committed.lock().expect("commit lock poisoned");
        let mut frames = s.frames.write().expect("frame list lock poisoned");
        if frames.iter().any(|f| f.name == name) {
            return Err(Error::FrameExists);
        }
        let id = frames.len();
        let epoch = s.epoch.read().expect("epoch lock poisoned").clone();
        let pending = Pending::open(epoch.as_ref(), id, &schema, &s.words, &s.options)?;
        let state = FrameState { segments: Arc::new(Vec::new()), committed_rows: 0, next_segment: 0, pending: Arc::new(pending) };
        let frame = Arc::new(Frame {
            id,
            name: name.to_owned(),
            schema,
            words: s.words.clone(),
            writable: s.writable.clone(),
            write_mutex: s.write_mutex.clone(),
            state: RwLock::new(state),
            shared: OnceLock::new(),
        });
        let _ = frame.shared.set(Arc::downgrade(s));
        let mut all: Vec<Arc<Frame>> = frames.clone();
        all.push(frame.clone());
        write_manifest(&s.dir, &s.manifest_now(&committed, &all))?;
        frames.push(frame.clone());
        Ok(frame)
    }

    /// The dataframe with the given name.
    pub fn frame(&self, name: &str) -> Result<Arc<Frame>> {
        let s = &self.shared;
        s.refresh()?;
        let find = || s.frames.read().expect("frame list lock poisoned").iter().find(|f| f.name == name).cloned();
        if let Some(frame) = find() {
            return Ok(frame);
        }
        if s.follows_writer() {
            // the writer may have created it since this store was opened
            s.reload()?;
        }
        find().ok_or(Error::UnknownFrame)
    }

    /// Names of all dataframes.
    pub fn frame_names(&self) -> Vec<String> {
        let s = &self.shared;
        if s.follows_writer() {
            let _ = s.reload();
        }
        s.frames.read().expect("frame list lock poisoned").iter().map(|f| f.name.clone()).collect()
    }

    /// Rows pushed since the last commit, in all dataframes.
    pub fn pending_rows(&self) -> u64 {
        self.shared.frames.read().expect("frame list lock poisoned").iter().map(|f| f.pending_rows()).sum()
    }

    /// Bytes the pending rows take: the cells and row heaps of all dataframes and the words added since the last commit.
    pub fn pending_bytes(&self) -> u64 {
        let s = &self.shared;
        let frames: u64 = s.frames.read().expect("frame list lock poisoned").iter().map(|f| f.pending_bytes()).sum();
        let word_bytes = s.committed.lock().expect("commit lock poisoned").word_bytes;
        let words = s.words.read().expect("word store lock poisoned").heap().byte_len();
        frames + words.saturating_sub(word_bytes)
    }

    /// Commits if any dataframe has at least `segment_rows` pending rows. Returns whether it committed.
    pub fn commit_if_full(&self) -> Result<bool> {
        let rows = self.shared.options.segment_rows as u64;
        let full = self.shared.frames.read().expect("frame list lock poisoned").iter().any(|f| f.pending_rows() >= rows);
        if full {
            self.commit()?;
        }
        Ok(full)
    }

    /// Commits when it is due (DESIGN.md 14.9): a dataframe has `segment_rows` pending rows, the pending rows take
    /// `commit_policy.max_bytes`, or the oldest pending row is `commit_policy.max_age` old. Returns what triggered the
    /// commit, or `None`. Call it regularly while writing (for example every 1,000 rows, and from a timer when an age
    /// limit is set).
    pub fn commit_if_due(&self) -> Result<Option<CommitTrigger>> {
        if !self.is_writable() {
            return Err(Error::ReadOnly);
        }
        let trigger = self.due();
        if trigger.is_some() {
            self.commit()?;
        }
        Ok(trigger)
    }

    fn due(&self) -> Option<CommitTrigger> {
        let s = &self.shared;
        let policy = &s.options.commit_policy;
        let frames = s.frames.read().expect("frame list lock poisoned").clone();
        if frames.iter().any(|f| f.pending_rows() >= s.options.segment_rows as u64) {
            return Some(CommitTrigger::Rows);
        }
        if self.pending_bytes() >= policy.max_bytes {
            return Some(CommitTrigger::Bytes);
        }
        let max_age = policy.max_age?;
        let pending = |f: &Arc<Frame>| f.state.read().expect("frame lock poisoned").pending.clone();
        let oldest = frames.iter().map(pending).map(|p| p.first_push_ms()).filter(|&ms| ms > 0).min()?;
        (now_unix_ms().saturating_sub(oldest) >= max_age.as_millis() as u64).then_some(CommitTrigger::Age)
    }

    /// Writes everything pending to disk: new words, one segment per dataframe with new rows, then the manifest.
    pub fn commit(&self) -> Result<()> {
        self.shared.commit(false)
    }

    /// Commits and closes the store for writing; it can only be opened read-only afterwards (log rotation).
    pub fn seal(&self) -> Result<()> {
        self.shared.commit(true)
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
        let segment_files = FilePool::new(1);
        for frame in &manifest.frames {
            let flags = |c: &Column| c.flags.index_words || c.flags.case_insensitive || c.flags.index_equality;
            if !frame.schema.columns().iter().any(flags) {
                continue;
            }
            for segment in &frame.segments {
                if dir.join(segment.file.replace(".seg", ".idx")).exists() {
                    continue;
                }
                let reader = SegmentReader::open(&dir.join(&segment.file), frame.schema.len(), &segment_files)?;
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

    /// The ordinal of a word in the shared heap (including the words of pending rows).
    pub fn lookup_word(&self, word: &str) -> Option<u32> {
        let s = &self.shared;
        if s.refresh().and_then(|()| s.load_pending_words()).is_err() {
            return None;
        }
        let words = s.words.read().expect("word store lock poisoned");
        match &*s.word_table.read().expect("word table lock poisoned") {
            Some(table) => table.lookup(&words, word),
            None => words.lookup(word),
        }
    }

    /// Up to `limit` words that start with `prefix`, in byte order (for suggestions). A sealed store answers from its
    /// memory-mapped word table; otherwise the heap is scanned.
    pub fn words_with_prefix(&self, prefix: &str, limit: usize) -> Vec<String> {
        let s = &self.shared;
        let _ = s.refresh().and_then(|()| s.load_pending_words());
        let words = s.words.read().expect("word store lock poisoned");
        if let Some(table) = &*s.word_table.read().expect("word table lock poisoned") {
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
        let s = &self.shared;
        let manifest = read_manifest(&s.dir)?;
        read_heap(&s.dir.join("heap.bin"), manifest.heap_bytes, manifest.heap_words)?;
        for frame in s.frames.read().expect("frame list lock poisoned").iter() {
            let segments = frame.state.read().expect("frame lock poisoned").segments.clone();
            for segment in segments.iter() {
                segment.reader.verify()?;
                if let Some(index) = &segment.index {
                    index.verify()?;
                }
            }
        }
        if let Some(table) = &*s.word_table.read().expect("word table lock poisoned") {
            table.verify()?;
        }
        Ok(())
    }

    /// Size in bytes of the files of the store (manifest, heap and segments). The pending buffer is not counted.
    pub fn disk_bytes(&self) -> Result<u64> {
        let mut total = 0;
        for entry in std::fs::read_dir(&self.shared.dir)? {
            let entry = entry?;
            let name = entry.file_name().to_string_lossy().into_owned();
            if name == "manifest" || name == "heap.bin" || name.ends_with(".seg") || name.ends_with(".idx") {
                total += entry.metadata()?.len();
            }
        }
        Ok(total)
    }
}

impl Shared {
    fn is_writable(&self) -> bool {
        self.writable.load(Ordering::Acquire)
    }

    /// The manifest of the committed state with `frames` (used when a dataframe is added between commits).
    fn manifest_now(&self, committed: &Committed, frames: &[Arc<Frame>]) -> Manifest {
        Manifest {
            sealed: committed.sealed,
            heap_bytes: committed.heap_bytes,
            heap_words: committed.heap_words,
            frames: frames
                .iter()
                .map(|frame| {
                    let state = frame.state.read().expect("frame lock poisoned");
                    FrameMeta {
                        name: frame.name.clone(),
                        schema: frame.schema.clone(),
                        segments: state.segments.iter().map(|s| SegmentMeta { file: s.file.clone(), rows: s.rows }).collect(),
                        next_segment: state.next_segment,
                    }
                })
                .collect(),
            epoch: committed.epoch,
            store_id: committed.store_id,
            mapped: committed.mapped,
            pending_dir: committed.pending_dir.clone(),
        }
    }

    /// Commits (DESIGN.md 7 and 14.6): new words, one segment per dataframe with pending rows, the empty pending buffers of
    /// the next epoch, then the manifest; then the old epoch is marked as superseded and its files are deleted.
    fn commit(&self, seal: bool) -> Result<()> {
        if !self.is_writable() {
            return Err(Error::ReadOnly);
        }
        let _write = self.write_mutex.lock().expect("write mutex poisoned");
        let mut committed = self.committed.lock().expect("commit lock poisoned");
        let frames = self.frames.read().expect("frame list lock poisoned");
        let mut guards: Vec<_> = frames.iter().map(|f| f.state.write().expect("frame lock poisoned")).collect();

        let (heap_bytes, heap_words, word_bytes) = {
            let words = self.words.read().expect("word store lock poisoned");
            let count = words.word_count();
            let o = &self.options;
            let path = self.dir.join("heap.bin");
            let bytes = append_heap(&path, committed.heap_bytes, &words, committed.heap_words, count, o.heap_block_bytes, o.zstd_level)?;
            (bytes, count, words.heap().byte_len())
        };

        let mut written: Vec<(usize, String, u64)> = Vec::new();
        let mut failure = None;
        for (i, (frame, guard)) in frames.iter().zip(&guards).enumerate() {
            let rows = match guard.pending.rows() {
                Ok(rows) => rows,
                Err(e) => {
                    failure = Some(e);
                    break;
                }
            };
            if rows == 0 {
                continue;
            }
            let file = format!("f{}-{:06}.seg", frame.id, guard.next_segment);
            match guard.pending.write_segment(&self.dir, &file, &frame.schema, &self.words, &self.options) {
                Ok(()) => written.push((i, file, rows)),
                Err(e) => {
                    let _ = std::fs::remove_file(self.dir.join(&file));
                    let _ = std::fs::remove_file(self.dir.join(file.replace(".seg", ".idx")));
                    failure = Some(e);
                    break;
                }
            }
        }

        // the pending buffers of the next epoch
        let store_id = if committed.store_id == 0 { new_store_id(&self.dir) } else { committed.store_id };
        let next = committed.epoch + 1;
        let mapped = !seal && self.options.mapped();
        let mut pending_text = String::new();
        let mut next_epoch = None;
        let mut next_pending = Vec::new();
        if failure.is_none() {
            let made = (|| -> Result<_> {
                pending_text = if mapped { self.options.pending_dir_text()? } else { String::new() };
                let dir = pending_dir(&self.dir, &pending_text);
                let epoch = if mapped { Some(Epoch::create(&dir, store_id, next, heap_words, self.options.chunk_sizes())?) } else { None };
                let pending = frames
                    .iter()
                    .map(|f| Pending::open(epoch.as_ref(), f.id, &f.schema, &self.words, &self.options).map(Arc::new))
                    .collect::<Result<Vec<_>>>()?;
                Ok((epoch, pending))
            })();
            match made {
                Ok((epoch, pending)) => (next_epoch, next_pending) = (epoch, pending),
                Err(e) => failure = Some(e),
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
            epoch: next,
            store_id,
            mapped,
            pending_dir: pending_text.clone(),
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
            drop(next_pending);
            if let Some(epoch) = next_epoch {
                let dir = epoch.dir().to_path_buf();
                drop(epoch);
                remove_epoch(&dir, store_id, next);
            }
            return Err(e);
        }

        // The manifest is replaced: the commit happened. Now switch the in-memory state over.
        for (i, file, rows) in written {
            let guard = &mut guards[i];
            let meta = SegmentMeta { file, rows };
            let segment = open_segment(&self.dir, &meta, frames[i].schema.len(), guard.committed_rows, &self.options, &self.segment_files)?;
            let mut segments: Vec<Arc<Segment>> = guard.segments.iter().cloned().collect();
            segments.push(Arc::new(segment));
            guard.segments = Arc::new(segments);
            guard.committed_rows += rows;
            guard.next_segment += 1;
        }
        for (i, guard) in guards.iter_mut().enumerate() {
            // a sealed store gets empty memory buffers: no more rows come
            guard.pending = match next_pending.get(i) {
                Some(pending) => pending.clone(),
                None => Arc::new(Pending::memory(&frames[i].schema, &self.words, &self.options)),
            };
        }
        drop(guards);
        let old_epoch = std::mem::replace(&mut *self.epoch.write().expect("epoch lock poisoned"), next_epoch.clone());
        *committed = Committed::from_manifest(&manifest, word_bytes);
        if seal {
            let path = self.dir.join("words.idx");
            let _ = std::fs::remove_file(&path);
            WordTable::write(&path, &self.words.read().expect("word store lock poisoned"))?;
            *self.word_table.write().expect("word table lock poisoned") = Some(WordTable::open(&path)?);
            self.writable.store(false, Ordering::Release);
        }
        // Readers still on the old epoch move on at their next query; then its files go (later on Windows if a reader
        // still maps them: retried at the next commit and open).
        if let Some(old) = old_epoch {
            old.set_superseded_by(if seal { SEALED } else { next });
            let dir = old.dir().to_path_buf();
            drop(old);
            let keep = next_epoch.as_ref().filter(|e| e.dir() == dir).map(|e| e.number());
            let _ = remove_other_epochs(&dir, store_id, keep, next);
        }
        if let Some(epoch) = &next_epoch {
            let _ = remove_other_epochs(epoch.dir(), store_id, Some(next), next);
        }
        Ok(())
    }

    /// Whether this is a read-only store that follows a writer with a mapped pending buffer.
    fn follows_writer(&self) -> bool {
        if self.is_writable() {
            return false;
        }
        let committed = self.committed.lock().expect("commit lock poisoned");
        committed.mapped && !committed.sealed
    }

    /// Read-only stores: picks up a commit of the writer (DESIGN.md 14.7). Costs a few atomic loads when nothing changed.
    fn refresh(&self) -> Result<()> {
        if !self.follows_writer() {
            return Ok(());
        }
        let epoch = self.epoch.read().expect("epoch lock poisoned").clone();
        let changed = match &epoch {
            Some(epoch) => epoch.superseded_by() != 0,
            // the pending files were missing: a writer that reopens the store creates them again
            None => {
                let committed = self.committed.lock().expect("commit lock poisoned");
                let dir = pending_dir(&self.dir, &committed.pending_dir);
                crate::pending::epoch_exists(&dir, committed.store_id, committed.epoch)
            }
        };
        if changed { self.reload() } else { Ok(()) }
    }

    /// Read-only stores: loads the words of pending rows published since the last call (cheap when there are none).
    fn load_pending_words(&self) -> Result<()> {
        if self.is_writable() {
            return Ok(());
        }
        let has_new = self.epoch.read().expect("epoch lock poisoned").as_ref().is_some_and(|e| e.has_new_words());
        if has_new {
            // the word store first, then the epoch: a reload swaps the epoch while it holds the word store
            let mut words = self.words.write().expect("word store lock poisoned");
            if let Some(epoch) = self.epoch.read().expect("epoch lock poisoned").clone() {
                epoch.load_words(&mut words)?;
            }
        }
        Ok(())
    }

    /// Read-only stores: re-reads the manifest and takes over what changed: new words, new segments, new dataframes and
    /// the pending buffer of a new epoch.
    fn reload(&self) -> Result<()> {
        let _reload = self.reload_lock.lock().expect("reload lock poisoned");
        let manifest = read_manifest(&self.dir)?;
        let current = self.epoch.read().expect("epoch lock poisoned").clone();
        let same_epoch = current.as_ref().is_some_and(|e| e.number() == manifest.epoch && e.superseded_by() == 0);
        let epoch = if same_epoch {
            current
        } else if manifest.mapped && !manifest.sealed {
            let dir = pending_dir(&self.dir, &manifest.pending_dir);
            Epoch::open(&dir, manifest.store_id, manifest.epoch, manifest.heap_words, self.options.chunk_sizes(), false)?
        } else {
            None
        };
        let mut frames = self.frames.write().expect("frame list lock poisoned");
        for (id, meta) in manifest.frames.iter().enumerate() {
            let is_new = id >= frames.len();
            let frame = match frames.get(id) {
                Some(frame) => frame.clone(),
                None => {
                    let state = FrameState {
                        segments: Arc::new(Vec::new()),
                        committed_rows: 0,
                        next_segment: 0,
                        pending: Arc::new(Pending::memory(&meta.schema, &self.words, &self.options)),
                    };
                    let frame = Arc::new(Frame {
                        id,
                        name: meta.name.clone(),
                        schema: meta.schema.clone(),
                        words: self.words.clone(),
                        writable: self.writable.clone(),
                        write_mutex: self.write_mutex.clone(),
                        state: RwLock::new(state),
                        shared: OnceLock::new(),
                    });
                    if let Some(me) = self.me.get() {
                        let _ = frame.shared.set(me.clone());
                    }
                    frames.push(frame.clone());
                    frame
                }
            };
            let mut state = frame.state.write().expect("frame lock poisoned");
            if meta.segments.len() < state.segments.len() {
                return Err(Error::Corrupt("the manifest lost segments"));
            }
            if meta.segments.len() > state.segments.len() {
                let mut segments: Vec<Arc<Segment>> = state.segments.iter().cloned().collect();
                let mut start = state.committed_rows;
                for segment in &meta.segments[segments.len()..] {
                    segments.push(Arc::new(open_segment(
                        &self.dir,
                        segment,
                        meta.schema.len(),
                        start,
                        &self.options,
                        &self.segment_files,
                    )?));
                    start += segment.rows;
                }
                state.segments = Arc::new(segments);
                state.committed_rows = start;
            }
            state.next_segment = meta.next_segment;
            // a new epoch, a new dataframe, or one whose pending files did not exist yet when it was opened
            let reopen = !same_epoch || is_new || (epoch.is_some() && matches!(state.pending.buffer, Buffer::Memory(_)));
            if reopen {
                state.pending = Arc::new(Pending::open(epoch.as_ref(), id, &meta.schema, &self.words, &self.options)?);
            }
        }
        if manifest.sealed {
            let mut table = self.word_table.write().expect("word table lock poisoned");
            let path = self.dir.join("words.idx");
            if table.is_none() && path.exists() {
                *table = Some(WordTable::open(&path)?);
            }
        }
        // The words committed since they were loaded, and the switch to the new epoch, under the word store lock (see
        // load_pending_words).
        let mut words = self.words.write().expect("word store lock poisoned");
        let mut word_bytes = self.committed.lock().expect("commit lock poisoned").word_bytes;
        if !same_epoch {
            if words.word_count() < manifest.heap_words {
                let heap = read_heap(&self.dir.join("heap.bin"), manifest.heap_bytes, manifest.heap_words)?;
                for ordinal in words.word_count()..manifest.heap_words {
                    let word = heap.get(ordinal).ok_or(Error::Corrupt("missing word"))?;
                    if words.intern(word)? != ordinal {
                        return Err(Error::Corrupt("heap word ordinal"));
                    }
                }
            } else if words.word_count() > manifest.heap_words {
                return Err(Error::Corrupt("the word store has more words than the heap"));
            }
            word_bytes = words.heap().byte_len();
        }
        *self.epoch.write().expect("epoch lock poisoned") = epoch;
        *self.committed.lock().expect("commit lock poisoned") = Committed::from_manifest(&manifest, word_bytes);
        Ok(())
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dataframe::decode_value;

    fn options() -> StoreOptions {
        StoreOptions {
            zstd_level: 3,
            block_rows: 16,
            row_heap_block_bytes: 256,
            heap_block_bytes: 512,
            // small chunk files, so the tests cross many chunks
            pending_chunk_min_bytes: 4096,
            pending_chunk_max_bytes: 16384,
            ..StoreOptions::default()
        }
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
    fn uncommitted_rows_are_readable_but_lost_on_close_in_memory_mode() {
        let dir = tempfile::tempdir().unwrap();
        let options = || StoreOptions { pending: PendingMode::Memory, ..options() };
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
        assert_eq!(store.recovery(), RecoveryReport::default());
    }

    #[cfg(feature = "mmap")]
    #[test]
    fn uncommitted_rows_survive_a_close_in_mapped_mode() {
        let dir = tempfile::tempdir().unwrap();
        let expected = {
            let store = Store::create(dir.path(), options()).unwrap();
            let frame = store.create_frame("logs", schema()).unwrap();
            for id in 0..10 {
                push(&frame, id);
            }
            store.commit().unwrap();
            for id in 10..15 {
                push(&frame, id);
            }
            all_rows(&frame)
        };
        let store = Store::open(dir.path(), options()).unwrap();
        assert_eq!(store.recovery(), RecoveryReport { pending_rows: 5, pending_files_missing: false });
        let frame = store.frame("logs").unwrap();
        assert_eq!((frame.len(), frame.pending_rows()), (15, 5));
        assert_eq!(all_rows(&frame), expected);
        // writing goes on after the recovered rows
        push(&frame, 15);
        store.commit().unwrap();
        drop(store);
        let frame = Store::open_read_only(dir.path(), options()).unwrap().frame("logs").unwrap();
        assert_eq!((frame.len(), frame.pending_rows()), (16, 0));
        assert_eq!(&all_rows(&frame)[..15], &expected[..]);
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
        let options = || StoreOptions { pending: PendingMode::Memory, ..options() };
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

    /// A child process forked by another thread holds a copy of the lock file until it executes; dropping a store must
    /// release the lock for that copy too, so an immediate reopen never fails with `Error::Locked`.
    #[cfg(unix)]
    #[test]
    fn reopening_while_another_thread_spawns_children_is_never_locked() {
        let dir = tempfile::tempdir().unwrap();
        let options = || StoreOptions { pending: PendingMode::Memory, ..options() };
        drop(Store::create(dir.path(), options()).unwrap());
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let spawner = {
            let stop = Arc::clone(&stop);
            std::thread::spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    let _ = std::process::Command::new("true").status();
                }
            })
        };
        let mut locked = 0;
        for _ in 0..1000 {
            match Store::open(dir.path(), options()) {
                Ok(store) => drop(store),
                Err(Error::Locked) => locked += 1,
                Err(e) => panic!("{e}"),
            }
        }
        stop.store(true, Ordering::Relaxed);
        spawner.join().unwrap();
        assert_eq!(locked, 0, "spurious Error::Locked on reopen");
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
        ground_truth_in_every_state(options());
    }

    #[test]
    fn search_matches_the_ground_truth_in_every_state_in_memory_mode() {
        ground_truth_in_every_state(StoreOptions { pending: PendingMode::Memory, ..options() });
    }

    /// The ground-truth queries on the writer's frames and, with a mapped buffer, on a read-only store that follows the
    /// writer: pending rows only, one segment, a segment plus pending rows, and after sealing.
    fn ground_truth_in_every_state(options: StoreOptions) {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::create(dir.path(), options.clone()).unwrap();
        let (indexed, plain) =
            (store.create_frame("indexed", search_schema(true)).unwrap(), store.create_frame("plain", search_schema(false)).unwrap());
        let reader = Store::open_read_only(dir.path(), options.clone()).unwrap();
        let live = options.mapped();
        let check_reader = |label: &str| {
            if live {
                check_search(&reader.frame("indexed").unwrap(), &format!("reader, {label}"));
                check_search(&reader.frame("plain").unwrap(), &format!("reader plain, {label}"));
            }
        };
        let push = |range: std::ops::Range<u64>| {
            for i in range {
                push_search_row(&indexed, i);
                push_search_row(&plain, i);
            }
        };
        push(0..60); // everything still in the buffer
        check_search(&indexed, "indexed, buffer");
        check_search(&plain, "plain, buffer");
        check_reader("buffer");
        assert_eq!(reader.frame("indexed").unwrap().len(), if live { 60 } else { 0 });
        store.commit().unwrap();
        check_search(&indexed, "indexed, segment");
        check_search(&plain, "plain, segment");
        check_reader("segment");
        push(60..100); // a segment plus a buffer
        check_search(&indexed, "indexed, segment+buffer");
        check_search(&plain, "plain, segment+buffer");
        check_reader("segment+buffer");
        store.commit().unwrap();
        push(100..130);
        check_reader("two segments+buffer");
        store.commit().unwrap();
        store.seal().unwrap();
        check_reader("sealed");
        if live {
            assert_eq!(reader.frame("indexed").unwrap().len(), 130);
            assert!(!reader.words_with_prefix("req", 5).is_empty());
            assert!(reader.shared.word_table.read().unwrap().is_some(), "the reader took over the word table of the seal");
        }
        drop(store);
        let store = Store::open_read_only(dir.path(), options).unwrap();
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

    /// Open `*.seg` files of this process inside `dir`, counted from `/proc/self/fd`.
    #[cfg(target_os = "linux")]
    fn seg_fds(dir: &Path) -> usize {
        std::fs::read_dir("/proc/self/fd")
            .unwrap()
            .flatten()
            .filter_map(|e| std::fs::read_link(e.path()).ok())
            .filter(|t| t.starts_with(dir) && t.extension().is_some_and(|x| x == "seg"))
            .count()
    }

    fn msg_of(id: u64) -> String {
        format!("request {} from host-{} finished with status {}", id % 13, id % 5, 200 + id % 3)
    }

    /// Everything the read API returns for a frame, as text: rows, single values, strings, searches, counts and pages.
    /// `check` runs after every read.
    fn read_everything(frame: &Frame, check: &dyn Fn()) -> Vec<String> {
        let mut out = Vec::new();
        for row in frame.rows() {
            out.push(format!("{:?}", row.unwrap()));
            check();
        }
        let mut buf = BytesMut::new();
        for row in (0..frame.len()).rev().step_by(7) {
            for column in 0..frame.schema().len() {
                out.push(format!("{:?}", frame.value(row, column).unwrap()));
            }
            buf.clear();
            let present = frame.write_string(row, 2, &mut buf).unwrap();
            out.push(format!("{present} {:?}", &buf[..]));
            check();
        }
        let queries = [
            Query::new().word(1, "host"),
            Query::new().word(1, "request").equals(0, Value::U64(17)),
            Query::new().phrase(1, "from host-3 finished"),
            Query::new().word_ci(1, "STATUS").range(0, Some(Value::U64(100)), Some(Value::U64(400))),
            Query::new().range(4, Some(Value::F64(10.0)), Some(Value::F64(60.0))),
            Query::new().phrase(2, "line one\nline 4"),
        ];
        for q in &queries {
            let rows: Vec<u64> = frame.search(q).unwrap().map(|r| r.unwrap()).collect();
            out.push(format!("{rows:?} {}", frame.count(q).unwrap()));
            check();
            for order_by in [None, Some(OrderBy { column: 0, descending: true }), Some(OrderBy { column: 4, descending: false })] {
                let page = frame.search_page(q, &Page { order_by, offset: 3, limit: 25 }).unwrap();
                out.push(format!("{page:?}"));
                check();
            }
        }
        out
    }

    #[test]
    fn many_segments_read_the_same_with_few_open_files() {
        const SEGMENTS: u64 = 200;
        const ROWS: u64 = 4;
        const LIMIT: usize = 8;
        let dir = tempfile::tempdir().unwrap();
        let store = Store::create(dir.path(), options()).unwrap();
        assert_eq!(store.max_open_segments(), 64);
        store.set_max_open_segments(LIMIT);
        let frame = store.create_frame("events", schema()).unwrap();
        let bounded = |store: &Store| {
            assert!(store.open_segment_files() <= LIMIT, "{} open", store.open_segment_files());
            #[cfg(target_os = "linux")]
            assert!(seg_fds(dir.path()) <= LIMIT, "{} descriptors", seg_fds(dir.path()));
        };
        for s in 0..SEGMENTS {
            for i in 0..ROWS {
                push(&frame, s * ROWS + i);
            }
            store.commit().unwrap();
            bounded(&store);
            // read across every segment so far now and then, while committing
            if s % 50 == 49 {
                let q = Query::new().word(1, "request");
                assert_eq!(frame.count(&q).unwrap(), (s + 1) * ROWS);
                bounded(&store);
            }
        }
        assert_eq!(frame.len(), SEGMENTS * ROWS);
        #[cfg(target_os = "linux")]
        assert!(seg_fds(dir.path()) <= LIMIT);
        let written = read_everything(&frame, &|| bounded(&store));
        for row in 0..frame.len() {
            assert_eq!(frame.value(row, 0).unwrap(), OwnedValue::U64(row));
        }
        drop((frame, store));

        // the reference: a store that keeps every file open
        let wide = Store::open_read_only(dir.path(), options()).unwrap();
        wide.set_max_open_segments(usize::MAX);
        let expected = read_everything(&wide.frame("events").unwrap(), &|| {});
        assert_eq!(wide.open_segment_files(), SEGMENTS as usize, "nothing was closed");
        #[cfg(target_os = "linux")]
        assert_eq!(seg_fds(dir.path()), SEGMENTS as usize);
        drop(wide);
        #[cfg(target_os = "linux")]
        assert_eq!(seg_fds(dir.path()), 0, "dropping the store closes its files");

        let narrow = Store::open_read_only(dir.path(), options()).unwrap();
        assert!(narrow.open_segment_files() <= 64);
        narrow.set_max_open_segments(LIMIT);
        let read = read_everything(&narrow.frame("events").unwrap(), &|| bounded(&narrow));
        assert_eq!(written, expected);
        assert_eq!(read, expected);
        narrow.verify().unwrap();
        bounded(&narrow);
        narrow.set_max_open_segments(1);
        assert!(narrow.open_segment_files() <= 1);
        assert_eq!(read_everything(&narrow.frame("events").unwrap(), &|| assert!(narrow.open_segment_files() <= 1)), expected);
    }

    /// Readers on several threads while another thread pushes and commits, with fewer open files allowed than segments:
    /// files are closed and reopened under the readers' feet, and every read must still be right.
    #[test]
    fn concurrent_reads_and_commits_with_few_open_files() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(Store::create(dir.path(), options()).unwrap());
        store.set_max_open_segments(3);
        let frame = store.create_frame("events", schema()).unwrap();
        for id in 0..60 {
            push(&frame, id);
            if id % 3 == 2 {
                store.commit().unwrap();
            }
        }
        let stop = Arc::new(AtomicBool::new(false));
        let (done_tx, done_rx) = std::sync::mpsc::channel::<&'static str>();
        let mut threads = Vec::new();
        {
            let (store, tx) = (store.clone(), done_tx.clone());
            threads.push(std::thread::spawn(move || {
                let frame = store.frame("events").unwrap();
                for id in 60..600u64 {
                    assert_eq!(push(&frame, id), id);
                    if id % 4 == 3 {
                        store.commit().unwrap();
                    }
                }
                store.commit().unwrap();
                tx.send("writer").unwrap();
            }));
        }
        for n in 0..4u64 {
            let (store, stop) = (store.clone(), stop.clone());
            threads.push(std::thread::spawn(move || {
                let frame = store.frame("events").unwrap();
                let mut buf = BytesMut::new();
                let mut x = n + 1;
                while !stop.load(Ordering::Relaxed) {
                    let len = frame.len();
                    for _ in 0..20 {
                        x = x.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
                        let row = (x >> 33) % len;
                        assert_eq!(frame.value(row, 0).unwrap(), OwnedValue::U64(row));
                        buf.clear();
                        assert!(frame.write_string(row, 1, &mut buf).unwrap());
                        assert_eq!(&buf[..], msg_of(row).as_bytes());
                    }
                    let k = (x >> 20) % len;
                    let q = Query::new().equals(0, Value::U64(k));
                    assert_eq!(frame.search(&q).unwrap().map(|r| r.unwrap()).collect::<Vec<_>>(), vec![k]);
                    let q = Query::new().range(0, Some(Value::U64(0)), Some(Value::U64(len - 1)));
                    assert!(frame.count(&q).unwrap() >= len);
                }
            }));
        }
        let r = done_rx.recv_timeout(std::time::Duration::from_secs(120));
        stop.store(true, Ordering::Relaxed);
        assert_eq!(r, Ok("writer"), "reads and commits deadlocked");
        for t in threads {
            t.join().unwrap();
        }
        assert_eq!(frame.len(), 600);
        assert!(store.open_segment_files() <= 3);
        let rows = all_rows(&frame);
        for (id, row) in rows.iter().enumerate() {
            assert_eq!(row[0], OwnedValue::U64(id as u64));
        }
        drop((frame, store));
        let wide = Store::open_read_only(dir.path(), options()).unwrap();
        wide.set_max_open_segments(usize::MAX);
        assert_eq!(all_rows(&wide.frame("events").unwrap()), rows);
    }

    // -----------------------------------------------------------------------------------------------------------------
    // Version 2: mapped pending buffer, live readers, recovery, commit policy (DESIGN.md 14)

    #[cfg(feature = "mmap")]
    /// Rows that exercise the row heap: repeated strings (dedup), long strings that cross chunk files, blobs and nulls.
    fn push_varied(frame: &Frame, id: u64) {
        let msg = match id % 5 {
            0 => format!("long {} {}", id, "lorem ipsum dolor sit amet ".repeat(200)),
            1 | 2 => "the same message every time".to_owned(),
            _ => format!("request {} from host-{} took {} ms", id % 13, id % 5, id * 7),
        };
        let data = format!(r#"{{"id":{id},"text":"line\n{}"}}"#, id % 4);
        let blob = vec![id as u8; (id % 3) as usize * 700];
        let score = if id % 4 == 0 { Value::Null } else { Value::F64(id as f64) };
        let msg = if id % 11 == 0 { Value::Null } else { Value::Str(&msg) };
        frame.push_row(&[Value::U64(id), msg, Value::Str(&data), Value::Blob(&blob), score]).unwrap();
    }

    #[cfg(feature = "mmap")]
    /// The bytes of every file of a store that a commit writes (heap, segments, indexes).
    fn committed_files(dir: &Path) -> Vec<(String, Vec<u8>)> {
        let mut files: Vec<(String, Vec<u8>)> = std::fs::read_dir(dir)
            .unwrap()
            .flatten()
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .filter(|n| n == "heap.bin" || n.ends_with(".seg") || n.ends_with(".idx"))
            .map(|n| (n.clone(), std::fs::read(dir.join(&n)).unwrap()))
            .collect();
        files.sort();
        files
    }

    #[cfg(feature = "mmap")]
    fn pending_files(dir: &Path) -> Vec<String> {
        let mut names: Vec<String> = std::fs::read_dir(dir)
            .unwrap()
            .flatten()
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .filter(|n| n.starts_with("p-"))
            .collect();
        names.sort();
        names
    }

    #[cfg(feature = "mmap")]
    #[test]
    fn mapped_and_memory_buffers_write_identical_segments_even_after_recovery() {
        let indexed = || {
            Schema::new(vec![
                Column::new("id", ColumnType::U64).index_equality(),
                Column::new("msg", ColumnType::String).index_words().case_insensitive(),
                Column::new("data", ColumnType::String).json_escapes().index_words(),
                Column::new("blob", ColumnType::Blob),
                Column::new("score", ColumnType::F64),
            ])
            .unwrap()
        };
        let write = |mode: PendingMode, restarts: &[u64]| {
            let dir = tempfile::tempdir().unwrap();
            let options = StoreOptions { pending: mode, ..options() };
            let store = Store::create(dir.path(), options.clone()).unwrap();
            store.create_frame("logs", indexed()).unwrap();
            drop(store);
            let mut from = 0;
            let mut store = None;
            for &to in restarts.iter().chain([&300]) {
                // each writer but the last ends without committing: the next one recovers the pending rows
                drop(store.take());
                let next = Store::open(dir.path(), options.clone()).unwrap();
                assert_eq!(next.recovery().pending_rows, from);
                let frame = next.frame("logs").unwrap();
                for id in from..to {
                    push_varied(&frame, id);
                }
                from = to;
                store = Some(next);
            }
            let store = store.unwrap();
            store.commit().unwrap();
            (committed_files(dir.path()), all_rows(&store.frame("logs").unwrap()), dir)
        };
        let (memory, rows, _a) = write(PendingMode::Memory, &[]);
        let (mapped, mapped_rows, _b) = write(PendingMode::Mapped, &[]);
        assert_eq!(mapped_rows, rows);
        assert!(memory.iter().any(|(n, _)| n.ends_with(".idx")));
        assert_eq!(mapped, memory, "the segment, index and heap files are byte for byte the same");
        // recovery rebuilds the dedup map and the row-heap blocks exactly
        let (recovered, recovered_rows, _c) = write(PendingMode::Mapped, &[1, 77, 150, 151, 299]);
        assert_eq!(recovered_rows, rows);
        assert_eq!(recovered, memory, "the same files after five restarts without commits");
    }

    #[cfg(feature = "mmap")]
    #[test]
    fn a_failed_row_in_mapped_mode_changes_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::create(dir.path(), options()).unwrap();
        let frame = store.create_frame("logs", schema()).unwrap();
        push(&frame, 0);
        let words = store.words().read().unwrap().word_count();
        let bad = frame.push_row(&[Value::U64(1), Value::Str("brand new words here"), Value::I32(3), Value::Null, Value::Null]);
        assert!(matches!(bad, Err(Error::TypeMismatch { .. })));
        assert!(matches!(frame.push_row(&[Value::U64(1)]), Err(Error::ColumnCount { .. })));
        assert_eq!((frame.len(), store.words().read().unwrap().word_count()), (1, words));
        push(&frame, 1);
        drop(store);
        let store = Store::open(dir.path(), options()).unwrap();
        let frame = store.frame("logs").unwrap();
        assert_eq!(frame.len(), 2);
        assert_eq!(frame.row(1).unwrap()[0], OwnedValue::U64(1));
    }

    #[cfg(feature = "mmap")]
    #[test]
    fn a_reader_follows_commits_new_frames_and_a_running_search_keeps_its_snapshot() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::create(dir.path(), options()).unwrap();
        let writer = store.create_frame("indexed", search_schema(true)).unwrap();
        let reader = Store::open_read_only(dir.path(), options()).unwrap();
        let frame = reader.frame("indexed").unwrap();
        for i in 0..40 {
            push_search_row(&writer, i);
        }
        // a search started now sees 40 rows, whatever happens while it runs
        let all = Query::new();
        let mut running = frame.search(&all).unwrap();
        let first: Vec<u64> = running.by_ref().take(5).map(|r| r.unwrap()).collect();
        store.commit().unwrap();
        for i in 40..50 {
            push_search_row(&writer, i);
        }
        let rest: Vec<u64> = running.map(|r| r.unwrap()).collect();
        assert_eq!([first, rest].concat(), (0..40).collect::<Vec<_>>());
        assert_eq!((frame.len(), frame.pending_rows()), (50, 10));
        check_search(&frame, "after a commit");
        assert_eq!(frame.row(45).unwrap(), writer.row(45).unwrap());
        // a dataframe created after the reader opened
        let late = store.create_frame("late", schema()).unwrap();
        push(&late, 7);
        assert_eq!(reader.frame_names(), ["indexed", "late"]);
        let seen = reader.frame("late").unwrap();
        assert_eq!(seen.row(0).unwrap(), late.row(0).unwrap());
        push(&late, 8);
        store.commit().unwrap();
        assert_eq!(seen.len(), 2);
        assert_eq!(seen.pending_rows(), 0);
        // words of pending rows are known to the reader
        let w = writer.row(0).unwrap();
        let OwnedValue::Str(text) = &w[2] else { panic!("text column") };
        let word = text.split(' ').next().unwrap();
        assert!(reader.lookup_word(word).is_some());
    }

    #[cfg(feature = "mmap")]
    #[test]
    fn commit_if_due_commits_by_rows_bytes_and_age() {
        let dir = tempfile::tempdir().unwrap();
        let policy = CommitPolicy { max_bytes: u64::MAX, max_age: None };
        let store = Store::create(dir.path(), StoreOptions { segment_rows: 10, commit_policy: policy, ..options() }).unwrap();
        let frame = store.create_frame("logs", schema()).unwrap();
        for id in 0..9 {
            push(&frame, id);
            assert_eq!(store.commit_if_due().unwrap(), None);
        }
        push(&frame, 9);
        assert_eq!(store.commit_if_due().unwrap(), Some(CommitTrigger::Rows));
        assert_eq!(frame.pending_rows(), 0);
        drop((frame, store));

        let by_bytes = StoreOptions { commit_policy: CommitPolicy { max_bytes: 4000, max_age: None }, ..options() };
        let store = Store::open(dir.path(), by_bytes).unwrap();
        let frame = store.frame("logs").unwrap();
        let mut id = 10;
        let trigger = loop {
            let before = store.pending_bytes();
            push(&frame, id);
            assert!(store.pending_bytes() > before);
            id += 1;
            if let Some(trigger) = store.commit_if_due().unwrap() {
                break trigger;
            }
        };
        assert_eq!(trigger, CommitTrigger::Bytes);
        assert_eq!(store.pending_bytes(), 0);
        drop((frame, store));

        let options =
            StoreOptions { commit_policy: CommitPolicy { max_bytes: u64::MAX, max_age: Some(Duration::from_millis(40)) }, ..options() };
        let store = Store::open(dir.path(), options.clone()).unwrap();
        assert_eq!(store.commit_if_due().unwrap(), None, "nothing pending");
        push(&store.frame("logs").unwrap(), 100);
        assert_eq!(store.commit_if_due().unwrap(), None, "too young");
        drop(store);
        std::thread::sleep(Duration::from_millis(60));
        // the age of the oldest pending row survives a restart
        let store = Store::open(dir.path(), options).unwrap();
        assert_eq!(store.commit_if_due().unwrap(), Some(CommitTrigger::Age));
        assert_eq!(store.commit_if_due().unwrap(), None);
        let reader = Store::open_read_only(dir.path(), StoreOptions::default()).unwrap();
        assert!(matches!(reader.commit_if_due(), Err(Error::ReadOnly)));
    }

    #[cfg(feature = "mmap")]
    #[test]
    fn a_format_1_store_is_read_and_upgraded() {
        let dir = tempfile::tempdir().unwrap();
        let memory = StoreOptions { pending: PendingMode::Memory, ..options() };
        {
            let store = Store::create(dir.path(), memory).unwrap();
            let frame = store.create_frame("logs", schema()).unwrap();
            for id in 0..20 {
                push(&frame, id);
            }
            store.commit().unwrap();
        }
        let v1 = read_manifest(dir.path()).unwrap().encode_with_version(1);
        std::fs::write(dir.path().join("manifest"), v1).unwrap();
        let expected = all_rows(&Store::open_read_only(dir.path(), options()).unwrap().frame("logs").unwrap());
        assert_eq!(expected.len(), 20);
        {
            let store = Store::open(dir.path(), options()).unwrap();
            let manifest = read_manifest(dir.path()).unwrap();
            assert!(manifest.mapped && manifest.store_id != 0 && manifest.epoch >= 1, "upgraded to format 2 with a mapped buffer");
            push(&store.frame("logs").unwrap(), 20);
        }
        let store = Store::open(dir.path(), options()).unwrap();
        assert_eq!(store.recovery().pending_rows, 1);
        let rows = all_rows(&store.frame("logs").unwrap());
        assert_eq!((&rows[..20], rows.len()), (&expected[..], 21));
    }

    #[cfg(feature = "mmap")]
    #[test]
    fn switching_the_pending_mode_commits_the_pending_rows() {
        let dir = tempfile::tempdir().unwrap();
        {
            let store = Store::create(dir.path(), options()).unwrap();
            let frame = store.create_frame("logs", schema()).unwrap();
            for id in 0..5 {
                push(&frame, id);
            }
        }
        assert!(!pending_files(dir.path()).is_empty());
        {
            let store = Store::open(dir.path(), StoreOptions { pending: PendingMode::Memory, ..options() }).unwrap();
            assert_eq!(store.recovery().pending_rows, 5);
            let frame = store.frame("logs").unwrap();
            assert_eq!((frame.len(), frame.pending_rows()), (5, 0), "committed when switching");
            assert!(pending_files(dir.path()).is_empty());
            push(&frame, 5);
            store.commit().unwrap();
        }
        let store = Store::open(dir.path(), options()).unwrap();
        assert!(!pending_files(dir.path()).is_empty());
        assert_eq!(store.frame("logs").unwrap().len(), 6);
    }

    #[cfg(feature = "mmap")]
    #[test]
    fn a_separate_pending_dir_and_lost_pending_files() {
        let dir = tempfile::tempdir().unwrap();
        let shm = tempfile::tempdir().unwrap();
        let options = StoreOptions { pending_dir: Some(shm.path().to_path_buf()), ..options() };
        {
            let store = Store::create(dir.path(), options.clone()).unwrap();
            let frame = store.create_frame("logs", schema()).unwrap();
            for id in 0..10 {
                push(&frame, id);
            }
            store.commit().unwrap();
            for id in 10..14 {
                push(&frame, id);
            }
        }
        assert!(pending_files(dir.path()).is_empty(), "nothing pending in the store folder");
        assert!(!pending_files(shm.path()).is_empty());
        // a reader finds the pending rows through the manifest, without being told the directory
        let reader = Store::open_read_only(dir.path(), StoreOptions::default()).unwrap();
        assert_eq!(reader.frame("logs").unwrap().len(), 14);
        drop(reader);

        // a reboot empties the tmpfs
        for name in pending_files(shm.path()) {
            std::fs::remove_file(shm.path().join(name)).unwrap();
        }
        let reader = Store::open_read_only(dir.path(), StoreOptions::default()).unwrap();
        let seen = reader.frame("logs").unwrap();
        assert_eq!(seen.len(), 10);
        let store = Store::open(dir.path(), options).unwrap();
        assert_eq!(store.recovery(), RecoveryReport { pending_rows: 0, pending_files_missing: true });
        let frame = store.frame("logs").unwrap();
        assert_eq!(frame.len(), 10);
        push(&frame, 10);
        // the reader picks up the pending files the writer created again
        assert_eq!(seen.len(), 11);
        assert_eq!(seen.row(10).unwrap(), frame.row(10).unwrap());
    }

    #[cfg(feature = "mmap")]
    #[test]
    fn old_epoch_files_are_deleted_and_retried() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::create(dir.path(), options()).unwrap();
        let frame = store.create_frame("logs", schema()).unwrap();
        push(&frame, 0);
        let id = read_manifest(dir.path()).unwrap().store_id;
        let epoch = read_manifest(dir.path()).unwrap().epoch;
        assert!(pending_files(dir.path()).iter().all(|n| n.starts_with(&crate::pending::epoch_prefix(id, epoch))));
        // a file of an older epoch that could not be deleted before (as on Windows while a reader maps it), and a file of
        // another store sharing the directory
        let stale = dir.path().join(format!("{}.f0.cells.0000", crate::pending::epoch_prefix(id, epoch - 1)));
        let other = dir.path().join(format!("{}.hdr", crate::pending::epoch_prefix(id ^ 1, 7)));
        std::fs::write(&stale, b"x").unwrap();
        std::fs::write(&other, b"x").unwrap();
        store.commit().unwrap();
        let epoch = read_manifest(dir.path()).unwrap().epoch;
        assert!(!stale.exists() && other.exists());
        let ours: Vec<String> =
            pending_files(dir.path()).into_iter().filter(|n| !n.starts_with(&crate::pending::epoch_prefix(id ^ 1, 7))).collect();
        assert!(ours.iter().all(|n| n.starts_with(&format!("{}.", crate::pending::epoch_prefix(id, epoch)))), "{ours:?}");
        store.seal().unwrap();
        let ours: Vec<String> = pending_files(dir.path()).into_iter().filter(|n| n.contains(&format!("{id:032x}"))).collect();
        assert!(ours.is_empty(), "a sealed store has no pending files: {ours:?}");
    }

    // ---- across processes: a child process writes, the test process reads

    #[cfg(feature = "mmap")]
    fn search_ids(frame: &Frame, query: &Query) -> Vec<u64> {
        frame.search(query).unwrap().map(|r| r.unwrap()).collect()
    }

    /// Entry point of the child processes of the cross-process tests; does nothing in a normal test run.
    #[cfg(feature = "mmap")]
    #[test]
    fn child_process_entry() {
        let Ok(spec) = std::env::var("LSQ_STORE_CHILD") else { return };
        let (role, dir) = spec.split_once(':').unwrap();
        let store = Store::open(Path::new(dir), options()).unwrap();
        let frame = store.frame("indexed").unwrap();
        match role {
            "write" => {
                for i in 0..450 {
                    push_search_row(&frame, i);
                    if i % 120 == 119 {
                        store.commit().unwrap();
                    }
                }
                store.seal().unwrap();
            }
            "abort" => {
                for i in 0..250 {
                    push_search_row(&frame, i);
                    if i == 199 {
                        store.commit().unwrap();
                    }
                }
                std::process::abort();
            }
            other => panic!("unknown child role {other}"),
        }
    }

    #[cfg(feature = "mmap")]
    fn spawn_child(role: &str, dir: &Path) -> std::process::Child {
        std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "store::tests::child_process_entry", "--test-threads=1", "--nocapture"])
            .env("LSQ_STORE_CHILD", format!("{role}:{}", dir.display()))
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .unwrap()
    }

    #[cfg(feature = "mmap")]
    #[test]
    fn a_reader_process_searches_while_a_writer_process_pushes_and_commits() {
        let dir = tempfile::tempdir().unwrap();
        drop(Store::create(dir.path(), options()).unwrap().create_frame("indexed", search_schema(true)).unwrap());
        let reader = Store::open_read_only(dir.path(), options()).unwrap();
        let frame = reader.frame("indexed").unwrap();
        let mut child = spawn_child("write", dir.path());
        type Rule = fn(u64) -> bool;
        let queries: Vec<(Query, Rule)> = vec![
            (Query::new().word(2, "timeout"), |i| i % 4 == 1),
            (Query::new().equals(1, Value::Str("WARN")), |i| i % 3 == 1),
            (Query::new().word_ci(2, "ERROR").range(0, Some(Value::I64(100)), None), |i| i % 3 == 2 && i >= 100),
        ];
        let deadline = std::time::Instant::now() + Duration::from_secs(120);
        let (mut checks, mut seen_partial) = (0, false);
        loop {
            assert!(std::time::Instant::now() < deadline, "the writer process did not finish");
            let finished = child.try_wait().unwrap().is_some();
            for (query, rule) in &queries {
                // rows are pushed in order: every match below the length before the search must be there, and nothing
                // past the length after it
                let before = frame.len();
                let got = search_ids(frame.as_ref(), query);
                let after = frame.len();
                assert!(got.windows(2).all(|w| w[0] < w[1]), "ascending, no duplicates");
                assert!(got.iter().all(|&i| rule(i) && i < after), "only real matches");
                let expected: Vec<u64> = (0..before).filter(|&i| rule(i)).collect();
                assert_eq!(&got[..expected.len().min(got.len())], &expected[..], "no match is missed");
                seen_partial |= before > 0 && before < 450;
                checks += 1;
            }
            if let Some(n) = (!frame.is_empty()).then(|| frame.len() - 1) {
                assert_eq!(frame.row(n).unwrap()[0], OwnedValue::I64(n as i64));
            }
            if finished {
                break;
            }
        }
        assert!(child.wait().unwrap().success());
        assert!(checks > 3 && seen_partial, "the reader saw the writer at work ({checks} checks)");
        assert_eq!(frame.len(), 450);
        check_search(&frame, "after the writer process sealed");
        assert!(!reader.is_writable());
    }

    #[cfg(feature = "mmap")]
    #[test]
    fn a_writer_process_killed_mid_way_loses_no_pushed_row() {
        let dir = tempfile::tempdir().unwrap();
        drop(Store::create(dir.path(), options()).unwrap().create_frame("indexed", search_schema(true)).unwrap());
        let status = spawn_child("abort", dir.path()).wait().unwrap();
        assert!(!status.success(), "the child aborts");
        // a reader sees every pushed row while no writer runs
        let reader = Store::open_read_only(dir.path(), options()).unwrap();
        let seen = reader.frame("indexed").unwrap();
        assert_eq!((seen.len(), seen.pending_rows()), (250, 50));
        check_search(&seen, "reader after the crash");
        let store = Store::open(dir.path(), options()).unwrap();
        assert_eq!(store.recovery(), RecoveryReport { pending_rows: 50, pending_files_missing: false });
        let frame = store.frame("indexed").unwrap();
        check_search(&frame, "recovered writer");
        for i in 250..260 {
            push_search_row(&frame, i);
        }
        store.commit().unwrap();
        check_search(&frame, "recovered and committed");
        check_search(&seen, "the reader followed");
    }
}
