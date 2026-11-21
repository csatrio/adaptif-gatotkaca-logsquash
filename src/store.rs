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
    /// Target size of a row-heap block in bytes (default 64 KiB).
    pub row_heap_block_bytes: usize,
    /// Target size of a heap block in bytes (default 1 MiB).
    pub heap_block_bytes: usize,
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
            row_heap_block_bytes: 64 * 1024,
            heap_block_bytes: 1 << 20,
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
}

struct FrameState {
    active: DataFrame<MemHeap>,
    segments: Vec<Segment>,
    committed_rows: u64,
    next_segment: u32,
}

/// One dataframe of a store: committed segments on disk plus the rows buffered since the last commit.
pub struct Frame {
    id: usize,
    name: String,
    schema: Schema,
    words: SharedWords,
    writable: Arc<AtomicBool>,
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

    /// An iterator over all rows in insertion order.
    pub fn rows(&self) -> FrameRows<'_> {
        FrameRows { frame: self, next: 0 }
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
    committed: Mutex<Committed>,
    frames: RwLock<Vec<Arc<Frame>>>,
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
            committed: Mutex::new(committed),
            frames: RwLock::new(Vec::new()),
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
            let known: Vec<&str> = manifest.frames.iter().flat_map(|f| f.segments.iter().map(|s| s.file.as_str())).collect();
            for entry in std::fs::read_dir(dir)?.flatten() {
                let name = entry.file_name().to_string_lossy().into_owned();
                if name == "manifest.tmp" || (name.ends_with(".seg") && !known.contains(&name.as_str())) {
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

        let mut frames = Vec::new();
        for (id, meta) in manifest.frames.iter().enumerate() {
            let mut segments = Vec::new();
            let mut start = 0;
            for segment in &meta.segments {
                let reader = SegmentReader::open(&dir.join(&segment.file), meta.schema.len())?;
                if reader.rows() != segment.rows {
                    return Err(Error::Corrupt("segment row count differs from the manifest"));
                }
                segments.push(Segment { file: segment.file.clone(), rows: segment.rows, start, reader });
                start += segment.rows;
            }
            let state = FrameState {
                active: new_active(&meta.schema, &words, &options),
                segments,
                committed_rows: start,
                next_segment: meta.next_segment,
            };
            frames.push(Arc::new(Frame {
                id,
                name: meta.name.clone(),
                schema: meta.schema.clone(),
                words: words.clone(),
                writable: flag.clone(),
                state: RwLock::new(state),
            }));
        }
        let committed = Committed { heap_bytes: manifest.heap_bytes, heap_words: manifest.heap_words, sealed: manifest.sealed };
        Ok(Self {
            dir: dir.to_path_buf(),
            options,
            words,
            writable: flag,
            committed: Mutex::new(committed),
            frames: RwLock::new(frames),
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
        let mut frames = self.frames.write().expect("frame list lock poisoned");
        if frames.iter().any(|f| f.name == name) {
            return Err(Error::FrameExists);
        }
        let state = FrameState {
            active: new_active(&schema, &self.words, &self.options),
            segments: Vec::new(),
            committed_rows: 0,
            next_segment: 0,
        };
        let frame = Arc::new(Frame {
            id: frames.len(),
            name: name.to_owned(),
            schema,
            words: self.words.clone(),
            writable: self.writable.clone(),
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
            let result = guard.active.with_inner(|inner| write_segment(&self.dir.join(&file), inner, write_options));
            match result {
                Ok(()) => written.push((i, file, guard.active.len())),
                Err(e) => {
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
            }
            return Err(e);
        }

        // The manifest is replaced: the commit happened. Now switch the in-memory state over.
        for (i, file, rows) in written {
            let guard = &mut guards[i];
            let reader = SegmentReader::open(&self.dir.join(&file), frames[i].schema.len())?;
            let start = guard.committed_rows;
            guard.segments.push(Segment { file, rows, start, reader });
            guard.committed_rows += rows;
            guard.next_segment += 1;
            guard.active.clear();
        }
        *committed = Committed { heap_bytes, heap_words, sealed: manifest.sealed };
        if seal {
            self.writable.store(false, Ordering::Release);
        }
        Ok(())
    }

    /// Size in bytes of the files of the store (manifest, heap and segments).
    pub fn disk_bytes(&self) -> Result<u64> {
        let mut total = 0;
        for entry in std::fs::read_dir(&self.dir)? {
            let entry = entry?;
            let name = entry.file_name().to_string_lossy().into_owned();
            if name == "manifest" || name == "heap.bin" || name.ends_with(".seg") {
                total += entry.metadata()?.len();
            }
        }
        Ok(total)
    }
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
