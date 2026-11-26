//! The mapped pending buffer of a store (DESIGN.md 14): the rows of every frame since the last commit, kept in
//! memory-mapped files that outlive a crash of the writer process and that readers in other processes can search.
//!
//! One **epoch** is the buffered state between two commits. Its files, in the pending directory (by default the store
//! folder), are named after the store id and the epoch number:
//!
//! ```text
//! p-<store id>-<epoch>.hdr               counters: superseded_by, words region, next epoch
//! p-<store id>-<epoch>.words.<nnnn>      words added to the heap since the last commit
//! p-<store id>-<epoch>.f<id>.ctr         counters of frame <id>: cells and row-heap regions, first push time
//! p-<store id>-<epoch>.f<id>.cells.<nnnn> row-major cells and null bits
//! p-<store id>-<epoch>.f<id>.rheap.<nnnn> row heap (symbol streams and blobs)
//! ```
//!
//! A row is published by storing the words length, then the row-heap length, then the cells length (14.4).

use crate::dataframe::{DataFrameOptions, Prepared, RowEncoder};
use crate::dictionary::xxh3;
use crate::error::{Error, Result};
pub(crate) use crate::mapped::ChunkSizes;
use crate::mapped::{CounterFile, Region, RegionView};
use crate::query::RowSource;
use crate::schema::Schema;
use crate::segment::SegmentInput;
use crate::sync::Unpoison;
use crate::types::Value;
use crate::varint;
use crate::words::{SharedWords, WordStore};
use bytes::BytesMut;
use std::borrow::Cow;
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

// Slots of the epoch header.
const SUPERSEDED: usize = 0;
const WORDS_LEN: usize = 1;
const WORDS_CHUNKS: usize = 2;
/// The epoch that receives the new rows while this one is being committed (DESIGN.md 14.17), or 0.
const NEXT: usize = 3;
const HEADER_SLOTS: usize = 8;
/// `superseded_by` of an epoch that ended because the store was sealed.
pub(crate) const SEALED: u64 = u64::MAX;

// Slots of a frame counter file.
const CELLS_LEN: usize = 0;
const CELLS_CHUNKS: usize = 1;
const RHEAP_LEN: usize = 2;
const RHEAP_CHUNKS: usize = 3;
const FIRST_PUSH: usize = 4;
const FRAME_SLOTS: usize = 8;

/// The common start of the file names of one epoch.
pub(crate) fn epoch_prefix(store_id: u128, epoch: u64) -> String {
    format!("p-{store_id:032x}-{epoch}")
}

fn store_prefix(store_id: u128) -> String {
    format!("p-{store_id:032x}-")
}

/// Milliseconds since the Unix epoch (0 if the clock is before it).
pub(crate) fn now_unix_ms() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map_or(0, |d| d.as_millis() as u64)
}

/// Bytes of one row in the cells region: the cells, then the null bits, padded to a multiple of 8.
fn stride(columns: usize) -> u64 {
    ((columns * 8 + columns.div_ceil(8)).div_ceil(8) * 8) as u64
}

/// The epochs of `store_id` that have files in `dir` (from the header files and every other file name).
fn epochs_in(dir: &Path, store_id: u128) -> Result<Vec<u64>> {
    let prefix = store_prefix(store_id);
    let mut epochs = Vec::new();
    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(e) if e.kind() == ErrorKind::NotFound => return Ok(epochs),
        Err(e) => return Err(e.into()),
    };
    for entry in entries.flatten() {
        let name = entry.file_name().to_string_lossy().into_owned();
        if let Some(rest) = name.strip_prefix(&prefix) {
            if let Some(Ok(epoch)) = rest.split('.').next().map(str::parse::<u64>) {
                if !epochs.contains(&epoch) {
                    epochs.push(epoch);
                }
            }
        }
    }
    epochs.sort_unstable();
    Ok(epochs)
}

/// Deletes the pending files of `store_id` in `dir`, except those of the epochs in `keep`. Before an epoch older than all
/// of `keep` is deleted, its header gets `superseded_by = superseded` if it has none yet (a commit interrupted after the
/// manifest was written), so readers still on it move on. Files that cannot be deleted (on Windows, while a reader maps
/// them) stay and are retried at the next call. Returns whether every file was deleted.
pub(crate) fn remove_other_epochs(dir: &Path, store_id: u128, keep: &[u64], superseded: u64) -> Result<bool> {
    let mut complete = true;
    let oldest_kept = keep.iter().copied().min();
    for epoch in epochs_in(dir, store_id)? {
        if keep.contains(&epoch) {
            continue;
        }
        let prefix = epoch_prefix(store_id, epoch);
        if oldest_kept.is_some_and(|kept| epoch < kept) {
            if let Ok(header) = CounterFile::open(&dir.join(format!("{prefix}.hdr")), true) {
                if header.load(SUPERSEDED) == 0 {
                    header.store(SUPERSEDED, superseded);
                }
            }
        }
        for entry in std::fs::read_dir(dir)?.flatten() {
            let name = entry.file_name().to_string_lossy().into_owned();
            let ours = name.strip_prefix(&prefix).is_some_and(|rest| rest.starts_with('.'));
            if ours {
                match std::fs::remove_file(entry.path()) {
                    Ok(()) => {}
                    Err(e) if e.kind() == ErrorKind::NotFound => {}
                    Err(_) => complete = false,
                }
            }
        }
    }
    Ok(complete)
}

/// Whether the header of an epoch exists (a read-only store whose pending files were missing checks for it).
pub(crate) fn epoch_exists(dir: &Path, store_id: u128, number: u64) -> bool {
    dir.join(format!("{}.hdr", epoch_prefix(store_id, number))).exists()
}

/// Deletes the files of one epoch, as far as possible (after a commit that failed before its manifest was written).
pub(crate) fn remove_epoch(dir: &Path, store_id: u128, number: u64) {
    let prefix = epoch_prefix(store_id, number);
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            let name = entry.file_name().to_string_lossy().into_owned();
            if name.strip_prefix(&prefix).is_some_and(|rest| rest.starts_with('.')) {
                let _ = std::fs::remove_file(entry.path());
            }
        }
    }
}

/// Progress of the words region: what the writer wrote and published, or what a reader loaded.
struct WordsProgress {
    /// Words written to the region (writer; equals `published` for a reader).
    written: u32,
    /// Words published (writer) or loaded into the word store (reader and recovery).
    published: u32,
    /// Bytes of the region loaded into the word store.
    loaded_bytes: u64,
}

/// The files of one epoch that are shared by all its frames: the header and the words region.
pub(crate) struct Epoch {
    number: u64,
    dir: PathBuf,
    prefix: String,
    header: Arc<CounterFile>,
    words: Region,
    /// Words in the heap when the epoch started; the words region holds the ordinals from here on.
    base_words: u32,
    sizes: ChunkSizes,
    writable: bool,
    progress: Mutex<WordsProgress>,
}

impl Epoch {
    /// Creates the empty epoch `number` (its header; regions get chunk files on first use). Leftover files of the same
    /// epoch (from a commit that failed before its manifest) are deleted first.
    pub(crate) fn create(dir: &Path, store_id: u128, number: u64, base_words: u32, sizes: ChunkSizes) -> Result<Arc<Self>> {
        std::fs::create_dir_all(dir)?;
        let prefix = epoch_prefix(store_id, number);
        for entry in std::fs::read_dir(dir)?.flatten() {
            let name = entry.file_name().to_string_lossy().into_owned();
            if name.strip_prefix(&prefix).is_some_and(|rest| rest.starts_with('.')) {
                std::fs::remove_file(entry.path())?;
            }
        }
        let header = Arc::new(CounterFile::create(&dir.join(format!("{prefix}.hdr")), HEADER_SLOTS)?);
        Self::with_header(dir, prefix, number, header, base_words, sizes, true)
    }

    /// Opens epoch `number`; `None` if its header does not exist (never created, or lost with a tmpfs directory).
    pub(crate) fn open(
        dir: &Path,
        store_id: u128,
        number: u64,
        base_words: u32,
        sizes: ChunkSizes,
        writable: bool,
    ) -> Result<Option<Arc<Self>>> {
        let prefix = epoch_prefix(store_id, number);
        let header = match CounterFile::open(&dir.join(format!("{prefix}.hdr")), writable) {
            Ok(header) => Arc::new(header),
            Err(Error::Io(e)) if e.kind() == ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e),
        };
        if header.slots() < HEADER_SLOTS {
            return Err(Error::Corrupt("epoch header slot count"));
        }
        Self::with_header(dir, prefix, number, header, base_words, sizes, writable).map(Some)
    }

    fn with_header(
        dir: &Path,
        prefix: String,
        number: u64,
        header: Arc<CounterFile>,
        base_words: u32,
        sizes: ChunkSizes,
        writable: bool,
    ) -> Result<Arc<Self>> {
        let words = Region::open(&dir.join(format!("{prefix}.words")), header.clone(), WORDS_LEN, WORDS_CHUNKS, sizes, writable)?;
        Ok(Arc::new(Self {
            number,
            dir: dir.to_path_buf(),
            prefix,
            header,
            words,
            base_words,
            sizes,
            writable,
            progress: Mutex::new(WordsProgress { written: 0, published: 0, loaded_bytes: 0 }),
        }))
    }

    /// The epoch number.
    pub(crate) fn number(&self) -> u64 {
        self.number
    }

    /// The directory of the epoch's files.
    pub(crate) fn dir(&self) -> &Path {
        &self.dir
    }

    /// 0 while this is the current epoch; the next epoch's number after a commit; [`SEALED`] after a seal.
    pub(crate) fn superseded_by(&self) -> u64 {
        self.header.load(SUPERSEDED)
    }

    /// Marks the epoch as ended (after the manifest that ends it was written).
    pub(crate) fn set_superseded_by(&self, next: u64) {
        self.header.store(SUPERSEDED, next);
    }

    /// The epoch that takes the new rows while this one is committed, or 0 (DESIGN.md 14.17). Once it is set, this
    /// epoch gets no more rows or words.
    pub(crate) fn next(&self) -> u64 {
        self.header.load(NEXT)
    }

    /// Writer: links the next epoch (created before), after the last row of this one.
    pub(crate) fn set_next(&self, next: u64) {
        self.header.store(NEXT, next);
    }

    /// Writer: appends the words of `words` that are not in the region yet (unpublished until [`Epoch::publish_words`]).
    fn mirror_words(&self, words: &WordStore) -> Result<()> {
        let mut progress = self.progress.lock().or_poisoned()?;
        let mut buf = BytesMut::new();
        while self.base_words + progress.written < words.word_count() {
            let word = words.get(self.base_words + progress.written).ok_or(Error::Corrupt("missing word"))?;
            buf.clear();
            varint::put(&mut buf, word.len() as u64);
            buf.extend_from_slice(word.as_bytes());
            self.words.append(&buf)?;
            progress.written += 1;
        }
        Ok(())
    }

    fn publish_words(&self) {
        let mut progress = self.progress.lock().ignore_poison();
        self.words.publish();
        progress.published = progress.written;
        progress.loaded_bytes = self.words.written_len();
    }

    fn rollback_words(&self) -> Result<()> {
        let mut progress = self.progress.lock().or_poisoned()?;
        progress.written = progress.published;
        self.words.rollback()
    }

    /// Whether the region has published words that were not loaded yet (a cheap check before taking the word store).
    pub(crate) fn has_new_words(&self) -> bool {
        self.words.published_len() > self.progress.lock().ignore_poison().loaded_bytes
    }

    /// Adds the published words that are not in `words` yet, with the ordinals the writer gave them (readers, and the
    /// writer when it recovers). `words` must hold exactly the words before them.
    pub(crate) fn load_words(&self, words: &mut WordStore) -> Result<()> {
        let mut progress = self.progress.lock().or_poisoned()?;
        let len = self.words.refresh()?;
        if progress.loaded_bytes >= len {
            return Ok(());
        }
        if words.word_count() != self.base_words + progress.published {
            return Err(Error::Corrupt("the word store and the pending words disagree"));
        }
        let view = self.words.view(len);
        while progress.loaded_bytes < view.len() {
            let slice = view.slice_from(progress.loaded_bytes)?;
            let mut pos = 0;
            let n = varint::get(slice, &mut pos)? as usize;
            let bytes = slice.get(pos..pos + n).ok_or(Error::Corrupt("pending word length"))?;
            let word = std::str::from_utf8(bytes).map_err(|_| Error::Corrupt("pending word is not UTF-8"))?;
            if words.intern(word)? != self.base_words + progress.published {
                return Err(Error::Corrupt("pending word ordinal"));
            }
            progress.loaded_bytes += (pos + n) as u64;
            progress.published += 1;
            progress.written = progress.published;
        }
        Ok(())
    }

    fn frame_paths(&self, id: usize) -> (PathBuf, PathBuf, PathBuf) {
        let base = self.dir.join(format!("{}.f{id}", self.prefix));
        let with = |ext: &str| {
            let mut name = base.as_os_str().to_owned();
            name.push(ext);
            PathBuf::from(name)
        };
        (with(".ctr"), with(".cells"), with(".rheap"))
    }

    /// Opens the buffer of frame `id` in this epoch. A writer creates its files if they are missing and recovers the
    /// published rows (14.8); a reader gets `None` when the files do not exist (yet).
    pub(crate) fn open_frame(
        self: &Arc<Self>,
        id: usize,
        schema: Arc<Schema>,
        words: &SharedWords,
        options: &DataFrameOptions,
    ) -> Result<Option<MappedFrame>> {
        let (ctr, cells_base, rheap_base) = self.frame_paths(id);
        let counters = match CounterFile::open(&ctr, self.writable) {
            Ok(counters) => Arc::new(counters),
            Err(Error::Io(e)) if e.kind() == ErrorKind::NotFound => {
                if !self.writable {
                    return Ok(None);
                }
                Arc::new(CounterFile::create(&ctr, FRAME_SLOTS)?)
            }
            Err(e) => return Err(e),
        };
        if counters.slots() < FRAME_SLOTS {
            return Err(Error::Corrupt("frame counter slot count"));
        }
        let cells = Region::open(&cells_base, counters.clone(), CELLS_LEN, CELLS_CHUNKS, self.sizes, self.writable)?;
        let rheap = Region::open(&rheap_base, counters.clone(), RHEAP_LEN, RHEAP_CHUNKS, self.sizes, self.writable)?;
        let columns = schema.len();
        let mut frame = MappedFrame { epoch: self.clone(), columns, stride: stride(columns), counters, cells, rheap, writer: None };
        if self.writable {
            let writer = frame.recover(RowEncoder::new(schema, words.clone(), options.tokenizer.clone()), options)?;
            frame.writer = Some(Mutex::new(writer));
        }
        Ok(Some(frame))
    }
}

/// The state of the whole-string dedup and of the row-heap blocks while writing (rebuilt by recovery).
struct EntryState {
    dedup: HashMap<u64, (u64, u32)>,
    dedup_entries: usize,
    block_starts: Vec<u64>,
    block_bytes: u64,
    /// Dedup keys added by the row being written (removed again if it fails).
    added: Vec<u64>,
}

struct FrameWriter {
    encoder: RowEncoder,
    state: EntryState,
    entries: BytesMut,
    prepared: Vec<Prepared>,
    row: Vec<u8>,
}

/// The pending rows of one frame in one epoch.
pub(crate) struct MappedFrame {
    epoch: Arc<Epoch>,
    columns: usize,
    stride: u64,
    counters: Arc<CounterFile>,
    cells: Region,
    rheap: Region,
    writer: Option<Mutex<FrameWriter>>,
}

impl MappedFrame {
    /// Rebuilds the dedup map and the row-heap block starts from the published rows (DESIGN.md 14.8): the distinct
    /// row-heap offsets of all cells, in ascending order, are exactly the entry starts, because entries are appended back
    /// to back and every published entry belongs to a published row.
    fn recover(&self, encoder: RowEncoder, options: &DataFrameOptions) -> Result<FrameWriter> {
        let mut state = EntryState {
            dedup: HashMap::new(),
            dedup_entries: options.dedup_entries,
            block_starts: Vec::new(),
            block_bytes: options.row_heap_block_bytes.max(1) as u64,
            added: Vec::new(),
        };
        let rows = self.published_rows()?;
        if rows > 0 {
            let source = self.source(rows)?;
            let schema = encoder.schema();
            let mut starts: Vec<(u64, bool)> = Vec::new();
            for row in 0..rows {
                for (column, col) in schema.columns().iter().enumerate() {
                    if !col.ty.is_variable() {
                        continue;
                    }
                    let (cell, null) = RowSource::cell(&source, row, column)?;
                    if !null {
                        let offset = cell.checked_sub(1).ok_or(Error::Corrupt("row heap offset"))?;
                        starts.push((offset, col.ty == crate::types::ColumnType::String));
                    }
                }
            }
            starts.sort_unstable();
            starts.dedup_by_key(|(offset, _)| *offset);
            let heap_len = source.rheap.len();
            for (i, &(start, string)) in starts.iter().enumerate() {
                let end = starts.get(i + 1).map_or(heap_len, |(next, _)| *next);
                if end > heap_len || start >= end {
                    return Err(Error::Corrupt("row heap offset past the published bytes"));
                }
                if state.block_starts.last().is_none_or(|&last| start - last >= state.block_bytes) {
                    state.block_starts.push(start);
                }
                if string && state.dedup.len() < state.dedup_entries {
                    let bytes = source.rheap.range(start, end)?;
                    state.dedup.entry(xxh3(&bytes, 0)).or_insert((start, (end - start) as u32));
                }
            }
        }
        Ok(FrameWriter { encoder, state, entries: BytesMut::new(), prepared: Vec::new(), row: Vec::new() })
    }

    /// Published rows, after mapping any new chunks (readers).
    pub(crate) fn published_rows(&self) -> Result<u64> {
        let len = self.cells.refresh()?;
        if len % self.stride != 0 {
            return Err(Error::Corrupt("pending cells length"));
        }
        Ok(len / self.stride)
    }

    /// Published bytes of the cells and the row heap.
    pub(crate) fn pending_bytes(&self) -> u64 {
        self.cells.published_len() + self.rheap.published_len()
    }

    /// When the first row of this epoch was pushed (Unix milliseconds), or 0.
    pub(crate) fn first_push_ms(&self) -> u64 {
        self.counters.load(FIRST_PUSH)
    }

    /// A source over the first `rows` published rows (call [`MappedFrame::published_rows`] first, which loads the cells
    /// length; the row heap is loaded after it and so covers those rows).
    pub(crate) fn source(&self, rows: u64) -> Result<MappedSource> {
        let cells = self.cells.view(rows * self.stride);
        self.rheap.refresh()?;
        let rheap = self.rheap.view(u64::MAX);
        let rows = cells.len() / self.stride;
        Ok(MappedSource { cells, rheap, rows, columns: self.columns, stride: self.stride, block_starts: Vec::new() })
    }

    /// Writer: a source over every row with the row-heap block starts, to write a segment from.
    pub(crate) fn segment_source(&self) -> Result<MappedSource> {
        let writer = self.writer.as_ref().ok_or(Error::ReadOnly)?.lock().or_poisoned()?;
        let rows = self.published_rows()?;
        let mut source = self.source(rows)?;
        source.block_starts = writer.state.block_starts.clone();
        Ok(source)
    }

    /// Appends a row and publishes it; returns its number in this buffer. On an error nothing is published and every
    /// in-memory structure is as before (the row is atomic, as in [`crate::DataFrame::push_row`]).
    pub(crate) fn push_row(&self, row: &[Value<'_>]) -> Result<u64> {
        let mut guard = self.writer.as_ref().ok_or(Error::ReadOnly)?.lock().or_poisoned()?;
        let FrameWriter { encoder, state, entries, prepared, row: bytes } = &mut *guard;
        let mut words = encoder.words().write().or_poisoned()?;
        let checkpoint = words.checkpoint();
        encoder.prepare_row(&mut words, row, entries, prepared)?;
        let blocks = state.block_starts.len();
        state.added.clear();
        let written = self.write_row(state, entries, prepared, bytes).and_then(|()| self.epoch.mirror_words(&words));
        if let Err(e) = written {
            let _ = self.cells.rollback();
            let _ = self.rheap.rollback();
            let _ = self.epoch.rollback_words();
            for key in state.added.drain(..) {
                state.dedup.remove(&key);
            }
            state.block_starts.truncate(blocks);
            words.rollback(checkpoint);
            return Err(e);
        }
        let number = self.cells.published_len() / self.stride;
        if number == 0 && self.counters.load(FIRST_PUSH) == 0 {
            self.counters.store(FIRST_PUSH, now_unix_ms().max(1));
        }
        // the commit point of the row is the cells length, stored last
        self.epoch.publish_words();
        self.rheap.publish();
        self.cells.publish();
        Ok(number)
    }

    fn write_row(&self, state: &mut EntryState, entries: &[u8], prepared: &[Prepared], row: &mut Vec<u8>) -> Result<()> {
        row.clear();
        row.resize(self.stride as usize, 0);
        let bits = self.columns * 8;
        for (column, p) in prepared.iter().enumerate() {
            let (cell, null) = match *p {
                Prepared::Cell(Some(cell)) => (cell, false),
                Prepared::Cell(None) | Prepared::Null => (0, true),
                Prepared::Entry { start, end, dedup } => (self.store_entry(state, &entries[start..end], dedup)? + 1, false),
            };
            row[column * 8..column * 8 + 8].copy_from_slice(&cell.to_le_bytes());
            if null {
                row[bits + column / 8] |= 1 << (column % 8);
            }
        }
        self.cells.append(row)?;
        Ok(())
    }

    /// Appends an entry to the row heap (or reuses an identical earlier one when `remember` is set), with the same rules
    /// as the in-memory buffer, so the segment is the same.
    fn store_entry(&self, state: &mut EntryState, bytes: &[u8], remember: bool) -> Result<u64> {
        let key = remember.then(|| xxh3(bytes, 0));
        if let Some(key) = key {
            if let Some(&(offset, len)) = state.dedup.get(&key) {
                if len as usize == bytes.len() && self.rheap.written_eq(offset, bytes) {
                    return Ok(offset);
                }
            }
        }
        let offset = self.rheap.written_len();
        if state.block_starts.last().is_none_or(|&last| offset - last >= state.block_bytes) {
            state.block_starts.push(offset);
        }
        self.rheap.append(bytes)?;
        if let Some(key) = key {
            if state.dedup.len() < state.dedup_entries {
                if let Entry::Vacant(slot) = state.dedup.entry(key) {
                    slot.insert((offset, bytes.len() as u32));
                    state.added.push(key);
                }
            }
        }
        Ok(offset)
    }
}

/// The published rows of a [`MappedFrame`] (a snapshot): row-major cells and the row heap.
pub(crate) struct MappedSource {
    cells: RegionView,
    rheap: RegionView,
    rows: u64,
    columns: usize,
    stride: u64,
    /// Row-heap block starts (only for writing a segment).
    block_starts: Vec<u64>,
}

impl RowSource for MappedSource {
    fn row_count(&self) -> u64 {
        self.rows
    }

    fn cell(&self, row: u64, column: usize) -> Result<(u64, bool)> {
        if column >= self.columns {
            return Err(Error::ColumnOutOfRange);
        }
        if row >= self.rows {
            return Err(Error::RowOutOfRange);
        }
        let bytes = self.cells.read(row * self.stride, self.stride)?;
        let cell = u64::from_le_bytes(bytes[column * 8..column * 8 + 8].try_into().expect("8 bytes"));
        let null = (bytes[self.columns * 8 + column / 8] >> (column % 8)) & 1 == 1;
        Ok((cell, null))
    }

    fn with_entry(&self, cell: u64, f: &mut dyn FnMut(&[u8]) -> Result<()>) -> Result<()> {
        f(self.rheap.slice_from(cell.checked_sub(1).ok_or(Error::Corrupt("row heap offset"))?)?)
    }

    fn range(&self, column: usize, lo: u64, hi: u64) -> Result<Vec<u32>> {
        let mut rows = Vec::new();
        for row in 0..self.rows {
            let (cell, null) = RowSource::cell(self, row, column)?;
            if !null && (lo..=hi).contains(&cell) {
                rows.push(row as u32);
            }
        }
        Ok(rows)
    }
}

impl SegmentInput for MappedSource {
    fn rows(&self) -> u64 {
        self.rows
    }

    fn columns(&self) -> usize {
        self.columns
    }

    fn cell(&self, row: u64, column: usize) -> Result<(u64, bool)> {
        RowSource::cell(self, row, column)
    }

    fn heap_blocks(&self) -> usize {
        self.block_starts.len()
    }

    fn heap_block(&self, index: usize) -> Result<(u64, Cow<'_, [u8]>)> {
        let start = self.block_starts[index];
        let end = self.block_starts.get(index + 1).copied().unwrap_or(self.rheap.len());
        Ok((start, self.rheap.range(start, end)?))
    }
}
