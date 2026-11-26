//! Segment files (DESIGN.md 6.3, 6.4): an immutable, column-major, block-compressed chunk of a dataframe.
//!
//! ```text
//! "LSQSEG01"
//! column blocks   for each block of `block_rows` rows, for each column: zstd(flags, null bitmap?, byte planes of cells)
//! row heap blocks zstd blocks of the row heap (an entry never crosses a block)
//! index           zstd(rows, block table with min/max per column block, row heap block table)
//! footer          index offset, compressed and raw length, xxh3 checksum of the index, "LSQSEGEN"
//! ```
//! Every block is checksummed (xxh3-64 of its compressed bytes) in the index and verified when read.

use crate::codec::{Reader, Writer};
use crate::dataframe::{Inner, decode_value};
use crate::dictionary::xxh3;
use crate::error::{Error, Result};
use crate::heap::Heap;
use crate::schema::Schema;
use crate::sync::Unpoison;
use crate::types::OwnedValue;
use crate::words::WordStore;
use std::borrow::Cow;
use std::collections::HashMap;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

const MAGIC: &[u8; 8] = b"LSQSEG01";
const FOOTER_MAGIC: &[u8; 8] = b"LSQSEGEN";
const FOOTER_LEN: u64 = 8 + 4 + 4 + 8 + 8;
/// Default size limit of the decoded-block cache of one segment reader, in bytes.
pub(crate) const DEFAULT_CACHE_BYTES: usize = 32 << 20;
/// Default number of segment files a store keeps open at once.
pub(crate) const DEFAULT_MAX_OPEN_SEGMENTS: usize = 64;

#[derive(Debug, Clone, Copy)]
struct BlockRef {
    offset: u64,
    comp_len: u32,
    raw_len: u32,
    checksum: u64,
}

#[derive(Debug, Clone, Copy)]
struct ColumnBlock {
    block: BlockRef,
    min: u64,
    max: u64,
    non_null: u32,
}

#[derive(Debug)]
struct Index {
    rows: u64,
    block_rows: u32,
    columns: u32,
    column_blocks: Vec<ColumnBlock>,
    /// `(logical start in the row heap, block)`.
    heap_blocks: Vec<(u64, BlockRef)>,
}

fn write_block_ref(w: &mut Writer, b: &BlockRef) {
    w.u64(b.offset);
    w.u32(b.comp_len);
    w.u32(b.raw_len);
    w.u64(b.checksum);
}

fn read_block_ref(r: &mut Reader<'_>) -> Result<BlockRef> {
    Ok(BlockRef { offset: r.u64()?, comp_len: r.u32()?, raw_len: r.u32()?, checksum: r.u64()? })
}

impl Index {
    fn encode(&self) -> Vec<u8> {
        let mut w = Writer::new();
        w.u64(self.rows);
        w.u32(self.block_rows);
        w.u32(self.columns);
        w.u32(self.column_blocks.len() as u32);
        for cb in &self.column_blocks {
            write_block_ref(&mut w, &cb.block);
            w.u64(cb.min);
            w.u64(cb.max);
            w.u32(cb.non_null);
        }
        w.u32(self.heap_blocks.len() as u32);
        for (start, block) in &self.heap_blocks {
            w.u64(*start);
            write_block_ref(&mut w, block);
        }
        w.buf
    }

    fn decode(bytes: &[u8]) -> Result<Self> {
        let mut r = Reader::new(bytes);
        let (rows, block_rows, columns) = (r.u64()?, r.u32()?, r.u32()?);
        let mut column_blocks = Vec::new();
        for _ in 0..r.u32()? {
            let block = read_block_ref(&mut r)?;
            column_blocks.push(ColumnBlock { block, min: r.u64()?, max: r.u64()?, non_null: r.u32()? });
        }
        let mut heap_blocks = Vec::new();
        for _ in 0..r.u32()? {
            let start = r.u64()?;
            heap_blocks.push((start, read_block_ref(&mut r)?));
        }
        let blocks = rows.div_ceil(u64::from(block_rows.max(1)));
        if !r.is_empty() || block_rows == 0 || column_blocks.len() as u64 != blocks * u64::from(columns) {
            return Err(Error::Corrupt("segment index"));
        }
        Ok(Self { rows, block_rows, columns, column_blocks, heap_blocks })
    }
}

/// Settings of [`write_segment`].
#[derive(Debug, Clone, Copy)]
pub(crate) struct WriteOptions {
    pub block_rows: usize,
    pub zstd_level: i32,
}

struct BlockWriter<W: Write> {
    out: W,
    pos: u64,
    level: i32,
}

impl<W: Write> BlockWriter<W> {
    fn put(&mut self, raw: &[u8]) -> Result<BlockRef> {
        let comp = zstd::bulk::compress(raw, self.level)?;
        let block = BlockRef { offset: self.pos, comp_len: comp.len() as u32, raw_len: raw.len() as u32, checksum: xxh3(&comp, 0) };
        self.out.write_all(&comp)?;
        self.pos += comp.len() as u64;
        Ok(block)
    }
}

/// The rows a segment is written from: the in-memory buffer of a dataframe or a mapped pending buffer.
pub(crate) trait SegmentInput {
    fn rows(&self) -> u64;
    fn columns(&self) -> usize;
    /// The cell of a row and whether it is null.
    fn cell(&self, row: u64, column: usize) -> Result<(u64, bool)>;
    /// Number of row-heap blocks.
    fn heap_blocks(&self) -> usize;
    /// The logical start offset and the bytes of a row-heap block (an entry never crosses a block).
    fn heap_block(&self, index: usize) -> Result<(u64, Cow<'_, [u8]>)>;
}

impl SegmentInput for Inner {
    fn rows(&self) -> u64 {
        self.rows
    }

    fn columns(&self) -> usize {
        self.columns.len()
    }

    fn cell(&self, row: u64, column: usize) -> Result<(u64, bool)> {
        let data = self.columns.get(column).ok_or(Error::ColumnOutOfRange)?;
        Ok((*data.cells.get(row as usize).ok_or(Error::RowOutOfRange)?, data.nulls.get(row as usize)))
    }

    fn heap_blocks(&self) -> usize {
        self.block_starts.len()
    }

    fn heap_block(&self, index: usize) -> Result<(u64, Cow<'_, [u8]>)> {
        let start = self.block_starts[index];
        let end = self.block_starts.get(index + 1).copied().unwrap_or(self.row_heap.len() as u64);
        Ok((start, Cow::Borrowed(&self.row_heap[start as usize..end as usize])))
    }
}

/// Writes the buffered rows of a dataframe as a segment file and syncs it to disk.
pub(crate) fn write_segment(path: &Path, data: &dyn SegmentInput, options: WriteOptions) -> Result<()> {
    let file = std::fs::OpenOptions::new().write(true).create_new(true).open(path)?;
    let mut writer = BlockWriter { out: std::io::BufWriter::new(file), pos: 0, level: options.zstd_level };
    writer.out.write_all(MAGIC)?;
    writer.pos = MAGIC.len() as u64;

    let block_rows = options.block_rows.max(1);
    let rows = data.rows() as usize;
    let columns = data.columns();
    let mut column_blocks = Vec::new();
    let mut payload = Vec::new();
    let mut cells = Vec::with_capacity(block_rows.min(rows));
    let mut nulls = Vec::with_capacity(block_rows.min(rows));
    for first in (0..rows).step_by(block_rows) {
        let last = (first + block_rows).min(rows);
        for column in 0..columns {
            cells.clear();
            nulls.clear();
            for row in first..last {
                let (cell, null) = data.cell(row as u64, column)?;
                cells.push(cell);
                nulls.push(null);
            }
            let has_nulls = nulls.iter().any(|&n| n);
            payload.clear();
            payload.push(u8::from(has_nulls));
            if has_nulls {
                payload.resize(1 + nulls.len().div_ceil(8), 0);
                for (i, _) in nulls.iter().enumerate().filter(|(_, n)| **n) {
                    payload[1 + i / 8] |= 1 << (i % 8);
                }
            }
            for plane in 0..8 {
                payload.extend(cells.iter().map(|&cell| (cell >> (8 * plane)) as u8));
            }
            let present = cells.iter().zip(&nulls).filter(|(_, null)| !**null).map(|(cell, _)| *cell);
            let (min, max, non_null) = present.fold((u64::MAX, 0u64, 0u32), |(lo, hi, n), c| (lo.min(c), hi.max(c), n + 1));
            let (min, max) = if non_null == 0 { (0, 0) } else { (min, max) };
            column_blocks.push(ColumnBlock { block: writer.put(&payload)?, min, max, non_null });
        }
    }

    let mut heap_blocks = Vec::new();
    for i in 0..data.heap_blocks() {
        let (start, bytes) = data.heap_block(i)?;
        heap_blocks.push((start, writer.put(&bytes)?));
    }

    let index = Index { rows: data.rows(), block_rows: block_rows as u32, columns: columns as u32, column_blocks, heap_blocks };
    let raw = index.encode();
    let comp = zstd::bulk::compress(&raw, options.zstd_level)?;
    let index_offset = writer.pos;
    writer.out.write_all(&comp)?;
    let mut footer = Writer::new();
    footer.u64(index_offset);
    footer.u32(comp.len() as u32);
    footer.u32(raw.len() as u32);
    footer.u64(xxh3(&comp, 0));
    footer.buf.extend_from_slice(FOOTER_MAGIC);
    writer.out.write_all(&footer.buf)?;
    writer.out.flush()?;
    writer.out.into_inner().map_err(|e| Error::Io(e.into_error()))?.sync_all()?;
    Ok(())
}

/// Decoded cells of one column block.
struct ColumnData {
    cells: Vec<u64>,
    nulls: Option<Vec<u8>>,
}

impl ColumnData {
    fn is_null(&self, i: usize) -> bool {
        self.nulls.as_ref().is_some_and(|bits| (bits[i / 8] >> (i % 8)) & 1 == 1)
    }
}

#[derive(Hash, PartialEq, Eq)]
enum CacheKey {
    Column { block: u64, column: u32 },
    Heap(usize),
}

enum Cached {
    Column(Arc<ColumnData>),
    Heap(Arc<Vec<u8>>),
}

/// An open segment file shared by the pool and the reads that use it. Dropping it closes the file.
struct Handle {
    file: Mutex<File>,
    live: Arc<AtomicUsize>,
}

impl Drop for Handle {
    fn drop(&mut self) {
        self.live.fetch_sub(1, Ordering::Relaxed);
    }
}

#[derive(Default)]
struct PoolState {
    /// Most files kept open (at least 1).
    limit: usize,
    /// Counter of uses, for least-recently-used eviction.
    tick: u64,
    next_id: u64,
    /// Open file of each reader that has one: `reader id -> (handle, last use)`.
    open: HashMap<u64, (Arc<Handle>, u64)>,
}

impl PoolState {
    /// Closes the least recently used files until at most `limit` are open, never the one of `keep`. The handles are
    /// returned so that the caller drops (closes) them after releasing the pool lock.
    fn evict(&mut self, keep: Option<u64>) -> Vec<Arc<Handle>> {
        let mut closed = Vec::new();
        while self.open.len() > self.limit {
            let oldest = self.open.iter().filter(|(id, _)| Some(**id) != keep).min_by_key(|(_, (_, used))| *used).map(|(id, _)| *id);
            match oldest.and_then(|id| self.open.remove(&id)) {
                Some((handle, _)) => closed.push(handle),
                None => break,
            }
        }
        closed
    }
}

/// The open segment files of one store, bounded by a limit and evicted least recently used first. A reader whose file
/// was closed opens it again on its next read.
///
/// Locking: the pool lock is only held to look up, insert or remove a handle, never while a file lock or a reader's
/// cache lock is held, and no other lock is taken while it is held. A read clones the handle out of the pool, releases
/// the pool lock and then locks the file, so an eviction during a read only closes the file once that read is done
/// (the number of open files can exceed the limit by the reads in progress).
pub(crate) struct FilePool {
    state: Mutex<PoolState>,
    /// Open files, including evicted ones still used by a read.
    live: Arc<AtomicUsize>,
}

impl FilePool {
    pub(crate) fn new(limit: usize) -> Arc<Self> {
        let state = PoolState { limit: limit.max(1), ..PoolState::default() };
        Arc::new(Self { state: Mutex::new(state), live: Arc::new(AtomicUsize::new(0)) })
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, PoolState> {
        self.state.lock().ignore_poison()
    }

    /// The limit of open files.
    pub(crate) fn limit(&self) -> usize {
        self.lock().limit
    }

    /// Changes the limit (at least 1), closing files if there are too many.
    pub(crate) fn set_limit(&self, limit: usize) {
        let closed = {
            let mut state = self.lock();
            state.limit = limit.max(1);
            state.evict(None)
        };
        drop(closed);
    }

    /// Segment files open now (those in the pool plus evicted ones that a read still uses).
    pub(crate) fn open_files(&self) -> usize {
        self.live.load(Ordering::Relaxed)
    }

    fn register(&self) -> u64 {
        let mut state = self.lock();
        state.next_id += 1;
        state.next_id
    }

    fn get(&self, id: u64) -> Option<Arc<Handle>> {
        let mut state = self.lock();
        state.tick += 1;
        let tick = state.tick;
        state.open.get_mut(&id).map(|(handle, used)| {
            *used = tick;
            Arc::clone(handle)
        })
    }

    /// Adds the file of reader `id` (or returns the one another thread added meanwhile), closing the least recently used.
    fn insert(&self, id: u64, file: File) -> Arc<Handle> {
        let (handle, closed) = {
            let mut state = self.lock();
            state.tick += 1;
            let tick = state.tick;
            if let Some((handle, used)) = state.open.get_mut(&id) {
                *used = tick;
                return Arc::clone(handle);
            }
            self.live.fetch_add(1, Ordering::Relaxed);
            let handle = Arc::new(Handle { file: Mutex::new(file), live: Arc::clone(&self.live) });
            state.open.insert(id, (Arc::clone(&handle), tick));
            (handle, state.evict(Some(id)))
        };
        drop(closed);
        handle
    }

    fn remove(&self, id: u64) {
        let closed = self.lock().open.remove(&id);
        drop(closed);
    }
}

/// What identifies the file a reader opened, checked when it is opened again.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct FileId {
    len: u64,
    #[cfg(unix)]
    dev: u64,
    #[cfg(unix)]
    ino: u64,
}

impl FileId {
    fn of(file: &File) -> Result<Self> {
        let meta = file.metadata()?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            Ok(Self { len: meta.len(), dev: meta.dev(), ino: meta.ino() })
        }
        #[cfg(not(unix))]
        Ok(Self { len: meta.len() })
    }
}

/// Reads rows of one segment file lazily, block by block. The file is opened through the store's [`FilePool`]; when the
/// pool has closed it, the next block read opens it again.
pub(crate) struct SegmentReader {
    path: PathBuf,
    file_id: FileId,
    id: u64,
    pool: Arc<FilePool>,
    index: Index,
    cache: Mutex<Cache>,
    cache_limit: usize,
}

impl Drop for SegmentReader {
    fn drop(&mut self) {
        self.pool.remove(self.id);
    }
}

/// Decoded blocks kept for reuse, cleared when they exceed the size limit.
#[derive(Default)]
struct Cache {
    map: HashMap<CacheKey, Cached>,
    bytes: usize,
}

impl SegmentReader {
    /// Opens a segment and verifies its header, footer and index. Its file stays open in `pool` until evicted.
    pub(crate) fn open(path: &Path, columns: usize, pool: &Arc<FilePool>) -> Result<Self> {
        let mut file = File::open(path)?;
        let file_id = FileId::of(&file)?;
        let len = file_id.len;
        if len < MAGIC.len() as u64 + FOOTER_LEN {
            return Err(Error::Corrupt("segment file is too short"));
        }
        let mut magic = [0u8; 8];
        file.read_exact(&mut magic)?;
        if &magic != MAGIC {
            return Err(Error::Corrupt("segment magic"));
        }
        let mut footer = vec![0u8; FOOTER_LEN as usize];
        file.seek(SeekFrom::Start(len - FOOTER_LEN))?;
        file.read_exact(&mut footer)?;
        let mut r = Reader::new(&footer);
        let (offset, comp_len, raw_len, checksum) = (r.u64()?, r.u32()?, r.u32()?, r.u64()?);
        if r.take(8)? != FOOTER_MAGIC || offset.checked_add(u64::from(comp_len)) != Some(len - FOOTER_LEN) {
            return Err(Error::Corrupt("segment footer"));
        }
        let mut comp = vec![0u8; comp_len as usize];
        file.seek(SeekFrom::Start(offset))?;
        file.read_exact(&mut comp)?;
        if xxh3(&comp, 0) != checksum {
            return Err(Error::Corrupt("segment index checksum"));
        }
        let index = Index::decode(&zstd::bulk::decompress(&comp, raw_len as usize).map_err(|_| Error::Corrupt("segment index"))?)?;
        if index.columns as usize != columns {
            return Err(Error::Corrupt("segment column count"));
        }
        let id = pool.register();
        pool.insert(id, file);
        Ok(Self {
            path: path.to_path_buf(),
            file_id,
            id,
            pool: Arc::clone(pool),
            index,
            cache: Mutex::new(Cache::default()),
            cache_limit: DEFAULT_CACHE_BYTES,
        })
    }

    /// The open file of the segment, opened again if the pool closed it. A file that is gone fails with the error of
    /// opening it; a different file at the same path (another length or, on Unix, another inode) is an error too.
    fn handle(&self) -> Result<Arc<Handle>> {
        if let Some(handle) = self.pool.get(self.id) {
            return Ok(handle);
        }
        let file = File::open(&self.path)?;
        if FileId::of(&file)? != self.file_id {
            return Err(Error::Io(std::io::Error::other("segment file was replaced while the store was open")));
        }
        Ok(self.pool.insert(self.id, file))
    }

    /// Reads and checks every block of the segment.
    pub(crate) fn verify(&self) -> Result<()> {
        for entry in &self.index.column_blocks {
            self.read_block(&entry.block)?;
        }
        for (_, block) in &self.index.heap_blocks {
            self.read_block(block)?;
        }
        Ok(())
    }

    /// Sets how many bytes of decoded blocks the reader may keep (default 32 MiB).
    pub(crate) fn with_cache_limit(mut self, bytes: usize) -> Self {
        self.cache_limit = bytes;
        self
    }

    /// Number of rows.
    pub(crate) fn rows(&self) -> u64 {
        self.index.rows
    }

    /// Smallest and largest non-null cell of a column in a block, and the number of non-null cells.
    #[cfg(test)]
    pub(crate) fn column_block_stats(&self, block: u64, column: usize) -> (u64, u64, u32) {
        let cb = &self.index.column_blocks[block as usize * self.index.columns as usize + column];
        (cb.min, cb.max, cb.non_null)
    }

    fn read_block(&self, block: &BlockRef) -> Result<Vec<u8>> {
        let mut comp = vec![0u8; block.comp_len as usize];
        {
            // the pool lock is released by now: only the file lock is held while reading
            let handle = self.handle()?;
            let mut file = handle.file.lock().or_poisoned()?;
            file.seek(SeekFrom::Start(block.offset))?;
            file.read_exact(&mut comp)?;
        }
        if xxh3(&comp, 0) != block.checksum {
            return Err(Error::Corrupt("block checksum"));
        }
        zstd::bulk::decompress(&comp, block.raw_len as usize).map_err(|_| Error::Corrupt("block does not decompress"))
    }

    fn remember(&self, key: CacheKey, value: Cached, size: usize) {
        let mut cache = self.cache.lock().ignore_poison();
        if cache.bytes + size > self.cache_limit {
            cache.map.clear();
            cache.bytes = 0;
        }
        cache.bytes += size;
        cache.map.insert(key, value);
    }

    fn column_data(&self, block: u64, column: u32) -> Result<Arc<ColumnData>> {
        let key = CacheKey::Column { block, column };
        if let Some(Cached::Column(data)) = self.cache.lock().or_poisoned()?.map.get(&key) {
            return Ok(Arc::clone(data));
        }
        let entry = &self.index.column_blocks[block as usize * self.index.columns as usize + column as usize];
        let raw = self.read_block(&entry.block)?;
        let n = (self.index.rows - block * u64::from(self.index.block_rows)).min(u64::from(self.index.block_rows)) as usize;
        let (&flags, rest) = raw.split_first().ok_or(Error::Corrupt("empty column block"))?;
        let bitmap_len = if flags == 1 { n.div_ceil(8) } else { 0 };
        if flags > 1 || rest.len() != bitmap_len + 8 * n {
            return Err(Error::Corrupt("column block size"));
        }
        let (bitmap, planes) = rest.split_at(bitmap_len);
        let mut cells = vec![0u64; n];
        for (plane, bytes) in planes.chunks(n.max(1)).enumerate().take(8) {
            for (cell, &byte) in cells.iter_mut().zip(bytes) {
                *cell |= u64::from(byte) << (8 * plane);
            }
        }
        let data = Arc::new(ColumnData { cells, nulls: (flags == 1).then(|| bitmap.to_vec()) });
        let size = data.cells.len() * 8 + data.nulls.as_ref().map_or(0, Vec::len);
        self.remember(key, Cached::Column(Arc::clone(&data)), size);
        Ok(data)
    }

    fn heap_block(&self, index: usize) -> Result<Arc<Vec<u8>>> {
        let key = CacheKey::Heap(index);
        if let Some(Cached::Heap(data)) = self.cache.lock().or_poisoned()?.map.get(&key) {
            return Ok(Arc::clone(data));
        }
        let data = Arc::new(self.read_block(&self.index.heap_blocks[index].1)?);
        self.remember(key, Cached::Heap(Arc::clone(&data)), data.len());
        Ok(data)
    }

    /// The row-heap block holding `offset`, and the position of `offset` inside it.
    fn heap_at(&self, offset: u64) -> Result<(Arc<Vec<u8>>, usize)> {
        let blocks = &self.index.heap_blocks;
        let i = blocks.partition_point(|(start, _)| *start <= offset).checked_sub(1).ok_or(Error::Corrupt("row heap offset"))?;
        let block = self.heap_block(i)?;
        let pos = usize::try_from(offset - blocks[i].0).map_err(|_| Error::Corrupt("row heap offset"))?;
        if pos > block.len() {
            return Err(Error::Corrupt("row heap offset"));
        }
        Ok((block, pos))
    }

    pub(crate) fn cell(&self, row: u64, column: usize) -> Result<(u64, bool)> {
        if row >= self.index.rows || column >= self.index.columns as usize {
            return Err(Error::RowOutOfRange);
        }
        let block_rows = u64::from(self.index.block_rows);
        let data = self.column_data(row / block_rows, column as u32)?;
        let i = (row % block_rows) as usize;
        Ok((data.cells[i], data.is_null(i)))
    }

    /// Calls `f` with the row-heap bytes starting at the entry of a string cell.
    pub(crate) fn with_entry(&self, cell: u64, f: &mut dyn FnMut(&[u8]) -> Result<()>) -> Result<()> {
        let (block, pos) = self.heap_at(cell - 1)?;
        f(&block[pos..])
    }

    /// Rows whose non-null cell in `column` lies in `lo..=hi`, skipping blocks by their minimum and maximum.
    pub(crate) fn range(&self, column: usize, lo: u64, hi: u64) -> Result<Vec<u32>> {
        let columns = self.index.columns as usize;
        if column >= columns {
            return Err(Error::ColumnOutOfRange);
        }
        let block_rows = u64::from(self.index.block_rows);
        let mut rows = Vec::new();
        for block in 0..self.index.rows.div_ceil(block_rows) {
            let stats = &self.index.column_blocks[block as usize * columns + column];
            if stats.non_null == 0 || stats.max < lo || stats.min > hi {
                continue;
            }
            let data = self.column_data(block, column as u32)?;
            for (i, &c) in data.cells.iter().enumerate() {
                if !data.is_null(i) && (lo..=hi).contains(&c) {
                    rows.push((block * block_rows) as u32 + i as u32);
                }
            }
        }
        Ok(rows)
    }

    /// Reads one value.
    pub(crate) fn value<H: Heap>(&self, schema: &Schema, words: &WordStore<H>, row: u64, column: usize) -> Result<OwnedValue> {
        let ty = schema.columns().get(column).ok_or(Error::ColumnOutOfRange)?.ty;
        let (cell, null) = self.cell(row, column)?;
        if ty.is_variable() && !null {
            let (block, pos) = self.heap_at(cell - 1)?;
            decode_value(ty, null, cell, &block[pos..], words)
        } else {
            decode_value(ty, null, cell, &[], words)
        }
    }

    /// Writes the text of a string cell into `out`; `false` for null.
    pub(crate) fn write_string<H: Heap>(
        &self,
        words: &WordStore<H>,
        row: u64,
        column: usize,
        out: &mut impl bytes::BufMut,
    ) -> Result<bool> {
        let (cell, null) = self.cell(row, column)?;
        if null {
            return Ok(false);
        }
        let (block, pos) = self.heap_at(cell - 1)?;
        crate::tokenizer::decode_with(words, &block[pos..], out)?;
        Ok(true)
    }

    /// Reads one row.
    pub(crate) fn row<H: Heap>(&self, schema: &Schema, words: &WordStore<H>, row: u64) -> Result<Vec<OwnedValue>> {
        (0..schema.len()).map(|column| self.value(schema, words, row, column)).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dataframe::{DataFrame, DataFrameOptions};
    use crate::dictionary::Dictionary;
    use crate::heap::MemHeap;
    use crate::schema::Column;
    use crate::types::{ColumnType, Value};
    use crate::words::SharedWords;

    fn words() -> SharedWords {
        WordStore::new(MemHeap::new(), Dictionary::new(1 << 20)).into_shared()
    }

    fn schema() -> Schema {
        Schema::new(vec![
            Column::new("id", ColumnType::I64),
            Column::new("score", ColumnType::F64),
            Column::new("msg", ColumnType::String),
            Column::new("raw", ColumnType::Blob),
            Column::new("flag", ColumnType::Bool),
        ])
        .unwrap()
    }

    fn filled(shared: &SharedWords, rows: i64) -> DataFrame {
        let options = DataFrameOptions { row_heap_block_bytes: 200, ..DataFrameOptions::default() };
        let frame = DataFrame::with_options(schema(), shared.clone(), options);
        for i in 0..rows {
            let msg = format!("message number {} says hello to {}", i % 7, i);
            let null = i % 5 == 0;
            let row = [
                Value::I64(i - 50),
                if null { Value::Null } else { Value::F64(i as f64 / 4.0) },
                if i % 11 == 0 { Value::Null } else { Value::Str(&msg) },
                Value::Blob(&[i as u8; 3]),
                Value::Bool(i % 2 == 0),
            ];
            frame.push_row(&row).unwrap();
        }
        frame
    }

    fn pool() -> Arc<FilePool> {
        FilePool::new(DEFAULT_MAX_OPEN_SEGMENTS)
    }

    fn write(frame: &DataFrame, dir: &Path, block_rows: usize) -> std::path::PathBuf {
        let path = dir.join("a.seg");
        frame.with_inner(|inner| write_segment(&path, inner, WriteOptions { block_rows, zstd_level: 3 })).unwrap();
        path
    }

    #[test]
    fn round_trips_across_many_blocks() {
        let shared = words();
        let frame = filled(&shared, 1000);
        let dir = tempfile::tempdir().unwrap();
        let path = write(&frame, dir.path(), 64);
        let reader = SegmentReader::open(&path, 5, &pool()).unwrap();
        assert_eq!(reader.rows(), 1000);
        let store = shared.read().unwrap();
        for row in (0..1000).chain([999, 0, 500]) {
            assert_eq!(reader.row(frame.schema(), &store, row).unwrap(), frame.row(row).unwrap(), "row {row}");
        }
        let mut out = bytes::BytesMut::new();
        assert!(reader.write_string(&store, 1, 2, &mut out).unwrap());
        assert_eq!(&out[..], b"message number 1 says hello to 1");
        assert!(!reader.write_string(&store, 0, 2, &mut out).unwrap());
        assert!(matches!(reader.value(frame.schema(), &store, 1000, 0), Err(Error::RowOutOfRange)));
    }

    #[test]
    fn min_max_and_null_counts_are_recorded_per_block() {
        let shared = words();
        let frame = filled(&shared, 100);
        let dir = tempfile::tempdir().unwrap();
        let reader = SegmentReader::open(&write(&frame, dir.path(), 40), 5, &pool()).unwrap();
        let (min, max, non_null) = reader.column_block_stats(1, 0); // ids 40..79 are -10..29
        assert_eq!((min, max, non_null), (crate::cell::from_signed(-10), crate::cell::from_signed(29), 40));
        let (_, _, score_non_null) = reader.column_block_stats(0, 1); // rows 0, 5, 10, ... are null
        assert_eq!(score_non_null, 32);
    }

    #[test]
    fn empty_and_all_null_segments() {
        let shared = words();
        let dir = tempfile::tempdir().unwrap();
        let empty = DataFrame::new(schema(), shared.clone());
        let reader = SegmentReader::open(&write(&empty, dir.path(), 64), 5, &pool()).unwrap();
        assert_eq!(reader.rows(), 0);
        let nulls = DataFrame::new(schema(), shared.clone());
        nulls.push_row(&[Value::Null, Value::Null, Value::Null, Value::Null, Value::Null]).unwrap();
        let dir2 = tempfile::tempdir().unwrap();
        let reader = SegmentReader::open(&write(&nulls, dir2.path(), 64), 5, &pool()).unwrap();
        let store = shared.read().unwrap();
        assert_eq!(reader.row(nulls.schema(), &store, 0).unwrap(), vec![OwnedValue::Null; 5]);
        assert_eq!(reader.column_block_stats(0, 2), (0, 0, 0));
    }

    #[test]
    fn corruption_is_detected() {
        let shared = words();
        let frame = filled(&shared, 300);
        let dir = tempfile::tempdir().unwrap();
        let path = write(&frame, dir.path(), 64);
        let good = std::fs::read(&path).unwrap();
        let store = shared.read().unwrap();

        // a flipped byte inside a data block is caught by that block's checksum when it is read
        let mut bad = good.clone();
        bad[100] ^= 0xff;
        std::fs::write(&path, &bad).unwrap();
        let reader = SegmentReader::open(&path, 5, &pool()).unwrap();
        let all_ok = (0..300).all(|row| reader.row(frame.schema(), &store, row).is_ok());
        assert!(!all_ok);

        // damage to the index, the footer or the length is caught when opening
        for position in [good.len() - 1, good.len() - 20, good.len() - 40] {
            let mut bad = good.clone();
            bad[position] ^= 0x55;
            std::fs::write(&path, &bad).unwrap();
            assert!(SegmentReader::open(&path, 5, &pool()).is_err(), "byte {position}");
        }
        std::fs::write(&path, &good[..good.len() - 5]).unwrap();
        assert!(SegmentReader::open(&path, 5, &pool()).is_err());
        std::fs::write(&path, &good).unwrap();
        assert!(SegmentReader::open(&path, 4, &pool()).is_err()); // wrong column count
        assert!(SegmentReader::open(&path, 5, &pool()).is_ok());
    }

    #[test]
    fn readers_reopen_files_closed_by_the_pool() {
        let shared = words();
        let frame = filled(&shared, 300);
        let dirs: Vec<_> = (0..5).map(|_| tempfile::tempdir().unwrap()).collect();
        let pool = FilePool::new(2);
        let readers: Vec<_> = dirs.iter().map(|d| SegmentReader::open(&write(&frame, d.path(), 32), 5, &pool).unwrap()).collect();
        assert_eq!(pool.open_files(), 2);
        let store = shared.read().unwrap();
        for round in 0..3 {
            for (i, reader) in readers.iter().enumerate() {
                // a fresh reader of the same file has an empty cache, so every block read goes to the file
                let fresh = SegmentReader::open(&dirs[i].path().join("a.seg"), 5, &pool).unwrap().with_cache_limit(0);
                for row in (round..300).step_by(37) {
                    assert_eq!(reader.row(frame.schema(), &store, row).unwrap(), frame.row(row).unwrap());
                    assert_eq!(fresh.row(frame.schema(), &store, row).unwrap(), frame.row(row).unwrap());
                    assert!(pool.open_files() <= 2);
                }
            }
        }
        drop(readers);
        assert!(pool.open_files() <= 2);
        pool.set_limit(1);
        assert!(pool.open_files() <= 1);
        let reader = SegmentReader::open(&dirs[0].path().join("a.seg"), 5, &pool).unwrap();
        drop(reader);
        assert_eq!(pool.open_files(), 0, "a dropped reader closes its file");
    }

    #[test]
    fn a_missing_or_replaced_file_fails_after_eviction() {
        let shared = words();
        let frame = filled(&shared, 300);
        let (a, b) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
        let pool = FilePool::new(1);
        let first = SegmentReader::open(&write(&frame, a.path(), 32), 5, &pool).unwrap().with_cache_limit(0);
        let second = SegmentReader::open(&write(&frame, b.path(), 32), 5, &pool).unwrap().with_cache_limit(0);
        let store = shared.read().unwrap();
        assert!(first.row(frame.schema(), &store, 1).is_ok());
        assert!(second.row(frame.schema(), &store, 1).is_ok()); // closes the file of `first`

        std::fs::remove_file(a.path().join("a.seg")).unwrap();
        let err = first.row(frame.schema(), &store, 2).unwrap_err();
        assert!(matches!(&err, Error::Io(e) if e.kind() == std::io::ErrorKind::NotFound), "{err:?}");
        assert!(second.row(frame.schema(), &store, 2).is_ok());

        // a different file under the same name is not read in place of the old one
        drop(store);
        let other = filled(&shared, 299);
        write(&other, a.path(), 32);
        let store = shared.read().unwrap();
        assert!(matches!(first.row(frame.schema(), &store, 2), Err(Error::Io(_))));
        assert!(second.row(frame.schema(), &store, 3).is_ok());
    }
}
