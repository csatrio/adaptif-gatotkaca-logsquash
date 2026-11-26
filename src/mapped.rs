//! Shared, memory-mapped files of the pending buffer (DESIGN.md 14.3, 14.4): **counter files**, a page of atomic slots
//! shared between processes, and **regions**, logical append-only byte streams stored in chunk files.
//!
//! With `filebytes`, this is the only module that uses `unsafe`. These rules make it sound:
//! - Bytes of a region below its published length are never written again. The writer writes only at or beyond its
//!   cursor (which is never below the published length), through raw pointers, so no `&mut [u8]` exists that could
//!   overlap a slice held by a reader, in this process or another.
//! - Readers form slices only of bytes below a published length that they loaded with acquire ordering. The writer
//!   stored that length with release ordering after writing the bytes.
//! - Slices are bounded by the mapped capacity of their chunk, so even a wrong length cannot reach outside a mapping.
//! - Chunk and counter files are never resized or truncated while they exist. They appear by rename, already fully
//!   initialized, and are only deleted when no counter publishes them (or the whole epoch is over).
//! - Counters are `AtomicU64` values at 8-aligned offsets of a page-aligned map. A read-only map only loads them, which
//!   is allowed on read-only memory for lock-free atomics of at most 8 bytes.
//!
//! Counters are native-endian: the files belong to one machine. A byte-order mark rejects files from another one.

#![allow(unsafe_code)]
// Used by the pending buffer from step 2 of DESIGN.md 14.15 on; until then only the tests use it.
#![cfg_attr(not(test), allow(dead_code))]

use crate::error::{Error, Result};
use memmap2::{MmapOptions, MmapRaw};
use std::fs::{File, OpenOptions};
use std::io::{ErrorKind, Write};
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};

/// Size of a counter file and of the header of a chunk file.
pub(crate) const PAGE: u64 = 4096;
const COUNTER_MAGIC: &[u8; 8] = b"LSQCTR01";
const CHUNK_MAGIC: &[u8; 8] = b"LSQCHNK1";
const FORMAT: u32 = 1;
const BYTE_ORDER: u64 = 0x0102_0304_0506_0708;
const COUNTER_HEADER: usize = 64;
/// Slots that fit in one counter file.
pub(crate) const MAX_SLOTS: usize = (PAGE as usize - COUNTER_HEADER) / 8;

fn tmp_path(path: &Path) -> PathBuf {
    let mut name = path.as_os_str().to_owned();
    name.push(".tmp");
    PathBuf::from(name)
}

fn remove_if_present(path: &Path) -> Result<bool> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(true),
        Err(e) if e.kind() == ErrorKind::NotFound => Ok(false),
        Err(e) => Err(e.into()),
    }
}

/// Reserves `len` bytes for `file`, so that writing through a map cannot fail later for lack of space (a full disk then
/// fails here, with `Error::Io`). File systems without allocation support get a sparse file instead.
fn reserve(file: &File, len: u64) -> Result<()> {
    match fs4::FileExt::allocate(file, len) {
        Ok(()) => {}
        Err(e) if e.kind() == ErrorKind::Unsupported => {}
        Err(e) => return Err(e.into()),
    }
    if file.metadata()?.len() < len {
        file.set_len(len)?;
    }
    Ok(())
}

/// Creates `path` of `len` bytes starting with `header`, through a temporary file and a rename, so the file appears
/// complete. Fails with `AlreadyExists` if `path` exists.
fn create_file(path: &Path, header: &[u8], len: u64) -> Result<File> {
    if path.exists() {
        return Err(Error::Io(std::io::Error::new(ErrorKind::AlreadyExists, "mapped file exists")));
    }
    let tmp = tmp_path(path);
    remove_if_present(&tmp)?;
    let result = (|| {
        let mut file = OpenOptions::new().read(true).write(true).create_new(true).open(&tmp)?;
        reserve(&file, len)?;
        file.write_all(header)?;
        std::fs::rename(&tmp, path)?;
        Ok(file)
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(&tmp);
    }
    result
}

fn map(file: &File, writable: bool) -> Result<MmapRaw> {
    let options = MmapOptions::new();
    Ok(if writable { options.map_raw(file)? } else { options.map_raw_read_only(file)? })
}

fn u32_at(bytes: &[u8], at: usize) -> u32 {
    u32::from_le_bytes(bytes[at..at + 4].try_into().expect("4 bytes"))
}

fn u64_at(bytes: &[u8], at: usize) -> u64 {
    u64::from_le_bytes(bytes[at..at + 8].try_into().expect("8 bytes"))
}

/// The first `len` bytes of a map. Only for headers, which never change once the file has its final name.
fn header_of(map: &MmapRaw, len: usize) -> &[u8] {
    assert!(map.len() >= len);
    // SAFETY: the map is at least `len` bytes long and the header is never written after the file was renamed into place.
    unsafe { std::slice::from_raw_parts(map.as_ptr(), len) }
}

/// One page of `AtomicU64` slots shared between processes (DESIGN.md 14.4).
///
/// Layout: magic `LSQCTR01`, format `u32` LE, slot count `u32` LE, byte-order mark `u64` (native), reserved up to byte
/// 64, then the slots.
pub(crate) struct CounterFile {
    map: MmapRaw,
    slots: usize,
    writable: bool,
}

impl CounterFile {
    /// Creates a counter file with `slots` slots, all 0. Fails if `path` exists.
    pub(crate) fn create(path: &Path, slots: usize) -> Result<Self> {
        assert!(slots <= MAX_SLOTS, "too many counter slots");
        let mut header = Vec::with_capacity(COUNTER_HEADER);
        header.extend_from_slice(COUNTER_MAGIC);
        header.extend_from_slice(&FORMAT.to_le_bytes());
        header.extend_from_slice(&(slots as u32).to_le_bytes());
        header.extend_from_slice(&BYTE_ORDER.to_ne_bytes());
        header.resize(COUNTER_HEADER, 0);
        let file = create_file(path, &header, PAGE)?;
        Ok(Self { map: map(&file, true)?, slots, writable: true })
    }

    /// Opens an existing counter file; `writable` maps it for storing.
    pub(crate) fn open(path: &Path, writable: bool) -> Result<Self> {
        let file = OpenOptions::new().read(true).write(writable).open(path)?;
        if file.metadata()?.len() != PAGE {
            return Err(Error::Corrupt("counter file size"));
        }
        let map = map(&file, writable)?;
        let header = header_of(&map, COUNTER_HEADER);
        if &header[..8] != COUNTER_MAGIC {
            return Err(Error::Corrupt("counter file magic"));
        }
        if u32_at(header, 8) != FORMAT {
            return Err(Error::UnsupportedVersion);
        }
        if header[16..24] != BYTE_ORDER.to_ne_bytes() {
            return Err(Error::Corrupt("counter file from a machine with another byte order"));
        }
        let slots = u32_at(header, 12) as usize;
        if slots > MAX_SLOTS {
            return Err(Error::Corrupt("counter file slot count"));
        }
        Ok(Self { map, slots, writable })
    }

    fn slot(&self, index: usize) -> &AtomicU64 {
        assert!(index < self.slots, "counter slot out of range");
        // SAFETY: the slot lies inside the page-sized map (checked against the slot count), is 8-aligned (the map is page
        // aligned and the slots start at byte 64), lives as long as `self`, and is only ever accessed atomically.
        unsafe { AtomicU64::from_ptr(self.map.as_mut_ptr().add(COUNTER_HEADER + 8 * index).cast()) }
    }

    /// Number of slots.
    pub(crate) fn slots(&self) -> usize {
        self.slots
    }

    /// Loads a slot with acquire ordering.
    pub(crate) fn load(&self, index: usize) -> u64 {
        self.slot(index).load(Ordering::Acquire)
    }

    /// Stores a slot with release ordering. Panics on a read-only counter file.
    pub(crate) fn store(&self, index: usize, value: u64) {
        assert!(self.writable, "store on a read-only counter file");
        self.slot(index).store(value, Ordering::Release);
    }
}

/// Data capacities of the chunks of a region: chunk `i` holds `min(max, min << i)` bytes, or more for an entry that is
/// larger (rounded up to whole pages).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ChunkSizes {
    pub(crate) min: u64,
    pub(crate) max: u64,
}

impl Default for ChunkSizes {
    fn default() -> Self {
        Self { min: 1 << 20, max: 64 << 20 }
    }
}

impl ChunkSizes {
    fn capacity(&self, index: usize, need: u64) -> u64 {
        let min = self.min.max(PAGE);
        let grown = 1u64.checked_shl(index as u32).map_or(u64::MAX, |f| min.saturating_mul(f));
        grown.min(self.max.max(min)).max(need.div_ceil(PAGE) * PAGE)
    }
}

/// A mapped chunk file: a `PAGE` header, then `capacity` data bytes holding the logical bytes from `start`.
///
/// Header: magic `LSQCHNK1`, format `u32` LE, reserved `u32`, byte-order mark `u64` (native), start `u64` LE,
/// capacity `u64` LE.
struct Chunk {
    map: MmapRaw,
    start: u64,
    capacity: u64,
}

impl Chunk {
    fn data(&self) -> *mut u8 {
        // SAFETY: the map is `PAGE + capacity` bytes long (checked when mapped), so the data start is inside it.
        unsafe { self.map.as_mut_ptr().add(PAGE as usize) }
    }

    fn end(&self) -> u64 {
        self.start + self.capacity
    }
}

/// Published bytes of a region, borrowed from a chunk mapping (which stays mapped while this exists).
pub(crate) struct MappedSlice {
    chunk: Arc<Chunk>,
    offset: usize,
    len: usize,
}

impl Deref for MappedSlice {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        // SAFETY: the range was checked to be inside the chunk's data capacity and below a published length when this
        // slice was made; published bytes are never written again, and the `Arc` keeps the mapping alive.
        unsafe { std::slice::from_raw_parts(self.chunk.data().add(self.offset), self.len) }
    }
}

impl std::fmt::Debug for MappedSlice {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MappedSlice").field("start", &(self.chunk.start + self.offset as u64)).field("len", &self.len).finish()
    }
}

/// A range of published bytes: borrowed when it lies in one chunk, copied when it spans several.
#[derive(Debug)]
pub(crate) enum RegionBytes {
    Mapped(MappedSlice),
    Owned(Vec<u8>),
}

impl Deref for RegionBytes {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        match self {
            RegionBytes::Mapped(slice) => slice,
            RegionBytes::Owned(bytes) => bytes,
        }
    }
}

/// A logical append-only byte stream in chunk files `<base>.0000`, `<base>.0001`, ... (DESIGN.md 14.3), published
/// through two slots of a counter file: the length and the chunk count.
///
/// The writer role appends past the published length and publishes, or rolls back. An entry (one `append`) never
/// crosses a chunk: when it does not fit, a new chunk starts at the current logical offset. The reader role maps the
/// published chunks read-only and maps more on [`Region::refresh`].
pub(crate) struct Region {
    base: PathBuf,
    counters: Arc<CounterFile>,
    len_slot: usize,
    chunks_slot: usize,
    sizes: ChunkSizes,
    writable: bool,
    /// Mapped chunks in order. The writer also holds chunks created since the last publish (not counted yet).
    chunks: RwLock<Vec<Arc<Chunk>>>,
    /// Writer: logical length written so far (published or not).
    cursor: Mutex<u64>,
}

/// The path of chunk `index` of the region at `base`.
pub(crate) fn chunk_path(base: &Path, index: usize) -> PathBuf {
    let mut name = base.as_os_str().to_owned();
    name.push(format!(".{index:04}"));
    PathBuf::from(name)
}

impl Region {
    /// Opens the region at `base` whose length and chunk count are in slots `len_slot` and `chunks_slot` of `counters`.
    /// A new region is opened the same way (its counters are 0).
    ///
    /// A writable open recovers: it maps the published chunks writable, deletes chunk files the counters do not publish
    /// (and leftover temporary files), and continues writing at the published length.
    pub(crate) fn open(
        base: &Path,
        counters: Arc<CounterFile>,
        len_slot: usize,
        chunks_slot: usize,
        sizes: ChunkSizes,
        writable: bool,
    ) -> Result<Self> {
        assert!(!writable || counters.writable, "a writable region needs writable counters");
        let region = Self {
            base: base.to_path_buf(),
            counters,
            len_slot,
            chunks_slot,
            sizes,
            writable,
            chunks: RwLock::new(Vec::new()),
            cursor: Mutex::new(0),
        };
        let len = region.refresh()?;
        if writable {
            let counted = region.chunks.read().expect("chunk list lock poisoned").len();
            remove_chunk_files(base, counted)?;
            *region.cursor.lock().expect("cursor lock poisoned") = len;
        }
        Ok(region)
    }

    /// The published length.
    pub(crate) fn published_len(&self) -> u64 {
        self.counters.load(self.len_slot)
    }

    /// Writer: the length written so far, published or not.
    pub(crate) fn written_len(&self) -> u64 {
        *self.cursor.lock().expect("cursor lock poisoned")
    }

    /// Number of mapped chunks (the writer counts unpublished ones too).
    pub(crate) fn chunk_count(&self) -> usize {
        self.chunks.read().expect("chunk list lock poisoned").len()
    }

    /// Loads the published length and maps any newly published chunks. Returns the published length; slices up to it
    /// can be read afterwards.
    pub(crate) fn refresh(&self) -> Result<u64> {
        // The writer stores the chunk count before the length, so a count loaded after the length covers it.
        let len = self.counters.load(self.len_slot);
        let count = usize::try_from(self.counters.load(self.chunks_slot)).map_err(|_| Error::Corrupt("chunk count"))?;
        if self.chunks.read().expect("chunk list lock poisoned").len() >= count {
            return Ok(len);
        }
        let mut chunks = self.chunks.write().expect("chunk list lock poisoned");
        while chunks.len() < count {
            let chunk = self.map_chunk(chunks.len())?;
            let ordered = match chunks.last() {
                Some(previous) => chunk.start > previous.start && chunk.start <= previous.end(),
                None => chunk.start == 0,
            };
            if !ordered {
                return Err(Error::Corrupt("chunk start offset"));
            }
            chunks.push(Arc::new(chunk));
        }
        Ok(len)
    }

    fn map_chunk(&self, index: usize) -> Result<Chunk> {
        let file = OpenOptions::new().read(true).write(self.writable).open(chunk_path(&self.base, index))?;
        let file_len = file.metadata()?.len();
        if file_len < PAGE {
            return Err(Error::Corrupt("chunk file size"));
        }
        let map = map(&file, self.writable)?;
        let header = header_of(&map, 40);
        if &header[..8] != CHUNK_MAGIC {
            return Err(Error::Corrupt("chunk file magic"));
        }
        if u32_at(header, 8) != FORMAT {
            return Err(Error::UnsupportedVersion);
        }
        if header[16..24] != BYTE_ORDER.to_ne_bytes() {
            return Err(Error::Corrupt("chunk file from a machine with another byte order"));
        }
        let (start, capacity) = (u64_at(header, 24), u64_at(header, 32));
        if capacity.checked_add(PAGE).is_none_or(|need| need > map.len() as u64) || start.checked_add(capacity).is_none() {
            return Err(Error::Corrupt("chunk capacity"));
        }
        Ok(Chunk { map, start, capacity })
    }

    fn new_chunk(&self, chunks: &mut Vec<Arc<Chunk>>, start: u64, need: u64) -> Result<Arc<Chunk>> {
        let index = chunks.len();
        let capacity = self.sizes.capacity(index, need);
        let mut header = Vec::with_capacity(40);
        header.extend_from_slice(CHUNK_MAGIC);
        header.extend_from_slice(&FORMAT.to_le_bytes());
        header.extend_from_slice(&0u32.to_le_bytes());
        header.extend_from_slice(&BYTE_ORDER.to_ne_bytes());
        header.extend_from_slice(&start.to_le_bytes());
        header.extend_from_slice(&capacity.to_le_bytes());
        let path = chunk_path(&self.base, index);
        // A file left by an interrupted row (never published) may still be here.
        remove_if_present(&path)?;
        let file = create_file(&path, &header, PAGE + capacity)?;
        let chunk = Arc::new(Chunk { map: map(&file, true)?, start, capacity });
        chunks.push(chunk.clone());
        Ok(chunk)
    }

    /// Writer: appends `bytes` after everything written so far, without publishing them, and returns their logical
    /// offset. The bytes never cross a chunk.
    pub(crate) fn append(&self, bytes: &[u8]) -> Result<u64> {
        assert!(self.writable, "append to a read-only region");
        let mut cursor = self.cursor.lock().expect("cursor lock poisoned");
        let offset = *cursor;
        if bytes.is_empty() {
            return Ok(offset);
        }
        let len = bytes.len() as u64;
        let chunk = {
            let mut chunks = self.chunks.write().expect("chunk list lock poisoned");
            match chunks.last() {
                // every chunk starts at or before the cursor (rollback removes the others)
                Some(last) if offset - last.start + len <= last.capacity => last.clone(),
                _ => self.new_chunk(&mut chunks, offset, len)?,
            }
        };
        // SAFETY: `offset - start + len <= capacity`, so the destination is inside the chunk's data. The bytes at and after
        // the cursor are not published, so no reader holds a slice of them, and the writer is serialized by `cursor`.
        unsafe {
            std::ptr::copy_nonoverlapping(bytes.as_ptr(), chunk.data().add((offset - chunk.start) as usize), bytes.len());
        }
        *cursor = offset + len;
        Ok(offset)
    }

    /// Writer: publishes everything written so far (the chunk count, then the length).
    pub(crate) fn publish(&self) {
        assert!(self.writable, "publish on a read-only region");
        let cursor = self.cursor.lock().expect("cursor lock poisoned");
        let count = self.chunks.read().expect("chunk list lock poisoned").len();
        self.counters.store(self.chunks_slot, count as u64);
        self.counters.store(self.len_slot, *cursor);
    }

    /// Writer: forgets everything written since the last publish and deletes the chunk files created for it.
    pub(crate) fn rollback(&self) -> Result<()> {
        assert!(self.writable, "rollback on a read-only region");
        let mut cursor = self.cursor.lock().expect("cursor lock poisoned");
        *cursor = self.counters.load(self.len_slot);
        let counted = self.counters.load(self.chunks_slot) as usize;
        let mut chunks = self.chunks.write().expect("chunk list lock poisoned");
        if chunks.len() > counted {
            // Unpublished chunks are only referenced here: unmap them before deleting (required on Windows).
            chunks.truncate(counted);
            remove_chunk_files(&self.base, counted)?;
        }
        Ok(())
    }

    /// Writer: whether the written bytes at `offset` (published or not) equal `bytes` (for the whole-string dedup, which
    /// may reuse an entry written earlier in the same row).
    pub(crate) fn written_eq(&self, offset: u64, bytes: &[u8]) -> bool {
        assert!(self.writable, "written_eq on a read-only region");
        let cursor = self.cursor.lock().expect("cursor lock poisoned");
        let len = bytes.len() as u64;
        if offset.checked_add(len).is_none_or(|end| end > *cursor) {
            return false;
        }
        let chunks = self.chunks.read().expect("chunk list lock poisoned");
        let index = chunks.partition_point(|c| c.start <= offset);
        let Some(chunk) = index.checked_sub(1).map(|i| &chunks[i]) else { return false };
        let end = chunks.get(index).map_or(u64::MAX, |c| c.start).min(chunk.end());
        if offset + len > end {
            return false;
        }
        // SAFETY: the range is inside the chunk's data and below the cursor. The cursor lock is held, so the writer cannot
        // write to it now, and bytes below the cursor are never written again until a rollback (which needs this lock).
        let written = unsafe { std::slice::from_raw_parts(chunk.data().add((offset - chunk.start) as usize), bytes.len()) };
        written == bytes
    }

    /// A snapshot of the published bytes up to `limit` (at most the published length), readable without locks. Holds
    /// the chunks it covers mapped.
    pub(crate) fn view(&self, limit: u64) -> RegionView {
        // The chunk count is loaded after the length, so it covers every published byte (see `refresh`).
        let limit = limit.min(self.published_len());
        let counted = self.counters.load(self.chunks_slot) as usize;
        let chunks = self.chunks.read().expect("chunk list lock poisoned");
        // A reader that has not refreshed may have fewer chunks mapped: then the view ends where they end.
        let chunks: Vec<Arc<Chunk>> = chunks.iter().take(counted).cloned().collect();
        let limit = match chunks.last() {
            Some(last) => limit.min(last.end()),
            None => 0,
        };
        RegionView { chunks, limit }
    }

    /// The chunk holding `offset` and the logical end of the readable bytes in it (at most `limit`).
    fn locate(&self, offset: u64, limit: u64) -> Result<(Arc<Chunk>, u64)> {
        let limit = limit.min(self.published_len());
        let chunks = self.chunks.read().expect("chunk list lock poisoned");
        let index = chunks.partition_point(|c| c.start <= offset);
        let chunk = index.checked_sub(1).map(|i| &chunks[i]).ok_or(Error::Corrupt("mapped offset"))?;
        let next = chunks.get(index).map_or(u64::MAX, |c| c.start);
        let end = next.min(limit).min(chunk.end());
        if offset >= end {
            return Err(Error::Corrupt("mapped offset past the published bytes"));
        }
        Ok((chunk.clone(), end))
    }

    /// The published bytes from `offset` to the end of its chunk (or `limit`, or the published length, whichever comes
    /// first). An entry never crosses a chunk, so a whole entry starting at `offset` is in the result.
    pub(crate) fn slice_from(&self, offset: u64, limit: u64) -> Result<MappedSlice> {
        let (chunk, end) = self.locate(offset, limit)?;
        let start = offset - chunk.start;
        Ok(MappedSlice { len: (end - offset) as usize, offset: start as usize, chunk })
    }

    /// The `len` published bytes at `offset`, which must lie in one chunk (one entry, or part of one).
    pub(crate) fn read(&self, offset: u64, len: u64, limit: u64) -> Result<MappedSlice> {
        if len == 0 {
            return Err(Error::Corrupt("empty mapped read"));
        }
        let mut slice = self.slice_from(offset, limit)?;
        if (slice.len as u64) < len {
            return Err(Error::Corrupt("mapped read past the end of its chunk"));
        }
        slice.len = len as usize;
        Ok(slice)
    }

    /// The published bytes `start..end`, borrowed when they lie in one chunk and copied otherwise.
    pub(crate) fn range(&self, start: u64, end: u64, limit: u64) -> Result<RegionBytes> {
        if start >= end {
            return Ok(RegionBytes::Owned(Vec::new()));
        }
        let first = self.slice_from(start, limit)?;
        if first.len as u64 >= end - start {
            return Ok(RegionBytes::Mapped(self.read(start, end - start, limit)?));
        }
        let mut out = Vec::with_capacity((end - start) as usize);
        let mut at = start;
        while at < end {
            let slice = self.slice_from(at, limit)?;
            let take = (slice.len as u64).min(end - at) as usize;
            out.extend_from_slice(&slice[..take]);
            at += take as u64;
        }
        Ok(RegionBytes::Owned(out))
    }
}

/// A snapshot of the published bytes `0..len()` of a region (see [`Region::view`]). Lookups take no lock.
pub(crate) struct RegionView {
    chunks: Vec<Arc<Chunk>>,
    limit: u64,
}

impl RegionView {
    /// Number of bytes in the view.
    pub(crate) fn len(&self) -> u64 {
        self.limit
    }

    /// The bytes from `offset` to the end of its chunk or of the view. A whole entry starting at `offset` is in it.
    pub(crate) fn slice_from(&self, offset: u64) -> Result<&[u8]> {
        let index = self.chunks.partition_point(|c| c.start <= offset);
        let chunk = index.checked_sub(1).map(|i| &self.chunks[i]).ok_or(Error::Corrupt("mapped offset"))?;
        let end = self.chunks.get(index).map_or(u64::MAX, |c| c.start).min(self.limit).min(chunk.end());
        if offset >= end {
            return Err(Error::Corrupt("mapped offset past the published bytes"));
        }
        // SAFETY: `offset..end` is inside the chunk's data and below a published length; published bytes are never
        // written again, and the view holds the chunk mapped for as long as the slice borrows it.
        Ok(unsafe { std::slice::from_raw_parts(chunk.data().add((offset - chunk.start) as usize), (end - offset) as usize) })
    }

    /// The `len` bytes at `offset`, which must lie in one chunk.
    pub(crate) fn read(&self, offset: u64, len: u64) -> Result<&[u8]> {
        let slice = self.slice_from(offset)?;
        slice.get(..len as usize).ok_or(Error::Corrupt("mapped read past the end of its chunk"))
    }

    /// The bytes `start..end`, borrowed when they lie in one chunk, copied otherwise.
    pub(crate) fn range(&self, start: u64, end: u64) -> Result<std::borrow::Cow<'_, [u8]>> {
        if start >= end {
            return Ok(std::borrow::Cow::Borrowed(&[]));
        }
        let first = self.slice_from(start)?;
        if first.len() as u64 >= end - start {
            return Ok(std::borrow::Cow::Borrowed(&first[..(end - start) as usize]));
        }
        let mut out = Vec::with_capacity((end - start) as usize);
        let mut at = start;
        while at < end {
            let slice = self.slice_from(at)?;
            let take = (slice.len() as u64).min(end - at) as usize;
            out.extend_from_slice(&slice[..take]);
            at += take as u64;
        }
        Ok(std::borrow::Cow::Owned(out))
    }
}

/// Deletes the chunk files of the region at `base` from index `from` on, and their temporary files. Chunk files are
/// created in order, so this stops at the first index that has neither.
pub(crate) fn remove_chunk_files(base: &Path, from: usize) -> Result<()> {
    let mut index = from;
    loop {
        let path = chunk_path(base, index);
        let removed = remove_if_present(&path)?;
        let removed_tmp = remove_if_present(&tmp_path(&path))?;
        if !removed && !removed_tmp {
            return Ok(());
        }
        index += 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::process::Command;
    use std::time::{Duration, Instant};

    const SMALL: ChunkSizes = ChunkSizes { min: 4096, max: 16384 };

    fn writer(dir: &Path, sizes: ChunkSizes) -> (Arc<CounterFile>, Region) {
        let counters = Arc::new(CounterFile::create(&dir.join("r.ctr"), 4).unwrap());
        let region = Region::open(&dir.join("r"), counters.clone(), 0, 1, sizes, true).unwrap();
        (counters, region)
    }

    fn reader(dir: &Path, sizes: ChunkSizes) -> Region {
        let counters = Arc::new(CounterFile::open(&dir.join("r.ctr"), false).unwrap());
        Region::open(&dir.join("r"), counters, 0, 1, sizes, false).unwrap()
    }

    /// A test record: `[u64 number][u32 payload length][payload]`, the payload bytes derived from the number.
    fn record(number: u64, payload: usize) -> Vec<u8> {
        let mut out = Vec::with_capacity(12 + payload);
        out.extend_from_slice(&number.to_le_bytes());
        out.extend_from_slice(&(payload as u32).to_le_bytes());
        out.extend((0..payload).map(|j| (number as u8).wrapping_add(j as u8)));
        out
    }

    /// Parses the record at the start of `bytes`, checks its payload and returns (number, length in bytes).
    fn parse(bytes: &[u8]) -> (u64, u64) {
        let number = u64_at(bytes, 0);
        let payload = u32_at(bytes, 8) as usize;
        let body = &bytes[12..12 + payload];
        assert!(body.iter().enumerate().all(|(j, &b)| b == (number as u8).wrapping_add(j as u8)), "payload of record {number}");
        (number, 12 + payload as u64)
    }

    fn read_all(region: &Region, limit: u64) -> Vec<u64> {
        let mut at = 0;
        let mut numbers = Vec::new();
        while at < limit {
            let (number, len) = parse(&region.slice_from(at, limit).unwrap());
            numbers.push(number);
            at += len;
        }
        assert_eq!(at, limit);
        numbers
    }

    fn payload_of(number: u64) -> usize {
        if number % 97 == 5 { 40_000 } else { (number % 300) as usize }
    }

    #[test]
    fn counter_file_round_trip_and_checks() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("c");
        let counters = CounterFile::create(&path, 3).unwrap();
        counters.store(2, 42);
        assert!(matches!(CounterFile::create(&path, 3), Err(Error::Io(e)) if e.kind() == ErrorKind::AlreadyExists));
        let reader = CounterFile::open(&path, false).unwrap();
        assert_eq!((reader.slots(), reader.load(0), reader.load(2)), (3, 0, 42));
        counters.store(0, 7);
        assert_eq!(reader.load(0), 7, "a store is seen through another mapping");
        drop((counters, reader));

        let mut bytes = std::fs::read(&path).unwrap();
        bytes[0] = b'X';
        std::fs::write(&path, &bytes).unwrap();
        assert!(matches!(CounterFile::open(&path, false), Err(Error::Corrupt(_))));
        bytes[0] = b'L';
        bytes[8] = 9;
        std::fs::write(&path, &bytes).unwrap();
        assert!(matches!(CounterFile::open(&path, false), Err(Error::UnsupportedVersion)));
        std::fs::write(&path, &bytes[..100]).unwrap();
        assert!(matches!(CounterFile::open(&path, false), Err(Error::Corrupt(_))));
    }

    #[test]
    fn chunk_capacities_grow_and_fit_large_entries() {
        let sizes = ChunkSizes { min: 4096, max: 32768 };
        let caps: Vec<u64> = (0..6).map(|i| sizes.capacity(i, 1)).collect();
        assert_eq!(caps, [4096, 8192, 16384, 32768, 32768, 32768]);
        assert_eq!(sizes.capacity(0, 50_000), 53_248);
        assert_eq!(sizes.capacity(200, 1), 32768);
        assert_eq!(ChunkSizes { min: 1, max: 1 }.capacity(0, 1), PAGE);
    }

    #[test]
    fn appends_are_invisible_until_published() {
        let dir = tempfile::tempdir().unwrap();
        let (_counters, region) = writer(dir.path(), SMALL);
        let other = reader(dir.path(), SMALL);
        assert_eq!(region.append(&record(0, 10)).unwrap(), 0);
        assert_eq!((region.written_len(), region.published_len(), other.refresh().unwrap()), (22, 0, 0));
        assert!(region.slice_from(0, u64::MAX).is_err(), "unpublished bytes cannot be read, even by the writer");
        region.publish();
        assert_eq!(other.refresh().unwrap(), 22);
        assert_eq!(read_all(&other, 22), [0]);
        assert_eq!(read_all(&region, region.published_len()), [0]);
        assert_eq!(region.append(&[]).unwrap(), 22);
    }

    #[test]
    fn entries_never_cross_chunks_and_readers_follow_new_chunks() {
        let dir = tempfile::tempdir().unwrap();
        let (_counters, region) = writer(dir.path(), SMALL);
        let other = reader(dir.path(), SMALL);
        let mut expected_len = 0;
        for number in 0..400 {
            let bytes = record(number, payload_of(number));
            let offset = region.append(&bytes).unwrap();
            assert_eq!(offset, expected_len, "logical offsets are dense");
            expected_len += bytes.len() as u64;
            if number % 7 == 0 {
                region.publish();
            }
        }
        region.publish();
        assert!(region.chunk_count() > 10);
        let len = other.refresh().unwrap();
        assert_eq!(len, expected_len);
        assert_eq!(other.chunk_count(), region.chunk_count());
        assert_eq!(read_all(&other, len), (0..400).collect::<Vec<_>>());
        // a range across chunks is copied, one inside a chunk is borrowed
        let whole = other.range(0, len, len).unwrap();
        assert!(matches!(whole, RegionBytes::Owned(_)));
        assert_eq!(whole.len() as u64, len);
        assert!(matches!(other.range(0, 12, len).unwrap(), RegionBytes::Mapped(_)));
        assert_eq!(&*other.range(0, 12, len).unwrap(), &whole[..12]);
        // a limit below the published length hides the rest
        assert_eq!(read_all(&other, 12 + 13), [0, 1]);
        assert!(other.read(0, len, len).is_err(), "a read must stay in one chunk");
    }

    #[test]
    fn rollback_forgets_unpublished_bytes_and_chunks() {
        let dir = tempfile::tempdir().unwrap();
        let (_counters, region) = writer(dir.path(), SMALL);
        region.append(&record(0, 3000)).unwrap();
        region.publish();
        // does not fit in the first chunk: creates chunk 1, which is never published
        region.append(&record(1, 3000)).unwrap();
        assert!(chunk_path(&dir.path().join("r"), 1).exists());
        region.rollback().unwrap();
        assert!(!chunk_path(&dir.path().join("r"), 1).exists());
        assert_eq!((region.written_len(), region.chunk_count()), (3012, 1));
        // writing continues as if the rolled-back row never happened
        region.append(&record(2, 50)).unwrap();
        region.append(&record(3, 3000)).unwrap();
        region.publish();
        assert_eq!(read_all(&region, region.published_len()), [0, 2, 3]);
        assert_eq!(read_all(&reader(dir.path(), SMALL), region.published_len()), [0, 2, 3]);
    }

    #[test]
    fn reopening_a_writer_recovers_the_published_state() {
        let dir = tempfile::tempdir().unwrap();
        {
            let (_counters, region) = writer(dir.path(), SMALL);
            for number in 0..50 {
                region.append(&record(number, payload_of(number))).unwrap();
            }
            region.publish();
            // unpublished: a partial row in the last chunk and a new chunk, as left by a crash
            region.append(&record(50, 100)).unwrap();
            region.append(&record(51, 20_000)).unwrap();
            std::fs::write(tmp_path(&chunk_path(&dir.path().join("r"), region.chunk_count())), b"junk").unwrap();
        }
        let counters = Arc::new(CounterFile::open(&dir.path().join("r.ctr"), true).unwrap());
        let region = Region::open(&dir.path().join("r"), counters, 0, 1, SMALL, true).unwrap();
        let published = region.published_len();
        assert_eq!(region.written_len(), published);
        assert!(!chunk_path(&dir.path().join("r"), region.chunk_count()).exists());
        assert!(!tmp_path(&chunk_path(&dir.path().join("r"), region.chunk_count() + 1)).exists());
        assert_eq!(read_all(&region, published), (0..50).collect::<Vec<_>>());
        region.append(&record(52, 10)).unwrap();
        region.publish();
        let mut expected: Vec<u64> = (0..50).collect();
        expected.push(52);
        assert_eq!(read_all(&reader(dir.path(), SMALL), region.published_len()), expected);
    }

    #[test]
    fn corrupt_chunk_headers_are_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let (_counters, region) = writer(dir.path(), SMALL);
        region.append(&record(0, 3000)).unwrap();
        region.append(&record(1, 3000)).unwrap();
        region.publish();
        drop(region);
        let path = chunk_path(&dir.path().join("r"), 1);
        let mut bytes = std::fs::read(&path).unwrap();
        bytes[24..32].copy_from_slice(&0u64.to_le_bytes()); // start offset 0 for the second chunk
        std::fs::write(&path, &bytes).unwrap();
        let counters = Arc::new(CounterFile::open(&dir.path().join("r.ctr"), false).unwrap());
        assert!(matches!(Region::open(&dir.path().join("r"), counters, 0, 1, SMALL, false), Err(Error::Corrupt(_))));
    }

    /// Entry point of the child processes of the cross-process tests; does nothing in a normal test run.
    #[test]
    fn child_process_entry() {
        let Ok(spec) = std::env::var("LSQ_MAPPED_CHILD") else { return };
        let (role, dir) = spec.split_once(':').unwrap();
        let dir = Path::new(dir);
        let (counters, region) = writer(dir, SMALL);
        match role {
            "write" => {
                for number in 0..3000 {
                    region.append(&record(number, payload_of(number))).unwrap();
                    if number % 5 == 0 {
                        region.publish();
                    }
                }
                region.publish();
                counters.store(2, 1);
            }
            "abort" => {
                for number in 0..10 {
                    region.append(&record(number, 1000)).unwrap();
                }
                region.publish();
                counters.store(2, region.chunk_count() as u64);
                // half a row in a new chunk, then the process dies without unwinding
                region.append(&record(10, 20_000)).unwrap();
                std::process::abort();
            }
            other => panic!("unknown child role {other}"),
        }
    }

    fn spawn_child(role: &str, dir: &Path) -> std::process::Child {
        Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "mapped::tests::child_process_entry", "--test-threads=1", "--nocapture"])
            .env("LSQ_MAPPED_CHILD", format!("{role}:{}", dir.display()))
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .unwrap()
    }

    #[test]
    fn reader_process_follows_a_writer_process() {
        let dir = tempfile::tempdir().unwrap();
        let mut child = spawn_child("write", dir.path());
        let deadline = Instant::now() + Duration::from_secs(60);
        while !dir.path().join("r.ctr").exists() {
            assert!(Instant::now() < deadline, "the writer process did not start");
            std::thread::sleep(Duration::from_millis(1));
        }
        let counters = Arc::new(CounterFile::open(&dir.path().join("r.ctr"), false).unwrap());
        let region = Region::open(&dir.path().join("r"), counters.clone(), 0, 1, SMALL, false).unwrap();
        let (mut at, mut next, mut refreshes) = (0, 0, 0);
        loop {
            assert!(Instant::now() < deadline, "the reader did not see every record");
            let done = counters.load(2) == 1;
            let len = region.refresh().unwrap();
            refreshes += 1;
            while at < len {
                let (number, size) = parse(&region.slice_from(at, len).unwrap());
                assert_eq!(number, next, "records arrive complete and in order");
                at += size;
                next += 1;
            }
            assert_eq!(at, len, "the published length ends on a record boundary");
            if done && next == 3000 {
                break;
            }
        }
        assert!(child.wait().unwrap().success());
        assert!(refreshes > 1);
    }

    #[test]
    fn a_writer_killed_in_the_middle_of_a_row_loses_only_that_row() {
        let dir = tempfile::tempdir().unwrap();
        let status = spawn_child("abort", dir.path()).wait().unwrap();
        assert!(!status.success(), "the child aborts");
        let base = dir.path().join("r");
        let counters = Arc::new(CounterFile::open(&dir.path().join("r.ctr"), true).unwrap());
        let counted = counters.load(2) as usize;
        assert!(chunk_path(&base, counted).exists(), "the unpublished chunk was left behind");

        // a reader sees the published rows while the writer is gone
        let other =
            Region::open(&base, Arc::new(CounterFile::open(&dir.path().join("r.ctr"), false).unwrap()), 0, 1, SMALL, false).unwrap();
        assert_eq!(read_all(&other, other.refresh().unwrap()), (0..10).collect::<Vec<_>>());

        let region = Region::open(&base, counters, 0, 1, SMALL, true).unwrap();
        assert!(!chunk_path(&base, counted).exists(), "recovery deletes the unpublished chunk");
        assert_eq!(region.written_len(), 10 * 1012);
        region.append(&record(11, 20_000)).unwrap();
        region.publish();
        let mut expected: Vec<u64> = (0..10).collect();
        expected.push(11);
        assert_eq!(other.refresh().unwrap(), region.published_len());
        assert_eq!(read_all(&other, region.published_len()), expected);
    }
}
