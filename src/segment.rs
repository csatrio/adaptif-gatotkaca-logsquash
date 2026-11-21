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
use crate::types::OwnedValue;
use crate::words::WordStore;
use std::collections::HashMap;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::{Arc, Mutex};

const MAGIC: &[u8; 8] = b"LSQSEG01";
const FOOTER_MAGIC: &[u8; 8] = b"LSQSEGEN";
const FOOTER_LEN: u64 = 8 + 4 + 4 + 8 + 8;
const CACHE_LIMIT: usize = 512;

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

/// Writes the buffered rows of a dataframe as a segment file and syncs it to disk.
pub(crate) fn write_segment(path: &Path, data: &Inner, options: WriteOptions) -> Result<()> {
    let file = std::fs::OpenOptions::new().write(true).create_new(true).open(path)?;
    let mut writer = BlockWriter { out: std::io::BufWriter::new(file), pos: 0, level: options.zstd_level };
    writer.out.write_all(MAGIC)?;
    writer.pos = MAGIC.len() as u64;

    let block_rows = options.block_rows.max(1);
    let rows = data.rows as usize;
    let columns = data.columns.len();
    let mut column_blocks = Vec::new();
    let mut payload = Vec::new();
    for first in (0..rows).step_by(block_rows) {
        let last = (first + block_rows).min(rows);
        for column in &data.columns {
            let cells = &column.cells[first..last];
            let nulls: Vec<bool> = (first..last).map(|row| column.nulls.get(row)).collect();
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
    for (i, &start) in data.block_starts.iter().enumerate() {
        let end = data.block_starts.get(i + 1).copied().unwrap_or(data.row_heap.len() as u64);
        heap_blocks.push((start, writer.put(&data.row_heap[start as usize..end as usize])?));
    }

    let index = Index { rows: data.rows, block_rows: block_rows as u32, columns: columns as u32, column_blocks, heap_blocks };
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

/// Reads rows of one segment file lazily, block by block.
pub(crate) struct SegmentReader {
    file: Mutex<File>,
    index: Index,
    cache: Mutex<HashMap<CacheKey, Cached>>,
}

impl SegmentReader {
    /// Opens a segment and verifies its header, footer and index.
    pub(crate) fn open(path: &Path, columns: usize) -> Result<Self> {
        let mut file = File::open(path)?;
        let len = file.metadata()?.len();
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
        Ok(Self { file: Mutex::new(file), index, cache: Mutex::new(HashMap::new()) })
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
            let mut file = self.file.lock().expect("segment file lock poisoned");
            file.seek(SeekFrom::Start(block.offset))?;
            file.read_exact(&mut comp)?;
        }
        if xxh3(&comp, 0) != block.checksum {
            return Err(Error::Corrupt("block checksum"));
        }
        zstd::bulk::decompress(&comp, block.raw_len as usize).map_err(|_| Error::Corrupt("block does not decompress"))
    }

    fn remember(&self, key: CacheKey, value: Cached) {
        let mut cache = self.cache.lock().expect("segment cache lock poisoned");
        if cache.len() >= CACHE_LIMIT {
            cache.clear();
        }
        cache.insert(key, value);
    }

    fn column_data(&self, block: u64, column: u32) -> Result<Arc<ColumnData>> {
        let key = CacheKey::Column { block, column };
        if let Some(Cached::Column(data)) = self.cache.lock().expect("segment cache lock poisoned").get(&key) {
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
        self.remember(key, Cached::Column(Arc::clone(&data)));
        Ok(data)
    }

    fn heap_block(&self, index: usize) -> Result<Arc<Vec<u8>>> {
        let key = CacheKey::Heap(index);
        if let Some(Cached::Heap(data)) = self.cache.lock().expect("segment cache lock poisoned").get(&key) {
            return Ok(Arc::clone(data));
        }
        let data = Arc::new(self.read_block(&self.index.heap_blocks[index].1)?);
        self.remember(key, Cached::Heap(Arc::clone(&data)));
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

    fn cell(&self, row: u64, column: usize) -> Result<(u64, bool)> {
        if row >= self.index.rows || column >= self.index.columns as usize {
            return Err(Error::RowOutOfRange);
        }
        let block_rows = u64::from(self.index.block_rows);
        let data = self.column_data(row / block_rows, column as u32)?;
        let i = (row % block_rows) as usize;
        Ok((data.cells[i], data.is_null(i)))
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
        let reader = SegmentReader::open(&path, 5).unwrap();
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
        let reader = SegmentReader::open(&write(&frame, dir.path(), 40), 5).unwrap();
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
        let reader = SegmentReader::open(&write(&empty, dir.path(), 64), 5).unwrap();
        assert_eq!(reader.rows(), 0);
        let nulls = DataFrame::new(schema(), shared.clone());
        nulls.push_row(&[Value::Null, Value::Null, Value::Null, Value::Null, Value::Null]).unwrap();
        let dir2 = tempfile::tempdir().unwrap();
        let reader = SegmentReader::open(&write(&nulls, dir2.path(), 64), 5).unwrap();
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
        let reader = SegmentReader::open(&path, 5).unwrap();
        let all_ok = (0..300).all(|row| reader.row(frame.schema(), &store, row).is_ok());
        assert!(!all_ok);

        // damage to the index, the footer or the length is caught when opening
        for position in [good.len() - 1, good.len() - 20, good.len() - 40] {
            let mut bad = good.clone();
            bad[position] ^= 0x55;
            std::fs::write(&path, &bad).unwrap();
            assert!(SegmentReader::open(&path, 5).is_err(), "byte {position}");
        }
        std::fs::write(&path, &good[..good.len() - 5]).unwrap();
        assert!(SegmentReader::open(&path, 5).is_err());
        std::fs::write(&path, &good).unwrap();
        assert!(SegmentReader::open(&path, 4).is_err()); // wrong column count
        assert!(SegmentReader::open(&path, 5).is_ok());
    }
}
