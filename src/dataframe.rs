//! The in-memory dataframe (DESIGN.md 6): typed columns of `u64` cells, with strings and blobs in a row heap.

use crate::cell;
use crate::dictionary::xxh3;
use crate::error::{Error, Result};
use crate::heap::{Heap, MemHeap};
use crate::schema::{Column, Schema};
use crate::splitter::{DefaultSplitter, JsonEscapeSplitter, Splitter};
use crate::tokenizer::{Tokenizer, TokenizerOptions, decode_with};
use crate::types::{ColumnType, OwnedValue, Value};
use crate::varint;
use crate::words::{SharedWords, WordStore};
use bytes::{BufMut, BytesMut};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

/// Options of a [`DataFrame`].
#[derive(Debug, Clone)]
pub struct DataFrameOptions {
    /// Thresholds of the tokenizer.
    pub tokenizer: TokenizerOptions,
    /// Maximum entries of the whole-string dedup map; once full, new strings are stored but not remembered.
    pub dedup_entries: usize,
}

impl Default for DataFrameOptions {
    fn default() -> Self {
        Self { tokenizer: TokenizerOptions::default(), dedup_entries: 1_000_000 }
    }
}

/// One bit per row, set for null.
#[derive(Default)]
struct NullBits {
    words: Vec<u64>,
    len: usize,
}

impl NullBits {
    fn push(&mut self, null: bool) {
        if self.len % 64 == 0 {
            self.words.push(0);
        }
        if null {
            *self.words.last_mut().expect("a word was just pushed") |= 1 << (self.len % 64);
        }
        self.len += 1;
    }

    fn get(&self, index: usize) -> bool {
        (self.words[index / 64] >> (index % 64)) & 1 == 1
    }
}

#[derive(Default)]
struct ColumnData {
    /// Fixed-size types: the encoded value. String/Blob: row-heap offset + 1.
    cells: Vec<u64>,
    nulls: NullBits,
}

#[derive(Default)]
struct Inner {
    columns: Vec<ColumnData>,
    rows: u64,
    /// Symbol streams of strings and `[varint length][bytes]` blobs, back to back.
    row_heap: BytesMut,
    /// Whole-string dedup: hash of an entry -> (offset, length).
    dedup: HashMap<u64, (u64, u32)>,
    scratch: BytesMut,
}

/// A cell prepared for insertion.
enum Prepared {
    Cell(Option<u64>),
    Null,
    /// A range of the scratch buffer to store in the row heap.
    Entry {
        start: usize,
        end: usize,
        dedup: bool,
    },
}

/// A table of typed columns sharing a word store with other dataframes.
pub struct DataFrame<H: Heap = MemHeap> {
    schema: Arc<Schema>,
    tokenizer: Tokenizer<H>,
    dedup_entries: usize,
    inner: RwLock<Inner>,
}

impl<H: Heap> DataFrame<H> {
    /// An empty dataframe with default options.
    pub fn new(schema: Schema, words: SharedWords<H>) -> Self {
        Self::with_options(schema, words, DataFrameOptions::default())
    }

    /// An empty dataframe.
    pub fn with_options(schema: Schema, words: SharedWords<H>, options: DataFrameOptions) -> Self {
        let columns = (0..schema.len()).map(|_| ColumnData::default()).collect();
        Self {
            schema: Arc::new(schema),
            tokenizer: Tokenizer::new(words, options.tokenizer),
            dedup_entries: options.dedup_entries,
            inner: RwLock::new(Inner { columns, ..Inner::default() }),
        }
    }

    /// The schema.
    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    /// Number of rows.
    pub fn len(&self) -> u64 {
        self.inner.read().expect("dataframe lock poisoned").rows
    }

    /// Whether there are no rows.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Appends a row and returns its row number. On any error nothing is added (not even new words).
    pub fn push_row(&self, row: &[Value<'_>]) -> Result<u64> {
        let columns = self.schema.columns();
        if row.len() != columns.len() {
            return Err(Error::ColumnCount { expected: columns.len(), got: row.len() });
        }
        let mut guard = self.inner.write().expect("dataframe lock poisoned");
        let Inner { columns: data, rows, row_heap, dedup, scratch } = &mut *guard;
        let mut entries = std::mem::take(scratch);
        entries.clear();

        let mut words = self.tokenizer.words().write().expect("word store lock poisoned");
        let checkpoint = words.checkpoint();
        let mut prepared = Vec::with_capacity(columns.len());
        for (column, value) in columns.iter().zip(row) {
            match self.prepare(&mut words, &mut entries, column, value) {
                Ok(p) => prepared.push(p),
                Err(e) => {
                    words.rollback(checkpoint);
                    *scratch = entries;
                    return Err(e);
                }
            }
        }
        drop(words);

        for (column, p) in data.iter_mut().zip(prepared) {
            match p {
                Prepared::Cell(Some(cell)) => {
                    column.cells.push(cell);
                    column.nulls.push(false);
                }
                Prepared::Cell(None) | Prepared::Null => {
                    column.cells.push(0);
                    column.nulls.push(true);
                }
                Prepared::Entry { start, end, dedup: remember } => {
                    let offset = store_entry(row_heap, dedup, self.dedup_entries, &entries[start..end], remember);
                    column.cells.push(offset + 1);
                    column.nulls.push(false);
                }
            }
        }
        *rows += 1;
        *scratch = entries;
        Ok(*rows - 1)
    }

    fn prepare(&self, words: &mut WordStore<H>, entries: &mut BytesMut, column: &Column, value: &Value<'_>) -> Result<Prepared> {
        let start = entries.len();
        match (column.ty, value) {
            (ty, _) if !ty.is_variable() => Ok(Prepared::Cell(cell::encode(ty, &column.name, value)?)),
            (_, Value::Null) => Ok(Prepared::Null),
            (ColumnType::String, Value::Str(text)) => {
                let splitter: &dyn Splitter = if column.flags.json_escapes { &JsonEscapeSplitter } else { &DefaultSplitter };
                self.tokenizer.encode_with(words, text, splitter, entries)?;
                Ok(Prepared::Entry { start, end: entries.len(), dedup: true })
            }
            (ColumnType::Blob, Value::Blob(bytes)) => {
                varint::put(entries, bytes.len() as u64);
                entries.put_slice(bytes);
                Ok(Prepared::Entry { start, end: entries.len(), dedup: false })
            }
            _ => Err(Error::TypeMismatch { column: column.name.clone() }),
        }
    }

    /// Reads one row.
    pub fn row(&self, row: u64) -> Result<Vec<OwnedValue>> {
        let inner = self.inner.read().expect("dataframe lock poisoned");
        let words = self.tokenizer.words().read().expect("word store lock poisoned");
        (0..self.schema.len()).map(|column| self.value_at(&inner, &words, row, column)).collect()
    }

    /// Reads one value.
    pub fn value(&self, row: u64, column: usize) -> Result<OwnedValue> {
        let inner = self.inner.read().expect("dataframe lock poisoned");
        let words = self.tokenizer.words().read().expect("word store lock poisoned");
        self.value_at(&inner, &words, row, column)
    }

    /// Writes the text of a string cell into `out` without allocating a `String`. Returns `false` for null.
    pub fn write_string(&self, row: u64, column: usize, out: &mut BytesMut) -> Result<bool> {
        let inner = self.inner.read().expect("dataframe lock poisoned");
        let words = self.tokenizer.words().read().expect("word store lock poisoned");
        let col = self.check(&inner, row, column)?;
        if col.ty != ColumnType::String {
            return Err(Error::TypeMismatch { column: col.name.clone() });
        }
        match inner.columns[column].cells[row as usize] {
            0 => Ok(false),
            cell => {
                decode_with(&words, &inner.row_heap[(cell - 1) as usize..], out)?;
                Ok(true)
            }
        }
    }

    /// An iterator over all rows in insertion order; each step reads one row.
    pub fn rows(&self) -> Rows<'_, H> {
        Rows { frame: self, next: 0 }
    }

    /// Size of the row heap in bytes.
    pub fn row_heap_len(&self) -> u64 {
        self.inner.read().expect("dataframe lock poisoned").row_heap.len() as u64
    }

    /// A copy of the row heap (for measuring tools).
    pub fn row_heap_snapshot(&self) -> Vec<u8> {
        self.inner.read().expect("dataframe lock poisoned").row_heap.to_vec()
    }

    /// Entries in the whole-string dedup map.
    pub fn dedup_len(&self) -> usize {
        self.inner.read().expect("dataframe lock poisoned").dedup.len()
    }

    fn check(&self, inner: &Inner, row: u64, column: usize) -> Result<&Column> {
        let col = self.schema.columns().get(column).ok_or(Error::ColumnOutOfRange)?;
        if row >= inner.rows {
            return Err(Error::RowOutOfRange);
        }
        Ok(col)
    }

    fn value_at(&self, inner: &Inner, words: &WordStore<H>, row: u64, column: usize) -> Result<OwnedValue> {
        let col = self.check(inner, row, column)?;
        let data = &inner.columns[column];
        let cell = data.cells[row as usize];
        if data.nulls.get(row as usize) {
            return Ok(OwnedValue::Null);
        }
        match col.ty {
            ColumnType::String => {
                let mut text = Vec::new();
                decode_with(words, &inner.row_heap[(cell - 1) as usize..], &mut text)?;
                String::from_utf8(text).map(OwnedValue::Str).map_err(|_| Error::Corrupt("string is not UTF-8"))
            }
            ColumnType::Blob => {
                let heap: &[u8] = &inner.row_heap;
                let mut pos = (cell - 1) as usize;
                let len = varint::get(heap, &mut pos)? as usize;
                let bytes = heap.get(pos..pos.checked_add(len).ok_or(Error::Corrupt("blob length"))?);
                bytes.map(|b| OwnedValue::Blob(b.to_vec())).ok_or(Error::Corrupt("blob is cut off"))
            }
            ty => Ok(cell::decode(ty, cell)),
        }
    }
}

/// Appends `bytes` to the row heap (or reuses an identical earlier entry when `remember` is set); returns its offset.
fn store_entry(heap: &mut BytesMut, map: &mut HashMap<u64, (u64, u32)>, cap: usize, bytes: &[u8], remember: bool) -> u64 {
    let key = remember.then(|| xxh3(bytes, 0));
    if let Some(key) = key {
        if let Some(&(offset, len)) = map.get(&key) {
            let start = offset as usize;
            if len as usize == bytes.len() && &heap[start..start + bytes.len()] == bytes {
                return offset;
            }
        }
    }
    let offset = heap.len() as u64;
    heap.put_slice(bytes);
    if let Some(key) = key {
        if map.len() < cap {
            map.entry(key).or_insert((offset, bytes.len() as u32));
        }
    }
    offset
}

/// Iterator over the rows of a [`DataFrame`].
pub struct Rows<'a, H: Heap> {
    frame: &'a DataFrame<H>,
    next: u64,
}

impl<H: Heap> Iterator for Rows<'_, H> {
    type Item = Result<Vec<OwnedValue>>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.next >= self.frame.len() {
            return None;
        }
        self.next += 1;
        Some(self.frame.row(self.next - 1))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dictionary::{Dictionary, ENTRY_COST_BYTES};

    fn words(max_entries: usize) -> SharedWords {
        WordStore::new(MemHeap::new(), Dictionary::new(max_entries * ENTRY_COST_BYTES)).into_shared()
    }

    fn log_schema() -> Schema {
        Schema::new(vec![
            Column::new("id", ColumnType::U64),
            Column::new("msg", ColumnType::String),
            Column::new("data", ColumnType::String).json_escapes(),
            Column::new("score", ColumnType::F64),
        ])
        .unwrap()
    }

    fn all_types_schema() -> Schema {
        use ColumnType::*;
        let types = [
            I8,
            I16,
            I32,
            I64,
            U8,
            U16,
            U32,
            U64,
            F32,
            F64,
            Bool,
            Char,
            TimestampSec,
            TimestampMs,
            TimestampUs,
            TimestampNs,
            Date,
            TimeOfDay,
            TimestampTz,
            String,
            Blob,
        ];
        Schema::new(types.iter().map(|&t| Column::new(format!("c{}", t.code()), t)).collect()).unwrap()
    }

    #[test]
    fn every_type_round_trips_including_nulls() {
        let frame = DataFrame::new(all_types_schema(), words(1000));
        let row = vec![
            OwnedValue::I8(-8),
            OwnedValue::I16(-1600),
            OwnedValue::I32(i32::MIN),
            OwnedValue::I64(i64::MAX),
            OwnedValue::U8(255),
            OwnedValue::U16(65_535),
            OwnedValue::U32(u32::MAX),
            OwnedValue::U64(u64::MAX),
            OwnedValue::F32(-0.0),
            OwnedValue::F64(1.5e300),
            OwnedValue::Bool(true),
            OwnedValue::Char('日'),
            OwnedValue::TimestampSec(-62_135_596_800),
            OwnedValue::TimestampMs(1_795_323_600_123),
            OwnedValue::TimestampUs(1),
            OwnedValue::TimestampNs(i64::MIN),
            OwnedValue::Date(-25_567),
            OwnedValue::TimeOfDay(86_399_999_999_999),
            OwnedValue::TimestampTz { millis: 1_700_000_000_000, offset_minutes: 420 },
            OwnedValue::Str("hello, wörld".into()),
            OwnedValue::Blob(vec![0, 1, 2, 255]),
        ];
        let values: Vec<Value> = row.iter().map(OwnedValue::as_value).collect();
        assert_eq!(frame.push_row(&values).unwrap(), 0);
        assert_eq!(frame.push_row(&vec![Value::Null; 21]).unwrap(), 1);
        assert_eq!(frame.row(0).unwrap(), row);
        assert_eq!(frame.row(1).unwrap(), vec![OwnedValue::Null; 21]);
        assert_eq!(frame.len(), 2);
    }

    #[test]
    fn floats_are_bit_exact() {
        let schema = Schema::new(vec![Column::new("f", ColumnType::F64), Column::new("g", ColumnType::F32)]).unwrap();
        let frame = DataFrame::new(schema, words(10));
        let weird_nan = f64::from_bits(0x7ff8_dead_beef_0001);
        frame.push_row(&[Value::F64(weird_nan), Value::F32(f32::from_bits(0x7fc0_1234))]).unwrap();
        let row = frame.row(0).unwrap();
        assert!(matches!(row[0], OwnedValue::F64(x) if x.to_bits() == weird_nan.to_bits()));
        assert!(matches!(row[1], OwnedValue::F32(x) if x.to_bits() == 0x7fc0_1234));
    }

    #[test]
    fn empty_string_is_not_null_and_identical_strings_share_one_entry() {
        let frame = DataFrame::new(log_schema(), words(1000));
        let json = r#"{"a":"x\ny"}"#;
        let push = |id, msg: &str| frame.push_row(&[Value::U64(id), Value::Str(msg), Value::Str(json), Value::Null]).unwrap();
        push(1, "");
        let after_first = frame.row_heap_len();
        push(2, "");
        push(3, "same message");
        let after_third = frame.row_heap_len();
        push(4, "same message");
        assert_eq!(frame.row_heap_len(), after_third);
        assert!(after_first > 0 && after_third > after_first);
        assert_eq!(frame.row(0).unwrap()[1], OwnedValue::Str(String::new()));
        assert_eq!(frame.row(3).unwrap()[2], OwnedValue::Str(json.into()));
        assert_eq!(frame.dedup_len(), 3); // "", "same message", json
        frame.push_row(&[Value::U64(5), Value::Null, Value::Null, Value::Null]).unwrap();
        assert_eq!(frame.row(4).unwrap()[1], OwnedValue::Null);
    }

    #[test]
    fn dedup_map_is_capped_but_storing_still_works() {
        let options = DataFrameOptions { dedup_entries: 1, ..DataFrameOptions::default() };
        let schema = Schema::new(vec![Column::new("s", ColumnType::String)]).unwrap();
        let frame = DataFrame::with_options(schema, words(1000), options);
        for text in ["one one", "two two", "two two"] {
            frame.push_row(&[Value::Str(text)]).unwrap();
        }
        assert_eq!(frame.dedup_len(), 1);
        let texts: Vec<_> = frame.rows().map(|r| r.unwrap().remove(0)).collect();
        assert_eq!(texts, ["one one", "two two", "two two"].map(|t| OwnedValue::Str(t.into())));
    }

    #[test]
    fn blobs_are_stored_raw_and_not_deduplicated() {
        let schema = Schema::new(vec![Column::new("b", ColumnType::Blob)]).unwrap();
        let frame = DataFrame::new(schema, words(10));
        let blob = [9u8; 100];
        frame.push_row(&[Value::Blob(&blob)]).unwrap();
        let once = frame.row_heap_len();
        frame.push_row(&[Value::Blob(&blob)]).unwrap();
        frame.push_row(&[Value::Blob(&[])]).unwrap();
        assert!(frame.row_heap_len() > 2 * once);
        assert_eq!(frame.row(0).unwrap()[0], OwnedValue::Blob(blob.to_vec()));
        assert_eq!(frame.row(2).unwrap()[0], OwnedValue::Blob(vec![]));
    }

    #[test]
    fn bad_rows_change_nothing() {
        let shared = words(1000);
        let frame = DataFrame::new(log_schema(), shared.clone());
        frame.push_row(&[Value::U64(1), Value::Str("keep"), Value::Str("{}"), Value::F64(1.0)]).unwrap();
        let (words_before, heap_before) = (shared.read().unwrap().word_count(), frame.row_heap_len());
        let bad_rows: Vec<Vec<Value>> = vec![
            vec![Value::U64(2), Value::Str("brand new words here"), Value::Str("{}")], // too few values
            vec![Value::U64(2), Value::Str("brand new words here"), Value::Str("{}"), Value::Str("x")], // wrong type, after new words
            vec![Value::I64(-1), Value::Str("brand new words here"), Value::Str("{}"), Value::F64(1.0)], // out of range
            vec![Value::U64(2), Value::Blob(&[1]), Value::Str("{}"), Value::F64(1.0)], // blob into string
        ];
        for row in &bad_rows {
            assert!(frame.push_row(row).is_err());
            assert_eq!((frame.len(), shared.read().unwrap().word_count(), frame.row_heap_len()), (1, words_before, heap_before));
        }
        assert!(matches!(frame.push_row(&bad_rows[0]), Err(Error::ColumnCount { expected: 4, got: 3 })));
        assert!(matches!(frame.push_row(&bad_rows[2]), Err(Error::ValueOutOfRange { .. })));
        assert!(matches!(frame.push_row(&bad_rows[1]), Err(Error::TypeMismatch { .. })));
    }

    #[test]
    fn a_full_dictionary_rejects_the_whole_row() {
        let shared = words(3);
        let frame = DataFrame::new(log_schema(), shared.clone());
        let row = [Value::U64(1), Value::Str("aa bb"), Value::Str("cc dd"), Value::Null];
        assert!(matches!(frame.push_row(&row), Err(Error::DictionaryFull)));
        assert_eq!((frame.len(), shared.read().unwrap().word_count()), (0, 0));
        frame.push_row(&[Value::U64(1), Value::Str("aa bb"), Value::Str("cc"), Value::Null]).unwrap();
        assert_eq!(frame.len(), 1);
    }

    #[test]
    fn json_columns_use_the_escape_aware_splitter() {
        let shared = words(1000);
        let frame = DataFrame::new(log_schema(), shared.clone());
        frame.push_row(&[Value::U64(1), Value::Str(r"a\nb"), Value::Str(r"line1\nline2"), Value::Null]).unwrap();
        let store = shared.read().unwrap();
        let stored: Vec<_> = (0..store.word_count()).map(|i| store.get(i).unwrap().to_string()).collect();
        assert!(stored.contains(&"\\n".to_string()) && stored.contains(&"line1".to_string()), "{stored:?}");
        assert!(stored.contains(&"nb".to_string()), "{stored:?}"); // the plain column glues, as designed
    }

    #[test]
    fn write_string_and_out_of_range_access() {
        let frame = DataFrame::new(log_schema(), words(1000));
        frame.push_row(&[Value::U64(1), Value::Str("hello world"), Value::Null, Value::Null]).unwrap();
        let mut out = BytesMut::new();
        assert!(frame.write_string(0, 1, &mut out).unwrap());
        assert_eq!(&out[..], b"hello world");
        assert!(!frame.write_string(0, 2, &mut out).unwrap());
        assert!(matches!(frame.write_string(0, 0, &mut out), Err(Error::TypeMismatch { .. })));
        assert!(matches!(frame.row(1), Err(Error::RowOutOfRange)));
        assert!(matches!(frame.value(0, 9), Err(Error::ColumnOutOfRange)));
    }

    #[test]
    fn two_dataframes_share_one_word_store() {
        let shared = words(1000);
        let a = DataFrame::new(log_schema(), shared.clone());
        let b = DataFrame::new(log_schema(), shared.clone());
        a.push_row(&[Value::U64(1), Value::Str("shared words"), Value::Null, Value::Null]).unwrap();
        let count = shared.read().unwrap().word_count();
        b.push_row(&[Value::U64(1), Value::Str("shared words"), Value::Null, Value::Null]).unwrap();
        assert_eq!(shared.read().unwrap().word_count(), count);
        assert_eq!(b.row(0).unwrap()[1], OwnedValue::Str("shared words".into()));
    }

    #[test]
    fn concurrent_writers_lose_no_rows() {
        let shared = words(100_000);
        let frame = DataFrame::new(log_schema(), shared);
        std::thread::scope(|scope| {
            for thread in 0..4u64 {
                let frame = &frame;
                scope.spawn(move || {
                    for i in 0..250u64 {
                        let id = thread * 1000 + i;
                        let msg = format!("thread {thread} says item {i} of 250");
                        frame.push_row(&[Value::U64(id), Value::Str(&msg), Value::Null, Value::F64(id as f64)]).unwrap();
                    }
                });
            }
        });
        assert_eq!(frame.len(), 1000);
        let mut seen = std::collections::HashSet::new();
        for row in frame.rows() {
            let row = row.unwrap();
            let OwnedValue::U64(id) = row[0] else { panic!("id") };
            assert_eq!(row[1], OwnedValue::Str(format!("thread {} says item {} of 250", id / 1000, id % 1000)));
            assert!(seen.insert(id));
        }
    }
}
