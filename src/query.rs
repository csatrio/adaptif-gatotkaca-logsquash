//! Queries (DESIGN.md 6.5): word, phrase, equality and range filters over a dataframe, combined with AND.
//!
//! **What a word is.** Search works on the symbol streams, so a *word* is a word the tokenizer stored in the heap (two or
//! more characters) or a single character. Inline runs (UUIDs, hex, base64 and long random-looking ids) are not words and
//! never match a word or phrase filter; find them with an equality filter on the whole value. Words are matched with
//! the column's splitter, so a phrase is split into words the same way the text was when it was stored.

use crate::cell;
use crate::dataframe::Inner;
use crate::dictionary::xxh3;
use crate::error::{Error, Result};
use crate::index::{KIND_EQUAL, KIND_FOLD, KIND_WORD, SegmentIndex, fold_hash};
use crate::schema::Schema;
use crate::splitter::{DefaultSplitter, JsonEscapeSplitter, Splitter, split_words};
use crate::tokenizer::{Symbol, decode_with, symbols};
use crate::types::{ColumnType, OwnedValue, Value};
use crate::words::WordStore;

/// One condition of a [`Query`].
#[derive(Debug, Clone, PartialEq)]
pub enum Filter {
    /// The string column contains the word. With `case_insensitive` the comparison uses full Unicode case folding.
    Word {
        /// Column index.
        column: usize,
        /// The word.
        word: String,
        /// Fold case before comparing.
        case_insensitive: bool,
    },
    /// The string column contains the words of `text` next to each other, in order.
    Phrase {
        /// Column index.
        column: usize,
        /// The phrase.
        text: String,
    },
    /// The column equals the value (`Null` matches null cells).
    Equals {
        /// Column index.
        column: usize,
        /// The value.
        value: OwnedValue,
    },
    /// The fixed-size column lies between the bounds (inclusive; `None` is unbounded). Nulls never match.
    Range {
        /// Column index.
        column: usize,
        /// Lower bound.
        min: Option<OwnedValue>,
        /// Upper bound.
        max: Option<OwnedValue>,
    },
}

/// Filters combined with AND. An empty query matches every row.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Query {
    pub(crate) filters: Vec<Filter>,
}

impl Query {
    /// A query that matches every row.
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds a filter.
    pub fn filter(mut self, filter: Filter) -> Self {
        self.filters.push(filter);
        self
    }

    /// Adds a case-sensitive word filter.
    pub fn word(self, column: usize, word: &str) -> Self {
        self.filter(Filter::Word { column, word: word.to_owned(), case_insensitive: false })
    }

    /// Adds a case-insensitive word filter.
    pub fn word_ci(self, column: usize, word: &str) -> Self {
        self.filter(Filter::Word { column, word: word.to_owned(), case_insensitive: true })
    }

    /// Adds a phrase filter.
    pub fn phrase(self, column: usize, text: &str) -> Self {
        self.filter(Filter::Phrase { column, text: text.to_owned() })
    }

    /// Adds an equality filter.
    pub fn equals(self, column: usize, value: Value<'_>) -> Self {
        self.filter(Filter::Equals { column, value: value.to_owned_value() })
    }

    /// Adds a range filter.
    pub fn range(self, column: usize, min: Option<Value<'_>>, max: Option<Value<'_>>) -> Self {
        self.filter(Filter::Range { column, min: min.map(|v| v.to_owned_value()), max: max.map(|v| v.to_owned_value()) })
    }
}

/// Sort order of a page of results.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OrderBy {
    /// A fixed-size column.
    pub column: usize,
    /// Largest first.
    pub descending: bool,
}

/// Which rows of the matches to return. Without `order_by` rows come in insertion order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Page {
    /// Sort order.
    pub order_by: Option<OrderBy>,
    /// Matches to skip.
    pub offset: usize,
    /// Matches to return.
    pub limit: usize,
}

/// A page of matching row numbers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PageResult {
    /// Number of matches in all.
    pub total: u64,
    /// The row numbers of this page.
    pub rows: Vec<u64>,
}

/// Rows of one segment (or of the buffer of unwritten rows) that a query is evaluated on.
pub(crate) trait RowSource {
    fn row_count(&self) -> u64;
    fn cell(&self, row: u64, column: usize) -> Result<(u64, bool)>;
    /// Calls `f` with the row-heap bytes that start at the entry of a string cell.
    fn with_entry(&self, cell: u64, f: &mut dyn FnMut(&[u8]) -> Result<()>) -> Result<()>;
    /// Rows whose non-null cell in `column` lies in `lo..=hi`.
    fn range(&self, column: usize, lo: u64, hi: u64) -> Result<Vec<u32>>;
}

/// The rows buffered in a dataframe, as a query (and index) source, over a buffer lock the caller already holds.
///
/// Lock order: the caller takes the buffer lock (`DataFrame::with_inner`) **before** the word store, as `push_row` does
/// (buffer, then words). A source that locked the buffer per cell while the caller held the words would invert that
/// order and deadlock against a concurrent `push_row` (and read-lock the buffer recursively).
pub(crate) struct BufferSource<'a> {
    inner: &'a Inner,
    /// Rows visible through this source (a snapshot: rows pushed later are not seen).
    rows: u64,
}

impl<'a> BufferSource<'a> {
    /// All rows of the buffer.
    pub(crate) fn all(inner: &'a Inner) -> Self {
        Self { inner, rows: inner.rows }
    }

    /// The first `rows` rows of the buffer.
    pub(crate) fn first(inner: &'a Inner, rows: u64) -> Self {
        Self { inner, rows: rows.min(inner.rows) }
    }

    fn column(&self, column: usize) -> Result<&crate::dataframe::ColumnData> {
        self.inner.columns.get(column).ok_or(Error::ColumnOutOfRange)
    }
}

impl RowSource for BufferSource<'_> {
    fn row_count(&self) -> u64 {
        self.rows
    }

    fn cell(&self, row: u64, column: usize) -> Result<(u64, bool)> {
        let data = self.column(column)?;
        if row >= self.rows {
            return Err(Error::RowOutOfRange);
        }
        Ok((data.cells[row as usize], data.nulls.get(row as usize)))
    }

    fn with_entry(&self, cell: u64, f: &mut dyn FnMut(&[u8]) -> Result<()>) -> Result<()> {
        let tail = self.inner.row_heap.get((cell - 1) as usize..).ok_or(Error::Corrupt("row heap offset"))?;
        f(tail)
    }

    fn range(&self, column: usize, lo: u64, hi: u64) -> Result<Vec<u32>> {
        let data = self.column(column)?;
        Ok((0..self.rows as usize).filter(|&r| !data.nulls.get(r) && (lo..=hi).contains(&data.cells[r])).map(|r| r as u32).collect())
    }
}

/// A query symbol of a phrase.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum QSym {
    Word(u32),
    Char(char),
}

#[derive(Debug)]
pub(crate) enum Target {
    Cell(u64),
    Text { text: String, hash: u64 },
    Null,
}

/// A query with its words resolved against the store.
#[derive(Debug)]
pub(crate) enum Prepared {
    /// Can never match.
    Nothing,
    WordMulti {
        column: usize,
        ordinal: Option<u32>,
        fold_hash: u64,
        folded: String,
        ci: bool,
    },
    WordSingle {
        column: usize,
        c: char,
        folded: String,
        ci: bool,
    },
    Phrase {
        column: usize,
        syms: Vec<QSym>,
        ordinals: Vec<u32>,
    },
    Equals {
        column: usize,
        target: Target,
    },
    Range {
        column: usize,
        lo: u64,
        hi: u64,
    },
}

fn splitter_for(schema: &Schema, column: usize) -> &'static dyn Splitter {
    if schema.columns()[column].flags.json_escapes { &JsonEscapeSplitter } else { &DefaultSplitter }
}

fn string_column(schema: &Schema, column: usize) -> Result<()> {
    let col = schema.columns().get(column).ok_or(Error::ColumnOutOfRange)?;
    if col.ty != ColumnType::String {
        return Err(Error::TypeMismatch { column: col.name.clone() });
    }
    Ok(())
}

/// The cell value of a filter value; text is parsed with the column's text format for date/time columns.
fn encode_bound(col: &crate::schema::Column, ty: ColumnType, value: &OwnedValue) -> Result<u64> {
    let parsed;
    let value = match value {
        OwnedValue::Str(text) if ty.is_temporal() => {
            parsed = crate::datetime::parse(ty, &col.text_format, text).map_err(|e| match e {
                Error::DateParse { reason, .. } => Error::DateParse { column: col.name.clone(), reason },
                other => other,
            })?;
            &parsed
        }
        other => other,
    };
    cell::encode(ty, &col.name, &value.as_value())?.ok_or_else(|| Error::TypeMismatch { column: col.name.clone() })
}

/// Resolves the words and values of `query`. Fails on bad columns, types and bounds.
pub(crate) fn prepare(query: &Query, schema: &Schema, words: &WordStore) -> Result<Vec<Prepared>> {
    let mut out = Vec::new();
    for filter in &query.filters {
        out.push(match filter {
            Filter::Word { column, word, case_insensitive } => {
                string_column(schema, *column)?;
                let (column, ci) = (*column, *case_insensitive);
                let mut chars = word.chars();
                match (chars.next(), chars.next()) {
                    (None, _) => return Err(Error::InvalidQuery("empty word")),
                    (Some(c), None) => Prepared::WordSingle { column, c, folded: caseless::default_case_fold_str(word), ci },
                    _ => {
                        let ordinal = words.lookup(word);
                        if ordinal.is_none() && !ci {
                            Prepared::Nothing
                        } else {
                            Prepared::WordMulti {
                                column,
                                ordinal,
                                fold_hash: fold_hash(word),
                                folded: caseless::default_case_fold_str(word),
                                ci,
                            }
                        }
                    }
                }
            }
            Filter::Phrase { column, text } => {
                string_column(schema, *column)?;
                if text.is_empty() {
                    return Err(Error::InvalidQuery("empty phrase"));
                }
                let mut syms = Vec::new();
                let mut missing = false;
                for w in split_words(splitter_for(schema, *column), text) {
                    let mut chars = w.chars();
                    match (chars.next(), chars.next()) {
                        (Some(c), None) => syms.push(QSym::Char(c)),
                        _ => match words.lookup(w) {
                            Some(o) => syms.push(QSym::Word(o)),
                            None => missing = true,
                        },
                    }
                }
                if missing {
                    Prepared::Nothing
                } else {
                    let ordinals = syms.iter().filter_map(|s| if let QSym::Word(o) = s { Some(*o) } else { None }).collect();
                    Prepared::Phrase { column: *column, syms, ordinals }
                }
            }
            Filter::Equals { column, value } => {
                let col = schema.columns().get(*column).ok_or(Error::ColumnOutOfRange)?;
                let target = match (col.ty, value) {
                    (_, OwnedValue::Null) => Target::Null,
                    (ColumnType::String, OwnedValue::Str(text)) => Target::Text { hash: xxh3(text.as_bytes(), 0), text: text.clone() },
                    (ColumnType::String | ColumnType::Blob, _) => return Err(Error::TypeMismatch { column: col.name.clone() }),
                    (ty, v) => Target::Cell(encode_bound(col, ty, v)?),
                };
                Prepared::Equals { column: *column, target }
            }
            Filter::Range { column, min, max } => {
                let col = schema.columns().get(*column).ok_or(Error::ColumnOutOfRange)?;
                if col.ty.is_variable() {
                    return Err(Error::TypeMismatch { column: col.name.clone() });
                }
                let bound = |v: &Option<OwnedValue>, default: u64| match v {
                    None => Ok(default),
                    Some(v) => encode_bound(col, col.ty, v),
                };
                Prepared::Range { column: *column, lo: bound(min, 0)?, hi: bound(max, u64::MAX)? }
            }
        });
    }
    Ok(out)
}

/// Intersection of two ascending lists.
fn intersect(a: &[u32], b: &[u32]) -> Vec<u32> {
    let (mut i, mut j, mut out) = (0, 0, Vec::new());
    while i < a.len() && j < b.len() {
        match a[i].cmp(&b[j]) {
            std::cmp::Ordering::Less => i += 1,
            std::cmp::Ordering::Greater => j += 1,
            std::cmp::Ordering::Equal => {
                out.push(a[i]);
                i += 1;
                j += 1;
            }
        }
    }
    out
}

fn row_symbols(src: &dyn RowSource, row: u64, column: usize) -> Result<Option<Vec<Symbol>>> {
    let (cell, null) = src.cell(row, column)?;
    if null {
        return Ok(None);
    }
    let mut found = None;
    src.with_entry(cell, &mut |tail| {
        found = Some(symbols(tail)?);
        Ok(())
    })?;
    Ok(found)
}

fn row_matches(src: &dyn RowSource, words: &WordStore, filter: &Prepared, row: u64) -> Result<bool> {
    Ok(match filter {
        Prepared::Nothing => false,
        Prepared::WordMulti { column, ordinal, folded, ci, .. } => match row_symbols(src, row, *column)? {
            None => false,
            Some(syms) => syms.iter().any(|s| match s {
                Symbol::Word(o) if !ci => Some(*o) == *ordinal,
                Symbol::Word(o) => words.get(*o).is_some_and(|w| caseless::default_case_fold_str(w) == *folded),
                _ => false,
            }),
        },
        Prepared::WordSingle { column, c, folded, ci } => match row_symbols(src, row, *column)? {
            None => false,
            Some(syms) => syms.iter().any(|s| match s {
                Symbol::Char(x) if !ci => x == c,
                Symbol::Char(x) => caseless::default_case_fold_str(&x.to_string()) == *folded,
                _ => false,
            }),
        },
        Prepared::Phrase { column, syms, .. } => match row_symbols(src, row, *column)? {
            None => false,
            Some(row_syms) => row_syms.windows(syms.len()).any(|window| {
                window.iter().zip(syms).all(|(have, want)| match (have, want) {
                    (Symbol::Word(a), QSym::Word(b)) => a == b,
                    (Symbol::Char(a), QSym::Char(b)) => a == b,
                    _ => false,
                })
            }),
        },
        Prepared::Equals { column, target } => {
            let (cell, null) = src.cell(row, *column)?;
            match target {
                Target::Null => null,
                Target::Cell(c) => !null && cell == *c,
                Target::Text { text, .. } => {
                    if null {
                        return Ok(false);
                    }
                    let mut equal = false;
                    src.with_entry(cell, &mut |tail| {
                        let mut decoded = Vec::new();
                        decode_with(words, tail, &mut decoded)?;
                        equal = decoded == text.as_bytes();
                        Ok(())
                    })?;
                    equal
                }
            }
        }
        Prepared::Range { column, lo, hi } => {
            let (cell, null) = src.cell(row, *column)?;
            !null && cell >= *lo && cell <= *hi
        }
    })
}

/// The ascending row numbers of `src` that match every filter. `index` is the index of these rows, if there is one.
pub(crate) fn evaluate(src: &dyn RowSource, index: Option<&SegmentIndex>, words: &WordStore, filters: &[Prepared]) -> Result<Vec<u32>> {
    let mut exact: Vec<Vec<u32>> = Vec::new();
    let mut scans: Vec<&Prepared> = Vec::new();
    let usable = |column: usize, kind: u8| index.filter(|i| i.has(column, kind));
    for filter in filters {
        match filter {
            Prepared::Nothing => return Ok(Vec::new()),
            Prepared::WordMulti { column, ordinal, fold_hash, ci, .. } => {
                let list = if *ci {
                    usable(*column, KIND_FOLD).map(|i| i.postings(*column, KIND_FOLD, *fold_hash))
                } else {
                    usable(*column, KIND_WORD).map(|i| i.postings(*column, KIND_WORD, u64::from(ordinal.unwrap_or(u32::MAX))))
                };
                match list {
                    Some(postings) => exact.push(postings?.unwrap_or_default()),
                    None => scans.push(filter),
                }
            }
            Prepared::WordSingle { .. } => scans.push(filter),
            Prepared::Phrase { column, ordinals, .. } => {
                if let Some(i) = usable(*column, KIND_WORD) {
                    for o in ordinals {
                        exact.push(i.postings(*column, KIND_WORD, u64::from(*o))?.unwrap_or_default());
                    }
                }
                scans.push(filter); // the phrase itself is always verified against the symbols
            }
            Prepared::Equals { column, target } => {
                let key = match target {
                    Target::Cell(c) => Some(*c),
                    Target::Text { hash, .. } => Some(*hash),
                    Target::Null => None,
                };
                match (usable(*column, KIND_EQUAL), key) {
                    (Some(i), Some(key)) => exact.push(i.postings(*column, KIND_EQUAL, key)?.unwrap_or_default()),
                    _ => scans.push(filter),
                }
            }
            Prepared::Range { column, lo, hi } => exact.push(src.range(*column, *lo, *hi)?),
        }
    }
    exact.sort_by_key(Vec::len);
    let mut candidates: Vec<u32> = match exact.split_first() {
        None => (0..src.row_count() as u32).collect(),
        Some((first, rest)) => rest.iter().fold(first.clone(), |acc, list| intersect(&acc, list)),
    };
    if !scans.is_empty() {
        let mut kept = Vec::with_capacity(candidates.len());
        for row in candidates {
            let mut all = true;
            for filter in &scans {
                if !row_matches(src, words, filter, u64::from(row))? {
                    all = false;
                    break;
                }
            }
            if all {
                kept.push(row);
            }
        }
        candidates = kept;
    }
    Ok(candidates)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn intersection() {
        assert_eq!(intersect(&[1, 3, 5, 7], &[3, 4, 5, 8]), [3, 5]);
        assert!(intersect(&[], &[1]).is_empty());
    }

    #[test]
    fn query_builder() {
        let q = Query::new().word(1, "a").word_ci(1, "b").phrase(2, "c d").equals(0, Value::I64(1)).range(0, Some(Value::I64(1)), None);
        assert_eq!(q.filters.len(), 5);
        assert_eq!(q.filters[4], Filter::Range { column: 0, min: Some(OwnedValue::I64(1)), max: None });
    }
}
