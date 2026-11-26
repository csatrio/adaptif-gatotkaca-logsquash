//! Log squasher: stores logs in compact binary dataframes, compressed by word-level string deduplication, compact varint
//! symbols and zstd blocks, with optional persisted indexes for word, phrase, equality and range search.
//!
//! Start with a [`Store`] (a folder for one day or period), create a frame with a [`Schema`], `push_row` values, `commit`
//! and `seal`; later open the folder with [`Store::open_read_only`] and search with a [`Query`]. The lower layers are the
//! [`splitter`], the shared [`words`] store (heap and dictionary) and the [`tokenizer`] that turns text into symbols; an
//! in-memory [`DataFrame`] sits beneath the on-disk [`Frame`]. See the README for a complete example and `DESIGN.md` for
//! the design.

#![deny(unsafe_code)]
#![allow(clippy::module_name_repetitions)]

pub mod cell;
pub(crate) mod codec;
pub mod dataframe;
pub mod datetime;
pub mod dictionary;
pub mod error;
pub(crate) mod filebytes;
pub mod heap;
pub(crate) mod index;
#[cfg(feature = "mmap")]
pub(crate) mod mapped;
#[cfg(feature = "mmap")]
pub(crate) mod pending;
#[cfg(not(feature = "mmap"))]
#[path = "pending_stub.rs"]
pub(crate) mod pending;
pub mod query;
pub mod schema;
pub(crate) mod segment;
pub mod splitter;
pub mod store;
pub mod tokenizer;
pub mod types;
pub mod varint;
pub mod words;

pub use dataframe::{DataFrame, DataFrameOptions, Rows};
pub use dictionary::{Dictionary, ENTRY_COST_BYTES};
pub use error::{Error, Result};
pub use heap::{Heap, MemHeap};
pub use query::{Filter, OrderBy, Page, PageResult, Query};
pub use schema::{Column, ColumnFlags, Schema};
pub use splitter::{DefaultSplitter, JsonEscapeSplitter, Splitter};
pub use store::{CommitPolicy, CommitTrigger, Frame, FrameRows, IndexReport, PendingMode, RecoveryReport, RowIds, Store, StoreOptions};
pub use tokenizer::{StreamStats, Symbol, Tokenizer, TokenizerOptions, analyze, symbols};
pub use types::{ColumnType, OwnedValue, Value};
pub use words::{Checkpoint, SharedWords, WordStore};
