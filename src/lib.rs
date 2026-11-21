//! Log squasher: stores logs in compact binary dataframes, compressed by word-level string
//! deduplication, compact varint symbols and block compression. See `DESIGN.md`.
//!
//! The string layer is the [`splitter`], the shared [`words`] store (heap + dictionary) and the [`tokenizer`] that turns
//! text into symbol streams. On top of it, a [`DataFrame`] stores typed rows (in memory for now).

#![deny(unsafe_code)]
#![allow(clippy::module_name_repetitions)]

pub mod cell;
pub mod dataframe;
pub mod dictionary;
pub mod error;
pub mod heap;
pub mod schema;
pub mod splitter;
pub mod tokenizer;
pub mod types;
pub mod varint;
pub mod words;

pub use dataframe::{DataFrame, DataFrameOptions, Rows};
pub use dictionary::{Dictionary, ENTRY_COST_BYTES};
pub use error::{Error, Result};
pub use heap::{Heap, MemHeap};
pub use schema::{Column, ColumnFlags, Schema};
pub use splitter::{DefaultSplitter, JsonEscapeSplitter, Splitter};
pub use tokenizer::{StreamStats, Tokenizer, TokenizerOptions, analyze};
pub use types::{ColumnType, OwnedValue, Value};
pub use words::{Checkpoint, SharedWords, WordStore};
