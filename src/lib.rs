//! Log squasher: stores logs in compact binary dataframes, compressed by word-level string
//! deduplication, compact varint symbols and block compression. See `DESIGN.md`.
//!
//! This crate currently contains the string layer (milestone 1): the [`splitter`], the shared
//! [`words`] store (heap + dictionary) and the [`tokenizer`] that turns text into symbol streams.

#![deny(unsafe_code)]
#![allow(clippy::module_name_repetitions)]

pub mod dictionary;
pub mod error;
pub mod heap;
pub mod splitter;
pub mod tokenizer;
pub mod varint;
pub mod words;

pub use dictionary::{Dictionary, ENTRY_COST_BYTES};
pub use error::{Error, Result};
pub use heap::{Heap, MemHeap};
pub use splitter::{DefaultSplitter, JsonEscapeSplitter, Splitter};
pub use tokenizer::{StreamStats, Tokenizer, TokenizerOptions, analyze};
pub use words::{Checkpoint, SharedWords, WordStore};
