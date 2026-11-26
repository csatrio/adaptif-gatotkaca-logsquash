//! Stand-in for the mapped pending buffer when the `mmap` feature is off: stores keep their pending rows in memory
//! (`PendingMode::Memory`). The types are uninhabited, so every code path that would use them is unreachable.

use crate::dataframe::DataFrameOptions;
use crate::error::{Error, Result};
use crate::query::RowSource;
use crate::schema::Schema;
use crate::segment::SegmentInput;
use crate::types::Value;
use crate::words::{SharedWords, WordStore};
use std::borrow::Cow;
use std::path::Path;
use std::sync::Arc;

/// `superseded_by` of an epoch that ended because the store was sealed.
pub(crate) const SEALED: u64 = u64::MAX;

/// Data capacities of the chunks of a region (unused without the `mmap` feature).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ChunkSizes {
    pub(crate) min: u64,
    pub(crate) max: u64,
}

/// Milliseconds since the Unix epoch (0 if the clock is before it).
pub(crate) fn now_unix_ms() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map_or(0, |d| d.as_millis() as u64)
}

pub(crate) fn remove_other_epochs(_: &Path, _: u128, _: &[u64], _: u64) -> Result<bool> {
    Ok(true)
}

pub(crate) fn epoch_exists(_: &Path, _: u128, _: u64) -> bool {
    false
}
pub(crate) fn remove_epoch(_: &Path, _: u128, _: u64) {}

fn unsupported() -> Error {
    Error::Io(std::io::Error::new(std::io::ErrorKind::Unsupported, "the pending rows of this store need the `mmap` feature"))
}

pub(crate) enum Epoch {}

impl Epoch {
    pub(crate) fn create(_: &Path, _: u128, _: u64, _: u32, _: ChunkSizes) -> Result<Arc<Self>> {
        Err(unsupported())
    }

    /// Readers see no pending rows; a writer cannot recover them.
    pub(crate) fn open(_: &Path, _: u128, _: u64, _: u32, _: ChunkSizes, writable: bool) -> Result<Option<Arc<Self>>> {
        if writable { Err(unsupported()) } else { Ok(None) }
    }

    pub(crate) fn number(&self) -> u64 {
        match *self {}
    }

    pub(crate) fn dir(&self) -> &Path {
        match *self {}
    }

    pub(crate) fn superseded_by(&self) -> u64 {
        match *self {}
    }

    pub(crate) fn next(&self) -> u64 {
        match *self {}
    }

    pub(crate) fn set_next(&self, _: u64) {
        match *self {}
    }

    pub(crate) fn set_superseded_by(&self, _: u64) {
        match *self {}
    }

    pub(crate) fn has_new_words(&self) -> bool {
        match *self {}
    }

    pub(crate) fn load_words(&self, _: &mut WordStore) -> Result<()> {
        match *self {}
    }

    pub(crate) fn open_frame(
        self: &Arc<Self>,
        _: usize,
        _: Arc<Schema>,
        _: &SharedWords,
        _: &DataFrameOptions,
    ) -> Result<Option<MappedFrame>> {
        match **self {}
    }
}

pub(crate) enum MappedFrame {}

impl MappedFrame {
    pub(crate) fn published_rows(&self) -> Result<u64> {
        match *self {}
    }

    pub(crate) fn pending_bytes(&self) -> u64 {
        match *self {}
    }

    pub(crate) fn first_push_ms(&self) -> u64 {
        match *self {}
    }

    pub(crate) fn source(&self, _: u64) -> Result<MappedSource> {
        match *self {}
    }

    pub(crate) fn segment_source(&self) -> Result<MappedSource> {
        match *self {}
    }

    pub(crate) fn push_row(&self, _: &[Value<'_>]) -> Result<u64> {
        match *self {}
    }
}

pub(crate) enum MappedSource {}

impl RowSource for MappedSource {
    fn row_count(&self) -> u64 {
        match *self {}
    }

    fn cell(&self, _: u64, _: usize) -> Result<(u64, bool)> {
        match *self {}
    }

    fn with_entry(&self, _: u64, _: &mut dyn FnMut(&[u8]) -> Result<()>) -> Result<()> {
        match *self {}
    }

    fn range(&self, _: usize, _: u64, _: u64) -> Result<Vec<u32>> {
        match *self {}
    }
}

impl SegmentInput for MappedSource {
    fn rows(&self) -> u64 {
        match *self {}
    }

    fn columns(&self) -> usize {
        match *self {}
    }

    fn cell(&self, _: u64, _: usize) -> Result<(u64, bool)> {
        match *self {}
    }

    fn heap_blocks(&self) -> usize {
        match *self {}
    }

    fn heap_block(&self, _: usize) -> Result<(u64, Cow<'_, [u8]>)> {
        match *self {}
    }
}
