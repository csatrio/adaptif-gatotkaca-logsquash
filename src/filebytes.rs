//! Read-only access to a whole file: memory-mapped with the `mmap` feature, read into memory without it.
//!
//! This is the only place in the crate that uses `unsafe`.

use crate::error::Result;
use std::ops::Deref;
use std::path::Path;

/// The bytes of a file.
pub(crate) enum FileBytes {
    #[cfg(feature = "mmap")]
    Mapped(memmap2::Mmap),
    Owned(Vec<u8>),
}

impl FileBytes {
    /// Maps (or reads) the file at `path`.
    pub(crate) fn open(path: &Path) -> Result<Self> {
        #[cfg(feature = "mmap")]
        {
            let file = std::fs::File::open(path)?;
            if file.metadata()?.len() > 0 {
                // SAFETY: the files are immutable once written (segments, indexes) or only replaced by renaming a new file over
                // them, and nothing in this crate truncates a file that is mapped. A file that is modified from outside while
                // it is mapped is a violation of the store's contract, as with any memory-mapped database.
                #[allow(unsafe_code)]
                let map = unsafe { memmap2::Mmap::map(&file)? };
                return Ok(FileBytes::Mapped(map));
            }
        }
        Ok(FileBytes::Owned(std::fs::read(path)?))
    }
}

impl Deref for FileBytes {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        match self {
            #[cfg(feature = "mmap")]
            FileBytes::Mapped(map) => map,
            FileBytes::Owned(bytes) => bytes,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_files_including_empty_ones() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("a"), b"hello").unwrap();
        std::fs::write(dir.path().join("b"), b"").unwrap();
        assert_eq!(&*FileBytes::open(&dir.path().join("a")).unwrap(), b"hello");
        assert!(FileBytes::open(&dir.path().join("b")).unwrap().is_empty());
        assert!(FileBytes::open(&dir.path().join("missing")).is_err());
    }
}
