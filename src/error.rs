//! Error type of the library.

use std::fmt;

/// Everything that can go wrong in the library.
#[derive(Debug)]
pub enum Error {
    /// The heap reached its hard limit (2^32 words or 4 GB of word bytes).
    HeapFull,
    /// The dictionary reached its memory limit; the caller should start a new folder.
    DictionaryFull,
    /// Stored data is malformed.
    Corrupt(&'static str),
    /// An I/O error.
    Io(std::io::Error),
}

/// Result alias using [`Error`].
pub type Result<T> = std::result::Result<T, Error>;

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::HeapFull => write!(f, "heap is full"),
            Error::DictionaryFull => write!(f, "dictionary is full"),
            Error::Corrupt(what) => write!(f, "corrupt data: {what}"),
            Error::Io(e) => write!(f, "i/o error: {e}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error::Io(e)
    }
}
