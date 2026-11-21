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
    /// The schema is not valid.
    InvalidSchema(&'static str),
    /// A row has the wrong number of values.
    ColumnCount {
        /// Columns in the schema.
        expected: usize,
        /// Values in the row.
        got: usize,
    },
    /// A value has the wrong kind for its column (for example text for an integer column).
    TypeMismatch {
        /// The column.
        column: String,
    },
    /// A value is of the right kind but does not fit the column (for example 300 in an `i8` column).
    ValueOutOfRange {
        /// The column.
        column: String,
    },
    /// A row number is past the end of the dataframe.
    RowOutOfRange,
    /// A column index is past the end of the schema.
    ColumnOutOfRange,
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
            Error::InvalidSchema(what) => write!(f, "invalid schema: {what}"),
            Error::ColumnCount { expected, got } => write!(f, "row has {got} values, schema has {expected} columns"),
            Error::TypeMismatch { column } => write!(f, "value has the wrong type for column {column:?}"),
            Error::ValueOutOfRange { column } => write!(f, "value does not fit column {column:?}"),
            Error::RowOutOfRange => write!(f, "row number out of range"),
            Error::ColumnOutOfRange => write!(f, "column index out of range"),
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
