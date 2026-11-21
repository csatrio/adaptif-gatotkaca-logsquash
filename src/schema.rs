//! The schema of a dataframe (DESIGN.md 6.1).

use crate::error::{Error, Result};
use crate::types::ColumnType;

/// Per-column options.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ColumnFlags {
    /// Split text with the JSON-escape-aware splitter (string columns holding JSON text).
    pub json_escapes: bool,
    /// Also keep a case-insensitive word index (string columns; used from milestone 4).
    pub case_insensitive: bool,
    /// Index the column for equality filters (used from milestone 4).
    pub index_equality: bool,
    /// Index the words of the column for word and phrase search (string columns; used from milestone 4).
    pub index_words: bool,
}

/// One column of a schema.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Column {
    /// Column name, unique within the schema.
    pub name: String,
    /// Column type.
    pub ty: ColumnType,
    /// Column options.
    pub flags: ColumnFlags,
    /// Text format of a date/time column (empty means ISO-8601/RFC-3339); see `DESIGN.md` 6.2.2.
    pub text_format: String,
}

impl Column {
    /// A column with default options.
    pub fn new(name: impl Into<String>, ty: ColumnType) -> Self {
        Self { name: name.into(), ty, flags: ColumnFlags::default(), text_format: String::new() }
    }

    /// Marks the column as holding JSON text.
    pub fn json_escapes(mut self) -> Self {
        self.flags.json_escapes = true;
        self
    }

    /// Enables the case-insensitive word index.
    pub fn case_insensitive(mut self) -> Self {
        self.flags.case_insensitive = true;
        self
    }

    /// Enables the equality index.
    pub fn index_equality(mut self) -> Self {
        self.flags.index_equality = true;
        self
    }

    /// Enables the word index.
    pub fn index_words(mut self) -> Self {
        self.flags.index_words = true;
        self
    }

    /// The text of a value read from this (date/time) column, in the column's text format; `None` for null.
    pub fn format_value(&self, value: &crate::types::OwnedValue) -> Result<Option<String>> {
        if matches!(value, crate::types::OwnedValue::Null) {
            return Ok(None);
        }
        crate::datetime::format(self.ty, &self.text_format, value).map(Some).map_err(|e| match e {
            Error::TypeMismatch { .. } => Error::TypeMismatch { column: self.name.clone() },
            other => other,
        })
    }

    /// Sets the text format of a date/time column.
    pub fn text_format(mut self, format: impl Into<String>) -> Self {
        self.text_format = format.into();
        self
    }
}

/// An ordered list of columns.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Schema {
    columns: Vec<Column>,
}

impl Schema {
    /// Validates and builds a schema.
    pub fn new(columns: Vec<Column>) -> Result<Self> {
        if columns.is_empty() {
            return Err(Error::InvalidSchema("a schema needs at least one column"));
        }
        for (i, c) in columns.iter().enumerate() {
            if c.name.is_empty() || c.name.contains('\0') {
                return Err(Error::InvalidSchema("column names must be non-empty and contain no NUL"));
            }
            if columns[..i].iter().any(|other| other.name == c.name) {
                return Err(Error::InvalidSchema("duplicate column name"));
            }
            let text_flags = c.flags.json_escapes || c.flags.case_insensitive || c.flags.index_words;
            if text_flags && c.ty != ColumnType::String {
                return Err(Error::InvalidSchema("json_escapes, case_insensitive and index_words need a String column"));
            }
            if c.flags.index_equality && c.ty == ColumnType::Blob {
                return Err(Error::InvalidSchema("Blob columns cannot be indexed"));
            }
            if !c.text_format.is_empty() {
                if !c.ty.is_temporal() {
                    return Err(Error::InvalidSchema("a text format is only for date/time columns"));
                }
                crate::datetime::validate(c.ty, &c.text_format)?;
            }
        }
        Ok(Self { columns })
    }

    /// The columns.
    pub fn columns(&self) -> &[Column] {
        &self.columns
    }

    /// Number of columns.
    pub fn len(&self) -> usize {
        self.columns.len()
    }

    /// Always false: a schema has at least one column.
    pub fn is_empty(&self) -> bool {
        false
    }

    /// Index of the column with the given name.
    pub fn column_index(&self, name: &str) -> Option<usize> {
        self.columns.iter().position(|c| c.name == name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validation() {
        let ok =
            vec![Column::new("a", ColumnType::String).json_escapes(), Column::new("t", ColumnType::TimestampMs).text_format("builtin:clf")];
        let schema = Schema::new(ok).unwrap();
        assert_eq!((schema.len(), schema.column_index("t"), schema.column_index("x")), (2, Some(1), None));
        let bad = |cols: Vec<Column>| matches!(Schema::new(cols), Err(Error::InvalidSchema(_)));
        assert!(bad(vec![]));
        assert!(bad(vec![Column::new("", ColumnType::I8)]));
        assert!(bad(vec![Column::new("a", ColumnType::I8), Column::new("a", ColumnType::I8)]));
        assert!(bad(vec![Column::new("a", ColumnType::I8).json_escapes()]));
        assert!(bad(vec![Column::new("a", ColumnType::Blob).index_equality()]));
        assert!(bad(vec![Column::new("a", ColumnType::String).text_format("x")]));
    }
}
