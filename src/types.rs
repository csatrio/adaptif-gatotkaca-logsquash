//! Column types and values (DESIGN.md 6.1, 6.6).

/// The type of a column. The numeric codes are stored in the schema, so they never change.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum ColumnType {
    /// `i8`.
    I8 = 1,
    /// `i16`.
    I16 = 2,
    /// `i32`.
    I32 = 3,
    /// `i64`.
    I64 = 4,
    /// `u8`.
    U8 = 5,
    /// `u16`.
    U16 = 6,
    /// `u32`.
    U32 = 7,
    /// `u64`.
    U64 = 8,
    /// `f32`.
    F32 = 9,
    /// `f64`.
    F64 = 10,
    /// `bool`.
    Bool = 11,
    /// Unicode scalar value.
    Char = 12,
    /// Seconds since the Unix epoch (UTC).
    TimestampSec = 13,
    /// Milliseconds since the Unix epoch (UTC).
    TimestampMs = 14,
    /// Microseconds since the Unix epoch (UTC).
    TimestampUs = 15,
    /// Nanoseconds since the Unix epoch (UTC).
    TimestampNs = 16,
    /// Days since 1970-01-01.
    Date = 17,
    /// Nanoseconds since midnight.
    TimeOfDay = 18,
    /// Milliseconds since the epoch plus a UTC offset in minutes.
    TimestampTz = 19,
    /// UTF-8 text, tokenized.
    String = 20,
    /// Raw bytes.
    Blob = 21,
}

impl ColumnType {
    /// The stable numeric code.
    pub fn code(self) -> u8 {
        self as u8
    }

    /// The type with the given code.
    pub fn from_code(code: u8) -> Option<Self> {
        use ColumnType::*;
        const ALL: [ColumnType; 21] = [
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
        ALL.get(usize::from(code).wrapping_sub(1)).copied()
    }

    /// Whether the cell points into the row heap (`String` and `Blob`).
    pub fn is_variable(self) -> bool {
        matches!(self, ColumnType::String | ColumnType::Blob)
    }

    /// Whether the column holds a date or time (and may name a text format).
    pub fn is_temporal(self) -> bool {
        use ColumnType::*;
        matches!(self, TimestampSec | TimestampMs | TimestampUs | TimestampNs | Date | TimeOfDay | TimestampTz)
    }

    /// Whether the column holds an integer.
    pub fn is_integer(self) -> bool {
        use ColumnType::*;
        matches!(self, I8 | I16 | I32 | I64 | U8 | U16 | U32 | U64)
    }
}

macro_rules! value_enum {
    ($(#[$meta:meta])* $name:ident $(<$lt:lifetime>)?, $str:ty, $blob:ty) => {
        $(#[$meta])*
        #[derive(Debug, Clone, PartialEq)]
        pub enum $name $(<$lt>)? {
            /// No value (valid in every column).
            Null,
            /// `i8`.
            I8(i8),
            /// `i16`.
            I16(i16),
            /// `i32`.
            I32(i32),
            /// `i64`.
            I64(i64),
            /// `u8`.
            U8(u8),
            /// `u16`.
            U16(u16),
            /// `u32`.
            U32(u32),
            /// `u64`.
            U64(u64),
            /// `f32`.
            F32(f32),
            /// `f64`.
            F64(f64),
            /// `bool`.
            Bool(bool),
            /// Unicode scalar value.
            Char(char),
            /// Seconds since the Unix epoch.
            TimestampSec(i64),
            /// Milliseconds since the Unix epoch.
            TimestampMs(i64),
            /// Microseconds since the Unix epoch.
            TimestampUs(i64),
            /// Nanoseconds since the Unix epoch.
            TimestampNs(i64),
            /// Days since 1970-01-01.
            Date(i32),
            /// Nanoseconds since midnight (less than 86,400,000,000,000).
            TimeOfDay(u64),
            /// Milliseconds since the epoch (within +-2^47) and a UTC offset in minutes.
            TimestampTz {
                /// Milliseconds since the Unix epoch.
                millis: i64,
                /// Offset from UTC in minutes.
                offset_minutes: i16,
            },
            /// Text.
            Str($str),
            /// Bytes.
            Blob($blob),
        }
    };
}

value_enum!(
    /// A value being inserted; text and bytes are borrowed.
    Value<'a>, &'a str, &'a [u8]
);
value_enum!(
    /// A value read back from a dataframe; text and bytes are owned.
    OwnedValue,
    String,
    Vec<u8>
);

impl Value<'_> {
    /// An owned copy.
    pub fn to_owned_value(&self) -> OwnedValue {
        match self.clone() {
            Value::Null => OwnedValue::Null,
            Value::I8(v) => OwnedValue::I8(v),
            Value::I16(v) => OwnedValue::I16(v),
            Value::I32(v) => OwnedValue::I32(v),
            Value::I64(v) => OwnedValue::I64(v),
            Value::U8(v) => OwnedValue::U8(v),
            Value::U16(v) => OwnedValue::U16(v),
            Value::U32(v) => OwnedValue::U32(v),
            Value::U64(v) => OwnedValue::U64(v),
            Value::F32(v) => OwnedValue::F32(v),
            Value::F64(v) => OwnedValue::F64(v),
            Value::Bool(v) => OwnedValue::Bool(v),
            Value::Char(v) => OwnedValue::Char(v),
            Value::TimestampSec(v) => OwnedValue::TimestampSec(v),
            Value::TimestampMs(v) => OwnedValue::TimestampMs(v),
            Value::TimestampUs(v) => OwnedValue::TimestampUs(v),
            Value::TimestampNs(v) => OwnedValue::TimestampNs(v),
            Value::Date(v) => OwnedValue::Date(v),
            Value::TimeOfDay(v) => OwnedValue::TimeOfDay(v),
            Value::TimestampTz { millis, offset_minutes } => OwnedValue::TimestampTz { millis, offset_minutes },
            Value::Str(v) => OwnedValue::Str(v.to_owned()),
            Value::Blob(v) => OwnedValue::Blob(v.to_vec()),
        }
    }
}

impl OwnedValue {
    /// A borrowing view, for inserting a value read from another dataframe.
    pub fn as_value(&self) -> Value<'_> {
        match self {
            OwnedValue::Null => Value::Null,
            OwnedValue::I8(v) => Value::I8(*v),
            OwnedValue::I16(v) => Value::I16(*v),
            OwnedValue::I32(v) => Value::I32(*v),
            OwnedValue::I64(v) => Value::I64(*v),
            OwnedValue::U8(v) => Value::U8(*v),
            OwnedValue::U16(v) => Value::U16(*v),
            OwnedValue::U32(v) => Value::U32(*v),
            OwnedValue::U64(v) => Value::U64(*v),
            OwnedValue::F32(v) => Value::F32(*v),
            OwnedValue::F64(v) => Value::F64(*v),
            OwnedValue::Bool(v) => Value::Bool(*v),
            OwnedValue::Char(v) => Value::Char(*v),
            OwnedValue::TimestampSec(v) => Value::TimestampSec(*v),
            OwnedValue::TimestampMs(v) => Value::TimestampMs(*v),
            OwnedValue::TimestampUs(v) => Value::TimestampUs(*v),
            OwnedValue::TimestampNs(v) => Value::TimestampNs(*v),
            OwnedValue::Date(v) => Value::Date(*v),
            OwnedValue::TimeOfDay(v) => Value::TimeOfDay(*v),
            OwnedValue::TimestampTz { millis, offset_minutes } => Value::TimestampTz { millis: *millis, offset_minutes: *offset_minutes },
            OwnedValue::Str(v) => Value::Str(v),
            OwnedValue::Blob(v) => Value::Blob(v),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn codes_round_trip() {
        for code in 0..=255u8 {
            match ColumnType::from_code(code) {
                Some(t) => assert_eq!(t.code(), code),
                None => assert!(code == 0 || code > 21),
            }
        }
    }

    #[test]
    fn owned_and_borrowed_convert() {
        let blob = [1u8, 2, 3];
        let v = Value::Blob(&blob);
        assert_eq!(v.to_owned_value().as_value(), v);
        assert_eq!(Value::Str("é").to_owned_value(), OwnedValue::Str("é".into()));
    }
}
