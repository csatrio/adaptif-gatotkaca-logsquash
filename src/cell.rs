//! Order-preserving `u64` cell encodings of fixed-size types (DESIGN.md 6.2.1).
//!
//! Comparing two encoded cells as unsigned integers gives the same order as comparing the values.

use crate::error::{Error, Result};
use crate::types::{ColumnType, OwnedValue, Value};

const SIGN64: u64 = 1 << 63;
const SIGN32: u32 = 1 << 31;
/// `TimestampTz` keeps 48 bits of biased milliseconds: the valid range is `-2^47 <= millis < 2^47`.
const TZ_BIAS: i64 = 1 << 47;
const TZ_OFFSET_BIAS: i32 = 1 << 15;
const NANOS_PER_DAY: u64 = 86_400_000_000_000;

/// Encodes a signed integer.
pub fn from_signed(v: i64) -> u64 {
    (v as u64) ^ SIGN64
}

/// Decodes [`from_signed`].
pub fn to_signed(cell: u64) -> i64 {
    (cell ^ SIGN64) as i64
}

/// Encodes an `f64` bit-exactly.
pub fn from_f64(v: f64) -> u64 {
    let bits = v.to_bits();
    if bits & SIGN64 != 0 { !bits } else { bits | SIGN64 }
}

/// Decodes [`from_f64`].
pub fn to_f64(cell: u64) -> f64 {
    f64::from_bits(if cell & SIGN64 != 0 { cell & !SIGN64 } else { !cell })
}

/// Encodes an `f32` bit-exactly in the low 32 bits.
pub fn from_f32(v: f32) -> u64 {
    let bits = v.to_bits();
    u64::from(if bits & SIGN32 != 0 { !bits } else { bits | SIGN32 })
}

/// Decodes [`from_f32`].
pub fn to_f32(cell: u64) -> f32 {
    let cell = cell as u32;
    f32::from_bits(if cell & SIGN32 != 0 { cell & !SIGN32 } else { !cell })
}

/// Packs milliseconds and a UTC offset into one cell; `None` if the milliseconds do not fit 48 bits.
pub fn from_tz(millis: i64, offset_minutes: i16) -> Option<u64> {
    let biased = u64::try_from(millis.checked_add(TZ_BIAS)?).ok().filter(|&b| b < 1 << 48)?;
    Some((biased << 16) | (i32::from(offset_minutes) + TZ_OFFSET_BIAS) as u64)
}

/// Decodes [`from_tz`] into `(millis, offset_minutes)`.
pub fn to_tz(cell: u64) -> (i64, i16) {
    ((cell >> 16) as i64 - TZ_BIAS, ((cell & 0xffff) as i32 - TZ_OFFSET_BIAS) as i16)
}

/// Encodes `value` for a column of type `ty`. `Ok(None)` means null. Only for fixed-size types.
pub fn encode(ty: ColumnType, column: &str, value: &Value<'_>) -> Result<Option<u64>> {
    use ColumnType as T;
    let mismatch = || Error::TypeMismatch { column: column.to_owned() };
    let range = || Error::ValueOutOfRange { column: column.to_owned() };
    if matches!(value, Value::Null) {
        return Ok(None);
    }
    let cell = match (ty, value) {
        (t, v) if t.is_integer() => {
            let n: i128 = match *v {
                Value::I8(n) => n.into(),
                Value::I16(n) => n.into(),
                Value::I32(n) => n.into(),
                Value::I64(n) => n.into(),
                Value::U8(n) => n.into(),
                Value::U16(n) => n.into(),
                Value::U32(n) => n.into(),
                Value::U64(n) => n.into(),
                _ => return Err(mismatch()),
            };
            let (min, max): (i128, i128) = match t {
                T::I8 => (i8::MIN.into(), i8::MAX.into()),
                T::I16 => (i16::MIN.into(), i16::MAX.into()),
                T::I32 => (i32::MIN.into(), i32::MAX.into()),
                T::I64 => (i64::MIN.into(), i64::MAX.into()),
                T::U8 => (0, u8::MAX.into()),
                T::U16 => (0, u16::MAX.into()),
                T::U32 => (0, u32::MAX.into()),
                _ => (0, u64::MAX.into()),
            };
            if n < min || n > max {
                return Err(range());
            }
            if min < 0 { from_signed(n as i64) } else { n as u64 }
        }
        (T::F64, Value::F64(x)) => from_f64(*x),
        (T::F64, Value::F32(x)) => {
            let wide = f64::from(*x);
            if (wide as f32).to_bits() != x.to_bits() {
                return Err(range());
            }
            from_f64(wide)
        }
        (T::F32, Value::F32(x)) => from_f32(*x),
        (T::F32, Value::F64(x)) => {
            if f64::from(*x as f32).to_bits() != x.to_bits() {
                return Err(range());
            }
            from_f32(*x as f32)
        }
        (T::Bool, Value::Bool(b)) => u64::from(*b),
        (T::Char, Value::Char(c)) => u64::from(*c),
        (T::TimestampSec, Value::TimestampSec(n))
        | (T::TimestampMs, Value::TimestampMs(n))
        | (T::TimestampUs, Value::TimestampUs(n))
        | (T::TimestampNs, Value::TimestampNs(n)) => from_signed(*n),
        (T::Date, Value::Date(n)) => from_signed(i64::from(*n)),
        (T::TimeOfDay, Value::TimeOfDay(n)) if *n < NANOS_PER_DAY => *n,
        (T::TimeOfDay, Value::TimeOfDay(_)) => return Err(range()),
        (T::TimestampTz, Value::TimestampTz { millis, offset_minutes }) => from_tz(*millis, *offset_minutes).ok_or_else(range)?,
        _ => return Err(mismatch()),
    };
    Ok(Some(cell))
}

/// Decodes the cell of a non-null fixed-size value.
pub fn decode(ty: ColumnType, cell: u64) -> OwnedValue {
    use ColumnType as T;
    match ty {
        T::I8 => OwnedValue::I8(to_signed(cell) as i8),
        T::I16 => OwnedValue::I16(to_signed(cell) as i16),
        T::I32 => OwnedValue::I32(to_signed(cell) as i32),
        T::I64 => OwnedValue::I64(to_signed(cell)),
        T::U8 => OwnedValue::U8(cell as u8),
        T::U16 => OwnedValue::U16(cell as u16),
        T::U32 => OwnedValue::U32(cell as u32),
        T::U64 => OwnedValue::U64(cell),
        T::F32 => OwnedValue::F32(to_f32(cell)),
        T::F64 => OwnedValue::F64(to_f64(cell)),
        T::Bool => OwnedValue::Bool(cell != 0),
        T::Char => OwnedValue::Char(char::from_u32(cell as u32).unwrap_or(char::REPLACEMENT_CHARACTER)),
        T::TimestampSec => OwnedValue::TimestampSec(to_signed(cell)),
        T::TimestampMs => OwnedValue::TimestampMs(to_signed(cell)),
        T::TimestampUs => OwnedValue::TimestampUs(to_signed(cell)),
        T::TimestampNs => OwnedValue::TimestampNs(to_signed(cell)),
        T::Date => OwnedValue::Date(to_signed(cell) as i32),
        T::TimeOfDay => OwnedValue::TimeOfDay(cell),
        T::TimestampTz => {
            let (millis, offset_minutes) = to_tz(cell);
            OwnedValue::TimestampTz { millis, offset_minutes }
        }
        T::String | T::Blob => OwnedValue::Null,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn signed_floats_and_tz_keep_their_order() {
        let signed = [i64::MIN, -5, -1, 0, 1, 5, i64::MAX];
        assert!(signed.windows(2).all(|w| from_signed(w[0]) < from_signed(w[1])));
        let floats = [f64::NEG_INFINITY, -1e300, -1.5, -0.0, 0.0, 1e-300, 2.5, f64::INFINITY];
        assert!(floats.windows(2).all(|w| from_f64(w[0]) < from_f64(w[1])));
        let floats32 = [f32::NEG_INFINITY, -2.5, -0.0, 0.0, 1.5, f32::INFINITY];
        assert!(floats32.windows(2).all(|w| from_f32(w[0]) < from_f32(w[1])));
        assert!(from_tz(1000, 420) < from_tz(1000, 421));
        assert!(from_tz(1000, 600) < from_tz(1001, -600));
    }

    #[test]
    fn tz_range() {
        assert!(from_tz(-(1 << 47), 0).is_some() && from_tz((1 << 47) - 1, 0).is_some());
        assert!(from_tz(1 << 47, 0).is_none() && from_tz(-(1 << 47) - 1, 0).is_none());
        assert_eq!(to_tz(from_tz(-1, -720).unwrap()), (-1, -720));
    }

    #[test]
    fn range_and_type_errors() {
        let t = |ty, v: Value| encode(ty, "c", &v);
        assert!(matches!(t(ColumnType::I8, Value::I64(300)), Err(Error::ValueOutOfRange { .. })));
        assert!(matches!(t(ColumnType::U8, Value::I32(-1)), Err(Error::ValueOutOfRange { .. })));
        assert!(matches!(t(ColumnType::I64, Value::U64(u64::MAX)), Err(Error::ValueOutOfRange { .. })));
        assert!(matches!(t(ColumnType::F32, Value::F64(0.1)), Err(Error::ValueOutOfRange { .. })));
        assert!(matches!(t(ColumnType::I64, Value::Str("1")), Err(Error::TypeMismatch { .. })));
        assert!(matches!(t(ColumnType::TimestampMs, Value::TimestampSec(1)), Err(Error::TypeMismatch { .. })));
        assert!(matches!(t(ColumnType::TimeOfDay, Value::TimeOfDay(NANOS_PER_DAY)), Err(Error::ValueOutOfRange { .. })));
        assert_eq!(t(ColumnType::I8, Value::U64(127)).unwrap(), Some(from_signed(127)));
        assert_eq!(t(ColumnType::F32, Value::F64(0.5)).unwrap(), Some(from_f32(0.5)));
        assert_eq!(t(ColumnType::Date, Value::Null).unwrap(), None);
    }

    proptest! {
        #[test]
        fn signed_order_and_round_trip(a: i64, b: i64) {
            prop_assert_eq!(to_signed(from_signed(a)), a);
            prop_assert_eq!(a.cmp(&b), from_signed(a).cmp(&from_signed(b)));
        }

        #[test]
        fn f64_order_and_bit_exact_round_trip(abits: u64, bbits: u64) {
            let (a, b) = (f64::from_bits(abits), f64::from_bits(bbits));
            prop_assert_eq!(to_f64(from_f64(a)).to_bits(), abits);
            prop_assert_eq!(a.total_cmp(&b), from_f64(a).cmp(&from_f64(b)));
        }

        #[test]
        fn f32_order_and_bit_exact_round_trip(abits: u32, bbits: u32) {
            let (a, b) = (f32::from_bits(abits), f32::from_bits(bbits));
            prop_assert_eq!(to_f32(from_f32(a)).to_bits(), abits);
            prop_assert_eq!(a.total_cmp(&b), from_f32(a).cmp(&from_f32(b)));
        }

        #[test]
        fn tz_round_trip(millis in -(1i64 << 47)..(1i64 << 47), offset: i16) {
            prop_assert_eq!(to_tz(from_tz(millis, offset).unwrap()), (millis, offset));
        }
    }
}
