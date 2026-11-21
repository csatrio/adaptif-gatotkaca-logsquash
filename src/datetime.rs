//! Date and time text (DESIGN.md 6.2.2): parsing text into the values of date/time columns, and formatting them back.
//!
//! Every date/time column names its text format in the schema (`Column::text_format`):
//!
//! | spec | meaning |
//! |------|---------|
//! | `""` or `builtin:iso8601` | ISO-8601 / RFC-3339, for example `2026-11-21T10:15:30.123+07:00`, `2026-11-21 10:15:30`, `2026-11-21` |
//! | `builtin:rfc2822` | RFC-2822 / HTTP date, for example `Sat, 21 Nov 2026 10:15:30 +0700` (timestamps only) |
//! | `builtin:clf` | Common Log Format time, for example `21/Nov/2026:10:15:30 +0700` (timestamps only) |
//! | `strftime:<pattern>` | a chrono/strftime pattern; text with a UTC offset must use `%z` or `%:z` |
//!
//! Rules: text without a UTC offset is read as UTC (a `TimestampTz` then gets offset 0). A value that does not parse, a
//! UTC offset with seconds, a leap second (`23:59:60`), and a value that does not fit the column's unit or range are
//! errors. Parsing never loses precision: text finer than the column's unit (for example milliseconds in a
//! `TimestampSec` column) is an error, so pick the unit that matches the data. Formatting writes as many fractional digits as the unit has
//! (seconds none, milliseconds 3, microseconds 6, nanoseconds 9); `rfc2822` and `clf` have no fractions, so they drop
//! them when formatting a finer unit.

use crate::error::{Error, Result};
use crate::types::{ColumnType, OwnedValue};
use chrono::format::{Item, StrftimeItems};
use chrono::{DateTime, Datelike, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime, SecondsFormat, TimeZone, Timelike};
use std::fmt::Write;

const NANOS_PER_SEC: i64 = 1_000_000_000;
const NANOS_PER_DAY: u64 = 86_400_000_000_000;
/// Days from 0001-01-01 (day 1 of the common era) to 1970-01-01.
const DAYS_FROM_CE_TO_EPOCH: i64 = 719_163;
const CLF: &str = "%d/%b/%Y:%H:%M:%S %z";

/// A parsed `text_format` spec.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TextFormat {
    /// ISO-8601 / RFC-3339.
    Iso8601,
    /// RFC-2822 / HTTP date.
    Rfc2822,
    /// Common Log Format time.
    Clf,
    /// A strftime pattern.
    Strftime(String),
}

impl TextFormat {
    /// Parses a spec such as `builtin:clf` or `strftime:%d.%m.%Y`. The empty spec is ISO-8601.
    pub fn parse(spec: &str) -> Result<Self> {
        match spec {
            "" | "builtin:iso8601" => Ok(TextFormat::Iso8601),
            "builtin:rfc2822" => Ok(TextFormat::Rfc2822),
            "builtin:clf" => Ok(TextFormat::Clf),
            _ => match spec.strip_prefix("strftime:") {
                Some(pattern) if !pattern.is_empty() && !StrftimeItems::new(pattern).any(|i| matches!(i, Item::Error)) => {
                    Ok(TextFormat::Strftime(pattern.to_owned()))
                }
                Some(_) => Err(Error::InvalidSchema("invalid strftime pattern")),
                None => Err(Error::InvalidSchema("unknown text format")),
            },
        }
    }
}

/// Checks that `spec` is a valid text format for a column of type `ty` (used when a schema is built).
pub(crate) fn validate(ty: ColumnType, spec: &str) -> Result<()> {
    let format = TextFormat::parse(spec)?;
    if matches!(format, TextFormat::Rfc2822 | TextFormat::Clf) && matches!(ty, ColumnType::Date | ColumnType::TimeOfDay) {
        return Err(Error::InvalidSchema("rfc2822 and clf are only for timestamp columns"));
    }
    Ok(())
}

fn fail(reason: impl Into<String>) -> Error {
    Error::DateParse { column: String::new(), reason: reason.into() }
}

fn utc_naive(naive: NaiveDateTime) -> DateTime<FixedOffset> {
    FixedOffset::east_opt(0).expect("zero offset").from_utc_datetime(&naive)
}

fn midnight(date: NaiveDate) -> NaiveDateTime {
    date.and_hms_opt(0, 0, 0).expect("midnight exists")
}

fn parse_iso(text: &str) -> Result<DateTime<FixedOffset>> {
    if let Ok(dt) = DateTime::parse_from_rfc3339(text) {
        return Ok(dt);
    }
    for pattern in ["%Y-%m-%d %H:%M:%S%.f%:z", "%Y-%m-%dT%H:%M:%S%.f%z", "%Y-%m-%d %H:%M:%S%.f%z"] {
        if let Ok(dt) = DateTime::parse_from_str(text, pattern) {
            return Ok(dt);
        }
    }
    for pattern in ["%Y-%m-%dT%H:%M:%S%.f", "%Y-%m-%d %H:%M:%S%.f", "%Y-%m-%dT%H:%M", "%Y-%m-%d %H:%M"] {
        if let Ok(naive) = NaiveDateTime::parse_from_str(text, pattern) {
            return Ok(utc_naive(naive));
        }
    }
    NaiveDate::parse_from_str(text, "%Y-%m-%d").map(|d| utc_naive(midnight(d))).map_err(|_| fail("not an ISO-8601 / RFC-3339 date or time"))
}

fn parse_pattern(pattern: &str, text: &str) -> Result<DateTime<FixedOffset>> {
    if let Ok(dt) = DateTime::parse_from_str(text, pattern) {
        return Ok(dt);
    }
    if let Ok(naive) = NaiveDateTime::parse_from_str(text, pattern) {
        return Ok(utc_naive(naive));
    }
    NaiveDate::parse_from_str(text, pattern)
        .map(|d| utc_naive(midnight(d)))
        .map_err(|_| fail(format!("does not match the pattern {pattern:?}")))
}

fn parse_datetime(format: &TextFormat, text: &str) -> Result<DateTime<FixedOffset>> {
    let dt = match format {
        TextFormat::Iso8601 => parse_iso(text)?,
        TextFormat::Rfc2822 => DateTime::parse_from_rfc2822(text).map_err(|_| fail("not an RFC-2822 date"))?,
        TextFormat::Clf => DateTime::parse_from_str(text, CLF).map_err(|_| fail("not a Common Log Format time"))?,
        TextFormat::Strftime(pattern) => parse_pattern(pattern, text)?,
    };
    if dt.nanosecond() >= 1_000_000_000 {
        return Err(fail("leap seconds are not supported"));
    }
    if dt.offset().local_minus_utc() % 60 != 0 {
        return Err(fail("a UTC offset with seconds is not supported"));
    }
    Ok(dt)
}

fn parse_time(format: &TextFormat, text: &str) -> Result<NaiveTime> {
    let time = match format {
        TextFormat::Strftime(pattern) => {
            NaiveTime::parse_from_str(text, pattern).map_err(|_| fail(format!("does not match the pattern {pattern:?}")))?
        }
        _ => NaiveTime::parse_from_str(text, "%H:%M:%S%.f")
            .or_else(|_| NaiveTime::parse_from_str(text, "%H:%M"))
            .map_err(|_| fail("not a time of day (HH:MM:SS[.fraction])"))?,
    };
    if time.nanosecond() >= 1_000_000_000 {
        return Err(fail("leap seconds are not supported"));
    }
    Ok(time)
}

fn parse_date(format: &TextFormat, text: &str) -> Result<NaiveDate> {
    match format {
        TextFormat::Strftime(pattern) => {
            NaiveDate::parse_from_str(text, pattern).map_err(|_| fail(format!("does not match the pattern {pattern:?}")))
        }
        _ => NaiveDate::parse_from_str(text, "%Y-%m-%d").map_err(|_| fail("not a date (YYYY-MM-DD)")),
    }
}

/// Parses `text` as a value of the date/time column type `ty` using the column's `spec`.
pub fn parse(ty: ColumnType, spec: &str, text: &str) -> Result<OwnedValue> {
    let format = TextFormat::parse(spec)?;
    let exact = |value: Option<i64>| value.ok_or_else(|| fail("outside the range of the column"));
    match ty {
        ColumnType::Date => {
            let days = parse_date(&format, text)?.num_days_from_ce() as i64 - DAYS_FROM_CE_TO_EPOCH;
            Ok(OwnedValue::Date(i32::try_from(days).map_err(|_| fail("outside the range of the column"))?))
        }
        ColumnType::TimeOfDay => {
            let time = parse_time(&format, text)?;
            Ok(OwnedValue::TimeOfDay(u64::from(time.num_seconds_from_midnight()) * 1_000_000_000 + u64::from(time.nanosecond())))
        }
        ColumnType::TimestampSec
        | ColumnType::TimestampMs
        | ColumnType::TimestampUs
        | ColumnType::TimestampNs
        | ColumnType::TimestampTz => {
            let dt = parse_datetime(&format, text)?;
            let (secs, nanos) = (dt.timestamp(), i64::from(dt.timestamp_subsec_nanos()));
            // `unit` is how many nanoseconds one count of the column is worth; finer text is rejected, not rounded.
            let unit_count = |unit: i64| -> Result<i64> {
                if nanos % unit != 0 {
                    return Err(fail("finer than the precision of the column"));
                }
                exact(secs.checked_mul(NANOS_PER_SEC / unit).and_then(|v| v.checked_add(nanos / unit)))
            };
            Ok(match ty {
                ColumnType::TimestampSec => OwnedValue::TimestampSec(unit_count(NANOS_PER_SEC)?),
                ColumnType::TimestampMs => OwnedValue::TimestampMs(unit_count(1_000_000)?),
                ColumnType::TimestampUs => OwnedValue::TimestampUs(unit_count(1_000)?),
                ColumnType::TimestampNs => OwnedValue::TimestampNs(unit_count(1)?),
                _ => {
                    OwnedValue::TimestampTz { millis: unit_count(1_000_000)?, offset_minutes: (dt.offset().local_minus_utc() / 60) as i16 }
                }
            })
        }
        _ => Err(Error::InvalidSchema("only date and time columns have a text format")),
    }
}

fn write_with(shown: impl std::fmt::Display) -> Result<String> {
    let mut out = String::new();
    write!(out, "{shown}").map_err(|_| fail("the value cannot be formatted with this pattern"))?;
    Ok(out)
}

/// The instant of a timestamp value split into whole seconds and nanoseconds.
fn split(count: i64, per_second: i64) -> (i64, u32) {
    (count.div_euclid(per_second), (count.rem_euclid(per_second) * (NANOS_PER_SEC / per_second)) as u32)
}

fn format_instant(format: &TextFormat, dt: &DateTime<FixedOffset>, utc: bool, digits: SecondsFormat) -> Result<String> {
    Ok(match format {
        TextFormat::Iso8601 => dt.to_rfc3339_opts(digits, utc),
        TextFormat::Rfc2822 => dt.to_rfc2822(),
        TextFormat::Clf => write_with(dt.format(CLF))?,
        TextFormat::Strftime(pattern) => write_with(dt.format(pattern))?,
    })
}

/// Formats a non-null value of the date/time column type `ty` using the column's `spec`.
pub fn format(ty: ColumnType, spec: &str, value: &OwnedValue) -> Result<String> {
    let format = TextFormat::parse(spec)?;
    let out_of_range = || fail("outside the range that can be formatted");
    let timestamp = |count: i64, per_second: i64, offset_minutes: i16, digits: SecondsFormat, utc: bool| -> Result<String> {
        let (secs, nanos) = split(count, per_second);
        let offset = FixedOffset::east_opt(i32::from(offset_minutes) * 60).ok_or_else(out_of_range)?;
        let dt = offset.timestamp_opt(secs, nanos).single().ok_or_else(out_of_range)?;
        format_instant(&format, &dt, utc, digits)
    };
    match (ty, value) {
        (ColumnType::TimestampSec, OwnedValue::TimestampSec(v)) => timestamp(*v, 1, 0, SecondsFormat::Secs, true),
        (ColumnType::TimestampMs, OwnedValue::TimestampMs(v)) => timestamp(*v, 1_000, 0, SecondsFormat::Millis, true),
        (ColumnType::TimestampUs, OwnedValue::TimestampUs(v)) => timestamp(*v, 1_000_000, 0, SecondsFormat::Micros, true),
        (ColumnType::TimestampNs, OwnedValue::TimestampNs(v)) => timestamp(*v, NANOS_PER_SEC, 0, SecondsFormat::Nanos, true),
        (ColumnType::TimestampTz, OwnedValue::TimestampTz { millis, offset_minutes }) => {
            timestamp(*millis, 1_000, *offset_minutes, SecondsFormat::Millis, false)
        }
        (ColumnType::Date, OwnedValue::Date(days)) => {
            let days_ce = i32::try_from(i64::from(*days) + DAYS_FROM_CE_TO_EPOCH).map_err(|_| out_of_range())?;
            let date = NaiveDate::from_num_days_from_ce_opt(days_ce).ok_or_else(out_of_range)?;
            match &format {
                TextFormat::Strftime(pattern) => write_with(date.format(pattern)),
                _ => write_with(date.format("%Y-%m-%d")),
            }
        }
        (ColumnType::TimeOfDay, OwnedValue::TimeOfDay(nanos)) => {
            if *nanos >= NANOS_PER_DAY {
                return Err(out_of_range());
            }
            let secs = (*nanos / 1_000_000_000) as u32;
            let time = NaiveTime::from_num_seconds_from_midnight_opt(secs, (*nanos % 1_000_000_000) as u32).ok_or_else(out_of_range)?;
            match &format {
                TextFormat::Strftime(pattern) => write_with(time.format(pattern)),
                _ => write_with(time.format("%H:%M:%S%.9f")),
            }
        }
        _ => Err(Error::TypeMismatch { column: String::new() }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn ts(ty: ColumnType, spec: &str, text: &str) -> OwnedValue {
        parse(ty, spec, text).unwrap_or_else(|e| panic!("{text:?}: {e}"))
    }

    fn reason(result: Result<OwnedValue>) -> String {
        match result {
            Err(Error::DateParse { reason, .. }) => reason,
            other => panic!("expected a DateParse error, got {other:?}"),
        }
    }

    // 2026-11-21T10:15:30Z
    const SECS: i64 = 1_795_256_130;

    #[test]
    fn iso_8601_in_its_common_shapes() {
        use ColumnType::*;
        assert_eq!(ts(TimestampMs, "", "2026-11-21T10:15:30.123Z"), OwnedValue::TimestampMs(SECS * 1000 + 123));
        assert_eq!(ts(TimestampMs, "", "2026-11-21T17:15:30.123+07:00"), OwnedValue::TimestampMs(SECS * 1000 + 123));
        assert_eq!(ts(TimestampMs, "builtin:iso8601", "2026-11-21 17:15:30.123+0700"), OwnedValue::TimestampMs(SECS * 1000 + 123));
        assert_eq!(ts(TimestampSec, "", "2026-11-21T10:15:30"), OwnedValue::TimestampSec(SECS)); // no offset: UTC
        assert_eq!(ts(TimestampSec, "", "2026-11-21 10:15:30"), OwnedValue::TimestampSec(SECS));
        assert_eq!(ts(TimestampSec, "", "2026-11-21"), OwnedValue::TimestampSec(SECS - 10 * 3600 - 15 * 60 - 30));
        assert_eq!(ts(TimestampSec, "", "1969-12-31T23:59:59Z"), OwnedValue::TimestampSec(-1));
        assert_eq!(ts(TimestampUs, "", "2026-11-21T10:15:30.123456Z"), OwnedValue::TimestampUs(SECS * 1_000_000 + 123_456));
        assert_eq!(ts(TimestampNs, "", "2026-11-21T10:15:30.123456789Z"), OwnedValue::TimestampNs(SECS * 1_000_000_000 + 123_456_789));
        assert_eq!(ts(Date, "", "2026-11-21"), OwnedValue::Date(20_778));
        assert_eq!(ts(Date, "", "1969-12-31"), OwnedValue::Date(-1));
        assert_eq!(ts(TimeOfDay, "", "10:15:30.5"), OwnedValue::TimeOfDay(36_930_500_000_000));
        assert_eq!(ts(TimeOfDay, "", "10:15"), OwnedValue::TimeOfDay(36_900_000_000_000));
    }

    #[test]
    fn timestamp_with_zone_keeps_the_offset_in_minutes() {
        let tz = |text: &str| ts(ColumnType::TimestampTz, "", text);
        assert_eq!(tz("2026-11-21T17:15:30.123+07:00"), OwnedValue::TimestampTz { millis: SECS * 1000 + 123, offset_minutes: 420 });
        assert_eq!(tz("2026-11-21T10:15:30.123Z"), OwnedValue::TimestampTz { millis: SECS * 1000 + 123, offset_minutes: 0 });
        assert_eq!(tz("2026-11-21T10:15:30"), OwnedValue::TimestampTz { millis: SECS * 1000, offset_minutes: 0 }); // no offset: +00:00
        assert_eq!(tz("2026-11-21T05:45:30-04:30"), OwnedValue::TimestampTz { millis: SECS * 1000, offset_minutes: -270 });
    }

    #[test]
    fn the_other_builtin_formats() {
        use ColumnType::*;
        assert_eq!(ts(TimestampSec, "builtin:rfc2822", "Sat, 21 Nov 2026 17:15:30 +0700"), OwnedValue::TimestampSec(SECS));
        assert_eq!(ts(TimestampSec, "builtin:rfc2822", "Sat, 21 Nov 2026 10:15:30 GMT"), OwnedValue::TimestampSec(SECS));
        assert_eq!(ts(TimestampSec, "builtin:clf", "21/Nov/2026:17:15:30 +0700"), OwnedValue::TimestampSec(SECS));
        assert_eq!(
            ts(TimestampTz, "builtin:clf", "21/Nov/2026:17:15:30 +0700"),
            OwnedValue::TimestampTz { millis: SECS * 1000, offset_minutes: 420 }
        );
        assert!(parse(TimestampSec, "builtin:clf", "2026-11-21T10:15:30Z").is_err());
        assert!(parse(TimestampSec, "", "21/Nov/2026:17:15:30 +0700").is_err());
    }

    #[test]
    fn strftime_patterns_with_and_without_an_offset() {
        use ColumnType::*;
        assert_eq!(ts(TimestampSec, "strftime:%d.%m.%Y %H:%M:%S", "21.11.2026 10:15:30"), OwnedValue::TimestampSec(SECS)); // UTC
        assert_eq!(ts(TimestampSec, "strftime:%d.%m.%Y %H:%M:%S %z", "21.11.2026 17:15:30 +0700"), OwnedValue::TimestampSec(SECS));
        assert_eq!(ts(TimestampSec, "strftime:%d.%m.%Y %H:%M:%S %:z", "21.11.2026 17:15:30 +07:00"), OwnedValue::TimestampSec(SECS));
        assert_eq!(ts(TimestampSec, "strftime:%d.%m.%Y", "21.11.2026"), OwnedValue::TimestampSec(SECS - 36_930)); // date only: midnight
        assert_eq!(ts(Date, "strftime:%d/%m/%y", "21/11/26"), OwnedValue::Date(20_778));
        assert_eq!(ts(TimeOfDay, "strftime:%I:%M %p", "10:15 PM"), OwnedValue::TimeOfDay(22 * 3_600_000_000_000 + 15 * 60_000_000_000));
        assert!(parse(TimestampSec, "strftime:%d.%m.%Y", "2026-11-21").is_err());
    }

    #[test]
    fn errors_for_what_cannot_be_stored_exactly() {
        use ColumnType::*;
        assert!(reason(parse(TimestampSec, "", "2026-11-21T10:15:30.5Z")).contains("precision"));
        assert!(reason(parse(TimestampMs, "", "2026-11-21T10:15:30.1234Z")).contains("precision"));
        assert!(reason(parse(TimestampUs, "", "2026-11-21T10:15:30.1234567Z")).contains("precision"));
        assert!(reason(parse(TimestampTz, "", "2026-11-21T10:15:30.123456Z")).contains("precision"));
        assert!(reason(parse(TimestampSec, "", "2026-11-21T23:59:60Z")).contains("leap"));
        assert!(reason(parse(TimeOfDay, "", "23:59:60")).contains("leap"));
        assert!(reason(parse(TimestampNs, "", "2300-01-01T00:00:00Z")).contains("range")); // past the nanosecond range
        assert!(reason(parse(TimestampNs, "", "1600-01-01T00:00:00Z")).contains("range"));
        for bad in ["", "yesterday", "2026-13-01", "2026-02-30", "2026-11-21T25:00:00Z", "10:15:30"] {
            assert!(parse(TimestampMs, "", bad).is_err(), "{bad:?}");
        }
        assert!(parse(Date, "", "2026-11-21T10:15:30Z").is_err());
        assert!(parse(TimeOfDay, "", "24:00:00").is_err());
        assert!(matches!(parse(I64, "", "1"), Err(Error::InvalidSchema(_))));
    }

    #[test]
    fn specs_are_validated() {
        use ColumnType::*;
        for ok in ["", "builtin:iso8601", "builtin:rfc2822", "builtin:clf", "strftime:%Y-%m-%d %H:%M:%S%.3f %z"] {
            assert!(validate(TimestampMs, ok).is_ok(), "{ok:?}");
        }
        for bad in ["iso", "builtin:other", "strftime:", "strftime:%Q", "strftime:%"] {
            assert!(matches!(validate(TimestampMs, bad), Err(Error::InvalidSchema(_))), "{bad:?}");
        }
        assert!(validate(Date, "builtin:rfc2822").is_err() && validate(TimeOfDay, "builtin:clf").is_err());
        assert!(validate(Date, "strftime:%d.%m.%Y").is_ok() && validate(Date, "builtin:iso8601").is_ok());
    }

    #[test]
    fn formatting_uses_the_precision_of_the_unit() {
        use ColumnType::*;
        let f = |ty, spec: &str, v: OwnedValue| format(ty, spec, &v).unwrap();
        assert_eq!(f(TimestampSec, "", OwnedValue::TimestampSec(SECS)), "2026-11-21T10:15:30Z");
        assert_eq!(f(TimestampMs, "", OwnedValue::TimestampMs(SECS * 1000 + 5)), "2026-11-21T10:15:30.005Z");
        assert_eq!(f(TimestampUs, "", OwnedValue::TimestampUs(SECS * 1_000_000 + 5)), "2026-11-21T10:15:30.000005Z");
        assert_eq!(f(TimestampNs, "", OwnedValue::TimestampNs(SECS * 1_000_000_000 + 5)), "2026-11-21T10:15:30.000000005Z");
        assert_eq!(
            f(TimestampTz, "", OwnedValue::TimestampTz { millis: SECS * 1000 + 123, offset_minutes: 420 }),
            "2026-11-21T17:15:30.123+07:00"
        );
        assert_eq!(f(TimestampTz, "", OwnedValue::TimestampTz { millis: SECS * 1000, offset_minutes: 0 }), "2026-11-21T10:15:30.000+00:00");
        assert_eq!(f(TimestampSec, "", OwnedValue::TimestampSec(-1)), "1969-12-31T23:59:59Z");
        assert_eq!(f(TimestampMs, "", OwnedValue::TimestampMs(-1)), "1969-12-31T23:59:59.999Z");
        assert_eq!(f(TimestampSec, "builtin:rfc2822", OwnedValue::TimestampSec(SECS)), "Sat, 21 Nov 2026 10:15:30 +0000");
        assert_eq!(f(TimestampSec, "builtin:clf", OwnedValue::TimestampSec(SECS)), "21/Nov/2026:10:15:30 +0000");
        assert_eq!(
            f(TimestampTz, "builtin:clf", OwnedValue::TimestampTz { millis: SECS * 1000, offset_minutes: 420 }),
            "21/Nov/2026:17:15:30 +0700"
        );
        assert_eq!(f(TimestampSec, "strftime:%d.%m.%Y %H:%M", OwnedValue::TimestampSec(SECS)), "21.11.2026 10:15");
        assert_eq!(f(Date, "", OwnedValue::Date(20_778)), "2026-11-21");
        assert_eq!(f(Date, "strftime:%d/%m/%y", OwnedValue::Date(20_778)), "21/11/26");
        assert_eq!(f(TimeOfDay, "", OwnedValue::TimeOfDay(36_930_500_000_000)), "10:15:30.500000000");
        assert!(format(TimeOfDay, "", &OwnedValue::TimeOfDay(NANOS_PER_DAY)).is_err());
        assert!(format(TimestampMs, "", &OwnedValue::TimestampMs(i64::MAX)).is_err());
        assert!(matches!(format(TimestampMs, "", &OwnedValue::I64(1)), Err(Error::TypeMismatch { .. })));
    }

    proptest! {
        #[test]
        fn timestamps_round_trip_in_the_default_format(secs in -62_000_000_000i64..250_000_000_000, sub in 0u32..1_000_000_000, offset in -1439i16..=1439) {
            let ms = secs * 1000 + i64::from(sub / 1_000_000);
            for (ty, v) in [
                (ColumnType::TimestampSec, OwnedValue::TimestampSec(secs)),
                (ColumnType::TimestampMs, OwnedValue::TimestampMs(ms)),
                (ColumnType::TimestampUs, OwnedValue::TimestampUs(secs * 1_000_000 + i64::from(sub / 1000))),
                (ColumnType::TimestampTz, OwnedValue::TimestampTz { millis: ms, offset_minutes: offset }),
            ] {
                let text = format(ty, "", &v).unwrap();
                prop_assert_eq!(parse(ty, "", &text).unwrap(), v, "{}", text);
            }
        }

        #[test]
        fn nanoseconds_round_trip_in_their_range(secs in -9_000_000_000i64..9_000_000_000, sub in 0i64..1_000_000_000) {
            let v = OwnedValue::TimestampNs(secs * 1_000_000_000 + sub);
            let text = format(ColumnType::TimestampNs, "", &v).unwrap();
            prop_assert_eq!(parse(ColumnType::TimestampNs, "", &text).unwrap(), v);
        }

        #[test]
        fn whole_seconds_round_trip_in_every_timestamp_format(secs in 0i64..250_000_000_000) {
            let v = OwnedValue::TimestampSec(secs);
            for spec in ["", "builtin:rfc2822", "builtin:clf", "strftime:%Y-%m-%d %H:%M:%S %z", "strftime:%Y%m%d%H%M%S"] {
                let text = format(ColumnType::TimestampSec, spec, &v).unwrap();
                prop_assert_eq!(parse(ColumnType::TimestampSec, spec, &text).unwrap(), v.clone(), "{} / {}", spec, text);
            }
        }

        #[test]
        fn dates_and_times_of_day_round_trip(days in -719_162i32..2_932_896, nanos in 0u64..86_400_000_000_000) {
            let date = OwnedValue::Date(days);
            for spec in ["", "strftime:%Y/%m/%d"] {
                let text = format(ColumnType::Date, spec, &date).unwrap();
                prop_assert_eq!(parse(ColumnType::Date, spec, &text).unwrap(), date.clone());
            }
            let time = OwnedValue::TimeOfDay(nanos);
            let text = format(ColumnType::TimeOfDay, "", &time).unwrap();
            prop_assert_eq!(parse(ColumnType::TimeOfDay, "", &text).unwrap(), time);
        }
    }
}
