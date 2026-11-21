//! Writes a directory of JSONL logs into a real on-disk store, reports the file sizes, then reopens the store read-only
//! and checks every row against the source (the corpus check for milestone 3).
//!
//! Usage: `cargo run --release --example store_corpus -- <dir> <output folder> [step] [zstd level] [segment rows]`
//!
//! Streams the files twice (schema inference, then loading) and once more to verify, so memory stays modest.

use adaptif_gatotkaca_logsquash::{Column, ColumnType, OwnedValue, Schema, Store, StoreOptions, Value};
use serde_json::{Map, Value as Json};
use std::path::{Path, PathBuf};
use std::time::Instant;

/// Columns of the transcripts that hold ISO-8601 timestamps with milliseconds.
const TIMESTAMPS: [&str; 4] = ["ts", "created_at", "started_at", "ended_at"];

type Fallible<T> = Result<T, Box<dyn std::error::Error>>;

fn for_each_line(files: &[PathBuf], mut f: impl FnMut(Map<String, Json>) -> Fallible<()>) -> Fallible<()> {
    for path in files {
        for line in std::fs::read_to_string(path)?.lines() {
            if let Json::Object(object) = serde_json::from_str(line)? {
                f(object)?;
            }
        }
    }
    Ok(())
}

fn infer_schema(files: &[PathBuf]) -> Fallible<Schema> {
    let mut columns: Vec<Column> = Vec::new();
    let mut seen: Vec<Option<ColumnType>> = Vec::new();
    for_each_line(files, |object| {
        for (key, value) in &object {
            let ty = match value {
                Json::Null => None,
                Json::String(_) | Json::Object(_) | Json::Array(_) => Some(ColumnType::String),
                Json::Bool(_) => Some(ColumnType::Bool),
                Json::Number(n) if n.is_i64() => Some(ColumnType::I64),
                Json::Number(_) => Some(ColumnType::F64),
            };
            let index = columns.iter().position(|c| &c.name == key).unwrap_or_else(|| {
                columns.push(Column::new(key.clone(), ColumnType::String));
                seen.push(None);
                columns.len() - 1
            });
            if matches!(value, Json::Object(_) | Json::Array(_)) {
                columns[index].flags.json_escapes = true;
            }
            seen[index] = match (seen[index], ty) {
                (current, None) => current,
                (None, new) => new,
                (Some(a), Some(b)) if a == b => Some(a),
                (Some(ColumnType::I64), Some(ColumnType::F64)) | (Some(ColumnType::F64), Some(ColumnType::I64)) => Some(ColumnType::F64),
                (Some(a), Some(b)) => return Err(format!("column {key}: {a:?} and {b:?} mixed").into()),
            };
        }
        Ok(())
    })?;
    for (column, ty) in columns.iter_mut().zip(seen) {
        column.ty = ty.unwrap_or(ColumnType::String);
        if column.ty == ColumnType::String && TIMESTAMPS.contains(&column.name.as_str()) {
            column.ty = ColumnType::TimestampMs; // ISO-8601 text such as 2026-11-18T00:25:28.366Z, parsed by the library
        }
    }
    Ok(Schema::new(columns)?)
}

fn text_of(value: &Json) -> Option<String> {
    match value {
        Json::Null => None,
        Json::String(s) => Some(s.clone()),
        other => Some(serde_json::to_string(other).expect("JSON serialises")),
    }
}

fn expected(ty: ColumnType, value: Option<&Json>) -> OwnedValue {
    match (ty, value) {
        (_, None | Some(Json::Null)) => OwnedValue::Null,
        (ColumnType::I64, Some(Json::Number(n))) => OwnedValue::I64(n.as_i64().expect("integer")),
        (ColumnType::F64, Some(Json::Number(n))) => OwnedValue::F64(n.as_f64().expect("number")),
        (ColumnType::Bool, Some(Json::Bool(b))) => OwnedValue::Bool(*b),
        (_, Some(other)) => OwnedValue::Str(text_of(other).expect("not null")),
    }
}

fn folder_size(dir: &Path) -> Fallible<Vec<(String, u64)>> {
    let mut sizes = Vec::new();
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        sizes.push((entry.file_name().to_string_lossy().into_owned(), entry.metadata()?.len()));
    }
    sizes.sort();
    Ok(sizes)
}

fn main() -> Fallible<()> {
    let args: Vec<String> = std::env::args().collect();
    let usage = "usage: store_corpus <dir> <output folder> [step] [zstd level] [segment rows]";
    let dir = PathBuf::from(args.get(1).ok_or(usage)?);
    let out = PathBuf::from(args.get(2).ok_or(usage)?);
    let step: usize = args.get(3).map_or(Ok(10), |s| s.parse())?;
    let level: i32 = args.get(4).map_or(Ok(19), |s| s.parse())?;
    let segment_rows: usize = args.get(5).map_or(Ok(100_000), |s| s.parse())?;
    let mut files: Vec<PathBuf> = std::fs::read_dir(&dir)?.filter_map(|e| e.ok().map(|e| e.path())).collect();
    files.retain(|p| p.extension().is_some_and(|e| e == "jsonl"));
    files.sort();
    let files: Vec<PathBuf> = files.into_iter().step_by(step).collect();
    let raw: u64 = files.iter().map(|f| std::fs::metadata(f).map_or(0, |m| m.len())).sum();

    let schema = infer_schema(&files)?;
    println!("{} files, {:.2} MB raw, {} columns", files.len(), raw as f64 / 1e6, schema.len());

    if out.exists() {
        std::fs::remove_dir_all(&out)?;
    }
    let options = StoreOptions { zstd_level: level, segment_rows, ..StoreOptions::default() };
    let started = Instant::now();
    let rows = {
        let store = Store::create(&out, options.clone())?;
        let frame = store.create_frame("logs", schema.clone())?;
        let mut rows = 0u64;
        for_each_line(&files, |object| {
            let present: Vec<Option<&Json>> = schema.columns().iter().map(|c| object.get(&c.name)).collect();
            let texts: Vec<Option<String>> = present.iter().map(|v| v.and_then(text_of)).collect();
            let row: Vec<Value> = schema
                .columns()
                .iter()
                .zip(present.iter().zip(&texts))
                .map(|(c, (v, t))| match (c.ty, v) {
                    (_, None | Some(Json::Null)) => Value::Null,
                    (ColumnType::I64, Some(Json::Number(n))) => Value::I64(n.as_i64().expect("integer")),
                    (ColumnType::F64, Some(Json::Number(n))) => Value::F64(n.as_f64().expect("number")),
                    (ColumnType::Bool, Some(Json::Bool(b))) => Value::Bool(*b),
                    _ => t.as_deref().map_or(Value::Null, Value::Str),
                })
                .collect();
            frame.push_row(&row)?;
            rows += 1;
            if rows % 1000 == 0 {
                store.commit_if_full()?;
            }
            Ok(())
        })?;
        store.commit()?;
        store.seal()?;
        rows
    };
    println!("wrote {rows} rows in {:.1}s", started.elapsed().as_secs_f64());

    let sizes = folder_size(&out)?;
    let total: u64 = sizes.iter().map(|(_, n)| n).sum();
    let segments: Vec<&(String, u64)> = sizes.iter().filter(|(n, _)| n.ends_with(".seg")).collect();
    println!("--- files in {} ---", out.display());
    for (name, size) in sizes.iter().filter(|(n, _)| !n.ends_with(".seg")) {
        println!("{name:<12} {:>10.2} MB", *size as f64 / 1e6);
    }
    println!(
        "{} segments  {:>10.2} MB (largest {:.2} MB)",
        segments.len(),
        segments.iter().map(|(_, n)| n).sum::<u64>() as f64 / 1e6,
        segments.iter().map(|(_, n)| *n).max().unwrap_or(0) as f64 / 1e6
    );
    println!("TOTAL        {:>10.2} MB = {:.1}% of raw", total as f64 / 1e6, 100.0 * total as f64 / raw as f64);

    let started = Instant::now();
    let store = Store::open_read_only(&out, options)?;
    let frame = store.frame("logs")?;
    println!("opened read-only in {:.2}s, {} rows", started.elapsed().as_secs_f64(), frame.len());
    let started = Instant::now();
    let (mut n, mut mismatches) = (0u64, 0u64);
    for_each_line(&files, |object| {
        let row = frame.row(n)?;
        for (column, got) in schema.columns().iter().zip(&row) {
            let same = if column.ty.is_temporal() {
                column.format_value(got)? == object.get(&column.name).and_then(text_of)
            } else {
                *got == expected(column.ty, object.get(&column.name))
            };
            if !same {
                mismatches += 1;
                if mismatches <= 5 {
                    println!("MISMATCH row {n} column {}: {got:?}", column.name);
                }
            }
        }
        n += 1;
        Ok(())
    })?;
    println!("verified {n} rows x {} columns from disk in {:.1}s: {mismatches} mismatches", schema.len(), started.elapsed().as_secs_f64());
    if mismatches > 0 || n != frame.len() {
        return Err("round trip failed".into());
    }
    Ok(())
}
