//! Loads a directory of JSONL logs into one wide in-memory dataframe, then reads every row back and checks that
//! it equals the source line (the corpus check for milestone 2).
//!
//! Usage: `cargo run --release --example dataframe_corpus -- <dir> [step] [zstd level]`
//!
//! Columns are the union of all keys: strings stay `String`, integers `I64`, floats `F64`, and nested JSON
//! (`data`) is stored as JSON text in a `json_escapes` string column.

use adaptif_gatotkaca_logsquash::{Column, ColumnType, DataFrame, Dictionary, MemHeap, OwnedValue, Schema, Value, WordStore};
use serde_json::{Map, Value as Json};
use std::path::PathBuf;
use std::time::Instant;

type Fallible<T> = Result<T, Box<dyn std::error::Error>>;

fn read_lines(files: &[PathBuf]) -> Fallible<Vec<Map<String, Json>>> {
    let mut lines = Vec::new();
    for path in files {
        for line in std::fs::read_to_string(path)?.lines() {
            if let Json::Object(object) = serde_json::from_str(line)? {
                lines.push(object);
            }
        }
    }
    Ok(lines)
}

fn infer_schema(lines: &[Map<String, Json>]) -> Fallible<Schema> {
    let mut columns: Vec<Column> = Vec::new();
    let mut seen_type: Vec<Option<ColumnType>> = Vec::new();
    for object in lines {
        for (key, value) in object {
            let ty = match value {
                Json::Null => None,
                Json::String(_) => Some(ColumnType::String),
                Json::Bool(_) => Some(ColumnType::Bool),
                Json::Number(n) if n.is_i64() => Some(ColumnType::I64),
                Json::Number(_) => Some(ColumnType::F64),
                Json::Object(_) | Json::Array(_) => Some(ColumnType::String),
            };
            let index = match columns.iter().position(|c| &c.name == key) {
                Some(i) => i,
                None => {
                    columns.push(Column::new(key.clone(), ColumnType::String));
                    seen_type.push(None);
                    columns.len() - 1
                }
            };
            let nested = matches!(value, Json::Object(_) | Json::Array(_));
            if nested {
                columns[index].flags.json_escapes = true;
            }
            seen_type[index] = match (seen_type[index], ty) {
                (current, None) => current,
                (None, new) => new,
                (Some(a), Some(b)) if a == b => Some(a),
                (Some(ColumnType::I64), Some(ColumnType::F64)) | (Some(ColumnType::F64), Some(ColumnType::I64)) => Some(ColumnType::F64),
                (Some(a), Some(b)) => return Err(format!("column {key}: {a:?} and {b:?} mixed").into()),
            };
        }
    }
    for (column, ty) in columns.iter_mut().zip(seen_type) {
        column.ty = ty.unwrap_or(ColumnType::String);
    }
    Ok(Schema::new(columns)?)
}

/// The text a JSON value is stored as in a string column.
fn text_of(value: &Json) -> Option<String> {
    match value {
        Json::Null => None,
        Json::String(s) => Some(s.clone()),
        other => Some(serde_json::to_string(other).expect("JSON serialises")),
    }
}

fn to_value<'a>(ty: ColumnType, value: &'a Json, text: &'a Option<String>) -> Value<'a> {
    match (ty, value) {
        (_, Json::Null) => Value::Null,
        (ColumnType::I64, Json::Number(n)) => Value::I64(n.as_i64().expect("integer")),
        (ColumnType::F64, Json::Number(n)) => Value::F64(n.as_f64().expect("number")),
        (ColumnType::Bool, Json::Bool(b)) => Value::Bool(*b),
        _ => text.as_deref().map_or(Value::Null, Value::Str),
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

fn main() -> Fallible<()> {
    let args: Vec<String> = std::env::args().collect();
    let dir = PathBuf::from(args.get(1).ok_or("usage: dataframe_corpus <dir> [step] [zstd level]")?);
    let step: usize = args.get(2).map_or(Ok(10), |s| s.parse())?;
    let level: i32 = args.get(3).map_or(Ok(19), |s| s.parse())?;
    let mut files: Vec<PathBuf> = std::fs::read_dir(&dir)?.filter_map(|e| e.ok().map(|e| e.path())).collect();
    files.retain(|p| p.extension().is_some_and(|e| e == "jsonl"));
    files.sort();
    let files: Vec<PathBuf> = files.into_iter().step_by(step).collect();

    let lines = read_lines(&files)?;
    let raw: usize = files.iter().map(|f| std::fs::metadata(f).map_or(0, |m| m.len() as usize)).sum();
    let schema = infer_schema(&lines)?;
    println!("{} files, {} rows, {} columns, {:.2} MB raw", files.len(), lines.len(), schema.len(), raw as f64 / 1e6);

    let words = WordStore::<MemHeap>::new(MemHeap::new(), Dictionary::new(64 << 20)).into_shared();
    let frame = DataFrame::new(schema.clone(), words.clone());
    let started = Instant::now();
    for object in &lines {
        let present: Vec<Option<&Json>> = schema.columns().iter().map(|c| object.get(&c.name)).collect();
        let texts: Vec<Option<String>> = present.iter().map(|v| v.and_then(text_of)).collect();
        let row: Vec<Value> =
            schema.columns().iter().zip(&present).zip(&texts).map(|((c, v), t)| v.map_or(Value::Null, |v| to_value(c.ty, v, t))).collect();
        frame.push_row(&row)?;
    }
    println!("inserted in {:.1}s", started.elapsed().as_secs_f64());

    let started = Instant::now();
    let mut mismatches = 0u64;
    for (n, (object, row)) in lines.iter().zip(frame.rows()).enumerate() {
        let row = row?;
        for (column, got) in schema.columns().iter().zip(&row) {
            if *got != expected(column.ty, object.get(&column.name)) {
                mismatches += 1;
                if mismatches <= 5 {
                    println!("MISMATCH row {n} column {}: {:?}", column.name, got);
                }
            }
        }
    }
    println!(
        "read back and compared {} rows x {} columns in {:.1}s: {} mismatches",
        frame.len(),
        schema.len(),
        started.elapsed().as_secs_f64(),
        mismatches
    );

    let row_heap = frame.row_heap_snapshot();
    let store = words.read().unwrap();
    let z_row_heap = zstd::encode_all(&row_heap[..], level)?.len();
    let z_heap = zstd::encode_all(store.heap().as_bytes(), level)?.len();
    let cells = frame.len() as usize * schema.len() * 8;
    println!(
        "row heap {:.2} MB ({} whole-string entries), words {} ({:.2} MB), cells {:.2} MB",
        row_heap.len() as f64 / 1e6,
        frame.dedup_len(),
        store.word_count(),
        store.heap().as_bytes().len() as f64 / 1e6,
        cells as f64 / 1e6
    );
    let total = z_row_heap + z_heap + cells;
    println!(
        "estimated stored (zstd-{level} row heap + heap + raw cells): {:.2} MB = {:.1}% of raw",
        total as f64 / 1e6,
        100.0 * total as f64 / raw as f64
    );
    if mismatches > 0 {
        return Err("round trip failed".into());
    }
    Ok(())
}
