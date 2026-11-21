//! Loads a directory of JSONL logs into a store twice, once with search indexes and once without, then reports the file
//! sizes (so the cost of the indexes is visible) and runs queries on both, checking that they agree (milestone 4).
//!
//! Usage: `cargo run --release --example search_corpus -- <dir> <output folder> [step] [zstd level] [flags]`
//! `flags` picks the indexes: `w` words on `text` and `data`, `t` words on `text` only, `c` case-insensitive on `text`,
//! `e` equality on kind/type/subtype/tool/run_id/project_id (default `wce`), `z` zstd-compressed index files.

use adaptif_gatotkaca_logsquash::{Column, ColumnType, Filter, OrderBy, Page, Query, Schema, Store, StoreOptions, Value};
use serde_json::{Map, Value as Json};
use std::path::PathBuf;
use std::time::Instant;

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

fn infer_columns(files: &[PathBuf]) -> Fallible<Vec<Column>> {
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
        if column.ty == ColumnType::String && ["ts", "created_at", "started_at", "ended_at"].contains(&column.name.as_str()) {
            column.ty = ColumnType::TimestampMs; // ISO-8601 text, parsed by the library
        }
    }
    Ok(columns)
}

fn text_of(value: &Json) -> Option<String> {
    match value {
        Json::Null => None,
        Json::String(s) => Some(s.clone()),
        other => Some(serde_json::to_string(other).expect("JSON serialises")),
    }
}

fn file_bytes(dir: &std::path::Path, prefix: &str, suffix: &str) -> u64 {
    std::fs::read_dir(dir)
        .unwrap()
        .flatten()
        .filter(|e| {
            let name = e.file_name().to_string_lossy().into_owned();
            name.starts_with(prefix) && name.ends_with(suffix)
        })
        .map(|e| e.metadata().map_or(0, |m| m.len()))
        .sum()
}

fn main() -> Fallible<()> {
    let args: Vec<String> = std::env::args().collect();
    let usage = "usage: search_corpus <dir> <output folder> [step] [zstd level]";
    let dir = PathBuf::from(args.get(1).ok_or(usage)?);
    let out = PathBuf::from(args.get(2).ok_or(usage)?);
    let step: usize = args.get(3).map_or(Ok(10), |s| s.parse())?;
    let level: i32 = args.get(4).map_or(Ok(19), |s| s.parse())?;
    let flags = args.get(5).map_or("wce", String::as_str);
    let mut files: Vec<PathBuf> = std::fs::read_dir(&dir)?.filter_map(|e| e.ok().map(|e| e.path())).collect();
    files.retain(|p| p.extension().is_some_and(|e| e == "jsonl"));
    files.sort();
    let files: Vec<PathBuf> = files.into_iter().step_by(step).collect();
    let raw: u64 = files.iter().map(|f| std::fs::metadata(f).map_or(0, |m| m.len())).sum();

    let plain_columns = infer_columns(&files)?;
    let mut indexed_columns = plain_columns.clone();
    for c in &mut indexed_columns {
        let (words_text, words_data) = (flags.contains('w') || flags.contains('t'), flags.contains('w'));
        match c.name.as_str() {
            "text" => {
                c.flags.index_words = words_text;
                c.flags.case_insensitive = flags.contains('c');
            }
            "data" => c.flags.index_words = words_data,
            "kind" | "type" | "subtype" | "tool" | "run_id" | "project_id" => c.flags.index_equality = flags.contains('e'),
            _ => {}
        }
    }
    let (indexed_schema, plain_schema) = (Schema::new(indexed_columns)?, Schema::new(plain_columns)?);
    println!("{} files, {:.2} MB raw, {} columns", files.len(), raw as f64 / 1e6, plain_schema.len());

    if out.exists() {
        std::fs::remove_dir_all(&out)?;
    }
    let options = StoreOptions { zstd_level: level, compress_indexes: flags.contains('z'), ..StoreOptions::default() };
    let started = Instant::now();
    {
        let store = Store::create(&out, options.clone())?;
        let indexed = store.create_frame("indexed", indexed_schema.clone())?;
        let plain = store.create_frame("plain", plain_schema.clone())?;
        let mut rows = 0u64;
        for_each_line(&files, |object| {
            let texts: Vec<Option<String>> = plain_schema.columns().iter().map(|c| object.get(&c.name).and_then(text_of)).collect();
            let row: Vec<Value> = plain_schema
                .columns()
                .iter()
                .zip(&texts)
                .map(|(c, t)| match (c.ty, object.get(&c.name)) {
                    (_, None | Some(Json::Null)) => Value::Null,
                    (ColumnType::I64, Some(Json::Number(n))) => Value::I64(n.as_i64().expect("integer")),
                    (ColumnType::F64, Some(Json::Number(n))) => Value::F64(n.as_f64().expect("number")),
                    (ColumnType::Bool, Some(Json::Bool(b))) => Value::Bool(*b),
                    _ => t.as_deref().map_or(Value::Null, Value::Str),
                })
                .collect();
            indexed.push_row(&row)?;
            plain.push_row(&row)?;
            rows += 1;
            if rows % 1000 == 0 {
                store.commit_if_full()?;
            }
            Ok(())
        })?;
        store.commit()?;
        store.seal()?;
        println!("wrote {rows} rows into two dataframes in {:.1}s", started.elapsed().as_secs_f64());
    }

    let heap = file_bytes(&out, "heap", ".bin");
    let (seg0, idx0, seg1) = (file_bytes(&out, "f0-", ".seg"), file_bytes(&out, "f0-", ".idx"), file_bytes(&out, "f1-", ".seg"));
    let words_idx = file_bytes(&out, "words", ".idx");
    let mb = |n: u64| n as f64 / 1e6;
    let pct = |n: u64| 100.0 * n as f64 / raw as f64;
    println!("--- sizes (heap is shared and counted once per configuration) ---");
    println!("heap.bin              {:8.2} MB", mb(heap));
    println!("without indexes       {:8.2} MB = {:5.1}% of raw  (segments {:.2} MB + heap)", mb(seg1 + heap), pct(seg1 + heap), mb(seg1));
    println!("indexes               {:8.2} MB = {:5.1}% of raw  (+ words.idx {:.2} MB)", mb(idx0), pct(idx0), mb(words_idx));
    println!("with indexes          {:8.2} MB = {:5.1}% of raw", mb(seg0 + idx0 + heap + words_idx), pct(seg0 + idx0 + heap + words_idx));

    let store = Store::open_read_only(&out, options)?;
    let (indexed, plain) = (store.frame("indexed")?, store.frame("plain")?);
    let col = |name: &str| indexed_schema.column_index(name).expect("column exists");
    let (text, data, kind, ty, subtype, tool, run_id, id) =
        (col("text"), col("data"), col("kind"), col("type"), col("subtype"), col("tool"), col("run_id"), col("id"));
    let queries: Vec<(&str, Query)> = vec![
        ("word text 'error'", Query::new().word(text, "error")),
        ("word text 'the'", Query::new().word(text, "the")),
        ("word text ci 'Error'", Query::new().word_ci(text, "Error")),
        ("word data 'estimated'", Query::new().word(data, "estimated")),
        ("phrase text 'let me'", Query::new().phrase(text, "let me")),
        ("word text ':' (single char)", Query::new().word(text, ":")),
        ("equals type 'assistant'", Query::new().equals(ty, Value::Str("assistant"))),
        ("equals tool 'Bash'", Query::new().equals(tool, Value::Str("Bash"))),
        ("equals run_id 1000", Query::new().equals(run_id, Value::I64(1000))),
        ("range id 1156473..1157473", Query::new().range(id, Some(Value::I64(1_156_473)), Some(Value::I64(1_157_473)))),
        (
            "tool_result + word 'error'",
            Query::new().equals(ty, Value::Str("user")).equals(subtype, Value::Str("tool_result")).word(data, "error"),
        ),
        (
            "ts 00:25..00:30 on 2026-11-18",
            Query::new().range(col("ts"), Some(Value::Str("2026-11-18T00:25:00Z")), Some(Value::Str("2026-11-18T00:30:00Z"))),
        ),
        (
            "kind run",
            Query::new().filter(Filter::Equals { column: kind, value: adaptif_gatotkaca_logsquash::OwnedValue::Str("run".into()) }),
        ),
    ];
    println!("--- queries ({} rows) ---   rows: indexed vs without indexes", indexed.len());
    println!("{:<30} {:>9} {:>12} {:>12}", "query", "matches", "indexed ms", "no index ms");
    let mut disagreements = 0;
    for (name, query) in &queries {
        let t = Instant::now();
        let a: Vec<u64> = indexed.search(query)?.collect::<Result<_, _>>()?;
        let ta = t.elapsed().as_secs_f64() * 1e3;
        let t = Instant::now();
        let b: Vec<u64> = plain.search(query)?.collect::<Result<_, _>>()?;
        let tb = t.elapsed().as_secs_f64() * 1e3;
        if a != b {
            disagreements += 1;
        }
        println!("{name:<30} {:>9} {ta:>12.1} {tb:>12.1}{}", a.len(), if a == b { "" } else { "   MISMATCH" });
    }
    let t = Instant::now();
    let page = indexed.search_page(
        &Query::new().equals(ty, Value::Str("assistant")),
        &Page { order_by: Some(OrderBy { column: id, descending: true }), offset: 0, limit: 50 },
    )?;
    println!("page: assistant events, id descending, first 50 of {} in {:.1} ms", page.total, t.elapsed().as_secs_f64() * 1e3);
    let t = Instant::now();
    let suggestions = store.words_with_prefix("compil", 5);
    println!("prefix 'compil' -> {suggestions:?} in {:.3} ms", t.elapsed().as_secs_f64() * 1e3);
    store.verify()?;
    println!("store.verify() ok; {disagreements} disagreements between indexed and plain results");
    if disagreements > 0 {
        return Err("indexed and plain searches disagree".into());
    }
    Ok(())
}
