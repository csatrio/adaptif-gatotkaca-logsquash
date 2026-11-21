//! Parameter sweeps on a directory of JSONL logs (milestone 6): for every configuration given, loads the logs into a fresh
//! store, then reports the size on disk and the time to write, open, hydrate and search.
//!
//! Usage: `cargo run --release --example tune_corpus -- <dir> <step> <work folder> <config> [<config> ...]`
//!
//! A config is a comma-separated list of `key=value` (an empty config, written `-`, means all defaults):
//! `level`, `block_rows`, `rh_block` (row-heap block bytes), `segment_rows`, `indexing` (0/1), `compress_idx` (0/1),
//! `dedup`, `cache` (bytes per segment), `b64_len`, `b64_ent`, `hex_len`, `opq_len`, `opq_ent`.
//! The logs are parsed once and reused for every config. Search indexes are on for the same columns as in `search_corpus`.

use adaptif_gatotkaca_logsquash::{Column, ColumnType, Heap, Query, Schema, Store, StoreOptions, Value};
use serde_json::Value as Json;
use std::path::{Path, PathBuf};
use std::time::Instant;

type Fallible<T> = Result<T, Box<dyn std::error::Error>>;

const TIMESTAMPS: [&str; 4] = ["ts", "created_at", "started_at", "ended_at"];

enum Cell {
    Null,
    Int(i64),
    Float(f64),
    Bool(bool),
    Text(String),
}

fn text_of(value: &Json) -> Option<String> {
    match value {
        Json::Null => None,
        Json::String(s) => Some(s.clone()),
        other => Some(serde_json::to_string(other).expect("JSON serialises")),
    }
}

fn load(files: &[PathBuf]) -> Fallible<(Schema, Vec<Vec<Cell>>)> {
    let mut objects = Vec::new();
    for path in files {
        for line in std::fs::read_to_string(path)?.lines() {
            if let Json::Object(object) = serde_json::from_str(line)? {
                objects.push(object);
            }
        }
    }
    let mut columns: Vec<Column> = Vec::new();
    let mut seen: Vec<Option<ColumnType>> = Vec::new();
    for object in &objects {
        for (key, value) in object {
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
    }
    for (column, ty) in columns.iter_mut().zip(seen) {
        column.ty = ty.unwrap_or(ColumnType::String);
        match column.name.as_str() {
            name if column.ty == ColumnType::String && TIMESTAMPS.contains(&name) => column.ty = ColumnType::TimestampMs,
            "text" => {
                column.flags.index_words = true;
                column.flags.case_insensitive = true;
            }
            "data" => column.flags.index_words = true,
            "kind" | "type" | "subtype" | "tool" | "run_id" | "project_id" => column.flags.index_equality = true,
            _ => {}
        }
    }
    let schema = Schema::new(columns)?;
    let rows = objects
        .iter()
        .map(|object| {
            schema
                .columns()
                .iter()
                .map(|c| match (c.ty, object.get(&c.name)) {
                    (_, None | Some(Json::Null)) => Cell::Null,
                    (ColumnType::I64, Some(Json::Number(n))) => Cell::Int(n.as_i64().expect("integer")),
                    (ColumnType::F64, Some(Json::Number(n))) => Cell::Float(n.as_f64().expect("number")),
                    (ColumnType::Bool, Some(Json::Bool(b))) => Cell::Bool(*b),
                    (_, Some(other)) => text_of(other).map_or(Cell::Null, Cell::Text),
                })
                .collect()
        })
        .collect();
    Ok((schema, rows))
}

fn folder_bytes(dir: &Path, suffix: &str) -> u64 {
    std::fs::read_dir(dir)
        .unwrap()
        .flatten()
        .filter(|e| e.file_name().to_string_lossy().ends_with(suffix))
        .map(|e| e.metadata().map_or(0, |m| m.len()))
        .sum()
}

fn options_from(config: &str) -> Fallible<StoreOptions> {
    let mut o = StoreOptions { zstd_level: 19, ..StoreOptions::default() };
    for pair in config.split(',').filter(|p| !p.is_empty() && *p != "-") {
        let (key, value) = pair.split_once('=').ok_or_else(|| format!("bad setting {pair:?}"))?;
        let int = || value.parse::<usize>().map_err(|e| format!("{key}: {e}"));
        let float = || value.parse::<f64>().map_err(|e| format!("{key}: {e}"));
        match key {
            "level" => o.zstd_level = value.parse()?,
            "block_rows" => o.block_rows = int()?,
            "rh_block" => o.row_heap_block_bytes = int()?,
            "segment_rows" => o.segment_rows = int()?,
            "indexing" => o.indexing = int()? != 0,
            "compress_idx" => o.compress_indexes = int()? != 0,
            "dedup" => o.dedup_entries = int()?,
            "cache" => o.cache_bytes = int()?,
            "b64_len" => o.tokenizer.base64_min_len = int()?,
            "b64_ent" => o.tokenizer.base64_entropy_bits = float()?,
            "hex_len" => o.tokenizer.hex_min_len = int()?,
            "opq_len" => o.tokenizer.opaque_min_len = int()?,
            "opq_ent" => o.tokenizer.opaque_entropy_bits = float()?,
            other => return Err(format!("unknown setting {other:?}").into()),
        }
    }
    Ok(o)
}

fn main() -> Fallible<()> {
    let args: Vec<String> = std::env::args().collect();
    let usage = "usage: tune_corpus <dir> <step> <work folder> <config> [<config> ...]";
    let dir = PathBuf::from(args.get(1).ok_or(usage)?);
    let step: usize = args.get(2).ok_or(usage)?.parse()?;
    let work = PathBuf::from(args.get(3).ok_or(usage)?);
    let configs = &args[4..];
    if configs.is_empty() {
        return Err(usage.into());
    }
    let mut files: Vec<PathBuf> = std::fs::read_dir(&dir)?.filter_map(|e| e.ok().map(|e| e.path())).collect();
    files.retain(|p| p.extension().is_some_and(|e| e == "jsonl"));
    files.sort();
    let files: Vec<PathBuf> = files.into_iter().step_by(step).collect();
    let raw: u64 = files.iter().map(|f| std::fs::metadata(f).map_or(0, |m| m.len())).sum();
    let (schema, rows) = load(&files)?;
    let text_column = schema.column_index("text").expect("text column");
    println!("{} rows, {:.2} MB raw, {} columns", rows.len(), raw as f64 / 1e6, schema.len());
    println!(
        "{:<44} {:>8} {:>7} {:>6} {:>8} {:>6} {:>7} {:>8} {:>9} {:>8}",
        "config", "seg MB", "idx MB", "heap", "total %", "wr s", "open ms", "row us", "scan r/s", "word ms"
    );

    for config in configs {
        let options = options_from(config)?;
        if work.exists() {
            std::fs::remove_dir_all(&work)?;
        }
        let started = Instant::now();
        {
            let store = Store::create(&work, options.clone())?;
            let frame = store.create_frame("logs", schema.clone())?;
            let mut n = 0u64;
            for row in &rows {
                let values: Vec<Value> = row
                    .iter()
                    .map(|c| match c {
                        Cell::Null => Value::Null,
                        Cell::Int(v) => Value::I64(*v),
                        Cell::Float(v) => Value::F64(*v),
                        Cell::Bool(v) => Value::Bool(*v),
                        Cell::Text(t) => Value::Str(t),
                    })
                    .collect();
                frame.push_row(&values)?;
                n += 1;
                if n % 1000 == 0 {
                    store.commit_if_full()?;
                }
            }
            store.commit()?;
            eprintln!(
                "  writer: {} words, dictionary {:.1} MB",
                store.words().read().unwrap().word_count(),
                store.words().read().unwrap().dictionary().estimated_bytes() as f64 / 1e6
            );
            store.seal()?;
        }
        let write_s = started.elapsed().as_secs_f64();
        let (seg, idx, heap) = (folder_bytes(&work, ".seg"), folder_bytes(&work, ".idx"), folder_bytes(&work, "heap.bin"));

        let started = Instant::now();
        let store = Store::open_read_only(&work, options.clone())?;
        let open_ms = started.elapsed().as_secs_f64() * 1e3;
        let frame = store.frame("logs")?;
        let total_rows = frame.len();
        // random single-row hydration (a viewer opening arbitrary rows)
        let mut x = 0x9e3779b97f4a7c15u64;
        let started = Instant::now();
        for _ in 0..2000 {
            x = x.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            frame.row((x >> 33) % total_rows)?;
        }
        let row_us = started.elapsed().as_secs_f64() * 1e6 / 2000.0;
        // sequential hydration of the first 20,000 rows
        let started = Instant::now();
        let mut scanned = 0u64;
        for r in frame.rows().take(20_000) {
            r?;
            scanned += 1;
        }
        let scan_rate = scanned as f64 / started.elapsed().as_secs_f64();
        let started = Instant::now();
        let hits = frame.count(&Query::new().word(text_column, "error"))?;
        let word_ms = started.elapsed().as_secs_f64() * 1e3;
        let _ = hits;

        let (word_count, heap_raw) = {
            let words = store.words().read().unwrap();
            (words.word_count(), words.heap().byte_len())
        };
        let total = seg + idx + heap + folder_bytes(&work, "words.idx");
        println!(
            "{:<44} {:>8.2} {:>7.2} {:>6.2} {:>7.2}% {:>6.1} {:>7.1} {:>8.0} {:>9.0} {:>8.2}  [{} words, heap {:.2} MB raw]",
            config,
            seg as f64 / 1e6,
            idx as f64 / 1e6,
            heap as f64 / 1e6,
            100.0 * total as f64 / raw as f64,
            write_s,
            open_ms,
            row_us,
            scan_rate,
            word_ms,
            word_count,
            heap_raw as f64 / 1e6
        );
    }
    std::fs::remove_dir_all(&work)?;
    Ok(())
}
