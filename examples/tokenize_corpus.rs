//! Measures the string layer on a directory of JSONL logs (the corpus harness for milestone 1).
//!
//! Usage: `cargo run --release --example tokenize_corpus -- <dir> [step] [zstd level] [dictionary MB]`
//!
//! Every `step`-th `*.jsonl` file is read. Each string field of a line (and the `data` object, re-serialised as
//! compact JSON) is tokenized into one shared word store. The report estimates the stored size as
//! `zstd(row heap) + zstd(heap) + 8-byte cells`, the layout planned for the first file format.

use adaptif_gatotkaca_logsquash::{
    DefaultSplitter, Dictionary, Error, JsonEscapeSplitter, MemHeap, Splitter, StreamStats, Tokenizer, TokenizerOptions, WordStore, analyze,
};
use bytes::BytesMut;
use serde_json::Value;
use std::collections::HashSet;
use std::path::PathBuf;
use std::time::Instant;

const COLUMNS: u64 = 12;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    let dir = PathBuf::from(args.get(1).ok_or("usage: tokenize_corpus <dir> [step] [zstd level] [dictionary MB]")?);
    let step: usize = args.get(2).map_or(Ok(10), |s| s.parse())?;
    let level: i32 = args.get(3).map_or(Ok(19), |s| s.parse())?;
    let dictionary_mb: usize = args.get(4).map_or(Ok(64), |s| s.parse())?;

    let mut files: Vec<PathBuf> = std::fs::read_dir(&dir)?.filter_map(|e| e.ok().map(|e| e.path())).collect();
    files.retain(|p| p.extension().is_some_and(|e| e == "jsonl"));
    files.sort();
    let files: Vec<PathBuf> = files.into_iter().step_by(step).collect();

    let words = WordStore::new(MemHeap::new(), Dictionary::new(dictionary_mb << 20)).into_shared();
    let tokenizer = Tokenizer::new(words.clone(), TokenizerOptions::default());
    let (plain, json): (&dyn Splitter, &dyn Splitter) = (&DefaultSplitter, &JsonEscapeSplitter);

    let started = Instant::now();
    let (mut raw_bytes, mut rows, mut strings, mut string_bytes) = (0u64, 0u64, 0u64, 0u64);
    let (mut stream_bytes, mut unique_stream_bytes) = (0u64, 0u64);
    let mut row_heap: Vec<u8> = Vec::new();
    let mut seen: HashSet<Vec<u8>> = HashSet::new();
    let mut scratch = BytesMut::new();
    let mut full = false;
    let mut composition = StreamStats::default();

    'files: for path in &files {
        for line in std::fs::read_to_string(path)?.lines() {
            let value: Value = serde_json::from_str(line)?;
            let Some(object) = value.as_object() else { continue };
            let mut row_streams: Vec<(Vec<u8>, usize)> = Vec::new();
            for (key, field) in object {
                let (text, splitter) = match field {
                    Value::String(s) => (s.clone(), plain),
                    Value::Null => continue,
                    other if key == "data" => (serde_json::to_string(other)?, json),
                    _ => continue,
                };
                scratch.clear();
                match tokenizer.encode(&text, splitter, &mut scratch) {
                    Ok(()) => row_streams.push((scratch.to_vec(), text.len())),
                    Err(Error::DictionaryFull) => {
                        full = true;
                        break 'files;
                    }
                    Err(e) => return Err(e.into()),
                }
            }
            rows += 1;
            raw_bytes += line.len() as u64 + 1;
            for (stream, text_len) in row_streams {
                strings += 1;
                string_bytes += text_len as u64;
                stream_bytes += stream.len() as u64;
                if !seen.contains(&stream) {
                    let c = analyze(&stream)?;
                    composition.words += c.words;
                    composition.chars += c.chars;
                    composition.opaque_runs += c.opaque_runs;
                    composition.opaque_bytes += c.opaque_bytes;
                    composition.decoded_runs += c.decoded_runs;
                    composition.decoded_bytes += c.decoded_bytes;
                    unique_stream_bytes += stream.len() as u64;
                    row_heap.extend_from_slice(&stream);
                    seen.insert(stream);
                }
            }
        }
    }
    let elapsed = started.elapsed();

    let store = words.read().unwrap();
    let heap_bytes = store.heap().as_bytes();
    let z_row_heap = zstd::encode_all(&row_heap[..], level)?.len() as u64;
    let z_heap = zstd::encode_all(heap_bytes, level)?.len() as u64;
    let cells = rows * COLUMNS * 8;
    let mb = |n: u64| n as f64 / 1e6;
    let pct = |n: u64| 100.0 * n as f64 / raw_bytes as f64;
    let plain_total = unique_stream_bytes + heap_bytes.len() as u64 + cells;

    println!("files read            {} (every {step}th){}", files.len(), if full { ", stopped: dictionary full" } else { "" });
    println!("rows                  {rows}");
    println!("raw                   {:8.2} MB", mb(raw_bytes));
    println!("strings               {strings} ({:.2} MB of text)", mb(string_bytes));
    println!("words in heap         {} ({:.2} MB)", store.word_count(), mb(heap_bytes.len() as u64));
    println!("dictionary            {:.2} MB estimated", store.dictionary().estimated_bytes() as f64 / 1e6);
    println!(
        "symbol streams        {:8.2} MB, {:.2} MB after whole-string dedup ({} unique)",
        mb(stream_bytes),
        mb(unique_stream_bytes),
        seen.len()
    );
    println!(
        "  of which            {} word symbols, {} chars, {} opaque runs ({:.2} MB), {} decoded runs ({:.2} MB)",
        composition.words,
        composition.chars,
        composition.opaque_runs,
        mb(composition.opaque_bytes),
        composition.decoded_runs,
        mb(composition.decoded_bytes)
    );
    println!("8-byte cells ({COLUMNS} cols) {:7.2} MB", mb(cells));
    println!("--- estimated stored size ---");
    println!("without zstd          {:8.2} MB  {:5.1}%", mb(plain_total), pct(plain_total));
    println!("row heap  zstd-{level}     {:8.2} MB", mb(z_row_heap));
    println!("heap      zstd-{level}     {:8.2} MB", mb(z_heap));
    println!("total (cells raw)     {:8.2} MB  {:5.1}%", mb(z_row_heap + z_heap + cells), pct(z_row_heap + z_heap + cells));
    println!("tokenize time         {:.1}s", elapsed.as_secs_f64());
    Ok(())
}
