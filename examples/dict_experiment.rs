//! Experiment for milestone 6: how much would a trained zstd dictionary help the small row-heap blocks?
//!
//! Usage: `cargo run --release --example dict_experiment -- <dir> <step> [level]`
//!
//! Builds the row heap of the logs (as the store does), cuts it into blocks of several sizes at entry boundaries and
//! compares: independent blocks, blocks compressed with a dictionary trained on other blocks, and one big block.

use adaptif_gatotkaca_logsquash::{
    DefaultSplitter, Dictionary, JsonEscapeSplitter, MemHeap, Splitter, Tokenizer, TokenizerOptions, WordStore,
};
use bytes::BytesMut;
use serde_json::Value;
use std::collections::HashSet;
use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    let dir = PathBuf::from(args.get(1).ok_or("usage: dict_experiment <dir> <step> [level]")?);
    let step: usize = args.get(2).map_or(Ok(10), |s| s.parse())?;
    let level: i32 = args.get(3).map_or(Ok(19), |s| s.parse())?;
    let mut files: Vec<PathBuf> = std::fs::read_dir(&dir)?.filter_map(|e| e.ok().map(|e| e.path())).collect();
    files.retain(|p| p.extension().is_some_and(|e| e == "jsonl"));
    files.sort();

    let words = WordStore::<MemHeap>::new(MemHeap::new(), Dictionary::new(64 << 20)).into_shared();
    let tokenizer = Tokenizer::new(words, TokenizerOptions::default());
    let (plain, json): (&dyn Splitter, &dyn Splitter) = (&DefaultSplitter, &JsonEscapeSplitter);
    let mut entries: Vec<Vec<u8>> = Vec::new();
    let mut seen: HashSet<Vec<u8>> = HashSet::new();
    let mut scratch = BytesMut::new();
    for path in files.iter().step_by(step) {
        for line in std::fs::read_to_string(path)?.lines() {
            let Value::Object(object) = serde_json::from_str(line)? else { continue };
            for (key, field) in &object {
                let (text, splitter) = match field {
                    Value::String(s) => (s.clone(), plain),
                    Value::Object(_) | Value::Array(_) => (serde_json::to_string(field)?, json),
                    _ => continue,
                };
                scratch.clear();
                tokenizer.encode(&text, splitter, &mut scratch)?;
                if seen.insert(scratch.to_vec()) {
                    entries.push(scratch.to_vec());
                }
                let _ = key;
            }
        }
    }
    let total: usize = entries.iter().map(Vec::len).sum();
    println!("{} unique entries, {:.2} MB row heap, zstd level {level}", entries.len(), total as f64 / 1e6);
    println!("{:<12} {:>9} {:>14} {:>14} {:>14} {:>14}", "block size", "blocks", "independent", "dict 32K", "dict 128K", "dict 512K");

    for block in [16 << 10, 64 << 10, 256 << 10, 1 << 20, 4 << 20] {
        let mut blocks: Vec<Vec<u8>> = Vec::new();
        let mut current: Vec<u8> = Vec::new();
        for entry in &entries {
            if !current.is_empty() && current.len() >= block {
                blocks.push(std::mem::take(&mut current));
            }
            current.extend_from_slice(entry);
        }
        if !current.is_empty() {
            blocks.push(current);
        }
        let independent: usize = blocks.iter().map(|b| zstd::bulk::compress(b, level).map(|c| c.len())).sum::<Result<_, _>>()?;
        let mut cells = Vec::new();
        for dict_size in [32 << 10, 128 << 10, 512 << 10] {
            // train on every other block, measure on all blocks (the dictionary is stored once per folder)
            let samples: Vec<&[u8]> = blocks.iter().step_by(2).map(Vec::as_slice).collect();
            let size = match zstd::dict::from_samples(&samples, dict_size) {
                Ok(dict) => {
                    let mut compressor = zstd::bulk::Compressor::with_dictionary(level, &dict)?;
                    let packed: usize = blocks.iter().map(|b| compressor.compress(b).map(|c| c.len())).sum::<Result<_, _>>()?;
                    format!("{:.2} MB", (packed + dict.len()) as f64 / 1e6)
                }
                Err(e) => format!("n/a ({e})"),
            };
            cells.push(size);
        }
        println!(
            "{:<12} {:>9} {:>11.2} MB {:>14} {:>14} {:>14}",
            format!("{} KiB", block >> 10),
            blocks.len(),
            independent as f64 / 1e6,
            cells[0],
            cells[1],
            cells[2]
        );
    }
    Ok(())
}
