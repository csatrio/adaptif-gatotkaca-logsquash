# adaptif-gatotkaca-logsquash

[![crates.io](https://img.shields.io/crates/v/adaptif-gatotkaca-logsquash.svg)](https://crates.io/crates/adaptif-gatotkaca-logsquash)
[![docs.rs](https://docs.rs/adaptif-gatotkaca-logsquash/badge.svg)](https://docs.rs/adaptif-gatotkaca-logsquash)
![MSRV 1.85](https://img.shields.io/badge/MSRV-1.85-blue)
[![license](https://img.shields.io/crates/l/adaptif-gatotkaca-logsquash.svg)](#license)

Developed and maintained by **[PT Adaptif Sinergi Dinamis](https://adaptif.co.id/)**.

**A log squasher: logs in compact binary dataframes that are still easy to search and read back.**

Logs repeat themselves. The same words, ids, paths and base64 blobs appear millions of times. This crate stores each
distinct word once, in a heap shared by every dataframe of a folder, and writes each log line as a short stream of
varint symbols. Columns are laid out in compressed blocks with zstd, and optional on-disk indexes make word, phrase,
equality and range searches fast without reading the logs.

Priorities, in order: **(1) small files**, **(2) easy rehydration and search**, for example from a web viewer. Insert
speed is not a goal. Real Claude Code transcripts (775.5 MB of JSON lines) end up at **25.5% of their raw size with every
search index included**; see [Benchmarks](#benchmarks).

This is a library. A server or web app calls it; neither is part of this crate.

## Features

- **Word-level string dedup.** Text is split into words (runs of letters and digits, every other character on its own).
  Each distinct word is stored once in a heap; cells hold ordinals. The split is lossless: every string, including
  invalid-looking JSON and any Unicode, comes back byte for byte.
- **Compact tagged varint symbols.** A word ordinal, a single character, or an inline run costs one or two bytes.
- **Inline runs.** Base64 (four variants), UUIDs (lower and upper), hex strings and long high-entropy ids are stored as
  decoded bytes instead of polluting the dictionary. This alone is worth about 10 points of size on real logs.
- **Typed columns.** `I8..I64`, `U8..U64`, `F32`, `F64`, `Bool`, `Char`, `String`, `Blob`, and date/time types:
  `TimestampSec/Ms/Us/Ns`, `Date`, `TimeOfDay`, `TimestampTz` (millis plus UTC offset). Fixed-size values use
  order-preserving `u64` encodings, so range search and sorting work on the encoded form.
- **Date and time text.** Text is accepted on insert and in filters. Formats: ISO-8601/RFC-3339 (default), RFC-2822,
  Common Log Format, or any `strftime:<pattern>`. Text with no offset is read as UTC. Invalid or over-precise input is an
  error, never silently rounded.
- **Column-major compressed segments.** Blocks of 4,096 rows, byte-plane transposition plus zstd (level 19 by default),
  per-block min/max and non-null counts, per-block xxh3 checksums.
- **A folder is a store.** One folder covers a day or any period. Its dataframes share one word heap and one
  dictionary (64 MiB limit; start a new folder when full). Sealing the folder at rotation drops the dictionary; it can
  be rebuilt from the heap.
- **Crash-safe commits.** Heap blocks, then segments, then the manifest, which is replaced by an atomic rename. A crash
  loses at most the uncommitted rows.
- **Search.** Word, case-insensitive word (full Unicode case folding), phrase, equality and range filters, AND-combined.
  `search` (lazy row ids), `count`, and `search_page` (sort by a fixed-size column, offset, limit).
- **Persisted, optional indexes.** Word, case-folded word and equality indexes with delta-varint postings, compressed or
  memory-mapped. Switch indexing off while writing and build the indexes later from the stored data with
  `Store::build_indexes`. Scans and indexed paths return identical results.
- **Word table.** At seal, a sorted word table gives `lookup_word` and `words_with_prefix` for search suggestions.
- **Hydration.** `row`, `value`, `rows()` iterator, and `write_string` into a reusable `BytesMut` for few allocations.
- **Many readers, one writer.** One writer per folder (a lock file, plus a mutex serializing `push_row`, `commit` and
  `create_frame`). Readers take no lock.
- **Integrity.** `Store::verify` checks every checksum; corrupt data returns `Error::Corrupt`, never a panic.
- **Safe Rust.** `#![deny(unsafe_code)]`, with a single `unsafe` block in `filebytes.rs` for memory mapping (feature
  `mmap`).

## Quick start

```toml
[dependencies]
adaptif-gatotkaca-logsquash = "0.1"
```

```rust
use adaptif_gatotkaca_logsquash::{Column, ColumnType, OrderBy, OwnedValue, Page, Query, Schema, Store, StoreOptions, Value};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = std::env::temp_dir().join("logsquash-quickstart");

    let schema = Schema::new(vec![
        Column::new("ts", ColumnType::TimestampMs),
        Column::new("level", ColumnType::String).index_equality(),
        Column::new("message", ColumnType::String).index_words().case_insensitive(),
    ])?;

    // Write: one writer per folder.
    let store = Store::create(&dir, StoreOptions::default())?;
    let logs = store.create_frame("logs", schema)?;
    for i in 0..1000u64 {
        let message = format!("request {i} failed: connection refused by upstream");
        logs.push_row(&[
            Value::TimestampMs(1_760_000_000_000 + i as i64 * 1000),
            Value::Str(if i % 10 == 0 { "ERROR" } else { "INFO" }),
            Value::Str(&message),
        ])?;
        store.commit_if_full()?; // commits when the buffer reaches `segment_rows`
    }
    store.commit()?;
    store.seal()?; // end of the period: dictionary dropped, indexes finished
    drop(store);

    // Read: any number of readers, no lock.
    let store = Store::open_read_only(&dir, StoreOptions::default())?;
    let logs = store.frame("logs")?;
    let query = Query::new().equals(1, Value::Str("ERROR")).word_ci(2, "REFUSED");
    println!("{} matches", logs.count(&query)?);
    let order = OrderBy { column: 0, descending: true };
    let page = logs.search_page(&query, &Page { order_by: Some(order), offset: 0, limit: 5 })?;
    for row in page.rows {
        if let OwnedValue::Str(message) = &logs.row(row)?[2] {
            println!("{row}: {message}");
        }
    }
    Ok(())
}
```

This is [`examples/quickstart.rs`](examples/quickstart.rs); run it with `cargo run --release --example quickstart`.
The 1,000 rows above take about 25 KB on disk.

## Concepts

| Term | Meaning |
|---|---|
| **Store** | A folder: `manifest`, `heap.bin`, segment files `f<id>-<n>.seg`, index files `.idx`, `words.idx`, `lock`. |
| **Frame** | A named dataframe in a store (a table with a fixed schema). Many frames share the store's heap. |
| **Segment** | The rows of one frame written by one commit, stored column by column in compressed blocks. |
| **Heap** | Append-only list of the distinct multi-character words. A word's position is its ordinal. |
| **Dictionary** | In-memory map from a word to its ordinal, used by the writer. Limited to 64 MiB; rebuilt from the heap. |
| **Row** | A global number (0-based, across all segments of a frame, including uncommitted rows). |

Typical lifecycle of a folder: `create` → `create_frame` → many `push_row` and `commit_if_full` → `commit` → `seal`
at rotation. After that, open it read-only for as long as you keep the logs.

## Schemas and columns

```rust
use adaptif_gatotkaca_logsquash::{Column, ColumnType, Schema};

let schema = Schema::new(vec![
    Column::new("ts", ColumnType::TimestampTz),             // text such as 2026-11-21T10:00:00+07:00
    Column::new("status", ColumnType::U16),
    Column::new("path", ColumnType::String).index_words(),
    Column::new("agent", ColumnType::String).index_equality(),
    Column::new("body", ColumnType::String).json_escapes(), // JSON text: splits escapes (\n, \", é) as units
])?;
```

| Option | Effect |
|---|---|
| `index_words()` | Word and phrase index on a string column. |
| `case_insensitive()` | Also a case-folded word index, for `word_ci`. |
| `index_equality()` | Equality index on the whole value. |
| `json_escapes()` | The column holds JSON text; escapes are split as units for better dedup. Still lossless. |
| `Column::text_format` | Text format of a date/time column: empty or `builtin:iso8601`, `builtin:rfc2822`, `builtin:clf`, `strftime:<pattern>`. |

Indexes are only written for flagged columns and only when `StoreOptions::indexing` is on. Range and sort work on every
fixed-size column without an index, using per-block min and max to skip blocks.

## Searching

```rust
let q = Query::new()
    .word(2, "timeout")                               // word in a string column
    .word_ci(2, "Upstream")                           // case-insensitive (needs case_insensitive())
    .phrase(2, "connection refused")                  // words next to each other, in order
    .equals(1, Value::Str("ERROR"))                   // equality, also on numbers, dates and null
    .range(0, Some(Value::Str("2026-11-21T00:00:00Z")), None); // text accepted for date/time columns

let n = frame.count(&q)?;                             // number of matches
for row in frame.search(&q)? { let row = row?; /* lazy row ids */ }
let page = frame.search_page(&q, &Page { order_by: None, offset: 100, limit: 50 })?; // page.total, page.rows
```

Filters are AND-combined; an empty query matches every row. Search works on symbols: a UUID or a long base64/hex run
is not a word. Find those with `equals`. Words are runs of letters and digits, so `connection_refused` is two words.
`Store::words_with_prefix(prefix, limit)` and `lookup_word` serve autocomplete from the sealed word table.

## Hydration

```rust
let row: Vec<OwnedValue> = frame.row(42)?;            // one row
let cell: OwnedValue = frame.value(42, 2)?;           // one cell
for r in frame.rows() { let r = r?; /* sequential scan, decodes block by block */ }

let mut buf = bytes::BytesMut::new();
frame.write_string(42, 2, &mut buf)?;                 // append the text to a reusable buffer; false if null
```

## Write options

```rust
let options = StoreOptions {
    row_heap_block_bytes: 4 << 20, // 24.0% instead of 25.5%, random row 1.9 ms instead of 0.85 ms
    indexing: false,               // write fast now, call Store::build_indexes(path, options) later
    ..StoreOptions::default()
};
```

| Option | Default | Meaning |
|---|---|---|
| `zstd_level` | 19 | Level of every block. 22 gains nothing measurable. |
| `segment_rows` | 100,000 | Rows at which `commit_if_full` commits. |
| `block_rows` | 4,096 | Rows per column block. |
| `row_heap_block_bytes` | 1 MiB | Larger blocks are smaller on disk and slower for a random row. |
| `heap_block_bytes` | 1 MiB | Size of a heap block. |
| `dictionary_bytes` | 64 MiB | Dictionary limit; `Error::DictionaryFull` or `HeapFull` means: seal and start a new folder. |
| `dedup_entries` | 4,000,000 | Whole-string dedup map of the buffer, reset on commit. |
| `cache_bytes` | 32 MiB | Decoded blocks each segment reader keeps. |
| `indexing` | true | Write index files on commit. |
| `compress_indexes` | true | zstd-compress index files; off means memory-mapped. |
| `tokenizer` | see docs | Thresholds of inline-run detection. |

A store keeps at most 64 segment files open (least recently used first out; a closed file is reopened on its next
read). Change it on an open store with `store.set_max_open_segments(n)`; `store.open_segment_files()` reports how many
are open. It is a `Store` method rather than a `StoreOptions` field so that 0.1.x stays compatible with code that
builds `StoreOptions` with every field listed.

## Feature flags

| Feature | Default | Effect |
|---|---|---|
| `mmap` | on | Memory-map uncompressed index files (`memmap2`). Without it they are read into memory. |
| `json` | off | Optional `serde` and `serde_json` dependencies, reserved for JSON helpers; no effect on the API yet. |

## Benchmarks

Real data: 831,523 rows from 775.5 MB of Claude Code transcripts (JSON lines), all indexes on, zstd level 19.
Every row was read back and compared with the original: 0 mismatches.

| Configuration | Size of raw | Random row read | Sequential read |
|---|---|---|---|
| 64 KiB row-heap blocks (old default) | 28.28% | 0.36 ms | 162,000 rows/s |
| **1 MiB row-heap blocks (default)** | **25.48%** | 0.85 ms | 170,000 rows/s |
| 4 MiB row-heap blocks | 24.00% | 1.9 ms | 172,000 rows/s |

On disk at the default: segments 172.2 MB, indexes 24.1 MB, heap 0.7 MB. Without inline-run detection the same corpus is
38.0% (base64 decoding alone brings it from 38.0% to 34.9%).

Queries on the whole corpus, indexed versus a scan of the same data without indexes:

| Query | Indexed | Scan |
|---|---|---|
| word `error` in `text` | 0.1 ms | 733 ms |
| case-insensitive word | 0.1 ms | 1.6 s |
| word in `data` matching 654,821 rows | 3.3 ms | 1.1 s |
| `type = assistant` | 0.3 ms | 91 ms |
| `type` + `subtype` + word `error` | 0.6 ms | 684 ms |
| phrase `let me` | 75 ms | 730 ms |
| page of 50 sorted rows out of 77,406 matches | 14 ms | |
| time range, 1,247 matches | 0.1 ms | 0.1 ms |
| prefix suggestions | 0.6 ms | |

Opening a store read-only takes 80–90 ms on the whole corpus. A single-character word such as `:` cannot use an
index usefully and takes about 0.7 s.

Micro-benchmarks (`cargo bench --bench store`, synthetic rows, warm cache, Apple Silicon; relative numbers):

| Operation | Time |
|---|---|
| tokenize 1,000 JSON strings | 0.98 ms (116 MiB/s) |
| `push_row` × 1,000 in memory | 2.0 ms |
| commit 20,000 rows with indexes | 73 ms |
| `build_indexes`, 4 segments of 5,000 rows | 75 ms |
| `open_read_only` (100,000 rows) | 3.6 ms |
| indexed word search | about 70 µs |
| indexed equality | about 160 µs |
| page of 50 sorted by id | 0.78 ms |

Tuning experiments that did not help (zstd level 22, a trained zstd dictionary, other block and segment sizes, other
entropy thresholds) are in [DESIGN.md](DESIGN.md), section 13. Reproduce with
`cargo run --release --example tune_corpus -- <dir of .jsonl> 1 <work folder> -` (see the example's header for settings).

## Limits and semantics

- One writer per folder. A second writer fails with `Error::Locked`.
- The dictionary is bounded (64 MiB, about 2 million words). When full, `push_row` fails with `Error::DictionaryFull`
  and no row is half-written: seal this folder and create a new one.
- Search is on symbols, not substrings. Use `word`, `phrase` and `equals`; there is no substring or regex search.
- Index files are not noticed by stores that are already open; reopen after `build_indexes`.
- Open segment files are bounded per store (64 by default, see `Store::set_max_open_segments`), so a long-lived store
  that commits often does not run out of file descriptors. Segment files must stay in place while a store uses them:
  a reader whose file was closed and then deleted or replaced gets `Error::Io` on its next read.
- The format is versioned; a folder written with another format or splitter version fails with
  `Error::UnsupportedVersion`.
- Text types are validated: out-of-range numbers, bad dates and over-precise times are errors.

## MSRV

The minimum supported Rust version is **1.85** (edition 2024). Raising the MSRV counts as a minor version bump.

## Design

[DESIGN.md](DESIGN.md) holds the full design, the decision log and the measurements. [AGENTS.md](AGENTS.md) is a
practical guide for coding agents using this crate.

## About

`adaptif-gatotkaca-logsquash` is developed and maintained by
[PT Adaptif Sinergi Dinamis](https://adaptif.co.id/). Source code, issues and
contributions: <https://github.com/csatrio/adaptif-gatotkaca-logsquash>.

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or <https://www.apache.org/licenses/LICENSE-2.0>)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or <https://opensource.org/licenses/MIT>)

at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in this work by you, as defined in the Apache-2.0 license, shall
be dual licensed as above, without any additional terms or conditions.

Copyright © 2026 [PT Adaptif Sinergi Dinamis](https://adaptif.co.id/).
