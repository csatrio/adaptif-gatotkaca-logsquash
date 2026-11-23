# AGENTS.md: using `adaptif-gatotkaca-logsquash`

Instructions for coding agents (and humans) writing code that stores or searches logs with this crate. The crate name
is `adaptif-gatotkaca-logsquash`; the Rust import is `adaptif_gatotkaca_logsquash`; the main types are `Store`, `Frame`,
`Schema`, `Query`. It is a library: it has no server, CLI or web UI. Run `cargo run --release --example quickstart` to
see the whole flow.

## 1. Mental model

- A **Store** is a folder for one day or period. It holds one or more **Frames** (tables with a fixed schema) that
  share one word heap, which is what makes it small.
- You **write** with one writer per folder: `push_row` into the frame, then `commit` (or `commit_if_full`). At rotation
  call `seal`. A sealed folder is immutable and is opened with `open_read_only`.
- You **read** with any number of `open_read_only` stores at once, no lock. Reading works on a store that is still
  being written too, but only sees committed rows (plus, in the writer's own process, the pending ones).
- Rows are numbered globally per frame, from 0. `search` gives row numbers; `row(n)` gives the values.

## 2. Golden rules

1. **One writer per folder.** A second `Store::create`/`open` for writing fails with `Error::Locked`. Share one
   `Arc<Store>`/`Arc<Frame>` between threads instead; `push_row` and `commit` are serialized for you, and searches and
   reads may run on other threads at the same time (use 0.1.1 or later: 0.1.0 could deadlock a search of uncommitted
   rows against a concurrent `push_row`).
2. **Always `commit` before you stop, and `seal` at the end of the period.** Rows that are not committed are lost on
   exit. `seal` finishes the indexes and the word table and closes the store for writing.
3. **Call `commit_if_full()` regularly while writing** (for example every 1,000 rows). It commits at `segment_rows`
   (100,000). Never commit after every row: each commit writes a segment and tiny segments compress badly.
4. **Flag the columns you will search, at schema time.** `index_words()` for word/phrase, `case_insensitive()` for
   `word_ci`, `index_equality()` for `equals`. Without a flag the search still works but scans the column.
5. **Put separate log sources into separate columns**, one wide frame is fine; nulls cost almost nothing. Use real types
   (`TimestampMs`, `U16`, ...) for timestamps and numbers, not strings: they compress better and support range and sort.
6. **Search is by word, phrase, equality and range, not substring.** Words are runs of letters and digits; every other
   character is a separate symbol. Find ids, UUIDs and long base64/hex values with `equals`, not `word`.
7. **Handle `Error::DictionaryFull` and `Error::HeapFull`:** the row was not stored; `commit`, `seal`, create a new folder
   (for example `<root>/2026-11-21-b`) and retry the row there.
8. **Do not mix `Value` types with the schema.** `push_row` takes one `Value` per column in column order and fails with
   `Error::TypeMismatch`, `ColumnCount`, `ValueOutOfRange` otherwise. It is atomic: a failed row stores nothing.
9. **Do not delete or edit files in the folder by hand.** Commit points are atomic renames of `manifest`.

## 3. Recipes

### Write logs, rotating daily

```rust
use adaptif_gatotkaca_logsquash::{Column, ColumnType, Error, Schema, Store, StoreOptions, Value};

let schema = Schema::new(vec![
    Column::new("ts", ColumnType::TimestampMs),
    Column::new("level", ColumnType::String).index_equality(),
    Column::new("line", ColumnType::String).index_words().case_insensitive(),
])?;
let store = Store::create(root.join("2026-11-21"), StoreOptions::default())?;
let logs = store.create_frame("logs", schema.clone())?;

for (ts, level, line) in incoming {
    match logs.push_row(&[Value::TimestampMs(ts), Value::Str(&level), Value::Str(&line)]) {
        Ok(_) => {}
        Err(Error::DictionaryFull | Error::HeapFull) => { /* seal, open the next folder, retry the row */ }
        Err(e) => return Err(e.into()),
    }
    store.commit_if_full()?;
}
store.commit()?;
store.seal()?; // at midnight
```

To reopen a folder for more writing (same day, after a restart): `Store::open(path, options)`, then
`store.frame("logs")`. The dictionary is rebuilt from the heap.

### Serve a viewer (read-only)

Open once, keep the `Arc<Store>` in your server state, clone the `Arc<Frame>` per request.

```rust
let store = Store::open_read_only(path, StoreOptions::default())?;
let logs = store.frame("logs")?;

// Search page: newest first, 50 per page, with the total for a pager.
let q = Query::new().equals(1, Value::Str("ERROR")).word_ci(2, "timeout");
let page = logs.search_page(&q, &Page { order_by: Some(OrderBy { column: 0, descending: true }), offset: 0, limit: 50 })?;
for n in &page.rows { let row = logs.row(*n)?; /* render */ }
// page.total is the number of matches in all.
```

- Calls are blocking and CPU/IO bound: from an async server run them in `spawn_blocking`.
- `Frame::row` costs about 1 ms cold (one 1 MiB row-heap block decompress), much less when the block is cached.
  Fetch only the rows of the page you show.
- `Frame::count(&q)` is cheaper than iterating `search`.
- Autocomplete: `store.words_with_prefix("tim", 10)` (needs a sealed store).
- Stream a long text without allocating each time: reuse a `bytes::BytesMut` with `frame.write_string(row, col, &mut buf)`.
- Time ranges: `Query::range(col, Some(Value::Str("2026-11-21T00:00:00Z")), Some(Value::Str("2026-11-21T01:00:00Z")))`.
  Text is parsed with the column's `text_format`; set `column.text_format = "builtin:clf".into()` for nginx time stamps or
  `"strftime:%d/%b/%Y:%H:%M:%S %z"` for a custom pattern.

### Indexes later

Write fast with `indexing: false`, then build indexes in the background:

```rust
let options = StoreOptions { indexing: false, ..StoreOptions::default() };
// ... write, commit, seal ...
let report = Store::build_indexes(path, StoreOptions::default())?; // needs the writer lock: not while a writer is open
```

Stores that were already open do not see the new indexes: reopen them.

### Check a folder

`Store::open_read_only(path, opts)?.verify()?` re-reads every block and checks the checksums. `disk_bytes()` is the
folder size.

## 4. Choosing options

| Goal | Setting |
|---|---|
| Smallest files | `row_heap_block_bytes: 4 << 20` (24.0% vs 25.5% of raw; random row read 1.9 ms vs 0.85 ms) |
| Fastest random row reads | `row_heap_block_bytes: 64 << 10` (28.3% of raw, 0.36 ms) |
| Fast ingest | `indexing: false`, `zstd_level: 3..9` (larger files), build indexes afterwards |
| Less memory per reader | lower `cache_bytes` (32 MiB per segment by default) |
| Index files read with mmap | `compress_indexes: false` (bigger index files, no decompress on open) |

Leave `block_rows`, `segment_rows`, `zstd_level 19` and the tokenizer thresholds alone: sweeps on 775 MB of real logs
showed no useful gain. Details: DESIGN.md section 13.

## 5. Column types

| Rust `Value` | `ColumnType` | Notes |
|---|---|---|
| `I8/I16/I32/I64`, `U8/U16/U32/U64`, `F32/F64` | same names | Floats are stored bit-exact. |
| `Bool`, `Char` | `Bool`, `Char` | |
| `TimestampSec/Ms/Us/Ns(i64)` | same | UTC instants. |
| `Date`, `TimeOfDay` | `Date`, `TimeOfDay` | |
| `TimestampTz { millis, offset_minutes }` | `TimestampTz` | Keeps the original UTC offset. |
| `Str(&str)` | `String` | Deduplicated words. Add `json_escapes()` for JSON text. |
| `Blob(&[u8])` | `Blob` | Stored raw in the row heap; not word-indexed. |
| `Null` | any | Allowed in every column. |

`Str` is also accepted for date/time columns and parsed (see `Column::text_format`).

## 6. Mapping common log shapes

- **JSON lines:** parse each line; one column per top-level key (nulls for absent keys); nested objects/arrays as `String`
  with `json_escapes()`. `examples/tune_corpus.rs` shows a loader with automatic schema inference.
- **nginx / Apache access:** `ts` as `TimestampTz` with `builtin:clf`, `status` `U16`, `bytes` `U64`, `method` and `path`
  `String`, `agent` `String`.
- **syslog / app text logs:** `ts` `TimestampMs`, `host` and `level` with `index_equality()`, `message` with
  `index_words().case_insensitive()`.
- **One wide frame** with a `kind` column is better than many frames when the sources are similar; separate frames are
  fine too (they still share the heap).

## 7. Errors

`Error` variants worth handling: `Locked` (another writer), `DictionaryFull`/`HeapFull` (rotate), `Corrupt`
(bad checksum or truncated file: report the folder, do not retry), `UnsupportedVersion`, `TypeMismatch`,
`ColumnCount`, `ValueOutOfRange`, `DateParse { column, reason }`, `RowOutOfRange`, `InvalidQuery`, `ReadOnly`
(writing to a read-only store), `UnknownFrame`, `FrameExists`, `Io`.

## 8. Testing code that uses the crate

Use `tempfile::tempdir()` for the folder, `StoreOptions { zstd_level: 3, ..Default::default() }` to be fast, and compare a
search result against a plain filter over the rows you inserted. The crate's own tests do the same: 97 tests including
`proptest` round trips.
