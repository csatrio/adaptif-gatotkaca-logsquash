# Log Squasher

A library that stores logs in a compact binary dataframe. Strings are compressed by word-level deduplication, compact symbol encoding and block compression, and the stored logs can be searched and hydrated easily, for viewing in a web app.

## 1. Scope and Principles
- **Library only** (Q85). The web app reads data through a Rust server that calls the HTTP/JSON API (Q81); the server and the web app are **separate projects** that use this library. This repo provides the library API, including an optional JSON output feature for them. No CLI and no log-file ingestion format; callers build rows through the API. Date/time values can also be parsed from text (6.2).
- **Priorities, in order:** (1) file size (target <= 30% of the raw text); (2) ease of rehydration, search and viewing. Insert throughput is not a goal.
- **Compression pipeline:** word-level string deduplication -> compact varint symbols -> zstd on every block.
- **Hash:** xxh3-64. On a hash hit the stored bytes are compared with the candidate; if they differ, the word is re-hashed with a salt (see 5.3).
- **Lossless.** Hydrating a row returns exactly the values that were inserted.

## 2. Glossary
| Term | Meaning |
|------|---------|
| Word | A unit produced by the splitter (4.1), also called a token in logs. |
| Ordinal | The index of a unique multi-character word in the heap, in first-seen order. |
| Symbol | One varint in a string's symbol stream (4.3): a word ordinal, a single character, or an inline run. |
| Heap | Per-folder, append-only store of unique words, in ordinal order (5.1). |
| Dictionary | In-memory map from `hash(word, salt)` to ordinal, persisted across sessions (5.2). |
| Row heap | Per-segment, append-only store of symbol streams and blobs (6.4). |
| Segment | One file of up to 100,000 rows of a dataframe (6.3). |
| Block | A compressed unit inside a file (a run of heap words, 4096 rows of one column, or about 1 MiB of row heap). |
| Folder / generation | A store directory holding a manifest, the heap/dictionary and dataframe segments. A new one starts when the dictionary is full (7). |

## 3. Components
```
 &str ──► Splitter ──► words / runs ──► Tokenizer ──► Symbol stream ──► Row heap (per segment)
                                          │  ▲
                                          ▼  │
                                  Dictionary ──► Heap   (shared by all segments of a folder)
```
The Heap and Dictionary are shared by all dataframe segments of one folder and need locking for multithreading.

## 4. Tokenizer

### 4.1 Word splitting
An input string (`&str`, valid UTF-8) is split into words:
- a maximal run of alphanumeric characters (Unicode `char::is_alphanumeric`) is one word;
- every other character (space, comma, punctuation, control character, line ending, ...) is its own single-character word.

**Maximum word length:** an alphanumeric run longer than 64 characters (and not stored inline, see 4.2) is split into consecutive 64-character chunks, each its own word.

**Escape-aware splitter (per column):** a column may be flagged `json_escapes`. In such a column a backslash plus the next character (`\n`, `\"`, `\\`, `\t`, ...) is one two-character word, and `\u` plus four hex digits is one six-character word. This stops JSON escapes from gluing onto the following word (`\nfoo` would otherwise become `\` + `nfoo`). The default splitter (without the flag) keeps the plain rules, so Windows paths such as `C:\Users` are not broken up.

Concatenating all words in order reproduces the original string exactly. Splitting is behind a `Splitter` trait; the default and escape-aware splitters are built in, and callers may supply their own (it must be lossless). The splitter id and parameters are recorded in the folder's manifest, and opening a folder with a different splitter is an error.

### 4.2 Inline runs (UUID, hex, base64, opaque)
Some runs are never worth a dictionary entry because each occurs once. The tokenizer scans left to right and, before splitting words, checks for these runs. They are stored inline in the symbol stream and skip the dictionary. Checks are made in this order:
1. **UUID:** a hyphenated 8-4-4-4-12 hex group of 36 characters, bounded by non-alphanumeric characters (or the string ends), with all letters in one case. Stored as 16 bytes.
2. **Base64 run:** a maximal run of at least 64 characters of the base64 alphabet (standard `+/` or URL-safe `-_`, optional `=` padding) whose Shannon entropy is at least 4.5 bits per character, that decodes successfully and **re-encodes to exactly the same text**. Stored as its decoded bytes (25% smaller). The alphabet and padding variant are recorded in the symbol. A run that does not round-trip is not treated as base64.
3. **Hex run:** an alphanumeric run of at least 16 characters, an even number of them, that is all hex digits with letters in one case (a git SHA, a hash). Stored as bytes (half the size).
4. **Opaque run:** an alphanumeric run of at least 24 characters whose **Shannon entropy is at least 4.0 bits per character** (Q87) and which is not already in the dictionary. Stored raw, inline, with no dictionary entry and no 64-character chunking.

A run that is already in the dictionary is always emitted as an ordinal. All thresholds (64, 4.5, 16, 24, 4.0) are `TokenizerOptions` settings; the entropy values are first guesses to be calibrated on the corpus. A failed base64 candidate is remembered so a long non-base64 run is scanned only once.

### 4.3 Symbol encoding
A string becomes a stream of varints (LEB128). The low 2 bits of each value are a tag:

| Tag | Meaning | Value layout |
|-----|---------|--------------|
| `00` | word ordinal (multi-character word in the heap) | `ordinal << 2` |
| `01` | single character | `code_point << 2 \| 1` |
| `10` | inline opaque run | `byte_length << 2 \| 2`, followed by that many bytes of the original text |
| `11` | inline decoded run | `((decoded_length << 3 \| variant) << 2) \| 3`, followed by the decoded bytes. `variant` 0-3: base64 standard padded / standard unpadded / URL-safe padded / URL-safe unpadded; 4-5: UUID lower / upper case; 6-7: hex lower / upper case |

Common punctuation, spaces and newlines fit in 1-2 bytes; ordinals below 32 take 1 byte and below 4,096 take 2 bytes. The stream is stored with a leading varint symbol count. There is no 0 symbol: a null string is a null cell (6.2), not an empty stream.

### 4.4 Inserting a word
1. If the word has one character, emit a tag-`01` symbol and stop.
2. Compute `hash = xxh3_64(word, salt)`, starting with `salt = 0`.
3. Look up `hash` in the Dictionary.
   - **Missing:** append the word to the Heap (it gets the next ordinal), store `hash -> ordinal`, emit the ordinal symbol.
   - **Present:** compare the word's bytes with the heap bytes of that ordinal. If equal, emit the ordinal symbol. If not equal (hash collision), increment `salt` and go back to step 2.

## 5. Heap and Dictionary

### 5.1 Heap
- Stores unique multi-character words in ordinal order, each as `[varint length][bytes]`. Append-only and dense.
- Stored in compressed blocks (zstd). A word never crosses a block. An in-memory offset table (ordinal -> block and position) is built when the folder is opened, by scanning the blocks.
- Defined as a trait so implementations can be memory-backed (`BytesMut`) or file-backed (`memmap2` over the uncompressed blocks cache). Growth of a file-backed heap happens under the write lock; ordinals stay valid because they are indexes, not addresses.
- Hard limit: 2^32 words and 4 GB of word bytes (`HeapFull`).

### 5.2 Dictionary
- A `HashMap<u64, u32>` (hash with salt -> ordinal) with a pass-through hasher.
- **Lives as long as its folder is open for writing** (Q80, Q86 note). A folder covers a day or another time period; all dataframes in it share one heap and dictionary for maximum compaction. A reopened folder restores the dictionary, so new dataframes keep using the same words during the period. When the logs are **rotated** (the folder is sealed), the dictionary is discarded; the heap stays, because it is the data. **On disk (Q84):** only the heap is stored. Its length-prefixed words in ordinal order are enough: the dictionary is rebuilt by re-hashing every word when a folder is opened for writing or word search (one pass over the words). Hydration alone needs only the heap offset table. A sealed folder has no dictionary: word search there uses the sorted word table (7).
- **Limit: 64 MB, measured as the estimated memory of the in-memory dictionary** (Q79): entries x an assumed per-entry cost of 32 bytes (about 2,097,152 words). When an insert would exceed it, it fails with `DictionaryFull`; the insert is atomic (nothing from the failing row is added). The caller commits, closes the folder and starts a **new folder** for the next set of dataframes, with a fresh heap and dictionary. The library does not roll over by itself.

### 5.3 Collision handling
Because the Dictionary stores only hashes, a hit is verified against the heap bytes (4.4). A mismatch means a different word collided on the hash, so the word is re-hashed with an incremented salt until a match or an empty slot is found.

### 5.4 Locking
The Heap and Dictionary live together behind one `RwLock` inside a `Store` (one folder), shared as `Arc<Store>`.
- Lookups and hydration take the read lock; inserting a new word takes the write lock after a read-locked miss, re-checking under the write lock.
- **One writer at a time per folder.** A single `Store` write mutex serializes everything that writes: `push_row` on any dataframe, `create_frame` and `commit`. So a commit never overlaps an insert, and a folder is written by one thread at a time (and one process at a time, through the writer file lock). Reads (`row`, `search`, ...) do not take the mutex and run in parallel with each other and with a writer. Lock order is: write mutex, then dataframe lock, then the word store lock. The segment file pool lock (7) is a leaf: it may be taken under the locks above, but no lock is taken while it is held, and it is never taken while a segment file or block cache lock is held.
- Insert throughput is not a goal, so there is no sharding or per-row batching.

## 6. Dataframe
A dataframe is a table with a schema. All of one folder's rows may live in a single wide dataframe (Q83).

### 6.1 Schema
```
column_count | per column: name, type code (u8), flags, text format (null-terminated strings)
```
- **Type codes** are plain `u8` values: integers `i8..i64`, `u8..u64`; floats `f32`, `f64`; `bool`, `char`; timestamps `TimestampSec/Ms/Us/Ns`; `Date`; `TimeOfDay`; `TimestampTz`; and `String`, `Blob`. 128-bit integers are not supported.
- **Flags per column:** `json_escapes` (4.1) and `case_insensitive` (6.5).
- **Text format** (date/time columns only): empty = ISO-8601/RFC-3339; otherwise `builtin:rfc2822`, `builtin:clf`, `builtin:iso8601` or `strftime:<pattern>` (6.2.2).
- The schema is stored in the folder manifest and shared by all segments of the dataframe.

### 6.2 Cells
- **Fixed-size types** are stored by value as `u64` with an **order-preserving encoding**, so unsigned comparison of cells equals logical comparison. Little-endian on disk. Nulls are tracked by a per-block null bitmap (a value of 0 is valid).
- **String / Blob:** the cell is `row-heap logical offset + 1`; the null cell value is `0`.

#### 6.2.1 Encoding of fixed-size types
| Type | Encoding in the u64 |
|------|---------------------|
| `u8 u16 u32 u64` | value, zero-extended |
| `i8 i16 i32 i64` | value sign-extended to `i64`, then sign bit flipped |
| `f64` | `to_bits()`; if the sign bit is set invert all bits, otherwise set the sign bit |
| `f32` | same transform on its 32 bits, in the low 32 bits (bit-exact; NaN payloads and `-0.0` preserved) |
| `bool` | `0` or `1` |
| `char` | Unicode code point |
| `TimestampSec/Ms/Us/Ns`, `Date` | signed count of the unit since the epoch, encoded like `i64` |
| `TimeOfDay` | nanoseconds since midnight, unsigned |
| `TimestampTz` | high 48 bits: `epoch_millis + 2^47`; low 16 bits: `utc_offset_minutes + 2^15` |

A value that does not fit the column type is an error (`ValueOutOfRange`), never a silent truncation. `TimestampTz` covers about +-4,450 years, keeps whole-minute offsets, and sorts by instant then offset.

#### 6.2.2 Date and time text parsing
The `datetime` module parses date/time text into the values of date/time columns and formats them back, using `chrono`. Each column names its text format in the schema (`Column::text_format`, validated when the schema is built):
- `""` or `builtin:iso8601`: ISO-8601 / RFC-3339 (`2026-11-21T10:15:30.123+07:00`, `2026-11-21T10:15:30Z`, `2026-11-21 10:15:30`, `2026-11-21`; a date alone is midnight UTC; `Date` columns take `YYYY-MM-DD`, `TimeOfDay` columns `HH:MM[:SS[.fraction]]`)
- `builtin:rfc2822`: RFC-2822 / HTTP date (`Sat, 21 Nov 2026 10:15:30 +0700`, `... GMT`); timestamps only
- `builtin:clf`: Common Log Format time (`21/Nov/2026:10:15:30 +0700`); timestamps only
- `strftime:<pattern>`: a chrono/strftime pattern (use `%z` or `%:z` for an offset); a date-only pattern gives midnight

**Rules.** Text without a UTC offset is read as UTC (a `TimestampTz` then gets offset 0); an offset is folded into the UTC instant for the plain timestamp types and kept (in whole minutes) by `TimestampTz`. These are errors (`Error::DateParse`, naming the column): text that does not match the format, a leap second (`23:59:60`), a UTC offset with seconds, a value outside the column's range (nanoseconds cover about 1677-2262), and **text finer than the column's unit** (milliseconds in a `TimestampSec` column, microseconds in a `TimestampMs` or `TimestampTz` column): parsing never rounds or truncates, so choose the unit that matches the data. The library never stores null on a parse failure.

**Formatting** writes as many fractional digits as the unit has (seconds none, milliseconds 3, microseconds 6, nanoseconds 9) and `Z` for UTC (`+07:00` style for `TimestampTz`). `rfc2822` and `clf` have no fractions, so they drop them for finer units.

**Where it is used.** Inserting `Value::Str(text)` into a date/time column parses it with the column's format (a bad value leaves the dataframe unchanged); `Column::format_value(&OwnedValue)` gives the text of a value read back; and `Equals`/`Range` filters on a date/time column accept text bounds, parsed the same way. Numeric values (`Value::TimestampMs(..)`, ...) are accepted as before.

### 6.3 Segments and blocks
A dataframe is stored as one or more **segment files** (Q78). Each **commit** writes the rows buffered since the previous commit as one new, immutable segment; `commit_if_full()` commits when a dataframe has buffered `segment_rows` (default 100,000) rows, so segments are normally about that size, but may be smaller (a commit after a few rows) or larger (a caller that pushes more between commits). The manifest lists every segment with its row count, and **global row numbers** are resolved by binary search over the cumulative counts (this replaces the fixed `row / segment_size` rule of Q90, which does not hold for variable-size segments). A segment is **column-major**: rows are grouped into blocks of 4,096; for each column in a block the file stores a null bitmap flag, the **minimum and maximum** value, and a zstd-compressed block holding the null bitmap and the cell bytes transposed into 8 byte planes (which compresses integers and offsets well). A footer index gives each block's file position, size and checksum, so a reader opens a segment by reading only the footer and loads and decompresses only the blocks it touches (with a small cache). Many tiny segments (frequent commits) cost some overhead; compaction at seal time is future work.

### 6.4 Row heap
- Per segment; append-only; holds the symbol streams (4.3) of string cells and the raw bytes of blob cells. A string entry is `[varint symbol count][symbols]`; a blob entry is `[varint length][bytes]`.
- Stored in zstd blocks of about 1 MiB uncompressed (`row_heap_block_bytes`; chosen in milestone 6, see 13). An entry never crosses a block. A cell's logical offset locates its block through the block index. Reading one row decompresses the row-heap block of each of its string cells, so larger blocks compress better but make a random single-row read slower.
- **Whole-string dedup:** the in-memory buffer of a dataframe keeps a map `xxh3_64(entry bytes) -> logical offset`. An identical entry (bytes compared) is reused, so identical strings cost one cell each. The map is capped at 4,000,000 entries and starts empty again after each commit (so identical strings in different segments are stored once per segment); once full, new entries are written normally but are not added to the map. Blobs are not deduplicated.

### 6.5 Search and filters
The viewer needs: word/phrase search, equality filters, a time range, sorting and pagination (Q82). Logs are immutable and only searched, so **indexes are persisted** (Q89) in one index file per segment (`<segment>.idx`) and read through `memmap2` (Q93).

**What a word is.** Search works on the symbol streams, so a *word* is a word the tokenizer stored in the heap (two or more characters) or a single character. **Inline runs (UUIDs, hex, base64 and long random-looking ids) are not words** and never match a word or phrase filter; find them with an equality filter on the whole value. Words and phrases are split with the column's splitter, so a phrase is split the way the text was when it was stored. This definition is the same whether a query is answered from an index or by scanning, so results never depend on which indexes exist.

**Filters** (a `Query` is a list of filters combined with AND):
- `Word { column, word, case_insensitive }`: the string column contains the word (case-insensitive with full Unicode case folding via the `caseless` crate).
- `Phrase { column, text }`: the words of `text` appear next to each other, in order. Candidates come from the intersection of the words' posting lists and are then verified against the symbol streams.
- `Equals { column, value }`: whole-value equality (`Null` matches null cells). Strings are compared by their decoded text; fixed-size columns by their cell value.
- `Range { column, min, max }`: inclusive bounds on a fixed-size column; blocks are skipped by their stored minimum and maximum; nulls never match.

**Evaluation.** `Frame::search(&Query)` returns a lazy iterator of row numbers in insertion order: it evaluates one segment at a time, then the rows buffered since the last commit. Per segment: filters that have an index give sorted candidate lists, which are intersected (shortest first); filters without a usable index (single-character words, columns without the flag, `Null`) are checked row by row on the remaining candidates by decoding only the needed symbol stream or cell. `count(&Query)` and `search_page(&Query, &Page)` build on it; a page can be sorted by any fixed-size column (nulls last, ties by row number) and cut with `offset` and `limit`, and reports the total number of matches. The buffer of unwritten rows gets an in-memory index of the same format, rebuilt when the buffer has grown.

**Per-column index flags** (in the schema): `index_words` (word index, string columns), `case_insensitive` (an additional index of case-folded words, string columns) and `index_equality` (any column except `Blob`). Only flagged columns are indexed, which keeps the file size under control.

**Index file** (`LSQIDX01`): a table of term indexes, one per (column, kind). Each term index is a sorted directory of `(key, postings offset, count)` entries (20 bytes each) and delta-varint posting lists of ascending row numbers. Keys: the word ordinal (words), the xxh3 hash of the case-folded word (case-insensitive), and the cell value, or the xxh3 hash of the text for strings (equality). A whole-file xxh3 checksum is checked by `Store::verify()`, not on every open. Single-character words are never indexed (they would match nearly every row) and are checked by scanning.
- **Compression:** index files are stored zstd-compressed by default (`StoreOptions::compress_indexes`, `LSQIDXZ1`, about 2.7 times smaller) and decompressed into memory when opened; turn it off to memory-map plain index files (`LSQIDX01`) instead. Compressed files are checked when opened; plain files by `Store::verify()`. See 13 for the sizes.
- **Indexing is optional:** `StoreOptions::indexing = false` writes no index files at commit (searches then scan, with identical results). `Store::build_indexes(path, options)` builds every missing segment index, and a sealed store's `words.idx`, later from the stored segments and the heap (it takes the writer lock and may be run on a sealed store; a store that is already open sees the new files after reopening). Index building reads rows through the same row-source interface as queries, so it works on rows in memory and on written segments.
- **Word table:** a sealed folder also gets `words.idx`: the ordinals sorted by the words' bytes (4 bytes per word, memory-mapped). It serves `Store::lookup_word` and prefix suggestions (`Store::words_with_prefix`) with binary search; before sealing, suggestions scan the heap.
- Rows are matched against the symbol streams, so the index of a segment is built once, when the segment is written, from the rows in memory (no re-decoding of the segment).

### 6.6 Inserting rows
- `push_row(&self, &[Value])` checks the values against the schema and returns an error on a column-count or type mismatch.
- `Value` has one variant per fixed-size type code, plus `Str(&str)`, `Blob(&[u8])` and `Null`; hydration returns an owned form.
- A scratch `BytesMut` is reused per insert so the allocator is not pressured. Date/time text is parsed first; a failure aborts the insert and leaves the dataframe unchanged.
- Strings must be valid UTF-8; other data goes into a `Blob` column.
- Errors are one hand-written `Error` enum implementing `std::error::Error` (`HeapFull`, `DictionaryFull`, `TypeMismatch`, `ColumnCount`, `ValueOutOfRange`, `DateParse`, `Io`, `Corrupt`, `UnsupportedVersion`, ...).

### 6.7 Hydrating rows
- A row iterator yields typed values (`OwnedValue`). A `write_string(cell, &mut BytesMut)` helper decodes a string directly into a caller buffer. Decoding walks the symbol stream: ordinals are resolved through the heap, characters become `char`s, inline runs are copied (base64 runs are re-encoded).
- The store read lock is taken once per row. An optional `json` feature (using `serde`) converts rows to JSON for the server.

## 7. Persistence and Folders
A store is a **folder** covering one day or other time period. All its dataframes share one heap and dictionary for maximum compaction. An **archive** root directory holds numbered generation folders (`0000/`, `0001/`, ...); only the newest accepts writes.

```
<root>/0000/
  manifest            format version, splitter id/parameters, schemas, file list with committed lengths
  heap.bin            heap blocks (the dictionary is rebuilt from these at open, see 5.2)
  words.idx           sorted word table, written when the folder is sealed (6.5)
  <frame>-000000.seg  dataframe segment (up to 100,000 rows)
  <frame>-000000.idx  its persisted search index (6.5), memory-mapped for reads
  <frame>-000001.seg  ...
```
- **Append-only files, manifest as commit point.** `commit()` writes, in order: the new words as blocks appended to `heap.bin` (each block `[u32 compressed length][u32 raw length][u32 word count][u64 xxh3 checksum][zstd bytes]`, after the 8-byte magic `LSQHEAP1`), then one new segment file per dataframe with buffered rows, then atomically replaces the manifest (write `manifest.tmp`, sync, rename) with the new committed lengths. Every file is synced before the manifest is replaced. On a writable open, anything past the committed `heap.bin` length, segment files the manifest does not list, and `manifest.tmp` are removed. Sealed segments never change. **Rows are only durable after a commit**; rows buffered in memory when the process ends are lost, so callers commit regularly (`commit_if_full`). Automatic commits by byte count (`auto_commit_bytes`) are not implemented.
- **Integrity:** every block has an xxh3-64 checksum, verified when read; a mismatch is `Corrupt`. A reader rejects a manifest with a newer format version (`UnsupportedVersion`).
- **Reuse within a period:** a folder is reopened for appending during its period, so restarts keep sharing one heap and dictionary (Q80). On rotation the caller seals the folder (writes `words.idx` and the final indexes, drops the dictionary) and starts the next one. If the dictionary fills first (`DictionaryFull`), the caller also starts the next folder.
- **Access:** a writable open takes an exclusive advisory lock on the folder's `lock` file (`fs4`), so only one process writes. **Readers take no lock** (`Store::open_read_only`): committed segments are immutable, `heap.bin` is only ever appended to and read up to the committed length, and the manifest is replaced atomically, so a reader sees a consistent snapshot of the last commit even while a writer is running (this deviates from Q71, which would have blocked the web server's reads while the daily folder is being written). A reader sees new data by reopening. Read-only opens read lazily: only the manifest, `heap.bin` and each segment's footer index are read at open.
- **Open segment files are bounded** (decision 100): every `Store` has one pool of open segment files, 64 by default (`Store::max_open_segments`, `set_max_open_segments`, `open_segment_files`). A segment reader keeps its path and the file's identity (length; device and inode on Unix) instead of an open file. A block read takes the reader's handle from the pool (or opens the file again and adds it, closing the least recently used one), releases the pool lock, and only then locks the file to read. The pool lock is a leaf lock: no other lock is taken while it is held, and it is never taken while a file or cache lock is held. A handle evicted during a read is closed when that read ends. A reopened file that is gone fails with the open error (`Error::Io`, not found); one whose identity changed fails with `Error::Io` as well. Dropping a reader (or the store) closes its file. Decoded-block caches are per reader and unaffected.
- **Rows are immutable**: no updates or deletes.
- **Creating a store:** `Store::create(path, options)`, `Store::open(path, options)` (writable), `Store::open_read_only(path, options)`, with a `StoreOptions` struct (tokenizer thresholds, dictionary limit, zstd level, segment rows, block rows, row-heap and heap block sizes, dedup cap). `Store::create_frame(name, schema)` adds a dataframe (identified by its unique name, Q68), `commit()`, `commit_if_full()`, and `seal()` (commit and close for writing). A sealed store opens read-only only. An in-memory store is just a `DataFrame` over a `WordStore`.

## 8. Decisions Log
| # | Question | Decision |
|---|----------|----------|
| 1 | What is a "word"? | Alphanumeric runs; every other char is its own single-char word. Lossless. |
| 2 | Role of s-box? | Single-character words are stored directly in the symbol stream as a tagged code point (no heap entry). (S-box replaced by the tag, see 4.3.) |
| 3 | Heap addressing / dictionary key? | Append-only dense heap; hash is only the dictionary key; collisions detected by byte compare and resolved by re-salting. |
| 4 | Input / API? | Library only; no CLI or parsing. |
| 5 | Distinguish s-box vs heap Token? | Superseded by Q73: symbols are tagged varints, not packed Tokens. |
| 6 | Charset? | Full Unicode scalar values; a single character is stored as its code point; input is valid UTF-8. |
| 7 | `DataType` u8 and null? | Drop the u8 payload (type code only); Int/Float null via bitmap; String null = 0. |
| 8 | Row heap addressing? | Per-dataframe append-only row heap; cell = byte offset + 1; search via inverted index. |
| 9 | Primitive types? | `i8-i64`, `u8-u64`, `f32`, `f64`, `bool`, `char`. No 128-bit. |
| 10 | Date/time types? | Timestamp sec/ms/us/ns, Date (days), TimeOfDay, TimestampTz; all stored in a u64. |
| 11 | Meaning of "format"? | Distinct type code per representation; additionally the library parses text date/time formats (scope extended). |
| 12 | Cell encoding? | Order-preserving: sign-flip for signed ints, bit-transform for floats; little-endian on disk. |
| 13 | Text formats parsed? | ISO-8601/RFC-3339, RFC-2822/HTTP, Apache/nginx CLF, user strftime pattern (stored in header). |
| 14 | Parse failure? | Return an error; never silently store null. |
| 15 | `TimestampTz` packing? | 48-bit biased epoch millis (high) + 16-bit biased offset minutes (low). |
| 16 | `TimeOfDay` unit and crate? | Nanoseconds since midnight; `chrono`. |
| 17 | How a column picks its text format? | Explicit per column in the header; empty = ISO-8601; no auto-detect. |
| 18 | Zone when text has no offset? | UTC. |
| 19 | Blob layout? | Raw in the row heap as `[u32 len][bytes]`; no tokenizing or dedup. |
| 20 | Header `length`? | Total header size in bytes; shared via `Arc` in memory, written once per file/segment. |
| 21 | Inverted index? | Per dataframe, built incrementally on insert. |
| 22 | Persistence? | Superseded by Q78: a store is a folder (a generation) with a manifest, heap, dictionary and segment files. |
| 23 | Locking? | One `RwLock` around heap + dictionary together. |
| 24 | Heap full / dictionary type? | `HeapFull` error; `HashMap<u64, Token>`. |
| 25 | Hydration API? | Row iterator of typed values plus a `write_string` helper decoding into `BytesMut`. |
| 26 | Insert API? | Slice of dynamic values, encoded through a reused scratch `BytesMut`. |
| 27 | Store handle? | `Arc<Store>` holding `RwLock<{heap, dictionary}>`. |
| 28 | Crate layout? | Library crate: `sbox`, `heap`, `dictionary`, `tokenizer`, `dataframe`, `datetime`. |
| 29 | `Value` enum? | Exact variant per fixed type, plus `Str(&str)`, `Blob(&[u8])`, `Null`; borrowed on insert, owned on read. |
| 30 | Error type? | One hand-written `Error` enum implementing `std::error::Error`. |
| 31 | Value does not fit column type? | Error. |
| 32 | Float NaN / -0.0? | Preserve exact bits; `f32` encoded in its own 32 bits. |
| 33 | Invalid UTF-8? | Strings must be valid UTF-8; anything else goes in a `Blob` column. |
| 34 | Maximum word length? | Alphanumeric runs over 64 characters are split into 64-character chunks. |
| 35 | High-cardinality words? | Pluggable `Splitter` trait; default is the 4.1 splitter. |
| 36 | Whole-string dedup? | Yes: per-dataframe map from hash of the Symbol array to row-heap offset. |
| 37 | Case sensitivity? | Case-sensitive storage; optional per-column case-insensitive search via a second (lowercased) index. |
| 38 | Index scope? | Per column: key `(column, Symbol)`. |
| 39 | Query types? | Single word, AND, exact phrase. |
| 40 | One-character query words? | Not indexed; scan fallback. |
| 41 | Postings format? | `Vec<u32>` row ids (max 2^32 rows per dataframe). |
| 42 | Index on load? | Rebuilt from the row heap; a record id is reserved for a persisted index. |
| 43 | Mapped heap? | Mapped heap blocks are append-only and read through `memmap2`; remapped on growth under the write lock. |
| 44 | Updates/deletes? | None; rows are immutable. |
| 45 | Saving? | Incremental, append-only files; the manifest is the commit point (see 7). Replaces the single-file log. |
| 46 | Integrity? | Checksum (xxh3-64) per block, verified on load. |
| 47 | Newer file version? | Reject. |
| 48 | Dictionary on disk? | Superseded by Q80/Q84: the dictionary persists across sessions but is rebuilt from the heap, not stored as pairs. |
| 49 | Writers? | One writer per folder: a single store-wide mutex serializes `push_row`, `create_frame` and `commit` (supersedes the earlier per-dataframe locking). |
| 50 | Store write-lock contention? | Accepted for v1; no per-row batching; measure. |
| 51 | Header format names? | Prefix scheme: `builtin:<name>` and `strftime:<pattern>`. |
| 52 | Text output precision? | Follows the column unit (0/3/6/9 digits). |
| 53 | `TimestampTz` offsets? | No offset = `+00:00`; whole-minute offsets preserved; seconds in an offset = error. |
| 54 | Out-of-range dates? | Error on insert. |
| 55 | Leap seconds? | Rejected. |
| 56 | MSRV? | Pin the lowest version that builds all dependencies (see 10). |
| 57 | Tests/benchmarks? | Unit tests, `proptest` round-trip properties, and `criterion` benchmarks. |
| 58 | Success metric? | File size <= 30% of raw text. (The 500 MB/s insert target was dropped later.) |
| 59 | Test corpus? | Claude Code LLM output first; then nginx access, syslog, JSON lines, app logs. |
| 60 | Heap placement in the file? | Chunked `HeapChunk` records plus an in-memory chunk table; words never cross a chunk; one file, incremental saves. |
| 61 | Edition / MSRV? | Keep edition 2024; `rust-version = "1.85"`. |
| 62 | Memory bounds? | Dictionary limited to 64 MB, shared across the files of a store folder; when full, a new folder is started for the next set of dataframes. `DictionaryFull` is atomic. (Replaces the earlier 3 GB heap / 1M entry idea.) Whole-string map capped at 1M entries. |
| 63 | Case-insensitive search? | Per-column schema flag; full Unicode case folding via the `caseless` crate. |
| 64 | Throughput vs write lock? | Simple single-`RwLock`; insert throughput is not a goal. |
| 65 | HeapChunk size? | Variable: one chunk per commit with all new words. |
| 66 | Commits? | Explicit `commit()` plus optional automatic commit after N new bytes (default off). |
| 67 | Splitter in the file? | Yes: splitter id and parameters in the file header; mismatch is an error. |
| 68 | Dataframe identity? | Unique name string. |
| 69 | Search result? | Lazy iterator of row ids plus a hydrate-by-id helper. |
| 70 | Store creation? | Constructors plus `StoreOptions` builder. |
| 71 | Multiple processes? | Superseded in milestone 3: one writer at a time (exclusive lock on the `lock` file); readers take no lock and read the last commit, so the web server can read a folder that is being written. |
| 72 | Read-only mode? | Yes: `Store::open_read_only`. |
| 73 | Symbol encoding? | Varint, 2-bit tag: word ordinal, single character, inline raw run, inline base64 run. |
| 74 | Block compression? | Yes: zstd per block (heap, row heap, column blocks); each block independently readable. |
| 75 | Base64 / random-looking runs? | Valid base64 runs are stored decoded inline (exact round-trip verified); other long random-looking runs are stored inline raw; both skip the dictionary. |
| 76 | JSON escapes? | Escape-aware splitter (a backslash plus the next character is one word); chosen per column. |
| 77 | Body layout? | Column-major blocks of 4096 rows, each column block compressed, with per-block min/max and null bitmap. |
| 78 | Folder layout? | manifest + dictionary/heap + dataframe segment files of up to 100,000 rows. |
| 79 | Meaning of the 64 MB limit? | Estimated memory of the in-memory dictionary. |
| 80 | Persist the dictionary? | Yes: it persists across sessions so new dataframes reuse it (daily-rotated logs). |
| 81 | How does the web app read data? | A Rust server hydrates and searches; the web app calls its HTTP/JSON API. |
| 82 | Viewer needs? | Word/phrase search, equality filters, time range, sorting and pagination. |
| 83 | Corpus mapping? | One wide dataframe for runs and events together (`kind` column tells them apart). |
| 84 | Dictionary on disk? | Not stored; rebuilt from the length-prefixed heap when a folder is opened. |
| 85 | Server scope? | Library only; the server and the web app are separate projects. |
| 86 | Default thresholds? | Base64 run 64+, opaque run 24+, hex run 16+, segment 100,000 rows, column block 4,096 rows, row-heap block 1 MiB (changed from 64 KiB in milestone 6), zstd 19, dictionary entry cost 32 bytes. All settings, tuned after measuring. |
| 87 | Opaque run test? | Shannon entropy per character (4.0 bits default), not already in the dictionary. |
| 88 | UUID / hex ids? | UUIDs and long hex runs are stored decoded inline (tag 11 variants 4-7). |
| 89 | Search indexes? | Persisted per segment (logs are immutable); per-column index flags; memory-mapped for reads. |
| 90 | Row identity? | One global row number per dataframe; resolved by binary search over the cumulative segment row counts in the manifest (segments are one per commit, so their sizes vary). |
| 91 | Stubs? | `main.rs` and the placeholder structs are deleted; `lib.rs` with the design's modules. |
| 92 | Harness? | `examples/` with dev-dependencies; corpus tests skip when the corpus is missing; `scratch/` is git-ignored. |
| 93 | mmap? | `memmap2` feature on by default but used only for index files; blocks use plain reads. |
| 94 | Review points? | Stop after milestone 1 (with compression numbers) and after milestone 3. |
| 95 | Search semantics? | Words are heap words or single characters from the symbol streams; inline runs are not words (use equality); index and scan paths give identical results. |
| 96 | Index compression? | Compressed index files by default (`compress_indexes`); plain mmap-able files remain an option. |
| 97 | Indexing optional? | Yes: `StoreOptions::indexing`; `Store::build_indexes` builds missing indexes later from the stored segments and heap. |
| 98 | Date/time parsing details? | ISO-8601/RFC-3339, RFC-2822, CLF and strftime; UTC when no offset; errors for finer-than-unit precision, leap seconds, offsets with seconds and out-of-range values; text accepted on insert and in filters. |
| 99 | Tuned defaults? | Row-heap block 1 MiB (was 64 KiB: -10% total size, random row read about 0.85 ms instead of 0.36 ms); everything else kept (zstd 19, 4,096-row column blocks, 100,000-row segments, inline-run thresholds). A trained zstd dictionary was tried and rejected (see 13). Decoded-block cache is bounded by bytes (`cache_bytes`, 32 MiB per segment). |
| 100 | Open segment files? | At most 64 per store by default, least recently used closed first and reopened on demand (0.1.2). Set with `Store::set_max_open_segments` rather than a `StoreOptions` field, because adding a field to a struct with only public fields breaks struct literals that list every field (not allowed in a 0.1.x patch). |
| (note) | Folder = period | A folder covers a day or other period and is shared by all its dataframes; on rotation the dictionary is dropped, the heap and indexes stay. |

## 9. Open Questions
- Entropy thresholds: settled in milestone 6 (the sizes are insensitive to them, see 13).
- Index files are about 3% of raw when compressed, 7.8% when plain; the whole corpus with every index flag set is 28.2% of raw (see 13). Consider flagging fewer columns only if a future corpus misses the 30% target.
- Row-heap block size trades size against random-read latency (1 MiB default; 4 MiB is 6% smaller but 2.2 times slower per random row); a deployment that cares more about one or the other can change `StoreOptions::row_heap_block_bytes`.
- MSRV: `caseless` does not declare a minimum Rust version; `rust-version = "1.85"` is not yet verified against it.

## 10. Crate Layout and Dependencies
Library crate (`lib.rs`) with modules: `splitter`, `tokenizer`, `heap`, `dictionary`, `dataframe`, `segment`, `datetime`, `store`. The server and web app are separate projects (Q85). The stub `main.rs` is not part of the library.

| Crate | Use |
|-------|-----|
| `xxhash-rust` (feature `xxh3`) | xxh3-64 hashing and checksums |
| `chrono` | date/time parsing and formatting (6.2.2) |
| `bytes` (`BytesMut`) | scratch and in-memory buffers |
| `memmap2` (feature `mmap`, on by default) | memory-mapped reads of the index files and `words.idx` only (Windows, Linux, macOS); compressed blocks use plain file reads (Q93). All `unsafe` is confined to one module; the rest of the crate has `#![deny(unsafe_code)]` |
| `fs4` | advisory file locks |
| `caseless` | full Unicode case folding |
| `zstd` | block compression |
| `base64` | exact base64 detection and round-trip (4.2) |
| `serde` / `serde_json` (optional `json` feature) | JSON output for the separate server project |
| dev: `serde_json`, `proptest`, `criterion`, `tempfile` | tests, the corpus harness in `examples/`, benchmarks (Q92) |

`adaptif-gatotkaca-eventfd` is an eventfd crate, not a memory-mapping crate, so it is not used.

**MSRV.** The highest minimum Rust version among the original dependencies is 1.82 (the Windows crates pulled in by `chrono`); the project uses edition 2024, which requires 1.85, so `rust-version = "1.85"` is pinned. Re-check when dependencies change.

**Code style.** Lines are at most 140 characters (`max_width = 140` in `rustfmt.toml`, plus `use_small_heuristics = "Max"` so short expressions stay on one line). Lint and formatting preferences live in the code itself: crate-level attributes at the top of `lib.rs` (for example `#![allow(clippy::too_many_arguments)]`) and `#[rustfmt::skip]` where a table reads better unformatted, so formatting and clippy warnings do not need to be chased by hand.

## 11. Quality and Goals
**Priorities (decided):** (1) file size, target <= 30% of the raw text; (2) ease of rehydration and search, with logs viewable in a web app. Insert throughput is not a goal.

**Tests:** unit tests per module, plus `proptest` properties: `hydrate(insert(x)) == x` for all types, splitter losslessness (default and escape-aware), base64 round-trip, manifest/torn-write recovery.
**Benchmarks:** `criterion` benchmarks for compression ratio and for hydrate and search latency.
**Corpus:** the harness run logs in `scratch/transcripts` first (see 12); then nginx access logs, syslog, JSON lines and application logs.

## 12. Findings from the Sample Corpus
Measured on `scratch/transcripts` (743 JSONL files, 831,523 lines, 775 MB). Detailed figures below come from every 10th file (75 files, 91,681 lines, 90.5 MB); the whole-corpus line counts are exact.

**Shape of the data.** Each file starts with one `run` line (46-key object with run metadata: name, project, model, prompt, cost, ...) followed by `event` lines with the fixed fields `kind, id, run_id, project_id, ts, type, subtype, tool, tool_use_id, parent, text` and a nested JSON `data` object whose shape depends on `type/subtype`. By bytes: `data` is 60%, `text` 15%, run `prompt` 5%, `ts` 3%. By line count, 79% of events are `system/thinking_tokens` (tiny: counters plus a UUID each); by volume, `user/tool_result` `data` is the largest (60% of all `data` bytes).

**Reference compressors** on the 90.5 MB sample: `zstd -19` 20.4 MB (22.6%), `xz -9` 20.3 MB (22.4%).

**The design as written does not reach the target on this data:**
| Layout (on the 90.5 MB sample) | Size | % of raw |
|---|---|---|
| Design as written: one 8-byte Symbol per word or character, 16 bytes per dictionary entry, whole-string dedup | 229 MB | 253% |
| Same with 4-byte Symbols | 133 MB | 147% |
| Varint ids by frequency rank, length-prefixed heap, 8-byte cells x 12 columns, no dictionary on disk | 62 MB | 68% |
| Same, with the row heap and heap zstd-19 compressed per file | 31 MB | 35% |

Reasons: (1) an 8-byte Symbol per word or punctuation mark is larger than the text it replaces; (2) the dictionary (16 bytes per unique word) is about 15 MB, 17% of raw, and is not needed to read rows; (3) 8-byte fixed cells for 12 columns cost 8.8 MB, 10% of raw; (4) about 80% of the heap is **base64 text** (screenshots and other binary payloads inside `tool_result`): 758k of 860k distinct words appear once, and about 16 of the 21 MB of heap are long mixed-case alphanumeric runs. Word dedup cannot compress these, and the 64-character chunking adds an 8-byte reference plus a 16-byte dictionary entry per 64 bytes; (5) JSON string escapes (`\n`, `\"`) glue onto the next word under the default splitter ("`\nfoo`" becomes `\` + `nfoo`), producing spurious unique words.

These findings drove the symbol encoding (4.3), inline base64/opaque runs (4.2), the escape-aware splitter (4.1), block compression and the column-major layout (6.3). The numbers should be re-measured on the Rust implementation.

### Measured with the Rust milestone-1 code
`cargo run --release --example tokenize_corpus -- scratch/transcripts <step> 19` (tokenizer thresholds at their defaults; the estimate is `zstd-19(row heap) + zstd-19(heap) + 8-byte raw cells for 12 columns`, so it does not yet include column compression, indexes or file overhead):

| Input | Raw | Estimated stored | % of raw | Words in heap | Dictionary |
|---|---|---|---|---|---|
| every 10th file (75 files, 91,681 rows) | 90.5 MB | 29.3 MB | 32.3% | 41,672 | 1.3 MB |
| whole corpus (743 files, 831,523 rows) | 775.5 MB | 227.0 MB | 29.3% | 152,474 | 4.9 MB |

- Inline runs removed almost all unique words from the heap: 152k words instead of about 5 million, so the whole 775 MB corpus needs only 4.9 MB of the 64 MB dictionary budget (one folder).
- After whole-string dedup the symbol streams hold 520.7 MB, of which 131.8 MB is inline decoded runs (base64 images, UUIDs, hex; 17% of raw, incompressible) and 127.8M single-character symbols. zstd takes the row heap from 520.7 MB to 146.4 MB.
- The raw 8-byte cells are 79.8 MB (10% of raw); column-major compressed blocks (6.3) should shrink them to a few MB, which would put the estimate at about 25%. Tokenizing took 11 s for 775 MB.

## 13. Implementation Status
- **Milestone 1 done:** `varint`, `splitter` (default and JSON-escape), `heap` (`MemHeap`), `dictionary`, `words` (shared store with rollback and rebuild from heap), `tokenizer` (UUID, base64, hex and opaque inline runs; tagged varint symbols), and `examples/tokenize_corpus.rs`.
- **Milestone 2 done:** `types` (`ColumnType` with stable codes, `Value`, `OwnedValue`), `cell` (order-preserving cell encodings, bit-exact floats, `TimestampTz` packing, range checks), `schema` (column flags, validation), `dataframe` (in-memory `DataFrame`: `push_row(&self)` that changes nothing on error, whole-string dedup with a capped map, blobs, null bits, `row`, `value`, `write_string`, `rows()` iterator, several writer threads, several dataframes sharing one word store), and `examples/dataframe_corpus.rs`. Date/time text parsing is milestone 5; until then date/time columns take the numeric values.
- **Checks:** 48 unit and property tests pass and clippy is clean. The corpus example loads all 831,523 rows (41 columns, 775 MB) into one wide dataframe in 8 s, reads every row back in 4 s, and finds 0 mismatches against the source JSON.
- **Measured, wide dataframe (whole corpus):** the schema has 41 columns because runs and events share one dataframe (Q83), so the raw 8-byte cells are 272.7 MB (35% of raw) and the in-memory estimate is 420 MB (54%). That is expected before milestone 3: column-major blocks with null bitmaps and zstd should reduce mostly-null and repetitive columns to a few MB. This is the first thing to verify in milestone 3. The whole-string dedup map reached its 1,000,000-entry cap on the full corpus.
- **Milestone 3 done:** `codec`, `segment` (column-major blocks with min/max per block, byte-plane transposition, zstd, row-heap blocks, footer index, per-block checksums, lazy block reader with a small cache), and `store` (`Store`, `Frame`, `StoreOptions`: manifest, `heap.bin`, commit/seal, crash-safe cleanup, writer lock, lock-free read-only open, global row numbers across segments and the unflushed buffer) plus `examples/store_corpus.rs`. 65 tests pass and clippy is clean. Tests cover round trips over many blocks, corruption detection (blocks, index, footer, manifest, heap), torn-commit cleanup, version rejection, the dictionary surviving a reopen, a full dictionary, read-only and sealed stores, two dataframes sharing one heap, and concurrent writers with commits.
- **Measured on disk (real files, zstd level 19):**

| Input | Raw | `heap.bin` | Segments | Total | % of raw |
|---|---|---|---|---|---|
| every 10th file (91,681 rows, 40 columns) | 90.5 MB | 0.18 MB | 1 x 25.17 MB | 25.35 MB | 28.0% |
| whole corpus (831,523 rows, 41 columns) | 775.5 MB | 0.73 MB | 9 x 197.7 MB total (largest 36 MB) | 198.4 MB | 25.6% |

  Reopening read-only takes 0.01 s; reading all 831,523 rows back from disk and comparing them with the source JSON takes 8.5 s with 0 mismatches. Writing took 102 s, almost all of it zstd level 19. For reference, `zstd -19` on the raw file gives about 22.6%. Indexes (milestone 4) are not included yet; see section 9.
- **Milestone 4 done:** `filebytes` (the only `unsafe`, for memory mapping), `index` (index files, `SegmentIndex`, `WordTable`, optional compression), `query` (`Query`, `Filter`, `Page`, `OrderBy`, evaluation over segments and the buffer), tokenizer symbol parsing (`symbols`), and in `store`: index files written at commit and cleaned up with their segments, `Frame::search`/`count`/`search_page`, `Store::lookup_word`, `words_with_prefix`, `verify`, `words.idx` at seal. 80 tests pass and clippy is clean. The main test generates data from known rules and checks 27 queries (words, case-insensitive, single characters, phrases, JSON escapes, equality, null, ranges, combinations) against ground truth computed from the rules, on an indexed and an unindexed dataframe, with the rows in the buffer, in one segment, in segment plus buffer, and after reopening a sealed store with three segments.
- **Measured on the corpus** (`examples/search_corpus.rs`; every flag set: words on `text` and `data`, case-insensitive on `text`, equality on six columns; zstd level 19):

| Input | Without indexes | Index files | With indexes |
|---|---|---|---|
| sample (91,681 rows, 90.5 MB), plain index files | 25.35 MB = 28.0% | 7.02 MB = 7.8% | 32.53 MB = 35.9% |
| sample, zstd-compressed index files | 25.35 MB = 28.0% | 2.62 MB = 2.9% | 28.13 MB = 31.1% |
| whole corpus (831,523 rows, 775.5 MB), compressed | 198.39 MB = 25.6% | 23.50 MB = 3.0% | 222.50 MB = 28.7% |

  On the sample the plain index cost splits as: words on `text` 2.0%, words on `data` 3.4% more, the case-insensitive index 1.8% more, equality on six columns 0.5%. (`words.idx` adds 0.17 MB on the sample and 0.61 MB on the corpus, included above.) Query times on the whole corpus, indexed versus a scan of the unindexed twin: word `error` in `text` 0.1 ms vs 733 ms; case-insensitive 0.1 ms vs 1.6 s; word in `data` matching 654,821 rows 3.3 ms vs 1.1 s; `type = assistant` 0.3 ms vs 91 ms; phrase `let me` 75 ms vs 730 ms; type + subtype + word `error` 0.6 ms vs 684 ms; a page of 50 sorted rows out of 77,406 matches 14 ms; prefix suggestions 0.6 ms. A single-character word (`:`) is scanned either way (0.7 s). Indexed and unindexed searches agree on every query.
- **Changes after milestone 4:** compressed index files are the default; `StoreOptions::indexing` can turn index files off and `Store::build_indexes` builds them later from the stored data; all writes in a folder go through one mutex (5.4).
- **Milestone 5 done:** the `datetime` module (formats, strict parsing, formatting), validation of `text_format` in schemas, text values accepted for date/time columns on insert and in `Equals`/`Range` filters, `Column::format_value`. 96 tests pass and clippy is clean. Tests cover every format, all five timestamp units, `Date` and `TimeOfDay`, offsets, the error rules, property tests for format-then-parse round trips, atomic failure of a bad row, and queries and sorting by time after a reopen.
- **Measured on the corpus** with `ts`, `created_at`, `started_at` and `ended_at` stored as `TimestampMs` (the example harnesses insert the ISO text and check that formatting gives back exactly the original text; 0 mismatches):

| Input | Without indexes | With every index (compressed) |
|---|---|---|
| sample (90.5 MB) | 24.95 MB = 27.6% | not run with compression |
| whole corpus (775.5 MB) | 194.56 MB = 25.1% (was 25.6% with timestamps as text) | 218.66 MB = 28.2% |

  A time-range query on the whole corpus (`ts` between two ISO strings) returns the same 1,247 rows with and without indexes, in about 0.1 ms either way (the range filter uses the per-block minimum and maximum).
- **Milestone 6 done: benchmarks and tuning.** New: `benches/store.rs` (criterion, synthetic logs), `examples/tune_corpus.rs` (parameter sweeps on real logs), `examples/dict_experiment.rs`, a byte-bounded block cache (`StoreOptions::cache_bytes`), and a new default row-heap block size. 96 tests pass and clippy is clean.

**Sweeps** (`tune_corpus`; every search index on and compressed, zstd 19; totals include indexes and are % of raw; sample = every 10th file, 90.5 MB; "row" is the average of 2,000 random single-row reads on a freshly opened store, so mostly cold):

| Setting | Values tried and totals (sample) | Decision |
|---|---|---|
| zstd level | 3: 33.6%, 9: 31.7%, 15: 31.2%, 19: 30.8%, 22: 30.8% (write 1.4 s, 1.8 s, 4.8 s, 11.9 s, 17.2 s) | keep 19 (22 gains nothing) |
| column block rows | 512: 31.2% (row 166 us), 1,024: 31.0%, 2,048: 30.9%, 4,096: 30.8% (row 438 us), 8,192: 30.8%, 16,384: 30.7% | keep 4,096 |
| segment rows | 25,000: 31.2%, 50,000: 31.0%, 100,000 and 200,000: 30.8% (whole corpus: 400,000 is no smaller than 100,000 with 4 MiB blocks: 24.06% vs 24.00%) | keep 100,000 |
| row-heap block bytes | 16 KiB: 32.3%, 32 KiB: 31.5%, 64 KiB: 30.8%, 128 KiB: 30.2%, 256 KiB: 29.6%, 1 MiB: 28.2%, 2 MiB: 27.5%, 4 MiB: 27.1%, 16 MiB: 26.2% (row 0.4 ms at 64 KiB, 0.9 ms at 1 MiB, 1.5 ms at 4 MiB, 1.9 ms at 16 MiB) | **1 MiB** |
| opaque run (entropy 3.0-5.0, length 16-48 or off) | 28.2-28.3% in every case | keep 4.0 / 24 (they only keep one-off ids out of the dictionary) |
| hex run (length 8, 32, or off) | 28.2% | keep 16 |
| base64 run (length 32-256, entropy 3.5-5.0) | 28.2% in every case | keep 64 / 4.5 |
| inline detection fully off (base64, hex and opaque) | 38.0%: the 20 MB of base64 text becomes 733,000 heap words (15 MB heap file, bigger indexes) | inline runs are worth about 10 points |
| base64 detection off only | 34.9% | (base64 decoding is the important one) |

**Whole corpus** (775.5 MB, all indexes compressed, zstd 19):

| Row-heap block | Total | Random row read | Sequential hydration |
|---|---|---|---|
| 64 KiB (old default) | 28.28% | 0.36 ms | 162,000 rows/s |
| **1 MiB (new default)** | **25.48%** | 0.85 ms | 170,000 rows/s |
| 4 MiB | 24.00% | 1.9 ms | 172,000 rows/s |
| 4 MiB, 400,000-row segments | 24.06% | 2.5 ms | 167,000 rows/s |
| 4 MiB, 1,024-row column blocks | 24.20% | 1.4 ms | 169,000 rows/s |

The 1 MiB default makes the whole corpus 25.5% of raw with every index (segments 172.2 MB, indexes 24.1 MB, heap 0.7 MB), a fifth under the 30% target. Opening a store read-only takes about 80-90 ms on the whole corpus; a word search takes 0.09 ms.

**Trained zstd dictionary: rejected.** On the row heap of the sample (61.5 MB), 64 KiB blocks compress to 24.6 MB independently, 23.5 MB with a 128 KiB dictionary trained on other blocks, but 22.2 MB as plain 1 MiB blocks; a dictionary on top of 1 MiB blocks gains only 0.3 MB more. Larger blocks are the cheaper lever, with no extra file or format.

**Criterion benchmarks** (`cargo bench --bench store -- --quick`, 100,000 synthetic rows, zstd 3, warm cache, one machine; the synthetic rows are far smaller than the real logs, so absolute numbers are indicative only; cold reads on real logs are in the sweep above):

| Benchmark | Time |
|---|---|
| tokenize 1,000 JSON strings | 0.98 ms (116 MiB/s) |
| `push_row` x 1,000 in memory | 2.0 ms (496,000 rows/s) |
| commit 20,000 rows with indexes | 73 ms |
| `build_indexes` for 4 segments of 5,000 rows | 75 ms |
| `open_read_only` (100,000 rows) | 3.6 ms |
| hydrate one row (cached) / 100 consecutive rows | 1.7 us / 154 us |
| word search (`rejected`) / case-insensitive | 72 us / 71 us |
| equality, level = ERROR | 158 us |
| word + equality + range | 249 us |
| time range with text bounds | 15 us |
| word in JSON text | 595 us |
| page of 50 sorted by id | 0.78 ms |
| prefix suggestions | 0.23 us |
| phrase `attempt 3` | 39.7 ms (every row contains `attempt`, so all 100,000 candidates are verified) |
| single-character word `:` | 35.6 ms (scan) |

Things the benchmarks make visible: a phrase made of very common words (or single characters) falls back to verifying many rows and takes tens of milliseconds, and a single-character word always scans.
- Next: nothing is planned; remaining work is whatever the user asks for next.
