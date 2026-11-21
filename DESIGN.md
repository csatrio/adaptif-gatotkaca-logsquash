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
| Block | A compressed unit inside a file (a run of heap words, 4096 rows of one column, or about 64 KiB of row heap). |
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
- Each dataframe has its own internal lock (several threads may `push_row(&self)`); lock order is dataframe lock, then store lock.
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
The library parses and formats date/time text with `chrono`: ISO-8601/RFC-3339, RFC-2822/HTTP date, Apache/nginx Common Log Format time, and user `strftime` patterns. Each column names its format explicitly (no auto-detection). Text with no offset is read as UTC (`TimestampTz` stores offset 0). Parse failures, out-of-range dates, offsets with seconds and leap seconds are errors. Formatting precision follows the column unit (0/3/6/9 digits).

### 6.3 Segments and blocks
A dataframe is stored as one or more **segment files** of up to 100,000 rows each (Q78). A segment is **column-major**: rows are grouped into blocks of 4,096; for each column in a block the file stores a null bitmap, the **minimum and maximum** value (fixed-size types), and the zstd-compressed cell array. A block index at the end of the segment gives each block's file position. This lets a reader load only the columns and blocks it needs, and skip whole blocks by min/max when filtering a time range.

### 6.4 Row heap
- Per segment; append-only; holds the symbol streams (4.3) of string cells and the raw bytes of blob cells. A string entry is `[varint symbol count][symbols]`; a blob entry is `[varint length][bytes]`.
- Stored in zstd blocks of about 64 KiB uncompressed. An entry never crosses a block. A cell's logical offset locates its block through the block index.
- **Whole-string dedup:** the segment keeps a map `xxh3_64(entry bytes) -> logical offset`. An identical entry (bytes compared) is reused, so identical strings cost one cell each. The map is capped at 1,000,000 entries; once full, new entries are written normally but are not added to the map. Blobs are not deduplicated.

### 6.5 Search and filters
The viewer needs: word/phrase search, equality filters, a time range, sorting and pagination (Q82). Logs are immutable and only searched, so **indexes are persisted** (Q89) in per-segment index files and read through `memmap2`, which makes reads cheap (Q93).
- **Per-column index flag:** `none`, `equality`, `words` or both. Index files only cover flagged columns, so file size stays under control: the transcripts corpus would index `text` and `data` by words and `kind`, `type`, `subtype`, `tool`, `run_id` and `project_id` by equality. (A Python estimate for the 91k-row sample: about 2.8M word postings for `text` and `data`, roughly 4 MB with delta-varint postings, about 13% of the raw size; indexing every column would add roughly another 1 MB.)
- **Word index** (per segment): a sorted directory of `(column, word ordinal)` entries with offsets, and one **delta-varint posting list per term** (ascending row numbers, each list decodable on its own). Queries: single word, AND of words (intersect, shortest first), exact phrase (AND, then verify the symbol sequence in the row heap). Single-character words are not indexed; queries on them scan.
- **Equality index** (per segment): a string cell is identified by its row-heap offset (identical strings share one), so equality on a string column is a posting list keyed by that offset; fixed-size columns are keyed by the cell value.
- **Time range, sorting, pagination:** a range filter prunes blocks by min/max and scans the rest; results come in insertion order or sorted by any fixed-size column, one page at a time. Rows are numbered by one global row number per dataframe (Q90): the segment is `row / segment_size` (the segment size is fixed in the manifest).
- **Word table:** to look a query word up in a sealed folder (which has no dictionary) the folder keeps `words.idx`: the ordinals sorted by word bytes (4 bytes per word, memory-mapped). A binary search over it finds a word's ordinal, and also gives prefix search. It is written when the folder is sealed.
- **Case-insensitive search** is optional per column (flag): a second word index keyed by `(column, xxh3_64(case-folded word))`, queries folded the same way (full Unicode case folding, `caseless` crate). Off by default.
- The active (unsealed) segment's index is kept in memory and written when the segment is sealed or the folder is committed; if an index file is missing or covers fewer rows than the segment, it is rebuilt from the row heap.
- A search returns a lazy iterator of row numbers; a helper hydrates a row by number.

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
- **Append-only files, manifest as commit point.** `commit()` appends new blocks to `heap.bin` and the active segment, then atomically replaces the manifest (write a temp file, then rename) with the new committed lengths. On open, anything past the committed lengths (a torn write) is truncated. Sealed segments (full) never change. An optional `auto_commit_bytes` triggers a commit automatically (default off).
- **Integrity:** every block has an xxh3-64 checksum, verified when read; a mismatch is `Corrupt`. A reader rejects a manifest with a newer format version (`UnsupportedVersion`).
- **Reuse within a period:** a folder is reopened for appending during its period, so restarts keep sharing one heap and dictionary (Q80). On rotation the caller seals the folder (writes `words.idx` and the final indexes, drops the dictionary) and starts the next one. If the dictionary fills first (`DictionaryFull`), the caller also starts the next folder.
- **Access:** a writable open takes an exclusive advisory lock (`fs4`); read-only opens (`Store::open_read_only`, used by the server) take a shared lock and map files read-only.
- **Rows are immutable**: no updates or deletes.
- **Creating a store:** `Store::in_memory()`, `Store::create(path)`, `Store::open(path)`, `Store::open_read_only(path)`, each with a `StoreOptions` variant (splitter, `auto_commit_bytes`, dictionary cap, whole-string map cap, zstd level, mmap on/off).

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
| 49 | Writers per dataframe? | Several threads allowed; each dataframe has an internal lock (`push_row(&self)`). |
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
| 71 | Multiple processes? | Exclusive advisory lock for writers, shared for readers (`fs4`). |
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
| 86 | Default thresholds? | Base64 run 64+, opaque run 24+, hex run 16+, segment 100,000 rows, column block 4,096 rows, row-heap block 64 KiB, zstd 19, dictionary entry cost 32 bytes. All settings, tuned after measuring. |
| 87 | Opaque run test? | Shannon entropy per character (4.0 bits default), not already in the dictionary. |
| 88 | UUID / hex ids? | UUIDs and long hex runs are stored decoded inline (tag 11 variants 4-7). |
| 89 | Search indexes? | Persisted per segment (logs are immutable); per-column index flags; memory-mapped for reads. |
| 90 | Row identity? | One global row number per dataframe; segment = row / segment size. |
| 91 | Stubs? | `main.rs` and the placeholder structs are deleted; `lib.rs` with the design's modules. |
| 92 | Harness? | `examples/` with dev-dependencies; corpus tests skip when the corpus is missing; `scratch/` is git-ignored. |
| 93 | mmap? | `memmap2` feature on by default but used only for index files; blocks use plain reads. |
| 94 | Review points? | Stop after milestone 1 (with compression numbers) and after milestone 3. |
| (note) | Folder = period | A folder covers a day or other period and is shared by all its dataframes; on rotation the dictionary is dropped, the heap and indexes stay. |

## 9. Open Questions
- Entropy thresholds (4.5 base64, 4.0 opaque) are guesses; calibrate on the corpus in milestone 1.
- Index size versus the 30% target: persisted indexes are an estimated 13-16% of raw on the corpus; measure in milestone 3 and consider flagging fewer columns if the target is missed.
- Thresholds to measure: base64 run length (64), opaque run length (24), segment size (100,000 rows), column block size (4,096 rows), row-heap block size (64 KiB), zstd level, per-entry dictionary cost estimate (32 bytes).
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
- Next: milestone 3 (segment and folder files, blocks, manifest), then review with you.
