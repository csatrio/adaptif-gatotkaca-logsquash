# Open Questions and Ambiguities (Q&A)

> **Status: answered (Q29-Q59).** The decisions are recorded in the Decisions Log of [DESIGN.md](DESIGN.md) (rows 29-59). This file is kept as the record of the options considered.

Every question still open in [DESIGN.md](DESIGN.md), plus ambiguities found on a re-read. Each has options and a **recommendation**. Fill in `Answer:` (or just write "accept" to take the recommendation); once answered, the decision moves into DESIGN.md's Decisions Log.

Numbering continues from the Decisions Log (1-28).

---

## A. Types and API

**Q29. What does the `Value` enum look like?**
- a) One variant per type code (`I8`, `I16`, ... `TimestampMs`, `Date`, ...): exact, large enum.
- b) Grouped variants: `Int(i64)`, `UInt(u64)`, `Float(f64)`, `Bool`, `Char`, `Time(i64)` ... and the column's type code narrows/validates on insert.
- c) a) for fixed-size types, plus `Str(&str)`, `Blob(&[u8])`, `Null`, with a borrowed lifetime on the input form and an owned form for hydration.
- **Recommendation: c).** Exact types, zero-copy on insert, owned on read.
- Answer:

**Q30. Error type?**
- a) One `Error` enum (`HeapFull`, `TypeMismatch`, `ColumnCount`, `DateParse`, `Io`, `Corrupt`, ...) implementing `std::error::Error`, hand-written.
- b) Same, using the `thiserror` crate.
- **Recommendation: a)** (no extra dependency).
- Answer:

**Q31. Overflow when an insert value does not fit the column type** (e.g. `I64(300)` into an `i8` column, or an `f64` into an `f32` column)?
- a) Error.  b) Saturate/truncate silently.
- **Recommendation: a).**
- Answer:

**Q32. Float edge cases with the order-preserving encoding:** NaN payloads, `-0.0` vs `0.0`.
- a) Canonicalise NaN to one quiet NaN; keep `-0.0` distinct (it sorts just below `0.0`).
- b) Preserve exact bits (lossless), accept that NaNs sort at the ends by sign.
- **Recommendation: b)**, since the library is a lossless log store.
- Answer:

**Q33. Invalid UTF-8 in input.** Logs often contain stray bytes, but the API takes `&str`.
- a) Callers must supply valid UTF-8 (they may use `String::from_utf8_lossy` themselves).
- b) Provide a helper that converts lossily (U+FFFD) before tokenizing.
- c) Store such values as `Blob` columns instead.
- **Recommendation: a) plus c)** as the documented path for non-UTF-8 data.
- Answer:

---

## B. Tokenizer and dedup

**Q34. Maximum word length.** An alphanumeric run can be huge (base64 payloads, hex dumps). It would fill the heap with one-off words.
- a) No limit (up to 4 GB).
- b) Words longer than N bytes (e.g. 64) are split into chunks of N characters, each chunk deduplicated as its own word.
- c) Words longer than N bytes are stored as a `Blob`-like inline entry in the row heap, not in the shared heap.
- **Recommendation: b)** with N = 64; keeps the tokenizer lossless and the heap small.
- Answer:

**Q35. High-cardinality words** (numeric IDs, hex, timestamps inside messages) are unique per row, so they bloat the heap and dictionary with no dedup gain.
- a) Accept it; dedup is best-effort.
- b) Digit-only runs are split into fixed-size chunks (e.g. 4 digits) so repeated digit groups deduplicate.
- c) Provide a pluggable splitter trait with the default from 4.1.
- **Recommendation: c)** (default = 4.1), so this can be tuned without a format change.
- Answer:

**Q36. Dedup of whole strings.** Log messages often repeat exactly. Each repeat currently writes a new `[count][Symbols]` entry in the row heap.
- a) No whole-string dedup.
- b) Hash the whole Symbol array and reuse an existing row-heap entry (per-dataframe map).
- **Recommendation: b)**: cheap and gives a large win on repetitive logs. Costs one more map per dataframe.
- Answer:

**Q37. Case sensitivity of dedup and search.** "Error" and "error" are different words.
- a) Case-sensitive everywhere.
- b) Case-sensitive storage, but an optional case-insensitive search (lowercase the query and match against a lowercased index).
- **Recommendation: a)** for v1.
- Answer:

---

## C. Search

**Q38. Search scope.** Is the inverted index per column or per dataframe?
- a) Per dataframe (any column).
- b) Per column (`column, Symbol -> rows`), so a query can be limited to e.g. the `message` column.
- **Recommendation: b)** with key `(column index, Symbol)`.
- Answer:

**Q39. What queries exist?**
- a) Single word -> row ids.
- b) a) plus AND of several words (postings intersection).
- c) b) plus exact phrase (words adjacent, in order), which needs word positions or a check against the row heap.
- **Recommendation: c)**, using the row heap Symbol array to verify phrases after intersecting postings.
- Answer:

**Q40. Query word of one character** (stored as an s-box value) such as `:` or `a` would match nearly every row.
- a) Allowed, returns huge postings.
- b) Single-character words are not indexed; queries on them fall back to a scan.
- **Recommendation: b)**: saves memory and these tokens are useless for selective search.
- Answer:

**Q41. Postings format and row-id width.**
- a) `Vec<u32>` row ids (max ~4.29 billion rows per dataframe).
- b) `Vec<u64>`.
- c) Delta-encoded varints / roaring bitmap.
- **Recommendation: a)** for v1 (rows are appended in order, so postings stay sorted for free); revisit compression after measuring.
- Answer:

**Q42. Index rebuild on load.** Rebuilding scans the whole row heap.
- a) Accept it (simple).
- b) Persist the index in the file too.
- **Recommendation: a)** for v1, with the file format reserving a section id for a persisted index later.
- Answer:

---

## D. Storage and persistence

**Q43. Is the mmap-backed heap read-only?**
- a) Mmap is read-only (for loaded stores). New words go to an in-memory `BytesMut` overlay, which is merged on save.
- b) Mmap is a growable mapped file that can accept appends (remap on growth).
- **Recommendation: a)** (simpler and safe on Windows, where remapping a growing file is awkward). Tokens stay valid because the overlay continues the same offset space.
- Answer:

**Q44. Updates and deletes.** Everything is append-only.
- a) No row updates or deletes, ever (logs are immutable).
- b) Deletion marker per row (tombstone bit), space reclaimed by an offline compaction.
- **Recommendation: a)**.
- Answer:

**Q45. Saving an existing store.**
- a) Always rewrite the whole file (write to a temp file, then atomic rename).
- b) Append-only incremental save (new heap bytes, new dictionary entries, new rows).
- **Recommendation: a)** for v1: crash-safe and simple.
- Answer:

**Q46. Integrity.** Should the file carry checksums?
- a) None.
- b) An xxh3-64 checksum per section, verified on load.
- **Recommendation: b)**: we already depend on xxh3 and it catches truncated or corrupt files.
- Answer:

**Q47. Version compatibility.** What does a reader do with a newer format version?
- a) Reject with an error.
- b) Read if the major version matches.
- **Recommendation: a)** until the format has stabilised.
- Answer:

**Q48. Dictionary on disk.** Persist `(hash, Token)` pairs (loaded straight into the `HashMap`) or rebuild the Dictionary by re-hashing words from the heap?
- Rebuilding needs word boundaries, which the heap does not record, so it is not possible from the heap alone.
- **Recommendation: persist the pairs.** (Included for confirmation because section 7 already says this.)
- Answer:

---

## E. Concurrency

**Q49. Number of writers per dataframe.**
- a) One writer per dataframe (`&mut self` on `push_row`); many dataframes may write concurrently through the shared `Arc<Store>`.
- b) Multiple threads may push to the same dataframe (needs an internal lock).
- **Recommendation: a).**
- Answer:

**Q50. Write-lock contention on the shared store.** Every new word takes the global write lock.
- a) Accept it for v1.
- b) Each tokenizer batches: it resolves all words of a row under one read-lock pass, collects the misses, then takes the write lock once per row.
- **Recommendation: b)**, which is cheap to implement and keeps lock acquisitions to at most two per row.
- Answer:

---

## F. Date and time details

**Q51. Format names in the header.** Built-in formats are named `rfc2822`, `clf`; anything else is a strftime pattern. How do we avoid ambiguity if a pattern literally equals `clf`?
- a) Built-in names are reserved; a pattern must contain at least one `%`.
- b) Prefix scheme: `builtin:clf`, `strftime:%Y...`.
- **Recommendation: b)**; explicit and extensible.
- Answer:

**Q52. Formatting (cell -> text) precision.** ISO output may include fractional seconds.
- a) Output precision follows the column's unit (sec -> none, ms -> 3 digits, us -> 6, ns -> 9).
- b) Always the shortest exact form.
- **Recommendation: a).**
- Answer:

**Q53. `TimestampTz` text without an offset** (UTC is assumed per Q18): is the offset stored as `+00:00`? And when text has an offset, is it preserved exactly on round-trip?
- **Recommendation:** yes: stored as `0` minutes when absent, preserved exactly (in whole minutes) otherwise. Offsets with seconds are an error.
- Answer:

**Q54. Out-of-range dates.** Values outside what `chrono` or the 48-bit `TimestampTz` range supports.
- a) Error on insert.  b) Clamp.
- **Recommendation: a).**
- Answer:

**Q55. Leap seconds** (`23:59:60`).
- a) Reject.  b) Accept and map to `23:59:59.999999999` (lossy).
- **Recommendation: a).**
- Answer:

---

## G. Project and quality

**Q56. Minimum Rust version and edition.** `Cargo.toml` uses edition 2024.
- **Recommendation:** keep edition 2024; no separate MSRV promise until first release.
- Answer:

**Q57. Testing and benchmarks.**
- a) Unit tests per module plus property tests (`proptest`) for the round trip (`hydrate(insert(x)) == x`) and for splitter losslessness.
- b) Add `criterion` benchmarks: compression ratio and throughput on a sample log corpus.
- **Recommendation: a) and b)**; the round-trip property is the key correctness guarantee, and compression ratio is the point of the project.
- Answer:

**Q58. Target success metric.** What compression ratio or throughput counts as "good enough"? (e.g. file size <= 30% of the raw text, 500 MB/s insert on one core.)
- Answer:

**Q59. Sample data.** Which real log format(s) should we test against (nginx access, syslog, JSON lines, app logs)? There is no corpus in the repo yet.
- Answer:

---

## Summary table

| Area | Questions |
|------|-----------|
| Types and API | Q29-Q33 |
| Tokenizer and dedup | Q34-Q37 |
| Search | Q38-Q42 |
| Storage and persistence | Q43-Q48 |
| Concurrency | Q49-Q50 |
| Date and time | Q51-Q55 |
| Project and quality | Q56-Q59 |
