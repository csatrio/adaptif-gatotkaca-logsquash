# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.2] - 2026-11-24

### Fixed

- A store kept every committed segment file open for as long as it was open, so a long-lived writer that commits often
  (or a cache of read-only stores) held thousands of file descriptors, one more per commit. Each store now keeps at
  most 64 segment files open, closes the least recently used one when it needs another and opens it again on its next
  read. Read results, durability and the per-segment block caches are unchanged. The pool lock is never held while a
  file or cache lock is, so it adds no lock-order cycle. A reader whose file was closed and has since been deleted or
  replaced now fails with `Error::Io` on its next read (before, an open descriptor could keep a deleted file readable
  on Unix); the store itself never removes a segment it is reading. Regression tests:
  `store::tests::many_segments_read_the_same_with_few_open_files`,
  `store::tests::concurrent_reads_and_commits_with_few_open_files` and the `segment::tests` of the file pool.

### Added

- `Store::max_open_segments`, `Store::set_max_open_segments` and `Store::open_segment_files`. The limit is a method,
  not a `StoreOptions` field, so that code building `StoreOptions` with every field listed keeps compiling.

## [0.1.1] - 2026-11-23

### Fixed

- Deadlock between a search and a concurrent `push_row` on a writable store. A search over the buffered (not yet
  committed) rows held the word-store lock and then locked the buffer once per cell, while `push_row` holds the buffer
  lock and then takes the word store for writing. Each thread waited on the other, and a later `commit` blocked behind
  both, so every read and write of the store hung. The buffer lock is now always taken before the word store: queries
  and indexes over the buffer read a buffer guard their caller already holds (`BufferSource` wraps the locked buffer,
  not the dataframe). This also removes a recursive read lock of the buffer. Regression test:
  `store::tests::concurrent_push_search_and_commit_do_not_deadlock`.

## [0.1.0] - 2026-11-21

### Added

- README.md, AGENTS.md, licences (MIT OR Apache-2.0) and package metadata.
- Word-level dedup, tagged varint symbols, inline base64/UUID/hex/opaque runs, typed and date/time columns.
- On-disk store (manifest, heap, compressed column-major segments), persisted optional indexes, queries and paging.
- Date/time text parsing and formatting; criterion benchmarks and tuning examples.
