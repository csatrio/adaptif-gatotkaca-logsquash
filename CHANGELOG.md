# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.1] - 2026-11-27

### Fixed

- Dropping a writable store and reopening it at once could fail with `Error::Locked` while another thread of the
  process was spawning a child process: the child shared the lock file until it executed. The writer lock is now
  unlocked explicitly when the store is dropped (only by the process that took it), so the reopen always succeeds.

## [0.2.0] - 2026-11-27

Version 2: the pending buffer in memory-mapped files, live readers and a commit policy (DESIGN.md section 14).

### Added

- `PendingMode` (`StoreOptions::pending`). With `Mapped`, the default, the rows pushed since the last commit live in
  append-only memory-mapped files instead of the writer's memory. They survive a crash or kill of the writer process
  (not a power loss or kernel crash: there is no `msync`), and the next `Store::open` continues with them without
  committing. `Memory` is the 0.1 behaviour, and the only mode without the `mmap` feature.
- Live readers: a store opened with `Store::open_read_only` sees and searches the pending rows of a `Mapped` writer in
  another process, and picks up its commits, new dataframes and the seal by itself at the start of each read or query.
- `Store::commit_if_due` with `StoreOptions::commit_policy` (`CommitPolicy { max_bytes, max_age }`): commits when a
  dataframe has `segment_rows` pending rows, the pending rows take `max_bytes` (256 MiB by default) or the oldest one is
  `max_age` old (no limit by default; the age is kept across restarts). Returns the `CommitTrigger`.
- `Store::recovery` (`RecoveryReport`: pending rows found at open, pending files missing), `Store::pending_bytes`,
  `Frame::pending_bytes`.
- `StoreOptions::pending_dir` (for example `/dev/shm`), `pending_chunk_min_bytes`, `pending_chunk_max_bytes`.

### Changed

- **Breaking:** new public fields in `StoreOptions` (code that lists every field must add them or use
  `..StoreOptions::default()`).
- **Breaking (format):** the manifest is format 2 (it names the epoch of the pending buffer, a store id and the pending
  directory). 0.2 reads 0.1 folders and upgrades them at the first writable open; 0.1 rejects 0.2 folders with
  `Error::UnsupportedVersion`. Segments, indexes, `heap.bin` and `words.idx` are unchanged, and a `Mapped` commit writes
  them byte for byte as a `Memory` commit of the same rows.
- `Store::create_frame` writes the manifest at once, so readers see a new dataframe before its first commit.
- Reads and searches work on a snapshot taken when they start: a search no longer skips or repeats rows when a commit
  happens while its iterator is consumed. `Frame::len` and `Frame::pending_rows` of a read-only store include the
  writer's pending rows.
- Opening a store with another `pending` mode or `pending_dir` than it uses commits its pending rows, then switches.
- `push_row` into a store is about 25% slower with the mapped buffer (4.0 µs instead of 3.2 µs per row in the benchmark);
  `PendingMode::Memory` is as fast as before.

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
