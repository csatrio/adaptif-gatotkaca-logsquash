# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- README.md, AGENTS.md, licences (MIT OR Apache-2.0) and package metadata.

## [0.1.0]

### Added

- Word-level dedup, tagged varint symbols, inline base64/UUID/hex/opaque runs, typed and date/time columns.
- On-disk store (manifest, heap, compressed column-major segments), persisted optional indexes, queries and paging.
- Date/time text parsing and formatting; criterion benchmarks and tuning examples.
