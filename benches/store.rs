//! Benchmarks on synthetic logs (no corpus needed): tokenizing, inserting, committing, opening, hydrating rows and
//! searching. Run with `cargo bench` (add `-- --quick` for a fast pass). For compression ratios on real logs use the
//! examples (`tune_corpus`, `search_corpus`).

use adaptif_gatotkaca_logsquash::{
    Column, ColumnType, DataFrame, DefaultSplitter, Dictionary, MemHeap, OrderBy, Page, Query, Schema, Store, StoreOptions, Tokenizer,
    TokenizerOptions, Value, WordStore,
};
use bytes::BytesMut;
use criterion::{BatchSize, Criterion, Throughput, criterion_group, criterion_main};
use std::hint::black_box;
use std::path::Path;
use std::sync::OnceLock;

const ROWS: u64 = 100_000;

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        self.0 >> 33
    }
}

const LEVELS: [&str; 4] = ["INFO", "INFO", "WARN", "ERROR"];
const VERBS: [&str; 8] = ["fetched", "stored", "rejected", "retried", "compiled", "queued", "dropped", "resolved"];
const NOUNS: [&str; 8] = ["request", "session", "bundle", "message", "record", "token", "query", "snapshot"];
const HOSTS: [&str; 5] = ["web-1", "web-2", "worker-a", "worker-b", "db-main"];

/// One synthetic log row: (level, message, JSON data, id, timestamp text).
fn row(i: u64, rng: &mut Rng) -> (String, String, String, i64, String) {
    let level = LEVELS[rng.next() as usize % LEVELS.len()];
    let msg = format!(
        "{} {} {} on {} in {}ms (attempt {}) user{}",
        VERBS[rng.next() as usize % 8],
        NOUNS[rng.next() as usize % 8],
        rng.next() % 5000,
        HOSTS[rng.next() as usize % 5],
        rng.next() % 900,
        rng.next() % 4,
        rng.next() % 300
    );
    let uuid = format!(
        "{:08x}-{:04x}-{:04x}-{:04x}-{:012x}",
        rng.next() as u32,
        rng.next() as u16,
        rng.next() as u16,
        rng.next() as u16,
        rng.next()
    );
    let payload = if i % 50 == 0 { format!(",\"blob\":\"{}\"", "QUJDREVGR0hJSktMTU5PUFFSU1RVVldYWVo".repeat(40)) } else { String::new() };
    let data =
        format!("{{\"uuid\":\"{uuid}\",\"tokens\":{},\"note\":\"line one\\nline two {}\"{payload}}}", rng.next() % 4000, rng.next() % 20);
    let ts = format!("2026-11-18T{:02}:{:02}:{:02}.{:03}Z", (i / 3600) % 24, (i / 60) % 60, i % 60, rng.next() % 1000);
    (level.to_owned(), msg, data, i as i64, ts)
}

fn schema() -> Schema {
    Schema::new(vec![
        Column::new("id", ColumnType::I64).index_equality(),
        Column::new("ts", ColumnType::TimestampMs),
        Column::new("level", ColumnType::String).index_equality(),
        Column::new("msg", ColumnType::String).index_words().case_insensitive(),
        Column::new("data", ColumnType::String).json_escapes().index_words(),
    ])
    .unwrap()
}

fn fill(store: &Store, rows: u64) {
    let frame = store.create_frame("logs", schema()).unwrap();
    let mut rng = Rng(7);
    for i in 0..rows {
        let (level, msg, data, id, ts) = row(i, &mut rng);
        frame.push_row(&[Value::I64(id), Value::Str(&ts), Value::Str(&level), Value::Str(&msg), Value::Str(&data)]).unwrap();
        if i % 1000 == 999 {
            store.commit_if_full().unwrap();
        }
    }
    store.commit().unwrap();
}

fn options() -> StoreOptions {
    StoreOptions { zstd_level: 3, ..StoreOptions::default() }
}

/// A committed store of `ROWS` rows, built once and shared by the read benchmarks.
fn committed() -> &'static Path {
    static DIR: OnceLock<tempfile::TempDir> = OnceLock::new();
    DIR.get_or_init(|| {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::create(dir.path(), options()).unwrap();
        fill(&store, ROWS);
        store.seal().unwrap();
        dir
    })
    .path()
}

fn tokenizing(c: &mut Criterion) {
    let words = WordStore::<MemHeap>::new(MemHeap::new(), Dictionary::new(64 << 20)).into_shared();
    let tokenizer = Tokenizer::new(words, TokenizerOptions::default());
    let mut rng = Rng(1);
    let lines: Vec<String> = (0..1000).map(|i| row(i, &mut rng).2).collect();
    let bytes: usize = lines.iter().map(String::len).sum();
    let mut group = c.benchmark_group("tokenizer");
    group.throughput(Throughput::Bytes(bytes as u64));
    let mut out = BytesMut::new();
    group.bench_function("encode 1000 JSON strings", |b| {
        b.iter(|| {
            for line in &lines {
                out.clear();
                tokenizer.encode(line, &DefaultSplitter, &mut out).unwrap();
            }
            black_box(out.len())
        })
    });
    group.finish();
}

fn inserting(c: &mut Criterion) {
    let mut rng = Rng(3);
    let rows: Vec<_> = (0..1000).map(|i| row(i, &mut rng)).collect();
    let mut group = c.benchmark_group("dataframe");
    group.throughput(Throughput::Elements(rows.len() as u64));
    group.bench_function("push_row x1000 (in memory)", |b| {
        b.iter_batched(
            || DataFrame::new(schema(), WordStore::<MemHeap>::new(MemHeap::new(), Dictionary::new(64 << 20)).into_shared()),
            |frame| {
                for (level, msg, data, id, ts) in &rows {
                    frame.push_row(&[Value::I64(*id), Value::Str(ts), Value::Str(level), Value::Str(msg), Value::Str(data)]).unwrap();
                }
                frame
            },
            BatchSize::LargeInput,
        )
    });
    group.finish();
}

fn writing(c: &mut Criterion) {
    let mut group = c.benchmark_group("store");
    group.sample_size(10);
    group.bench_function("commit 20k rows with indexes (zstd 3)", |b| {
        b.iter_batched(
            || {
                let dir = tempfile::tempdir().unwrap();
                let store = Store::create(dir.path(), options()).unwrap();
                let frame = store.create_frame("logs", schema()).unwrap();
                let mut rng = Rng(5);
                for i in 0..20_000 {
                    let (level, msg, data, id, ts) = row(i, &mut rng);
                    frame.push_row(&[Value::I64(id), Value::Str(&ts), Value::Str(&level), Value::Str(&msg), Value::Str(&data)]).unwrap();
                }
                (dir, store)
            },
            |(dir, store)| {
                store.commit().unwrap();
                (dir, store)
            },
            BatchSize::PerIteration,
        )
    });
    group.bench_function("build_indexes (4 segments)", |b| {
        b.iter_batched(
            || {
                let dir = tempfile::tempdir().unwrap();
                let opts = StoreOptions { indexing: false, segment_rows: 5000, ..options() };
                let store = Store::create(dir.path(), opts).unwrap();
                fill(&store, 20_000);
                store.seal().unwrap();
                dir
            },
            |dir| {
                black_box(Store::build_indexes(dir.path(), options()).unwrap());
                dir
            },
            BatchSize::PerIteration,
        )
    });
    group.finish();
}

fn reading(c: &mut Criterion) {
    let dir = committed();
    let mut group = c.benchmark_group("read");
    group.bench_function("open_read_only (100k rows)", |b| b.iter(|| black_box(Store::open_read_only(dir, options()).unwrap())));

    let store = Store::open_read_only(dir, options()).unwrap();
    let frame = store.frame("logs").unwrap();
    let mut rng = Rng(11);
    group.bench_function("hydrate one random row", |b| b.iter(|| black_box(frame.row(rng.next() % ROWS).unwrap())));
    group.throughput(Throughput::Elements(100));
    group.bench_function("hydrate 100 consecutive rows", |b| {
        b.iter(|| {
            let start = rng.next() % (ROWS - 100);
            for row in start..start + 100 {
                black_box(frame.row(row).unwrap());
            }
        })
    });
    group.finish();

    let mut group = c.benchmark_group("search");
    let queries: Vec<(&str, Query)> = vec![
        ("word 'rejected' in msg", Query::new().word(3, "rejected")),
        ("word, case-insensitive", Query::new().word_ci(3, "REJECTED")),
        ("phrase 'in 5'", Query::new().phrase(3, "attempt 3")),
        ("equals level = ERROR", Query::new().equals(2, Value::Str("ERROR"))),
        (
            "word + equals + range",
            Query::new().word(3, "queued").equals(2, Value::Str("WARN")).range(0, Some(Value::I64(20_000)), Some(Value::I64(60_000))),
        ),
        (
            "time range (text bounds)",
            Query::new().range(1, Some(Value::Str("2026-11-18T05:00:00Z")), Some(Value::Str("2026-11-18T06:00:00Z"))),
        ),
        ("word in JSON 'line'", Query::new().word(4, "line")),
        ("single char scan ':'", Query::new().word(3, ":")),
    ];
    for (name, query) in &queries {
        group.bench_function(*name, |b| b.iter(|| black_box(frame.count(query).unwrap())));
    }
    let page = Page { order_by: Some(OrderBy { column: 0, descending: true }), offset: 0, limit: 50 };
    let errors = Query::new().equals(2, Value::Str("ERROR"));
    group.bench_function("page of 50, sorted by id", |b| b.iter(|| black_box(frame.search_page(&errors, &page).unwrap())));
    group.bench_function("prefix suggestions 'rej'", |b| b.iter(|| black_box(store.words_with_prefix("rej", 10))));
    group.finish();
}

criterion_group!(benches, tokenizing, inserting, writing, reading);
criterion_main!(benches);
