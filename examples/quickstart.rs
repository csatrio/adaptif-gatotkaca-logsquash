//! The README quick start: write a day of logs into a folder, seal it, reopen read-only, search and hydrate.

use adaptif_gatotkaca_logsquash::{Column, ColumnType, OrderBy, OwnedValue, Page, Query, Schema, Store, StoreOptions, Value};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = std::env::temp_dir().join("logsquash-quickstart");
    let _ = std::fs::remove_dir_all(&dir);

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
        store.commit_if_due()?; // commits at `segment_rows` rows, `commit_policy.max_bytes` or `commit_policy.max_age`
    }
    store.commit()?;
    store.seal()?; // end of the period: the dictionary is dropped, indexes are finished
    drop(store);

    // Read: any number of readers, no lock.
    let store = Store::open_read_only(&dir, StoreOptions::default())?;
    let logs = store.frame("logs")?;
    let query = Query::new().equals(1, Value::Str("ERROR")).word_ci(2, "REFUSED");
    println!("{} matches", logs.count(&query)?);
    let page = logs.search_page(&query, &Page { order_by: Some(OrderBy { column: 0, descending: true }), offset: 0, limit: 5 })?;
    for row in page.rows {
        let values = logs.row(row)?;
        if let OwnedValue::Str(message) = &values[2] {
            println!("{row}: {message}");
        }
    }
    println!("{} bytes on disk", store.disk_bytes()?);
    std::fs::remove_dir_all(&dir)?;
    Ok(())
}
