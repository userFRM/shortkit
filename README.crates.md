# shortkit

US daily short-sale volume for Rust.

```toml
[dependencies]
shortkit = "0.1.0"
```

```rust,no_run
#[tokio::main]
async fn main() -> shortkit::Result<()> {
    for r in shortkit::short_volume_for("AAPL").await?.iter().take(5) {
        println!("{} {} short of {} ({:.1}%)", r.date, r.short_volume, r.total_volume, r.short_pct * 100.0);
    }
    Ok(())
}
```

Full documentation: <https://github.com/userFRM/shortkit>

Licensed under MIT OR Apache-2.0.
