# shortkit

US daily short-sale volume for Rust. Served from bundled parquet with on-demand fetch and a local cache. No API keys. Offline after the first query.

## Install

```toml
[dependencies]
shortkit = "0.1.0"
```

To track unreleased changes, depend on the repository directly:

```toml
shortkit = { git = "https://github.com/userFRM/shortkit" }
```

## Quick start

```rust,no_run
#[tokio::main]
async fn main() -> shortkit::Result<()> {
    // Short-volume history for one symbol, most recent date first.
    for r in shortkit::short_volume_for("AAPL").await?.iter().take(5) {
        println!("{} {} short of {} ({:.1}%)", r.date, r.short_volume, r.total_volume, r.short_pct * 100.0);
    }

    // The 10 most-shorted liquid names on a given day (total volume >= 100k).
    for r in shortkit::most_shorted(20260624, 10, 100_000).await? {
        println!("{} {:.1}%", r.symbol, r.short_pct * 100.0);
    }
    Ok(())
}
```

## Client pattern

```rust,no_run
use shortkit::Shortkit;

#[tokio::main]
async fn main() -> shortkit::Result<()> {
    let client = Shortkit::new();

    // Every symbol's short volume on one trade date, most-shorted first.
    let day = client.latest(20260624).await?;
    println!("{} symbols reported", day.len());

    // One symbol over a date range.
    let window = client.short_volume_range("NVDA", 20260601, 20260624).await?;
    println!("{} days", window.len());
    Ok(())
}
```

Blocking siblings (`short_volume_for_blocking`, `latest_blocking`, `most_shorted_blocking`) call the async methods from synchronous code and are safe inside any tokio runtime.

## What the numbers mean

Each row is one `(date, symbol)` observation from FINRA's consolidated NMS daily short-sale volume file. `short_volume` is shares sold short, `total_volume` is total reported volume, and `short_pct` is `short_volume / total_volume` (computed at ingest, `0.0` when total is zero). This is reported short-sale *volume*, not short *interest* (the bi-monthly settled open-short position); see Roadmap.

## CLI

```bash
shortkit-cli backfill --from 2018 --to 2026   # one FINRA file per trading day
shortkit-cli nightly-append                    # fetch missing days, merge by (date, symbol)
shortkit-cli manifest
shortkit-cli query --symbol AAPL
shortkit-cli query --date 20260624 --top 20 --min-volume 100000
```

## Data

Sourced from FINRA's public consolidated NMS daily short-sale volume files. One parquet file per year under `data/year=YYYY/shortvol-YYYY.parquet`, zstd-compressed, one row per `(date, symbol)`. `data/manifest.json` carries a SHA-256 digest per file. Dates are stored as `i32` `YYYYMMDD`.

Backfill walks every trading day in range and downloads that day's file; weekends and holidays return no file and are skipped. A weekday nightly job resumes from the last date present and appends the missing trading days, deduplicated by `(date, symbol)`. FINRA's CDN retains a rolling window of roughly the last eight years.

## Cache

Fetched parquet is cached on disk (XDG cache dir, e.g. `~/.cache/shortkit/`) with ETag revalidation, so repeat queries are offline. On a network failure the client serves the last good cached copy. Override the origin with `SHORTKIT_BASE_URL` and the cache location with `SHORTKIT_CACHE_DIR`.

## Roadmap

This release ships daily short-sale *volume*. FINRA's bi-monthly consolidated short *interest* (settled open-short positions, days-to-cover) is a planned second table with its own row shape; it is not yet included.

## API

Full API reference is on [docs.rs](https://docs.rs/shortkit).

## License

Dual-licensed under either of [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE) at your option.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md).
