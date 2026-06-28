<!-- Canonical CHANGELOG header for every *kit. The body keeps each kit's real
release history; only this top block is standardized. -->
# Changelog

All notable changes to shortkit are documented here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
This project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0]

Initial release.

- Async `Shortkit` client plus blocking siblings and one-shot free functions.
- Query surface: `short_volume_for`, `short_volume_range`, `latest`, `most_shorted`.
- Bundled per-year parquet (`data/year=YYYY/shortvol-YYYY.parquet`) served from GitHub raw with on-demand fetch, ETag revalidation, SHA-256 manifest verification, and a CDN mirror plus stale-cache fallback.
- `shortkit-cli` with `backfill` (one FINRA consolidated NMS file per trading day), `nightly-append` (resume from the last date present, merged and deduplicated by `(date, symbol)`), `manifest`, and `query`.
