//! `shortkit` — US daily short-sale volume for Rust.
//!
//! Fetches year-partitioned parquet files on demand from GitHub raw, caches
//! them locally with ETag revalidation, and falls back to stale cache on
//! network errors. No API keys. Offline after the first successful fetch of
//! each year file.
//!
//! Data comes from FINRA's public consolidated NMS daily short-sale volume
//! files. Each row is one `(date, symbol)` observation; dates are `i32`
//! `YYYYMMDD`.
//!
//! # Quick start — free functions
//!
//! ```no_run
//! use shortkit::short_volume_for;
//!
//! #[tokio::main]
//! async fn main() -> shortkit::Result<()> {
//!     for r in short_volume_for("AAPL").await?.iter().take(5) {
//!         println!("{} {} short of {} ({:.1}%)", r.date, r.short_volume, r.total_volume, r.short_pct * 100.0);
//!     }
//!     Ok(())
//! }
//! ```
//!
//! For connection-pool reuse across many lookups, create a [`Shortkit`] client
//! once and call its methods instead of the free functions.
//!
//! # Environment overrides
//!
//! | Variable | Effect |
//! |---|---|
//! | `SHORTKIT_BASE_URL` | Replace the GitHub raw origin URL |
//! | `SHORTKIT_CACHE_DIR` | Override `~/.cache/shortkit/` |
//! | `SHORTKIT_MIRROR_URL` | Override the jsDelivr CDN mirror |
#![forbid(unsafe_code)]

mod error;
pub use error::{Error, Result};

mod record;
pub use record::ShortVol;

pub mod parquet_io;
pub use parquet_io::{read_shortvol, write_shortvol};

mod fetcher;

mod client;
pub use client::{latest, most_shorted, short_volume_for, Shortkit};
