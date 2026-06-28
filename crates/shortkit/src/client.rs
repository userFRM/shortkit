//! Stateful `Shortkit` client — async short-volume endpoints with blocking
//! wrappers.
//!
//! Fetches year-partitioned parquet shards from GitHub raw (or a configurable
//! origin) with ETag-aware caching, SHA-256 manifest verification, and CDN
//! mirror fallback. Falls back to stale cache on transient network failures.
//!
//! # Quick start — free functions
//!
//! ```no_run
//! use shortkit::short_volume_for;
//!
//! #[tokio::main]
//! async fn main() -> shortkit::Result<()> {
//!     for r in short_volume_for("AAPL").await?.iter().take(5) {
//!         println!("{} short {} / {} ({:.1}%)", r.date, r.short_volume, r.total_volume, r.short_pct * 100.0);
//!     }
//!     Ok(())
//! }
//! ```
//!
//! # Client pattern (reuse across calls)
//!
//! ```no_run
//! use shortkit::Shortkit;
//!
//! #[tokio::main]
//! async fn main() -> shortkit::Result<()> {
//!     let client = Shortkit::new();
//!     let top = client.most_shorted(20260624, 10, 100_000).await?;
//!     for r in &top {
//!         println!("{} {:.1}%", r.symbol, r.short_pct * 100.0);
//!     }
//!     Ok(())
//! }
//! ```

use std::path::PathBuf;

use crate::error::{Error, Result};
use crate::fetcher::{default_cache_dir, resolved_base_url, CachedFetcher};
use crate::parquet_io::read_shortvol;
use crate::record::ShortVol;

/// Stateful shortkit client.
///
/// Wraps an ETag-aware cached fetcher and exposes flat async query methods.
/// Create once and reuse; the internal reqwest client is kept alive for
/// connection pooling.
///
/// ```no_run
/// use shortkit::Shortkit;
/// use std::path::PathBuf;
///
/// let client = Shortkit::new()
///     .with_base_url("https://my-mirror.example.com/shortkit")
///     .with_cache_dir(PathBuf::from("/tmp/shortkit-test"));
/// ```
#[derive(Clone)]
pub struct Shortkit {
    fetcher: CachedFetcher,
}

impl Shortkit {
    /// Create a client with the default GitHub raw backend and XDG cache.
    ///
    /// Reads `SHORTKIT_BASE_URL` and `SHORTKIT_CACHE_DIR` from the environment
    /// if set. **This function never fails.** Errors are deferred to the first
    /// fetch.
    pub fn new() -> Self {
        let http = reqwest::Client::builder()
            .user_agent("shortkit/0.1 (+https://github.com/userFRM/shortkit)")
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .unwrap_or_else(|_| reqwest::Client::new());
        Self {
            fetcher: CachedFetcher::new(http, resolved_base_url(), default_cache_dir()),
        }
    }

    /// Override the origin URL.
    pub fn with_base_url(mut self, url: impl Into<String>) -> Self {
        self.fetcher.set_base_url(url.into());
        self
    }

    /// Override the on-disk cache directory.
    pub fn with_cache_dir(mut self, dir: PathBuf) -> Self {
        self.fetcher.set_cache_dir(dir);
        self
    }

    /// Override the CDN mirror URL. `None` disables mirror fallback.
    pub fn with_mirror_url(mut self, url: Option<String>) -> Self {
        self.fetcher.set_mirror_url(url);
        self
    }

    // ── Async query endpoints ───────────────────────────────────────────────

    /// Every short-volume row for a `symbol` (case-insensitive), most recent
    /// date first.
    pub async fn short_volume_for(&self, symbol: &str) -> Result<Vec<ShortVol>> {
        let rows = self.load_all_rows().await?;
        Ok(sort_desc(
            rows.into_iter()
                .filter(|r| r.symbol.eq_ignore_ascii_case(symbol))
                .collect(),
        ))
    }

    /// Short-volume rows for a `symbol` whose date is within `[from, to]`
    /// inclusive (`YYYYMMDD`), most recent first.
    pub async fn short_volume_range(
        &self,
        symbol: &str,
        from: i32,
        to: i32,
    ) -> Result<Vec<ShortVol>> {
        Ok(self
            .short_volume_for(symbol)
            .await?
            .into_iter()
            .filter(|r| r.date >= from && r.date <= to)
            .collect())
    }

    /// All symbols' rows for a single trade `date` (`YYYYMMDD`), descending by
    /// short percentage.
    pub async fn latest(&self, date: i32) -> Result<Vec<ShortVol>> {
        let mut rows: Vec<ShortVol> = self
            .load_all_rows()
            .await?
            .into_iter()
            .filter(|r| r.date == date)
            .collect();
        rows.sort_by(|a, b| b.short_pct.total_cmp(&a.short_pct));
        Ok(rows)
    }

    /// The `n` most-shorted symbols on `date` by `short_pct`, restricted to rows
    /// whose `total_volume >= min_volume` (a liquidity floor that keeps thinly
    /// traded names from dominating on noise). Descending by `short_pct`.
    pub async fn most_shorted(
        &self,
        date: i32,
        n: usize,
        min_volume: i64,
    ) -> Result<Vec<ShortVol>> {
        let mut rows: Vec<ShortVol> = self
            .load_all_rows()
            .await?
            .into_iter()
            .filter(|r| r.date == date && r.total_volume >= min_volume)
            .collect();
        rows.sort_by(|a, b| b.short_pct.total_cmp(&a.short_pct));
        rows.truncate(n);
        Ok(rows)
    }

    // ── Blocking wrappers ───────────────────────────────────────────────────

    /// Blocking variant of [`short_volume_for`](Self::short_volume_for).
    pub fn short_volume_for_blocking(&self, symbol: &str) -> Result<Vec<ShortVol>> {
        let c = self.clone();
        let s = symbol.to_owned();
        block(async move { c.short_volume_for(&s).await })
    }

    /// Blocking variant of [`latest`](Self::latest).
    pub fn latest_blocking(&self, date: i32) -> Result<Vec<ShortVol>> {
        let c = self.clone();
        block(async move { c.latest(date).await })
    }

    /// Blocking variant of [`most_shorted`](Self::most_shorted).
    pub fn most_shorted_blocking(
        &self,
        date: i32,
        n: usize,
        min_volume: i64,
    ) -> Result<Vec<ShortVol>> {
        let c = self.clone();
        block(async move { c.most_shorted(date, n, min_volume).await })
    }

    // ── Internal ─────────────────────────────────────────────────────────────

    /// Fetch every `shortvol-YYYY.parquet` shard listed in the manifest and
    /// flat-concatenate the rows.
    pub(crate) async fn load_all_rows(&self) -> Result<Vec<ShortVol>> {
        let keys = self.discover_shards().await?;
        let mut all = Vec::new();
        for key in keys {
            let bytes = self.fetcher.fetch(&key).await?;
            all.extend(read_shortvol(&bytes)?);
        }
        Ok(all)
    }

    /// Fetch `manifest.json` and return sorted shard keys (without `.parquet`).
    async fn discover_shards(&self) -> Result<Vec<String>> {
        let url = format!("{}/manifest.json", self.fetcher.base_url);
        let resp = self
            .fetcher
            .http
            .get(&url)
            .send()
            .await
            .map_err(Error::Http)?;
        if !resp.status().is_success() {
            return Err(Error::Other(format!(
                "manifest.json: HTTP {} {}",
                resp.status().as_u16(),
                resp.status().canonical_reason().unwrap_or("")
            )));
        }
        let manifest: serde_json::Value = resp.json().await.map_err(Error::Http)?;
        let obj = manifest
            .as_object()
            .ok_or_else(|| Error::Other("manifest.json is not a JSON object".into()))?;
        let mut keys: Vec<String> = obj
            .keys()
            .filter(|k| is_shortvol_shard(k))
            .map(|k| k.trim_end_matches(".parquet").to_string())
            .collect();
        keys.sort();
        Ok(keys)
    }
}

impl Default for Shortkit {
    fn default() -> Self {
        Self::new()
    }
}

fn sort_desc(mut rows: Vec<ShortVol>) -> Vec<ShortVol> {
    rows.sort_by_key(|r| std::cmp::Reverse(r.date));
    rows
}

/// Return `true` for filenames matching `shortvol-YYYY.parquet`.
fn is_shortvol_shard(name: &str) -> bool {
    let Some(rest) = name.strip_prefix("shortvol-") else {
        return false;
    };
    let Some(year) = rest.strip_suffix(".parquet") else {
        return false;
    };
    !year.is_empty() && year.bytes().all(|b| b.is_ascii_digit())
}

// ---------------------------------------------------------------------------
// Free functions
// ---------------------------------------------------------------------------

/// Every short-volume row for `symbol`, one-shot client.
pub async fn short_volume_for(symbol: &str) -> Result<Vec<ShortVol>> {
    Shortkit::new().short_volume_for(symbol).await
}

/// All symbols' rows for a single trade `date`, one-shot client.
pub async fn latest(date: i32) -> Result<Vec<ShortVol>> {
    Shortkit::new().latest(date).await
}

/// The `n` most-shorted symbols on `date` (volume floor `min_volume`), one-shot.
pub async fn most_shorted(date: i32, n: usize, min_volume: i64) -> Result<Vec<ShortVol>> {
    Shortkit::new().most_shorted(date, n, min_volume).await
}

// ---------------------------------------------------------------------------
// Blocking helper
// ---------------------------------------------------------------------------

/// Drive a future to completion from any context (sync or async).
///
/// - Inside a tokio **multi-thread** runtime: `block_in_place` + `block_on`.
/// - Inside a **current-thread** runtime or no runtime: the future is driven on
///   a dedicated OS thread with its own runtime so the caller is not re-entered.
pub(crate) fn block<F, T>(fut: F) -> Result<T>
where
    F: std::future::Future<Output = Result<T>> + Send + 'static,
    T: Send + 'static,
{
    match tokio::runtime::Handle::try_current() {
        Ok(handle) if handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread => {
            tokio::task::block_in_place(|| handle.block_on(fut))
        }
        _ => std::thread::spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(Error::Io)
                .and_then(|rt| rt.block_on(fut))
        })
        .join()
        .expect("blocking thread panicked"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn is_shard_matches_year_files_only() {
        assert!(is_shortvol_shard("shortvol-2024.parquet"));
        assert!(!is_shortvol_shard("manifest.json"));
        assert!(!is_shortvol_shard("shortvol-.parquet"));
        assert!(!is_shortvol_shard("insider-2024.parquet"));
    }

    #[test]
    fn sort_desc_orders_recent_first() {
        let rows = vec![
            ShortVol::new(20260101, "A".into(), 1, 0, 2, "Q".into()),
            ShortVol::new(20260103, "A".into(), 1, 0, 2, "Q".into()),
        ];
        let sorted = sort_desc(rows);
        assert_eq!(sorted[0].date, 20260103);
    }
}
