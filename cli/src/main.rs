//! `shortkit-cli` — build, refresh, and query the bundled daily short-sale
//! volume parquet data.
//!
//! # Commands
//!
//! ```text
//! shortkit-cli backfill [--from 2018] [--to 2026]
//! shortkit-cli nightly-append
//! shortkit-cli manifest
//! shortkit-cli query --symbol AAPL
//! shortkit-cli query --date 20260624 --top 20
//! ```
//!
//! `backfill` walks every trading day from `--from` (Jan 1) through `--to`
//! (today), downloads each day's FINRA consolidated NMS short-volume file, and
//! writes one parquet per year under `data/year=YYYY/shortvol-YYYY.parquet`.
//! Weekends and holidays return 403/404 from FINRA and are skipped gracefully.
//!
//! `nightly-append` resumes from the day after the latest date already present
//! in the current-year file, fetches the missing trading days, and merges them
//! deduplicated by `(date, symbol)`. Idempotent.

mod ingest;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use clap::{Parser, Subcommand};
use sha2::{Digest, Sha256};
use shortkit::{read_shortvol, write_shortvol, ShortVol};

/// Default first backfill year. FINRA's CDN retains a rolling window of roughly
/// the last eight years; older days return 403 and are skipped.
const DEFAULT_FROM_YEAR: i32 = 2018;

/// FINRA consolidated NMS daily short-volume file URL for a `YYYYMMDD` date.
fn cnms_url(date: i32) -> String {
    format!("https://cdn.finra.org/equity/regsho/daily/CNMSshvol{date:08}.txt")
}

#[derive(Parser)]
#[command(name = "shortkit-cli", about = "US daily short-sale volume (FINRA)")]
struct Cli {
    /// Data directory (default: `<cwd>/data`).
    #[arg(long, env = "SHORTKIT_DATA_DIR", global = true)]
    data_dir: Option<PathBuf>,

    #[command(subcommand)]
    cmd: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Download each trading day's FINRA file and rebuild per-year parquet.
    Backfill {
        /// First year to include (default 2018; FINRA retains ~8 years).
        #[arg(long)]
        from: Option<i32>,
        /// Last year to include (default: current year).
        #[arg(long)]
        to: Option<i32>,
    },
    /// Fetch trading days since the latest date present and merge them into the
    /// current-year parquet, deduplicated by `(date, symbol)`.
    NightlyAppend,
    /// Generate `data/manifest.json` with a SHA-256 per parquet file.
    Manifest,
    /// Read bundled parquet and print matching rows.
    Query {
        /// Symbol (case-insensitive); prints its short-volume history.
        #[arg(long)]
        symbol: Option<String>,
        /// Trade date `YYYYMMDD`; prints the most-shorted symbols that day.
        #[arg(long)]
        date: Option<i32>,
        /// With `--date`: number of most-shorted symbols to print.
        #[arg(long, default_value_t = 20)]
        top: usize,
        /// With `--date`: minimum total volume floor for `--top`.
        #[arg(long, default_value_t = 100_000)]
        min_volume: i64,
        /// Maximum rows to print for a `--symbol` history.
        #[arg(long, default_value_t = 20)]
        limit: usize,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let data_dir = cli.data_dir.unwrap_or_else(|| PathBuf::from("data"));

    match cli.cmd {
        Command::Backfill { from, to } => {
            let from = from.unwrap_or(DEFAULT_FROM_YEAR);
            let to = to.unwrap_or_else(current_year);
            backfill(&data_dir, from, to).await
        }
        Command::NightlyAppend => nightly_append(&data_dir).await,
        Command::Manifest => write_manifest(&data_dir),
        Command::Query {
            symbol,
            date,
            top,
            min_volume,
            limit,
        } => query(&data_dir, symbol, date, top, min_volume, limit),
    }
}

// ---------------------------------------------------------------------------
// backfill
// ---------------------------------------------------------------------------

async fn backfill(data_dir: &Path, from: i32, to: i32) -> Result<()> {
    let client = http_client()?;
    let today = today_ymd();
    for year in from..=to {
        // Re-seed each year from any rows already present so a resumed backfill
        // does not discard prior days for the year.
        let mut rows = load_year(data_dir, year)?;
        let mut seen = key_set(&rows);
        let start = year * 10000 + 101;
        let end = (year * 10000 + 1231).min(today);
        let mut day = start;
        let (mut hit, mut miss) = (0u32, 0u32);
        while day <= end {
            if is_weekend(day) {
                day = next_day(day);
                continue;
            }
            match fetch_day(&client, day).await {
                Ok(Some(body)) => {
                    let parsed = ingest::parse_cnms(&body);
                    hit += 1;
                    for r in parsed {
                        if seen.insert((r.date, r.symbol.clone())) {
                            rows.push(r);
                        }
                    }
                }
                Ok(None) => miss += 1, // holiday / not published
                Err(e) => eprintln!("{day}: fetch failed ({e}), skipping"),
            }
            day = next_day(day);
        }
        eprintln!("{year}: {hit} trading days fetched, {miss} skipped (weekend math excluded)");
        write_year(data_dir, year, &rows)?;
    }
    write_manifest(data_dir)
}

// ---------------------------------------------------------------------------
// nightly-append
// ---------------------------------------------------------------------------

async fn nightly_append(data_dir: &Path) -> Result<()> {
    let today = today_ymd();
    let year = today / 10000;
    let client = http_client()?;

    let existing = load_year(data_dir, year)?;
    let last = existing.iter().map(|r| r.date).max().unwrap_or(0);
    let start = if last >= year * 10000 + 101 {
        next_day(last)
    } else {
        year * 10000 + 101
    };
    eprintln!(
        "nightly-append: {start} through {today} (year {year}, {} existing rows)",
        existing.len()
    );

    let mut seen = key_set(&existing);
    let mut rows = existing;
    let before = rows.len();
    let mut day = start;
    while day <= today {
        if is_weekend(day) {
            day = next_day(day);
            continue;
        }
        match fetch_day(&client, day).await {
            Ok(Some(body)) => {
                for r in ingest::parse_cnms(&body) {
                    if seen.insert((r.date, r.symbol.clone())) {
                        rows.push(r);
                    }
                }
            }
            Ok(None) => {}
            Err(e) => eprintln!("{day}: fetch failed ({e}), skipping"),
        }
        day = next_day(day);
    }

    let added = rows.len() - before;
    if added == 0 {
        eprintln!("no new rows; leaving data unchanged");
        return Ok(());
    }
    eprintln!("merged: {added} new rows, {} total after dedup", rows.len());
    write_year(data_dir, year, &rows)?;
    write_manifest(data_dir)
}

/// Distinct `(date, symbol)` keys already present, for idempotent dedup.
fn key_set(rows: &[ShortVol]) -> std::collections::HashSet<(i32, String)> {
    rows.iter().map(|r| (r.date, r.symbol.clone())).collect()
}

/// Read the per-year parquet, or an empty vec if it does not exist yet.
fn load_year(data_dir: &Path, year: i32) -> Result<Vec<ShortVol>> {
    let path = data_dir
        .join(format!("year={year}"))
        .join(format!("shortvol-{year}.parquet"));
    if !path.exists() {
        return Ok(Vec::new());
    }
    read_shortvol(&std::fs::read(&path)?).with_context(|| format!("read {}", path.display()))
}

// ---------------------------------------------------------------------------
// FINRA fetch
// ---------------------------------------------------------------------------

fn http_client() -> Result<reqwest::Client> {
    reqwest::Client::builder()
        .user_agent("shortkit (+https://github.com/userFRM/shortkit)")
        .timeout(std::time::Duration::from_secs(120))
        .build()
        .context("build http client")
}

/// Fetch one day's CNMS file. Returns `Ok(None)` for 403/404 (weekend, holiday,
/// or outside FINRA's retention window); any other non-success is an error.
async fn fetch_day(client: &reqwest::Client, date: i32) -> Result<Option<String>> {
    let url = cnms_url(date);
    let resp = client.get(&url).send().await.context("send request")?;
    let status = resp.status();
    if status == reqwest::StatusCode::NOT_FOUND || status == reqwest::StatusCode::FORBIDDEN {
        return Ok(None);
    }
    if !status.is_success() {
        bail!("HTTP {status} for {url}");
    }
    Ok(Some(resp.text().await.context("read body")?))
}

// ---------------------------------------------------------------------------
// write per-year parquet
// ---------------------------------------------------------------------------

fn write_year(data_dir: &Path, year: i32, rows: &[ShortVol]) -> Result<()> {
    if rows.is_empty() {
        return Ok(());
    }
    let mut rows = rows.to_vec();
    // Stable on-disk order: by date then symbol. Keeps diffs minimal across
    // nightly appends and makes row-group pruning by date effective.
    rows.sort_by(|a, b| a.date.cmp(&b.date).then_with(|| a.symbol.cmp(&b.symbol)));
    let dir = data_dir.join(format!("year={year}"));
    std::fs::create_dir_all(&dir).with_context(|| format!("create {}", dir.display()))?;
    let path = dir.join(format!("shortvol-{year}.parquet"));
    write_shortvol(&path, &rows).with_context(|| format!("write {}", path.display()))?;
    eprintln!("wrote {} ({} rows)", path.display(), rows.len());
    Ok(())
}

// ---------------------------------------------------------------------------
// manifest
// ---------------------------------------------------------------------------

/// Write `data/manifest.json` mapping `shortvol-YYYY.parquet` -> `sha256:<hex>`.
/// Keys are bare filenames so the client (which fetches flat keys) resolves them
/// regardless of the on-disk `year=YYYY/` partitioning.
fn write_manifest(data_dir: &Path) -> Result<()> {
    let mut entries: BTreeMap<String, String> = BTreeMap::new();
    for path in find_parquet(data_dir)? {
        let name = path
            .file_name()
            .and_then(|n| n.to_str())
            .context("parquet filename")?
            .to_string();
        let bytes = std::fs::read(&path)?;
        let mut h = Sha256::new();
        h.update(&bytes);
        let hex: String = h.finalize().iter().map(|b| format!("{b:02x}")).collect();
        entries.insert(name, format!("sha256:{hex}"));
    }
    let json = serde_json::to_string_pretty(&entries)?;
    let path = data_dir.join("manifest.json");
    std::fs::create_dir_all(data_dir)?;
    std::fs::write(&path, json)?;
    eprintln!("wrote {} ({} files)", path.display(), entries.len());
    Ok(())
}

fn find_parquet(data_dir: &Path) -> Result<Vec<PathBuf>> {
    let mut out = Vec::new();
    if !data_dir.exists() {
        return Ok(out);
    }
    for entry in std::fs::read_dir(data_dir)? {
        let path = entry?.path();
        if path.is_dir() {
            for sub in std::fs::read_dir(&path)? {
                let p = sub?.path();
                if p.extension().and_then(|e| e.to_str()) == Some("parquet") {
                    out.push(p);
                }
            }
        } else if path.extension().and_then(|e| e.to_str()) == Some("parquet") {
            out.push(path);
        }
    }
    out.sort();
    Ok(out)
}

// ---------------------------------------------------------------------------
// query (reads local parquet)
// ---------------------------------------------------------------------------

fn query(
    data_dir: &Path,
    symbol: Option<String>,
    date: Option<i32>,
    top: usize,
    min_volume: i64,
    limit: usize,
) -> Result<()> {
    let mut rows = Vec::new();
    for path in find_parquet(data_dir)? {
        rows.extend(read_shortvol(&std::fs::read(&path)?)?);
    }

    if let Some(d) = date {
        rows.retain(|r| r.date == d && r.total_volume >= min_volume);
        rows.sort_by(|a, b| b.short_pct.total_cmp(&a.short_pct));
        rows.truncate(top);
    } else if let Some(s) = &symbol {
        rows.retain(|r| r.symbol.eq_ignore_ascii_case(s));
        rows.sort_by_key(|r| std::cmp::Reverse(r.date));
        rows.truncate(limit);
    } else {
        bail!("query requires --symbol or --date");
    }

    println!(
        "{:<10} {:<8} {:>14} {:>10} {:>14} {:>8} market",
        "date", "symbol", "short_vol", "exempt", "total_vol", "short_%"
    );
    for r in &rows {
        println!(
            "{:<10} {:<8} {:>14} {:>10} {:>14} {:>7.1}% {}",
            r.date,
            r.symbol,
            r.short_volume,
            r.short_exempt_volume,
            r.total_volume,
            r.short_pct * 100.0,
            r.market,
        );
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// calendar helpers (system clock; YYYYMMDD math)
// ---------------------------------------------------------------------------

fn current_year() -> i32 {
    today_ymd() / 10000
}

/// Today as a `YYYYMMDD` integer from the system clock.
fn today_ymd() -> i32 {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);
    days_to_ymd(secs / 86_400)
}

/// `true` for Saturday or Sunday. Lets backfill skip the obvious non-trading
/// days without a network round-trip; holidays still cost one 403/404.
fn is_weekend(yyyymmdd: i32) -> bool {
    // 1970-01-01 was a Thursday (index 4 if Monday=0).
    let dow = (ymd_to_days(yyyymmdd) + 3).rem_euclid(7); // Monday=0 .. Sunday=6
    dow >= 5
}

/// The calendar day after a `YYYYMMDD` integer.
fn next_day(yyyymmdd: i32) -> i32 {
    days_to_ymd(ymd_to_days(yyyymmdd) + 1)
}

/// `YYYYMMDD` -> days since 1970-01-01 (Hinnant's days-from-civil).
fn ymd_to_days(d: i32) -> i64 {
    let y = (d / 10000) as i64;
    let m = ((d / 100) % 100) as i64;
    let day = (d % 100) as i64;
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

/// Days since 1970-01-01 -> `YYYYMMDD` (Hinnant's civil-from-days).
fn days_to_ymd(days: i64) -> i32 {
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    (y * 10000 + m * 100 + d) as i32
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ymd_days_round_trip() {
        assert_eq!(ymd_to_days(19700101), 0);
        assert_eq!(days_to_ymd(0), 19700101);
        assert_eq!(next_day(20240228), 20240229); // leap year
        assert_eq!(next_day(20251231), 20260101);
        assert_eq!(days_to_ymd(ymd_to_days(20260624)), 20260624);
    }

    #[test]
    fn weekend_detection() {
        assert!(!is_weekend(20260626)); // Friday
        assert!(is_weekend(20260627)); // Saturday
        assert!(is_weekend(20260628)); // Sunday
        assert!(!is_weekend(20260629)); // Monday
    }
}
