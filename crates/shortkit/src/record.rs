//! The daily short-sale volume record.
//!
//! One [`ShortVol`] is one `(date, symbol)` row from FINRA's consolidated NMS
//! daily short-sale volume file. Dates are stored as `i32` in `YYYYMMDD` form
//! (e.g. `20260624`) so comparisons are integer-cheap and need no calendar
//! library on the hot path.
//!
//! FINRA reports volumes as decimals (odd-lot aggregation produces fractional
//! share counts); we round to whole shares at ingest and store `i64`.
use serde::{Deserialize, Serialize};

/// One day of short-sale volume for one symbol (one row in the bundled parquet).
///
/// `short_pct` is `short_volume / total_volume`, computed at ingest and stored
/// so a consumer never has to guard the divide. It is `0.0` when `total_volume`
/// is zero.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ShortVol {
    /// Trade date as `YYYYMMDD`.
    pub date: i32,
    /// Ticker symbol (FINRA's `Symbol` column; already the plain ticker).
    pub symbol: String,
    /// Shares sold short.
    pub short_volume: i64,
    /// Short-exempt shares (a subset of `short_volume` reporting context).
    pub short_exempt_volume: i64,
    /// Total reported volume across all sale types.
    pub total_volume: i64,
    /// `short_volume / total_volume`, `0.0` when `total_volume == 0`.
    pub short_pct: f64,
    /// Reporting market(s), e.g. `"B,Q,N"` (consolidated tape participants).
    pub market: String,
}

impl ShortVol {
    /// Build a row, computing `short_pct` with a divide-by-zero guard.
    pub fn new(
        date: i32,
        symbol: String,
        short_volume: i64,
        short_exempt_volume: i64,
        total_volume: i64,
        market: String,
    ) -> Self {
        let short_pct = if total_volume > 0 {
            short_volume as f64 / total_volume as f64
        } else {
            0.0
        };
        Self {
            date,
            symbol,
            short_volume,
            short_exempt_volume,
            total_volume,
            short_pct,
            market,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn short_pct_computed() {
        let r = ShortVol::new(20260624, "AAPL".into(), 250, 0, 1000, "Q".into());
        assert!((r.short_pct - 0.25).abs() < 1e-12);
    }

    #[test]
    fn short_pct_guards_zero_total() {
        let r = ShortVol::new(20260624, "AAPL".into(), 0, 0, 0, "Q".into());
        assert_eq!(r.short_pct, 0.0);
    }
}
