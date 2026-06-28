//! Parse a FINRA consolidated NMS daily short-sale volume file into rows.
//!
//! The file is pipe-delimited with a header line:
//!
//! ```text
//! Date|Symbol|ShortVolume|ShortExemptVolume|TotalVolume|Market
//! 20260624|AAPL|415301.481264|174|866014.483541|B,Q,N
//! ```
//!
//! Volumes arrive as decimals (odd-lot aggregation yields fractional share
//! counts). We round to the nearest whole share and store `i64`. A trailing
//! "grand total" footer line (some daily files carry one) is dropped because its
//! symbol does not parse as a normal ticker row.

use shortkit::ShortVol;

/// Parse a CNMS daily file body into rows for the given `date` fallback.
///
/// Each row carries its own `Date` column; we trust that. Lines that are blank,
/// the header, or otherwise malformed are skipped (never fatal) so one stray
/// footer line cannot poison a day.
pub fn parse_cnms(body: &str) -> Vec<ShortVol> {
    let mut out = Vec::new();
    for line in body.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        // Skip the header and any non-data line: a data line starts with an
        // 8-digit date.
        let first = line.split('|').next().unwrap_or("");
        if first.len() != 8 || !first.bytes().all(|b| b.is_ascii_digit()) {
            continue;
        }
        if let Some(row) = parse_line(line) {
            out.push(row);
        }
    }
    out
}

fn parse_line(line: &str) -> Option<ShortVol> {
    let mut f = line.split('|');
    let date: i32 = f.next()?.parse().ok()?;
    let symbol = f.next()?.trim();
    if symbol.is_empty() {
        return None;
    }
    let short_volume = parse_shares(f.next()?);
    let short_exempt_volume = parse_shares(f.next()?);
    let total_volume = parse_shares(f.next()?);
    // Market is optional on some legacy files; default to empty.
    let market = f.next().unwrap_or("").trim().to_string();

    Some(ShortVol::new(
        date,
        symbol.to_string(),
        short_volume,
        short_exempt_volume,
        total_volume,
        market,
    ))
}

/// Parse a (possibly fractional) share count to whole shares.
fn parse_shares(s: &str) -> i64 {
    let s = s.trim();
    if let Ok(i) = s.parse::<i64>() {
        return i;
    }
    s.parse::<f64>().map(|v| v.round() as i64).unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE: &str = "Date|Symbol|ShortVolume|ShortExemptVolume|TotalVolume|Market\n\
20260624|A|415301.481264|174|866014.483541|B,Q,N\n\
20260624|AAA|3340|0|3911.701689|Q\n\
20260624|ZYME|73174.853854|0|114808.611355|B,Q,N\n";

    #[test]
    fn parses_header_and_fractional_volumes() {
        let rows = parse_cnms(SAMPLE);
        assert_eq!(rows.len(), 3, "header skipped, 3 data rows");
        let a = &rows[0];
        assert_eq!(a.date, 20260624);
        assert_eq!(a.symbol, "A");
        assert_eq!(a.short_volume, 415301); // rounded from 415301.481264
        assert_eq!(a.short_exempt_volume, 174);
        assert_eq!(a.total_volume, 866014);
        assert_eq!(a.market, "B,Q,N");
        assert!((a.short_pct - 415301.0 / 866014.0).abs() < 1e-9);
    }

    #[test]
    fn skips_footer_and_blank_lines() {
        let body = format!("{SAMPLE}\nGrand Total|||||\n\n");
        let rows = parse_cnms(&body);
        assert_eq!(rows.len(), 3, "footer and blank lines dropped");
    }
}
