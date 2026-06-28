//! Unified error type for shortkit. All public methods return `shortkit::Result<T>`.
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("http error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("parquet I/O error: {0}")]
    Parquet(String),
    #[error("arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),
    #[error("parquet error: {0}")]
    ParquetNative(#[from] parquet::errors::ParquetError),
    #[error("checksum mismatch for {file}: expected sha256:{expected} got sha256:{actual}")]
    ChecksumMismatch {
        file: String,
        expected: String,
        actual: String,
    },
    #[error("not found: {0}")]
    NotFound(String),
    #[error("{0}")]
    Other(String),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn checksum_mismatch_displays_both_digests() {
        let e = Error::ChecksumMismatch {
            file: "shortvol-2026.parquet".into(),
            expected: "aaa".into(),
            actual: "bbb".into(),
        };
        let s = e.to_string();
        assert!(s.contains("aaa") && s.contains("bbb") && s.contains("shortvol-2026.parquet"));
    }
}
