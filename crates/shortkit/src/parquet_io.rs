//! Parquet reader/writer for daily short-sale volume rows.
//!
//! # File layout
//!
//! One row per `(date, symbol)`. Columns, in order:
//!
//! ```text
//! date Int32(YYYYMMDD), symbol Utf8, short_volume Int64,
//! short_exempt_volume Int64, total_volume Int64, short_pct Float64,
//! market Utf8
//! ```
//!
//! Dates are plain `i32` `YYYYMMDD` integers, not Arrow `Date32`, so a consumer
//! never needs a calendar library to compare or bucket them.

use std::fs;
use std::path::Path;
use std::sync::Arc;

use arrow::array::{Array, Float64Array, Int32Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;

use crate::error::{Error, Result};
use crate::record::ShortVol;

// ---------------------------------------------------------------------------
// Schema
// ---------------------------------------------------------------------------

/// The bundled-parquet schema, bound field by field. Every column non-null; the
/// writer fills empty strings rather than nulls so the read path can reject any
/// unexpected null as corruption.
fn shortvol_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("date", DataType::Int32, false),
        Field::new("symbol", DataType::Utf8, false),
        Field::new("short_volume", DataType::Int64, false),
        Field::new("short_exempt_volume", DataType::Int64, false),
        Field::new("total_volume", DataType::Int64, false),
        Field::new("short_pct", DataType::Float64, false),
        Field::new("market", DataType::Utf8, false),
    ]))
}

fn writer_props() -> WriterProperties {
    WriterProperties::builder()
        .set_compression(Compression::ZSTD(
            ZstdLevel::try_new(3).expect("valid zstd level"),
        ))
        .set_max_row_group_row_count(Some(50_000))
        .build()
}

// ---------------------------------------------------------------------------
// Write
// ---------------------------------------------------------------------------

/// Write `rows` to a parquet file at `path` (creates or overwrites).
pub fn write_shortvol(path: &Path, rows: &[ShortVol]) -> Result<()> {
    let schema = shortvol_schema();
    let file = fs::File::create(path)?;
    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(writer_props()))?;
    // Chunk into row-group-sized batches so a year file stays streamable.
    for chunk in rows.chunks(50_000) {
        writer.write(&batch_of(&schema, chunk)?)?;
    }
    writer.close()?;
    Ok(())
}

fn batch_of(schema: &Arc<Schema>, rows: &[ShortVol]) -> Result<RecordBatch> {
    let date: Int32Array = rows.iter().map(|r| Some(r.date)).collect();
    let symbol: StringArray = rows.iter().map(|r| Some(r.symbol.as_str())).collect();
    let short_volume: Int64Array = rows.iter().map(|r| Some(r.short_volume)).collect();
    let short_exempt_volume: Int64Array =
        rows.iter().map(|r| Some(r.short_exempt_volume)).collect();
    let total_volume: Int64Array = rows.iter().map(|r| Some(r.total_volume)).collect();
    let short_pct: Float64Array = rows.iter().map(|r| Some(r.short_pct)).collect();
    let market: StringArray = rows.iter().map(|r| Some(r.market.as_str())).collect();

    RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(date),
            Arc::new(symbol),
            Arc::new(short_volume),
            Arc::new(short_exempt_volume),
            Arc::new(total_volume),
            Arc::new(short_pct),
            Arc::new(market),
        ],
    )
    .map_err(Error::Arrow)
}

// ---------------------------------------------------------------------------
// Read
// ---------------------------------------------------------------------------

fn column_as<'a, A: Array + 'static>(batch: &'a RecordBatch, name: &str) -> Result<&'a A> {
    let idx = batch
        .schema()
        .index_of(name)
        .map_err(|_| Error::Parquet(format!("missing column: {name}")))?;
    batch
        .column(idx)
        .as_any()
        .downcast_ref::<A>()
        .ok_or_else(|| Error::Parquet(format!("{name} column type mismatch")))
}

#[inline]
fn require_non_null(col: &dyn Array, field: &str, i: usize) -> Result<()> {
    if col.is_null(i) {
        Err(Error::Parquet(format!("null {field} at row {i}")))
    } else {
        Ok(())
    }
}

/// Parse a parquet file (in-memory bytes) into [`ShortVol`] records.
pub fn read_shortvol(bytes: &[u8]) -> Result<Vec<ShortVol>> {
    let owned: bytes::Bytes = bytes::Bytes::copy_from_slice(bytes);
    let reader = ParquetRecordBatchReaderBuilder::try_new(owned)?.build()?;

    let mut rows = Vec::new();
    for batch in reader {
        let batch = batch?;
        let date = column_as::<Int32Array>(&batch, "date")?;
        let symbol = column_as::<StringArray>(&batch, "symbol")?;
        let short_volume = column_as::<Int64Array>(&batch, "short_volume")?;
        let short_exempt_volume = column_as::<Int64Array>(&batch, "short_exempt_volume")?;
        let total_volume = column_as::<Int64Array>(&batch, "total_volume")?;
        let short_pct = column_as::<Float64Array>(&batch, "short_pct")?;
        let market = column_as::<StringArray>(&batch, "market")?;

        for i in 0..batch.num_rows() {
            require_non_null(date, "date", i)?;
            require_non_null(symbol, "symbol", i)?;
            require_non_null(short_volume, "short_volume", i)?;
            require_non_null(total_volume, "total_volume", i)?;

            rows.push(ShortVol {
                date: date.value(i),
                symbol: symbol.value(i).to_owned(),
                short_volume: short_volume.value(i),
                short_exempt_volume: short_exempt_volume.value(i),
                total_volume: total_volume.value(i),
                short_pct: short_pct.value(i),
                market: market.value(i).to_owned(),
            });
        }
    }
    Ok(rows)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> ShortVol {
        ShortVol::new(20260624, "AAPL".into(), 415301, 174, 866014, "B,Q,N".into())
    }

    #[test]
    fn round_trips_rows() {
        let dir = std::env::temp_dir().join("shortkit_pq_test");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("shortvol-2026.parquet");
        let rows = vec![sample()];
        write_shortvol(&path, &rows).unwrap();
        let bytes = std::fs::read(&path).unwrap();
        let back = read_shortvol(&bytes).unwrap();
        assert_eq!(back, rows);
    }

    #[test]
    fn rejects_null_in_non_nullable_date() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("date", DataType::Int32, true), // nullable — the bad case
            Field::new("symbol", DataType::Utf8, false),
            Field::new("short_volume", DataType::Int64, false),
            Field::new("short_exempt_volume", DataType::Int64, false),
            Field::new("total_volume", DataType::Int64, false),
            Field::new("short_pct", DataType::Float64, false),
            Field::new("market", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![None])),
                Arc::new(StringArray::from(vec!["AAPL"])),
                Arc::new(Int64Array::from(vec![1i64])),
                Arc::new(Int64Array::from(vec![0i64])),
                Arc::new(Int64Array::from(vec![2i64])),
                Arc::new(Float64Array::from(vec![0.5])),
                Arc::new(StringArray::from(vec!["Q"])),
            ],
        )
        .unwrap();
        let mut buf = Vec::new();
        {
            let mut w = ArrowWriter::try_new(&mut buf, schema, None).unwrap();
            w.write(&batch).unwrap();
            w.close().unwrap();
        }
        let err = read_shortvol(&buf).unwrap_err().to_string();
        assert!(err.contains("null date"), "got: {err}");
    }
}
