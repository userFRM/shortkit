//! End-to-end: serve a manifest + a real parquet shard, then confirm the
//! client fetches, reads, and filters it.

use shortkit::{write_shortvol, ShortVol, Shortkit};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn sha256_hex(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    h.update(bytes);
    h.finalize().iter().map(|b| format!("{b:02x}")).collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn client_reads_served_parquet() {
    let dir = tempfile::TempDir::new().unwrap();
    let shard_path = dir.path().join("shortvol-2026.parquet");
    let rows = vec![
        ShortVol::new(20260624, "AAPL".into(), 800, 0, 1000, "Q".into()), // 80%
        ShortVol::new(20260623, "AAPL".into(), 100, 0, 1000, "Q".into()), // 10%
        ShortVol::new(20260624, "MSFT".into(), 300, 0, 1000, "Q".into()), // 30%
        ShortVol::new(20260624, "TINY".into(), 9, 0, 10, "Q".into()),     // 90% but illiquid
    ];
    write_shortvol(&shard_path, &rows).unwrap();
    let parquet = std::fs::read(&shard_path).unwrap();
    let digest = sha256_hex(&parquet);

    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/manifest.json"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string(format!(r#"{{"shortvol-2026.parquet":"sha256:{digest}"}}"#)),
        )
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/shortvol-2026.parquet"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(parquet))
        .mount(&server)
        .await;

    let cache = tempfile::TempDir::new().unwrap();
    let client = Shortkit::new()
        .with_base_url(server.uri())
        .with_cache_dir(cache.path().to_path_buf())
        .with_mirror_url(None);

    let aapl = client.short_volume_for("aapl").await.unwrap();
    assert_eq!(aapl.len(), 2, "two AAPL rows");
    assert_eq!(aapl[0].date, 20260624, "sorted most-recent first");

    let day = client.latest(20260624).await.unwrap();
    assert_eq!(day.len(), 3, "three symbols on 20260624");
    assert_eq!(day[0].symbol, "TINY", "highest short_pct first");

    // Volume floor excludes TINY; AAPL leads the liquid names.
    let top = client.most_shorted(20260624, 10, 100).await.unwrap();
    assert_eq!(top[0].symbol, "AAPL");
    assert!(top.iter().all(|r| r.symbol != "TINY"));

    let ranged = client
        .short_volume_range("AAPL", 20260624, 20260624)
        .await
        .unwrap();
    assert_eq!(ranged.len(), 1);
    assert_eq!(ranged[0].date, 20260624);
}
