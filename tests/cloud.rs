//! Opt-in service tests. They use synthetic records and unique prefixes only.
use harness_durable::{
    adapters, archive, config::RemoteConfig, model::Query, remote::Remote, state::State,
};
use tempfile::TempDir;

async fn exercise(remote: Remote) {
    let temp = TempDir::new().unwrap();
    let mut state = State::open(temp.path()).unwrap();
    let source = adapters::identify(
        &adapters::Pi,
        &std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/pi.jsonl"),
    )
    .unwrap();
    state.ingest(&source, 100, 100000).unwrap();
    let path = archive::flush(&mut state, 100, 100000)
        .await
        .unwrap()
        .unwrap();
    let manifest = archive::manifest(&path).unwrap();
    remote.upload(&path).await.unwrap();
    remote.upload(&path).await.unwrap();
    let listed = remote.list().await.unwrap();
    assert!(listed.contains(&manifest));
    let restored = remote
        .download(&manifest, &temp.path().join("download"))
        .await
        .unwrap();
    archive::verify(&restored, &manifest).unwrap();
    assert_eq!(
        archive::query(std::slice::from_ref(&path), &Query::default())
            .await
            .unwrap(),
        archive::query(&[restored], &Query::default())
            .await
            .unwrap()
    );
    // A different inventory with the same batch identity must be rejected.
    let mut conflict = manifest;
    conflict.created_at = "2020-01-01T00:00:00Z".into();
    std::fs::write(
        path.join("manifest.json"),
        serde_json::to_vec(&conflict).unwrap(),
    )
    .unwrap();
    assert!(remote.upload(&path).await.is_err());
}

#[tokio::test]
#[ignore = "requires a configured S3-compatible test bucket"]
async fn s3_round_trip() {
    let config = RemoteConfig::S3 {
        name: "test".into(),
        bucket: std::env::var("HARNESS_TEST_S3_BUCKET").expect("test bucket"),
        prefix: format!("integration/{}", uuid::Uuid::new_v4()),
        endpoint: Some(std::env::var("HARNESS_TEST_S3_ENDPOINT").expect("test endpoint")),
        region: Some("us-east-1".into()),
        allow_http: true,
    };
    exercise(Remote::new(&config).await.unwrap()).await;
}
#[tokio::test]
#[ignore = "requires wrangler dev and HARNESS_DURABLE_TOKEN"]
async fn cloudflare_round_trip() {
    let config = RemoteConfig::Cloudflare {
        name: "test".into(),
        url: std::env::var("HARNESS_TEST_CLOUDFLARE_URL")
            .unwrap_or_else(|_| "http://127.0.0.1:8787".into()),
        archive: uuid::Uuid::new_v4().to_string(),
        token_env: "HARNESS_DURABLE_TOKEN".into(),
    };
    exercise(Remote::new(&config).await.unwrap()).await;
}
