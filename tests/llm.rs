use harness_durable::llm::{HttpModel, LanguageModel, ModelConfig, Protocol};
use serde_json::{Value, json};
use std::sync::{Arc, Mutex};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

type Requests = Arc<Mutex<Vec<(String, Value)>>>;
async fn server(replies: Vec<(u16, Value)>) -> (String, Requests, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}/api", listener.local_addr().unwrap());
    let requests = Arc::new(Mutex::new(Vec::new()));
    let captured = requests.clone();
    let task = tokio::spawn(async move {
        for (status, body) in replies {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut bytes = Vec::new();
            let (headers, offset, len) = loop {
                let mut chunk = [0; 4096];
                let n = stream.read(&mut chunk).await.unwrap();
                assert!(n > 0);
                bytes.extend_from_slice(&chunk[..n]);
                if let Some(offset) = bytes.windows(4).position(|w| w == b"\r\n\r\n") {
                    let headers = String::from_utf8(bytes[..offset].to_vec()).unwrap();
                    let len = headers
                        .lines()
                        .find_map(|line| {
                            line.to_lowercase()
                                .strip_prefix("content-length: ")
                                .map(|s| s.parse::<usize>().unwrap())
                        })
                        .unwrap();
                    break (headers, offset + 4, len);
                }
            };
            while bytes.len() < offset + len {
                let mut chunk = [0; 4096];
                let n = stream.read(&mut chunk).await.unwrap();
                assert!(n > 0);
                bytes.extend_from_slice(&chunk[..n]);
            }
            let input = serde_json::from_slice(&bytes[offset..offset + len]).unwrap();
            captured.lock().unwrap().push((headers, input));
            let body = serde_json::to_vec(&body).unwrap();
            stream.write_all(format!("HTTP/1.1 {status} Result\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",body.len()).as_bytes()).await.unwrap();
            stream.write_all(&body).await.unwrap();
        }
    });
    (endpoint, requests, task)
}
fn config(protocol: Protocol, endpoint: String) -> ModelConfig {
    ModelConfig {
        protocol,
        endpoint,
        model: "test-model".into(),
        token_env: None,
        max_tokens: 8192,
    }
}
fn jev(reward: &str) -> Value {
    json!({"model":"jev-1.13.0","answers":{"reward":{"type":"choice","choice":reward,"probabilities":{"0":0.1,"1":0.9},"confidence":0.8}},"usage":{"input_tokens":50,"output_tokens":5}})
}

#[tokio::test]
async fn jev_native_choice_retries_and_preserves_confidence() {
    let (endpoint, requests, server) = server(vec![(429, json!({})), (200, jev("1"))]).await;
    let model = HttpModel::new(config(Protocol::Typesafe, endpoint)).unwrap();
    let result = model
        .complete(
            "Judge evidence. Return ONLY JSON",
            &json!({"candidate":"test"}),
        )
        .await
        .unwrap();
    let judgment: Value = serde_json::from_str(&result).unwrap();
    assert_eq!(judgment["reward"], 1);
    assert_eq!(
        judgment["provider_response"]["answers"]["reward"]["confidence"],
        0.8
    );
    server.await.unwrap();
    let requests = requests.lock().unwrap();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0].1, requests[1].1);
    assert_eq!(requests[0].1["state"]["candidate"], "test");
    assert_eq!(requests[0].1["questions"]["reward"]["type"], "choice");
    assert_eq!(
        requests[0].1["questions"]["reward"]["instructions"],
        "Judge evidence."
    );
    assert!(requests[0].1.get("messages").is_none());
}
#[tokio::test]
async fn anthropic_report_wire_format_and_truncation_detection() {
    let (endpoint,requests,server)=server(vec![(200,json!({"stop_reason":"end_turn","content":[{"type":"thinking","thinking":"hidden"},{"type":"text","text":"Report"}]})),(200,json!({"stop_reason":"max_tokens","content":[{"type":"text","text":"partial"}]}))]).await;
    let model = HttpModel::new(config(Protocol::Anthropic, endpoint)).unwrap();
    assert_eq!(
        model
            .complete("Report policy", &json!({"mean":0.5}))
            .await
            .unwrap(),
        "Report"
    );
    assert!(model.complete("Report policy", &json!({})).await.is_err());
    server.await.unwrap();
    let requests = requests.lock().unwrap();
    assert!(requests[0].0.contains("anthropic-version: 2023-06-01"));
    assert_eq!(requests[0].1["system"], "Report policy");
    assert_eq!(requests[0].1["max_tokens"], 8192);
}
#[tokio::test]
async fn malformed_jev_and_unauthorized_are_errors_not_zero_rewards() {
    let (endpoint, _, server) = server(vec![
        (200, jev("2")),
        (401, json!({"error":"private-provider-diagnostic"})),
    ])
    .await;
    let model = HttpModel::new(config(Protocol::Typesafe, endpoint)).unwrap();
    let missing = model.complete("policy", &json!({})).await.unwrap_err();
    assert!(missing.to_string().contains("response format marker"));
    assert!(
        model
            .complete("policy. Return ONLY JSON", &json!({}))
            .await
            .is_err()
    );
    let error = model
        .complete("policy. Return ONLY JSON", &json!({}))
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("401"));
    assert!(!error.contains("private-provider-diagnostic"));
    server.await.unwrap();
}
#[tokio::test]
async fn cli_compares_imported_sessions_through_jev_and_opus() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().join("state");
    let oracle = tmp.path().join("oracle.jsonl");
    let candidate = tmp.path().join("candidate.jsonl");
    let session = |id: &str| {
        format!(
            "{{\"type\":\"session_meta\",\"payload\":{{\"id\":\"{id}\"}}}}\n{{\"type\":\"response_item\",\"payload\":{{\"type\":\"message\",\"role\":\"assistant\",\"content\":[{{\"type\":\"output_text\",\"text\":\"Final answer is 42\"}}]}}}}\n"
        )
    };
    std::fs::write(&oracle, session("oracle")).unwrap();
    std::fs::write(&candidate, session("candidate")).unwrap();
    let binary = env!("CARGO_BIN_EXE_harness-durable");
    let result = std::process::Command::new(binary)
        .arg("--state-dir")
        .arg(&root)
        .args(["import", "--harness", "codex", "--path"])
        .arg(&oracle)
        .arg("--path")
        .arg(&candidate)
        .output()
        .unwrap();
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    let (endpoint,requests,server)=server(vec![(200,jev("1")),(200,json!({"stop_reason":"end_turn","content":[{"type":"text","text":"# Evaluation\nMean 1.0. Final output matches."}]}))]).await;
    let configuration = tmp.path().join("config.toml");
    std::fs::write(&configuration,format!("[feedback]\ndimensions=[]\n[feedback.judge]\nprotocol='typesafe'\nendpoint='{endpoint}'\nmodel='jev-1.13.0'\ntoken_env='HARNESS_MOCK_JEV_KEY'\n[feedback.reporter]\nprotocol='anthropic'\nendpoint='{endpoint}'\nmodel='claude-opus-5-5'\ntoken_env='HARNESS_MOCK_OPUS_KEY'\n")).unwrap();
    let output = tmp.path().join("evaluation");
    let mut command = std::process::Command::new(binary);
    command
        .arg("--state-dir")
        .arg(&root)
        .arg("--config")
        .arg(&configuration)
        .args([
            "compare",
            "--oracle-session",
            "oracle",
            "--candidate-session",
            "candidate",
            "--output",
        ])
        .arg(&output);
    // Dry run is usable without API keys and leaves no evaluation files.
    let dry = command.arg("--dry-run").output().unwrap();
    assert!(
        dry.status.success(),
        "{}",
        String::from_utf8_lossy(&dry.stderr)
    );
    let plan: Value = serde_json::from_slice(&dry.stdout).unwrap();
    assert_eq!(plan["alignment"][0]["oracle_step"], 0);
    assert!(!output.exists());
    let mut command = std::process::Command::new(binary);
    command
        .arg("--state-dir")
        .arg(&root)
        .arg("--config")
        .arg(&configuration)
        .args([
            "compare",
            "--oracle-session",
            "oracle",
            "--candidate-session",
            "candidate",
            "--output",
        ])
        .arg(&output)
        .env("HARNESS_MOCK_JEV_KEY", "mock-jev")
        .env("HARNESS_MOCK_OPUS_KEY", "mock-opus");
    let result = tokio::task::spawn_blocking(move || command.output().unwrap())
        .await
        .unwrap();
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    assert!(String::from_utf8_lossy(&result.stdout).contains("Mean reward: 1.000000"));
    assert!(output.join("archive/events.lance").is_dir());
    server.await.unwrap();
    let requests = requests.lock().unwrap();
    assert_eq!(requests.len(), 2);
    assert!(requests[0].0.contains("authorization: Bearer mock-jev"));
    assert!(requests[1].0.contains("x-api-key: mock-opus"));
    let report_input: Value =
        serde_json::from_str(requests[1].1["messages"][0]["content"].as_str().unwrap()).unwrap();
    assert_eq!(report_input["evaluation"]["mean_reward"], 1.0);
    assert_eq!(
        report_input["plan"]["candidate"]["final_output"],
        "Final answer is 42"
    );
}

#[tokio::test]
async fn compatible_text_models_and_unsafe_endpoint_rejection() {
    let (endpoint, _, server) = server(vec![(
        200,
        json!({"choices":[{"finish_reason":"stop","message":{"content":"Report"}}]}),
    )])
    .await;
    let model = HttpModel::new(config(Protocol::ChatCompletions, endpoint)).unwrap();
    assert_eq!(
        model.complete("Report policy", &json!({})).await.unwrap(),
        "Report"
    );
    server.await.unwrap();
    for endpoint in [
        "http://example.com/api",
        "https://user:secret@example.com/api",
        "https://example.com/api?token=secret",
    ] {
        assert!(HttpModel::new(config(Protocol::ChatCompletions, endpoint.into())).is_err());
    }
}
