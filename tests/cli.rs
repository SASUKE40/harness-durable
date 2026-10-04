use serde_json::Value;
use std::{
    fs,
    io::Write,
    path::Path,
    process::{Command, Stdio},
    thread,
    time::{Duration, Instant},
};
use tempfile::TempDir;

fn binary() -> &'static str {
    env!("CARGO_BIN_EXE_harness-durable")
}
fn run(root: &Path, args: &[&str]) -> std::process::Output {
    Command::new(binary())
        .arg("--state-dir")
        .arg(root)
        .args(args)
        .output()
        .unwrap()
}

#[test]
fn import_query_export_and_status() {
    let temp = TempDir::new().unwrap();
    let root = temp.path().join("state");
    let input = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/pi.jsonl");
    for _ in 0..2 {
        let result = run(
            &root,
            &[
                "import",
                "--harness",
                "pi",
                "--path",
                input.to_str().unwrap(),
            ],
        );
        assert!(
            result.status.success(),
            "{}",
            String::from_utf8_lossy(&result.stderr)
        );
    }
    let result = run(
        &root,
        &["query", "--kind", "tool_result", "--format", "jsonl"],
    );
    assert!(result.status.success());
    let event: Value = serde_json::from_slice(&result.stdout).unwrap();
    assert_eq!(event["text"], "hello world");
    let result = run(&root, &["status"]);
    let status: Value = serde_json::from_slice(&result.stdout).unwrap();
    assert_eq!(status["captured_records"], 7);
    assert_eq!(status["pending_records"], 0);
    let out = temp.path().join("export");
    let result = run(
        &root,
        &[
            "export",
            "--session",
            "pi-session",
            "--output",
            out.to_str().unwrap(),
        ],
    );
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    assert!(out.join("events.lance").is_dir());
    let query = run(
        &root,
        &["query", "--session", "x' OR 1=1 --", "--format", "jsonl"],
    );
    assert!(query.status.success());
    assert!(query.stdout.is_empty());
}

struct Child(std::process::Child);
impl Drop for Child {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[test]
fn watcher_captures_offline_and_allows_readers() {
    let temp = TempDir::new().unwrap();
    let root = temp.path().join("state");
    let source = temp.path().join("session.jsonl");
    fs::copy(
        Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/pi.jsonl"),
        &source,
    )
    .unwrap();
    // Hold connections open without responding, simulating a cloud outage.
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    listener.set_nonblocking(true).unwrap();
    let config = temp.path().join("config.toml");
    fs::write(&config,format!("flush_seconds=1\nrescan_seconds=1\n[[remotes]]\nname='offline'\nkind='cloudflare'\nurl='http://127.0.0.1:{port}'\narchive='test'\ntoken_env='HARNESS_TEST_TOKEN'\n")).unwrap();
    let log = temp.path().join("watch.log");
    let mut child = Child(
        Command::new(binary())
            .arg("--config")
            .arg(&config)
            .arg("--state-dir")
            .arg(&root)
            .args(["watch", "--harness", "pi", "--path"])
            .arg(&source)
            .env("HARNESS_TEST_TOKEN", "test")
            .stdout(Stdio::null())
            .stderr(fs::File::create(&log).unwrap())
            .spawn()
            .unwrap(),
    );
    let start = Instant::now();
    let mut appended = false;
    let mut captured = false;
    let mut connections = Vec::new();
    while start.elapsed() < Duration::from_secs(20) {
        if let Ok((stream, _)) = listener.accept() {
            connections.push(stream);
        }
        // Do not let a status process initialize an absent state directory before
        // the watcher has acquired its writer lock.
        if !root.join("state.sqlite").exists() {
            thread::sleep(Duration::from_millis(50));
            continue;
        }
        let result = run(&root, &["status"]);
        if let Ok(status) = serde_json::from_slice::<Value>(&result.stdout) {
            if status["batches"].as_u64().unwrap_or(0) > 0 && !appended {
                fs::OpenOptions::new().append(true).open(&source).unwrap().write_all(b"{\"type\":\"message\",\"id\":\"last\",\"parentId\":\"f\",\"message\":{\"role\":\"user\",\"content\":\"offline addition\"}}\n").unwrap();
                appended = true;
            }
            if status["captured_records"] == 8 && status["pending_records"] == 0 {
                let result = run(
                    &root,
                    &["query", "--text", "offline addition", "--format", "jsonl"],
                );
                if result.status.success() && !result.stdout.is_empty() {
                    captured = true;
                    break;
                }
            }
        }
        assert!(
            child.0.try_wait().unwrap().is_none(),
            "watcher exited unexpectedly: {}",
            fs::read_to_string(&log).unwrap_or_default()
        );
        thread::sleep(Duration::from_millis(100));
    }
    assert!(
        captured,
        "watcher must capture and publish locally during an unresponsive upload"
    );
    assert!(
        !connections.is_empty(),
        "test must exercise an in-flight cloud request"
    );
}
