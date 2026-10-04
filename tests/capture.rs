use harness_durable::{
    adapters::{self, SessionAdapter},
    archive, hooks,
    model::{Query, hash},
    state::State,
};
use std::{fs, io::Write, path::Path};
use tempfile::TempDir;

fn fixture(name: &str) -> std::path::PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(name)
}

#[test]
fn adapters_preserve_semantics_and_unknowns() {
    for (name, file) in [
        ("codex", "codex.jsonl"),
        ("pi", "pi.jsonl"),
        ("cursor", "cursor.jsonl"),
        ("cursor", "cursor-cli.jsonl"),
    ] {
        let a = adapters::adapter(name).unwrap();
        let p = fixture(file);
        let s = adapters::identify(a.as_ref(), &p).unwrap();
        let mut events = Vec::new();
        let mut offset = 0;
        for raw in fs::read(&p).unwrap().split_inclusive(|b| *b == b'\n') {
            let (r, e) = adapters::parse(a.as_ref(), &s, offset, raw.to_vec());
            assert_eq!(r.raw, raw);
            offset += raw.len() as u64;
            events.extend(e);
        }
        assert!(events.iter().any(|e| e.kind == "message"));
        assert!(events.iter().any(|e| e.kind == "tool_call"));
        if name == "codex" {
            assert_eq!(events.iter().filter(|e| e.kind == "message").count(), 2);
            assert!(events.iter().any(|e| e.kind == "unknown"));
            assert!(
                events
                    .iter()
                    .all(|e| e.parent_session_id.as_deref() == Some("parent"))
            );
        }
        if name == "pi" {
            let branch = events.iter().find(|e| e.kind == "branch").unwrap();
            assert_eq!(branch.parent_id.as_deref(), Some("b"));
            assert!(events.iter().any(|e| e.kind == "compaction"));
        }
        if file == "cursor.jsonl" {
            assert!(events.iter().all(|e| e.timestamp.is_none()));
        }
        if file == "cursor-cli.jsonl" {
            assert_eq!(
                events
                    .iter()
                    .filter(|e| e.kind == "message" && e.role.as_deref() == Some("assistant"))
                    .count(),
                1
            );
        }
    }
}
#[test]
fn repeated_content_has_distinct_ids_and_mirrored_sources_match() {
    let a = adapters::Cursor;
    let p = fixture("cursor.jsonl");
    let s = adapters::identify(&a, &p).unwrap();
    let raw = b"{\"role\":\"user\",\"message\":{\"content\":\"same\"}}\n";
    let (r1, _) = adapters::parse(&a, &s, 0, raw.to_vec());
    let (r2, _) = adapters::parse(&a, &s, 100, raw.to_vec());
    assert_ne!(r1.id, r2.id);
    let mut copy = s.clone();
    copy.path = "/another/machine/cursor.jsonl".into();
    assert_eq!(r1.id, adapters::parse(&a, &copy, 0, raw.to_vec()).0.id);
}
#[test]
fn checkpoints_partial_lines_malformed_records_rename_and_replacement() {
    let dir = TempDir::new().unwrap();
    let p = dir.path().join("session.jsonl");
    let bytes = fs::read(fixture("pi.jsonl")).unwrap();
    fs::write(&p, &bytes).unwrap();
    let source = adapters::identify(&adapters::Pi, &p).unwrap();
    let root = dir.path().join("state");
    let mut state = State::open(&root).unwrap();
    assert_eq!(state.ingest(&source, 100, 100000).unwrap(), 7);
    assert_eq!(state.ingest(&source, 100, 100000).unwrap(), 0);
    fs::OpenOptions::new()
        .append(true)
        .open(&p)
        .unwrap()
        .write_all(b"{\"type\":")
        .unwrap();
    assert_eq!(state.ingest(&source, 100, 100000).unwrap(), 0);
    drop(state);
    let mut state = State::open(&root).unwrap();
    fs::OpenOptions::new()
        .append(true)
        .open(&p)
        .unwrap()
        .write_all(b"\"future\"}\nnot json\n")
        .unwrap();
    assert_eq!(state.ingest(&source, 100, 100000).unwrap(), 2);
    assert_eq!(state.status(&[]).unwrap().parse_failures, 1);
    let renamed = dir.path().join("renamed.jsonl");
    fs::rename(&p, &renamed).unwrap();
    let source2 = adapters::identify(&adapters::Pi, &renamed).unwrap();
    state.ingest(&source2, 100, 100000).unwrap();
    assert_eq!(state.pending_size().unwrap().0, 9);
    fs::write(&renamed, &bytes).unwrap();
    state.ingest(&source2, 100, 100000).unwrap();
    assert_eq!(state.pending_size().unwrap().0, 9);
    let fresh = dir.path().join("fresh");
    let replacement = String::from_utf8(bytes)
        .unwrap()
        .replace("hello\"", "other\"");
    fs::write(&fresh, replacement).unwrap();
    fs::rename(fresh, &renamed).unwrap();
    state.ingest(&source2, 100, 100000).unwrap();
    assert_eq!(state.pending_size().unwrap().0, 10);
}
#[tokio::test]
async fn native_lance_round_trip_dedup_and_crash_recovery() {
    let temp = TempDir::new().unwrap();
    let mut state = State::open(temp.path()).unwrap();
    let source = adapters::identify(&adapters::Codex, &fixture("codex.jsonl")).unwrap();
    state.ingest(&source, 100, 100000).unwrap();
    let (records, events) = state.pending(100, 100000).unwrap();
    let path = archive::flush(&mut state, 100, 100000)
        .await
        .unwrap()
        .unwrap();
    let m = archive::manifest(&path).unwrap();
    archive::verify(&path, &m).unwrap();
    assert_eq!(archive::read_records(&path).await.unwrap(), records);
    assert_eq!(archive::read_events(&path).await.unwrap(), events);
    assert_eq!(state.pending_size().unwrap().0, 0);
    let q = Query {
        kind: Some("message".into()),
        ..Default::default()
    };
    assert_eq!(
        archive::query(&[path.clone(), path.clone()], &q)
            .await
            .unwrap()
            .len(),
        2
    );
    let q = Query {
        text: Some("hello world".into()),
        since: Some("2026-10-01T10:00:04.000Z".into()),
        ..Default::default()
    };
    assert_eq!(
        archive::query(std::slice::from_ref(&path), &q)
            .await
            .unwrap()
            .len(),
        2
    );
    // Simulate publication succeeding just before the SQLite commit: pending
    // rows restored from the durable spool must reuse the already-published batch.
    for (r, e) in records.iter().zip(events.iter().map(|e| vec![e])) {
        state
            .conn
            .execute(
                "INSERT INTO pending(id,record,events,bytes) VALUES(?1,?2,?3,?4)",
                rusqlite::params![
                    r.id,
                    serde_json::to_string(r).unwrap(),
                    serde_json::to_string(&e).unwrap(),
                    r.raw.len() as i64
                ],
            )
            .unwrap();
    }
    assert_eq!(
        archive::flush(&mut state, 100, 100000)
            .await
            .unwrap()
            .unwrap(),
        path
    );
    fs::write(path.join(&m.files[0].path), b"corruption").unwrap();
    assert!(archive::verify(&path, &m).is_err());
}
#[tokio::test]
async fn empty_and_malformed_only_datasets_are_readable() {
    let temp = TempDir::new().unwrap();
    let out = temp.path().join("empty");
    archive::write_archive(&out, "collector", "empty", &[], &[])
        .await
        .unwrap();
    assert!(archive::read_records(&out).await.unwrap().is_empty());
    assert!(archive::read_events(&out).await.unwrap().is_empty());
    let source = adapters::identify(&adapters::Pi, &fixture("pi.jsonl")).unwrap();
    let (record, events) = adapters::parse(&adapters::Pi, &source, 0, b"malformed\n".to_vec());
    let out = temp.path().join("malformed");
    archive::write_archive(
        &out,
        "collector",
        "malformed",
        std::slice::from_ref(&record),
        &events,
    )
    .await
    .unwrap();
    assert_eq!(archive::read_records(&out).await.unwrap(), vec![record]);
    assert!(archive::read_events(&out).await.unwrap().is_empty());
}
#[test]
fn hook_install_uninstall_preserves_other_entries() {
    let temp = TempDir::new().unwrap();
    let config = temp.path().join("hooks.json");
    let original = serde_json::json!({"version":1,"other":true,"hooks":{"sessionStart":[{"command":"keep-me"}],"stop":[{"command":"also-keep"}]}});
    fs::write(&config, serde_json::to_vec(&original).unwrap()).unwrap();
    hooks::install(&config, temp.path(), &std::env::current_exe().unwrap()).unwrap();
    hooks::install(&config, temp.path(), &std::env::current_exe().unwrap()).unwrap();
    let value: serde_json::Value = serde_json::from_slice(&fs::read(&config).unwrap()).unwrap();
    assert_eq!(value["hooks"]["sessionStart"].as_array().unwrap().len(), 2);
    hooks::uninstall(&config).unwrap();
    let value: serde_json::Value = serde_json::from_slice(&fs::read(config).unwrap()).unwrap();
    assert_eq!(
        value["hooks"]["sessionStart"],
        original["hooks"]["sessionStart"]
    );
    assert_eq!(value["other"], true);
}
#[tokio::test]
async fn cursor_hooks_enrich_transcripts_without_losing_repeated_deliveries() {
    let temp = TempDir::new().unwrap();
    let payload=br#"{"conversation_id":"cursor","hook_event_name":"postToolUse","tool_use_id":"cursor-tool","tool_output":"hello world"}"#;
    let p1 = hooks::receive(temp.path(), &payload[..]).unwrap();
    let p2 = hooks::receive(temp.path(), &payload[..]).unwrap();
    let a = adapters::Cursor;
    let s1 = adapters::identify(&a, &p1).unwrap();
    let s2 = adapters::identify(&a, &p2).unwrap();
    assert_ne!(s1.source_id, s2.source_id);
    let mut state = State::open(temp.path()).unwrap();
    state.ingest(&s1, 100, 100000).unwrap();
    state.ingest(&s2, 100, 100000).unwrap();
    state
        .ingest(
            &adapters::identify(&a, &fixture("cursor.jsonl")).unwrap(),
            100,
            100000,
        )
        .unwrap();
    let path = archive::flush(&mut state, 100, 100000)
        .await
        .unwrap()
        .unwrap();
    let events = archive::query(&[path], &Query::default()).await.unwrap();
    assert_eq!(events.iter().filter(|e| e.kind == "tool_result").count(), 1);
    assert_eq!(events.iter().filter(|e| e.kind == "tool_call").count(), 1);
    assert!(
        events
            .iter()
            .filter(|e| e.kind == "tool_result")
            .all(|e| e.timestamp.is_none())
    );
}
#[test]
fn malformed_and_unknown_are_explicit_and_paths_are_guarded() {
    let s = adapters::identify(&adapters::Pi, &fixture("pi.jsonl")).unwrap();
    let (r, e) = adapters::parse(&adapters::Pi, &s, 0, b"{broken}\n".to_vec());
    assert_eq!(r.status, "malformed");
    assert!(e.is_empty());
    assert!(!archive::safe_relative("../secret"));
    assert!(!archive::safe_relative("/secret"));
    assert!(!archive::safe_relative("a/../secret"));
    assert_eq!(hash(b"").len(), 64);
    assert_eq!(adapters::Pi.name(), "pi");
}
