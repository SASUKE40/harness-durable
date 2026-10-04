use harness_durable::{
    adapters::{self, SessionAdapter},
    archive, hooks,
    model::{Manifest, Query},
    state::State,
};
use serde_json::Value;
use std::{
    fs,
    path::{Path, PathBuf},
    time::{Duration, SystemTime},
};
use tempfile::TempDir;

fn fixture(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(name)
}
/// Make a file look settled, as after a normal pause between writes.
fn age(path: &Path) {
    fs::File::options()
        .write(true)
        .open(path)
        .unwrap()
        .set_modified(SystemTime::now() - Duration::from_secs(60))
        .unwrap();
}
#[cfg(unix)]
fn deny_read(path: &Path) {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(path, fs::Permissions::from_mode(0o000)).unwrap();
}
async fn batch(state: &mut State, source: &adapters::Source) -> PathBuf {
    state.ingest(source, 1000, 1 << 20).unwrap();
    archive::flush(state, 1000, 1 << 20).await.unwrap().unwrap()
}

#[test]
fn manifest_rules_match_the_shared_worker_fixture() {
    let fixture: Value =
        serde_json::from_slice(&fs::read(fixture("manifests.json")).unwrap()).unwrap();
    for case in fixture["cases"].as_array().unwrap() {
        let mut manifest = fixture["base"].clone();
        for (key, value) in case["patch"].as_object().unwrap() {
            manifest[key] = value.clone();
        }
        let valid = serde_json::from_value::<Manifest>(manifest)
            .map_err(anyhow::Error::from)
            .and_then(|m| archive::validate_manifest(&m))
            .is_ok();
        assert_eq!(valid, case["valid"] == true, "{}", case["name"]);
    }
}

#[tokio::test]
async fn compaction_merges_batches_and_keeps_query_results() {
    let temp = TempDir::new().unwrap();
    let mut state = State::open(&temp.path().join("state")).unwrap();
    let first = batch(
        &mut state,
        &adapters::identify(&adapters::Pi, &fixture("pi.jsonl")).unwrap(),
    )
    .await;
    let second = batch(
        &mut state,
        &adapters::identify(&adapters::Codex, &fixture("codex.jsonl")).unwrap(),
    )
    .await;
    let before = archive::query(&state.batch_paths().unwrap(), &Query::default())
        .await
        .unwrap();
    let summaries = archive::session_summaries(&state.batch_paths().unwrap(), &Query::default())
        .await
        .unwrap();

    assert_eq!(archive::compact(&mut state, 1 << 30).await.unwrap(), 2);
    let paths = state.batch_paths().unwrap();
    assert_eq!(paths.len(), 1);
    let merged = archive::manifest(&paths[0]).unwrap();
    let mut replaced = vec![
        archive::manifest(&first).unwrap().batch_id,
        archive::manifest(&second).unwrap().batch_id,
    ];
    replaced.sort();
    assert_eq!(merged.replaces, replaced);
    assert_eq!(
        archive::query(&paths, &Query::default()).await.unwrap(),
        before
    );
    assert_eq!(
        archive::session_summaries(&paths, &Query::default())
            .await
            .unwrap(),
        summaries
    );
    assert_eq!(state.status(&[]).unwrap().batches, 1);
    assert_eq!(archive::compact(&mut state, 1 << 30).await.unwrap(), 0);

    // Readers that list all published manifests ignore the replaced batches.
    let listed = archive::active(vec![
        archive::manifest(&first).unwrap(),
        merged.clone(),
        archive::manifest(&second).unwrap(),
    ]);
    assert_eq!(listed, vec![merged]);

    // Replaced files stay for readers until the retention time passes.
    assert!(first.exists());
    assert_eq!(state.purge_replaced(Duration::ZERO).unwrap(), 2);
    assert!(!first.exists() && !second.exists());
}

#[test]
#[cfg(unix)]
fn settled_unchanged_sources_are_not_opened_again() {
    let temp = TempDir::new().unwrap();
    let path = temp.path().join("session.jsonl");
    fs::copy(fixture("pi.jsonl"), &path).unwrap();
    age(&path);
    let mut state = State::open(&temp.path().join("state")).unwrap();
    let source = state.identify(&adapters::Pi, &path).unwrap();
    assert_eq!(state.ingest(&source, 100, 1 << 20).unwrap(), 7);
    deny_read(&path);
    if fs::read(&path).is_ok() {
        return; // Permissions do not apply to this user (for example, root).
    }
    assert_eq!(
        state.identify(&adapters::Pi, &path).unwrap().session_id,
        source.session_id
    );
    assert_eq!(state.ingest(&source, 100, 1 << 20).unwrap(), 0);
}

#[test]
fn recently_modified_sources_are_read_again() {
    let temp = TempDir::new().unwrap();
    let path = temp.path().join("session.jsonl");
    fs::copy(fixture("pi.jsonl"), &path).unwrap();
    let mut state = State::open(&temp.path().join("state")).unwrap();
    let source = state.identify(&adapters::Pi, &path).unwrap();
    assert_eq!(state.ingest(&source, 100, 1 << 20).unwrap(), 7);
    // Same size and a fresh mtime: the content must be examined, not trusted.
    let replaced = fs::read_to_string(&path)
        .unwrap()
        .replace("hello\"", "other\"");
    fs::write(&path, replaced).unwrap();
    assert_eq!(state.ingest(&source, 100, 1 << 20).unwrap(), 7);
}

#[tokio::test]
async fn published_hook_spool_files_are_removed_once() {
    let temp = TempDir::new().unwrap();
    let payload = br#"{"conversation_id":"cursor","hook_event_name":"postToolUse","tool_use_id":"t1","tool_output":"spooled"}"#;
    let spooled = hooks::receive(temp.path(), &payload[..]).unwrap();
    let mut state = State::open(temp.path()).unwrap();
    let source = adapters::identify(&adapters::Cursor, &spooled).unwrap();
    state.ingest(&source, 100, 1 << 20).unwrap();
    assert_eq!(
        state.purge_hook_spool(&temp.path().join("hooks")).unwrap(),
        0
    );
    let path = archive::flush(&mut state, 100, 1 << 20)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        state.purge_hook_spool(&temp.path().join("hooks")).unwrap(),
        1
    );
    assert!(!spooled.exists() && !spooled.with_extension("stdin").exists());
    assert_eq!(state.status(&[]).unwrap().sources, 0);
    let events = archive::query(&[path], &Query::default()).await.unwrap();
    assert_eq!(events[0].text.as_deref(), Some("spooled"));
}

#[test]
fn pending_rows_keep_raw_bytes_in_a_blob() {
    let temp = TempDir::new().unwrap();
    let mut state = State::open(temp.path()).unwrap();
    let source = adapters::identify(&adapters::Pi, &fixture("pi.jsonl")).unwrap();
    state.ingest(&source, 1, 1 << 20).unwrap();
    let (record, raw, bytes): (String, Vec<u8>, i64) = state
        .conn
        .query_row("SELECT record,raw,bytes FROM pending", [], |r| {
            Ok((r.get(0)?, r.get(1)?, r.get(2)?))
        })
        .unwrap();
    let events: String = state
        .conn
        .query_row("SELECT events FROM pending", [], |r| r.get(0))
        .unwrap();
    assert!(record.contains("\"raw\":[]"));
    assert_eq!(bytes as usize, record.len() + events.len() + raw.len());
    let (records, _) = state.pending(10, 1 << 20).unwrap();
    assert_eq!(records[0].raw, raw);
    assert!(raw.starts_with(b"{"));
}

#[tokio::test]
async fn transcript_text_about_hooks_is_not_mistaken_for_a_hook() {
    let temp = TempDir::new().unwrap();
    let transcript = temp.path().join("transcript.jsonl");
    fs::write(
        &transcript,
        concat!(
            r#"{"role":"assistant","message":{"content":[{"type":"tool_use","id":"t1","name":"Read","input":{"hook_event_name":"postToolUse"}}]}}"#,
            "\n",
            r#"{"role":"user","message":{"content":[{"type":"tool_result","tool_use_id":"t1","content":"from transcript","structured":{"hook_event_name":"postToolUse"}}]}}"#,
            "\n"
        ),
    )
    .unwrap();
    let payload = br#"{"conversation_id":"transcript","hook_event_name":"postToolUse","tool_use_id":"t1","tool_output":"from hook"}"#;
    let hook = hooks::receive(temp.path(), &payload[..]).unwrap();
    let mut state = State::open(temp.path()).unwrap();
    for path in [&hook, &transcript] {
        state
            .ingest(
                &adapters::identify(&adapters::Cursor, path).unwrap(),
                100,
                1 << 20,
            )
            .unwrap();
    }
    let path = archive::flush(&mut state, 100, 1 << 20)
        .await
        .unwrap()
        .unwrap();
    let results: Vec<_> = archive::query(&[path], &Query::default())
        .await
        .unwrap()
        .into_iter()
        .filter(|e| e.kind == "tool_result")
        .collect();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].text.as_deref(), Some("from transcript"));
}

#[tokio::test]
async fn events_are_found_by_id_and_text_without_a_full_read() {
    let temp = TempDir::new().unwrap();
    let mut state = State::open(temp.path()).unwrap();
    let path = batch(
        &mut state,
        &adapters::identify(&adapters::Pi, &fixture("pi.jsonl")).unwrap(),
    )
    .await;
    let all = archive::read_events(&path).await.unwrap();
    let paths = [path];
    let wanted = &all[all.len() / 2];
    assert_eq!(
        archive::find_event(&paths, &Query::default(), &wanted.id)
            .await
            .unwrap()
            .as_ref(),
        Some(wanted)
    );
    assert!(
        archive::find_event(&paths, &Query::default(), "missing")
            .await
            .unwrap()
            .is_none()
    );
    let quoted = Query {
        text: Some("it's".into()),
        ..Query::default()
    };
    assert!(archive::query(&paths, &quoted).await.unwrap().is_empty());
}

#[test]
fn abandoned_temporary_batches_are_removed_at_startup() {
    let temp = TempDir::new().unwrap();
    let collector = State::open(temp.path()).unwrap().collector_id;
    let abandoned = temp
        .path()
        .join("batches")
        .join(&collector)
        .join(".tmp-crashed");
    fs::create_dir_all(abandoned.join("records.lance")).unwrap();
    let recent_download = temp.path().join("cache/remote/.tmp-download");
    fs::create_dir_all(&recent_download).unwrap();
    drop(State::open(temp.path()).unwrap());
    assert!(!abandoned.exists());
    assert!(
        recent_download.exists(),
        "a reader may still be downloading"
    );
}

#[test]
fn cursor_adapter_name_is_stable() {
    assert_eq!(adapters::Cursor.name(), "cursor");
}
