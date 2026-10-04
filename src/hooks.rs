use anyhow::{Context, Result, ensure};
use serde_json::{Value, json};
use std::{
    io::{Read, Write},
    path::{Path, PathBuf},
};

const EVENTS: &[&str] = &[
    "sessionStart",
    "sessionEnd",
    "postToolUse",
    "postToolUseFailure",
];
fn shell_quote(s: &str) -> String {
    format!("'{}'", s.replace('\'', "'\"'\"'"))
}

fn atomic_json(path: &Path, value: &Value) -> Result<()> {
    let parent = path.parent().context("missing parent")?;
    std::fs::create_dir_all(parent)?;
    let tmp = parent.join(format!(".harness-durable-{}", uuid::Uuid::new_v4()));
    let mut f = std::fs::File::create(&tmp)?;
    f.write_all(&serde_json::to_vec_pretty(value)?)?;
    f.sync_all()?;
    std::fs::rename(&tmp, path)?;
    std::fs::File::open(parent)?.sync_all()?;
    Ok(())
}
fn marker(config: &Path) -> PathBuf {
    config.with_file_name(".harness-durable-hooks.json")
}

pub fn install(config: &Path, state: &Path, binary: &Path) -> Result<()> {
    ensure!(binary.is_file(), "collector executable does not exist");
    let mut value: Value = if config.exists() {
        serde_json::from_slice(&std::fs::read(config)?)?
    } else {
        json!({"version":1,"hooks":{}})
    };
    ensure!(value.is_object(), "hooks config must be an object");
    ensure!(
        value.get("version").is_none_or(|v| v == 1),
        "unsupported hooks config version"
    );
    if marker(config).exists() {
        let old: Value = serde_json::from_slice(&std::fs::read(marker(config))?)?;
        // Reinstallation only removes entries owned by the previous installation.
        remove_entries(&mut value, &old)?;
    }
    let command = format!(
        "{} --state-dir {} hooks receive",
        shell_quote(&binary.to_string_lossy()),
        shell_quote(&state.to_string_lossy())
    );
    let entry = json!({"command":command,"timeout":5,"failClosed":false});
    if value.get("hooks").is_none() {
        value["hooks"] = json!({});
    }
    let hooks = value["hooks"]
        .as_object_mut()
        .context("hooks must be an object")?;
    for name in EVENTS {
        let list = hooks
            .entry(*name)
            .or_insert(json!([]))
            .as_array_mut()
            .context("hook event must be an array")?;
        if !list.contains(&entry) {
            list.push(entry.clone());
        }
    }
    value["version"] = json!(1);
    // Persist ownership first; interrupted installation can safely be retried.
    atomic_json(&marker(config), &entry)?;
    atomic_json(config, &value)
}
fn remove_entries(value: &mut Value, entry: &Value) -> Result<()> {
    if let Some(hooks) = value.get_mut("hooks").and_then(Value::as_object_mut) {
        for name in EVENTS {
            if let Some(list) = hooks.get_mut(*name) {
                list.as_array_mut()
                    .context("hook event must be array")?
                    .retain(|v| v != entry);
            }
        }
    }
    Ok(())
}
pub fn uninstall(config: &Path) -> Result<()> {
    let owned = marker(config);
    if !owned.exists() {
        return Ok(());
    }
    let entry: Value = serde_json::from_slice(&std::fs::read(&owned)?)?;
    if config.exists() {
        let mut value = serde_json::from_slice(&std::fs::read(config)?)?;
        remove_entries(&mut value, &entry)?;
        atomic_json(config, &value)?;
    }
    std::fs::remove_file(owned)?;
    Ok(())
}
pub fn receive(state: &Path, mut input: impl Read) -> Result<PathBuf> {
    let mut raw = Vec::new();
    input.read_to_end(&mut raw)?;
    let value: Value = serde_json::from_slice(&raw)?;
    let session = value["conversation_id"]
        .as_str()
        .context("hook missing conversation_id")?;
    ensure!(
        value["hook_event_name"].is_string(),
        "missing hook_event_name"
    );
    let root = state.join("hooks").join(crate::model::hash(session));
    std::fs::create_dir_all(&root)?;
    let id = uuid::Uuid::new_v4();
    let path = root.join(format!("{id}.jsonl"));
    let tmp = root.join(format!(".{id}.tmp"));
    // Cursor sends one JSON document, potentially pretty-printed. Store compact
    // JSONL and preserve the exact stdin bytes in a separate raw input file.
    let raw_path = root.join(format!("{id}.stdin"));
    let mut original = std::fs::File::create(raw_path)?;
    original.write_all(&raw)?;
    original.sync_all()?;
    let mut f = std::fs::File::create(&tmp)?;
    serde_json::to_writer(&mut f, &value)?;
    f.write_all(b"\n")?;
    f.sync_all()?;
    std::fs::rename(tmp, &path)?;
    std::fs::File::open(root)?.sync_all()?;
    Ok(path)
}
