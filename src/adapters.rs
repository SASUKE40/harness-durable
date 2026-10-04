//! Harness parsers are deliberately tolerant: the raw record is always retained.
use crate::model::{ADAPTER_VERSION, Event, Record, hash, stable_id};
use anyhow::{Result, bail};
use chrono::{DateTime, SecondsFormat, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    io::{BufRead, BufReader},
    path::{Path, PathBuf},
};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Source {
    pub harness: String,
    pub session_id: String,
    pub source_id: String,
    pub parent_session_id: Option<String>,
    pub path: PathBuf,
    pub format: String,
    pub project: Option<String>,
}

pub trait SessionAdapter: Send + Sync {
    fn name(&self) -> &'static str;
    fn roots(&self, home: &Path) -> Vec<PathBuf>;
    fn identify(&self, path: &Path, first: &Value) -> Result<Source>;
    fn normalize(&self, source: &Source, value: &Value) -> Vec<Event>;
}

pub struct Codex;
pub struct Pi;
pub struct Cursor;

pub fn adapter(name: &str) -> Result<Box<dyn SessionAdapter>> {
    match name {
        "codex" => Ok(Box::new(Codex)),
        "pi" => Ok(Box::new(Pi)),
        "cursor" => Ok(Box::new(Cursor)),
        _ => bail!("unsupported harness: {name}"),
    }
}

fn s(v: &Value, key: &str) -> Option<String> {
    v.get(key).and_then(Value::as_str).map(str::to_owned)
}
fn timestamp(v: &Value) -> Option<String> {
    let t = v.get("timestamp").or_else(|| v.get("timestamp_ms"))?;
    let dt = if let Some(text) = t.as_str() {
        DateTime::parse_from_rfc3339(text).ok()?.with_timezone(&Utc)
    } else {
        DateTime::from_timestamp_millis(t.as_i64()?)?
    };
    Some(dt.to_rfc3339_opts(SecondsFormat::Millis, true))
}
fn event(kind: &str, v: &Value) -> Event {
    Event {
        kind: kind.into(),
        timestamp: timestamp(v),
        native_id: s(v, "id"),
        parent_id: s(v, "parentId"),
        model: s(v, "model").or_else(|| s(v, "modelId")),
        payload_json: v.to_string(),
        ..Event::default()
    }
}

fn base_source(harness: &str, path: &Path, session: String, first: &Value, format: &str) -> Source {
    Source {
        harness: harness.into(),
        source_id: stable_id(&[harness, &session, format]),
        session_id: session,
        path: path.to_owned(),
        format: format.into(),
        project: s(first, "cwd"),
        ..Source::default()
    }
}

pub fn identify(a: &dyn SessionAdapter, path: &Path) -> Result<Source> {
    // A corrupt leading record must not prevent discovery of the real header.
    let f = std::fs::File::open(path)?;
    for line in BufReader::new(f).split(b'\n').take(32) {
        if let Ok(v) = serde_json::from_slice::<Value>(&line?)
            && let Ok(source) = a.identify(path, &v)
        {
            return Ok(source);
        }
    }
    bail!(
        "unsupported or missing {} session header: {}",
        a.name(),
        path.display()
    )
}

pub fn discover(a: &dyn SessionAdapter, roots: &[PathBuf]) -> Result<Vec<Source>> {
    let mut result = Vec::new();
    for root in roots {
        if !root.exists() {
            continue;
        }
        for entry in walkdir::WalkDir::new(root).follow_links(false) {
            let entry = match entry {
                Ok(e) => e,
                Err(e) => {
                    eprintln!("discovery: {e}");
                    continue;
                }
            };
            if !entry.file_type().is_file()
                || entry
                    .path()
                    .extension()
                    .is_none_or(|e| e != "jsonl" && e != "ndjson")
            {
                continue;
            }
            match identify(a, entry.path()) {
                Ok(s) => result.push(s),
                Err(e) => eprintln!("discovery: {e}"),
            }
        }
    }
    result.sort_by(|a, b| a.path.cmp(&b.path));
    result.dedup_by(|a, b| a.path == b.path);
    Ok(result)
}

fn blocks(v: &Value, role: Option<String>) -> Vec<Event> {
    let content = v.get("content").unwrap_or(&Value::Null);
    if let Some(text) = content.as_str() {
        let mut e = event("message", v);
        e.role = role;
        e.text = Some(text.into());
        return vec![e];
    }
    let Some(items) = content.as_array() else {
        return vec![event("unknown", v)];
    };
    items
        .iter()
        .map(|b| {
            let kind = match b.get("type").and_then(Value::as_str).unwrap_or("") {
                "text" | "input_text" | "output_text" => "message",
                "tool_use" | "toolCall" => "tool_call",
                "tool_result" | "toolResult" => "tool_result",
                "thinking" | "reasoning" => "reasoning",
                "image" | "input_image" | "image_url" | "audio" | "file" => "attachment",
                _ => "unknown",
            };
            let mut e = event(kind, b);
            e.role = role.clone();
            e.text = s(b, "text")
                .or_else(|| s(b, "thinking"))
                .or_else(|| s(b, "content"));
            e.tool_call_id = s(b, "tool_use_id")
                .or_else(|| s(b, "toolCallId"))
                .or_else(|| {
                    if kind == "tool_call" {
                        s(b, "id")
                    } else {
                        None
                    }
                });
            e
        })
        .collect()
}

impl SessionAdapter for Codex {
    fn name(&self) -> &'static str {
        "codex"
    }
    fn roots(&self, home: &Path) -> Vec<PathBuf> {
        let root = std::env::var_os("CODEX_HOME")
            .map(PathBuf::from)
            .unwrap_or_else(|| home.join(".codex"));
        vec![root.join("sessions"), root.join("archived_sessions")]
    }
    fn identify(&self, path: &Path, v: &Value) -> Result<Source> {
        if v["type"] != "session_meta" {
            bail!("missing session_meta");
        }
        let p = &v["payload"];
        let id = s(p, "session_id")
            .or_else(|| s(p, "id"))
            .ok_or_else(|| anyhow::anyhow!("missing session ID"))?;
        let mut source = base_source("codex", path, id, p, "rollout");
        source.parent_session_id = s(p, "parent_thread_id").or_else(|| s(p, "forked_from_id"));
        Ok(source)
    }
    fn normalize(&self, _: &Source, v: &Value) -> Vec<Event> {
        let p = &v["payload"];
        let mut events = match v["type"].as_str().unwrap_or("") {
            "response_item" => match p["type"].as_str().unwrap_or("") {
                "message" => blocks(p, s(p, "role")),
                "function_call" | "custom_tool_call" => vec![event("tool_call", p)],
                "function_call_output" | "custom_tool_call_output" => vec![event("tool_result", p)],
                "reasoning" => vec![event("reasoning", p)],
                _ => vec![event("unknown", p)],
            },
            "session_meta" | "turn_context" => vec![event("metadata", p)],
            "compacted" => vec![event("compaction", p)],
            "token_usage_record" => vec![event("usage", p)],
            "event_msg" => {
                let kind = match p["type"].as_str().unwrap_or("") {
                    "task_started" | "task_complete" | "turn_aborted" => "lifecycle",
                    "token_count" => "usage",
                    // These are notifications of response_item records, not new messages.
                    _ => "auxiliary",
                };
                vec![event(kind, p)]
            }
            _ => vec![event("unknown", v)],
        };
        for e in &mut events {
            e.timestamp = timestamp(v);
            e.tool_call_id = e.tool_call_id.take().or_else(|| s(p, "call_id"));
            e.text = e
                .text
                .take()
                .or_else(|| s(p, "output"))
                .or_else(|| s(p, "summary"));
        }
        events
    }
}

impl SessionAdapter for Pi {
    fn name(&self) -> &'static str {
        "pi"
    }
    fn roots(&self, home: &Path) -> Vec<PathBuf> {
        vec![
            std::env::var_os("PI_CODING_AGENT_SESSION_DIR")
                .map(PathBuf::from)
                .unwrap_or_else(|| home.join(".pi/agent/sessions")),
        ]
    }
    fn identify(&self, path: &Path, v: &Value) -> Result<Source> {
        if v["type"] != "session" {
            bail!("missing session header");
        }
        let id = s(v, "id").ok_or_else(|| anyhow::anyhow!("missing session ID"))?;
        let mut source = base_source("pi", path, id, v, "session");
        source.parent_session_id = s(v, "parentSession");
        Ok(source)
    }
    fn normalize(&self, _: &Source, v: &Value) -> Vec<Event> {
        let mut events = if v["type"] == "message" {
            let m = &v["message"];
            if m["role"] == "toolResult" {
                let mut e = event("tool_result", m);
                e.role = Some("tool".into());
                e.tool_call_id = s(m, "toolCallId");
                e.text = text_content(&m["content"]);
                vec![e]
            } else {
                blocks(m, s(m, "role"))
            }
        } else {
            let kind = match v["type"].as_str().unwrap_or("") {
                "session" | "session_info" => "metadata",
                "model_change" => "model_change",
                "thinking_level_change" => "metadata",
                "compaction" => "compaction",
                "branch_summary" => "branch",
                _ => "unknown",
            };
            let mut e = event(kind, v);
            e.text = s(v, "summary");
            vec![e]
        };
        for e in &mut events {
            e.native_id = s(v, "id");
            e.parent_id = s(v, "parentId");
            e.timestamp = timestamp(v);
            e.model = e.model.take().or_else(|| s(&v["message"], "model"));
        }
        events
    }
}

fn text_content(v: &Value) -> Option<String> {
    if let Some(s) = v.as_str() {
        return Some(s.into());
    }
    let texts: Vec<_> = v
        .as_array()?
        .iter()
        .filter_map(|b| b["text"].as_str())
        .collect();
    if texts.is_empty() {
        None
    } else {
        Some(texts.join("\n"))
    }
}

impl SessionAdapter for Cursor {
    fn name(&self) -> &'static str {
        "cursor"
    }
    fn roots(&self, home: &Path) -> Vec<PathBuf> {
        vec![home.join(".cursor/projects")]
    }
    fn identify(&self, path: &Path, v: &Value) -> Result<Source> {
        let hook = v.get("hook_event_name").is_some();
        let format = if hook {
            "hooks"
        } else if v.get("session_id").is_some() {
            "cli"
        } else {
            "transcript"
        };
        if !hook
            && v.get("session_id").is_none()
            && v.get("role").is_none()
            && v["type"] != "turn_ended"
        {
            bail!("unsupported Cursor record");
        }
        let id = s(v, "conversation_id")
            .or_else(|| s(v, "session_id"))
            .unwrap_or_else(|| {
                path.file_stem()
                    .unwrap_or_default()
                    .to_string_lossy()
                    .into_owned()
            });
        let mut source = base_source("cursor", path, id, v, format);
        if hook {
            source.source_id = stable_id(&[
                "cursor",
                &source.session_id,
                "hooks",
                &path.file_stem().unwrap_or_default().to_string_lossy(),
            ]);
        }
        if path.parent().is_some_and(|p| p.ends_with("subagents")) {
            source.parent_session_id = path
                .parent()
                .and_then(Path::parent)
                .and_then(Path::file_name)
                .map(|s| s.to_string_lossy().into_owned());
        }
        Ok(source)
    }
    fn normalize(&self, _: &Source, v: &Value) -> Vec<Event> {
        if let Some(hook) = v["hook_event_name"].as_str() {
            let kind = match hook {
                "postToolUse" | "postToolUseFailure" => "tool_result",
                "preToolUse" => "tool_call",
                "sessionStart" => "metadata",
                "sessionEnd" | "stop" => "lifecycle",
                _ => "auxiliary",
            };
            let mut e = event(kind, v);
            e.tool_call_id = s(v, "tool_use_id");
            e.text = s(v, "tool_output").or_else(|| s(v, "error_message"));
            return vec![e];
        }
        let t = v["type"].as_str().unwrap_or("");
        let role = s(v, "role").or_else(|| s(&v["message"], "role"));
        if role.is_some() || t == "user" || t == "assistant" {
            // Partial-output streams repeat their deltas in flush records. Retain all
            // originals, but use complete messages for conversation queries.
            if v.get("timestamp_ms").is_some() && v.get("model_call_id").is_none() {
                return vec![event("delta", v)];
            }
            let mut result = blocks(&v["message"], role.or_else(|| Some(t.into())));
            for e in &mut result {
                e.timestamp = timestamp(v);
            }
            return result;
        }
        if t == "tool_call" {
            let mut e = event(
                if v["subtype"] == "completed" {
                    "tool_result"
                } else {
                    "tool_call"
                },
                v,
            );
            e.tool_call_id = s(v, "call_id");
            return vec![e];
        }
        vec![event(
            match t {
                "system" => "metadata",
                "result" | "turn_ended" => "lifecycle",
                _ => "unknown",
            },
            v,
        )]
    }
}

pub fn parse(
    a: &dyn SessionAdapter,
    source: &Source,
    position: u64,
    raw: Vec<u8>,
) -> (Record, Vec<Event>) {
    let id = stable_id(&[&source.source_id, &position.to_string(), &hash(&raw)]);
    let mut record = Record {
        id: id.clone(),
        harness: source.harness.clone(),
        session_id: source.session_id.clone(),
        source_id: source.source_id.clone(),
        source_path: source.path.to_string_lossy().into_owned(),
        position,
        captured_at: Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true),
        status: "parsed".into(),
        diagnostic: None,
        adapter_version: ADAPTER_VERSION.into(),
        raw,
    };
    let mut events = match serde_json::from_slice::<Value>(&record.raw) {
        Ok(v) if v.is_object() => a.normalize(source, &v),
        Ok(_) => {
            record.status = "malformed".into();
            record.diagnostic = Some("record must be a JSON object".into());
            vec![]
        }
        Err(e) => {
            record.status = "malformed".into();
            record.diagnostic = Some(e.to_string());
            vec![]
        }
    };
    if events.iter().any(|e| e.kind == "unknown") {
        record.status = "unknown".into();
        record.diagnostic = Some("unrecognized record or content type; preserved verbatim".into());
    }
    for (index, e) in events.iter_mut().enumerate() {
        e.id = stable_id(&[&id, &index.to_string()]);
        e.record_id = id.clone();
        e.harness = source.harness.clone();
        e.session_id = source.session_id.clone();
        e.source_id = source.source_id.clone();
        e.parent_session_id = source.parent_session_id.clone();
        e.position = position;
        e.sub_index = index as u32;
    }
    (record, events)
}
