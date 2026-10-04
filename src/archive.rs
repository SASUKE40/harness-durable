use crate::{model::*, state::State};
use anyhow::{Context, Result, ensure};
use arrow_array::{
    Array, ArrayRef, BinaryArray, RecordBatch, RecordBatchIterator, StringArray, UInt32Array,
    UInt64Array,
};
use arrow_schema::{DataType, Field, Schema};
use futures::TryStreamExt;
use lance::{Dataset, dataset::WriteParams};
use lance_file::version::LanceFileVersion;
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    fs::File,
    io::{Read, Write},
    path::{Path, PathBuf},
    sync::Arc,
};

fn schema(fields: Vec<Field>) -> Arc<Schema> {
    Arc::new(Schema::new(fields).with_metadata(HashMap::from([(
        "harness_durable_schema_version".into(),
        SCHEMA_VERSION.to_string(),
    )])))
}
pub fn records_schema() -> Arc<Schema> {
    schema(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("harness", DataType::Utf8, false),
        Field::new("session_id", DataType::Utf8, false),
        Field::new("source_id", DataType::Utf8, false),
        Field::new("source_path", DataType::Utf8, false),
        Field::new("position", DataType::UInt64, false),
        Field::new("captured_at", DataType::Utf8, false),
        Field::new("status", DataType::Utf8, false),
        Field::new("diagnostic", DataType::Utf8, true),
        Field::new("adapter_version", DataType::Utf8, false),
        Field::new("raw", DataType::Binary, false),
    ])
}
pub fn events_schema() -> Arc<Schema> {
    schema(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("record_id", DataType::Utf8, false),
        Field::new("harness", DataType::Utf8, false),
        Field::new("session_id", DataType::Utf8, false),
        Field::new("source_id", DataType::Utf8, false),
        Field::new("parent_session_id", DataType::Utf8, true),
        Field::new("native_id", DataType::Utf8, true),
        Field::new("parent_id", DataType::Utf8, true),
        Field::new("position", DataType::UInt64, false),
        Field::new("sub_index", DataType::UInt32, false),
        Field::new("timestamp", DataType::Utf8, true),
        Field::new("kind", DataType::Utf8, false),
        Field::new("role", DataType::Utf8, true),
        Field::new("text", DataType::Utf8, true),
        Field::new("model", DataType::Utf8, true),
        Field::new("tool_call_id", DataType::Utf8, true),
        Field::new("payload_json", DataType::Utf8, false),
    ])
}

pub fn record_batch(rows: &[Record]) -> Result<RecordBatch> {
    macro_rules! strings {
        ($f:ident) => {
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|r| r.$f.as_str()),
            )) as ArrayRef
        };
    }
    Ok(RecordBatch::try_new(
        records_schema(),
        vec![
            strings!(id),
            strings!(harness),
            strings!(session_id),
            strings!(source_id),
            strings!(source_path),
            Arc::new(UInt64Array::from_iter_values(
                rows.iter().map(|r| r.position),
            )),
            strings!(captured_at),
            strings!(status),
            Arc::new(StringArray::from_iter(
                rows.iter().map(|r| r.diagnostic.as_deref()),
            )),
            strings!(adapter_version),
            Arc::new(BinaryArray::from_iter_values(
                rows.iter().map(|r| r.raw.as_slice()),
            )),
        ],
    )?)
}
pub fn event_batch(rows: &[Event]) -> Result<RecordBatch> {
    macro_rules! strings {
        ($f:ident) => {
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|r| r.$f.as_str()),
            )) as ArrayRef
        };
    }
    macro_rules! optional {
        ($f:ident) => {
            Arc::new(StringArray::from_iter(rows.iter().map(|r| r.$f.as_deref()))) as ArrayRef
        };
    }
    Ok(RecordBatch::try_new(
        events_schema(),
        vec![
            strings!(id),
            strings!(record_id),
            strings!(harness),
            strings!(session_id),
            strings!(source_id),
            optional!(parent_session_id),
            optional!(native_id),
            optional!(parent_id),
            Arc::new(UInt64Array::from_iter_values(
                rows.iter().map(|r| r.position),
            )),
            Arc::new(UInt32Array::from_iter_values(
                rows.iter().map(|r| r.sub_index),
            )),
            optional!(timestamp),
            strings!(kind),
            optional!(role),
            optional!(text),
            optional!(model),
            optional!(tool_call_id),
            strings!(payload_json),
        ],
    )?)
}

async fn write_dataset(path: &Path, batch: RecordBatch) -> Result<()> {
    let reader = RecordBatchIterator::new(vec![Ok(batch.clone())], batch.schema());
    Dataset::write(
        reader,
        path.to_str().context("dataset path must be UTF-8")?,
        Some(WriteParams::with_storage_version(LanceFileVersion::V2_1)),
    )
    .await?;
    Ok(())
}

pub fn file_digest(path: &Path) -> Result<(u64, String)> {
    let mut f = File::open(path)?;
    let mut h = Sha256::new();
    let mut buf = [0; 65536];
    let mut size = 0;
    loop {
        let n = f.read(&mut buf)?;
        if n == 0 {
            break;
        }
        h.update(&buf[..n]);
        size += n as u64;
    }
    Ok((size, format!("{:x}", h.finalize())))
}

pub fn safe_relative(path: &str) -> bool {
    !path.is_empty()
        && !path.contains('\\')
        && path
            .split('/')
            .all(|s| !s.is_empty() && s != "." && s != "..")
        && !Path::new(path).is_absolute()
}
pub fn validate_manifest(m: &Manifest) -> Result<()> {
    ensure!(
        m.schema_version == SCHEMA_VERSION,
        "unsupported archive schema {}",
        m.schema_version
    );
    ensure!(
        safe_relative(&m.collector_id)
            && !m.collector_id.contains('/')
            && safe_relative(&m.batch_id)
            && !m.batch_id.contains('/'),
        "invalid batch identity"
    );
    let mut paths = HashSet::new();
    for f in &m.files {
        ensure!(
            safe_relative(&f.path)
                && (f.path.starts_with("records.lance/") || f.path.starts_with("events.lance/")),
            "unsafe dataset path"
        );
        ensure!(paths.insert(&f.path), "duplicate inventory path");
        ensure!(
            f.sha256.len() == 64 && f.sha256.bytes().all(|b| b.is_ascii_hexdigit()),
            "invalid checksum"
        );
    }
    for dataset in ["records.lance/", "events.lance/"] {
        ensure!(
            m.files
                .iter()
                .any(|f| f.path.starts_with(&format!("{dataset}_versions/"))),
            "missing dataset manifest"
        );
    }
    Ok(())
}
pub fn manifest(path: &Path) -> Result<Manifest> {
    let m = serde_json::from_slice(&std::fs::read(path.join("manifest.json"))?)?;
    validate_manifest(&m)?;
    Ok(m)
}
pub fn verify(path: &Path, m: &Manifest) -> Result<()> {
    validate_manifest(m)?;
    for f in &m.files {
        let (size, digest) = file_digest(&path.join(&f.path))?;
        ensure!(
            size == f.size && digest == f.sha256,
            "checksum mismatch: {}",
            f.path
        );
    }
    Ok(())
}

pub fn summaries(records: &[Record], events: &[Event]) -> Vec<SessionSummary> {
    let mut map = BTreeMap::new();
    for r in records {
        let s = map
            .entry((r.harness.clone(), r.session_id.clone()))
            .or_insert(SessionSummary {
                harness: r.harness.clone(),
                session_id: r.session_id.clone(),
                records: 0,
                events: 0,
                first_timestamp: None,
                last_timestamp: None,
            });
        s.records += 1;
    }
    for e in events {
        if let Some(s) = map.get_mut(&(e.harness.clone(), e.session_id.clone())) {
            s.events += 1;
            if let Some(t) = &e.timestamp {
                if s.first_timestamp.as_ref().is_none_or(|v| t < v) {
                    s.first_timestamp = Some(t.clone());
                }
                if s.last_timestamp.as_ref().is_none_or(|v| t > v) {
                    s.last_timestamp = Some(t.clone());
                }
            }
        }
    }
    map.into_values().collect()
}

/// Publish two native Lance datasets together using a final completion marker.
pub async fn write_archive(
    destination: &Path,
    collector: &str,
    batch: &str,
    records: &[Record],
    events: &[Event],
) -> Result<Manifest> {
    ensure!(
        !destination.exists(),
        "destination already exists: {}",
        destination.display()
    );
    let parent = destination.parent().context("destination needs a parent")?;
    std::fs::create_dir_all(parent)?;
    let tmp = parent.join(format!(".tmp-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir(&tmp)?;
    let result: Result<Manifest> = async {
        write_dataset(&tmp.join("records.lance"), record_batch(records)?).await?;
        write_dataset(&tmp.join("events.lance"), event_batch(events)?).await?;
        let mut files = Vec::new();
        for entry in walkdir::WalkDir::new(&tmp) {
            let entry = entry?;
            if !entry.file_type().is_file() {
                continue;
            }
            File::open(entry.path())?.sync_all()?;
            let (size, sha256) = file_digest(entry.path())?;
            files.push(InventoryFile {
                path: entry
                    .path()
                    .strip_prefix(&tmp)?
                    .to_string_lossy()
                    .replace('\\', "/"),
                size,
                sha256,
            });
        }
        files.sort_by(|a, b| a.path.cmp(&b.path));
        let m = Manifest {
            schema_version: SCHEMA_VERSION,
            collector_id: collector.into(),
            batch_id: batch.into(),
            created_at: chrono::Utc::now().to_rfc3339(),
            sessions: summaries(records, events),
            files,
        };
        validate_manifest(&m)?;
        let mut marker = File::create(tmp.join("manifest.json"))?;
        marker.write_all(&serde_json::to_vec_pretty(&m)?)?;
        marker.sync_all()?;
        // Directory entries must survive a power loss as well as file contents.
        for entry in walkdir::WalkDir::new(&tmp).contents_first(true) {
            let entry = entry?;
            if entry.file_type().is_dir() {
                File::open(entry.path())?.sync_all()?;
            }
        }
        std::fs::rename(&tmp, destination)?;
        File::open(parent)?.sync_all()?;
        Ok(m)
    }
    .await;
    if result.is_err() {
        let _ = std::fs::remove_dir_all(&tmp);
    }
    result
}

pub async fn flush(
    state: &mut State,
    max_records: usize,
    max_bytes: usize,
) -> Result<Option<PathBuf>> {
    let (records, events) = state.pending(max_records, max_bytes)?;
    if records.is_empty() {
        return Ok(None);
    }
    let ids: Vec<_> = records.iter().map(|r| r.id.as_str()).collect();
    let batch = stable_id(&ids);
    let path = state
        .root
        .join("batches")
        .join(&state.collector_id)
        .join(&batch);
    if path.exists() {
        verify(&path, &manifest(&path)?)?;
    } else {
        write_archive(&path, &state.collector_id, &batch, &records, &events).await?;
    }
    state.finish_batch(&batch, &path, &records)?;
    Ok(Some(path))
}

pub async fn read_events(path: &Path) -> Result<Vec<Event>> {
    read_events_filtered(path, &Query::default()).await
}

async fn read_events_filtered(path: &Path, q: &Query) -> Result<Vec<Event>> {
    let ds = Dataset::open(
        path.join("events.lance")
            .to_str()
            .context("UTF-8 path required")?,
    )
    .await?;
    let mut scanner = ds.scan();
    let mut predicates = Vec::new();
    for (column, op, value) in [
        ("harness", "=", &q.harness),
        ("session_id", "=", &q.session),
        ("kind", "=", &q.kind),
        ("timestamp", ">=", &q.since),
        ("timestamp", "<=", &q.until),
    ] {
        if let Some(value) = value {
            predicates.push(format!("{column} {op} '{}'", value.replace('\'', "''")));
        }
    }
    if !predicates.is_empty() {
        scanner.filter(&predicates.join(" AND "))?;
    }
    let mut stream = scanner.try_into_stream().await?;
    let mut result = Vec::new();
    while let Some(batch) = stream.try_next().await? {
        let mut writer = arrow_json::LineDelimitedWriter::new(Vec::new());
        writer.write_batches(&[&batch])?;
        writer.finish()?;
        for line in writer
            .into_inner()
            .split(|b| *b == b'\n')
            .filter(|b| !b.is_empty())
        {
            result.push(serde_json::from_slice(line)?);
        }
    }
    Ok(result)
}
pub async fn read_records(path: &Path) -> Result<Vec<Record>> {
    let ds = Dataset::open(
        path.join("records.lance")
            .to_str()
            .context("UTF-8 path required")?,
    )
    .await?;
    let mut stream = ds.scan().try_into_stream().await?;
    let mut result = Vec::new();
    while let Some(batch) = stream.try_next().await? {
        for i in 0..batch.num_rows() {
            let text = |name: &str| -> Result<String> {
                let a = batch
                    .column_by_name(name)
                    .context("missing column")?
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .context("invalid string column")?;
                Ok(a.value(i).into())
            };
            let diag = batch
                .column_by_name("diagnostic")
                .context("missing diagnostic")?;
            result.push(Record {
                id: text("id")?,
                harness: text("harness")?,
                session_id: text("session_id")?,
                source_id: text("source_id")?,
                source_path: text("source_path")?,
                position: batch
                    .column_by_name("position")
                    .context("missing position")?
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .context("invalid position")?
                    .value(i),
                captured_at: text("captured_at")?,
                status: text("status")?,
                diagnostic: if diag.is_null(i) {
                    None
                } else {
                    Some(text("diagnostic")?)
                },
                adapter_version: text("adapter_version")?,
                raw: batch
                    .column_by_name("raw")
                    .context("missing raw")?
                    .as_any()
                    .downcast_ref::<BinaryArray>()
                    .context("invalid raw")?
                    .value(i)
                    .to_vec(),
            });
        }
    }
    Ok(result)
}

pub async fn query(paths: &[PathBuf], q: &Query) -> Result<Vec<Event>> {
    let mut seen = HashSet::new();
    let mut events = Vec::new();
    for path in paths {
        let m = manifest(path)?;
        if !m.sessions.iter().any(|s| q.matches_session(s)) {
            continue;
        }
        for e in read_events_filtered(path, q).await? {
            if seen.insert(e.id.clone()) && q.matches(&e) {
                events.push(e);
            }
        }
    }
    // A hook may enrich the same tool result already present in a transcript.
    // Prefer transcript events; retain every raw record and all uncorrelated calls.
    events.sort_by_key(|e| {
        (
            e.payload_json.contains("\"hook_event_name\""),
            e.source_id.clone(),
            e.position,
            e.sub_index,
        )
    });
    let mut tools = HashSet::new();
    events.retain(|e| {
        if (e.kind == "tool_call" || e.kind == "tool_result") && e.tool_call_id.is_some() {
            tools.insert((
                e.harness.clone(),
                e.session_id.clone(),
                e.kind.clone(),
                e.tool_call_id.clone(),
            ))
        } else {
            true
        }
    });
    events.sort_by(|a, b| {
        (
            &a.harness,
            &a.session_id,
            &a.source_id,
            a.position,
            a.sub_index,
        )
            .cmp(&(
                &b.harness,
                &b.session_id,
                &b.source_id,
                b.position,
                b.sub_index,
            ))
    });
    Ok(events)
}
