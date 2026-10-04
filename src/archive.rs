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

/// Limits shared with the Worker (`worker/src/index.ts`). Keep them identical.
pub const MAX_INVENTORY: usize = 10_000;
const MAX_SAFE_INTEGER: u64 = (1 << 53) - 1;
const MAX_PATH_LEN: usize = 900;
const MAX_SESSION_ID_LEN: usize = 1024;

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
/// Collector, batch, and archive IDs: `^[A-Za-z0-9_-]{1,128}$`.
pub fn valid_id(s: &str) -> bool {
    (1..=128).contains(&s.len())
        && s.bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
}
/// `^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})$`, and a real instant.
fn valid_timestamp(s: &str) -> bool {
    let b = s.as_bytes();
    let digits = |from: usize, to: usize| {
        b.get(from..to)
            .is_some_and(|d| d.iter().all(u8::is_ascii_digit))
    };
    let shape = digits(0, 4)
        && b.get(4) == Some(&b'-')
        && digits(5, 7)
        && b.get(7) == Some(&b'-')
        && digits(8, 10)
        && b.get(10) == Some(&b'T')
        && digits(11, 13)
        && b.get(13) == Some(&b':')
        && digits(14, 16)
        && b.get(16) == Some(&b':')
        && digits(17, 19);
    if !shape {
        return false;
    }
    let mut rest = &s[19..];
    if let Some(fraction) = rest.strip_prefix('.') {
        let n = fraction.bytes().take_while(u8::is_ascii_digit).count();
        if n == 0 {
            return false;
        }
        rest = &fraction[n..];
    }
    let zone = rest == "Z"
        || (rest.len() == 6
            && matches!(rest.as_bytes()[0], b'+' | b'-')
            && rest[1..3].bytes().all(|c| c.is_ascii_digit())
            && rest.as_bytes()[3] == b':'
            && rest[4..6].bytes().all(|c| c.is_ascii_digit()));
    zone && chrono::DateTime::parse_from_rfc3339(s).is_ok()
}
pub fn validate_manifest(m: &Manifest) -> Result<()> {
    ensure!(
        m.schema_version == SCHEMA_VERSION,
        "unsupported archive schema {}",
        m.schema_version
    );
    ensure!(
        valid_id(&m.collector_id) && valid_id(&m.batch_id),
        "invalid batch identity"
    );
    ensure!(valid_timestamp(&m.created_at), "invalid created_at");
    ensure!(
        !m.files.is_empty() && m.files.len() <= MAX_INVENTORY,
        "invalid inventory"
    );
    let mut paths = HashSet::new();
    for f in &m.files {
        ensure!(
            f.path.len() < MAX_PATH_LEN
                && safe_relative(&f.path)
                && (f.path.starts_with("records.lance/") || f.path.starts_with("events.lance/")),
            "unsafe dataset path"
        );
        ensure!(f.size <= MAX_SAFE_INTEGER, "invalid file size");
        ensure!(
            f.sha256.len() == 64
                && f.sha256
                    .bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)),
            "invalid checksum"
        );
        ensure!(paths.insert(&f.path), "duplicate inventory path");
    }
    for dataset in ["records.lance/", "events.lance/"] {
        ensure!(
            m.files
                .iter()
                .any(|f| f.path.starts_with(&format!("{dataset}_versions/"))),
            "missing dataset manifest"
        );
    }
    ensure!(m.sessions.len() <= MAX_INVENTORY, "invalid sessions");
    for s in &m.sessions {
        ensure!(
            s.session_id.len() <= MAX_SESSION_ID_LEN
                && s.records <= MAX_SAFE_INTEGER
                && s.events <= MAX_SAFE_INTEGER,
            "invalid session summary"
        );
    }
    ensure!(
        m.replaces.len() <= MAX_INVENTORY,
        "invalid replaced batches"
    );
    let mut replaced = HashSet::new();
    for id in &m.replaces {
        ensure!(
            valid_id(id) && id != &m.batch_id && replaced.insert(id),
            "invalid replaced batch"
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

/// Drop manifests whose content a published compaction of the same collector contains.
pub fn active(manifests: Vec<Manifest>) -> Vec<Manifest> {
    let replaced: HashSet<(String, String)> = manifests
        .iter()
        .flat_map(|m| {
            m.replaces
                .iter()
                .map(|id| (m.collector_id.clone(), id.clone()))
        })
        .collect();
    manifests
        .into_iter()
        .filter(|m| !replaced.contains(&(m.collector_id.clone(), m.batch_id.clone())))
        .collect()
}

pub fn summaries(records: &[Record], events: &[Event]) -> Vec<SessionSummary> {
    let mut map = BTreeMap::new();
    for r in records {
        map.entry((r.harness.clone(), r.session_id.clone()))
            .or_insert_with(|| empty_summary(&r.harness, &r.session_id))
            .records += 1;
    }
    for e in events {
        if let Some(s) = map.get_mut(&(e.harness.clone(), e.session_id.clone())) {
            count_event(s, e);
        }
    }
    map.into_values().collect()
}
fn empty_summary(harness: &str, session_id: &str) -> SessionSummary {
    SessionSummary {
        harness: harness.into(),
        session_id: session_id.into(),
        records: 0,
        events: 0,
        first_timestamp: None,
        last_timestamp: None,
    }
}
fn count_event(s: &mut SessionSummary, e: &Event) {
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

/// Publish two native Lance datasets together using a final completion marker.
pub async fn write_archive(
    destination: &Path,
    collector: &str,
    batch: &str,
    records: &[Record],
    events: &[Event],
) -> Result<Manifest> {
    write_archive_replacing(destination, collector, batch, records, events, Vec::new()).await
}
async fn write_archive_replacing(
    destination: &Path,
    collector: &str,
    batch: &str,
    records: &[Record],
    events: &[Event],
    replaces: Vec<String>,
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
            replaces,
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

/// Merge consecutive small local batches into batches of at most `max_bytes`.
/// Returns the number of batches that were replaced.
pub async fn compact(state: &mut State, max_bytes: u64) -> Result<usize> {
    let mut groups = vec![Vec::new()];
    let (mut size, mut ids) = (0u64, 0usize);
    for (id, path) in state.active_batches()? {
        let m = manifest(&path)?;
        let bytes: u64 = m.files.iter().map(|f| f.size).sum();
        let count = 1 + m.replaces.len();
        let group = groups.last_mut().unwrap();
        if !group.is_empty() && (size + bytes > max_bytes || ids + count > MAX_INVENTORY) {
            groups.push(Vec::new());
            (size, ids) = (0, 0);
        }
        if bytes > max_bytes {
            continue;
        }
        size += bytes;
        ids += count;
        groups.last_mut().unwrap().push((id, path, m));
    }
    let mut merged = 0;
    for group in groups.into_iter().filter(|g| g.len() >= 2) {
        let members: Vec<_> = group.iter().map(|(id, _, _)| id.as_str()).collect();
        let batch = stable_id(&[&["compact"][..], &members[..]].concat());
        let path = state
            .root
            .join("batches")
            .join(&state.collector_id)
            .join(&batch);
        if path.exists() {
            verify(&path, &manifest(&path)?)?;
        } else {
            let mut replaces = Vec::new();
            let (mut records, mut events) = (Vec::new(), Vec::new());
            let (mut record_ids, mut event_ids) = (HashSet::new(), HashSet::new());
            for (id, source, m) in &group {
                replaces.push(id.clone());
                replaces.extend(m.replaces.iter().cloned());
                records.extend(
                    read_records(source)
                        .await?
                        .into_iter()
                        .filter(|r| record_ids.insert(r.id.clone())),
                );
                events.extend(
                    read_events(source)
                        .await?
                        .into_iter()
                        .filter(|e| event_ids.insert(e.id.clone())),
                );
            }
            replaces.sort();
            replaces.dedup();
            write_archive_replacing(
                &path,
                &state.collector_id,
                &batch,
                &records,
                &events,
                replaces,
            )
            .await?;
        }
        state.finish_compaction(&batch, &path, &members)?;
        merged += group.len();
    }
    Ok(merged)
}

fn quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}
fn record_filter(q: &Query) -> Option<String> {
    let mut predicates = Vec::new();
    for (column, value) in [("harness", &q.harness), ("session_id", &q.session)] {
        if let Some(value) = value {
            predicates.push(format!("{column} = {}", quote(value)));
        }
    }
    (!predicates.is_empty()).then(|| predicates.join(" AND "))
}
fn event_filter(q: &Query) -> Option<String> {
    let mut predicates = Vec::new();
    for (column, op, value) in [
        ("id", "=", &q.event_id),
        ("harness", "=", &q.harness),
        ("session_id", "=", &q.session),
        ("kind", "=", &q.kind),
        ("timestamp", ">=", &q.since),
        ("timestamp", "<=", &q.until),
    ] {
        if let Some(value) = value {
            predicates.push(format!("{column} {op} {}", quote(value)));
        }
    }
    if let Some(text) = &q.text {
        predicates.push(format!("contains(text, {})", quote(text)));
    }
    (!predicates.is_empty()).then(|| predicates.join(" AND "))
}
async fn open(path: &Path, dataset: &str) -> Result<Dataset> {
    Ok(Dataset::open(path.join(dataset).to_str().context("UTF-8 path required")?).await?)
}
/// Opening a Lance dataset reads its metadata, so reuse it while a query runs.
#[derive(Default)]
struct Datasets(HashMap<PathBuf, Dataset>);
impl Datasets {
    async fn events(&mut self, path: &Path) -> Result<Dataset> {
        if let Some(ds) = self.0.get(path) {
            return Ok(ds.clone());
        }
        let ds = open(path, "events.lance").await?;
        self.0.insert(path.into(), ds.clone());
        Ok(ds)
    }
}

pub async fn read_events(path: &Path) -> Result<Vec<Event>> {
    scan_events(&open(path, "events.lance").await?, None).await
}
async fn scan_events(ds: &Dataset, filter: Option<String>) -> Result<Vec<Event>> {
    let mut scanner = ds.scan();
    if let Some(filter) = filter {
        scanner.filter(&filter)?;
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
    read_records_where(path, None).await
}
async fn read_records_where(path: &Path, filter: Option<String>) -> Result<Vec<Record>> {
    let ds = open(path, "records.lance").await?;
    let mut scanner = ds.scan();
    if let Some(filter) = filter {
        scanner.filter(&filter)?;
    }
    let mut stream = scanner.try_into_stream().await?;
    let mut result = Vec::new();
    while let Some(batch) = stream.try_next().await? {
        let strings = |name: &str| -> Result<&StringArray> {
            batch
                .column_by_name(name)
                .with_context(|| format!("missing column {name}"))?
                .as_any()
                .downcast_ref::<StringArray>()
                .with_context(|| format!("invalid string column {name}"))
        };
        let (id, harness, session_id, source_id, source_path, captured_at, status) = (
            strings("id")?,
            strings("harness")?,
            strings("session_id")?,
            strings("source_id")?,
            strings("source_path")?,
            strings("captured_at")?,
            strings("status")?,
        );
        let (diagnostic, adapter_version) = (strings("diagnostic")?, strings("adapter_version")?);
        let position = batch
            .column_by_name("position")
            .context("missing position")?
            .as_any()
            .downcast_ref::<UInt64Array>()
            .context("invalid position")?;
        let raw = batch
            .column_by_name("raw")
            .context("missing raw")?
            .as_any()
            .downcast_ref::<BinaryArray>()
            .context("invalid raw")?;
        for i in 0..batch.num_rows() {
            result.push(Record {
                id: id.value(i).into(),
                harness: harness.value(i).into(),
                session_id: session_id.value(i).into(),
                source_id: source_id.value(i).into(),
                source_path: source_path.value(i).into(),
                position: position.value(i),
                captured_at: captured_at.value(i).into(),
                status: status.value(i).into(),
                diagnostic: (!diagnostic.is_null(i)).then(|| diagnostic.value(i).into()),
                adapter_version: adapter_version.value(i).into(),
                raw: raw.value(i).to_vec(),
            });
        }
    }
    Ok(result)
}
/// Record identities without the raw payload column.
async fn read_record_keys(path: &Path, q: &Query) -> Result<Vec<(String, String, String)>> {
    let ds = open(path, "records.lance").await?;
    let mut scanner = ds.scan();
    scanner.project(&["id", "harness", "session_id"])?;
    if let Some(filter) = record_filter(q) {
        scanner.filter(&filter)?;
    }
    let mut stream = scanner.try_into_stream().await?;
    let mut result = Vec::new();
    while let Some(batch) = stream.try_next().await? {
        let column = |name: &str| -> Result<&StringArray> {
            batch
                .column_by_name(name)
                .context("missing column")?
                .as_any()
                .downcast_ref::<StringArray>()
                .context("invalid string column")
        };
        let (id, harness, session) = (column("id")?, column("harness")?, column("session_id")?);
        for i in 0..batch.num_rows() {
            result.push((
                id.value(i).into(),
                harness.value(i).into(),
                session.value(i).into(),
            ));
        }
    }
    Ok(result)
}

/// A hook payload is the hook document itself, so the key is at its top level.
fn from_hook(e: &Event) -> bool {
    serde_json::from_str::<serde_json::Value>(&e.payload_json)
        .is_ok_and(|v| v.get("hook_event_name").is_some_and(|n| n.is_string()))
}
fn correlated(e: &Event) -> bool {
    (e.kind == "tool_call" || e.kind == "tool_result") && e.tool_call_id.is_some()
}
fn chronology(e: &Event) -> (&str, u64, u32) {
    (&e.source_id, e.position, e.sub_index)
}
/// A hook may enrich the same tool result already present in a transcript.
/// Prefer transcript events; retain every raw record and all uncorrelated calls.
fn collapse_hook_duplicates(events: Vec<Event>) -> Vec<Event> {
    let mut keyed: Vec<_> = events
        .into_iter()
        .map(|e| (correlated(&e) && from_hook(&e), e))
        .collect();
    keyed.sort_by(|(a_hook, a), (b_hook, b)| (a_hook, chronology(a)).cmp(&(b_hook, chronology(b))));
    let mut tools = HashSet::new();
    let mut events: Vec<_> = keyed
        .into_iter()
        .map(|(_, e)| e)
        .filter(|e| !correlated(e) || tools.insert((e.kind.clone(), e.tool_call_id.clone())))
        .collect();
    events.sort_by(|a, b| chronology(a).cmp(&chronology(b)));
    events
}

/// Emit matching events one session at a time, ordered by harness, session, source,
/// position, and content-block index. Memory is bounded by the largest session.
pub async fn query_each(
    paths: &[PathBuf],
    q: &Query,
    mut emit: impl FnMut(Event) -> Result<()>,
) -> Result<()> {
    let mut sessions: BTreeMap<(String, String), Vec<&PathBuf>> = BTreeMap::new();
    for path in paths {
        for s in manifest(path)?.sessions {
            if q.matches_session(&s) {
                let list = sessions.entry((s.harness, s.session_id)).or_default();
                if !list.contains(&path) {
                    list.push(path);
                }
            }
        }
    }
    let mut datasets = Datasets::default();
    for ((harness, session), paths) in sessions {
        let scoped = Query {
            harness: Some(harness),
            session: Some(session),
            ..q.clone()
        };
        let mut seen = HashSet::new();
        let mut events = Vec::new();
        for path in paths {
            let ds = datasets.events(path).await?;
            for e in scan_events(&ds, event_filter(&scoped)).await? {
                if seen.insert(e.id.clone()) && scoped.matches(&e) {
                    events.push(e);
                }
            }
        }
        for e in collapse_hook_duplicates(events) {
            emit(e)?;
        }
    }
    Ok(())
}
pub async fn query(paths: &[PathBuf], q: &Query) -> Result<Vec<Event>> {
    let mut events = Vec::new();
    query_each(paths, q, |e| {
        events.push(e);
        Ok(())
    })
    .await?;
    Ok(events)
}
/// Find one event by its stable ID without reading other events.
pub async fn find_event(paths: &[PathBuf], q: &Query, id: &str) -> Result<Option<Event>> {
    let q = Query {
        event_id: Some(id.into()),
        ..q.clone()
    };
    for path in paths {
        if !manifest(path)?
            .sessions
            .iter()
            .any(|s| q.matches_session(s))
        {
            continue;
        }
        let events = scan_events(&open(path, "events.lance").await?, event_filter(&q)).await?;
        if let Some(e) = events.into_iter().find(|e| q.matches(e)) {
            return Ok(Some(e));
        }
    }
    Ok(None)
}
fn filters_events(q: &Query) -> bool {
    q.kind.is_some()
        || q.text.is_some()
        || q.since.is_some()
        || q.until.is_some()
        || q.event_id.is_some()
}
/// Session counts deduplicate record IDs and correlated tool events. With an event
/// filter, only sessions that have a matching event are returned.
pub async fn session_summaries(paths: &[PathBuf], q: &Query) -> Result<Vec<SessionSummary>> {
    let mut map = BTreeMap::new();
    let mut seen = HashSet::new();
    for path in paths {
        if !manifest(path)?
            .sessions
            .iter()
            .any(|s| q.matches_session(s))
        {
            continue;
        }
        for (id, harness, session) in read_record_keys(path, q).await? {
            if seen.insert(id) {
                map.entry((harness.clone(), session.clone()))
                    .or_insert_with(|| empty_summary(&harness, &session))
                    .records += 1;
            }
        }
    }
    let mut matched = HashSet::new();
    query_each(paths, q, |e| {
        let key = (e.harness.clone(), e.session_id.clone());
        if let Some(s) = map.get_mut(&key) {
            count_event(s, &e);
        }
        matched.insert(key);
        Ok(())
    })
    .await?;
    let filtered = filters_events(q);
    Ok(map
        .into_iter()
        .filter(|(key, _)| !filtered || matched.contains(key))
        .map(|(_, s)| s)
        .collect())
}
/// Write the selected records and events as one standalone archive.
pub async fn export(
    paths: &[PathBuf],
    q: &Query,
    destination: &Path,
    collector: &str,
) -> Result<Manifest> {
    let events = query(paths, q).await?;
    let ids: HashSet<_> = events.iter().map(|e| e.record_id.as_str()).collect();
    let filtered = filters_events(q);
    let mut records = Vec::new();
    let mut seen = HashSet::new();
    for path in paths {
        if !manifest(path)?
            .sessions
            .iter()
            .any(|s| q.matches_session(s))
        {
            continue;
        }
        for r in read_records_where(path, record_filter(q)).await? {
            if (!filtered || ids.contains(r.id.as_str())) && seen.insert(r.id.clone()) {
                records.push(r);
            }
        }
    }
    write_archive(
        destination,
        collector,
        &uuid::Uuid::new_v4().to_string(),
        &records,
        &events,
    )
    .await
}
