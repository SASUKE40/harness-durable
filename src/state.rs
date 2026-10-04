use crate::{
    adapters::{self, SessionAdapter, Source, adapter, parse},
    model::{Event, Record, hash},
};
use anyhow::{Context, Result};
use fs2::FileExt;
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use std::{
    fs::{File, Metadata, OpenOptions},
    io::{BufRead, BufReader, Read, Seek, SeekFrom},
    path::{Path, PathBuf},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

/// Size and modification time are trusted only when the file was last modified
/// this long before it was examined, so coarse filesystem clocks cannot hide a
/// same-size rewrite.
const SETTLED_NS: u64 = 2_000_000_000;
/// Readers can hold paths of a replaced batch; keep its files for this long.
pub const REPLACED_RETENTION: Duration = Duration::from_secs(300);
const STALE_DOWNLOAD: Duration = Duration::from_secs(3600);

pub struct State {
    pub conn: Connection,
    pub root: PathBuf,
    pub collector_id: String,
    _lock: Option<File>,
}

#[derive(Debug, Serialize, Deserialize)]
struct Checkpoint {
    offset: u64,
    prefix: String,
    tail: String,
    identity: String,
    source_id: String,
    #[serde(default)]
    size: Option<u64>,
    #[serde(default)]
    mtime_ns: Option<u64>,
    #[serde(default)]
    checked_ns: Option<u64>,
}

#[derive(Debug, Serialize)]
pub struct Status {
    pub collector_id: String,
    pub sources: u64,
    pub captured_records: u64,
    pub pending_records: u64,
    pub parse_failures: u64,
    pub unknown_records: u64,
    pub batches: u64,
    pub uploads: Vec<UploadStatus>,
}
#[derive(Debug, Serialize)]
pub struct UploadStatus {
    pub remote: String,
    pub pending_batches: u64,
    pub last_success: Option<String>,
}

fn window(file: &mut File, start: u64, len: u64) -> Result<String> {
    file.seek(SeekFrom::Start(start))?;
    let mut bytes = vec![0; len as usize];
    file.read_exact(&mut bytes)?;
    Ok(hash(bytes))
}
fn identity(m: &Metadata) -> String {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        format!("{}:{}", m.dev(), m.ino())
    }
    #[cfg(not(unix))]
    {
        format!("{:?}", m.created().ok())
    }
}
fn nanos(t: SystemTime) -> u64 {
    t.duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos().min(u64::MAX as u128) as u64)
}
fn mtime_ns(m: &Metadata) -> u64 {
    m.modified().map_or(0, nanos)
}
fn settled(mtime: u64, checked: Option<u64>) -> bool {
    checked.is_some_and(|c| c.saturating_sub(mtime) > SETTLED_NS)
}
fn older_than(path: &Path, age: Duration) -> bool {
    std::fs::metadata(path)
        .and_then(|m| m.modified())
        .is_ok_and(|t| t.elapsed().is_ok_and(|e| e > age))
}
fn has_table(conn: &Connection, name: &str) -> Result<bool> {
    Ok(conn.query_row(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?1",
        [name],
        |r| r.get::<_, i64>(0),
    )? > 0)
}
fn has_column(conn: &Connection, table: &str, column: &str) -> Result<bool> {
    let mut stmt = conn.prepare(&format!("PRAGMA table_info({table})"))?;
    let names = stmt.query_map([], |r| r.get::<_, String>(1))?;
    for name in names {
        if name? == column {
            return Ok(true);
        }
    }
    Ok(false)
}
/// Remove `.tmp-*` directories directly below `dir` that are at least `age` old.
fn remove_temporaries(dir: &Path, age: Duration) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if entry.file_name().to_string_lossy().starts_with(".tmp-")
            && (age.is_zero() || older_than(&path, age))
        {
            let _ = if path.is_dir() {
                std::fs::remove_dir_all(&path)
            } else {
                std::fs::remove_file(&path)
            };
        }
    }
}

impl State {
    pub fn open(root: &Path) -> Result<Self> {
        std::fs::create_dir_all(root)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(root, std::fs::Permissions::from_mode(0o700))?;
        }
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(root.join("collector.lock"))?;
        lock.try_lock_exclusive()
            .context("another harness-durable process is using this state directory")?;
        let conn = Connection::open(root.join("state.sqlite"))?;
        conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;
            CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY,value TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS sources(path TEXT PRIMARY KEY,checkpoint TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS seen(id TEXT PRIMARY KEY,status TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS pending(seq INTEGER PRIMARY KEY AUTOINCREMENT,id TEXT UNIQUE NOT NULL,record TEXT NOT NULL,events TEXT NOT NULL,bytes INTEGER NOT NULL);
            CREATE TABLE IF NOT EXISTS batches(id TEXT PRIMARY KEY,path TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS uploads(remote TEXT NOT NULL,batch TEXT NOT NULL,completed_at TEXT NOT NULL,PRIMARY KEY(remote,batch));
            CREATE TABLE IF NOT EXISTS replaced(batch TEXT PRIMARY KEY,by_batch TEXT NOT NULL,replaced_at INTEGER NOT NULL);
            CREATE TABLE IF NOT EXISTS identities(path TEXT PRIMARY KEY,signature TEXT NOT NULL,checked_ns INTEGER NOT NULL,source TEXT NOT NULL);")?;
        // Version 1 kept raw bytes inside the record JSON; those rows have a NULL raw.
        if !has_column(&conn, "pending", "raw")? {
            conn.execute_batch("ALTER TABLE pending ADD COLUMN raw BLOB")?;
        }
        conn.execute(
            "INSERT OR IGNORE INTO meta VALUES('collector_id',?1)",
            [uuid::Uuid::new_v4().to_string()],
        )?;
        let collector_id: String =
            conn.query_row("SELECT value FROM meta WHERE key='collector_id'", [], |r| {
                r.get(0)
            })?;
        // Only the lock holder writes batches, so any temporary batch is abandoned.
        remove_temporaries(&root.join("batches").join(&collector_id), Duration::ZERO);
        if let Ok(caches) = std::fs::read_dir(root.join("cache")) {
            for cache in caches.flatten() {
                remove_temporaries(&cache.path(), STALE_DOWNLOAD);
            }
        }
        let state = Self {
            conn,
            root: root.into(),
            collector_id,
            _lock: Some(lock),
        };
        state.purge_replaced(REPLACED_RETENTION)?;
        Ok(state)
    }

    /// Read-only commands can inspect completed batches while the watcher owns
    /// the collector lock. WAL gives these connections a consistent snapshot.
    pub fn open_reader(root: &Path) -> Result<Self> {
        let conn = Connection::open_with_flags(
            root.join("state.sqlite"),
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        )?;
        let collector_id =
            conn.query_row("SELECT value FROM meta WHERE key='collector_id'", [], |r| {
                r.get(0)
            })?;
        Ok(Self {
            conn,
            root: root.into(),
            collector_id,
            _lock: None,
        })
    }

    /// Identify a source, reusing a cached header when the file did not change.
    pub fn identify(&self, a: &dyn SessionAdapter, path: &Path) -> Result<Source> {
        let meta = std::fs::metadata(path)?;
        let mtime = mtime_ns(&meta);
        let signature = format!("{}:{}:{mtime}", identity(&meta), meta.len());
        let key = path.to_string_lossy();
        let cached: Option<(String, u64, String)> = self
            .conn
            .query_row(
                "SELECT signature,checked_ns,source FROM identities WHERE path=?1",
                [&key],
                |r| Ok((r.get(0)?, r.get::<_, i64>(1)? as u64, r.get(2)?)),
            )
            .optional()?;
        if let Some((saved, checked, source)) = cached
            && saved == signature
            && settled(mtime, Some(checked))
            && let Ok(source) = serde_json::from_str::<Source>(&source)
            && source.harness == a.name()
        {
            return Ok(source);
        }
        let source = adapters::identify(a, path)?;
        self.conn.execute(
            "INSERT OR REPLACE INTO identities VALUES(?1,?2,?3,?4)",
            params![
                key,
                signature,
                nanos(SystemTime::now()) as i64,
                serde_json::to_string(&source)?
            ],
        )?;
        Ok(source)
    }

    /// Read a bounded group; caller repeats until caught up. Trailing partial lines
    /// remain in the source, with the checkpoint before them.
    pub fn ingest(
        &mut self,
        source: &Source,
        max_records: usize,
        max_bytes: usize,
    ) -> Result<usize> {
        let key = source.path.to_string_lossy().to_string();
        let saved: Option<Checkpoint> = self
            .conn
            .query_row(
                "SELECT checkpoint FROM sources WHERE path=?1",
                [&key],
                |r| r.get::<_, String>(0),
            )
            .optional()?
            .map(|s| serde_json::from_str(&s))
            .transpose()?;
        if let Some(c) = &saved {
            let meta = std::fs::metadata(&source.path)?;
            let mtime = mtime_ns(&meta);
            if c.source_id == source.source_id
                && c.identity == identity(&meta)
                && c.offset == meta.len()
                && c.size == Some(meta.len())
                && c.mtime_ns == Some(mtime)
                && settled(mtime, c.checked_ns)
            {
                return Ok(0);
            }
        }
        let mut file = File::open(&source.path)?;
        let meta = file.metadata()?;
        let size = meta.len();
        let identity = identity(&meta);
        let mut offset = 0;
        if let Some(c) = saved
            && c.source_id == source.source_id
            && c.identity == identity
            && c.offset <= size
        {
            let len = c.offset.min(4096);
            if c.prefix == window(&mut file, 0, len)?
                && c.tail == window(&mut file, c.offset - len, len)?
            {
                offset = c.offset;
            }
        }
        file.seek(SeekFrom::Start(offset))?;
        let mut reader = BufReader::new(file);
        let a = adapter(&source.harness)?;
        let mut captured = Vec::new();
        let mut bytes = 0;
        while captured.len() < max_records && bytes < max_bytes {
            let mut raw = Vec::new();
            let n = reader.read_until(b'\n', &mut raw)?;
            if n == 0 || raw.last() != Some(&b'\n') {
                break;
            }
            let raw = if source.format == "hooks" && source.path.with_extension("stdin").exists() {
                std::fs::read(source.path.with_extension("stdin"))?
            } else {
                raw
            };
            let (record, events) = parse(a.as_ref(), source, offset, raw);
            offset += n as u64;
            bytes += n;
            captured.push((record, events));
        }
        let count = captured.len();
        let mut file = reader.into_inner();
        let len = offset.min(4096);
        let checkpoint = Checkpoint {
            offset,
            prefix: window(&mut file, 0, len)?,
            tail: window(&mut file, offset - len, len)?,
            identity,
            source_id: source.source_id.clone(),
            size: Some(size),
            mtime_ns: Some(mtime_ns(&meta)),
            checked_ns: Some(nanos(SystemTime::now())),
        };
        let tx = self.conn.transaction()?;
        for (mut record, events) in captured {
            let fresh = tx.execute(
                "INSERT OR IGNORE INTO seen VALUES(?1,?2)",
                params![record.id, record.status],
            )?;
            if fresh > 0 {
                if let Some(d) = &record.diagnostic {
                    eprintln!("{}:{}: {}", source.path.display(), record.position, d);
                }
                let raw = std::mem::take(&mut record.raw);
                let record_json = serde_json::to_string(&record)?;
                let events_json = serde_json::to_string(&events)?;
                let bytes = record_json.len() + events_json.len() + raw.len();
                tx.execute(
                    "INSERT INTO pending(id,record,events,bytes,raw) VALUES(?1,?2,?3,?4,?5)",
                    params![record.id, record_json, events_json, bytes as i64, raw],
                )?;
            }
        }
        tx.execute("INSERT INTO sources VALUES(?1,?2) ON CONFLICT(path) DO UPDATE SET checkpoint=excluded.checkpoint",params![key,serde_json::to_string(&checkpoint)?])?;
        tx.commit()?;
        Ok(count)
    }

    pub fn pending(
        &self,
        max_records: usize,
        max_bytes: usize,
    ) -> Result<(Vec<Record>, Vec<Event>)> {
        let mut stmt = self
            .conn
            .prepare("SELECT record,events,bytes,raw FROM pending ORDER BY seq LIMIT ?1")?;
        let rows = stmt.query_map([max_records as i64], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, i64>(2)? as usize,
                r.get::<_, Option<Vec<u8>>>(3)?,
            ))
        })?;
        let mut records = Vec::new();
        let mut events = Vec::new();
        let mut bytes = 0;
        for row in rows {
            let (r, e, n, raw) = row?;
            let mut record: Record = serde_json::from_str(&r)?;
            if let Some(raw) = raw {
                record.raw = raw;
            }
            records.push(record);
            events.extend(serde_json::from_str::<Vec<Event>>(&e)?);
            bytes += n;
            if bytes >= max_bytes {
                break;
            }
        }
        Ok((records, events))
    }
    /// Pending row count and stored bytes (record JSON, event JSON, and raw bytes).
    pub fn pending_size(&self) -> Result<(usize, usize)> {
        Ok(self.conn.query_row(
            "SELECT COUNT(*),COALESCE(SUM(bytes),0) FROM pending",
            [],
            |r| Ok((r.get::<_, i64>(0)? as usize, r.get::<_, i64>(1)? as usize)),
        )?)
    }
    pub fn finish_batch(&mut self, id: &str, path: &Path, records: &[Record]) -> Result<()> {
        let tx = self.conn.transaction()?;
        tx.execute(
            "INSERT OR IGNORE INTO batches VALUES(?1,?2)",
            params![id, path.to_string_lossy()],
        )?;
        for r in records {
            tx.execute("DELETE FROM pending WHERE id=?1", [&r.id])?;
        }
        tx.commit()?;
        Ok(())
    }
    pub fn finish_compaction(&mut self, id: &str, path: &Path, members: &[&str]) -> Result<()> {
        let now = nanos(SystemTime::now()) as i64;
        let tx = self.conn.transaction()?;
        tx.execute(
            "INSERT OR IGNORE INTO batches VALUES(?1,?2)",
            params![id, path.to_string_lossy()],
        )?;
        for member in members {
            tx.execute(
                "INSERT OR IGNORE INTO replaced VALUES(?1,?2,?3)",
                params![member, id, now],
            )?;
        }
        tx.commit()?;
        Ok(())
    }
    /// Delete the files of batches that a compaction replaced at least `age` ago.
    pub fn purge_replaced(&self, age: Duration) -> Result<usize> {
        let before = nanos(SystemTime::now()).saturating_sub(age.as_nanos() as u64) as i64;
        let mut stmt = self.conn.prepare(
            "SELECT b.path FROM batches b JOIN replaced r ON r.batch=b.id WHERE r.replaced_at<=?1",
        )?;
        let paths = stmt
            .query_map([before], |r| r.get::<_, String>(0))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        let mut removed = 0;
        for path in paths.iter().map(PathBuf::from).filter(|p| p.exists()) {
            std::fs::remove_dir_all(&path)?;
            removed += 1;
        }
        Ok(removed)
    }
    /// Published batches that no compaction replaced, in creation order.
    pub fn active_batches(&self) -> Result<Vec<(String, PathBuf)>> {
        let sql = if has_table(&self.conn, "replaced")? {
            "SELECT id,path FROM batches WHERE id NOT IN (SELECT batch FROM replaced) ORDER BY rowid"
        } else {
            "SELECT id,path FROM batches ORDER BY rowid"
        };
        let mut stmt = self.conn.prepare(sql)?;
        Ok(stmt
            .query_map([], |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    PathBuf::from(r.get::<_, String>(1)?),
                ))
            })?
            .collect::<rusqlite::Result<_>>()?)
    }
    pub fn batch_paths(&self) -> Result<Vec<PathBuf>> {
        Ok(self.active_batches()?.into_iter().map(|(_, p)| p).collect())
    }
    /// Delete hook spool files whose single record is already in a published batch.
    pub fn purge_hook_spool(&self, spool: &Path) -> Result<usize> {
        if self.pending_size()?.0 > 0 {
            return Ok(0);
        }
        let mut stmt = self.conn.prepare("SELECT path,checkpoint FROM sources")?;
        let rows = stmt
            .query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        let mut removed = 0;
        for (key, checkpoint) in rows {
            let path = PathBuf::from(&key);
            if !path.starts_with(spool) || path.extension().is_none_or(|e| e != "jsonl") {
                continue;
            }
            let c: Checkpoint = serde_json::from_str(&checkpoint)?;
            match std::fs::metadata(&path) {
                Ok(m) if m.len() == c.offset && c.offset > 0 => {
                    std::fs::remove_file(&path)?;
                    let _ = std::fs::remove_file(path.with_extension("stdin"));
                    if let Some(parent) = path.parent() {
                        let _ = std::fs::remove_dir(parent);
                    }
                    removed += 1;
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                _ => continue,
            }
            self.conn
                .execute("DELETE FROM sources WHERE path=?1", [&key])?;
            self.conn
                .execute("DELETE FROM identities WHERE path=?1", [&key])?;
        }
        Ok(removed)
    }
    pub fn uploaded(&self, remote: &str, batch: &str) -> Result<bool> {
        Ok(self.conn.query_row(
            "SELECT COUNT(*) FROM uploads WHERE remote=?1 AND batch=?2",
            params![remote, batch],
            |r| r.get::<_, i64>(0),
        )? > 0)
    }
    pub fn mark_uploaded(&self, remote: &str, batch: &str) -> Result<()> {
        self.conn.execute(
            "INSERT OR REPLACE INTO uploads VALUES(?1,?2,?3)",
            params![remote, batch, chrono::Utc::now().to_rfc3339()],
        )?;
        Ok(())
    }
    pub fn status(&self, remotes: &[String]) -> Result<Status> {
        let count = |sql: &str| -> Result<u64> {
            Ok(self.conn.query_row(sql, [], |r| r.get::<_, i64>(0))? as u64)
        };
        let active = self.active_batches()?;
        let mut uploads = Vec::new();
        for remote in remotes {
            let mut pending = 0;
            for (id, _) in &active {
                if !self.uploaded(remote, id)? {
                    pending += 1;
                }
            }
            let last = self.conn.query_row(
                "SELECT MAX(completed_at) FROM uploads WHERE remote=?1",
                [remote],
                |r| r.get::<_, Option<String>>(0),
            )?;
            uploads.push(UploadStatus {
                remote: remote.clone(),
                pending_batches: pending,
                last_success: last,
            });
        }
        Ok(Status {
            collector_id: self.collector_id.clone(),
            sources: count("SELECT COUNT(*) FROM sources")?,
            captured_records: count("SELECT COUNT(*) FROM seen")?,
            pending_records: self.pending_size()?.0 as u64,
            parse_failures: count("SELECT COUNT(*) FROM seen WHERE status='malformed'")?,
            unknown_records: count("SELECT COUNT(*) FROM seen WHERE status='unknown'")?,
            batches: active.len() as u64,
            uploads,
        })
    }
}
