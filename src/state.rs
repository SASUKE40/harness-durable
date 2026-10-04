use crate::{
    adapters::{Source, adapter, parse},
    model::{Event, Record, hash},
};
use anyhow::{Context, Result};
use fs2::FileExt;
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use std::{
    fs::{File, OpenOptions},
    io::{BufRead, BufReader, Read, Seek, SeekFrom},
    path::{Path, PathBuf},
};

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
fn identity(file: &File) -> Result<String> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let m = file.metadata()?;
        Ok(format!("{}:{}", m.dev(), m.ino()))
    }
    #[cfg(not(unix))]
    {
        Ok(format!("{:?}", file.metadata()?.created().ok()))
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
            CREATE TABLE IF NOT EXISTS uploads(remote TEXT NOT NULL,batch TEXT NOT NULL,completed_at TEXT NOT NULL,PRIMARY KEY(remote,batch));")?;
        conn.execute(
            "INSERT OR IGNORE INTO meta VALUES('collector_id',?1)",
            [uuid::Uuid::new_v4().to_string()],
        )?;
        let collector_id =
            conn.query_row("SELECT value FROM meta WHERE key='collector_id'", [], |r| {
                r.get(0)
            })?;
        Ok(Self {
            conn,
            root: root.into(),
            collector_id,
            _lock: Some(lock),
        })
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

    /// Read a bounded group; caller repeats until caught up. Trailing partial lines
    /// remain in the source, with the checkpoint before them.
    pub fn ingest(
        &mut self,
        source: &Source,
        max_records: usize,
        max_bytes: usize,
    ) -> Result<usize> {
        let key = source.path.to_string_lossy().to_string();
        let saved: Option<String> = self
            .conn
            .query_row(
                "SELECT checkpoint FROM sources WHERE path=?1",
                [&key],
                |r| r.get(0),
            )
            .optional()?;
        let mut file = File::open(&source.path)?;
        let size = file.metadata()?.len();
        let identity = identity(&file)?;
        let mut offset = 0;
        if let Some(saved) = saved {
            let c: Checkpoint = serde_json::from_str(&saved)?;
            if c.source_id == source.source_id && c.identity == identity && c.offset <= size {
                let len = c.offset.min(4096);
                if c.prefix == window(&mut file, 0, len)?
                    && c.tail == window(&mut file, c.offset - len, len)?
                {
                    offset = c.offset;
                }
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
        };
        let tx = self.conn.transaction()?;
        for (record, events) in captured {
            let fresh = tx.execute(
                "INSERT OR IGNORE INTO seen VALUES(?1,?2)",
                params![record.id, record.status],
            )?;
            if fresh > 0 {
                tx.execute(
                    "INSERT INTO pending(id,record,events,bytes) VALUES(?1,?2,?3,?4)",
                    params![
                        record.id,
                        serde_json::to_string(&record)?,
                        serde_json::to_string(&events)?,
                        record.raw.len() as i64
                    ],
                )?;
                if let Some(d) = &record.diagnostic {
                    eprintln!("{}:{}: {}", source.path.display(), record.position, d);
                }
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
            .prepare("SELECT record,events,bytes FROM pending ORDER BY seq LIMIT ?1")?;
        let rows = stmt.query_map([max_records as i64], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, i64>(2)? as usize,
            ))
        })?;
        let mut records = Vec::new();
        let mut events = Vec::new();
        let mut bytes = 0;
        for row in rows {
            let (r, e, n) = row?;
            records.push(serde_json::from_str(&r)?);
            events.extend(serde_json::from_str::<Vec<Event>>(&e)?);
            bytes += n;
            if bytes >= max_bytes {
                break;
            }
        }
        Ok((records, events))
    }
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
    pub fn batch_paths(&self) -> Result<Vec<PathBuf>> {
        let mut stmt = self.conn.prepare("SELECT path FROM batches ORDER BY id")?;
        Ok(stmt
            .query_map([], |r| r.get::<_, String>(0))?
            .map(|r| r.map(PathBuf::from))
            .collect::<rusqlite::Result<_>>()?)
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
        let batches = count("SELECT COUNT(*) FROM batches")?;
        let mut uploads = Vec::new();
        for remote in remotes {
            let (n, last) = self.conn.query_row(
                "SELECT COUNT(*),MAX(completed_at) FROM uploads WHERE remote=?1",
                [remote],
                |r| Ok((r.get::<_, i64>(0)?, r.get::<_, Option<String>>(1)?)),
            )?;
            uploads.push(UploadStatus {
                remote: remote.clone(),
                pending_batches: batches.saturating_sub(n as u64),
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
            batches,
            uploads,
        })
    }
}
