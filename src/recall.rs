//! Shared local session discovery for the terminal browser and MCP tools.
use crate::{
    archive, feedback,
    model::{Event, Query},
};
use anyhow::{Context, Result, bail, ensure};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    io::{BufRead, Write},
    path::{Path, PathBuf},
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionRow {
    pub harness: String,
    pub session_id: String,
    pub events: usize,
    pub preview: String,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Pair {
    pub oracle_session: String,
    pub oracle_harness: String,
    pub candidate_session: String,
    pub candidate_harness: String,
    pub archive: Option<PathBuf>,
    pub remote: Option<String>,
}
/// Case-insensitive session search over text, harness, and session ID.
struct SessionIndex {
    filter: String,
    rows: BTreeMap<(String, String), SessionRow>,
    matching: std::collections::HashSet<(String, String)>,
}
impl SessionIndex {
    fn new(filter: &str) -> Self {
        Self {
            filter: filter.to_lowercase(),
            rows: BTreeMap::new(),
            matching: Default::default(),
        }
    }
    fn add(&mut self, e: &Event) {
        let key = (e.harness.clone(), e.session_id.clone());
        if self.filter.is_empty()
            || e.text
                .as_ref()
                .is_some_and(|t| t.to_lowercase().contains(&self.filter))
            || e.harness.to_lowercase().contains(&self.filter)
            || e.session_id.to_lowercase().contains(&self.filter)
        {
            self.matching.insert(key.clone());
        }
        let row = self.rows.entry(key).or_insert_with(|| SessionRow {
            harness: e.harness.clone(),
            session_id: e.session_id.clone(),
            events: 0,
            preview: String::new(),
        });
        row.events += 1;
        if row.preview.is_empty() && e.kind == "message" {
            row.preview = e.text.as_deref().unwrap_or("").chars().take(100).collect();
        }
    }
    fn rows(self) -> Vec<SessionRow> {
        let matching = self.matching;
        self.rows
            .into_iter()
            .filter_map(|(key, row)| matching.contains(&key).then_some(row))
            .collect()
    }
}
pub fn sessions(events: &[Event], filter: &str) -> Vec<SessionRow> {
    let mut index = SessionIndex::new(filter);
    for e in events {
        index.add(e);
    }
    index.rows()
}
pub fn safe_terminal(text: &str) -> String {
    text.chars()
        .flat_map(|c| {
            if c.is_control() && c != '\n' && c != '\t' {
                c.escape_default().collect::<Vec<_>>()
            } else {
                vec![c]
            }
        })
        .collect()
}

/// A portable, line-oriented terminal UI: no raw-mode dependencies or hidden model calls.
pub async fn browse(
    events: &[Event],
    archive: Option<PathBuf>,
    remote: Option<String>,
    pair_path: &Path,
    input: &mut impl BufRead,
    out: &mut impl Write,
) -> Result<()> {
    let mut rows = sessions(events, "");
    let mut offset = 0usize;
    let mut oracle: Option<SessionRow> = None;
    let mut candidate: Option<SessionRow> = None;
    writeln!(
        out,
        "Session browser — list [text], next, prev, show N, oracle N, candidate N, save, preview, quit"
    )?;
    loop {
        for (i, row) in rows.iter().enumerate().skip(offset).take(15) {
            writeln!(
                out,
                "{}. {} {} ({} events) {}",
                i + 1,
                safe_terminal(&row.harness),
                safe_terminal(&row.session_id),
                row.events,
                safe_terminal(&row.preview).replace('\n', " ")
            )?;
        }
        write!(
            out,
            "{} sessions; oracle={} candidate={} > ",
            rows.len(),
            oracle
                .as_ref()
                .map(|r| safe_terminal(&r.session_id))
                .unwrap_or_else(|| "unselected".into()),
            candidate
                .as_ref()
                .map(|r| safe_terminal(&r.session_id))
                .unwrap_or_else(|| "unselected".into())
        )?;
        out.flush()?;
        let mut line = String::new();
        if input.read_line(&mut line)? == 0 {
            break;
        }
        let (command, arg) = line.trim().split_once(' ').unwrap_or((line.trim(), ""));
        let result:Result<()>=async {
            match command {
                "quit"|"q"=>{},
                "list"=>{rows=sessions(events,arg);offset=0;},
                "next"=>{if offset+15<rows.len(){offset+=15;}},
                "prev"=>{offset=offset.saturating_sub(15);},
                "show"|"oracle"|"candidate"=>{
                    let n:usize=arg.parse().context("use a displayed session number")?;
                    let row=rows.get(n.checked_sub(1).context("session numbers start at 1")?).context("session number out of range")?.clone();
                    if command=="oracle"{oracle=Some(row);}else if command=="candidate"{candidate=Some(row);}else{
                        for e in events.iter().filter(|e|e.harness==row.harness && e.session_id==row.session_id){
                            writeln!(out,"{} {} {}\n{}",safe_terminal(&e.id),safe_terminal(&e.kind),safe_terminal(e.role.as_deref().unwrap_or("")),safe_terminal(e.text.as_deref().unwrap_or(&e.payload_json)))?;
                        }
                    }
                }
                "save"|"preview"=>{
                    let a=oracle.as_ref().context("select oracle N first")?;let b=candidate.as_ref().context("select candidate N first")?;
                    if command=="save"{
                        let pair=Pair{oracle_session:a.session_id.clone(),oracle_harness:a.harness.clone(),candidate_session:b.session_id.clone(),candidate_harness:b.harness.clone(),archive:archive.clone(),remote:remote.clone()};
                        let parent=pair_path.parent().context("pair file needs parent")?;std::fs::create_dir_all(parent)?;
                        // Explicit save is the only mutation in the browser; refuses overwrites.
                        ensure!(!pair_path.exists(),"pair file already exists; choose another --pair-output");
                        crate::feedback::save(pair_path,&serde_json::to_vec_pretty(&pair)?)?;
                        writeln!(out,"Saved {}. Use compare --pair FILE --output DIR [--dry-run].",pair_path.display())?;
                    }else{
                        let task=|row:&SessionRow|feedback::trajectory(events.iter().filter(|e|e.harness==row.harness && e.session_id==row.session_id).cloned().collect(),None,None);
                        let p=feedback::plan(task(a)?,task(b)?,feedback::FeedbackConfig::default()).await?;
                        writeln!(out,"{}",serde_json::to_string_pretty(&json!({"alignment":p.alignment,"diagnostics":p.diagnostics,"notice":"Dry run: no model rewards or report. Alignment indices are zero-based."}))?)?;
                    }
                }
                _=>bail!("unknown command; use list, next, prev, show, oracle, candidate, save, preview, quit"),
            }
            Ok(())
        }.await;
        if let Err(e) = result {
            writeln!(out, "Error: {}", safe_terminal(&format!("{e:#}")))?;
        }
        if matches!(command, "quit" | "q") {
            break;
        }
    }
    Ok(())
}

pub async fn tool_call(paths: &[PathBuf], name: &str, args: Value) -> Result<Value> {
    #[derive(Deserialize, Default)]
    #[serde(default, deny_unknown_fields)]
    struct Args {
        text: String,
        harness: Option<String>,
        session: Option<String>,
        event_id: Option<String>,
        offset: usize,
        limit: Option<usize>,
    }
    let a: Args = serde_json::from_value(args)?;
    let limit = a.limit.unwrap_or(20);
    ensure!((1..=100).contains(&limit), "limit must be 1..100");
    let q = Query {
        harness: a.harness,
        session: a.session.clone(),
        ..Query::default()
    };
    ensure!(
        matches!(name, "search_sessions" | "read_session" | "get_event"),
        "unknown tool"
    );
    if name == "read_session" {
        ensure!(a.session.is_some(), "read_session requires session");
    }
    if name == "get_event" {
        ensure!(a.event_id.is_some(), "get_event requires event_id");
    }
    let values: Vec<Value> = match name {
        "search_sessions" => {
            let mut index = SessionIndex::new(&a.text);
            archive::query_each(paths, &q, |e| {
                index.add(&e);
                Ok(())
            })
            .await?;
            index
                .rows()
                .into_iter()
                .map(serde_json::to_value)
                .collect::<std::result::Result<_, _>>()?
        }
        "read_session" => {
            let mut harnesses = std::collections::HashSet::new();
            for path in paths {
                for s in archive::manifest(path)?.sessions {
                    if q.matches_session(&s) {
                        harnesses.insert(s.harness);
                    }
                }
            }
            ensure!(
                harnesses.len() <= 1,
                "session ID is ambiguous; specify harness"
            );
            let q = Query {
                text: (!a.text.is_empty()).then(|| a.text.clone()),
                ..q
            };
            let mut values = Vec::new();
            archive::query_each(paths, &q, |e| {
                values.push(serde_json::to_value(e)?);
                Ok(())
            })
            .await?;
            values
        }
        _ => archive::find_event(paths, &q, a.event_id.as_deref().unwrap_or_default())
            .await?
            .into_iter()
            .map(serde_json::to_value)
            .collect::<std::result::Result<_, _>>()?,
    };
    let total = values.len();
    let mut items = Vec::new();
    let mut bytes = 0usize;
    for mut v in values.into_iter().skip(a.offset).take(limit) {
        let size = serde_json::to_vec(&v)?.len();
        if size > 256 * 1024 {
            v = json!({"id":v["id"],"harness":v["harness"],"session_id":v["session_id"],"kind":v["kind"],"truncated":true,"original_json_bytes":size,"text_preview":v["text"].as_str().unwrap_or("").chars().take(4096).collect::<String>(),"notice":"Large event; inspect full evidence with the local query CLI."});
        }
        let size = serde_json::to_vec(&v)?.len();
        if bytes + size > 1024 * 1024 {
            break;
        }
        bytes += size;
        items.push(v);
    }
    let next = a.offset.saturating_add(items.len());
    Ok(
        json!({"items":items,"total":total,"next_offset":if next<total{Some(next)}else{None},"source":"archived events; pending spool records are excluded"}),
    )
}
