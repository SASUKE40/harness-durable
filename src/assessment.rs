//! Matching policies and explicit, auditable evaluation criteria.
use crate::{
    feedback::{Alignment, Step, Trajectory, align, similarities},
    model::hash,
};
use anyhow::{Context, Result, ensure};
use clap::ValueEnum;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    path::PathBuf,
};

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq, ValueEnum)]
#[serde(rename_all = "snake_case")]
pub enum MatchMode {
    #[default]
    Bm25,
    Strict,
    Unordered,
    Required,
}

fn tool_parts(p: &Value) -> Option<(Value, Value)> {
    if let Some(name) = p.get("name").or_else(|| p.get("tool_name")) {
        let args = p
            .get("arguments")
            .or_else(|| p.get("input"))
            .or_else(|| p.get("tool_input"))
            .cloned()
            .unwrap_or(Value::Null);
        let args = args
            .as_str()
            .and_then(|s| serde_json::from_str::<Value>(s).ok())
            .unwrap_or(args);
        return Some((name.clone(), args));
    }
    // Cursor CLI uses a tagged tool_call object, unlike desktop content blocks.
    if let Some(calls) = p.get("tool_call").and_then(Value::as_object)
        && calls.len() == 1
    {
        let (name, call) = calls.iter().next().unwrap();
        return Some((
            json!(name),
            call.get("args").cloned().unwrap_or(Value::Null),
        ));
    }
    None
}
fn result_content(p: &Value) -> Value {
    for key in [
        "output",
        "content",
        "result",
        "tool_output",
        "error_message",
    ] {
        if let Some(v) = p.get(key) {
            return v.clone();
        }
    }
    if let Some(calls) = p.get("tool_call").and_then(Value::as_object) {
        return calls
            .iter()
            .filter_map(|(name, v)| v.get("result").map(|r| (name.clone(), r.clone())))
            .collect::<serde_json::Map<_, _>>()
            .into();
    }
    Value::Null
}
/// Normalize tool syntax without dropping parameters or confusing distinct actions.
pub fn action_key(step: &Step) -> String {
    let e = &step.events[0];
    let p: Value = serde_json::from_str(&e.payload_json).unwrap_or(Value::Null);
    if e.kind == "tool_call" {
        match tool_parts(&p) {
            Some((name, args)) => json!({"kind":e.kind,"name":name,"arguments":args}).to_string(),
            // Unknown tool shapes must not collapse into the same null name/arguments.
            None => json!({"kind":e.kind,"payload":p,"text":e.text}).to_string(),
        }
    } else {
        json!({"kind":e.kind,"role":e.role,"text":e.text,"payload":if e.text.is_none(){Some(&p)}else{None}}).to_string()
    }
}
pub fn evidence_key(step: &Step) -> String {
    json!({"action":action_key(step),"results":step.events.iter().filter(|e|e.kind=="tool_result").map(|e|{
        let mut p:Value=serde_json::from_str(&e.payload_json).unwrap_or(Value::Null);
        if let Some(object) = p.as_object_mut() {
            for field in ["id", "call_id", "tool_use_id", "toolCallId", "session_id", "conversation_id", "generation_id", "timestamp", "timestamp_ms", "parentId"] { object.remove(field); }
        }
        json!({"text":e.text,"payload":p})
    }).collect::<Vec<_>>()}).to_string()
}

pub async fn match_steps(
    oracle: &[Step],
    candidate: &[Step],
    mode: MatchMode,
    required: &[usize],
    threshold: f64,
) -> Result<Vec<Alignment>> {
    let keys: Vec<_> = oracle.iter().map(action_key).collect();
    let mut out: Vec<_> = candidate
        .iter()
        .enumerate()
        .map(|(i, _)| Alignment {
            candidate_step: i,
            oracle_step: None,
            similarity: None,
        })
        .collect();
    match mode {
        MatchMode::Bm25 => {
            return Ok(align(
                &similarities(oracle, candidate).await?,
                oracle.len(),
                threshold,
            ));
        }
        MatchMode::Strict => {
            for (i, step) in candidate.iter().enumerate() {
                if keys.get(i) == Some(&action_key(step)) {
                    out[i].oracle_step = Some(i);
                }
            }
        }
        MatchMode::Unordered => {
            let mut positions: HashMap<String, VecDeque<usize>> = HashMap::new();
            for (i, key) in keys.iter().enumerate() {
                positions.entry(key.clone()).or_default().push_back(i);
            }
            for (i, step) in candidate.iter().enumerate() {
                out[i].oracle_step = positions
                    .get_mut(&action_key(step))
                    .and_then(VecDeque::pop_front);
            }
        }
        MatchMode::Required => {
            ensure!(
                !required.is_empty(),
                "required matching needs required_steps (1-based oracle step numbers)"
            );
            let required: HashSet<_> = required.iter().copied().collect();
            let scores: Vec<_> = candidate
                .iter()
                .map(|s| {
                    let candidate_key = action_key(s);
                    keys.iter()
                        .enumerate()
                        .map(|(j, k)| {
                            if required.contains(&(j + 1)) && *k == candidate_key {
                                1.0
                            } else {
                                0.0
                            }
                        })
                        .collect()
                })
                .collect();
            out = align(&scores, oracle.len(), 0.0);
            for a in &mut out {
                a.similarity = None;
            }
        }
    }
    Ok(out)
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Dimension {
    ToolCorrectness,
    Progress,
}
impl Dimension {
    pub fn key(self) -> &'static str {
        match self {
            Self::ToolCorrectness => "tool_correctness",
            Self::Progress => "progress",
        }
    }
    pub fn criterion(self) -> &'static str {
        match self {
            Self::ToolCorrectness => {
                "Judge only tool correctness: choice, arguments, and handling of the observed result. Reward 1 when these are justified by available evidence, otherwise 0. A failed tool execution is not automatically an incorrect choice."
            }
            Self::Progress => {
                "Judge only useful progress toward the user task. Reward 1 for a relevant step that advances or validates the solution using available evidence; reward 0 for unnecessary repetition, irrelevant work, or unsupported progress."
            }
        }
    }
    pub fn eligible(self, s: &Step) -> bool {
        self == Self::Progress || matches!(s.events[0].kind.as_str(), "tool_call" | "tool_result")
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Milestone {
    pub id: String,
    pub description: String,
    #[serde(default)]
    pub oracle_steps: Vec<usize>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum OutcomeCheck {
    FinalEquals {
        id: String,
        expected: String,
    },
    FinalContains {
        id: String,
        text: String,
    },
    ToolResultContains {
        id: String,
        tool: String,
        text: String,
    },
    ArtifactSha256 {
        id: String,
        path: PathBuf,
        sha256: String,
    },
}
impl OutcomeCheck {
    pub fn id(&self) -> &str {
        match self {
            Self::FinalEquals { id, .. }
            | Self::FinalContains { id, .. }
            | Self::ToolResultContains { id, .. }
            | Self::ArtifactSha256 { id, .. } => id,
        }
    }
    pub fn validate(&self) -> Result<()> {
        ensure!(
            !self.id().trim().is_empty(),
            "outcome check ID must not be empty"
        );
        match self {
            Self::FinalEquals { expected, .. } => {
                ensure!(!expected.is_empty(), "empty expected output")
            }
            Self::FinalContains { text, .. } => {
                ensure!(!text.is_empty(), "empty outcome substring")
            }
            Self::ToolResultContains { tool, text, .. } => {
                ensure!(
                    !tool.trim().is_empty() && !text.is_empty(),
                    "tool and outcome substring must not be empty"
                );
            }
            Self::ArtifactSha256 { path, sha256, .. } => ensure!(
                path.is_absolute()
                    && sha256.len() == 64
                    && sha256.bytes().all(|c| c.is_ascii_hexdigit()),
                "artifact checks require an absolute path and SHA256 hex digest"
            ),
        }
        Ok(())
    }
    pub fn run(&self, t: &Trajectory) -> Result<CheckResult> {
        self.validate()?;
        let (passed, evidence) = match self {
            Self::FinalEquals { expected, .. } => (
                t.final_output.as_ref() == Some(expected),
                json!({"source":t.final_output_source,"observed":t.final_output}),
            ),
            Self::FinalContains { text, .. } => (
                t.final_output.as_ref().is_some_and(|s| s.contains(text)),
                json!({"source":t.final_output_source,"observed":t.final_output}),
            ),
            Self::ToolResultContains { tool, text, .. } => {
                let ids: Vec<_> = t
                    .steps
                    .iter()
                    .filter(|s| {
                        s.events[0].kind == "tool_call"
                            && serde_json::from_str::<Value>(&s.events[0].payload_json)
                                .ok()
                                .and_then(|v| tool_parts(&v))
                                .is_some_and(|(name, _)| name == *tool)
                    })
                    .flat_map(|s| {
                        s.events
                            .iter()
                            .filter(|e| {
                                e.kind == "tool_result"
                                    && (e.text.as_ref().is_some_and(|s| s.contains(text))
                                        || serde_json::from_str::<Value>(&e.payload_json)
                                            .ok()
                                            .is_some_and(|p| {
                                                let content = result_content(&p);
                                                !content.is_null()
                                                    && content.to_string().contains(text)
                                            }))
                            })
                            .map(|e| e.id.clone())
                    })
                    .collect();
                (
                    !ids.is_empty(),
                    json!({"source":"recorded_tool_result","event_ids":ids}),
                )
            }
            Self::ArtifactSha256 { path, sha256, .. } => match crate::archive::file_digest(path) {
                Ok((size, digest)) => (
                    digest.eq_ignore_ascii_case(sha256),
                    json!({"source":"local_artifact","path":path,"size":size,"sha256":digest}),
                ),
                Err(e)
                    if e.downcast_ref::<std::io::Error>()
                        .is_some_and(|e| e.kind() == std::io::ErrorKind::NotFound) =>
                {
                    (
                        false,
                        json!({"source":"local_artifact","path":path,"missing":true}),
                    )
                }
                Err(e) => return Err(e).context("reading outcome artifact"),
            },
        };
        Ok(CheckResult {
            id: self.id().into(),
            passed,
            evidence,
        })
    }
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CheckResult {
    pub id: String,
    pub passed: bool,
    pub evidence: Value,
}
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Criteria {
    /// Explicit structural milestones, numbered from one in the oracle.
    pub required_steps: Vec<usize>,
    pub milestones: Vec<Milestone>,
    pub outcome_checks: Vec<OutcomeCheck>,
}
impl Criteria {
    pub fn validate(&self, oracle_len: usize) -> Result<()> {
        let mut steps = HashSet::new();
        for n in &self.required_steps {
            ensure!(
                *n > 0 && *n <= oracle_len && steps.insert(*n),
                "invalid or duplicate required oracle step {n}"
            );
        }
        let mut ids = HashSet::new();
        for m in &self.milestones {
            ensure!(
                !m.id.trim().is_empty() && !m.description.trim().is_empty() && ids.insert(&m.id),
                "milestones need unique IDs and nonempty descriptions"
            );
            ensure!(
                m.oracle_steps.iter().all(|n| *n > 0 && *n <= oracle_len),
                "invalid oracle step in milestone {}",
                m.id
            );
        }
        let mut ids = HashSet::new();
        for c in &self.outcome_checks {
            c.validate()?;
            ensure!(ids.insert(c.id()), "duplicate outcome check ID");
        }
        Ok(())
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Diagnostics {
    pub reference_step_coverage: f64,
    pub required_steps_passed: Vec<usize>,
    pub required_steps_missing: Vec<usize>,
    pub repeated_candidate_steps: Vec<usize>,
    pub repetition_ratio: f64,
    pub outcome_checks: Vec<CheckResult>,
    pub outcome_passed: Option<bool>,
}
pub fn diagnostics(
    oracle: &Trajectory,
    candidate: &Trajectory,
    alignment: &[Alignment],
    criteria: &Criteria,
) -> Result<Diagnostics> {
    let matched: HashSet<_> = alignment.iter().filter_map(|a| a.oracle_step).collect();
    // Required actions use one-to-one ordered matching even in BM25 mode. A
    // lexical match alone never satisfies an explicit structural requirement.
    let required =
        match_steps_exact_required(&oracle.steps, &candidate.steps, &criteria.required_steps);
    let passed: Vec<_> = criteria
        .required_steps
        .iter()
        .copied()
        .filter(|n| required.contains(n))
        .collect();
    let missing = criteria
        .required_steps
        .iter()
        .copied()
        .filter(|n| !required.contains(n))
        .collect();
    let mut seen = HashSet::new();
    let repeated: Vec<_> = candidate
        .steps
        .iter()
        .enumerate()
        .filter_map(|(i, s)| (!seen.insert(action_key(s))).then_some(i + 1))
        .collect();
    let outcome_checks = criteria
        .outcome_checks
        .iter()
        .map(|c| c.run(candidate))
        .collect::<Result<Vec<_>>>()?;
    Ok(Diagnostics {
        reference_step_coverage: matched.len() as f64 / oracle.steps.len() as f64,
        required_steps_passed: passed,
        required_steps_missing: missing,
        repetition_ratio: repeated.len() as f64 / candidate.steps.len() as f64,
        repeated_candidate_steps: repeated,
        outcome_passed: (!outcome_checks.is_empty())
            .then(|| outcome_checks.iter().all(|c| c.passed)),
        outcome_checks,
    })
}
fn match_steps_exact_required(
    oracle: &[Step],
    candidate: &[Step],
    required: &[usize],
) -> HashSet<usize> {
    if required.is_empty() {
        return HashSet::new();
    }
    let keys: Vec<_> = oracle.iter().map(action_key).collect();
    let required: HashSet<_> = required.iter().copied().collect();
    let scores: Vec<_> = candidate
        .iter()
        .map(|s| {
            let candidate_key = action_key(s);
            keys.iter()
                .enumerate()
                .map(|(i, k)| {
                    if required.contains(&(i + 1)) && *k == candidate_key {
                        1.0
                    } else {
                        0.0
                    }
                })
                .collect()
        })
        .collect();
    align(&scores, oracle.len(), 0.0)
        .iter()
        .filter_map(|a| a.oracle_step.map(|i| i + 1))
        .collect()
}
pub fn trajectory_id(t: &Trajectory) -> Result<String> {
    Ok(hash(serde_json::to_vec(t)?))
}
