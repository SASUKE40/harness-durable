use anyhow::{Result, bail};
use harness_durable::{
    archive,
    feedback::{self, FeedbackConfig, Judgment, Trajectory},
    llm::LanguageModel,
    model::{Event, Query},
};
use serde_json::{Value, json};
use std::sync::{
    Mutex,
    atomic::{AtomicUsize, Ordering},
};

fn quality_only() -> FeedbackConfig {
    FeedbackConfig {
        dimensions: vec![],
        ..FeedbackConfig::default()
    }
}

fn event(session: &str, position: u64, text: &str) -> Event {
    Event {
        id: format!("{session}-{position}"),
        record_id: format!("record-{session}-{position}"),
        harness: "codex".into(),
        session_id: session.into(),
        source_id: session.into(),
        position,
        kind: "message".into(),
        role: Some("assistant".into()),
        text: Some(text.into()),
        payload_json: json!({"content":text}).to_string(),
        ..Event::default()
    }
}
fn task(session: &str, texts: &[&str]) -> Trajectory {
    feedback::trajectory(
        texts
            .iter()
            .enumerate()
            .map(|(i, t)| event(session, i as u64, t))
            .collect(),
        None,
        None,
    )
    .unwrap()
}

#[tokio::test]
async fn native_bm25_and_monotonic_alignment_with_unequal_lengths() {
    let oracle = task(
        "oracle",
        &[
            "inspect parser syntax",
            "repair parser syntax",
            "execute unit tests",
            "deliver final output",
        ],
    );
    let candidate = task(
        "candidate",
        &[
            "inspect parser syntax",
            "unrelated zebra picnic",
            "execute unit tests",
            "deliver final output",
        ],
    );
    let p = feedback::plan(oracle.clone(), candidate.clone(), quality_only())
        .await
        .unwrap();
    assert_eq!(
        p.alignment
            .iter()
            .map(|a| a.oracle_step)
            .collect::<Vec<_>>(),
        vec![Some(0), None, Some(2), Some(3)]
    );
    assert_eq!(p.unmatched_oracle_steps, vec![1]);
    assert!(p.alignment[0].similarity.unwrap() > 0.0);
    assert_eq!(
        p.id,
        feedback::plan(oracle, candidate, quality_only())
            .await
            .unwrap()
            .id
    );
    // A greedy first choice (9) would lose the globally better monotonic pair (8+8).
    let alignment = feedback::align(&[vec![8., 9.], vec![0., 8.]], 2, 0.1);
    assert_eq!(
        alignment.iter().map(|a| a.oracle_step).collect::<Vec<_>>(),
        vec![Some(0), Some(1)]
    );
    let p = feedback::plan(
        task("o", &["alpha", "beta", "gamma"]),
        task("c", &["alpha", "gamma"]),
        quality_only(),
    )
    .await
    .unwrap();
    assert_eq!(p.unmatched_oracle_steps, vec![1]);
}

#[test]
fn steps_preserve_order_tools_branches_and_missing_final_output() {
    let mut call = event("c", 1, "read file");
    call.kind = "tool_call".into();
    call.tool_call_id = Some("tool".into());
    let mut result = event("c", 2, "file contents");
    result.kind = "tool_result".into();
    result.tool_call_id = Some("tool".into());
    let trajectory = feedback::trajectory(
        vec![result.clone(), call.clone(), event("c", 0, "inspect")],
        None,
        None,
    )
    .unwrap();
    assert_eq!(trajectory.steps.len(), 2);
    assert_eq!(trajectory.steps[1].events[1].id, result.id);
    assert!(trajectory.final_output.is_none());
    assert_eq!(task("c", &["a", "a"]).steps.len(), 2);
    let mut other = event("c", 3, "other");
    other.source_id = "unknown-chronology".into();
    assert!(feedback::trajectory(vec![call, other], None, None).is_err());
    let mut root = event("p", 0, "root");
    root.harness = "pi".into();
    root.native_id = Some("root".into());
    let mut a = event("p", 1, "left");
    a.harness = "pi".into();
    a.native_id = Some("left".into());
    a.parent_id = Some("root".into());
    let mut b = event("p", 2, "right");
    b.harness = "pi".into();
    b.native_id = Some("right".into());
    b.parent_id = Some("root".into());
    let events = vec![root, a, b];
    assert!(feedback::trajectory(events.clone(), None, None).is_err());
    let t = feedback::trajectory(events.clone(), Some("left"), None).unwrap();
    assert_eq!(t.steps.len(), 2);
    assert_eq!(t.final_output.as_deref(), Some("left"));
    assert!(feedback::trajectory(events, Some("missing"), None).is_err());
    assert!(feedback::trajectory(vec![], None, None).is_err());
}

#[test]
fn rewards_are_strict_binary_json() {
    for text in [
        r#"{"reward":2,"rationale":"x"}"#,
        r#"{"reward":0.5,"rationale":"x"}"#,
        r#"{"reward":true,"rationale":"x"}"#,
        r#"{"reward":1,"rationale":""}"#,
        r#"{"reward":1,"rationale":"x","extra":1}"#,
        "1",
    ] {
        assert!(Judgment::parse(text).is_err(), "accepted {text}");
    }
    assert_eq!(
        Judgment::parse(r#"{"reward":0,"rationale":"unsupported"}"#)
            .unwrap()
            .reward,
        0
    );
}
struct Mock {
    calls: AtomicUsize,
    replies: Mutex<std::collections::VecDeque<Option<String>>>,
}
impl Mock {
    fn new(replies: &[Option<&str>]) -> Self {
        Self {
            calls: AtomicUsize::new(0),
            replies: Mutex::new(replies.iter().map(|s| s.map(str::to_string)).collect()),
        }
    }
}
#[async_trait::async_trait]
impl LanguageModel for Mock {
    async fn complete(&self, _system: &str, _input: &Value) -> Result<String> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        match self.replies.lock().unwrap().pop_front().flatten() {
            Some(s) => Ok(s),
            None => bail!("simulated outage"),
        }
    }
}

#[tokio::test]
async fn resume_mean_report_and_lance_roundtrip() {
    let p = feedback::plan(
        task("o", &["inspect file", "run tests"]),
        task("c", &["inspect file", "run tests"]),
        quality_only(),
    )
    .await
    .unwrap();
    let out = tempfile::tempdir().unwrap();
    let first = Mock::new(&[Some(r#"{"reward":1,"rationale":"c-0 correct"}"#), None]);
    let reporter = Mock::new(&[Some("# Assessment\nMean: 0.5; tests lack evidence.")]);
    assert!(
        feedback::evaluate(&p, out.path(), &first, &reporter)
            .await
            .is_err()
    );
    let partial: Value =
        serde_json::from_slice(&std::fs::read(out.path().join("evaluation.json")).unwrap())
            .unwrap();
    assert_eq!(partial["scores"].as_array().unwrap().len(), 1);
    assert!(partial["mean_reward"].is_null());
    assert!(!out.path().join("archive").exists());
    let second = Mock::new(&[Some(r#"{"reward":0,"rationale":"c-1 unsupported"}"#)]);
    let result = feedback::evaluate(&p, out.path(), &second, &reporter)
        .await
        .unwrap();
    assert_eq!(result.mean_reward, Some(0.5));
    assert_eq!(second.calls.load(Ordering::SeqCst), 1);
    assert_eq!(reporter.calls.load(Ordering::SeqCst), 1);
    let events = archive::query(
        &[out.path().join("archive")],
        &Query {
            kind: Some("feedback_reward".into()),
            ..Query::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(events.len(), 2);
    let reward: Value = serde_json::from_str(&events[1].payload_json).unwrap();
    assert_eq!(reward["score"]["judgment"]["reward"], 0);
    assert_eq!(reward["candidate_event_ids"][0], "c-1");
    feedback::evaluate(&p, out.path(), &second, &reporter)
        .await
        .unwrap();
    assert_eq!(second.calls.load(Ordering::SeqCst), 1);
    assert_eq!(reporter.calls.load(Ordering::SeqCst), 1);
    let mut conflict = p.clone();
    conflict.config.rubric = "changed".into();
    assert!(
        feedback::evaluate(&conflict, out.path(), &second, &reporter)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn invalid_judge_and_report_outage_do_not_create_false_scores() {
    let p = feedback::plan(task("o", &["done"]), task("c", &["done"]), quality_only())
        .await
        .unwrap();
    let out = tempfile::tempdir().unwrap();
    let invalid = Mock::new(&[Some("reward: yes")]);
    let no_report = Mock::new(&[None]);
    assert!(
        feedback::evaluate(&p, out.path(), &invalid, &no_report)
            .await
            .is_err()
    );
    assert!(out.path().join("invalid-judge-response.txt").exists());
    assert!(!out.path().join("archive").exists());
    let judge = Mock::new(&[Some(r#"{"reward":1,"rationale":"correct"}"#)]);
    assert!(
        feedback::evaluate(&p, out.path(), &judge, &no_report)
            .await
            .is_err()
    );
    let reporter = Mock::new(&[Some("Report")]);
    let result = feedback::evaluate(&p, out.path(), &judge, &reporter)
        .await
        .unwrap();
    assert_eq!(result.mean_reward, Some(1.));
    assert_eq!(judge.calls.load(Ordering::SeqCst), 1);
    let mut small = p.clone();
    small.config.max_prompt_bytes = 10;
    let fresh = tempfile::tempdir().unwrap();
    assert!(
        feedback::evaluate(&small, fresh.path(), &judge, &reporter)
            .await
            .is_err()
    );
    assert_eq!(judge.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn real_pi_fixture_requires_and_follows_branch_ancestry() {
    use harness_durable::{adapters, state::State};
    let temp = tempfile::tempdir().unwrap();
    let mut state = State::open(temp.path()).unwrap();
    let fixture = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/pi.jsonl");
    let source = adapters::identify(adapters::adapter("pi").unwrap().as_ref(), &fixture).unwrap();
    state.ingest(&source, 1000, 8 * 1024 * 1024).unwrap();
    archive::flush(&mut state, 1000, 8 * 1024 * 1024)
        .await
        .unwrap();
    let events = archive::query(&state.batch_paths().unwrap(), &Query::default())
        .await
        .unwrap();
    assert!(feedback::trajectory(events.clone(), None, None).is_err());
    let left = feedback::trajectory(events, Some("e"), None).unwrap();
    assert_eq!(left.steps.len(), 2);
    assert_eq!(left.steps[1].events.len(), 2);
    assert!(
        !left
            .events
            .iter()
            .any(|e| e.native_id.as_deref() == Some("f"))
    );
    assert!(left.final_output.is_none());
}

struct ChronologicalJudge;
#[async_trait::async_trait]
impl LanguageModel for ChronologicalJudge {
    async fn complete(&self, _system: &str, input: &Value) -> Result<String> {
        let step = input["alignment"]["candidate_step"].as_u64().unwrap();
        if step == 0 {
            assert!(!input.to_string().contains("later outcome"));
        } else {
            assert!(input.to_string().contains("later outcome"));
        }
        Ok(r#"{"reward":1,"rationale":"observed evidence"}"#.into())
    }
}
#[tokio::test]
async fn judging_does_not_leak_future_outcomes_and_output_lock_is_exclusive() {
    use fs2::FileExt;
    let p = feedback::plan(
        task("o", &["inspect", "later outcome"]),
        task("c", &["inspect", "later outcome"]),
        quality_only(),
    )
    .await
    .unwrap();
    let output = tempfile::tempdir().unwrap();
    let lock = std::fs::File::create(output.path().join(".lock")).unwrap();
    lock.try_lock_exclusive().unwrap();
    let reporter = Mock::new(&[Some("Report")]);
    assert!(
        feedback::evaluate(&p, output.path(), &ChronologicalJudge, &reporter)
            .await
            .unwrap_err()
            .to_string()
            .contains("another evaluation")
    );
    drop(lock);
    feedback::evaluate(&p, output.path(), &ChronologicalJudge, &reporter)
        .await
        .unwrap();
}
