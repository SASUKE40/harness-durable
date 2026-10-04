use anyhow::{Result, bail};
use harness_durable::{
    archive,
    assessment::{self, Criteria, MatchMode, Milestone, OutcomeCheck},
    feedback::{self, FeedbackConfig, Trajectory},
    llm::LanguageModel,
    mcp,
    model::{Event, Query, Record},
    recall,
    regression::{self, Gates, HumanLabel},
};
use serde_json::{Value, json};
use std::{collections::VecDeque, io::Cursor, sync::Mutex};

fn event(session: &str, position: usize, text: &str) -> Event {
    Event {
        id: format!("{session}-{position}"),
        record_id: format!("r-{session}-{position}"),
        harness: "codex".into(),
        session_id: session.into(),
        source_id: session.into(),
        position: position as u64,
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
            .map(|(i, t)| event(session, i, t))
            .collect(),
        None,
        None,
    )
    .unwrap()
}
fn config(mode: MatchMode) -> FeedbackConfig {
    FeedbackConfig {
        matching: mode,
        dimensions: vec![],
        ..FeedbackConfig::default()
    }
}
fn tool_task(session: &str, args: Value, result: &str) -> Trajectory {
    let mut call = event(session, 0, "shell");
    call.kind = "tool_call".into();
    call.tool_call_id = Some(format!("{session}-call"));
    call.payload_json = json!({"name":"shell","arguments":args}).to_string();
    let mut output = event(session, 1, result);
    output.kind = "tool_result".into();
    output.tool_call_id = call.tool_call_id.clone();
    feedback::trajectory(vec![call, output, event(session, 2, "done")], None, None).unwrap()
}
#[tokio::test]
async fn matching_policies_handle_order_duplicates_arguments_and_required_insertions() {
    let o = task("o", &["inspect", "test", "inspect"]);
    let c = task("c", &["test", "inspect", "inspect", "inspect"]);
    for (mode, expected) in [
        (MatchMode::Strict, vec![None, None, Some(2), None]),
        (MatchMode::Unordered, vec![Some(1), Some(0), Some(2), None]),
    ] {
        let p = feedback::plan(o.clone(), c.clone(), config(mode))
            .await
            .unwrap();
        assert_eq!(
            p.alignment
                .iter()
                .map(|a| a.oracle_step)
                .collect::<Vec<_>>(),
            expected
        );
        assert_eq!(p.diagnostics.repeated_candidate_steps, vec![3, 4]);
    }
    let mut cfg = config(MatchMode::Required);
    cfg.criteria.required_steps = vec![1, 3];
    let p = feedback::plan(
        o.clone(),
        task("c", &["extra", "inspect", "other", "inspect"]),
        cfg.clone(),
    )
    .await
    .unwrap();
    assert!(
        regression::assess("v", &p, None, &Gates::default())
            .unwrap()
            .passed
    );
    let p = feedback::plan(o, task("c", &["inspect"]), cfg)
        .await
        .unwrap();
    assert_eq!(p.diagnostics.required_steps_missing, vec![3]);
    assert!(
        !regression::assess("v", &p, None, &Gates::default())
            .unwrap()
            .passed
    );
    let a = tool_task(
        "a",
        json!("{\"cmd\":\"cargo test\",\"cwd\":\"/tmp\"}"),
        "ok",
    );
    let b = tool_task("b", json!({"cwd":"/tmp","cmd":"cargo test"}), "ok");
    assert_eq!(
        assessment::action_key(&a.steps[0]),
        assessment::action_key(&b.steps[0])
    );
    let p = feedback::plan(a.clone(), b, config(MatchMode::Strict))
        .await
        .unwrap();
    assert!(regression::differences(&p).is_empty());
    for changed in [
        tool_task("c", json!({"cmd":"cargo build","cwd":"/tmp"}), "ok"),
        tool_task("c", json!({"cmd":"cargo test","cwd":"/tmp"}), "failed"),
    ] {
        let p = feedback::plan(a.clone(), changed, config(MatchMode::Strict))
            .await
            .unwrap();
        assert_eq!(regression::differences(&p)[0].kind, "changed");
    }
}
#[tokio::test]
async fn coverage_and_outcome_never_come_from_lexical_similarity() {
    let mut cfg = config(MatchMode::Bm25);
    cfg.criteria.required_steps = vec![1];
    let p = feedback::plan(
        task("o", &["run cargo tests successfully"]),
        task("c", &["run cargo tests later"]),
        cfg,
    )
    .await
    .unwrap();
    assert!(p.alignment[0].oracle_step.is_some());
    assert_eq!(p.diagnostics.required_steps_missing, vec![1]);
    assert_eq!(p.diagnostics.outcome_passed, None);
    assert!(
        !regression::assess(
            "v",
            &p,
            None,
            &Gates {
                require_outcome: true,
                ..Gates::default()
            }
        )
        .unwrap()
        .passed
    );
    let tmp = tempfile::tempdir().unwrap();
    let artifact = tmp.path().join("answer");
    std::fs::write(&artifact, "42").unwrap();
    let digest = archive::file_digest(&artifact).unwrap().1;
    let mut cfg = config(MatchMode::Strict);
    cfg.criteria.outcome_checks = vec![
        OutcomeCheck::ArtifactSha256 {
            id: "artifact".into(),
            path: artifact.clone(),
            sha256: digest,
        },
        OutcomeCheck::FinalContains {
            id: "answer".into(),
            text: "42".into(),
        },
    ];
    let a = task("o", &["42"]);
    let b = task("c", &["42"]);
    let before = feedback::plan(a.clone(), b.clone(), cfg.clone())
        .await
        .unwrap();
    assert_eq!(before.diagnostics.outcome_passed, Some(true));
    std::fs::write(&artifact, "wrong").unwrap();
    let after = feedback::plan(a, b, cfg).await.unwrap();
    assert_ne!(before.id, after.id);
    assert_eq!(after.diagnostics.outcome_passed, Some(false));
    assert!(
        !regression::assess("v", &after, None, &Gates::default())
            .unwrap()
            .passed
    );
    assert!(
        Criteria {
            required_steps: vec![0],
            ..Criteria::default()
        }
        .validate(1)
        .is_err()
    );
}
#[tokio::test]
async fn baselines_accept_whole_variants_and_reject_regressions_and_tampering() {
    let tmp = tempfile::tempdir().unwrap();
    let base = tmp.path().join("baseline");
    let a = task("a", &["read", "test", "done"]);
    let b = task("b", &["inspect", "verify", "done"]);
    regression::snapshot(&base, a.clone(), config(MatchMode::Strict), false).unwrap();
    assert!(regression::snapshot(&base, a.clone(), config(MatchMode::Strict), false).is_err());
    assert_eq!(
        regression::snapshot(&base, a, config(MatchMode::Strict), true)
            .unwrap()
            .variants
            .len(),
        1
    );
    let baseline = regression::snapshot(&base, b, config(MatchMode::Strict), true).unwrap();
    let passed = regression::check(
        &baseline,
        task("c", &["inspect", "verify", "done"]),
        Gates::default(),
        &tmp.path().join("pass"),
        None,
    )
    .await
    .unwrap();
    assert!(passed.passed);
    assert_eq!(passed.selected_variant, baseline.variants[1].id);
    let failed = regression::check(
        &baseline,
        task("c", &["read", "verify", "done"]),
        Gates::default(),
        &tmp.path().join("fail"),
        None,
    )
    .await
    .unwrap();
    assert!(!failed.passed); // Mixing good steps from different variants cannot pass.
    assert!(
        regression::check(
            &baseline,
            task("c", &["done"]),
            Gates {
                min_quality: Some(0.5),
                ..Gates::default()
            },
            &tmp.path().join("needs-judge"),
            None
        )
        .await
        .is_err()
    );
    assert!(
        regression::check(
            &baseline,
            task("c", &["different"]),
            Gates::default(),
            &tmp.path().join("pass"),
            None
        )
        .await
        .is_err()
    );
    let mut bad = serde_json::to_value(&baseline).unwrap();
    bad["variants"][0]["id"] = json!("tampered");
    std::fs::write(
        base.join("baseline.json"),
        serde_json::to_vec(&bad).unwrap(),
    )
    .unwrap();
    assert!(regression::load(&base).is_err());
}
struct Mock {
    replies: Mutex<VecDeque<Option<&'static str>>>,
    inputs: Mutex<Vec<Value>>,
}
impl Mock {
    fn new(replies: Vec<Option<&'static str>>) -> Self {
        Self {
            replies: Mutex::new(replies.into()),
            inputs: Mutex::new(vec![]),
        }
    }
}
#[async_trait::async_trait]
impl LanguageModel for Mock {
    async fn complete(&self, _: &str, input: &Value) -> Result<String> {
        self.inputs.lock().unwrap().push(input.clone());
        match self.replies.lock().unwrap().pop_front().flatten() {
            Some(s) => Ok(s.into()),
            None => bail!("mock outage"),
        }
    }
}
const YES: &str = r#"{"reward":1,"rationale":"evidence c-0"}"#;
const NO: &str = r#"{"reward":0,"rationale":"missing observed evidence"}"#;
#[tokio::test]
async fn dimensions_milestones_resume_independently_and_calibrate_human_labels() {
    let tmp = tempfile::tempdir().unwrap();
    let out = tmp.path().join("eval");
    let mut cfg = FeedbackConfig {
        matching: MatchMode::Strict,
        ..FeedbackConfig::default()
    };
    cfg.criteria.milestones = vec![Milestone {
        id: "tests".into(),
        description: "Tests actually passed".into(),
        oracle_steps: vec![1],
    }];
    let p = feedback::plan(
        tool_task("o", json!({"cmd":"test"}), "failed"),
        tool_task("c", json!({"cmd":"test"}), "failed"),
        cfg,
    )
    .await
    .unwrap();
    let unknown = regression::assess("v", &p, None, &Gates::default()).unwrap();
    assert!(!unknown.passed);
    assert!(unknown.failures.iter().any(|s| s.contains("unjudged")));
    let reporter = Mock::new(vec![Some("# Report\nQuality 1, milestone failed.")]);
    let first = Mock::new(vec![Some(YES), Some(YES), Some(NO), None]);
    assert!(
        feedback::evaluate(&p, &out, &first, &reporter)
            .await
            .is_err()
    );
    assert!(reporter.inputs.lock().unwrap().is_empty());
    let second = Mock::new(vec![Some(YES), Some(NO), Some(NO)]);
    let result = feedback::evaluate(&p, &out, &second, &reporter)
        .await
        .unwrap();
    assert_eq!(second.inputs.lock().unwrap().len(), 3);
    assert_eq!(result.mean_reward, Some(1.0));
    assert_eq!(result.dimension_means["tool_correctness"], 0.0);
    assert_eq!(result.dimension_means["progress"], 0.5);
    assert_eq!(result.required_work_coverage, Some(0.0));
    assert!(
        !regression::assess("v", &p, Some(&result), &Gates::default())
            .unwrap()
            .passed
    );
    let no_calls = Mock::new(vec![]);
    feedback::evaluate(&p, &out, &no_calls, &no_calls)
        .await
        .unwrap();
    assert!(no_calls.inputs.lock().unwrap().is_empty());
    let rows = archive::query(&[out.join("archive")], &Query::default())
        .await
        .unwrap();
    assert_eq!(
        rows.iter()
            .filter(|e| e.kind == "feedback_dimension")
            .count(),
        3
    );
    assert!(
        rows.iter()
            .filter(|e| e.kind == "feedback_dimension")
            .all(|e| e.model.as_deref() == Some("jev-1.13.0"))
    );
    assert_eq!(
        rows.iter()
            .find(|e| e.kind == "feedback_diagnostics")
            .unwrap()
            .model,
        None
    );
    let labels = vec![
        HumanLabel {
            evaluation: out.clone(),
            candidate_step: 1,
            human_reward: 1,
        },
        HumanLabel {
            evaluation: out.clone(),
            candidate_step: 2,
            human_reward: 0,
        },
    ];
    let cal = regression::calibrate(&labels).unwrap();
    assert_eq!(cal["overall"]["agreement"], 0.5);
    assert_eq!(cal["overall"]["false_positive"], 1);
    assert!(regression::calibrate(&[labels[0].clone(), labels[0].clone()]).is_err());
}
#[tokio::test]
async fn terminal_browser_and_mcp_read_only_recall_share_lance_evidence() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("archive");
    let events = vec![
        event("a", 0, "first"),
        event("a", 1, "second"),
        event("b", 0, "other\x1b[31m"),
    ];
    let records: Vec<_> = events
        .iter()
        .map(|e| Record {
            id: e.record_id.clone(),
            harness: e.harness.clone(),
            session_id: e.session_id.clone(),
            source_id: e.source_id.clone(),
            source_path: "synthetic".into(),
            position: e.position,
            captured_at: "2026-10-04T00:00:00Z".into(),
            status: "parsed".into(),
            diagnostic: None,
            adapter_version: "test".into(),
            raw: e.payload_json.as_bytes().to_vec(),
        })
        .collect();
    archive::write_archive(&path, "test", "batch", &records, &events)
        .await
        .unwrap();
    let pair = tmp.path().join("pair.json");
    let mut display = vec![];
    recall::browse(
        &events,
        Some(path.clone()),
        None,
        &pair,
        &mut Cursor::new(b"oracle 1\ncandidate 2\nsave\nshow 2\nquit\n"),
        &mut display,
    )
    .await
    .unwrap();
    assert!(!display.contains(&0x1b));
    let p: recall::Pair = serde_json::from_slice(&std::fs::read(&pair).unwrap()).unwrap();
    assert_eq!(p.oracle_session, "a");
    assert_eq!(p.candidate_session, "b");
    let data = recall::tool_call(
        std::slice::from_ref(&path),
        "read_session",
        json!({"session":"a","limit":1}),
    )
    .await
    .unwrap();
    assert_eq!(data["next_offset"], 1);
    assert_eq!(data["items"][0]["id"], "a-0");
    assert!(
        recall::tool_call(
            std::slice::from_ref(&path),
            "read_session",
            json!({"session":"a","path":"/etc/passwd"})
        )
        .await
        .is_err()
    );
    let nonexistent = tmp.path().join("unused-state");
    let mut server = mcp::Server::new(nonexistent.clone(), Some(path));
    assert_eq!(
        server
            .handle(json!({"jsonrpc":"2.0","id":1,"method":"tools/list"}))
            .await
            .unwrap()["error"]["code"],
        -32002
    );
    let requests = concat!(
        "{bad}\n",
        "{\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"initialize\",\"params\":{\"protocolVersion\":\"2025-11-25\",\"capabilities\":{},\"clientInfo\":{\"name\":\"test\",\"version\":\"1\"}}}\n",
        "{\"jsonrpc\":\"2.0\",\"method\":\"notifications/initialized\"}\n",
        "{\"jsonrpc\":\"2.0\",\"id\":3,\"method\":\"tools/list\"}\n",
        "{\"jsonrpc\":\"2.0\",\"id\":4,\"method\":\"tools/call\",\"params\":{\"name\":\"get_event\",\"arguments\":{\"event_id\":\"a-1\"}}}\n"
    );
    let mut out = vec![];
    mcp::serve(&mut server, &mut Cursor::new(requests), &mut out)
        .await
        .unwrap();
    let replies: Vec<Value> = String::from_utf8(out)
        .unwrap()
        .lines()
        .map(|s| serde_json::from_str(s).unwrap())
        .collect();
    assert_eq!(replies.len(), 4);
    assert_eq!(replies[0]["error"]["code"], -32700);
    assert_eq!(replies[2]["result"]["tools"].as_array().unwrap().len(), 3);
    assert_eq!(
        replies[3]["result"]["structuredContent"]["items"][0]["text"],
        "second"
    );
    assert!(!nonexistent.exists());
}

#[tokio::test]
async fn cursor_stream_and_hook_parameters_and_results_are_not_lost() {
    fn cursor_task(path: &str, result: &str) -> Trajectory {
        let mut call = event("cursor", 0, "");
        call.text = None;
        call.kind = "tool_call".into();
        call.tool_call_id = Some("call".into());
        call.payload_json =
            json!({"tool_call":{"readToolCall":{"args":{"path":path}}},"call_id":"call"})
                .to_string();
        let mut output = call.clone();
        output.id = "cursor-1".into();
        output.kind = "tool_result".into();
        output.position = 1;
        output.payload_json=json!({"tool_call":{"readToolCall":{"result":{"success":{"content":result}}}},"call_id":"call"}).to_string();
        feedback::trajectory(vec![call, output], None, None).unwrap()
    }
    let reference = cursor_task("hello.txt", "hello");
    for candidate in [
        cursor_task("other.txt", "hello"),
        cursor_task("hello.txt", "wrong"),
    ] {
        let p = feedback::plan(reference.clone(), candidate, config(MatchMode::Strict))
            .await
            .unwrap();
        assert_eq!(regression::differences(&p).len(), 1);
    }
    let check = OutcomeCheck::ToolResultContains {
        id: "read".into(),
        tool: "readToolCall".into(),
        text: "hello".into(),
    };
    assert!(check.run(&reference).unwrap().passed);
    assert!(
        !check
            .run(&cursor_task("hello.txt", "wrong"))
            .unwrap()
            .passed
    );
    let mut hook = reference.steps[0].clone();
    hook.events[0].payload_json =
        json!({"tool_name":"Read","tool_input":{"path":"hello.txt"}}).to_string();
    let mut desktop = hook.clone();
    desktop.events[0].payload_json =
        json!({"name":"Read","input":{"path":"hello.txt"}}).to_string();
    assert_eq!(
        assessment::action_key(&hook),
        assessment::action_key(&desktop)
    );
    desktop.events[0].payload_json =
        json!({"name":"Read","input":{"path":"different.txt"}}).to_string();
    assert_ne!(
        assessment::action_key(&hook),
        assessment::action_key(&desktop)
    );
}

#[test]
fn cli_snapshot_check_pair_and_mcp_work_without_provider_credentials() {
    use std::{
        io::Write,
        process::{Command, Stdio},
    };
    let tmp = tempfile::tempdir().unwrap();
    let state = tmp.path().join("state");
    let bin = env!("CARGO_BIN_EXE_harness-durable");
    let run = |args: &[&str]| {
        Command::new(bin)
            .arg("--state-dir")
            .arg(&state)
            .args(args)
            .output()
            .unwrap()
    };
    for (id, texts) in [
        ("oracle", vec!["inspect", "run tests", "done"]),
        ("candidate", vec!["inspect", "done"]),
    ] {
        let source = tmp.path().join(format!("{id}.jsonl"));
        let mut raw = format!("{}\n", json!({"type":"session_meta","payload":{"id":id}}));
        for text in texts {
            raw.push_str(&format!("{}\n",json!({"type":"response_item","payload":{"type":"message","role":"assistant","content":[{"type":"output_text","text":text}]}})));
        }
        std::fs::write(&source, raw).unwrap();
        let out = run(&[
            "import",
            "--harness",
            "codex",
            "--path",
            source.to_str().unwrap(),
        ]);
        assert!(
            out.status.success(),
            "{}",
            String::from_utf8_lossy(&out.stderr)
        );
    }
    let baseline = tmp.path().join("baseline");
    let report = tmp.path().join("check");
    let out = run(&[
        "snapshot",
        "--session",
        "oracle",
        "--output",
        baseline.to_str().unwrap(),
    ]);
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let out = run(&[
        "check",
        "--session",
        "candidate",
        "--baseline",
        baseline.to_str().unwrap(),
        "--output",
        report.to_str().unwrap(),
    ]);
    assert_eq!(
        out.status.code(),
        Some(2),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let report: Value =
        serde_json::from_slice(&std::fs::read(report.join("check.json")).unwrap()).unwrap();
    assert_eq!(report["passed"], false);
    let pair = tmp.path().join("pair.json");
    std::fs::write(
        &pair,
        serde_json::to_vec(&recall::Pair {
            oracle_session: "oracle".into(),
            candidate_session: "candidate".into(),
            oracle_harness: "codex".into(),
            candidate_harness: "codex".into(),
            archive: None,
            remote: None,
        })
        .unwrap(),
    )
    .unwrap();
    let preview = tmp.path().join("preview");
    let out = run(&[
        "compare",
        "--pair",
        pair.to_str().unwrap(),
        "--matching",
        "strict",
        "--output",
        preview.to_str().unwrap(),
        "--dry-run",
    ]);
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let p: Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(p["config"]["matching"], "strict");
    assert!(!preview.join("evaluation.json").exists());
    let unused = tmp.path().join("unused");
    let mut child = Command::new(bin)
        .arg("--state-dir")
        .arg(&unused)
        .arg("mcp")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    child
        .stdin
        .take()
        .unwrap()
        .write_all(b"{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"ping\"}\n")
        .unwrap();
    let out = child.wait_with_output().unwrap();
    assert!(out.status.success());
    let p: Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(p["result"], json!({}));
    assert!(!unused.exists());
}
