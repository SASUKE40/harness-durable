use anyhow::Result;
use harness_durable::{
    archive,
    assessment::MatchMode,
    feedback::{self, FeedbackConfig, Trajectory},
    human::{self, EvaluationOptions, HumanGates, NewLabel, Origin, Registry},
    llm::LanguageModel,
    model::{Event, Query},
    regression::Gates,
};
use serde_json::{Value, json};
use std::{fs, sync::Mutex};

fn task(id: &str, texts: &[&str]) -> Trajectory {
    feedback::trajectory(
        texts
            .iter()
            .enumerate()
            .map(|(i, text)| Event {
                id: format!("{id}-{i}"),
                record_id: format!("r-{id}-{i}"),
                harness: "codex".into(),
                session_id: id.into(),
                source_id: id.into(),
                position: i as u64,
                kind: "message".into(),
                role: Some("assistant".into()),
                text: Some((*text).into()),
                payload_json: json!({"content":text}).to_string(),
                ..Event::default()
            })
            .collect(),
        None,
        None,
    )
    .unwrap()
}
fn label(reward: u8, oracle: bool) -> NewLabel {
    NewLabel {
        task: "parser".into(),
        step: None,
        reward,
        reviewer: "test-reviewer".into(),
        note: "Explicit fixture judgment".into(),
        origin: Origin::Human,
        oracle,
        replace: false,
    }
}
fn options() -> EvaluationOptions {
    EvaluationOptions {
        task: "parser".into(),
        oracle_label_ids: vec![],
        config: FeedbackConfig {
            matching: MatchMode::Strict,
            dimensions: vec![],
            ..FeedbackConfig::default()
        },
        gates: Gates::default(),
        human_gates: HumanGates::default(),
    }
}
#[test]
fn annotations_are_idempotent_versioned_and_bound_to_snapshots() {
    let tmp = tempfile::tempdir().unwrap();
    let t = task("oracle", &["inspect", "done"]);
    let original = human::annotate(tmp.path(), t.clone(), label(1, true)).unwrap();
    let retry = human::annotate(tmp.path(), t.clone(), label(1, true)).unwrap();
    assert_eq!(original.id, retry.id);
    assert!(human::annotate(tmp.path(), t.clone(), label(0, false)).is_err());
    let revised = human::annotate(
        tmp.path(),
        t.clone(),
        NewLabel {
            replace: true,
            ..label(0, false)
        },
    )
    .unwrap();
    assert_eq!(revised.supersedes, Some(original.id));
    let registry = Registry::load(tmp.path()).unwrap();
    assert_eq!(registry.labels.len(), 2);
    assert_eq!(registry.current().len(), 1);
    assert!(!registry.current()[0].oracle);
    let different = task("oracle", &["inspect", "changed final output"]);
    let new = human::annotate(tmp.path(), different, label(1, false)).unwrap();
    assert_ne!(new.trajectory_id, revised.trajectory_id);
    assert!(new.supersedes.is_none());
    assert!(human::annotate(tmp.path(), t.clone(), label(0, true)).is_err());
    assert!(
        human::annotate(
            tmp.path(),
            t.clone(),
            NewLabel {
                step: Some(1),
                ..label(1, true)
            }
        )
        .is_err()
    );
    assert!(
        human::annotate(
            tmp.path(),
            t,
            NewLabel {
                step: Some(3),
                ..label(1, false)
            }
        )
        .is_err()
    );
    let mut data: Value =
        serde_json::from_slice(&fs::read(tmp.path().join("labels.json")).unwrap()).unwrap();
    data["labels"][0]["reward"] = json!(0);
    fs::write(
        tmp.path().join("labels.json"),
        serde_json::to_vec(&data).unwrap(),
    )
    .unwrap();
    assert!(Registry::load(tmp.path()).is_err());
}
#[tokio::test]
async fn human_failure_cannot_be_hidden_by_passing_regression_and_lance_roundtrips() {
    let tmp = tempfile::tempdir().unwrap();
    let store = tmp.path().join("labels");
    human::annotate(&store, task("o", &["done"]), label(1, true)).unwrap();
    let c = task("c", &["done"]);
    human::annotate(&store, c.clone(), label(0, false)).unwrap();
    let registry = Registry::load(&store).unwrap();
    let out = tmp.path().join("eval");
    let result = human::evaluate(&registry, c.clone(), options(), &out, None)
        .await
        .unwrap();
    assert!(!result.passed);
    assert!(result.regression.passed);
    assert_eq!(result.human.task_reward, Some(0));
    assert_eq!(result.human.mean_step_reward, None);
    let events = archive::query(&[out.join("archive")], &Query::default())
        .await
        .unwrap();
    assert_eq!(events.iter().filter(|e| e.kind == "human_label").count(), 2);
    assert_eq!(
        events
            .iter()
            .filter(|e| e.kind == "human_evaluation")
            .count(),
        1
    );
    human::evaluate(&registry, c.clone(), options(), &out, None)
        .await
        .unwrap();
    human::annotate(
        &store,
        c.clone(),
        NewLabel {
            replace: true,
            ..label(1, false)
        },
    )
    .unwrap();
    let new = Registry::load(&store).unwrap();
    assert!(
        human::evaluate(&new, c.clone(), options(), &out, None)
            .await
            .is_err()
    );
    let result = human::evaluate(&new, c, options(), &tmp.path().join("revised"), None)
        .await
        .unwrap();
    assert!(result.passed);
}
#[tokio::test]
async fn step_means_exclude_unlabeled_steps_and_task_reward_does_not_fill_them() {
    let tmp = tempfile::tempdir().unwrap();
    let store = tmp.path().join("labels");
    human::annotate(&store, task("o", &["inspect", "done"]), label(1, true)).unwrap();
    let c = task("c", &["inspect", "done"]);
    human::annotate(&store, c.clone(), label(1, false)).unwrap();
    human::annotate(
        &store,
        c.clone(),
        NewLabel {
            step: Some(1),
            ..label(1, false)
        },
    )
    .unwrap();
    let registry = Registry::load(&store).unwrap();
    let opts = EvaluationOptions {
        human_gates: HumanGates {
            require_human_label: true,
            min_human_quality: Some(1.0),
            min_human_coverage: Some(1.0),
            ..HumanGates::default()
        },
        ..options()
    };
    let result = human::evaluate(&registry, c, opts, &tmp.path().join("eval"), None)
        .await
        .unwrap();
    assert!(!result.passed);
    assert_eq!(result.human.mean_step_reward, Some(1.0));
    assert_eq!(result.human.step_coverage, 0.5);
    assert_eq!(result.human.labeled_steps, 1);
    let changed = task("c", &["inspect", "done", "later event"]);
    let result = human::evaluate(
        &registry,
        changed,
        EvaluationOptions {
            human_gates: HumanGates {
                require_human_label: true,
                ..HumanGates::default()
            },
            ..options()
        },
        &tmp.path().join("changed"),
        None,
    )
    .await
    .unwrap();
    assert_eq!(result.human.task_reward, None);
    assert_eq!(result.human.mean_step_reward, None);
    assert!(!result.passed);
}
#[tokio::test]
async fn oracles_are_task_scoped_current_and_never_the_candidate_itself() {
    let tmp = tempfile::tempdir().unwrap();
    let store = tmp.path().join("labels");
    let o = task("o", &["done"]);
    let accepted = human::annotate(&store, o.clone(), label(1, true)).unwrap();
    let registry = Registry::load(&store).unwrap();
    assert!(
        human::evaluate(
            &registry,
            o.clone(),
            options(),
            &tmp.path().join("self"),
            None
        )
        .await
        .is_err()
    );
    assert!(
        human::evaluate(
            &registry,
            task("c", &["done"]),
            EvaluationOptions {
                task: "different-task".into(),
                oracle_label_ids: vec![accepted.id.clone()],
                ..options()
            },
            &tmp.path().join("wrong-task"),
            None
        )
        .await
        .is_err()
    );
    human::annotate(
        &store,
        o,
        NewLabel {
            replace: true,
            ..label(1, false)
        },
    )
    .unwrap();
    let registry = Registry::load(&store).unwrap();
    assert!(
        human::evaluate(
            &registry,
            task("c", &["done"]),
            EvaluationOptions {
                oracle_label_ids: vec![accepted.id],
                ..options()
            },
            &tmp.path().join("revoked"),
            None
        )
        .await
        .is_err()
    );
}
#[tokio::test]
async fn synthetic_labels_require_explicit_evaluation_opt_in() {
    let tmp = tempfile::tempdir().unwrap();
    let store = tmp.path().join("labels");
    human::annotate(
        &store,
        task("o", &["done"]),
        NewLabel {
            origin: Origin::Synthetic,
            ..label(1, true)
        },
    )
    .unwrap();
    let registry = Registry::load(&store).unwrap();
    let c = task("c", &["done"]);
    assert!(
        human::evaluate(
            &registry,
            c.clone(),
            options(),
            &tmp.path().join("forbidden"),
            None
        )
        .await
        .is_err()
    );
    let result = human::evaluate(
        &registry,
        c,
        EvaluationOptions {
            human_gates: HumanGates {
                allow_synthetic: true,
                ..HumanGates::default()
            },
            ..options()
        },
        &tmp.path().join("demo"),
        None,
    )
    .await
    .unwrap();
    assert!(result.passed);
    assert!(result.synthetic);
    assert!(
        fs::read_to_string(tmp.path().join("demo/report.md"))
            .unwrap()
            .contains("not human-reviewed")
    );
}
struct Mock {
    inputs: Mutex<Vec<Value>>,
    response: &'static str,
}
#[async_trait::async_trait]
impl LanguageModel for Mock {
    async fn complete(&self, _: &str, input: &Value) -> Result<String> {
        self.inputs.lock().unwrap().push(input.clone());
        Ok(self.response.into())
    }
}
#[tokio::test]
async fn model_scores_stay_independent_and_report_receives_human_disagreement() {
    let tmp = tempfile::tempdir().unwrap();
    let store = tmp.path().join("labels");
    human::annotate(&store, task("o", &["done"]), label(1, true)).unwrap();
    let c = task("c", &["done"]);
    human::annotate(&store, c.clone(), label(0, false)).unwrap();
    let registry = Registry::load(&store).unwrap();
    let out = tmp.path().join("eval");
    let judge = Mock {
        inputs: Mutex::new(vec![]),
        response: r#"{"reward":1,"rationale":"correct observed action"}"#,
    };
    let reporter = Mock {
        inputs: Mutex::new(vec![]),
        response: "Human reviewer disagrees with the model's passing score.",
    };
    let result = human::evaluate(
        &registry,
        c.clone(),
        options(),
        &out,
        Some((&judge, &reporter)),
    )
    .await
    .unwrap();
    assert!(!result.passed);
    assert_eq!(result.regression.variants[0].mean_quality, Some(1.0));
    assert!(
        judge.inputs.lock().unwrap()[0]
            .get("human_review")
            .is_none()
    );
    assert_eq!(
        reporter.inputs.lock().unwrap()[0]["human_review"]["assessment"]["task_reward"],
        0
    );
    human::evaluate(&registry, c, options(), &out, Some((&judge, &reporter)))
        .await
        .unwrap();
    assert_eq!(judge.inputs.lock().unwrap().len(), 1);
    assert_eq!(reporter.inputs.lock().unwrap().len(), 1);
}
#[test]
fn cli_labels_oracle_and_evaluation_apply_supplied_reviews() {
    let tmp = tempfile::tempdir().unwrap();
    let config = tmp.path().join("config.toml");
    fs::write(
        &config,
        format!(
            "state_dir = {}\nremotes = []\n",
            json!(tmp.path().join("state"))
        ),
    )
    .unwrap();
    let run = |args: &[&str]| {
        std::process::Command::new(env!("CARGO_BIN_EXE_harness-durable"))
            .arg("--config")
            .arg(&config)
            .args(args)
            .output()
            .unwrap()
    };
    for id in ["oracle", "candidate"] {
        let path = tmp.path().join(format!("{id}.jsonl"));
        fs::write(&path,format!("{}\n{}\n",json!({"type":"session_meta","payload":{"id":id}}),json!({"type":"response_item","payload":{"type":"message","role":"assistant","content":[{"type":"output_text","text":"done"}]}}))).unwrap();
        let p = run(&[
            "import",
            "--harness",
            "codex",
            "--path",
            path.to_str().unwrap(),
        ]);
        assert!(p.status.success(), "{}", String::from_utf8_lossy(&p.stderr));
    }
    let p = run(&[
        "label",
        "--task",
        "parser",
        "--session",
        "oracle",
        "--reward",
        "1",
        "--reviewer",
        "demo",
        "--note",
        "accepted fixture",
        "--oracle",
        "--synthetic",
    ]);
    assert!(p.status.success(), "{}", String::from_utf8_lossy(&p.stderr));
    let p = run(&[
        "label",
        "--task",
        "parser",
        "--session",
        "candidate",
        "--reward",
        "0",
        "--reviewer",
        "demo",
        "--note",
        "rejected fixture",
        "--synthetic",
    ]);
    assert!(p.status.success(), "{}", String::from_utf8_lossy(&p.stderr));
    let out = tmp.path().join("evaluation");
    let p = run(&[
        "evaluate",
        "--task",
        "parser",
        "--session",
        "candidate",
        "--output",
        out.to_str().unwrap(),
        "--matching",
        "strict",
        "--allow-synthetic",
        "--require-human-label",
    ]);
    assert_eq!(
        p.status.code(),
        Some(2),
        "{}",
        String::from_utf8_lossy(&p.stderr)
    );
    let result: Value =
        serde_json::from_slice(&fs::read(out.join("result.json")).unwrap()).unwrap();
    assert_eq!(result["passed"], false);
    assert_eq!(result["human"]["task_reward"], 0);
    let p = run(&["labels", "--task", "parser"]);
    assert!(p.status.success());
    let rows: Vec<Value> = String::from_utf8(p.stdout)
        .unwrap()
        .lines()
        .map(|s| serde_json::from_str(s).unwrap())
        .collect();
    assert_eq!(rows.len(), 2);
    assert!(rows.iter().any(|l| l["oracle"] == true));
    let p = run(&[
        "query",
        "--archive",
        out.join("archive").to_str().unwrap(),
        "--kind",
        "human_evaluation",
        "--format",
        "jsonl",
    ]);
    assert!(p.status.success());
    let e: Value = serde_json::from_slice(&p.stdout).unwrap();
    let payload: Value = serde_json::from_str(e["payload_json"].as_str().unwrap()).unwrap();
    assert_eq!(payload["human"]["task_reward"], 0);
}
