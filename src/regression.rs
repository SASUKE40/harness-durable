//! Accepted reference snapshots and offline-first regression gates.
use crate::{
    assessment::{self, MatchMode, evidence_key},
    feedback::{self, Evaluation, FeedbackConfig, Plan, Trajectory},
    llm::LanguageModel,
};
use anyhow::{Context, Result, ensure};
use fs2::FileExt;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::{
    fs::{self, OpenOptions},
    path::Path,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Baseline {
    pub version: u32,
    pub config: FeedbackConfig,
    pub variants: Vec<Variant>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Variant {
    pub id: String,
    pub accepted_at: String,
    pub trajectory: Trajectory,
}

pub fn snapshot(
    directory: &Path,
    trajectory: Trajectory,
    config: FeedbackConfig,
    append: bool,
) -> Result<Baseline> {
    config.validate()?;
    config.criteria.validate(trajectory.steps.len())?;
    ensure!(
        config.matching != MatchMode::Required || !config.criteria.required_steps.is_empty(),
        "required matching needs required_steps"
    );
    fs::create_dir_all(directory)?;
    let lock = OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(directory.join(".lock"))?;
    lock.try_lock_exclusive()
        .context("baseline is being updated")?;
    let path = directory.join("baseline.json");
    let mut baseline = if path.exists() {
        ensure!(
            append,
            "baseline already exists; --append adds an accepted variant without replacing prior references"
        );
        let b = load(directory)?;
        ensure!(
            serde_json::to_value(&b.config)? == serde_json::to_value(&config)?,
            "baseline policy changed; use a new baseline directory"
        );
        b
    } else {
        Baseline {
            version: 1,
            config,
            variants: Vec::new(),
        }
    };
    let id = assessment::trajectory_id(&trajectory)?;
    if !baseline.variants.iter().any(|v| v.id == id) {
        ensure!(
            baseline.variants.len() < 5,
            "a baseline supports at most five accepted variants"
        );
        baseline.variants.push(Variant {
            id,
            accepted_at: chrono::Utc::now().to_rfc3339(),
            trajectory,
        });
        feedback::save(&path, &serde_json::to_vec_pretty(&baseline)?)?;
    }
    Ok(baseline)
}
pub fn load(directory: &Path) -> Result<Baseline> {
    let b: Baseline = serde_json::from_slice(&fs::read(directory.join("baseline.json"))?)?;
    ensure!(
        b.version == 1 && !b.variants.is_empty() && b.variants.len() <= 5,
        "invalid baseline version or variant count"
    );
    b.config.validate()?;
    for v in &b.variants {
        ensure!(
            assessment::trajectory_id(&v.trajectory)? == v.id,
            "baseline snapshot checksum mismatch"
        );
        ensure!(!v.trajectory.steps.is_empty(), "empty baseline trajectory");
        b.config.criteria.validate(v.trajectory.steps.len())?;
    }
    Ok(b)
}
#[derive(Debug, Clone, Serialize, Deserialize, clap::Args, Default)]
#[serde(default, deny_unknown_fields)]
pub struct Gates {
    #[arg(long, default_value_t = 0)]
    pub max_missing_steps: usize,
    #[arg(long, default_value_t = 0)]
    pub max_extra_steps: usize,
    #[arg(long, default_value_t = 0)]
    pub max_changed_steps: usize,
    #[arg(long)]
    pub min_quality: Option<f64>,
    #[arg(long)]
    pub min_required_coverage: Option<f64>,
    #[arg(long)]
    pub min_tool_correctness: Option<f64>,
    #[arg(long)]
    pub min_progress: Option<f64>,
    #[arg(long)]
    pub max_repetition_ratio: Option<f64>,
    #[arg(long)]
    pub require_outcome: bool,
}
impl Gates {
    pub fn validate(&self) -> Result<()> {
        for v in [
            self.min_quality,
            self.min_required_coverage,
            self.min_tool_correctness,
            self.min_progress,
            self.max_repetition_ratio,
        ]
        .into_iter()
        .flatten()
        {
            ensure!(
                v.is_finite() && (0.0..=1.0).contains(&v),
                "gate thresholds must be between 0 and 1"
            );
        }
        Ok(())
    }
    pub fn needs_judge(&self) -> bool {
        self.min_quality.is_some()
            || self.min_tool_correctness.is_some()
            || self.min_progress.is_some()
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Change {
    pub kind: String,
    pub oracle_step: Option<usize>,
    pub candidate_step: Option<usize>,
    pub before: Option<String>,
    pub after: Option<String>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VariantCheck {
    pub variant_id: String,
    pub plan_id: String,
    pub passed: bool,
    pub failures: Vec<String>,
    pub changes: Vec<Change>,
    pub output_changed: bool,
    pub diagnostics: assessment::Diagnostics,
    pub mean_quality: Option<f64>,
    pub required_work_coverage: Option<f64>,
    pub dimension_means: std::collections::BTreeMap<String, f64>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckReport {
    pub version: u32,
    pub candidate_session: String,
    pub selected_variant: String,
    pub passed: bool,
    pub gates: Gates,
    pub variants: Vec<VariantCheck>,
}

pub fn differences(p: &Plan) -> Vec<Change> {
    let mut changes = Vec::new();
    if p.config.matching == MatchMode::Strict {
        for i in 0..p.oracle.steps.len().max(p.candidate.steps.len()) {
            let before = p.oracle.steps.get(i).map(evidence_key);
            let after = p.candidate.steps.get(i).map(evidence_key);
            if before != after {
                changes.push(Change {
                    kind: if before.is_none() {
                        "extra"
                    } else if after.is_none() {
                        "missing"
                    } else {
                        "changed"
                    }
                    .into(),
                    oracle_step: before.as_ref().map(|_| i + 1),
                    candidate_step: after.as_ref().map(|_| i + 1),
                    before,
                    after,
                });
            }
        }
    } else {
        for a in &p.alignment {
            let after = evidence_key(&p.candidate.steps[a.candidate_step]);
            if let Some(j) = a.oracle_step {
                let before = evidence_key(&p.oracle.steps[j]);
                if before != after {
                    changes.push(Change {
                        kind: "changed".into(),
                        oracle_step: Some(j + 1),
                        candidate_step: Some(a.candidate_step + 1),
                        before: Some(before),
                        after: Some(after),
                    });
                }
            } else if p.config.matching != MatchMode::Required {
                changes.push(Change {
                    kind: "extra".into(),
                    oracle_step: None,
                    candidate_step: Some(a.candidate_step + 1),
                    before: None,
                    after: Some(after),
                });
            }
        }
        for j in &p.unmatched_oracle_steps {
            if p.config.matching == MatchMode::Required
                && !p.config.criteria.required_steps.contains(&(j + 1))
            {
                continue;
            }
            changes.push(Change {
                kind: "missing".into(),
                oracle_step: Some(j + 1),
                candidate_step: None,
                before: Some(evidence_key(&p.oracle.steps[*j])),
                after: None,
            });
        }
    }
    changes
}
pub fn assess(
    variant_id: &str,
    p: &Plan,
    evaluation: Option<&Evaluation>,
    g: &Gates,
) -> Result<VariantCheck> {
    g.validate()?;
    if let Some(e) = evaluation {
        ensure!(
            e.plan_id == p.id,
            "evaluation does not belong to comparison"
        );
    }
    let changes = differences(p);
    let mut failures = Vec::new();
    for (kind, max) in [
        ("missing", g.max_missing_steps),
        ("extra", g.max_extra_steps),
        ("changed", g.max_changed_steps),
    ] {
        let n = changes.iter().filter(|c| c.kind == kind).count();
        if n > max {
            failures.push(format!("{kind} steps: {n} exceeds {max}"));
        }
    }
    let output_changed = p.oracle.final_output != p.candidate.final_output;
    if output_changed && g.max_changed_steps == 0 && p.config.matching != MatchMode::Required {
        failures.push("final output changed".into());
    }
    let coverage = if let Some(e) = evaluation {
        e.required_work_coverage
    } else if p.config.criteria.milestones.is_empty()
        && !p.config.criteria.required_steps.is_empty()
    {
        Some(
            p.diagnostics.required_steps_passed.len() as f64
                / p.config.criteria.required_steps.len() as f64,
        )
    } else {
        None
    };
    let dimensions = evaluation
        .map(|e| e.dimension_means.clone())
        .unwrap_or_default();
    let mean_quality = evaluation.and_then(|e| e.mean_reward);
    for (name, min, value) in [
        ("step quality", g.min_quality, mean_quality),
        ("required-work coverage", g.min_required_coverage, coverage),
        (
            "tool correctness",
            g.min_tool_correctness,
            dimensions.get("tool_correctness").copied(),
        ),
        (
            "progress",
            g.min_progress,
            dimensions.get("progress").copied(),
        ),
    ] {
        if let Some(min) = min {
            match value {
                Some(v) if v >= min => {}
                Some(v) => failures.push(format!("{name}: {v:.4} below {min:.4}")),
                None => failures.push(format!(
                    "{name}: unavailable; cannot pass requested threshold"
                )),
            }
        }
    }
    if !p.diagnostics.required_steps_missing.is_empty() {
        failures.push(format!(
            "required structural steps missing: {:?}",
            p.diagnostics.required_steps_missing
        ));
    }
    for milestone in &p.config.criteria.milestones {
        match evaluation.and_then(|e| {
            e.milestone_scores
                .iter()
                .find(|s| s.subject == milestone.id)
        }) {
            Some(score) if score.judgment.reward == 1 => {}
            Some(_) => failures.push(format!("required milestone failed: {}", milestone.id)),
            None => failures.push(format!(
                "required milestone unjudged: {}; use --judge",
                milestone.id
            )),
        }
    }
    if p.diagnostics.outcome_passed == Some(false) {
        failures.push("configured outcome checks failed".into());
    }
    if g.require_outcome && p.diagnostics.outcome_passed.is_none() {
        failures.push("outcome checks unavailable".into());
    }
    if let Some(max) = g.max_repetition_ratio
        && p.diagnostics.repetition_ratio > max
    {
        failures.push(format!(
            "repetition ratio {:.4} exceeds {max:.4}",
            p.diagnostics.repetition_ratio
        ));
    }
    Ok(VariantCheck {
        variant_id: variant_id.into(),
        plan_id: p.id.clone(),
        passed: failures.is_empty(),
        failures,
        changes,
        output_changed,
        diagnostics: p.diagnostics.clone(),
        mean_quality,
        required_work_coverage: coverage,
        dimension_means: dimensions,
    })
}
pub async fn check(
    baseline: &Baseline,
    candidate: Trajectory,
    gates: Gates,
    output: &Path,
    models: Option<(&dyn LanguageModel, &dyn LanguageModel)>,
) -> Result<CheckReport> {
    gates.validate()?;
    ensure!(
        !gates.needs_judge() || models.is_some(),
        "quality/dimension thresholds require --judge"
    );
    fs::create_dir_all(output)?;
    let lock = OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(output.join(".check.lock"))?;
    lock.try_lock_exclusive()
        .context("another check is using this output directory")?;
    let run_id = crate::model::hash(serde_json::to_vec(&(
        baseline,
        &candidate,
        &gates,
        models.is_some(),
    ))?);
    let marker = output.join("check-input.json");
    if marker.exists() {
        let v: serde_json::Value = serde_json::from_slice(&fs::read(&marker)?)?;
        ensure!(
            v["id"] == run_id,
            "output belongs to another regression check"
        );
    } else {
        feedback::save(
            &marker,
            &serde_json::to_vec_pretty(
                &json!({"id":run_id,"baseline":baseline,"candidate":candidate,"gates":gates,"judged":models.is_some()}),
            )?,
        )?;
    }
    ensure!(!baseline.variants.is_empty(), "empty baseline");
    // A failed retry must not leave a previous successful report looking current.
    for name in ["check.json", "check.md"] {
        match fs::remove_file(output.join(name)) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e.into()),
        }
    }
    let mut variants = Vec::new();
    for v in &baseline.variants {
        let p = feedback::plan(
            v.trajectory.clone(),
            candidate.clone(),
            baseline.config.clone(),
        )
        .await?;
        let result = if let Some((judge, reporter)) = models {
            Some(feedback::evaluate(&p, &output.join(&v.id), judge, reporter).await?)
        } else {
            None
        };
        variants.push(assess(&v.id, &p, result.as_ref(), &gates)?);
    }
    let selected = variants.iter().position(|v| v.passed).unwrap_or_else(|| {
        variants
            .iter()
            .enumerate()
            .min_by_key(|(_, v)| (v.failures.len(), v.changes.len()))
            .unwrap()
            .0
    });
    let result = CheckReport {
        version: 1,
        candidate_session: candidate.session_id,
        selected_variant: variants[selected].variant_id.clone(),
        passed: variants[selected].passed,
        gates,
        variants,
    };
    feedback::save(
        &output.join("check.json"),
        &serde_json::to_vec_pretty(&result)?,
    )?;
    let selected = &result.variants[selected];
    let report = format!(
        "# Regression check: {}\n\nCandidate: {}\n\nAccepted reference variant: {}\n\nFailures:\n{}\n\nChanges (1-based step numbers):\n\n```json\n{}\n```\n\nDiagnostics:\n\n```json\n{}\n```\n",
        if result.passed { "PASS" } else { "FAIL" },
        result.candidate_session,
        result.selected_variant,
        if selected.failures.is_empty() {
            "None".into()
        } else {
            selected.failures.join("\n")
        },
        serde_json::to_string_pretty(&selected.changes)?,
        serde_json::to_string_pretty(&selected.diagnostics)?
    );
    feedback::save(&output.join("check.md"), report.as_bytes())?;
    Ok(result)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HumanLabel {
    pub evaluation: std::path::PathBuf,
    pub candidate_step: usize,
    pub human_reward: u8,
}
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Calibration {
    pub samples: usize,
    pub true_positive: usize,
    pub true_negative: usize,
    pub false_positive: usize,
    pub false_negative: usize,
    pub agreement: Option<f64>,
    pub precision: Option<f64>,
    pub recall: Option<f64>,
}
impl Calibration {
    fn add(&mut self, p: u8, h: u8) {
        self.samples += 1;
        match (p, h) {
            (1, 1) => self.true_positive += 1,
            (0, 0) => self.true_negative += 1,
            (1, 0) => self.false_positive += 1,
            _ => self.false_negative += 1,
        };
    }
    fn finish(&mut self) {
        let ratio = |n: usize, d: usize| (d > 0).then(|| n as f64 / d as f64);
        self.agreement = ratio(self.true_positive + self.true_negative, self.samples);
        self.precision = ratio(self.true_positive, self.true_positive + self.false_positive);
        self.recall = ratio(self.true_positive, self.true_positive + self.false_negative);
    }
}
pub fn calibrate(labels: &[HumanLabel]) -> Result<serde_json::Value> {
    ensure!(!labels.is_empty(), "no human labels supplied");
    let mut all = Calibration::default();
    let mut groups: std::collections::BTreeMap<String, Calibration> =
        std::collections::BTreeMap::new();
    let mut seen = std::collections::HashSet::new();
    for l in labels {
        ensure!(
            l.human_reward <= 1 && l.candidate_step > 0,
            "labels require binary human_reward and a 1-based candidate_step"
        );
        let e: Evaluation =
            serde_json::from_slice(&fs::read(l.evaluation.join("evaluation.json"))?)?;
        let p: Plan = serde_json::from_slice(&fs::read(l.evaluation.join("plan.json"))?)?;
        ensure!(e.plan_id == p.id, "evaluation/plan mismatch");
        ensure!(
            seen.insert((e.plan_id.clone(), l.candidate_step)),
            "duplicate labeled step"
        );
        let score = e
            .scores
            .get(l.candidate_step - 1)
            .context("labeled step was not judged")?;
        let parsed = feedback::Judgment::parse(&score.raw_response)?;
        ensure!(
            parsed.reward == score.judgment.reward
                && score.alignment.candidate_step == l.candidate_step - 1,
            "invalid saved judgment"
        );
        let model = score
            .judgment
            .provider_response
            .as_ref()
            .and_then(|v| v["model"].as_str())
            .map(str::to_owned)
            .or_else(|| p.config.judge.as_ref().map(|m| m.model.clone()))
            .unwrap_or_else(|| "unspecified".into());
        let key = crate::model::hash(serde_json::to_vec(&(
            p.version,
            &p.config.judge,
            &p.config.rubric,
            &model,
        ))?);
        groups
            .entry(format!("{model}:{}", &key[..12]))
            .or_default()
            .add(score.judgment.reward, l.human_reward);
        all.add(score.judgment.reward, l.human_reward);
    }
    all.finish();
    for g in groups.values_mut() {
        g.finish();
    }
    Ok(
        json!({"overall":all,"by_judge_configuration":groups,"interpretation":"Agreement with supplied human labels, not proof of general accuracy; use held-out representative examples."}),
    )
}
