use anyhow::{Context, Result, ensure};
use clap::{Args, Parser, Subcommand};
use harness_durable::{
    adapters::{self, Source},
    archive,
    assessment::{Criteria, MatchMode},
    config::Config,
    feedback, hooks, human,
    llm::HttpModel,
    mcp,
    model::Query,
    recall, regression, remote,
    state::State,
};
use notify::Watcher;
use std::{
    collections::HashSet,
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

#[derive(Parser)]
#[command(
    version,
    about = "Capture coding-agent sessions into portable Lance archives"
)]
struct Cli {
    #[arg(long, global = true)]
    config: Option<PathBuf>,
    #[arg(long, global = true)]
    state_dir: Option<PathBuf>,
    #[command(subcommand)]
    command: Command,
}
#[derive(Subcommand)]
enum Command {
    Discover(Capture),
    Import(Capture),
    Watch(Capture),
    Sync {
        #[arg(long)]
        remote: Option<String>,
    },
    Status,
    Sessions(ReadOptions),
    Query(ReadOptions),
    /// Compare a candidate trajectory against an oracle using Lance BM25 and model judges.
    Compare(Box<Compare>),
    /// Save an explicitly accepted reference, or append another valid path.
    Snapshot(Box<Snapshot>),
    /// Compare with accepted snapshots. Exit 2 on regression, 1 on an error.
    Check(Box<Check>),
    /// Interactive, line-oriented terminal browser; never calls a model.
    Browse {
        #[command(flatten)]
        read: ReadOptions,
        #[arg(long, default_value = "pair.json")]
        pair_output: PathBuf,
    },
    /// Serve read-only local recall tools over MCP stdio.
    Mcp {
        #[arg(long)]
        archive: Option<PathBuf>,
    },
    /// Compare saved quality judgments against explicitly supplied human labels.
    Calibrate {
        #[arg(long)]
        labels: PathBuf,
        #[arg(long)]
        output: Option<PathBuf>,
    },
    /// Record a human task/step label; optionally accept the task as an oracle.
    Label(Box<LabelOptions>),
    /// List current labels, or include superseded revisions with --history.
    Labels {
        #[arg(long)]
        task: Option<String>,
        #[arg(long)]
        history: bool,
    },
    /// Apply accepted oracle snapshots, human labels, and optional model judgments.
    Evaluate(Box<EvaluateOptions>),
    Export {
        #[command(flatten)]
        read: ReadOptions,
        #[arg(long)]
        output: PathBuf,
    },
    Hooks {
        #[command(subcommand)]
        command: HookCommand,
    },
}
#[derive(Args)]
struct Compare {
    #[arg(long, required_unless_present = "pair")]
    oracle_session: Option<String>,
    #[arg(long, required_unless_present = "pair")]
    candidate_session: Option<String>,
    #[arg(long, conflicts_with_all=["oracle_session", "candidate_session"])]
    pair: Option<PathBuf>,
    #[arg(long, value_enum)]
    matching: Option<MatchMode>,
    /// JSON file declaring required steps, semantic milestones, and outcome checks.
    #[arg(long)]
    criteria: Option<PathBuf>,
    #[arg(long)]
    oracle_harness: Option<String>,
    #[arg(long)]
    candidate_harness: Option<String>,
    /// Read a standalone export/archive instead of the local collector.
    #[arg(long, conflicts_with = "oracle_remote")]
    oracle_archive: Option<PathBuf>,
    #[arg(long, conflicts_with = "candidate_remote")]
    candidate_archive: Option<PathBuf>,
    #[arg(long)]
    oracle_remote: Option<String>,
    #[arg(long)]
    candidate_remote: Option<String>,
    /// Native Pi entry ID identifying the chosen branch.
    #[arg(long)]
    oracle_leaf: Option<String>,
    #[arg(long)]
    candidate_leaf: Option<String>,
    /// UTF-8 file containing an explicit final output.
    #[arg(long)]
    oracle_output: Option<PathBuf>,
    #[arg(long)]
    candidate_output: Option<PathBuf>,
    #[arg(long)]
    output: PathBuf,
    /// Print the alignment as JSON without model calls or writing evaluation files.
    #[arg(long)]
    dry_run: bool,
}
#[derive(Args)]
struct SessionInput {
    #[arg(long)]
    session: String,
    #[arg(long)]
    harness: Option<String>,
    #[arg(long, conflicts_with = "remote")]
    archive: Option<PathBuf>,
    #[arg(long)]
    remote: Option<String>,
    #[arg(long)]
    leaf: Option<String>,
    #[arg(long)]
    final_output: Option<PathBuf>,
}
#[derive(Args)]
struct Snapshot {
    #[command(flatten)]
    input: SessionInput,
    #[arg(long)]
    output: PathBuf,
    #[arg(long)]
    append: bool,
    #[arg(long, value_enum, default_value = "strict")]
    matching: MatchMode,
    #[arg(long)]
    criteria: Option<PathBuf>,
}
#[derive(Args)]
struct Check {
    #[command(flatten)]
    input: SessionInput,
    #[arg(long)]
    baseline: PathBuf,
    #[arg(long)]
    output: PathBuf,
    /// Explicitly enable paid/provider inference for quality and semantic milestones.
    #[arg(long)]
    judge: bool,
    #[command(flatten)]
    gates: regression::Gates,
}
#[derive(Args)]
struct LabelOptions {
    #[command(flatten)]
    input: SessionInput,
    #[arg(long)]
    task: String,
    #[arg(long, value_parser = clap::value_parser!(u8).range(0..=1))]
    reward: u8,
    #[arg(long)]
    reviewer: String,
    #[arg(long)]
    note: String,
    /// Optional 1-based step number. Omit to label the whole task result.
    #[arg(long)]
    step: Option<usize>,
    /// Accept a passing whole-task result as an oracle for this task.
    #[arg(long, conflicts_with = "step")]
    oracle: bool,
    /// Record a revision; all previous annotations remain in the audit history.
    #[arg(long)]
    replace: bool,
    /// Label provenance for demonstrations; never presented as human review.
    #[arg(long)]
    synthetic: bool,
}
#[derive(Args)]
struct EvaluateOptions {
    #[command(flatten)]
    input: SessionInput,
    #[arg(long)]
    task: String,
    #[arg(long)]
    output: PathBuf,
    /// Choose specific current oracle label IDs; otherwise use all for this task.
    #[arg(long)]
    oracle_label: Vec<String>,
    #[arg(long, value_enum)]
    matching: Option<MatchMode>,
    #[arg(long)]
    criteria: Option<PathBuf>,
    /// Enable provider calls; without this, human and deterministic evaluation only.
    #[arg(long)]
    judge: bool,
    #[command(flatten)]
    gates: regression::Gates,
    #[command(flatten)]
    human_gates: human::HumanGates,
}
fn criteria_file(path: &Option<PathBuf>, config: &mut feedback::FeedbackConfig) -> Result<()> {
    if let Some(path) = path {
        config.criteria = serde_json::from_slice::<Criteria>(&std::fs::read(path)?)?;
    }
    Ok(())
}
async fn session_input(
    state: &State,
    c: &Config,
    input: &SessionInput,
) -> Result<feedback::Trajectory> {
    let q = Query {
        session: Some(input.session.clone()),
        harness: input.harness.clone(),
        ..Query::default()
    };
    let read = ReadOptions {
        archive: input.archive.clone(),
        remote: input.remote.clone(),
        ..ReadOptions::default()
    };
    feedback::trajectory(
        archive::query(&paths(state, c, &read, &q).await?, &q).await?,
        input.leaf.as_deref(),
        input
            .final_output
            .as_ref()
            .map(std::fs::read_to_string)
            .transpose()?,
    )
}
#[derive(Args, Default)]
struct Capture {
    #[arg(long,value_parser=["codex","pi","cursor"])]
    harness: Option<String>,
    #[arg(long)]
    path: Vec<PathBuf>,
    /// Match the recorded working directory, or transcript path, by substring.
    #[arg(long)]
    project: Option<String>,
}
#[derive(Args, Default)]
struct ReadOptions {
    /// Read a standalone archive, including a comparison's archive/ directory.
    #[arg(long, conflicts_with = "remote")]
    archive: Option<PathBuf>,
    #[arg(long)]
    remote: Option<String>,
    #[arg(long)]
    harness: Option<String>,
    #[arg(long)]
    session: Option<String>,
    #[arg(long)]
    since: Option<String>,
    #[arg(long)]
    until: Option<String>,
    #[arg(long)]
    kind: Option<String>,
    #[arg(long)]
    text: Option<String>,
    #[arg(long,default_value="text",value_parser=["text","jsonl"])]
    format: String,
}
#[derive(Subcommand)]
enum HookCommand {
    Install {
        #[arg(value_parser=["cursor"])]
        harness: String,
        #[arg(long)]
        hooks_file: Option<PathBuf>,
    },
    Uninstall {
        #[arg(value_parser=["cursor"])]
        harness: String,
        #[arg(long)]
        hooks_file: Option<PathBuf>,
    },
    #[command(hide = true)]
    Receive,
}

fn roots(config: &Config, capture: &Capture) -> Result<Vec<(String, Vec<PathBuf>)>> {
    ensure!(
        capture.path.is_empty() || capture.harness.is_some(),
        "--path requires --harness"
    );
    let home = directories::BaseDirs::new().context("home directory")?;
    let mut out = Vec::new();
    for name in ["codex", "pi", "cursor"] {
        if capture.harness.as_ref().is_some_and(|h| h != name) {
            continue;
        }
        let a = adapters::adapter(name)?;
        let mut paths = if !capture.path.is_empty() {
            capture.path.clone()
        } else {
            let configured: Vec<_> = config
                .sources
                .iter()
                .filter(|s| s.harness == name)
                .map(|s| s.path.clone())
                .collect();
            if configured.is_empty() {
                a.roots(home.home_dir())
            } else {
                configured
            }
        };
        if name == "cursor" {
            paths.push(config.state_dir.join("hooks"));
        }
        out.push((name.into(), paths));
    }
    Ok(out)
}
fn discover(config: &Config, capture: &Capture) -> Result<Vec<Source>> {
    let mut sources = Vec::new();
    for (name, paths) in roots(config, capture)? {
        sources.extend(adapters::discover(
            adapters::adapter(&name)?.as_ref(),
            &paths,
        )?);
    }
    sources.retain(|s| {
        capture.project.as_ref().is_none_or(|p| {
            s.project.as_ref().is_some_and(|v| v.contains(p))
                || s.path.to_string_lossy().contains(p)
        })
    });
    Ok(sources)
}

async fn flush_all(state: &mut State, c: &Config) -> Result<()> {
    while archive::flush(state, c.max_records, c.max_bytes)
        .await?
        .is_some()
    {}
    Ok(())
}
async fn capture_once(state: &mut State, c: &Config, args: &Capture) -> Result<usize> {
    let mut count = 0;
    for source in discover(c, args)? {
        loop {
            let n = match state.ingest(&source, c.max_records, c.max_bytes) {
                Ok(n) => n,
                Err(e) => {
                    eprintln!("capture {}: {e:#}", source.path.display());
                    break;
                }
            };
            count += n;
            let (records, bytes) = state.pending_size()?;
            if records >= c.max_records || bytes >= c.max_bytes {
                flush_all(state, c).await?;
            }
            if n == 0 {
                break;
            }
        }
    }
    Ok(count)
}
async fn watch(state: &mut State, c: &Config, args: &Capture) -> Result<()> {
    let (tx, mut rx) = tokio::sync::mpsc::channel(1);
    let mut watcher = notify::recommended_watcher(move |event: notify::Result<notify::Event>| {
        if event.is_ok() {
            let _ = tx.try_send(());
        }
    })?;
    for (_, paths) in roots(c, args)? {
        for path in paths {
            if path.exists()
                && let Err(e) = watcher.watch(&path, notify::RecursiveMode::Recursive)
            {
                eprintln!(
                    "watch {}: {e}; periodic scanning remains active",
                    path.display()
                );
            }
        }
    }
    let mut tick =
        tokio::time::interval(Duration::from_secs(c.flush_seconds.min(c.rescan_seconds)));
    let mut last_scan = Instant::now() - Duration::from_secs(c.rescan_seconds);
    let mut last_flush = Instant::now();
    let (sync_tx, mut sync_rx) = tokio::sync::mpsc::unbounded_channel();
    let mut uploading: Option<tokio::task::JoinHandle<()>> = None;
    loop {
        let changed = tokio::select! {
            _=tokio::signal::ctrl_c()=>break,
            _=tick.tick()=>false,
            event=rx.recv()=>event.is_some(),
            Some(update)=sync_rx.recv()=> {
                match update {
                    remote::SyncUpdate::Uploaded { remote,batch } => state.mark_uploaded(&remote,&batch)?,
                    remote::SyncUpdate::Failed(e) => eprintln!("sync pending; will retry: {e}"),
                    remote::SyncUpdate::Finished => uploading=None,
                }
                false
            }
        };
        if changed || last_scan.elapsed() >= Duration::from_secs(c.rescan_seconds) {
            capture_once(state, c, args).await?;
            last_scan = Instant::now();
        }
        if last_flush.elapsed() >= Duration::from_secs(c.flush_seconds) {
            flush_all(state, c).await?;
            if uploading.is_none() && !c.remotes.is_empty() {
                uploading = Some(remote::background_sync(state, &c.remotes, sync_tx.clone())?);
            }
            last_flush = Instant::now();
        }
    }
    flush_all(state, c).await?;
    if let Some(job) = uploading {
        job.abort();
    }
    eprintln!("Capture stopped; unpublished batches remain available for sync");
    Ok(())
}
fn query_options(r: &ReadOptions) -> Result<Query> {
    let normalize = |s: &Option<String>| -> Result<Option<String>> {
        s.as_ref()
            .map(|s| {
                Ok(chrono::DateTime::parse_from_rfc3339(s)?
                    .with_timezone(&chrono::Utc)
                    .to_rfc3339_opts(chrono::SecondsFormat::Millis, true))
            })
            .transpose()
    };
    let q = Query {
        harness: r.harness.clone(),
        session: r.session.clone(),
        since: normalize(&r.since)?,
        until: normalize(&r.until)?,
        kind: r.kind.clone(),
        text: r.text.clone(),
    };
    ensure!(
        q.since
            .as_ref()
            .zip(q.until.as_ref())
            .is_none_or(|(a, b)| a <= b),
        "--since must precede --until"
    );
    Ok(q)
}
async fn paths(state: &State, c: &Config, r: &ReadOptions, q: &Query) -> Result<Vec<PathBuf>> {
    if let Some(directory) = &r.archive {
        Ok(vec![absolute(directory)?])
    } else if let Some(name) = &r.remote {
        let remote = c
            .remotes
            .iter()
            .find(|x| x.name() == name)
            .with_context(|| format!("unknown remote {name}"))?;
        remote::cached_paths(remote, &state.root, q).await
    } else {
        state.batch_paths()
    }
}
fn default_hooks() -> Result<PathBuf> {
    Ok(directories::BaseDirs::new()
        .context("home directory")?
        .home_dir()
        .join(".cursor/hooks.json"))
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let mut c = Config::load(cli.config.as_deref(), cli.state_dir)?;
    if c.state_dir.is_relative() {
        c.state_dir = std::env::current_dir()?.join(&c.state_dir);
    }
    if let Command::Hooks { command } = &cli.command {
        match command {
            HookCommand::Receive => {
                if let Err(e) = hooks::receive(&c.state_dir, std::io::stdin().lock()) {
                    eprintln!("capture hook: {e:#}");
                }
                println!("{{}}");
            }
            HookCommand::Install { hooks_file, .. } => {
                hooks::install(
                    &hooks_file.clone().unwrap_or(default_hooks()?),
                    &c.state_dir,
                    &std::env::current_exe()?,
                )?;
                println!("Cursor capture hooks installed");
            }
            HookCommand::Uninstall { hooks_file, .. } => {
                hooks::uninstall(&hooks_file.clone().unwrap_or(default_hooks()?))?;
                println!("Cursor capture hooks removed");
            }
        }
        return Ok(());
    }
    if let Command::Discover(args) = &cli.command {
        for s in discover(&c, args)? {
            println!("{}", serde_json::to_string(&s)?);
        }
        return Ok(());
    }
    if let Command::Mcp { archive } = &cli.command {
        let mut server = mcp::Server::new(
            c.state_dir.clone(),
            archive.as_ref().map(|p| absolute(p)).transpose()?,
        );
        mcp::serve(
            &mut server,
            &mut std::io::stdin().lock(),
            &mut std::io::stdout().lock(),
        )
        .await?;
        return Ok(());
    }
    if let Command::Calibrate { labels, output } = &cli.command {
        let labels: Vec<regression::HumanLabel> = serde_json::from_slice(&std::fs::read(labels)?)?;
        let result = regression::calibrate(&labels)?;
        let bytes = serde_json::to_vec_pretty(&result)?;
        if let Some(path) = output {
            std::fs::write(path, &bytes)?;
        }
        println!("{}", String::from_utf8(bytes)?);
        return Ok(());
    }
    if let Command::Labels { task, history } = &cli.command {
        let registry = human::Registry::load(&c.state_dir.join("human-labels"))?;
        let labels = if *history {
            registry.labels.iter().collect()
        } else {
            registry.current()
        };
        let current_ids: HashSet<_> = registry.current().iter().map(|l| l.id.as_str()).collect();
        for l in labels
            .into_iter()
            .filter(|l| task.as_ref().is_none_or(|t| t == &l.task))
        {
            println!(
                "{}",
                serde_json::to_string(
                    &serde_json::json!({"id":l.id,"task":l.task,"session":l.trajectory.session_id,"harness":l.trajectory.harness,"trajectory_id":l.trajectory_id,"step":l.step,"reward":l.reward,"oracle":l.oracle,"reviewer":l.reviewer,"note":l.note,"origin":l.origin,"current":current_ids.contains(l.id.as_str()),"supersedes":l.supersedes})
                )?
            );
        }
        return Ok(());
    }
    let readonly = matches!(
        cli.command,
        Command::Status
            | Command::Query(_)
            | Command::Sessions(_)
            | Command::Export { .. }
            | Command::Compare(_)
            | Command::Snapshot(_)
            | Command::Check(_)
            | Command::Browse { .. }
            | Command::Label(_)
            | Command::Evaluate(_)
    );
    let mut state = if readonly && c.state_dir.join("state.sqlite").exists() {
        State::open_reader(&c.state_dir)?
    } else {
        State::open(&c.state_dir)?
    };
    match cli.command {
        Command::Label(args) => {
            let trajectory = session_input(&state, &c, &args.input).await?;
            ensure!(
                trajectory.steps.len() <= c.feedback.max_steps,
                "trajectory exceeds configured max_steps"
            );
            let label = human::annotate(
                &c.state_dir.join("human-labels"),
                trajectory,
                human::NewLabel {
                    task: args.task,
                    step: args.step,
                    reward: args.reward,
                    reviewer: args.reviewer,
                    note: args.note,
                    origin: if args.synthetic {
                        human::Origin::Synthetic
                    } else {
                        human::Origin::Human
                    },
                    oracle: args.oracle,
                    replace: args.replace,
                },
            )?;
            println!(
                "{}",
                serde_json::to_string_pretty(
                    &serde_json::json!({"label_id":label.id,"task":label.task,"session":label.trajectory.session_id,"trajectory_id":label.trajectory_id,"step":label.step,"reward":label.reward,"oracle":label.oracle,"origin":label.origin,"supersedes":label.supersedes})
                )?
            );
        }
        Command::Evaluate(args) => {
            let registry = human::Registry::load(&c.state_dir.join("human-labels"))?;
            let candidate = session_input(&state, &c, &args.input).await?;
            if let Some(mode) = args.matching {
                c.feedback.matching = mode;
            }
            criteria_file(&args.criteria, &mut c.feedback)?;
            let models = if args.judge {
                Some((
                    HttpModel::new(
                        c.feedback
                            .judge
                            .clone()
                            .context("configure feedback.judge")?,
                    )?,
                    HttpModel::new(
                        c.feedback
                            .reporter
                            .clone()
                            .context("configure feedback.reporter")?,
                    )?,
                ))
            } else {
                None
            };
            let result = human::evaluate(
                &registry,
                candidate,
                human::EvaluationOptions {
                    task: args.task,
                    oracle_label_ids: args.oracle_label,
                    config: c.feedback,
                    gates: args.gates,
                    human_gates: args.human_gates,
                },
                &absolute(&args.output)?,
                models.as_ref().map(|(j, r)| {
                    (
                        j as &dyn harness_durable::llm::LanguageModel,
                        r as &dyn harness_durable::llm::LanguageModel,
                    )
                }),
            )
            .await?;
            println!(
                "{} — {}\nReport: {}\nResult: {}",
                if result.passed { "PASS" } else { "FAIL" },
                result.task,
                absolute(&args.output)?.join("report.md").display(),
                absolute(&args.output)?.join("result.json").display()
            );
            if !result.passed {
                std::process::exit(2);
            }
        }
        Command::Snapshot(args) => {
            let trajectory = session_input(&state, &c, &args.input).await?;
            let mut config = c.feedback.clone();
            config.matching = args.matching;
            criteria_file(&args.criteria, &mut config)?;
            let result =
                regression::snapshot(&absolute(&args.output)?, trajectory, config, args.append)?;
            println!(
                "Accepted {} reference variant(s): {}",
                result.variants.len(),
                absolute(&args.output)?.display()
            );
        }
        Command::Check(args) => {
            let baseline = regression::load(&absolute(&args.baseline)?)?;
            let candidate = session_input(&state, &c, &args.input).await?;
            let models = if args.judge {
                Some((
                    HttpModel::new(
                        baseline
                            .config
                            .judge
                            .clone()
                            .context("configure feedback.judge")?,
                    )?,
                    HttpModel::new(
                        baseline
                            .config
                            .reporter
                            .clone()
                            .context("configure feedback.reporter")?,
                    )?,
                ))
            } else {
                None
            };
            let result = regression::check(
                &baseline,
                candidate,
                args.gates,
                &absolute(&args.output)?,
                models.as_ref().map(|(j, r)| {
                    (
                        j as &dyn harness_durable::llm::LanguageModel,
                        r as &dyn harness_durable::llm::LanguageModel,
                    )
                }),
            )
            .await?;
            println!(
                "{} — {}\n{}",
                if result.passed { "PASS" } else { "FAIL" },
                result.candidate_session,
                absolute(&args.output)?.join("check.json").display()
            );
            if !result.passed {
                std::process::exit(2);
            }
        }
        Command::Browse { read, pair_output } => {
            let q = query_options(&read)?;
            let events = archive::query(&paths(&state, &c, &read, &q).await?, &q).await?;
            recall::browse(
                &events,
                read.archive.as_ref().map(|p| absolute(p)).transpose()?,
                read.remote,
                &absolute(&pair_output)?,
                &mut std::io::stdin().lock(),
                &mut std::io::stdout().lock(),
            )
            .await?;
        }
        Command::Compare(mut args) => {
            if let Some(path) = &args.pair {
                let pair: recall::Pair = serde_json::from_slice(&std::fs::read(path)?)?;
                ensure!(
                    pair.archive.is_none() || pair.remote.is_none(),
                    "pair cannot specify both archive and remote"
                );
                args.oracle_session = Some(pair.oracle_session);
                args.candidate_session = Some(pair.candidate_session);
                args.oracle_harness.get_or_insert(pair.oracle_harness);
                args.candidate_harness.get_or_insert(pair.candidate_harness);
                if args.oracle_archive.is_none() && args.oracle_remote.is_none() {
                    args.oracle_archive = pair.archive.clone();
                    args.oracle_remote = pair.remote.clone();
                }
                if args.candidate_archive.is_none() && args.candidate_remote.is_none() {
                    args.candidate_archive = pair.archive;
                    args.candidate_remote = pair.remote;
                }
            }
            if let Some(mode) = args.matching {
                c.feedback.matching = mode;
            }
            criteria_file(&args.criteria, &mut c.feedback)?;
            let mut trajectories = Vec::new();
            for (session, harness, directory, remote, leaf, final_output) in [
                (
                    &args.oracle_session,
                    &args.oracle_harness,
                    &args.oracle_archive,
                    &args.oracle_remote,
                    &args.oracle_leaf,
                    &args.oracle_output,
                ),
                (
                    &args.candidate_session,
                    &args.candidate_harness,
                    &args.candidate_archive,
                    &args.candidate_remote,
                    &args.candidate_leaf,
                    &args.candidate_output,
                ),
            ] {
                let q = Query {
                    session: Some(session.clone().context("session must be selected")?),
                    harness: harness.clone(),
                    ..Query::default()
                };
                let p = if let Some(directory) = directory {
                    vec![absolute(directory)?]
                } else {
                    paths(
                        &state,
                        &c,
                        &ReadOptions {
                            remote: remote.clone(),
                            ..ReadOptions::default()
                        },
                        &q,
                    )
                    .await?
                };
                trajectories.push(feedback::trajectory(
                    archive::query(&p, &q).await?,
                    leaf.as_deref(),
                    final_output
                        .as_ref()
                        .map(std::fs::read_to_string)
                        .transpose()?,
                )?);
            }
            let candidate = trajectories.pop().unwrap();
            let oracle = trajectories.pop().unwrap();
            let plan = feedback::plan(oracle, candidate, c.feedback.clone()).await?;
            if args.dry_run {
                println!("{}", serde_json::to_string_pretty(&plan)?);
            } else {
                let judge = HttpModel::new(
                    c.feedback
                        .judge
                        .clone()
                        .context("configure feedback.judge")?,
                )?;
                let reporter = HttpModel::new(
                    c.feedback
                        .reporter
                        .clone()
                        .context("configure feedback.reporter")?,
                )?;
                let output = absolute(&args.output)?;
                let result = feedback::evaluate(&plan, &output, &judge, &reporter).await?;
                println!(
                    "Mean reward: {:.6} ({} steps)\nReport: {}\nLance archive: {}",
                    result.mean_reward.unwrap(),
                    result.scores.len(),
                    output.join("report.md").display(),
                    output.join("archive").display()
                );
                println!(
                    "Reference coverage: {:.4}\nRequired-work coverage: {:?}\nOutcome checks passed: {:?}\nDimensions: {}",
                    plan.diagnostics.reference_step_coverage,
                    result.required_work_coverage,
                    plan.diagnostics.outcome_passed,
                    serde_json::to_string(&result.dimension_means)?
                );
            }
        }
        Command::Import(args) => {
            let count = capture_once(&mut state, &c, &args).await?;
            flush_all(&mut state, &c).await?;
            eprintln!("Processed {count} complete source records");
            remote::sync(&state, &c.remotes, None).await?;
        }
        Command::Watch(args) => watch(&mut state, &c, &args).await?,
        Command::Sync { remote } => {
            flush_all(&mut state, &c).await?;
            remote::sync(&state, &c.remotes, remote.as_deref()).await?;
        }
        Command::Status => println!(
            "{}",
            serde_json::to_string_pretty(
                &state.status(
                    &c.remotes
                        .iter()
                        .map(|r| r.name().to_string())
                        .collect::<Vec<_>>()
                )?
            )?
        ),
        Command::Query(r) => {
            let q = query_options(&r)?;
            for e in archive::query(&paths(&state, &c, &r, &q).await?, &q).await? {
                if r.format == "jsonl" {
                    println!("{}", serde_json::to_string(&e)?);
                } else {
                    println!(
                        "{} {} {} {} {}",
                        e.harness,
                        e.session_id,
                        e.timestamp.as_deref().unwrap_or("—"),
                        e.kind,
                        e.text.as_deref().unwrap_or("")
                    );
                }
            }
        }
        Command::Sessions(r) => {
            let q = query_options(&r)?;
            let p = paths(&state, &c, &r, &q).await?;
            let mut records = Vec::new();
            let mut seen = HashSet::new();
            for path in &p {
                for rec in archive::read_records(path).await? {
                    if seen.insert(rec.id.clone())
                        && q.harness.as_ref().is_none_or(|h| h == &rec.harness)
                        && q.session.as_ref().is_none_or(|s| s == &rec.session_id)
                    {
                        records.push(rec);
                    }
                }
            }
            let events = archive::query(&p, &q).await?;
            let filtered =
                r.since.is_some() || r.until.is_some() || r.kind.is_some() || r.text.is_some();
            let matching: HashSet<_> = events.iter().map(|e| (&e.harness, &e.session_id)).collect();
            for s in archive::summaries(&records, &events) {
                if filtered && !matching.contains(&(&s.harness, &s.session_id)) {
                    continue;
                }
                if r.format == "jsonl" {
                    println!("{}", serde_json::to_string(&s)?);
                } else {
                    println!(
                        "{} {}: {} records, {} events",
                        s.harness, s.session_id, s.records, s.events
                    );
                }
            }
        }
        Command::Export { read, output } => {
            let q = query_options(&read)?;
            let p = paths(&state, &c, &read, &q).await?;
            let events = archive::query(&p, &q).await?;
            let ids: HashSet<_> = events.iter().map(|e| &e.record_id).collect();
            let mut records = Vec::new();
            let mut seen = HashSet::new();
            let event_filter =
                q.kind.is_some() || q.text.is_some() || q.since.is_some() || q.until.is_some();
            for path in p {
                for r in archive::read_records(&path).await? {
                    if seen.insert(r.id.clone())
                        && q.harness.as_ref().is_none_or(|h| h == &r.harness)
                        && q.session.as_ref().is_none_or(|s| s == &r.session_id)
                        && (!event_filter || ids.contains(&r.id))
                    {
                        records.push(r);
                    }
                }
            }
            let output = absolute(&output)?;
            archive::write_archive(
                &output,
                &state.collector_id,
                &uuid::Uuid::new_v4().to_string(),
                &records,
                &events,
            )
            .await?;
            println!("{}", output.display());
        }
        Command::Discover(_)
        | Command::Hooks { .. }
        | Command::Mcp { .. }
        | Command::Calibrate { .. }
        | Command::Labels { .. } => unreachable!(),
    }
    Ok(())
}
fn absolute(path: &Path) -> Result<PathBuf> {
    Ok(if path.is_absolute() {
        path.into()
    } else {
        std::env::current_dir()?.join(path)
    })
}
