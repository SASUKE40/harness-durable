# Harness Durable

Capture Codex, Pi, and Cursor sessions into portable, native [Lance](https://github.com/lance-format/lance) datasets. Import existing history, watch active sessions, and synchronize archives to S3 or Cloudflare R2 with Durable Objects.

The collector and query engine are Rust. Cloudflare runs a small TypeScript service that stores files in R2 and publication metadata in a SQLite-backed Durable Object. No Lance runtime or conversion runs inside a Worker.

## Build and try it

Requires Rust 1.91 or newer, a C/C++ toolchain, and `protoc`. The initial build includes Lance/Arrow/DataFusion and takes several minutes. macOS and Linux are supported.

```sh
cargo build --release --locked
./target/release/harness-durable --help

# Try synthetic fixtures without reading any personal sessions.
./target/release/harness-durable --state-dir /tmp/harness-demo import \
  --harness pi --path tests/fixtures/pi.jsonl
./target/release/harness-durable --state-dir /tmp/harness-demo sessions
./target/release/harness-durable --state-dir /tmp/harness-demo query \
  --kind tool_result --format jsonl
```

Install with `cargo install --path . --locked`. The CLI's global options are `--config PATH` and `--state-dir PATH`.

```sh
# Read-only discovery: prints source identities as JSONL.
harness-durable discover
harness-durable discover --harness codex

# Without --path, capture standard roots for selected harnesses.
harness-durable import --harness codex
harness-durable watch --harness pi
harness-durable watch --project /path/to/project

# Explicit input requires a harness. Repeat --path for multiple sources.
harness-durable import --harness cursor --path /path/to/transcript.jsonl
harness-durable watch --harness cursor --path /path/to/cli-output.ndjson

harness-durable status
harness-durable sessions --format jsonl
harness-durable query --session SESSION_ID --kind message
harness-durable query --harness pi --text "error" \
  --since 2026-10-01T00:00:00Z --until 2026-10-02T00:00:00Z --format jsonl
harness-durable export --session SESSION_ID --output /tmp/session-export
```

The export directory contains `records.lance`, `events.lance`, and `manifest.json`. Both `.lance` directories are standalone datasets readable with official Lance tooling, for example `lance.dataset("/tmp/session-export/events.lance")` in Python. Export destinations must not already exist.

Query filters are optional and intersect. Text matching is case-sensitive substring matching. Timestamps are optional UTC RFC3339 strings normalized to milliseconds; time filters exclude events whose source has no timestamp. Ordering is deterministic by harness, session, source, byte position, and content-block index. Separate transcript/hook streams retain their own order; there is no fabricated global chronology. `sessions` counts deduplicate record IDs and correlated tool events.

## Trajectory feedback: oracle versus candidate

`compare` evaluates one candidate session against one oracle session. It uses **Lance's native inverted index and BM25 search** to retrieve similar oracle steps, then computes a maximum-weight, monotonic, one-to-one alignment. This preserves chronological order while allowing unmatched steps on either side. BM25 similarity is a retrieval signal, not a reward.

The default judge is [TypeSafe Jev `jev-1.13.0`](https://docs.typesafe.ai/models), using its [native Choice API](https://docs.typesafe.ai/api) with options `0` and `1`. Its selected option becomes the binary reward; probabilities, confidence, actual provider model, and usage are preserved. Jev does not generate explanations: its stored rationale identifies the typed decision, and the report model supplies the analysis. The default reporter is [Claude Opus 5.5 (`claude-opus-5-5`)](https://platform.claude.com/docs/en/models/opus-5-5/overview), through Anthropic's Messages API. Configure either model independently in TOML.

```sh
# Import both tasks first, then inspect their session IDs.
harness-durable sessions

# Local alignment preview: no credentials and no inference requests.
harness-durable compare \
  --oracle-session ORACLE_ID --candidate-session CANDIDATE_ID \
  --output ./comparison --dry-run > alignment.json

# Set TYPESAFE_API_KEY and ANTHROPIC_API_KEY in your environment.
# This command sends the selected session evidence to the configured providers.
harness-durable compare \
  --oracle-session ORACLE_ID --candidate-session CANDIDATE_ID \
  --output ./comparison

# Native Lance reward/report events can be queried with the regular CLI.
harness-durable query --archive ./comparison/archive \
  --kind feedback_reward --format jsonl
```

A step is an assistant message content block or a tool call with its available, correlated tool results. An orphan tool result remains a separate step. User prompts, reasoning, compaction, metadata, and unknown events remain in the evaluation context but are not independently rewarded. Repeated identical messages at different positions remain separate steps.

Every candidate step receives one binary judgment, including unmatched steps; a valid alternative approach can earn `1` without a lexical match. **Mean reward = sum of candidate step rewards / number of candidate steps.** Unmatched oracle steps are listed separately and analyzed in the report; they do not add implicit zeroes to this denominator. The mean measures candidate step quality, not oracle coverage or independently verified task success. Empty trajectories fail rather than receiving a fabricated score.

Each judge request receives the candidate step, its matched oracle step, and chronological event prefixes through those steps (including attached tool results), without later outcomes. Unmatched steps receive oracle context through the preceding match, if any. The report receives both complete trajectories, rewards, the computed mean, unmatched oracle steps, and observed final outputs. By default, final output is the trailing assistant message, if there is one after the last user/tool event. This is labeled as an observation, never proof of completion. `--oracle-output FILE` and `--candidate-output FILE` supply explicit UTF-8 final outputs when needed. Missing final output stays absent.

Additional selection options:

- `--oracle-harness` / `--candidate-harness` disambiguate IDs across harnesses.
- `--oracle-archive DIR` / `--candidate-archive DIR` read standalone Lance exports.
- `--oracle-remote NAME` / `--candidate-remote NAME` read committed remote archives through the existing local cache.
- `--oracle-leaf ENTRY_ID` / `--candidate-leaf ENTRY_ID` choose Pi branch ancestry. Branched sessions require a selection; sibling branches are never blended.

Within one source, byte position and content-block index determine chronology. Multiple source streams require timestamps for every selected event; absent cross-source chronology is reported as an error instead of invented. Equal timestamps use source ID and source position as a deterministic tie-breaker.

Results in `--output`:

- `plan.json`: input snapshots, event IDs, rubric, model configuration without credentials, BM25 alignment, and content-derived comparison ID.
- `evaluation.json`: durable checkpoint, binary judgments, provider responses, mean, and generated report.
- `report.md`: the report for reading or sharing.
- `archive/records.lance`, `archive/events.lance`, and `archive/manifest.json`: standalone, checksummed Lance archive with `feedback_reward`, `feedback_dimension`, `feedback_milestone`, `feedback_diagnostics`, and `feedback_report` events. Scores and source event references live in `payload_json`.

Repeat the identical command to resume. Completed judgments are reused after a judge/report outage; a different input, rubric, or model configuration requires a new output directory. Invalid responses and failed requests never become zero rewards. Partial runs have no published Lance archive. Two processes cannot write the same evaluation directory concurrently. Feedback artifacts are separate from the captured-session spool and are not automatically synchronized to cloud destinations.

The default limits are 2,000 steps per trajectory and 1 MiB per model input. Inputs are not silently truncated. Provider token limits still apply (Jev has a smaller context than Opus); an oversized input fails with completed judgments preserved. Both model calls have a 120-second timeout and bounded retries for connection failures, timeouts, rate limits, and server errors. Live provider tests require credentials; routine tests use synthetic evidence and local mock servers.

## Matching, evidence, and regression checks

The evaluation and recall additions are implemented directly in Rust. They do not depend on AgentEvals, OpenJudge, EvalView, or sessiongrep. The existing Cloudflare deployment service remains TypeScript.

`compare --matching MODE` selects a policy:

- `bm25` (comparison default): native Lance retrieval followed by chronological, one-to-one alignment.
- `strict` (snapshot default): compare each position. Tool name and normalized JSON arguments must match; native call IDs do not have to match.
- `unordered`: exact actions with multiset semantics. Repeated actions consume separate reference occurrences.
- `required`: require selected oracle actions in order, allowing extra candidate actions and omitted nonrequired reference actions. Requires `required_steps` in the criteria file.

Exact matching also compares message text. Regression diffs check recorded tool results as well as actions, and distinguish changed, missing, and extra steps. Strict matching intentionally treats an insertion as positional changes. BM25 aligns similar actions but does not make them equivalent for a regression gate.

`--criteria FILE` on `compare` or `snapshot` supplies JSON with `required_steps`, `milestones`, and `outcome_checks`; omitted lists default to empty. See [examples/criteria.json](examples/criteria.json) and adapt its tool name and expected result to your transcripts. Oracle step numbers in criteria are **1-based**; low-level alignment JSON indices remain **0-based**.

- `required_steps`: exact structural requirements, matched one-to-one in oracle order, independently of the alignment policy. BM25 overlap cannot fulfill these requirements. Use semantic milestones when alternative implementations should qualify.
- `milestones`: each has `id`, `description`, and optional `oracle_steps` evidence hints. The judge scores achievement from the complete candidate evidence, accepting alternate tools and split/merged steps. These are task-level judgments; unlike step judgments, they can see later outcomes.
- `outcome_checks`: deterministic `final_equals` (`expected`), `final_contains` (`text`), `tool_result_contains` (`tool`, `text`), or `artifact_sha256` (`path`, `sha256`), each with an `id`. Artifact paths must be absolute. These checks read existing evidence/files; they do not execute tests. Transcript substring checks prove only that the text was recorded. A missing final output, result, or artifact fails its configured check. No configured checks means outcome is unknown.

The default evaluation now saves separate **tool correctness** and **progress** binary judgments in addition to quality. Tool correctness applies only to tool steps; progress applies to every candidate step. Their means remain separate. Required-work coverage is `(exact requirements met + semantic milestones rewarded 1) / declared requirements`; it is unknown when none are declared. Reference alignment coverage and exact-action repetition are diagnostics, not proof of success or waste. Repeated actions can be legitimate.

With `N` candidate steps, `T` tool steps, and `M` semantic milestones, a default evaluation makes `2N + T + M` judge requests and one report request, excluding retries. Set `feedback.dimensions = []` for quality-only step scoring. Milestones still require their own judgments. Every successful decision is checkpointed before the next request. Comparison format is now version 2; use a new output directory for old version 1 comparisons (their published Lance archives remain readable).

```sh
# Explicitly accept a reference; snapshot does not claim the task succeeded.
harness-durable snapshot --session ORACLE_ID --output ./baseline
# Add an alternative accepted trajectory under the SAME policy (up to five).
harness-durable snapshot --session ALTERNATE_ID --output ./baseline --append

# Offline CI gate: no model calls. Defaults allow no missing, extra, or changed steps.
harness-durable check --session CANDIDATE_ID \
  --baseline ./baseline --output ./regression

# Declared milestones need --judge. This sends evidence to configured providers.
# Model configuration is pinned in the baseline; credentials come from the environment.
harness-durable snapshot --session ORACLE_ID --criteria examples/criteria.json \
  --output ./semantic-baseline
harness-durable check --session CANDIDATE_ID --baseline ./semantic-baseline \
  --output ./judged-regression --judge --min-quality 0.8 \
  --min-tool-correctness 0.9 --min-progress 0.8 --require-outcome
```

`check` exits **0** on pass, **2** on regression, **1** on an operational error. It saves `check.json` and `check.md`, and evaluates each accepted variant independently: one whole variant must pass every gate. It never combines the best scores from different references. Baseline/check directories are locked and conflicting reuse is rejected. Commit reviewed `baseline.json` snapshots to your own repository when appropriate; they contain full session evidence, not credentials from model configuration.

Optional gates: `--max-missing-steps`, `--max-extra-steps`, `--max-changed-steps`, `--min-quality`, `--min-required-coverage`, `--min-tool-correctness`, `--min-progress`, `--max-repetition-ratio`, `--require-outcome`. A requested metric that is unavailable cannot pass. Declared required steps, milestones, and outcome checks must pass independently of average-score thresholds. Without `--judge`, declared semantic milestones are reported as unjudged failures. Final-output changes also fail when no changed steps are allowed (except in `required` mode, where output requirements must be explicit). Tune change allowances when semantic equivalence is intended.

For CI, import the current run into an isolated `--state-dir`, then run `check` against the reviewed baseline and retain the output directory as a build artifact. No provider credentials are needed for structural/outcome-only checks. `snapshot` and `check` also accept `--archive DIR`, `--remote NAME`, `--harness`, `--leaf`, and `--final-output FILE`.

## Terminal browsing and read-only MCP recall

```sh
harness-durable browse --pair-output ./pair.json
# Commands: list [text], next, prev, show N, oracle N, candidate N, preview, save, quit
harness-durable compare --pair ./pair.json --output ./selected-comparison --dry-run

# Start an MCP stdio server for local committed archives.
harness-durable mcp
# Or serve a standalone exported archive, without creating collector state.
harness-durable mcp --archive /absolute/path/to/export
```

The browser is a portable line-oriented terminal interface. `preview` shows BM25 alignment without inference; `save` explicitly writes a pair selection and refuses overwrites. For Pi branches, supply `compare --oracle-leaf` / `--candidate-leaf`. Session text is escaped before terminal rendering.

Configure your MCP client to launch `/absolute/path/to/harness-durable` with arguments `["--state-dir", "/absolute/path/to/state", "mcp"]`. The stdio server implements initialization, `tools/list`, and `tools/call` with three read-only tools: `search_sessions`, `read_session`, and `get_event`. Search is case-insensitive substring matching over text/session/harness. Reads support `offset` and `limit` (1–100), return stable event IDs, and reject ambiguous session IDs unless a harness is supplied. Oversized events return explicitly marked previews; use `query --format jsonl` to inspect full evidence. Requests and response data are bounded; queries currently materialize matching archive events before pagination. There is no network listener, remote sync, execution, or inference through MCP. Only committed local archives are visible; configure archive scope at server startup.

## Judge calibration

`calibrate` compares saved quality judgments with human-provided binary labels. It makes no model requests and does not generate labels.

```json
[
  {"evaluation": "/absolute/path/to/comparison", "candidate_step": 1, "human_reward": 1},
  {"evaluation": "/absolute/path/to/comparison", "candidate_step": 2, "human_reward": 0}
]
```

```sh
harness-durable calibrate --labels labels.json --output calibration.json
```

The output reports confusion counts, agreement, precision, and recall overall and grouped by judge/model/rubric configuration. Step numbers are 1-based; duplicate labels and invalid saved judgments are rejected. Undefined ratios are null. Use representative held-out labels; agreement on a small selected sample is not a general accuracy estimate. This first calibration command covers the original quality reward, not the separate dimension/milestone judgments.

## Human labels and accepted oracles

`label` records an explicit review of an archived task result or one of its steps. `--task` groups runs of the **same task**; use a different task key when the request or acceptance criteria change. Each annotation pins the complete trajectory, final output, and content hash, plus reviewer name, note, and creation time. A later append to the session creates a different snapshot and does not inherit earlier labels.

```sh
# Record a passing result and accept that exact snapshot as an oracle.
harness-durable label --task parser-fix --session ORACLE_ID \
  --reward 1 --reviewer edward --note "Reviewed implementation and test evidence" --oracle

# Accept another valid solution by labeling another session under the same task.
harness-durable label --task parser-fix --session ALTERNATE_ID \
  --reward 1 --reviewer edward --note "Accepted alternative implementation" --oracle

# Supply a candidate task verdict and, optionally, individual step judgments.
harness-durable label --task parser-fix --session CANDIDATE_ID \
  --reward 0 --reviewer edward --note "Required verification is missing"
harness-durable label --task parser-fix --session CANDIDATE_ID --step 1 \
  --reward 1 --reviewer edward --note "Inspected the relevant source"

harness-durable labels --task parser-fix
harness-durable labels --task parser-fix --history

# Apply the accepted oracles and candidate labels without any model requests.
harness-durable evaluate --task parser-fix --session CANDIDATE_ID \
  --output ./review-evaluation --require-human-label

# Optional human step gates; unlabeled steps remain unscored.
harness-durable evaluate --task parser-fix --session CANDIDATE_ID \
  --output ./fully-reviewed --require-human-label \
  --min-human-quality 0.9 --min-human-coverage 1
```

Only a passing **whole-task** label can be an oracle. Repeat `--oracle-label LABEL_ID` on `evaluate` to select particular active references; otherwise it uses all active oracles for the task, up to five. The exact candidate snapshot cannot also be an oracle. Matching defaults to configured BM25; `--matching`, `--criteria`, and the existing regression gates work as on `compare`/`check`. One complete reference variant must satisfy the regression gates. Structural differences still fail default zero-tolerance gates even when a human task label is positive; choose matching and allowances to fit your task.

Human task reward, mean human step reward, human labeling coverage, model quality, and required-work coverage are separate. A human task failure always fails the applied evaluation. A passing task label does not generate passing step labels or override failed outcome checks. The human step mean uses only explicitly labeled steps; `--min-human-coverage` prevents a small labeled subset from appearing complete. Missing task labels are allowed unless `--require-human-label` is specified; without them a structural pass is not human approval. `evaluate` exits 0 on pass, 2 on failed gates/review, and 1 on an operational error.

Add `--judge` to run configured Jev/Opus evaluation. Step judgments remain independent of candidate human labels; the report model receives annotations and reviewer notes to explain disagreements. These provider calls send selected session evidence and review annotations. Saved model decisions are reused on identical retries. The `--min-quality` flag gates the model mean; `--min-human-quality` gates the human mean.

Labels live in `STATE_DIR/human-labels/labels.json`, with atomic writes and a separate writer lock so labeling can coexist with capture. There is one active annotation per task/snapshot/step. Identical retries are idempotent. To change a verdict, note, reviewer, or oracle status, repeat `label` with `--replace`; superseded revisions remain in the history. To revoke an oracle, replace its whole-task label and omit `--oracle`. Reviewer names are supplied metadata, not authenticated accounts. This is a single-owner review workflow, not a multi-reviewer voting system.

Evaluation output contains:

- `human-input.json`: pinned annotations, accepted oracle snapshots, candidate evidence, and policy.
- `result.json` and `report.md`: combined decision with separate human/model scores and failures.
- `regression/check.json` and `regression/check.md`: reference selection and structural/outcome details.
- `regression/<variant-id>/`: resumable model checkpoints, report, and feedback Lance archive when `--judge` is used.
- `archive/`: standalone Lance records/events/manifest containing `human_label`, `human_evaluation`, and `human_report` events, including offline runs.

```sh
harness-durable query --archive ./review-evaluation/archive \
  --kind human_label --format jsonl
```

Changed labels, selected oracles, policies, or candidate evidence require a new evaluation directory. Existing reports remain historical snapshots. Annotations and evaluation archives are local and are not automatically added to the collector's cloud upload queue. `label` and `evaluate` accept `--archive`, `--remote`, `--harness`, `--leaf`, and `--final-output` for source selection.

For a complete synthetic demonstration with two accepted oracle variants, a failing candidate, and a passing candidate:

```sh
cargo build --locked
sh examples/human-review-demo.sh
```

The script uses an isolated temporary state directory and explicitly marks its annotations `--synthetic`. Applying them requires `--allow-synthetic`; reports identify them as demonstration labels, never actual human review. The demo uses no model APIs or cloud uploads and does not execute the tool commands recorded in its fixtures.

## Configuration

See [config.example.toml](config.example.toml). By default the configuration is `~/.harness-durable/config.toml`, and state is stored in `~/.harness-durable`. `--config` overrides the configuration location; `--state-dir` overrides storage only. Paths are literal and do not expand shell variables or `~`.

Default discovery roots:

- Codex: `$CODEX_HOME/sessions` and `$CODEX_HOME/archived_sessions`; `CODEX_HOME` defaults to `~/.codex`.
- Pi: `$PI_CODING_AGENT_SESSION_DIR`, otherwise `~/.pi/agent/sessions`.
- Cursor: JSONL transcripts beneath `~/.cursor/projects`, plus this collector's hook spool.

An explicit `sources` entry replaces default discovery roots for its harness. `--path` replaces configured/default roots for the selected harness. `--project` matches the recorded working directory or the source path by substring. Inputs are read-only; capture never modifies harness sessions.

The adapter API (`SessionAdapter`) provides roots, source identification, and normalization. Shared capture code handles incremental reads and durable checkpoints. Add a new adapter and register its name in `adapters::adapter` and the CLI to extend the supported harnesses.

## Cursor live enrichment

Desktop transcripts do not always include tool results. Optional hooks capture future tool outputs and lifecycle metadata. They cannot recover results omitted from historical files.

```sh
harness-durable hooks install cursor
harness-durable watch --harness cursor
# Later:
harness-durable hooks uninstall cursor
```

Installation uses the current executable's absolute path, so install the binary in a permanent location first. Default configuration is `~/.cursor/hooks.json`; use `--hooks-file PATH` for a different scope. The installer adds `sessionStart`, `sessionEnd`, `postToolUse`, and `postToolUseFailure` entries and records ownership next to the config. Uninstallation removes only unchanged entries owned by this installation, preserving unrelated hooks and settings.

The hidden `hooks receive` command accepts Cursor's JSON on stdin, writes an immutable local spool item, and emits `{}`. It makes no network request and fails open. The original stdin bytes are retained alongside compact JSONL; the archive's raw record uses the original bytes. Repeated deliveries remain in the raw archive, while matching `(harness, session, kind, tool-call ID)` events are collapsed in query results, preferring transcript events.

For CLI capture, save Cursor's structured output using its documented `--print --output-format stream-json` options and import/watch that file. Streaming deltas are classified separately from complete assistant messages; terminal result summaries are lifecycle events. The collector does not launch or control the harness.

## S3

Create a private test or production bucket yourself, then add:

```toml
[[remotes]]
name = "s3"
kind = "s3"
bucket = "my-session-archive"
prefix = "harness"
region = "us-west-2"
```

Credentials use the AWS SDK default provider chain, including environment variables, shared profiles, workload identity, and instance/task roles. Temporary credentials refresh through the provider. For S3-compatible endpoints, set `endpoint`; `allow_http = true` is only needed for local HTTP services. Grant list/get/put and multipart-upload permissions under the configured prefix. No bucket creation or remote deletion is performed by the collector.

```sh
harness-durable sync --remote s3
harness-durable sessions --remote s3
harness-durable query --remote s3 --session SESSION_ID --format jsonl
```

Files live below `<prefix>/<collector-id>/<batch-id>/`. Conditional registration reserves the batch inventory. Dataset objects upload before `manifest.json`, the publication marker. Readers ignore registrations and unfinished uploads. Multiple machines have separate collector IDs and never mutate a shared Lance dataset.

## Cloudflare R2 + Durable Objects

Requires Node 22+ and a Cloudflare account with R2 and SQLite-backed Durable Objects enabled. Deployment is explicit:

```sh
cd worker
npm ci
npx wrangler login
npx wrangler r2 bucket create harness-durable
npx wrangler secret put API_TOKEN
npm run deploy
```

Use a strong private token. Adjust the bucket binding in `worker/wrangler.toml` if you choose a different bucket name. Add a collector destination:

```toml
[[remotes]]
name = "cloudflare"
kind = "cloudflare"
url = "https://harness-durable.YOUR-SUBDOMAIN.workers.dev"
archive = "personal"
token_env = "HARNESS_DURABLE_TOKEN"
```

Set `HARNESS_DURABLE_TOKEN` to the same token, then use `sync`, `sessions`, `query`, or `export` with `--remote cloudflare`. Multiple collectors can use the same archive. This is a single-owner service: possession of the token grants access to all archive IDs in that deployment. No public file URLs are created.

The API requires `Authorization: Bearer TOKEN`. All routes begin `/v1/archives/{archive}`:

- `PUT /batches/{collector}/{batch}` registers the JSON manifest. Identical registration is idempotent; conflicting content returns 409.
- `PUT /batches/{collector}/{batch}/files/{relative-path}` streams a registered file to R2 with its declared length and SHA-256 checksum.
- `POST /batches/{collector}/{batch}/commit` publishes only when every registered file was successfully verified. Incomplete inventories return 409.
- `GET /batches?after=CURSOR` lists committed manifests, 100 per page, with an optional `next` cursor.
- `GET /sessions?after=CURSOR` lists committed per-batch session summaries. The CLI computes deduplicated counts from Lance data.
- `GET /batches/{collector}/{batch}/files/{relative-path}` downloads a published file; unpublished files return 404.

Metadata requests are limited to 1 MiB and 10,000 files/sessions per manifest. Dataset uploads stream through the Worker; normal Cloudflare request-size limits still apply. One Durable Object coordinates each archive. R2 writes verify content before SQLite marks files uploaded. Publication and session metadata update atomically in SQLite.

## Durability and format

The schemas carry `harness_durable_schema_version = 1`; files use stable Lance storage version 2.1. Dependencies and lockfiles pin the writer implementation.

- `records.lance`: `id`, `harness`, `session_id`, `source_id`, `source_path`, `position` (uint64 byte offset), `captured_at`, `status`, nullable `diagnostic`, `adapter_version`, and `raw` (binary).
- `events.lance`: `id`, `record_id`, `harness`, `session_id`, `source_id`, nullable `parent_session_id`, `native_id`, `parent_id`, `position`, `sub_index` (uint32), nullable `timestamp`, `kind`, nullable `role`, `text`, `model`, `tool_call_id`, and `payload_json`.

All other columns are UTF-8. `payload_json` preserves structured normalized content; complete source information remains in `records.raw`. Event kinds include message, tool_call, tool_result, metadata, lifecycle, compaction, branch, model_change, usage, reasoning, attachment, delta, auxiliary, and unknown. Unknown or malformed complete records remain recoverable and appear in diagnostics/status. Messages duplicated in Codex event notifications are auxiliary events.

SQLite uses WAL and FULL synchronization. Source checkpoints, seen IDs, and pending records commit together. Batch files and directories are synchronized before atomic publication; pending payloads are removed only after the batch is registered in SQLite. A crash after publication reuses the deterministic batch ID. The compact seen-ID catalog is retained to make repeated imports idempotent.

The watcher combines filesystem notifications with 30-second rescans. Batches flush after 5 seconds, 1,000 records, or 8 MiB; one oversized record is kept intact. Network transfers run separately, retry transient upload failures with bounded backoff, and leave failed uploads pending. Ctrl-C flushes local records and cancels outstanding transfers safely; use `sync` to drain the backlog. `status`, `sessions`, `query`, and `export` can read published batches during watch. Only one writer uses each state directory.

Renames, replaced files, truncation, and changed checkpoint boundaries trigger replay with deterministic deduplication. A trailing line without a newline is deferred until the writer completes it. Sources are treated as append-only between checkpoints; arbitrary edits deep inside a previously consumed file should be reimported using a fresh state directory. Raw IDs distinguish equal content at different source positions.

Remote queries list committed manifests, select relevant sessions, download and checksum-verify datasets into a local cache, and scan them with Lance. Queries do not execute on the Worker. This initial version favors inspectable immutable batches over automatic compaction; use `export` for a consolidated dataset.

## Tests

```sh
cargo fmt --check
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked
cd worker
npm ci
npm run typecheck
npm test
```

Fixtures contain synthetic sessions only. Tests cover parser semantics, branches/compaction, unknown records, incomplete lines, replacements, mirrored data, hook ownership/correlation, native Lance round trips, crash recovery, checksums, Worker authentication, concurrent collectors, and idempotent publication.

Run the S3 integration test with a local Moto server or an isolated S3-compatible test bucket. Set `HARNESS_TEST_S3_ENDPOINT`, `HARNESS_TEST_S3_BUCKET`, and AWS credentials, then run `cargo test --test cloud s3_round_trip -- --ignored`. CI runs this with Moto. No production account is needed.

For Rust-to-Worker integration, run `npx wrangler dev --var API_TOKEN:test-token` in `worker`, set `HARNESS_DURABLE_TOKEN=test-token`, and run `cargo test --test cloud cloudflare_round_trip -- --ignored`. `HARNESS_TEST_CLOUDFLARE_URL` defaults to `http://127.0.0.1:8787`. Service tests write synthetic data under unique archive/prefix IDs and do not delete cloud data.

## Boundaries

V1 archives and queries locally available JSONL/NDJSON sessions; it does not restore harness state, translate sessions between harnesses, scrape private Cursor databases, or claim coverage of remote-only/compressed proprietary history. Missing timestamps/results remain missing. External attachment URLs are preserved, not fetched. There is no automatic redaction: raw session text and tool outputs are intentionally retained. Remote synchronization begins only after you configure a destination. Retention/deletion, team identities, a web UI, and semantic search are outside this release.
