# Architecture Report: harness-durable

| Item | Value |
| --- | --- |
| Language of this report | ASD-STE100 Simplified Technical English |
| Commit examined | `7e44ea2` (branch `main`) |
| Date | 2026-10-04 |
| Code examined | `src/` (Rust, 14 modules), `worker/src/index.ts` (TypeScript), `tests/`, `.github/workflows/ci.yml`, `README.md` |

## 1. Scope

This report gives the architecture of the harness-durable repository. It also gives the problems that we found and the corrective actions.

We read all the Rust source files, the Worker source file, the CI workflow, and the README. We compiled the project and ran the tests. Section 8 gives the test results.

## 2. Summary

harness-durable is a command-line tool. It copies coding-agent sessions from Codex, Pi, and Cursor into Lance datasets. It can send these datasets to Amazon S3 or to Cloudflare R2. It can also compare two sessions and give a score for each step.

The design for data safety is good. The tool writes data in immutable batches. Each batch has checksums and a completion marker. If the power fails, the tool does not lose committed data. It does not publish a partial batch.

We found no high-level problems. The primary problems are about performance. When the archive becomes larger, queries and rescans become slower. The tool reads all the events that match a query into memory. Each rescan opens all the source files again. The tool does not merge small batches.

## 3. System overview

### 3.1 Components

| Component | File | Function |
| --- | --- | --- |
| CLI | `src/main.rs` | Reads the command and the configuration. Calls the library modules. |
| Adapters | `src/adapters.rs` | Reads one harness format. Changes each JSON line into one record and zero or more events. |
| State | `src/state.rs` | Keeps checkpoints, record IDs, pending records, batches, and uploads in SQLite. |
| Archive | `src/archive.rs` | Writes and reads the Lance datasets. Does the queries. |
| Remote | `src/remote.rs` | Uploads batches to S3 or to the Worker. Downloads batches into a local cache. |
| Hooks | `src/hooks.rs` | Installs the Cursor hooks. Writes each hook message into a spool file. |
| Worker | `worker/src/index.ts` | Keeps files in R2. Keeps batch metadata in a Durable Object with SQLite. |
| Feedback | `src/feedback.rs`, `src/assessment.rs` | Aligns two trajectories. Sends steps to a judge model. Writes a report. |
| Regression | `src/regression.rs` | Keeps accepted baselines. Compares a new session with a baseline. |
| Human labels | `src/human.rs` | Keeps human labels and accepted oracles. |
| LLM | `src/llm.rs` | Sends HTTP requests to the judge model and to the report model. |
| Recall and MCP | `src/recall.rs`, `src/mcp.rs` | Gives a terminal browser and a read-only MCP server. |

### 3.2 Data flow for capture

1. The adapter finds the session files. It uses the default directories or the configured directories.
2. The State module reads the new complete lines after the last checkpoint.
3. The adapter changes each line into one record and its events.
4. The State module writes the new checkpoint, the record IDs, and the pending rows in one SQLite transaction.
5. When the pending data is sufficiently large or old, the Archive module writes a batch. A batch has two Lance datasets and one `manifest.json` file.
6. The Remote module uploads each new batch to each configured remote.

### 3.3 Data flow for evaluation

1. The tool reads the events of one oracle session and one candidate session.
2. The Feedback module divides each session into steps.
3. The Assessment module aligns the steps. The default mode uses the Lance BM25 index.
4. The judge model gives a reward of 0 or 1 to each candidate step.
5. The tool writes each judgment to `evaluation.json` before it sends the next request.
6. The report model writes `report.md`. Then the tool writes a Lance archive of the results.

### 3.4 Durable state

| Location | Content | Writer |
| --- | --- | --- |
| `STATE_DIR/state.sqlite` | Checkpoints, record IDs, pending rows, batches, uploads | One collector process. A file lock prevents a second writer. |
| `STATE_DIR/batches/<collector>/<batch>/` | `records.lance`, `events.lance`, `manifest.json` | Collector |
| `STATE_DIR/hooks/` | Hook spool files | `hooks receive` command |
| `STATE_DIR/human-labels/labels.json` | Labels and their history | `label` command. A separate file lock prevents a second writer. |
| `STATE_DIR/cache/` | Downloaded remote batches | Read commands |
| R2 and Durable Object SQLite | Files, and the tables `batches`, `files`, `sessions` | Worker |

## 4. Good points

### 4.1 Data safety

- The batch ID is a hash of the record IDs. After a crash, the tool calculates the same batch ID again. This prevents duplicate batches.
- The tool writes each batch into a temporary directory. It synchronizes all files and directories to disk. Then it renames the directory. A reader sees the full batch or no batch.
- `manifest.json` has the size and the SHA-256 of each file. The tool examines the checksums before an upload and after a download.
- For S3, the tool uses a conditional PUT for `registration.json` and `manifest.json`. Two collectors cannot write different content to the same batch.
- The Worker has three steps: register, upload the files, and commit. The commit occurs only when all the files have correct checksums. The commit is one SQLite transaction.
- Read-only commands open SQLite in read-only mode with WAL. They can read published batches while `watch` writes.

### 4.2 Source safety

- The tool opens the session files for read only. It does not change the harness files.
- The checkpoint has the file identity (device and inode) and the SHA-256 of the first and last 4 KiB. If a file is replaced or truncated, the tool reads the file again from the start. The record IDs prevent duplicate records.
- The tool does not read a last line that has no newline. It waits until the harness completes the line.
- The adapters keep the raw bytes of each record. If the tool cannot parse a record, it keeps the record with the status `malformed` or `unknown`.
- The Cursor hook "fails open". If the hook has an error, Cursor continues.

### 4.3 No false results

- The tool does not make false timestamps, false tool results, or a false global time sequence.
- A failed model request does not become a reward of 0. The tool stops and keeps all the completed judgments.
- Each metric is separate. The tool does not combine the metrics into one success score.

### 4.4 Security controls

- The model prompts tell the model that the session text is untrusted data, not instructions.
- The HTTP clients do not follow redirects. Remote URLs must use HTTPS. Only loopback addresses can use HTTP.
- The tool does not show the error body from a model provider, because the body can have secrets.
- The Worker compares SHA-256 digests of the tokens. The time of the comparison does not change with the token length or prefix.
- The MCP server is read-only. Each request and each response has a limit of 1 MiB.
- The state directory has the permission `0700`.

### 4.5 Tests and CI

- The repository has 38 Rust tests and 6 Worker tests. Tests use synthetic fixtures only.
- CI runs an S3 test with Moto and a Worker test with `wrangler dev`.
- CI also runs `cargo fmt --check` and `cargo clippy` with `-D warnings`.

## 5. Problems

Each problem has a level:

- **HIGH**: The problem can cause data loss or access by an unauthorized person.
- **MEDIUM**: The problem makes the tool slow, or stops it, when the quantity of data increases.
- **LOW**: The problem is small, or it occurs only in unusual conditions.

We found no HIGH problems.

### 5.1 Problem 1: Queries read all events into memory (MEDIUM)

File: `src/archive.rs`, function `query`.

The function reads all the events that match the filter from all batches into one `Vec`. Then it sorts the `Vec` two times. Lance does not apply the text filter. The Rust code applies it after the scan. The MCP tool `get_event` reads all events from all batches to find one event ID. The `sessions` and `export` commands also read all records.

Result: The time and the memory for each query increase with the size of the archive. The MCP server becomes slow first, because each tool call reads the archive again.

Actions:

1. Put the text filter and the event ID filter into the Lance scan.
2. Return a stream of events, not a `Vec`. Sort the events for each session separately.
3. Keep an index from event ID to batch in SQLite.

### 5.2 Problem 2: Many small batches and no compaction (MEDIUM)

Files: `src/main.rs` (function `watch`), `src/remote.rs` (function `list`).

The `watch` command writes a batch each 5 seconds when new data is available. Each batch has two Lance datasets. The tool does not merge batches. Each query reads the manifest of each batch. Each remote query lists all the remote manifests again. For S3, the tool sends one GET request for each manifest.

Result: After some months of use, an archive can have tens of thousands of batches. Then queries become slow. Remote queries become slow first.

Actions:

1. Add a compaction command. The command writes one merged batch. Its manifest lists the IDs of the old batches that it replaces. Readers then ignore the replaced batches.
2. Keep the list of remote manifests in the local cache. Read only the manifests after the last cursor.

### 5.3 Problem 3: Each rescan opens all sources again (MEDIUM)

Files: `src/adapters.rs` (function `discover`), `src/state.rs` (function `ingest`), `src/hooks.rs` (function `receive`).

Each 30 seconds, `discover` reads all the source directories. For each `.jsonl` file, it parses up to 32 lines to find the header. Then `ingest` opens each source and calculates two SHA-256 values. The hook spool makes one new file for each hook message. The tool does not delete spool files. The quantity of files increases with each tool call in Cursor.

Result: The CPU time and the disk time for each rescan increase without a limit.

Actions:

1. Keep the file size and the modification time in the checkpoint. Do not open a file if these values did not change.
2. Keep the result of `discover` in SQLite. Identify only new paths.
3. After a spool file is in a published batch, merge it into one file for each session, or delete it.

### 5.4 Problem 4: One Durable Object moves all file data for an archive (MEDIUM)

File: `worker/src/index.ts`.

The Worker sends each request, with its body, to the Durable Object of the archive. All uploads and downloads for one archive go through one Durable Object. A Durable Object does its work on one thread.

Result: All collectors that use the same archive have the speed of one Durable Object. Large uploads can also cause the Durable Object to reach its CPU or memory limits.

Actions:

1. Let the Worker stream the file bodies directly between the client and R2.
2. Use the Durable Object only for metadata: registration, file status, and commit.
3. Before an R2 write, the Worker must get permission from the Durable Object. After the write, the Worker must send the result to the Durable Object.

### 5.5 Problem 5: Pending rows keep raw bytes as a JSON array of numbers (LOW)

Files: `src/model.rs` (struct `Record`), `src/state.rs` (table `pending`).

The field `Record.raw` is a `Vec<u8>`. `serde_json` writes a `Vec<u8>` as an array of numbers, for example `[123,34,105]`. Each byte becomes 2 to 4 characters. The column `events` has `payload_json`, which is a second copy of the same JSON. The column `bytes` counts only `raw.len()`.

Result: The pending rows are approximately 4 to 6 times larger than the source data. The limit `max_bytes` does not show the real size in memory.

Actions:

1. Keep `raw` in a BLOB column.
2. Count all the columns in `bytes`.

### 5.6 Problem 6: The query finds hook events with a text search (LOW)

File: `src/archive.rs`, function `query`.

The query must keep transcript events before hook events. To identify a hook event, the code searches `payload_json` for the text `"hook_event_name"`. A transcript event can also have this text. For example, a session about Cursor hooks can have it.

Result: The query can keep the hook event and remove the transcript event. The raw data stays in the archive, but the query result is incorrect.

Action: Keep the source format (`hooks`) in a column. Use this column, not a text search.

### 5.7 Problem 7: Two copies of the manifest rules (LOW)

Files: `src/archive.rs` (function `validate_manifest`), `src/remote.rs` (function `Remote::new`), `worker/src/index.ts` (function `validate`).

The Rust code and the TypeScript code each have a copy of the `Manifest` type and of its rules. The rules are not the same. For example, Rust accepts the archive ID `team.prod`. The Worker accepts only `[a-zA-Z0-9_-]{1,128}`, and it returns `404 unknown route`.

Result: The user gets an error message that does not identify the cause.

Actions:

1. Use the same regular expression in Rust and in TypeScript.
2. Add a test that sends the same set of manifests to the two validators.

### 5.8 Problem 8: `main.rs` has too much logic (LOW)

File: `src/main.rs` (975 lines).

The file has the logic of the `sessions` and `export` commands. These two commands have two copies of the same record filter. A manual list sets which commands open SQLite in read-only mode. When a developer adds a command, the developer must also update this list.

Actions:

1. Move the logic of `sessions` and `export` into the library.
2. Add a method on `Command` that tells if the command writes the state. Use a `match` with no default arm. Then the compiler finds each new command.

### 5.9 Problem 9: Temporary directories stay after a crash (LOW)

Files: `src/archive.rs` (function `write_archive`), `src/remote.rs` (function `download`).

These functions write into `.tmp-<uuid>` directories. If the process stops during a write, the directory stays on the disk. The tool does not delete these directories when it starts again.

Result: The used disk space can increase slowly. The data is not damaged, because readers use only the paths in SQLite or in committed manifests.

Action: When the collector gets the lock, delete the `.tmp-*` directories in `batches/` and in `cache/`.

### 5.10 Problem 10: The Typesafe request uses part of the prompt text (LOW)

File: `src/llm.rs`, function `complete`.

For the Typesafe protocol, the code divides the system prompt at the text `" Return ONLY"`. If a developer changes this text in a prompt, the instructions to Jev change. No error occurs.

Actions:

1. Keep the Jev instructions in a separate constant.
2. Add a test that examines the request body.

### 5.11 Other small items (LOW)

- `retryable` in `src/remote.rs` accepts all `object_store` errors and all I/O errors. The tool also sends an "access denied" request again 4 times.
- `watch` calls `capture_once`, which uses blocking file I/O and SQLite on a Tokio worker thread. Use `spawn_blocking` for this work.
- The Worker commit compares each manifest file with each uploaded file. For 10,000 files, this is 100,000,000 comparisons. Use a `Map`.
- `Config::default` stops the process (`expect`) if the home directory is not available. Return an error.

## 6. Security notes

The design has one owner. Because of this:

- One Worker token gives access to all the archive IDs in the deployment.
- The tool does not remove secrets from the session text. Sessions often have API keys, file content, and command output. When you configure a remote, the tool uploads all of this data.
- The commands `compare`, `check --judge`, and `evaluate --judge` send session data to external model providers.

> **CAUTION:** DO NOT CONFIGURE A REMOTE OR A JUDGE FOR SESSIONS THAT HAVE SECRETS, UNLESS YOUR ORGANIZATION APPROVES THE DESTINATION. THE TOOL DOES NOT REMOVE SECRETS FROM THE DATA.

Action: Add an optional step that removes secrets before the Lance write. Also add an option to encrypt the batches before the upload.

## 7. Sequence of actions

| Step | Problem | Action | Size of change |
| --- | --- | --- | --- |
| 1 | 1 | Put filters into the Lance scan. Return a stream. | Medium. `src/archive.rs`, `src/recall.rs`. |
| 2 | 3 | Do not open unchanged files. Merge or delete old spool files. | Small. `src/state.rs`, `src/hooks.rs`. |
| 3 | 2 | Add compaction and a cache of the remote manifest list. | Large. The manifest must have a new field for replaced batches. |
| 4 | 4 | Stream file bodies outside the Durable Object. | Medium. `worker/src/index.ts`. |
| 5 | 5, 6, 7 | Correct the formats and the validators. | Small. Problem 6 needs a new column in the events schema. |
| 6 | 8, 9, 10, 11 | Correct the code structure. | Small. |

## 8. Test results

| Test | Result |
| --- | --- |
| `npm run typecheck` (Worker) | Pass |
| `npm test` (Worker) | Pass. 6 of 6 tests. |
| `cargo test --locked` (Rust) | RUST_RESULT |

Note: The tests that use S3 and `wrangler dev` (`tests/cloud.rs`) are ignored by default. We did not run them. CI runs them.
