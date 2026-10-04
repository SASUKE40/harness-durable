#!/bin/sh
# All inputs and annotations are synthetic. No provider or cloud requests.
set -eu
project=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
binary=${HARNESS_DURABLE_BIN:-"$project/target/debug/harness-durable"}
if [ ! -x "$binary" ]; then
  echo "Build first: cargo build --locked" >&2
  exit 1
fi
demo=$(mktemp -d /tmp/harness-human-demo.XXXXXX)
cat > "$demo/config.toml" <<CONFIG
state_dir = '$demo/state'
remotes = []
CONFIG
hd() { "$binary" --config "$demo/config.toml" "$@"; }
fixtures="$project/tests/fixtures/human-review"
hd import --harness codex \
  --path "$fixtures/review-oracle-a.jsonl" \
  --path "$fixtures/review-oracle-b.jsonl" \
  --path "$fixtures/review-candidate.jsonl" \
  --path "$fixtures/review-verified.jsonl"
cat > "$demo/criteria.json" <<'CRITERIA'
{
  "required_steps": [1, 2, 3, 4],
  "outcome_checks": [
    {"kind":"tool_result_contains","id":"tests-passed","tool":"exec_command","text":"test result: ok"},
    {"kind":"final_contains","id":"final-answer","text":"Fixed parser handling for empty input."}
  ]
}
CRITERIA
for session in review-oracle-a review-oracle-b; do
  hd label --task parser-fix --session "$session" --reward 1 \
    --reviewer synthetic-demo --note "Fixture: accepted solution with recorded verification" \
    --oracle --synthetic > "$demo/$session-label.json"
done
hd label --task parser-fix --session review-candidate --reward 0 \
  --reviewer synthetic-demo --note "Fixture: missing verification step" \
  --synthetic > "$demo/candidate-label.json"
hd label --task parser-fix --session review-verified --reward 1 \
  --reviewer synthetic-demo --note "Fixture: accepted alternate verification command" \
  --synthetic > "$demo/verified-label.json"
for step in 1 2 3 4; do
  reward=1
  if [ "$step" = 4 ]; then reward=0; fi
  hd label --task parser-fix --session review-candidate --step "$step" --reward "$reward" \
    --reviewer synthetic-demo --note "Fixture step judgment: premature final answer receives 0" \
    --synthetic > "$demo/candidate-step-$step.json"
done
for step in 1 2 3 4 5; do
  hd label --task parser-fix --session review-verified --step "$step" --reward 1 \
    --reviewer synthetic-demo --note "Fixture: useful observed step" \
    --synthetic > "$demo/verified-step-$step.json"
done
hd labels --task parser-fix > "$demo/labels.jsonl"
set +e
hd evaluate --task parser-fix --session review-candidate --output "$demo/failing" \
  --matching required --criteria "$demo/criteria.json" --allow-synthetic \
  --require-human-label --min-human-quality 0.9 --min-human-coverage 1 --require-outcome
status=$?
set -e
if [ "$status" != 2 ]; then
  echo "Expected regression exit 2, got $status" >&2
  exit 1
fi
hd evaluate --task parser-fix --session review-verified --output "$demo/passing" \
  --matching required --criteria "$demo/criteria.json" --allow-synthetic \
  --require-human-label --min-human-quality 0.9 --min-human-coverage 1 --require-outcome
hd query --archive "$demo/failing/archive" --kind human_label --format jsonl > "$demo/archived-labels.jsonl"
echo "Demo directory: $demo"
cat "$demo/failing/report.md"
cat "$demo/passing/report.md"
