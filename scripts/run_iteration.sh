#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 4 ]]; then
  cat <<'EOF'
Usage:
  scripts/run_iteration.sh <round_name> <workbook_root> <score_root> <label_glob> [threshold]

Example:
  scripts/run_iteration.sh round_01 /path/to/train_workbooks /path/to/score_workbooks "data/labels/*.json" 0.75
EOF
  exit 1
fi

ROUND_NAME="$1"
WORKBOOK_ROOT="$2"
SCORE_ROOT="$3"
LABEL_GLOB="$4"
THRESHOLD="${5:-0.75}"

OUT_DIR="out/${ROUND_NAME}"
mkdir -p "${OUT_DIR}"

# shellcheck disable=SC2086
python3 -m txflow.cli normalize-ledgers \
  --root "${WORKBOOK_ROOT}" \
  --labels ${LABEL_GLOB} \
  --csv "${OUT_DIR}/normalized.csv" \
  --jsonl "${OUT_DIR}/normalized.jsonl"

# shellcheck disable=SC2086
python3 -m txflow.cli build-graph-dataset \
  --root "${WORKBOOK_ROOT}" \
  --labels ${LABEL_GLOB} \
  --json "${OUT_DIR}/graph_dataset.json"

# shellcheck disable=SC2086
python3 -m txflow.cli train-gnn \
  --root "${WORKBOOK_ROOT}" \
  --labels ${LABEL_GLOB} \
  --model "${OUT_DIR}/model.pt" \
  --metrics "${OUT_DIR}/metrics.json" \
  --metadata "${OUT_DIR}/metadata.json"

# shellcheck disable=SC2086
python3 -m txflow.cli score-gnn \
  --root "${SCORE_ROOT}" \
  --model "${OUT_DIR}/model.pt" \
  --labels ${LABEL_GLOB} \
  --json "${OUT_DIR}/scores.json" \
  --md "${OUT_DIR}/scores.md" \
  --top-k 200

python3 -m txflow.cli export-review-candidates \
  --scores "${OUT_DIR}/scores.json" \
  --csv "${OUT_DIR}/review.csv" \
  --md "${OUT_DIR}/review.md" \
  --threshold "${THRESHOLD}" \
  --limit 100

echo "completed iteration ${ROUND_NAME}"
echo "next step: review ${OUT_DIR}/review.csv and then run import-review-labels to create the next round manifests"
