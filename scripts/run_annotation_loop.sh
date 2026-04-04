#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 4 ]]; then
  cat <<'EOF'
Usage:
  scripts/run_annotation_loop.sh <round_name> <train_root> <score_root> <annotations_file> [threshold]

Example:
  scripts/run_annotation_loop.sh round_02 /path/to/train_workbooks /path/to/score_workbooks out/annotations.csv 0.75
EOF
  exit 1
fi

ROUND_NAME="$1"
TRAIN_ROOT="$2"
SCORE_ROOT="$3"
ANNOTATIONS_FILE="$4"
THRESHOLD="${5:-0.75}"

OUT_DIR="out/${ROUND_NAME}"
mkdir -p "${OUT_DIR}"

python3 -m txflow.cli train-gnn \
  --root "${TRAIN_ROOT}" \
  --annotations "${ANNOTATIONS_FILE}" \
  --model "${OUT_DIR}/model.pt" \
  --metrics "${OUT_DIR}/metrics.json" \
  --metadata "${OUT_DIR}/metadata.json"

python3 -m txflow.cli score-gnn \
  --root "${SCORE_ROOT}" \
  --model "${OUT_DIR}/model.pt" \
  --annotations "${ANNOTATIONS_FILE}" \
  --json "${OUT_DIR}/scores.json" \
  --md "${OUT_DIR}/scores.md" \
  --top-k 200

python3 -m txflow.cli export-review-candidates \
  --scores "${OUT_DIR}/scores.json" \
  --csv "${OUT_DIR}/review.csv" \
  --md "${OUT_DIR}/review.md" \
  --threshold "${THRESHOLD}" \
  --limit 100

echo "completed annotation loop ${ROUND_NAME}"
echo "next step: fill ${OUT_DIR}/review.csv and run import-review-labels --annotations-csv for the next round"
