#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

echo "[check] py_compile"
python3 -m py_compile \
  txflow/__init__.py \
  src/txflow/__init__.py \
  src/txflow/cli.py \
  src/txflow/gnn_pipeline.py \
  src/txflow/graph_risk.py \
  src/txflow/ledger_ops.py \
  src/txflow/report_io.py \
  src/txflow/round_ops.py \
  src/txflow/thresholds.py \
  tests/test_modules.py \
  tests/test_package_api.py

echo "[check] unittest"
python3 -m unittest discover -s tests -q
