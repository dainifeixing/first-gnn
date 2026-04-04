# txflow-risk

`txflow-risk` is a standalone transaction network risk analysis toolkit.

The current project position is:
- anomaly transaction pattern discovery
- account and relationship triage
- human-review-first outputs
- no automated determination of sensitive personal attributes or illegal conduct

It starts with a practical baseline:
- ingest transaction exports from CSV
- normalize accounts, amounts, timestamps, and directions
- build a transaction network
- score suspicious patterns with transparent rules
- export a JSON or Markdown report

The project is intentionally human-review-first:
- outputs are risk candidates, not final determinations
- the baseline uses explainable rules before any ML/GNN layer
- future GNN work can be added after the data and review loop are stable

## Current Scope

This initial version focuses on:
- account-level activity profiling
- counterparty concentration
- night activity ratio
- burst and pass-through style signals
- explainable findings for review

## Quick Start

Chinese documentation:
- `docs/project_manual_zh.md`
- `docs/iterative_training.md`
- `docs/user_guide_zh.md`
- `docs/developer_guide_zh.md`

Development checks:

```bash
make check
```

Or without `make`:

```bash
bash scripts/check.sh
```

```bash
python3 -m txflow.cli analyze data/sample.csv --format markdown
```

The repository includes `data/sample.csv`, a minimal night-activity example for
verifying the CSV analysis flow.

Or after installation:

```bash
txflow analyze data/sample.csv --format markdown
```

To export verified training samples from an xlsx ledger and one or more label
manifests:

```bash
python3 -m txflow.cli export-training \
  --xlsx /path/to/ledger.xlsx \
  --labels data/labels/wechat_yang_qianqian_verified.json \
  --csv out/samples.csv \
  --jsonl out/samples.jsonl
```

To export a full feature table for all rows, with positive labels attached when
they match:

```bash
python3 -m txflow.cli export-dataset \
  --xlsx /path/to/ledger.xlsx \
  --labels data/labels/wechat_yang_qianqian_verified.json \
  --csv out/dataset.csv \
  --jsonl out/dataset.jsonl
```

To build a manifest catalog for review:

```bash
python3 -m txflow.cli label-catalog \
  --labels data/labels/*.json \
  --json out/label_catalog.json \
  --md out/label_catalog.md
```

To split a labeled dataset into train and validation sets:

```bash
python3 -m txflow.cli split-dataset \
  --xlsx /path/to/ledger.xlsx \
  --labels data/labels/*.json \
  --train-csv out/train.csv \
  --validation-csv out/validation.csv \
  --ratio 0.8 \
  --seed 42
```

To train the lightweight baseline classifier:

```bash
python3 -m txflow.cli train-baseline \
  --xlsx /path/to/ledger.xlsx \
  --labels data/labels/*.json \
  --model out/baseline_model.json \
  --metrics out/baseline_metrics.json \
  --split-ratio 0.8 \
  --seed 42
```

To triage a directory of workbook files:

```bash
python3 -m txflow.cli triage-workbooks \
  --root /path/to/funds \
  --labels data/labels/*.json \
  --json out/triage.json \
  --md out/triage.md
```

To run graph-propagation risk scoring on a workbook directory:

```bash
python3 -m txflow.cli graph-triage \
  --root /path/to/funds \
  --labels data/labels/*.json \
  --json out/graph_triage.json \
  --md out/graph_triage.md \
  --top-k 100
```

To normalize workbook ledgers into an internal full-field table:

```bash
python3 -m txflow.cli normalize-ledgers \
  --root /path/to/funds \
  --labels data/labels/*.json \
  --roles out/role_annotations.csv \
  --owners out/owner_annotations.csv \
  --csv out/normalized.csv \
  --jsonl out/normalized.jsonl
```

To export a minimal manual-review ledger table from the normalized output:

```bash
python3 -m txflow.cli export-ledger-review \
  --normalized out/normalized.csv \
  --csv out/ledger_review.csv \
  --xlsx out/ledger_review.xlsx
```

To audit rule hits separately from the human-review table:

```bash
python3 -m txflow.cli export-rule-audit \
  --normalized out/normalized.csv \
  --csv out/rule_audit.csv \
  --xlsx out/rule_audit.xlsx
```

To summarize rule-hit distribution by channel, trade pattern, and reason:

```bash
python3 -m txflow.cli build-rule-summary \
  --normalized out/normalized.csv \
  --json out/rule_summary.json \
  --md out/rule_summary.md
```

To compare rule hits with manual review outcomes:

```bash
python3 -m txflow.cli build-rule-review-summary \
  --normalized out/normalized.csv \
  --reviews out/ledger_review.csv \
  --json out/rule_review_summary.json \
  --md out/rule_review_summary.md
```

The split is intentional:
- `normalize-ledgers` is for internal analysis and model features, with full derived fields
- `export-ledger-review` is for human review, with only the required columns
- `export-rule-audit` is for checking which preset rules actually fired, without mixing that view into manual transaction review
- `build-rule-summary` is for checking whether rules are too wide or too narrow by channel, trade pattern, and rule reason
- `build-rule-review-summary` is for checking which rules actually convert into confirmed positives after human review

The normalized internal table now includes unified rule-derived fields such as:
- `flow_family`
- `trade_pattern`
- `is_qr_transfer`
- `is_trade_like`
- `buyer_account`
- `seller_account`
- `seller_proxy_name`
- `rule_reason`

These fields are used as model features, not as labels.

Model responsibilities are split on purpose:
- rule layer: stable heuristics such as `trade_pattern`, QR hints, red-packet hints, platform-settlement hints
- GNN layer: learn combinations, graph propagation, and exceptions that simple rules cannot cover
- human label layer: verified positives/negatives, owner grouping, role review, and other confirmed supervision

The minimal review export also carries `rule_reason`, so reviewers can see why a row was classified into a given trade pattern.

To summarize the graph-ready dataset before training:

```bash
python3 -m txflow.cli build-graph-dataset \
  --root /path/to/funds \
  --labels data/labels/*.json \
  --roles out/role_annotations.csv \
  --owners out/owner_annotations.csv \
  --json out/graph_dataset.json
```

To summarize owner-level activity after account grouping:

```bash
python3 -m txflow.cli build-owner-summary \
  --root /path/to/funds \
  --labels data/labels/*.json \
  --roles out/role_annotations.csv \
  --owners out/owner_annotations.csv \
  --csv out/owner_summary.csv \
  --json out/owner_summary.json
```

To export an owner review template and convert the reviewed result back into role annotations:

```bash
python3 -m txflow.cli export-owner-review \
  --summary out/owner_summary.json \
  --csv out/owner_review.csv \
  --xlsx out/owner_review.xlsx

python3 -m txflow.cli import-owner-review \
  --reviews out/owner_review.csv \
  --roles-csv out/role_annotations.csv \
  --scene vice
```

To train the graph model and save its weights:

```bash
python3 -m txflow.cli train-gnn \
  --root /path/to/funds \
  --labels data/labels/*.json \
  --roles out/role_annotations.csv \
  --owners out/owner_annotations.csv \
  --model out/gnn_model.pt \
  --metrics out/gnn_metrics.json \
  --metadata out/gnn_metadata.json
```

To score workbook rows with a saved graph model:

```bash
python3 -m txflow.cli score-gnn \
  --root /path/to/funds \
  --model out/gnn_model.pt \
  --labels data/labels/*.json \
  --roles out/role_annotations.csv \
  --owners out/owner_annotations.csv \
  --json out/gnn_scores.json \
  --md out/gnn_scores.md \
  --top-k 100
```

To export high-score rows for manual review:

```bash
python3 -m txflow.cli export-review-candidates \
  --scores out/gnn_scores.json \
  --csv out/review_candidates.csv \
  --md out/review_candidates.md \
  --threshold 0.7
```

## Input Format

The CSV loader accepts common header aliases such as:
- `时间`, `交易时间`, `发生时间`
- `金额`, `交易金额`
- `付款方`, `收款方`, `交易对方`
- `方向`, `收/支`, `交易类型`
- `备注`, `摘要`, `附言`

If a field is missing, the parser keeps the record but marks unknown values explicitly.

## Outputs

The analyzer returns:
- `summary`
- `accounts`
- `edges`
- `findings`
- `network`

## Training Labels

Labeled examples live under `data/labels/`.

Current convention:
- one JSON manifest per verified label set
- one Markdown mirror for human review
- transaction IDs are preserved exactly as provided
- manifests may declare `polarity: positive` or `polarity: negative`
- feature exports keep matched rows labeled and preserve unlabeled rows
- the catalog summarizes all verified manifests before model training
- the manifest can be loaded later for supervised training or evaluation
- the baseline classifier is intentionally lightweight and fully serializable
- directory triage ranks files and rows for human review before labeling
- graph triage propagates risk over workbook-row feature graphs for review

Recommended label semantics for new work:
- `high_risk_transaction`
- `low_risk_transaction`
- `needs_review`
- `high_risk_account`

Some existing sample manifests still use older, case-specific label names. The
training and export pipeline remains backward compatible with those manifests,
but new labels should use the generic risk-oriented naming above.

## Role Annotations

If the same counterparties repeatedly play stable roles such as `buyer`,
`seller`, or `broker`, you can add a sidecar role file and let the baseline/GNN
consume it as extra context without changing the original ledger columns.

Recommended format:

```csv
target_type,target_id,scene,role_label,confidence,evidence,note
counterparty,夜间私单,vice,seller,high,manual_review,长期夜间多收款
counterparty,中转账户A,vice,broker,high,manual_review,先收后分账
transaction,420000...,vice,buyer,medium,pattern,单向付款到多个卖家
```

Rules:
- `target_type` supports `transaction` or `counterparty`
- `role_label` supports `buyer`, `seller`, `broker`, `mixed`, `unknown`
- `confidence` supports `high`, `medium`, `low`
- transaction-level matches override counterparty-level matches

## Owner Annotations

If one seller, broker, or buyer uses multiple WeChat, Alipay, or bank accounts,
you can group those accounts into one stable `owner_id` without changing the
original ledgers.

Recommended format:

```csv
target_type,target_id,owner_id,owner_name,confidence,evidence,note
counterparty,夜间私单,owner_001,张三,high,manual_review,同一卖家的微信收款号
counterparty,支付宝账号A,owner_001,张三,high,manual_review,同一卖家的支付宝收款号
transaction,420000...,owner_002,李四,medium,case_note,该笔明确属于中间商代收
```

Rules:
- `target_type` supports `transaction` or `counterparty`
- use the same `owner_id` for every account you know belongs to the same person
- transaction-level owner matches override counterparty-level matches
- owner annotations feed normalized outputs, graph summaries, and model features

After owner grouping, you can export one owner-level summary table to review
patterns such as one-to-many collecting, many-to-one receiving, and collect-then-split behavior:

```bash
python3 -m txflow.cli build-owner-summary \
  --root /path/to/funds \
  --roles out/role_annotations.csv \
  --owners out/owner_annotations.csv \
  --csv out/owner_summary.csv
```

## Iterative Training

The intended workflow is:
- normalize new ledger workbooks
- train on human-reviewed labels
- score unlabeled workbooks
- export review candidates
- feed confirmed results back into the next round of labels

See `docs/iterative_training.md` for a round-by-round operating guide and
`scripts/run_iteration.sh` for a one-round batch command wrapper.

Shortest annotation loop:

1. Score a workbook directory with the current GNN model.
2. Export review candidates as a fill-in CSV template.
3. Fill `review_label` with one of `confirmed_positive`, `confirmed_negative`, or `uncertain`.
4. Convert the reviewed CSV directly into `annotations.csv` and feed it back into `train-gnn`.

Example:

```bash
python3 -m txflow.cli score-gnn \
  --root /path/to/funds \
  --model out/gnn_model.pt \
  --json out/gnn_scores.json

python3 -m txflow.cli export-review-candidates \
  --scores out/gnn_scores.json \
  --csv out/review_candidates.csv \
  --threshold 0.7

python3 -m txflow.cli import-review-labels \
  --reviews out/review_candidates.csv \
  --dataset-prefix round_01_reviewed \
  --annotations-csv out/annotations.csv

python3 -m txflow.cli train-gnn \
  --root /path/to/funds \
  --annotations out/annotations.csv \
  --model out/gnn_model_round_02.pt \
  --metrics out/gnn_metrics_round_02.json
```

Or run the same loop with the helper script:

```bash
bash scripts/run_annotation_loop.sh \
  round_02 \
  /path/to/train_workbooks \
  /path/to/score_workbooks \
  out/annotations.csv \
  0.7
```

Notes:
- `export-review-candidates` now leaves `review_label` blank by default and adds a `review_options` hint column.
- `train-baseline`, `train-gnn`, and `score-gnn` accept either `--labels` or the lighter `--annotations`.

To convert manual review results into the next round of training labels:

```bash
python3 -m txflow.cli import-review-labels \
  --reviews out/review_candidates.csv \
  --dataset-prefix round_01_reviewed \
  --positive-json data/labels/round_01_reviewed_positive.json \
  --negative-json data/labels/round_01_reviewed_negative.json
```

If you still need manifest-based labels for archiving or compatibility, you can
emit both outputs in one step:

```bash
python3 -m txflow.cli import-review-labels \
  --reviews out/review_candidates.csv \
  --dataset-prefix round_01_reviewed \
  --positive-json data/labels/round_01_reviewed_positive.json \
  --negative-json data/labels/round_01_reviewed_negative.json \
  --annotations-csv out/annotations.csv
```

To merge multiple manifests into one positive and one negative training set:

```bash
python3 -m txflow.cli merge-label-manifests \
  --labels data/labels/*.json \
  --dataset-prefix merged_round \
  --positive-json data/labels/merged_round_positive.json \
  --negative-json data/labels/merged_round_negative.json
```

To compare multiple rounds of training and manual review:

```bash
python3 -m txflow.cli compare-round-metrics \
  --round round_01:out/round_01/metrics.json:out/round_01/review.csv \
  --round round_02:out/round_02/metrics.json:out/round_02/review.csv \
  --md out/round_comparison.md \
  --json out/round_comparison.json
```

To build a single-round report:

```bash
python3 -m txflow.cli make-round-report \
  --round-name round_02 \
  --metrics out/round_02/metrics.json \
  --scores out/round_02/scores.json \
  --reviews out/round_02/review.csv \
  --labels data/labels/round_02_reviewed_positive.json data/labels/round_02_reviewed_negative.json \
  --md out/round_02/report.md \
  --json out/round_02/report.json
```

To create a standard workspace for a new round:

```bash
python3 -m txflow.cli bootstrap-round \
  --round-name round_03 \
  --base-dir out \
  --train-root /path/to/train_workbooks \
  --score-root /path/to/score_workbooks \
  --label-glob "data/labels/*.json" \
  --md out/round_03/bootstrap.md \
  --json out/round_03/bootstrap.json
```

To compare different score thresholds for one round:

```bash
python3 -m txflow.cli score-threshold-sweep \
  --scores out/round_03/scores.json \
  --reviews out/round_03/review.csv \
  --threshold 0.60 \
  --threshold 0.70 \
  --threshold 0.80 \
  --md out/round_03/threshold_sweep.md \
  --json out/round_03/threshold_sweep.json
```

To estimate review workload for one round:

```bash
python3 -m txflow.cli review-workload-forecast \
  --scores out/round_03/scores.json \
  --reviews out/round_03/review.csv \
  --threshold 0.60 \
  --threshold 0.70 \
  --threshold 0.80 \
  --reviewers 2 \
  --daily-capacity 40 \
  --md out/round_03/workload_forecast.md \
  --json out/round_03/workload_forecast.json
```

To recommend an operating threshold under workload constraints:

```bash
python3 -m txflow.cli select-operating-threshold \
  --scores out/round_03/scores.json \
  --reviews out/round_03/review.csv \
  --threshold 0.60 \
  --threshold 0.70 \
  --threshold 0.80 \
  --reviewers 2 \
  --daily-capacity 40 \
  --max-team-days 1.0 \
  --min-confirmed-positive-rate 0.40 \
  --md out/round_03/operating_threshold.md \
  --json out/round_03/operating_threshold.json
```

To build a one-page decision summary for a round:

```bash
python3 -m txflow.cli round-decision-sheet \
  --round-name round_03 \
  --metrics out/round_03/metrics.json \
  --scores out/round_03/scores.json \
  --reviews out/round_03/review.csv \
  --labels data/labels/round_03_reviewed_positive.json data/labels/round_03_reviewed_negative.json \
  --threshold 0.60 \
  --threshold 0.70 \
  --threshold 0.80 \
  --reviewers 2 \
  --daily-capacity 40 \
  --max-team-days 1.0 \
  --min-confirmed-positive-rate 0.40 \
  --md out/round_03/decision_sheet.md \
  --json out/round_03/decision_sheet.json
```

The current project already includes one verified positive label set for
the provided WeChat ledger sample.

## Development Notes

The rule-analysis flow stays lightweight, while the graph-training and graph-scoring
commands require `torch` as declared in `pyproject.toml`.
That means CSV analysis and report generation are straightforward, but the GNN demo
and training workflows still need a heavier ML dependency on a new environment.

Planned next steps:
- add richer CSV adapters
- add unit tests for more export styles
- introduce optional graph libraries
- add a model-training layer after the baseline rules are stable
