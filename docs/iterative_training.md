# Iterative GNN Training

This project is designed for anomaly transaction pattern discovery with human
review in the loop.

## Goal

Each round should produce:
- a normalized ledger export
- one saved GNN model
- one score report
- one review candidate list
- one updated label set for the next round

The model should be used to rank risky transactions and accounts for review. It
should not be used to make final determinations about people.

## Label Policy

Use generic risk labels for new work:
- `high_risk_transaction`
- `low_risk_transaction`
- `needs_review`
- `high_risk_account`

Rules:
- only human-confirmed results become positive or negative training labels
- `needs_review` is not a training label
- keep positive and negative samples balanced when possible
- preserve the original transaction IDs exactly

## One Round

### 1. Normalize

```bash
python3 -m txflow.cli normalize-ledgers \
  --root /path/to/workbooks \
  --labels data/labels/*.json \
  --csv out/round_01_normalized.csv \
  --jsonl out/round_01_normalized.jsonl
```

Check:
- missing transaction IDs
- missing timestamps
- missing counterparties
- duplicate transaction IDs
- heavy night activity clusters

### 2. Summarize Graph Input

```bash
python3 -m txflow.cli build-graph-dataset \
  --root /path/to/workbooks \
  --labels data/labels/*.json \
  --json out/round_01_graph_dataset.json
```

Check:
- total rows
- labeled rows
- unlabeled rows
- flagged rows

### 3. Train

```bash
python3 -m txflow.cli train-gnn \
  --root /path/to/workbooks \
  --labels data/labels/*.json \
  --model out/round_01_model.pt \
  --metrics out/round_01_metrics.json \
  --metadata out/round_01_metadata.json \
  --epochs 120 \
  --split-ratio 0.8 \
  --seed 42
```

Check:
- `best_val_f1`
- `positive_rate`
- `train_nodes`
- `val_nodes`

Do not overwrite the previous round's model or metrics.

### 4. Score

```bash
python3 -m txflow.cli score-gnn \
  --root /path/to/unlabeled_workbooks \
  --model out/round_01_model.pt \
  --labels data/labels/*.json \
  --json out/round_01_scores.json \
  --md out/round_01_scores.md \
  --top-k 200
```

Review:
- top scored rows
- workbooks with the highest max score
- flagged rows that also have high scores

### 5. Export Review Queue

```bash
python3 -m txflow.cli export-review-candidates \
  --scores out/round_01_scores.json \
  --csv out/round_01_review.csv \
  --md out/round_01_review.md \
  --threshold 0.75 \
  --limit 100
```

Recommended review states:
- `confirmed_positive`
- `confirmed_negative`
- `uncertain`

Only the first two should be converted into the next round of training labels.

### 6. Import Reviewed Labels

After you edit the review CSV and replace `needs_review` with
`confirmed_positive`, `confirmed_negative`, or `uncertain`, convert the
confirmed rows into the next round's training manifests:

```bash
python3 -m txflow.cli import-review-labels \
  --reviews out/round_01_review.csv \
  --dataset-prefix round_01_reviewed \
  --positive-json data/labels/round_01_reviewed_positive.json \
  --negative-json data/labels/round_01_reviewed_negative.json \
  --subject reviewed_batch \
  --verified-by analyst
```

This produces:
- one positive manifest with `high_risk_transaction`
- one negative manifest with `low_risk_transaction`

Then include those manifests in the next round's `--labels` input.

### 7. Merge Labels For The Next Round

When you have many review-generated manifests, merge them into one positive and
one negative manifest before the next training run:

```bash
python3 -m txflow.cli merge-label-manifests \
  --labels data/labels/*.json \
  --dataset-prefix round_02_merged \
  --positive-json data/labels/round_02_merged_positive.json \
  --negative-json data/labels/round_02_merged_negative.json \
  --subject merged_batch \
  --verified-by analyst
```

This step deduplicates transaction IDs within each polarity and gives you a
stable pair of label files for the next round.

### 8. Compare Rounds

Compare multiple rounds of training and review outcomes:

```bash
python3 -m txflow.cli compare-round-metrics \
  --round round_01:out/round_01/metrics.json:out/round_01/review.csv \
  --round round_02:out/round_02/metrics.json:out/round_02/review.csv \
  --md out/round_comparison.md \
  --json out/round_comparison.json
```

Use this report to check:
- whether `best_val_f1` is improving
- whether manual review resolution rate is improving
- whether the confirmed positive rate is drifting too high or too low

### 9. Build A Single-Round Report

Generate a compact report for one round:

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

Use this report to review one round end-to-end before starting the next one.

### 10. Bootstrap A New Round

Create a standard workspace before starting a round:

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

This creates:
- `out/round_03/`
- a round `README.md`
- a suggested file layout
- ready-to-run command templates for the round

### 11. Sweep Score Thresholds

Use one round's score output to compare threshold choices:

```bash
python3 -m txflow.cli score-threshold-sweep \
  --scores out/round_03/scores.json \
  --reviews out/round_03/review.csv \
  --threshold 0.60 \
  --threshold 0.70 \
  --threshold 0.80 \
  --threshold 0.90 \
  --md out/round_03/threshold_sweep.md \
  --json out/round_03/threshold_sweep.json
```

Use this report to balance:
- candidate volume
- review workload
- confirmed positive yield

### 12. Forecast Review Workload

Estimate reviewer workload from one round's score output:

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

Use this report to estimate:
- how many reviewer-days are needed
- how many confirmed positives you may get
- whether a threshold is realistic for the available team size

### 13. Select An Operating Threshold

Recommend a working threshold under current workload constraints:

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
  --min-candidates 2 \
  --md out/round_03/operating_threshold.md \
  --json out/round_03/operating_threshold.json
```

Use this report to pick a practical threshold instead of comparing all tables
manually.

### 14. Build A Round Decision Sheet

Build a one-page summary for discussion and sign-off:

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

Use this summary to decide:
- whether the round is ready for review
- which threshold to run operationally
- what the next iteration should focus on

## Iteration Rules

- Start with `--self-train-rounds 0` unless you already have a strong reviewed
  dataset.
- Increase the review threshold before increasing top-k volume.
- Prefer adding diverse reviewed examples over repeatedly labeling the same
  pattern.
- Keep a per-round archive:
  - normalized export
  - metrics
  - score report
  - review candidate file
  - label manifests used for that round

## Recommended Cadence

Round 1:
- use a conservative threshold
- review a small top-k set

Round 2-3:
- add confirmed positives and negatives
- retrain from scratch
- compare against the previous round

Only enable pseudo-label self-training after human-reviewed precision is stable.
