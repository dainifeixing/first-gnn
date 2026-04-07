from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from pathlib import Path
from statistics import mean
from typing import Any

from .labels import LabelManifest, export_label_manifest
from .report_io import write_json_file, write_markdown_lines


@dataclass(frozen=True)
class RoundReport:
    round_name: str
    metrics: dict[str, Any]
    score_summary: dict[str, Any]
    frozen_eval_summary: dict[str, Any]
    review_summary: dict[str, Any]
    label_summary: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "round_name": self.round_name,
            "metrics": dict(self.metrics),
            "score_summary": dict(self.score_summary),
            "frozen_eval_summary": dict(self.frozen_eval_summary),
            "review_summary": dict(self.review_summary),
            "label_summary": dict(self.label_summary),
        }


@dataclass(frozen=True)
class RoundBootstrap:
    round_name: str
    output_dir: str
    files: dict[str, str]
    commands: list[str]

    def to_dict(self) -> dict[str, Any]:
        return {
            "round_name": self.round_name,
            "output_dir": self.output_dir,
            "files": dict(self.files),
            "commands": list(self.commands),
        }


def load_review_stats(review_csv_path: str | Path | None) -> dict[str, int]:
    stats = {
        "review_total": 0,
        "confirmed_positive": 0,
        "confirmed_negative": 0,
        "uncertain": 0,
    }
    if not review_csv_path:
        return stats
    path = Path(review_csv_path)
    if not path.exists():
        return stats
    with path.open(encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            stats["review_total"] += 1
            review_label = str(row.get("review_label", "")).strip().lower()
            if review_label == "confirmed_positive":
                stats["confirmed_positive"] += 1
            elif review_label == "confirmed_negative":
                stats["confirmed_negative"] += 1
            elif review_label == "uncertain":
                stats["uncertain"] += 1
    return stats


def _load_score_summary(score_json_path: str | Path | None) -> dict[str, Any]:
    summary = {
        "total_rows": 0,
        "top_rows": 0,
        "seller_candidates": 0,
        "top_score": 0.0,
        "avg_top_score": 0.0,
        "top_workbook": "",
        "top_seller_candidate": "",
    }
    if not score_json_path:
        return summary
    path = Path(score_json_path)
    if not path.exists():
        return summary
    payload = json.loads(path.read_text(encoding="utf-8"))
    top_rows = payload.get("top_rows", [])
    workbooks = payload.get("workbooks", [])
    seller_candidates = payload.get("seller_candidates", [])
    summary["total_rows"] = int(payload.get("total_rows", 0))
    summary["top_rows"] = len(top_rows)
    summary["seller_candidates"] = len(seller_candidates)
    if top_rows:
        scores = [float(item.get("score", 0.0)) for item in top_rows]
        summary["top_score"] = max(scores)
        summary["avg_top_score"] = mean(scores)
    if workbooks:
        summary["top_workbook"] = str(workbooks[0].get("path", ""))
    if seller_candidates:
        summary["top_seller_candidate"] = str(seller_candidates[0].get("seller_account", ""))
    return summary


def _load_frozen_eval_summary(frozen_eval_json_path: str | Path | None) -> dict[str, Any]:
    summary = {
        "total_rows": 0,
        "positive_rows": 0,
        "negative_rows": 0,
        "f1": 0.0,
        "seller_recovery_rate": 0.0,
        "recovered_positive_sellers": 0,
    }
    if not frozen_eval_json_path:
        return summary
    path = Path(frozen_eval_json_path)
    if not path.exists():
        return summary
    payload = json.loads(path.read_text(encoding="utf-8"))
    metrics = payload.get("metrics", {})
    recovery = payload.get("seller_candidate_recovery", {})
    summary["total_rows"] = int(payload.get("total_rows", 0))
    summary["positive_rows"] = int(payload.get("positive_rows", 0))
    summary["negative_rows"] = int(payload.get("negative_rows", 0))
    summary["f1"] = float(metrics.get("f1", 0.0))
    summary["seller_recovery_rate"] = float(recovery.get("recovery_rate", 0.0))
    summary["recovered_positive_sellers"] = int(recovery.get("recovered_positive_sellers", 0))
    return summary


def _load_label_summary(label_json_paths: list[str] | None) -> dict[str, Any]:
    summary = {
        "manifest_count": 0,
        "positive_manifest_count": 0,
        "negative_manifest_count": 0,
        "positive_transaction_count": 0,
        "negative_transaction_count": 0,
    }
    if not label_json_paths:
        return summary
    manifests = []
    for item in label_json_paths:
        path = Path(item)
        if path.exists():
            manifests.append(LabelManifest.from_dict(json.loads(path.read_text(encoding="utf-8"))))
    summary["manifest_count"] = len(manifests)
    positive = [manifest for manifest in manifests if manifest.polarity == "positive"]
    negative = [manifest for manifest in manifests if manifest.polarity == "negative"]
    summary["positive_manifest_count"] = len(positive)
    summary["negative_manifest_count"] = len(negative)
    summary["positive_transaction_count"] = sum(len(manifest.transaction_ids) for manifest in positive)
    summary["negative_transaction_count"] = sum(len(manifest.transaction_ids) for manifest in negative)
    return summary


def build_round_report(
    round_name: str,
    metrics_json_path: str | Path,
    score_json_path: str | Path | None = None,
    frozen_eval_json_path: str | Path | None = None,
    review_csv_path: str | Path | None = None,
    label_json_paths: list[str] | None = None,
) -> RoundReport:
    metrics = json.loads(Path(metrics_json_path).read_text(encoding="utf-8"))
    score_summary = _load_score_summary(score_json_path)
    frozen_eval_summary = _load_frozen_eval_summary(frozen_eval_json_path)
    review_summary = load_review_stats(review_csv_path)
    label_summary = _load_label_summary(label_json_paths)
    return RoundReport(
        round_name=round_name,
        metrics=metrics,
        score_summary=score_summary,
        frozen_eval_summary=frozen_eval_summary,
        review_summary=review_summary,
        label_summary=label_summary,
    )


def export_round_report_json(report: RoundReport, output_path: str | Path) -> Path:
    return write_json_file(output_path, report.to_dict())


def export_round_report_markdown(report: RoundReport, output_path: str | Path) -> Path:
    lines = [f"# Round Report: {report.round_name}", ""]
    lines.append("## Training")
    lines.append("")
    lines.append(f"- best_val_f1: {float(report.metrics.get('best_val_f1', 0.0)):.4f}")
    lines.append(f"- best_val_loss: {float(report.metrics.get('best_val_loss', 0.0)):.6f}")
    lines.append(f"- positive_rate: {float(report.metrics.get('positive_rate', 0.0)):.4f}")
    lines.append(f"- train_nodes: {int(report.metrics.get('train_nodes', 0))}")
    lines.append(f"- val_nodes: {int(report.metrics.get('val_nodes', 0))}")
    lines.append("")
    lines.append("## Scores")
    lines.append("")
    lines.append(f"- total_rows: {int(report.score_summary.get('total_rows', 0))}")
    lines.append(f"- top_rows: {int(report.score_summary.get('top_rows', 0))}")
    lines.append(f"- seller_candidates: {int(report.score_summary.get('seller_candidates', 0))}")
    lines.append(f"- top_score: {float(report.score_summary.get('top_score', 0.0)):.4f}")
    lines.append(f"- avg_top_score: {float(report.score_summary.get('avg_top_score', 0.0)):.4f}")
    lines.append(f"- top_workbook: {report.score_summary.get('top_workbook', '')}")
    lines.append(f"- top_seller_candidate: {report.score_summary.get('top_seller_candidate', '')}")
    lines.append("")
    lines.append("## Frozen Eval")
    lines.append("")
    lines.append(f"- total_rows: {int(report.frozen_eval_summary.get('total_rows', 0))}")
    lines.append(f"- positive_rows: {int(report.frozen_eval_summary.get('positive_rows', 0))}")
    lines.append(f"- negative_rows: {int(report.frozen_eval_summary.get('negative_rows', 0))}")
    lines.append(f"- f1: {float(report.frozen_eval_summary.get('f1', 0.0)):.4f}")
    lines.append(f"- seller_recovery_rate: {float(report.frozen_eval_summary.get('seller_recovery_rate', 0.0)):.4f}")
    lines.append(f"- recovered_positive_sellers: {int(report.frozen_eval_summary.get('recovered_positive_sellers', 0))}")
    lines.append("")
    lines.append("## Review")
    lines.append("")
    review_total = int(report.review_summary.get("review_total", 0))
    confirmed_positive = int(report.review_summary.get("confirmed_positive", 0))
    confirmed_negative = int(report.review_summary.get("confirmed_negative", 0))
    uncertain = int(report.review_summary.get("uncertain", 0))
    resolved = confirmed_positive + confirmed_negative
    resolution_rate = (resolved / review_total) if review_total else 0.0
    lines.append(f"- review_total: {review_total}")
    lines.append(f"- confirmed_positive: {confirmed_positive}")
    lines.append(f"- confirmed_negative: {confirmed_negative}")
    lines.append(f"- uncertain: {uncertain}")
    lines.append(f"- resolution_rate: {resolution_rate:.4f}")
    lines.append("")
    lines.append("## Labels")
    lines.append("")
    lines.append(f"- manifest_count: {int(report.label_summary.get('manifest_count', 0))}")
    lines.append(f"- positive_manifest_count: {int(report.label_summary.get('positive_manifest_count', 0))}")
    lines.append(f"- negative_manifest_count: {int(report.label_summary.get('negative_manifest_count', 0))}")
    lines.append(f"- positive_transaction_count: {int(report.label_summary.get('positive_transaction_count', 0))}")
    lines.append(f"- negative_transaction_count: {int(report.label_summary.get('negative_transaction_count', 0))}")
    return write_markdown_lines(output_path, lines)


def bootstrap_round(
    round_name: str,
    base_dir: str | Path = "out",
    train_root: str = "/path/to/train_workbooks",
    score_root: str = "/path/to/score_workbooks",
    label_glob: str = "data/labels/*.json",
) -> RoundBootstrap:
    output_dir = Path(base_dir) / round_name
    output_dir.mkdir(parents=True, exist_ok=True)
    files = {
        "seed_train_csv": str(output_dir / "seed_train.csv"),
        "holdout_eval_csv": str(output_dir / "holdout_eval.csv"),
        "feedback_pool_csv": str(output_dir / "feedback_pool.csv"),
        "train_annotations_csv": str(output_dir / "train_annotations.csv"),
        "normalized_csv": str(output_dir / "normalized.csv"),
        "normalized_jsonl": str(output_dir / "normalized.jsonl"),
        "graph_dataset_json": str(output_dir / "graph_dataset.json"),
        "model_pt": str(output_dir / "model.pt"),
        "metrics_json": str(output_dir / "metrics.json"),
        "metadata_json": str(output_dir / "metadata.json"),
        "scores_json": str(output_dir / "scores.json"),
        "scores_md": str(output_dir / "scores.md"),
        "seller_review_csv": str(output_dir / "seller_review.csv"),
        "seller_review_xlsx": str(output_dir / "seller_review.xlsx"),
        "seller_review_md": str(output_dir / "seller_review.md"),
        "frozen_eval_json": str(output_dir / "frozen_eval.json"),
        "frozen_eval_md": str(output_dir / "frozen_eval.md"),
        "report_json": str(output_dir / "report.json"),
        "report_md": str(output_dir / "report.md"),
    }
    (output_dir / "README.md").write_text(
        "\n".join(
            [
                f"# {round_name}",
                "",
                "Generated round workspace.",
                "",
                f"- train_root: {train_root}",
                f"- score_root: {score_root}",
                f"- label_glob: {label_glob}",
                "",
                "Use the commands below to run this round.",
                "",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    commands = [
        f"python3 -m txflow.cli split-feedback-loop --annotations YOUR_REVIEWED_ANNOTATIONS.csv --seed-train-csv {files['seed_train_csv']} --holdout-eval-csv {files['holdout_eval_csv']} --feedback-pool-csv {files['feedback_pool_csv']}",
        (
            f"python3 -m txflow.cli run-extension-round --round-name {round_name} "
            f"--train-root {train_root} --score-root {score_root} "
            f"--seed-annotations {files['seed_train_csv']} --holdout-annotations {files['holdout_eval_csv']} "
            f"--feedback-annotations {files['feedback_pool_csv']} --output-dir {output_dir}"
        ),
    ]
    return RoundBootstrap(
        round_name=round_name,
        output_dir=str(output_dir),
        files=files,
        commands=commands,
    )


def export_round_bootstrap_json(bootstrap: RoundBootstrap, output_path: str | Path) -> Path:
    return write_json_file(output_path, bootstrap.to_dict())


def export_round_bootstrap_markdown(bootstrap: RoundBootstrap, output_path: str | Path) -> Path:
    lines = [f"# Bootstrap: {bootstrap.round_name}", ""]
    lines.append(f"- output_dir: {bootstrap.output_dir}")
    lines.append("")
    lines.append("## Files")
    lines.append("")
    for key, value in bootstrap.files.items():
        lines.append(f"- {key}: {value}")
    lines.append("")
    lines.append("## Commands")
    lines.append("")
    for command in bootstrap.commands:
        lines.append(f"- `{command}`")
    return write_markdown_lines(output_path, lines)
