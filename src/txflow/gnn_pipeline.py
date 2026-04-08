from __future__ import annotations

import csv
import json
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from statistics import mean
from typing import Any

import torch

from .annotations import AnnotationRow, build_annotation_manifests_from_rows
from .excel import write_xlsx_table
from .graph_risk import GraphRiskModel, _metrics_from_predictions
from .ledger_ops import (
    GraphDatasetSummary,
    NormalizedTransaction,
    OwnerSummaryReport,
    build_duplicate_transaction_ids,
    build_owner_review_roles,
    build_owner_review_rows,
    build_review_flags,
    export_graph_dataset_summary,
    export_normalized_ledgers,
    export_owner_review_csv,
    export_owner_review_xlsx,
    export_owner_summary_csv,
    export_owner_summary_json,
    summarize_graph_dataset,
    summarize_owner_activity,
)
from .labels import LabelManifest
from .round_ops import (
    RoundBootstrap,
    RoundReport,
    bootstrap_round,
    build_round_report,
    export_round_bootstrap_json,
    export_round_bootstrap_markdown,
    export_round_report_json,
    export_round_report_markdown,
    load_review_stats,
)
from .report_io import write_json_file, write_markdown_lines
from .thresholds import (
    OperatingThresholdRecommendation,
    ReviewWorkloadForecast,
    ThresholdSweepReport,
    export_operating_threshold_json,
    export_operating_threshold_markdown,
    export_review_workload_json,
    export_review_workload_markdown,
    export_threshold_sweep_json,
    export_threshold_sweep_markdown,
    review_workload_forecast,
    score_threshold_sweep,
    select_operating_threshold,
)
from .training import TrainingExample, build_training_examples


@dataclass(frozen=True)
class GNNScoreRow:
    workbook_path: str
    row_index: int
    transaction_id: str
    label_status: str
    score: float
    amount: str
    timestamp: str
    counterparty: str
    direction: str
    channel: str
    remark: str
    subject_name: str
    subject_account: str
    counterparty_account: str
    counterparty_name: str
    payer_account: str
    payer_name: str
    payer_role: str
    payee_account: str
    payee_name: str
    payee_role: str
    buyer_account: str
    seller_account: str
    role_label: str
    extension_role: str
    review_flags: list[str]

    def to_dict(self) -> dict[str, Any]:
        return {
            "workbook_path": self.workbook_path,
            "row_index": self.row_index,
            "transaction_id": self.transaction_id,
            "label_status": self.label_status,
            "score": round(self.score, 4),
            "amount": self.amount,
            "timestamp": self.timestamp,
            "counterparty": self.counterparty,
            "direction": self.direction,
            "channel": self.channel,
            "remark": self.remark,
            "subject_name": self.subject_name,
            "subject_account": self.subject_account,
            "counterparty_account": self.counterparty_account,
            "counterparty_name": self.counterparty_name,
            "payer_account": self.payer_account,
            "payer_name": self.payer_name,
            "payer_role": self.payer_role,
            "payee_account": self.payee_account,
            "payee_name": self.payee_name,
            "payee_role": self.payee_role,
            "buyer_account": self.buyer_account,
            "seller_account": self.seller_account,
            "role_label": self.role_label,
            "extension_role": self.extension_role,
            "review_flags": list(self.review_flags),
        }


@dataclass(frozen=True)
class SellerCandidateRow:
    seller_account: str
    score: float
    avg_row_score: float
    bridge_uplift: float
    support_rows: int
    unique_buyers: int
    bridge_buyers: int
    bridge_support_ratio: float
    known_buyer_support: int
    candidate_tier: str
    unique_workbooks: int
    sample_counterparties: list[str]
    sample_workbooks: list[str]
    support_examples: list[dict[str, Any]]

    def to_dict(self) -> dict[str, Any]:
        return {
            "seller_account": self.seller_account,
            "score": round(self.score, 4),
            "avg_row_score": round(self.avg_row_score, 4),
            "bridge_uplift": round(self.bridge_uplift, 4),
            "support_rows": self.support_rows,
            "unique_buyers": self.unique_buyers,
            "bridge_buyers": self.bridge_buyers,
            "bridge_support_ratio": round(self.bridge_support_ratio, 4),
            "known_buyer_support": self.known_buyer_support,
            "candidate_tier": self.candidate_tier,
            "unique_workbooks": self.unique_workbooks,
            "sample_counterparties": list(self.sample_counterparties),
            "sample_workbooks": list(self.sample_workbooks),
            "support_examples": [dict(item) for item in self.support_examples],
        }


@dataclass(frozen=True)
class GNNScoreReport:
    total_workbooks: int
    total_rows: int
    labeled_rows: int
    positive_rows: int
    negative_rows: int
    unlabeled_rows: int
    model: dict[str, Any]
    summary: dict[str, Any]
    recommendations: list[str]
    top_rows: list[GNNScoreRow]
    seller_candidates: list[SellerCandidateRow]
    workbooks: list[dict[str, Any]]

    def to_dict(self) -> dict[str, Any]:
        return {
            "total_workbooks": self.total_workbooks,
            "total_rows": self.total_rows,
            "labeled_rows": self.labeled_rows,
            "positive_rows": self.positive_rows,
            "negative_rows": self.negative_rows,
            "unlabeled_rows": self.unlabeled_rows,
            "model": dict(self.model),
            "summary": dict(self.summary),
            "recommendations": list(self.recommendations),
            "top_rows": [item.to_dict() for item in self.top_rows],
            "seller_candidates": [item.to_dict() for item in self.seller_candidates],
            "workbooks": list(self.workbooks),
        }


@dataclass(frozen=True)
class FrozenEvalReport:
    total_rows: int
    positive_rows: int
    negative_rows: int
    metrics: dict[str, Any]
    extension_role_summary: list[dict[str, Any]]
    seller_candidate_recovery: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "total_rows": self.total_rows,
            "positive_rows": self.positive_rows,
            "negative_rows": self.negative_rows,
            "metrics": dict(self.metrics),
            "extension_role_summary": [dict(item) for item in self.extension_role_summary],
            "seller_candidate_recovery": dict(self.seller_candidate_recovery),
        }


@dataclass(frozen=True)
class RoundComparisonRow:
    round_name: str
    best_val_f1: float
    best_val_loss: float
    positive_rate: float
    train_nodes: int
    val_nodes: int
    review_total: int
    confirmed_positive: int
    confirmed_negative: int
    uncertain: int
    review_resolution_rate: float
    review_positive_rate: float

    def to_dict(self) -> dict[str, Any]:
        return {
            "round_name": self.round_name,
            "best_val_f1": round(self.best_val_f1, 4),
            "best_val_loss": round(self.best_val_loss, 6),
            "positive_rate": round(self.positive_rate, 4),
            "train_nodes": self.train_nodes,
            "val_nodes": self.val_nodes,
            "review_total": self.review_total,
            "confirmed_positive": self.confirmed_positive,
            "confirmed_negative": self.confirmed_negative,
            "uncertain": self.uncertain,
            "review_resolution_rate": round(self.review_resolution_rate, 4),
            "review_positive_rate": round(self.review_positive_rate, 4),
        }


@dataclass(frozen=True)
class RoundComparisonReport:
    rounds: list[RoundComparisonRow]

    def to_dict(self) -> dict[str, Any]:
        return {"rounds": [item.to_dict() for item in self.rounds]}


@dataclass(frozen=True)
class RoundDecisionSheet:
    round_name: str
    round_report: dict[str, Any]
    threshold_recommendation: dict[str, Any]
    workload_summary: dict[str, Any]
    next_actions: list[str]

    def to_dict(self) -> dict[str, Any]:
        return {
            "round_name": self.round_name,
            "round_report": dict(self.round_report),
            "threshold_recommendation": dict(self.threshold_recommendation),
            "workload_summary": dict(self.workload_summary),
            "next_actions": list(self.next_actions),
        }


def _collect_rows(
    root: str | Path,
    manifests: list[LabelManifest],
    role_annotation_path: str | Path | None = None,
    owner_annotation_path: str | Path | None = None,
    annotation_meta_by_id: dict[str, dict[str, str]] | None = None,
) -> tuple[list[tuple[str, TrainingExample]], dict[str, list[TrainingExample]]]:
    base = Path(root)
    rows: list[tuple[str, TrainingExample]] = []
    workbook_examples: dict[str, list[TrainingExample]] = {}
    for workbook in sorted(base.rglob("*.xlsx")):
        if workbook.name.startswith("~$") or workbook.name.startswith(".~lock."):
            continue
        examples = build_training_examples(
            workbook,
            manifests,
            role_annotation_path=role_annotation_path,
            owner_annotation_path=owner_annotation_path,
            annotation_meta_by_id=annotation_meta_by_id,
        )
        workbook_examples[str(workbook)] = examples
        for example in examples:
            rows.append((str(workbook), example))
    return rows, workbook_examples


def train_gnn_model(
    root: str | Path,
    manifests: list[LabelManifest],
    model_path: str | Path,
    metrics_path: str | Path,
    metadata_path: str | Path | None = None,
    hidden_dim: int = 64,
    dropout: float = 0.25,
    epochs: int = 120,
    seed: int = 42,
    split_ratio: float = 0.8,
    synthetic_warmup: int = 0,
    self_training_rounds: int = 0,
    pseudo_positive_threshold: float = 0.9,
    pseudo_negative_threshold: float = 0.1,
    pseudo_max_rows: int = 500,
    role_annotation_path: str | Path | None = None,
    owner_annotation_path: str | Path | None = None,
    annotation_meta_by_id: dict[str, dict[str, str]] | None = None,
) -> dict[str, Any]:
    rows, _ = _collect_rows(
        root,
        manifests,
        role_annotation_path=role_annotation_path,
        owner_annotation_path=owner_annotation_path,
        annotation_meta_by_id=annotation_meta_by_id,
    )
    model = GraphRiskModel(
        hidden_dim=hidden_dim,
        dropout=dropout,
        epochs=epochs,
        seed=seed,
        split_ratio=split_ratio,
    )
    model.fit(
        rows,
        synthetic_warmup=synthetic_warmup,
        self_training_rounds=self_training_rounds,
        pseudo_positive_threshold=pseudo_positive_threshold,
        pseudo_negative_threshold=pseudo_negative_threshold,
        pseudo_max_rows=pseudo_max_rows,
    )
    model.save(model_path, metadata_path=metadata_path)
    metrics = model.training_summary.to_dict()
    recommendations: list[str] = []
    if metrics.get("labeled_nodes", 0) < 20:
        recommendations.append("Collect more verified labels before relying on threshold-based review batches.")
    if float(metrics.get("best_val_f1", 0.0)) < 0.6:
        recommendations.append("Review label quality and feature coverage before the next training round.")
    else:
        recommendations.append("Use the trained model to score new workbooks and export review candidates.")
    if int(metrics.get("pseudo_labeled_rows", 0)) > 0:
        recommendations.append("Audit a sample of pseudo-labeled rows before increasing self-training rounds.")
    metrics["recommendations"] = recommendations
    metrics["model_path"] = str(model_path)
    if metadata_path:
        metrics["metadata_path"] = str(metadata_path)
    path = Path(metrics_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(metrics, ensure_ascii=False, indent=2), encoding="utf-8")
    return metrics


def score_gnn_directory(
    root: str | Path,
    model_path: str | Path,
    manifests: list[LabelManifest],
    top_k: int = 100,
    include_labeled: bool = False,
    role_annotation_path: str | Path | None = None,
    owner_annotation_path: str | Path | None = None,
    annotation_meta_by_id: dict[str, dict[str, str]] | None = None,
) -> GNNScoreReport:
    rows, workbook_examples = _collect_rows(
        root,
        manifests,
        role_annotation_path=role_annotation_path,
        owner_annotation_path=owner_annotation_path,
        annotation_meta_by_id=annotation_meta_by_id,
    )
    model = GraphRiskModel.load(model_path)
    scores = model.score_rows(rows)

    top_rows: list[GNNScoreRow] = []
    all_scored_rows: list[GNNScoreRow] = []
    workbook_summaries: list[dict[str, Any]] = []
    total_rows = labeled_rows = positive_rows = negative_rows = unlabeled_rows = 0

    for workbook_path, examples in workbook_examples.items():
        duplicate_ids = build_duplicate_transaction_ids(examples)
        workbook_rows: list[GNNScoreRow] = []
        for source_path, example in rows:
            if source_path != workbook_path:
                continue
            key = f"{source_path}::{example.row_index}::{example.transaction_id or 'unknown'}"
            score = scores.get(key, 0.5)
            subject_name = _subject_name_from_workbook(workbook_path)
            payer_name = _resolve_party_name(example.payer_account, example, subject_name)
            payee_name = _resolve_party_name(example.payee_account, example, subject_name)
            payer_role = _resolve_party_role(example.payer_account, example)
            payee_role = _resolve_party_role(example.payee_account, example)
            total_rows += 1
            if example.label_status == "positive":
                labeled_rows += 1
                positive_rows += 1
            elif example.label_status == "negative":
                labeled_rows += 1
                negative_rows += 1
            else:
                unlabeled_rows += 1
            item = GNNScoreRow(
                workbook_path=workbook_path,
                row_index=example.row_index,
                transaction_id=example.transaction_id,
                label_status=example.label_status,
                score=score,
                amount=example.amount,
                timestamp=example.timestamp,
                counterparty=example.counterparty,
                direction=example.direction,
                channel=example.channel,
                remark=example.remark,
                subject_name=subject_name,
                subject_account=example.subject_account,
                counterparty_account=example.counterparty_account,
                counterparty_name=example.counterparty_name,
                payer_account=example.payer_account,
                payer_name=payer_name,
                payer_role=payer_role,
                payee_account=example.payee_account,
                payee_name=payee_name,
                payee_role=payee_role,
                buyer_account=example.buyer_account,
                seller_account=example.seller_account,
                role_label=example.role_label,
                extension_role=example.extension_role,
                review_flags=build_review_flags(example, duplicate_ids),
            )
            workbook_rows.append(item)
            all_scored_rows.append(item)
            if include_labeled or example.label_status == "unlabeled":
                top_rows.append(item)

        workbook_rows.sort(key=lambda item: (-item.score, item.row_index))
        workbook_summaries.append(
            {
                "path": workbook_path,
                "rows": len(workbook_rows),
                "avg_score": round(mean(item.score for item in workbook_rows), 4) if workbook_rows else 0.0,
                "max_score": round(max((item.score for item in workbook_rows), default=0.0), 4),
                "flagged_rows": sum(1 for item in workbook_rows if item.review_flags),
                "top_rows": [item.to_dict() for item in workbook_rows[:5]],
            }
        )

    top_rows.sort(key=lambda item: (-item.score, item.workbook_path, item.row_index))
    explicit_known_sellers = _known_seller_accounts(all_scored_rows)
    inferred_anchor_sellers = _infer_anchor_seller_accounts(all_scored_rows)
    known_sellers = explicit_known_sellers | inferred_anchor_sellers
    seller_candidates = _build_seller_candidates(all_scored_rows, top_k=top_k, known_sellers=known_sellers)
    workbook_summaries.sort(key=lambda item: (-item["max_score"], item["path"]))
    selected_top_rows = top_rows[:top_k]
    summary = {
        "top_k": top_k,
        "returned_top_rows": len(selected_top_rows),
        "returned_seller_candidates": len(seller_candidates),
        "include_labeled": include_labeled,
        "avg_top_score": round(mean(item.score for item in selected_top_rows), 4) if selected_top_rows else 0.0,
        "max_top_score": round(max((item.score for item in selected_top_rows), default=0.0), 4),
        "avg_seller_candidate_score": round(mean(item.score for item in seller_candidates), 4) if seller_candidates else 0.0,
        "explicit_known_sellers": len(explicit_known_sellers),
        "inferred_anchor_sellers": len(inferred_anchor_sellers),
        "known_seller_seeds": len(known_sellers),
        "bridge_backed_candidates": sum(1 for item in seller_candidates if item.bridge_buyers > 0),
        "bridge_candidate_rate": round(
            sum(1 for item in seller_candidates if item.bridge_buyers > 0) / len(seller_candidates),
            4,
        )
        if seller_candidates
        else 0.0,
        "avg_bridge_buyers": round(mean(item.bridge_buyers for item in seller_candidates), 4) if seller_candidates else 0.0,
        "max_bridge_buyers": max((item.bridge_buyers for item in seller_candidates), default=0),
        "strong_bridge_candidates": sum(1 for item in seller_candidates if item.candidate_tier == "strong_bridge_unknown_seller"),
        "weak_bridge_candidates": sum(1 for item in seller_candidates if item.candidate_tier == "weak_bridge_high_score"),
        "avg_bridge_uplift": round(mean(item.bridge_uplift for item in seller_candidates), 4) if seller_candidates else 0.0,
        "top_workbook": selected_top_rows[0].workbook_path if selected_top_rows else "",
        "top_seller_candidate": seller_candidates[0].seller_account if seller_candidates else "",
        "top_bridge_seller_candidate": next((item.seller_account for item in seller_candidates if item.bridge_buyers > 0), ""),
    }
    recommendations: list[str] = []
    if seller_candidates:
        recommendations.append("Review seller-account candidates first; they are aggregated by bridge-buyer support instead of single-row score.")
    if selected_top_rows:
        recommendations.append("Keep the top rows for trace evidence, but prioritize buyer-to-seller bridge paths during review.")
        if summary["max_top_score"] >= 0.9:
            recommendations.append("Preserve confirmed bridge buyers and unknown seller accounts as next-round extension labels.")
    else:
        recommendations.append("No rows met the current selection criteria; consider increasing top-k or including labeled rows for inspection.")
    return GNNScoreReport(
        total_workbooks=len(workbook_summaries),
        total_rows=total_rows,
        labeled_rows=labeled_rows,
        positive_rows=positive_rows,
        negative_rows=negative_rows,
        unlabeled_rows=unlabeled_rows,
        model={"path": str(model_path), **model.training_summary.to_dict()},
        summary=summary,
        recommendations=recommendations,
        top_rows=selected_top_rows,
        seller_candidates=seller_candidates,
        workbooks=workbook_summaries,
    )


def _annotation_meta(rows: list[AnnotationRow]) -> dict[str, dict[str, str]]:
    return {
        row.transaction_id: {
            "extension_role": row.extension_role,
            "anchor_subject": row.anchor_subject,
        }
        for row in rows
    }


def build_frozen_eval_report(
    root: str | Path,
    model_path: str | Path,
    holdout_rows: list[AnnotationRow],
    seller_candidates: list[SellerCandidateRow] | None = None,
    role_annotation_path: str | Path | None = None,
    owner_annotation_path: str | Path | None = None,
) -> FrozenEvalReport:
    manifests = build_annotation_manifests_from_rows(
        holdout_rows,
        dataset_name="holdout_eval",
        source_file="holdout_eval",
    )
    meta_by_id = _annotation_meta(holdout_rows)
    rows, _ = _collect_rows(
        root,
        manifests,
        role_annotation_path=role_annotation_path,
        owner_annotation_path=owner_annotation_path,
        annotation_meta_by_id=meta_by_id,
    )
    model = GraphRiskModel.load(model_path)
    scores = model.score_rows(rows)

    y_true: list[int] = []
    y_prob: list[float] = []
    role_buckets: dict[str, dict[str, Any]] = defaultdict(lambda: {"rows": 0, "positive_rows": 0, "negative_rows": 0, "tp": 0, "fp": 0, "tn": 0, "fn": 0})
    positive_sellers: set[str] = set()

    for source_path, example in rows:
        if example.label_status not in {"positive", "negative"}:
            continue
        key = f"{source_path}::{example.row_index}::{example.transaction_id or 'unknown'}"
        score = float(scores.get(key, 0.5))
        actual = 1 if example.label_status == "positive" else 0
        predicted = 1 if score >= 0.5 else 0
        role_name = meta_by_id.get(example.transaction_id, {}).get("extension_role") or example.extension_role or "unknown"
        bucket = role_buckets[role_name]
        bucket["rows"] += 1
        if actual == 1:
            bucket["positive_rows"] += 1
            if example.seller_account:
                positive_sellers.add(example.seller_account)
        else:
            bucket["negative_rows"] += 1
        if predicted == 1 and actual == 1:
            bucket["tp"] += 1
        elif predicted == 1 and actual == 0:
            bucket["fp"] += 1
        elif predicted == 0 and actual == 0:
            bucket["tn"] += 1
        else:
            bucket["fn"] += 1
        y_true.append(actual)
        y_prob.append(score)

    metrics = _metrics_from_predictions(
        torch.tensor(y_true, dtype=torch.long),
        torch.tensor(y_prob, dtype=torch.float32),
    )
    extension_role_summary: list[dict[str, Any]] = []
    for role_name, bucket in sorted(role_buckets.items()):
        precision = bucket["tp"] / (bucket["tp"] + bucket["fp"]) if (bucket["tp"] + bucket["fp"]) else 0.0
        recall = bucket["tp"] / (bucket["tp"] + bucket["fn"]) if (bucket["tp"] + bucket["fn"]) else 0.0
        extension_role_summary.append(
            {
                "extension_role": role_name,
                "rows": bucket["rows"],
                "positive_rows": bucket["positive_rows"],
                "negative_rows": bucket["negative_rows"],
                "precision": round(precision, 4),
                "recall": round(recall, 4),
            }
        )

    candidate_accounts = [item.seller_account for item in (seller_candidates or [])]
    positive_seller_hits = sorted(account for account in positive_sellers if account in set(candidate_accounts))
    seller_candidate_recovery = {
        "holdout_positive_seller_count": len(positive_sellers),
        "candidate_count": len(candidate_accounts),
        "recovered_positive_sellers": len(positive_seller_hits),
        "recovery_rate": round((len(positive_seller_hits) / len(positive_sellers)) if positive_sellers else 0.0, 4),
        "matched_seller_accounts": positive_seller_hits[:20],
    }
    return FrozenEvalReport(
        total_rows=len(y_true),
        positive_rows=sum(y_true),
        negative_rows=len(y_true) - sum(y_true),
        metrics=metrics,
        extension_role_summary=extension_role_summary,
        seller_candidate_recovery=seller_candidate_recovery,
    )


def export_frozen_eval_json(report: FrozenEvalReport, output_path: str | Path) -> Path:
    return write_json_file(output_path, report.to_dict())


def export_frozen_eval_markdown(report: FrozenEvalReport, output_path: str | Path) -> Path:
    lines = ["# Frozen Eval Report", ""]
    lines.append(f"- total_rows: {report.total_rows}")
    lines.append(f"- positive_rows: {report.positive_rows}")
    lines.append(f"- negative_rows: {report.negative_rows}")
    lines.append(f"- accuracy: {float(report.metrics.get('accuracy', 0.0)):.4f}")
    lines.append(f"- precision: {float(report.metrics.get('precision', 0.0)):.4f}")
    lines.append(f"- recall: {float(report.metrics.get('recall', 0.0)):.4f}")
    lines.append(f"- f1: {float(report.metrics.get('f1', 0.0)):.4f}")
    lines.append("")
    lines.append("## Extension Roles")
    lines.append("")
    lines.append("| extension_role | rows | positive_rows | negative_rows | precision | recall |")
    lines.append("| --- | ---: | ---: | ---: | ---: | ---: |")
    for item in report.extension_role_summary:
        lines.append(
            f"| {item['extension_role']} | {item['rows']} | {item['positive_rows']} | {item['negative_rows']} | {item['precision']:.4f} | {item['recall']:.4f} |"
        )
    lines.append("")
    lines.append("## Seller Recovery")
    lines.append("")
    recovery = report.seller_candidate_recovery
    lines.append(f"- holdout_positive_seller_count: {int(recovery.get('holdout_positive_seller_count', 0))}")
    lines.append(f"- candidate_count: {int(recovery.get('candidate_count', 0))}")
    lines.append(f"- recovered_positive_sellers: {int(recovery.get('recovered_positive_sellers', 0))}")
    lines.append(f"- recovery_rate: {float(recovery.get('recovery_rate', 0.0)):.4f}")
    matched = recovery.get("matched_seller_accounts", [])
    if matched:
        lines.append(f"- matched_seller_accounts: {', '.join(str(item) for item in matched)}")
    return write_markdown_lines(output_path, lines)


def _subject_name_from_workbook(path: str) -> str:
    stem = Path(path).stem
    for token in ("卖淫女", "嫖客", "（支）", "（一年）", "（半年）", "(个人)", "（个人）"):
        stem = stem.replace(token, "")
    stem = stem.strip()
    return stem


def _resolve_party_name(account: str, example: TrainingExample, subject_name: str) -> str:
    if not account or account == "-":
        return ""
    if account == example.subject_account:
        return subject_name
    if account == example.counterparty_account:
        return example.counterparty_name or example.counterparty or ""
    if account == example.payee_account and example.merchant_name:
        return example.merchant_name
    if account == example.payer_account and example.counterparty_name:
        return example.counterparty_name
    if account == example.buyer_account and example.role_label == "buyer":
        return subject_name
    if account == example.seller_account:
        return example.counterparty_name or example.merchant_name or ""
    return ""


def _resolve_party_role(account: str, example: TrainingExample) -> str:
    if not account or account == "-":
        return ""
    if account == example.buyer_account:
        return "buyer"
    if account == example.seller_account:
        return "seller"
    if account == example.subject_account and example.role_label:
        return example.role_label
    return ""


def _sanitize_review_text(value: Any) -> str:
    text = ("" if value is None else str(value)).replace("\r", " ").replace("\n", " ").strip()
    text = text.replace(",", " | ")
    return " ".join(text.split())


def _valid_party_account(value: Any) -> bool:
    text = str(value or "").strip()
    return text not in {"", "-", "--", "/", "null", "none"}


def _candidate_tier(bridge_buyers: int, known_buyer_support: int, bridge_ratio: float, support_rows: int) -> str:
    if bridge_buyers >= 3 or known_buyer_support >= 5 or (bridge_buyers >= 2 and bridge_ratio >= 0.35):
        return "strong_bridge_unknown_seller"
    if bridge_buyers >= 1:
        return "weak_bridge_high_score"
    if support_rows >= 20:
        return "high_support_non_bridge"
    return "score_only"


def _known_seller_accounts(rows: list[GNNScoreRow]) -> set[str]:
    known: set[str] = set()
    for item in rows:
        seller = str(item.seller_account or "").strip()
        if not _valid_party_account(seller):
            continue
        if item.label_status == "positive" or item.extension_role in {"seller_anchor", "buyer_to_known_seller"}:
            known.add(seller)
    return known


def _buyer_like_workbook(path: str) -> bool:
    stem = Path(path).stem.lower()
    return "嫖客" in stem or "buyer" in stem or stem.startswith("wxid_")


def _infer_anchor_seller_accounts(rows: list[GNNScoreRow]) -> set[str]:
    anchors: set[str] = set()
    grouped: dict[str, list[GNNScoreRow]] = defaultdict(list)
    for item in rows:
        grouped[item.workbook_path].append(item)

    for workbook_path, workbook_rows in grouped.items():
        if _buyer_like_workbook(workbook_path):
            continue
        seller_groups: dict[str, list[GNNScoreRow]] = defaultdict(list)
        for row in workbook_rows:
            seller = str(row.seller_account or "").strip()
            if _valid_party_account(seller):
                seller_groups[seller].append(row)
        if not seller_groups:
            continue
        total_rows = len(workbook_rows)
        ranked_groups = sorted(
            seller_groups.items(),
            key=lambda entry: (
                -len(entry[1]),
                -len({str(item.buyer_account or "").strip() for item in entry[1] if str(item.buyer_account or "").strip()}),
                -max((item.score for item in entry[1]), default=0.0),
                entry[0],
            ),
        )
        added = 0
        for seller, seller_rows in ranked_groups:
            seller_share = (len(seller_rows) / total_rows) if total_rows else 0.0
            unique_buyers = len({str(item.buyer_account or "").strip() for item in seller_rows if str(item.buyer_account or "").strip()})
            inbound_ratio = (
                sum(1 for item in seller_rows if item.direction == "入账" and str(item.payee_account or "").strip() == seller)
                / len(seller_rows)
            ) if seller_rows else 0.0
            if (
                len(seller_rows) >= 25
                and unique_buyers >= 8
                and inbound_ratio >= 0.55
                and (seller_share >= 0.18 or len(seller_rows) >= 80 or unique_buyers >= 25)
            ):
                anchors.add(seller)
                added += 1
            if added >= 3:
                break
    return anchors


def _build_seller_candidates(
    rows: list[GNNScoreRow],
    top_k: int,
    known_sellers: set[str] | None = None,
) -> list[SellerCandidateRow]:
    known_sellers = set(known_sellers or ())
    buyer_known_links: dict[str, set[str]] = defaultdict(set)
    for item in rows:
        buyer = str(item.buyer_account or "").strip()
        seller = str(item.seller_account or "").strip()
        if not _valid_party_account(buyer) or not _valid_party_account(seller):
            continue
        if seller in known_sellers:
            buyer_known_links[buyer].add(seller)

    grouped: dict[str, list[GNNScoreRow]] = defaultdict(list)
    for item in rows:
        seller = str(item.seller_account or "").strip()
        if not _valid_party_account(seller) or seller in known_sellers:
            continue
        grouped[seller].append(item)

    candidates: list[SellerCandidateRow] = []
    for seller, items in grouped.items():
        buyers = {str(item.buyer_account or "").strip() for item in items if _valid_party_account(item.buyer_account)}
        bridge_buyers = {buyer for buyer in buyers if buyer_known_links.get(buyer)}
        counterparty_samples = sorted({item.counterparty_name or item.counterparty for item in items if item.counterparty_name or item.counterparty})[:3]
        workbook_samples = sorted({item.workbook_path for item in items if item.workbook_path})[:3]
        max_score = max(item.score for item in items)
        avg_score = mean(item.score for item in items)
        bridge_ratio = (len(bridge_buyers) / len(buyers)) if buyers else 0.0
        known_buyer_support = sum(len(buyer_known_links.get(buyer, set())) for buyer in bridge_buyers)
        bridge_uplift = (
            + 0.22 * len(bridge_buyers)
            + 0.05 * min(known_buyer_support, 12)
            + 0.05 * len(buyers)
            + 0.04 * bridge_ratio
        )
        candidate_score = max_score + bridge_uplift
        candidate_tier = _candidate_tier(len(bridge_buyers), known_buyer_support, bridge_ratio, len(items))
        support_examples = _select_support_examples(
            items,
            limit=12,
            bridge_buyers=bridge_buyers,
            buyer_known_links=buyer_known_links,
        )
        candidates.append(
            SellerCandidateRow(
                seller_account=seller,
                score=candidate_score,
                avg_row_score=avg_score,
                bridge_uplift=bridge_uplift,
                support_rows=len(items),
                unique_buyers=len(buyers),
                bridge_buyers=len(bridge_buyers),
                bridge_support_ratio=bridge_ratio,
                known_buyer_support=known_buyer_support,
                candidate_tier=candidate_tier,
                unique_workbooks=len({item.workbook_path for item in items}),
                sample_counterparties=counterparty_samples,
                sample_workbooks=workbook_samples,
                support_examples=support_examples,
            )
        )
    candidates.sort(
        key=lambda item: (
            item.candidate_tier != "strong_bridge_unknown_seller",
            item.candidate_tier != "weak_bridge_high_score",
            -(1 if item.bridge_buyers > 0 else 0),
            -item.bridge_buyers,
            -item.known_buyer_support,
            -item.bridge_support_ratio,
            -item.score,
            -item.unique_buyers,
            -item.support_rows,
            item.seller_account,
        )
    )
    return candidates[:top_k]


def _select_support_examples(
    items: list[GNNScoreRow],
    limit: int = 12,
    bridge_buyers: set[str] | None = None,
    buyer_known_links: dict[str, set[str]] | None = None,
) -> list[dict[str, Any]]:
    if not items or limit <= 0:
        return []
    bridge_buyer_set = {str(item).strip() for item in (bridge_buyers or set()) if str(item).strip()}
    known_link_map = buyer_known_links or {}
    selected: list[GNNScoreRow] = []
    seen_keys: set[str] = set()
    deduped_items = _dedupe_support_rows(items)
    grouped_by_buyer: dict[str, list[GNNScoreRow]] = defaultdict(list)
    for item in deduped_items:
        buyer = str(item.buyer_account or "").strip() or "(missing)"
        grouped_by_buyer[buyer].append(item)

    ordered_buyers = sorted(
        grouped_by_buyer.items(),
        key=lambda entry: (
            entry[0] not in bridge_buyer_set,
            -len(entry[1]),
            -max(row.score for row in entry[1]),
            _support_row_sort_key(entry[1][0]),
        ),
    )
    for _, buyer_items in ordered_buyers:
        buyer_items.sort(key=lambda row: _support_row_priority_key(row, bridge_buyer_set))
        if _append_support_row(selected, seen_keys, buyer_items[0], limit):
            return _serialize_support_rows(_finalize_support_rows(selected), bridge_buyer_set, known_link_map)

    bridge_timed_items = [item for item in deduped_items if _parse_support_timestamp(item.timestamp) is not None and str(item.buyer_account or "").strip() in bridge_buyer_set]
    bridge_timed_items.sort(key=_support_row_time_key)
    for item in _time_anchor_rows(bridge_timed_items):
        if _append_support_row(selected, seen_keys, item, limit):
            return _serialize_support_rows(_finalize_support_rows(selected), bridge_buyer_set, known_link_map)

    timed_items = [item for item in deduped_items if _parse_support_timestamp(item.timestamp) is not None]
    timed_items.sort(key=_support_row_time_key)
    for item in _time_anchor_rows(timed_items):
        if _append_support_row(selected, seen_keys, item, limit):
            return _serialize_support_rows(_finalize_support_rows(selected), bridge_buyer_set, known_link_map)

    buyer_queues = [buyer_items[1:] for _, buyer_items in ordered_buyers if len(buyer_items) > 1]
    queue_index = 0
    while len(selected) < limit and buyer_queues:
        buyer_rows = buyer_queues[queue_index]
        if buyer_rows:
            row = buyer_rows.pop(0)
            if _append_support_row(selected, seen_keys, row, limit):
                return [item.to_dict() for item in _finalize_support_rows(selected)]
        if not buyer_rows:
            buyer_queues.pop(queue_index)
            if not buyer_queues:
                break
            queue_index %= len(buyer_queues)
            continue
        queue_index = (queue_index + 1) % len(buyer_queues)

    if len(selected) < limit:
        for item in sorted(deduped_items, key=lambda row: _support_row_priority_key(row, bridge_buyer_set)):
            if _append_support_row(selected, seen_keys, item, limit):
                return _serialize_support_rows(_finalize_support_rows(selected), bridge_buyer_set, known_link_map)

    return _serialize_support_rows(_finalize_support_rows(selected), bridge_buyer_set, known_link_map)


def _dedupe_support_rows(items: list[GNNScoreRow]) -> list[GNNScoreRow]:
    deduped: list[GNNScoreRow] = []
    seen_keys: set[str] = set()
    for item in sorted(items, key=_support_row_priority_key):
        dedupe_key = _support_row_key(item)
        if dedupe_key in seen_keys:
            continue
        deduped.append(item)
        seen_keys.add(dedupe_key)
    return deduped


def _support_row_key(item: GNNScoreRow) -> str:
    return f"{item.workbook_path}::{item.row_index}::{item.transaction_id}"


def _append_support_row(
    selected: list[GNNScoreRow],
    seen_keys: set[str],
    item: GNNScoreRow,
    limit: int,
) -> bool:
    dedupe_key = _support_row_key(item)
    if dedupe_key in seen_keys:
        return False
    selected.append(item)
    seen_keys.add(dedupe_key)
    return len(selected) >= limit


def _parse_support_timestamp(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    normalized = text.replace("/", "-").replace("T", " ")
    normalized = " ".join(normalized.split())
    compact = "".join(ch for ch in text if ch.isdigit())
    candidates = [normalized, normalized[:19], normalized[:16], normalized[:10]]
    if compact:
        candidates.extend([compact, compact[:14], compact[:12], compact[:8]])
    formats = (
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d",
        "%Y%m%d%H%M%S",
        "%Y%m%d%H%M",
        "%Y%m%d",
    )
    for candidate in candidates:
        for fmt in formats:
            try:
                return datetime.strptime(candidate, fmt)
            except ValueError:
                continue
    return None


def _support_row_sort_key(item: GNNScoreRow) -> tuple[Any, ...]:
    parsed_time = _parse_support_timestamp(item.timestamp)
    return (
        parsed_time is None,
        parsed_time or datetime.max,
        -item.score,
        item.row_index,
        item.workbook_path,
    )


def _support_row_time_key(item: GNNScoreRow) -> tuple[Any, ...]:
    parsed_time = _parse_support_timestamp(item.timestamp)
    return (
        parsed_time or datetime.max,
        -item.score,
        item.row_index,
        item.workbook_path,
    )


def _support_row_priority_key(item: GNNScoreRow, bridge_buyers: set[str] | None = None) -> tuple[Any, ...]:
    parsed_time = _parse_support_timestamp(item.timestamp)
    buyer = str(item.buyer_account or "").strip()
    return (
        buyer not in (bridge_buyers or set()),
        -item.score,
        parsed_time is None,
        parsed_time or datetime.max,
        not buyer,
        item.row_index,
        item.workbook_path,
    )


def _time_anchor_rows(items: list[GNNScoreRow]) -> list[GNNScoreRow]:
    if not items:
        return []
    anchor_indices = {0, len(items) - 1}
    if len(items) >= 3:
        anchor_indices.add(len(items) // 2)
    if len(items) >= 4:
        anchor_indices.add(max(0, round((len(items) - 1) * 0.25)))
        anchor_indices.add(max(0, round((len(items) - 1) * 0.75)))
    return [items[index] for index in sorted(anchor_indices)]


def _finalize_support_rows(items: list[GNNScoreRow]) -> list[GNNScoreRow]:
    return sorted(items, key=_support_row_time_key)


def _serialize_support_rows(
    items: list[GNNScoreRow],
    bridge_buyers: set[str],
    buyer_known_links: dict[str, set[str]],
) -> list[dict[str, Any]]:
    serialized: list[dict[str, Any]] = []
    for item in items:
        row = item.to_dict()
        buyer = str(item.buyer_account or "").strip()
        row["bridge_buyer"] = buyer in bridge_buyers
        row["known_seller_links"] = len(buyer_known_links.get(buyer, set()))
        serialized.append(row)
    return serialized


def export_gnn_score_json(report: GNNScoreReport, output_path: str | Path) -> Path:
    return write_json_file(output_path, report.to_dict())


def export_gnn_score_markdown(report: GNNScoreReport, output_path: str | Path) -> Path:
    lines = ["# GNN Score Report", ""]
    lines.append(f"- workbooks: {report.total_workbooks}")
    lines.append(f"- rows: {report.total_rows}")
    lines.append(f"- labeled: {report.labeled_rows}")
    lines.append(f"- positive: {report.positive_rows}")
    lines.append(f"- negative: {report.negative_rows}")
    lines.append(f"- unlabeled: {report.unlabeled_rows}")
    lines.append(f"- model: {report.model.get('model_name', 'row_gnn')}")
    lines.append(f"- best_val_f1: {float(report.model.get('best_val_f1', 0.0)):.4f}")
    lines.append(f"- returned_top_rows: {int(report.summary.get('returned_top_rows', 0))}")
    lines.append(f"- returned_seller_candidates: {int(report.summary.get('returned_seller_candidates', 0))}")
    lines.append(f"- avg_top_score: {float(report.summary.get('avg_top_score', 0.0)):.4f}")
    lines.append(f"- avg_seller_candidate_score: {float(report.summary.get('avg_seller_candidate_score', 0.0)):.4f}")
    lines.append(f"- explicit_known_sellers: {int(report.summary.get('explicit_known_sellers', 0))}")
    lines.append(f"- inferred_anchor_sellers: {int(report.summary.get('inferred_anchor_sellers', 0))}")
    lines.append(f"- known_seller_seeds: {int(report.summary.get('known_seller_seeds', 0))}")
    lines.append(f"- bridge_backed_candidates: {int(report.summary.get('bridge_backed_candidates', 0))}")
    lines.append(f"- bridge_candidate_rate: {float(report.summary.get('bridge_candidate_rate', 0.0)):.4f}")
    lines.append(f"- avg_bridge_buyers: {float(report.summary.get('avg_bridge_buyers', 0.0)):.4f}")
    lines.append(f"- max_bridge_buyers: {int(report.summary.get('max_bridge_buyers', 0))}")
    lines.append(f"- strong_bridge_candidates: {int(report.summary.get('strong_bridge_candidates', 0))}")
    lines.append(f"- weak_bridge_candidates: {int(report.summary.get('weak_bridge_candidates', 0))}")
    lines.append(f"- avg_bridge_uplift: {float(report.summary.get('avg_bridge_uplift', 0.0)):.4f}")
    lines.append(f"- top_workbook: {report.summary.get('top_workbook', '')}")
    lines.append(f"- top_seller_candidate: {report.summary.get('top_seller_candidate', '')}")
    lines.append(f"- top_bridge_seller_candidate: {report.summary.get('top_bridge_seller_candidate', '')}")
    lines.append("")
    lines.append("## Recommendations")
    lines.append("")
    for item in report.recommendations:
        lines.append(f"- {item}")
    lines.append("")
    lines.append("## Seller Candidates")
    lines.append("")
    lines.append("| tier | score | uplift | seller_account | bridge_buyers | bridge_ratio | known_links | unique_buyers | rows | workbooks |")
    lines.append("| --- | ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: |")
    for item in report.seller_candidates:
        lines.append(
            f"| {item.candidate_tier} | {item.score:.4f} | {item.bridge_uplift:.4f} | {item.seller_account} | {item.bridge_buyers} | {item.bridge_support_ratio:.4f} | {item.known_buyer_support} | {item.unique_buyers} | {item.support_rows} | {item.unique_workbooks} |"
        )
    lines.append("")
    lines.append("## Top Rows")
    lines.append("")
    lines.append("| score | workbook | row | transaction_id | status | flags | counterparty | remark |")
    lines.append("| --- | --- | ---: | --- | --- | --- | --- | --- |")
    for item in report.top_rows:
        lines.append(
            f"| {item.score:.4f} | {item.workbook_path} | {item.row_index} | {item.transaction_id} | {item.label_status} | {'/'.join(item.review_flags)} | {item.counterparty} | {item.remark} |"
        )
    lines.append("")
    lines.append("## Workbooks")
    lines.append("")
    lines.append("| score | workbook | rows | flagged |")
    lines.append("| --- | --- | ---: | ---: |")
    for item in report.workbooks:
        lines.append(f"| {item['max_score']:.4f} | {item['path']} | {item['rows']} | {item['flagged_rows']} |")
    return write_markdown_lines(output_path, lines)


def export_review_candidates(
    score_json_path: str | Path,
    csv_path: str | Path | None = None,
    xlsx_path: str | Path | None = None,
    md_path: str | Path | None = None,
    threshold: float = 0.7,
    limit: int = 100,
    entity_type: str = "auto",
    tier: str = "",
) -> list[dict[str, Any]]:
    transaction_review_fields = [
        "record_id",
        "entity_type",
        "pred_score",
        "review_label",
        "review_options",
        "review_note",
        "workbook_path",
        "row_index",
        "transaction_id",
        "transaction_id_text",
        "amount",
        "timestamp",
        "subject_name",
        "subject_account",
        "payer_account",
        "payer_name",
        "payer_role",
        "payee_account",
        "payee_name",
        "payee_role",
        "counterparty",
        "counterparty_account",
        "counterparty_name",
        "buyer_account",
        "seller_account",
        "model_role",
        "direction",
        "channel",
        "remark",
    ]
    seller_review_fields = [
        "record_id",
        "entity_type",
        "pred_score",
        "review_label",
        "review_options",
        "review_note",
        "seller_account",
        "candidate_tier",
        "bridge_uplift",
        "bridge_buyers",
        "bridge_support_ratio",
        "known_buyer_support",
        "unique_buyers",
        "support_rows",
        "unique_workbooks",
        "sample_counterparties",
        "sample_workbooks",
    ]
    payload = json.loads(Path(score_json_path).read_text(encoding="utf-8"))
    use_seller_candidates = entity_type == "seller_account"
    rows = payload.get("seller_candidates", []) if use_seller_candidates else payload.get("top_rows", [])
    candidates: list[dict[str, Any]] = []
    for item in rows:
        score = float(item.get("score", 0.0))
        if score < threshold:
            continue
        item_tier = str(item.get("candidate_tier", "")).strip()
        if tier and item_tier != tier:
            continue
        if use_seller_candidates:
            reason_parts = [
                f"score={score:.4f}",
                f"tier={str(item.get('candidate_tier', ''))}",
                f"bridge_uplift={float(item.get('bridge_uplift', 0.0)):.4f}",
                f"bridge_buyers={int(item.get('bridge_buyers', 0))}",
                f"bridge_ratio={float(item.get('bridge_support_ratio', 0.0)):.4f}",
                f"known_links={int(item.get('known_buyer_support', 0))}",
                f"unique_buyers={int(item.get('unique_buyers', 0))}",
            ]
            candidates.append(
                {
                    "record_id": str(item.get("seller_account", "")),
                    "entity_type": "seller_account",
                    "pred_score": round(score, 4),
                    "review_label": "",
                    "review_options": "confirmed_positive|confirmed_negative|uncertain",
                    "review_note": " ; ".join(reason_parts),
                    "seller_account": item.get("seller_account", ""),
                    "candidate_tier": item.get("candidate_tier", ""),
                    "bridge_uplift": item.get("bridge_uplift", 0.0),
                    "bridge_buyers": item.get("bridge_buyers", 0),
                    "bridge_support_ratio": item.get("bridge_support_ratio", 0.0),
                    "known_buyer_support": item.get("known_buyer_support", 0),
                    "unique_buyers": item.get("unique_buyers", 0),
                    "support_rows": item.get("support_rows", 0),
                    "unique_workbooks": item.get("unique_workbooks", 0),
                    "sample_counterparties": "|".join(item.get("sample_counterparties", [])),
                    "sample_workbooks": "|".join(item.get("sample_workbooks", [])),
                }
            )
        else:
            flags = [str(flag) for flag in item.get("review_flags", [])]
            reason_parts = [f"score={score:.4f}"]
            if flags:
                reason_parts.append("flags=" + "/".join(flags))
            if item.get("counterparty"):
                reason_parts.append(f"counterparty={item['counterparty']}")
            candidates.append(
                {
                    "record_id": f"{item.get('workbook_path', '')}:{item.get('row_index', '')}",
                    "entity_type": "transaction",
                    "pred_score": round(score, 4),
                    "review_label": "",
                    "review_options": "confirmed_positive|confirmed_negative|uncertain",
                    "review_note": " ; ".join(reason_parts),
                    "workbook_path": item.get("workbook_path", ""),
                    "row_index": item.get("row_index", 0),
                    "transaction_id": item.get("transaction_id", ""),
                    "transaction_id_text": f"'{item.get('transaction_id', '')}" if item.get("transaction_id") else "",
                    "amount": item.get("amount", ""),
                    "timestamp": item.get("timestamp", ""),
                    "subject_name": item.get("subject_name", ""),
                    "subject_account": item.get("subject_account", ""),
                    "payer_account": item.get("payer_account", ""),
                    "payer_name": item.get("payer_name", ""),
                    "payer_role": item.get("payer_role", ""),
                    "payee_account": item.get("payee_account", ""),
                    "payee_name": item.get("payee_name", ""),
                    "payee_role": item.get("payee_role", ""),
                    "counterparty": item.get("counterparty", ""),
                    "counterparty_account": item.get("counterparty_account", ""),
                    "counterparty_name": item.get("counterparty_name", ""),
                    "buyer_account": item.get("buyer_account", ""),
                    "seller_account": item.get("seller_account", ""),
                    "model_role": item.get("role_label", ""),
                    "direction": item.get("direction", ""),
                    "channel": item.get("channel", ""),
                    "remark": item.get("remark", ""),
                }
            )
    candidates = candidates[:limit]
    review_fields = seller_review_fields if use_seller_candidates else transaction_review_fields

    if csv_path:
        path = Path(csv_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=review_fields)
            writer.writeheader()
            for item in candidates:
                writer.writerow({key: _sanitize_review_text(item.get(key, "")) for key in review_fields})

    if xlsx_path:
        write_xlsx_table(
            xlsx_path,
            headers=review_fields,
            rows=[{key: _sanitize_review_text(item.get(key, "")) for key in review_fields} for item in candidates],
            row_fills=["yellow"] * len(candidates),
        )

    if md_path:
        path = Path(md_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        lines = ["# Review Candidates", ""]
        if use_seller_candidates:
            lines.append("| score | seller_account | bridge_buyers | unique_buyers | rows | note |")
            lines.append("| --- | --- | ---: | ---: | ---: | --- |")
            for item in candidates:
                lines.append(
                    f"| {item['pred_score']:.4f} | {item['seller_account']} | {item['bridge_buyers']} | {item['unique_buyers']} | {item['support_rows']} | {item['review_note']} |"
                )
        else:
            lines.append("| score | workbook | row | transaction_id | counterparty | note |")
            lines.append("| --- | --- | ---: | --- | --- | --- |")
            for item in candidates:
                lines.append(
                    f"| {item['pred_score']:.4f} | {item['workbook_path']} | {item['row_index']} | {item['transaction_id']} | {item['counterparty']} | {item['review_note']} |"
                )
        path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return candidates


def compare_round_metrics(round_specs: list[dict[str, str]]) -> RoundComparisonReport:
    rows: list[RoundComparisonRow] = []
    for spec in round_specs:
        round_name = spec.get("round_name", "").strip() or Path(spec["metrics"]).stem
        metrics_payload = json.loads(Path(spec["metrics"]).read_text(encoding="utf-8"))
        review_stats = load_review_stats(spec.get("reviews"))
        resolved = review_stats["confirmed_positive"] + review_stats["confirmed_negative"]
        review_total = review_stats["review_total"]
        rows.append(
            RoundComparisonRow(
                round_name=round_name,
                best_val_f1=float(metrics_payload.get("best_val_f1", 0.0)),
                best_val_loss=float(metrics_payload.get("best_val_loss", 0.0)),
                positive_rate=float(metrics_payload.get("positive_rate", 0.0)),
                train_nodes=int(metrics_payload.get("train_nodes", 0)),
                val_nodes=int(metrics_payload.get("val_nodes", 0)),
                review_total=review_total,
                confirmed_positive=review_stats["confirmed_positive"],
                confirmed_negative=review_stats["confirmed_negative"],
                uncertain=review_stats["uncertain"],
                review_resolution_rate=(resolved / review_total) if review_total else 0.0,
                review_positive_rate=(review_stats["confirmed_positive"] / resolved) if resolved else 0.0,
            )
        )
    return RoundComparisonReport(rounds=rows)


def export_round_comparison_json(report: RoundComparisonReport, output_path: str | Path) -> Path:
    return write_json_file(output_path, report.to_dict())


def export_round_comparison_markdown(report: RoundComparisonReport, output_path: str | Path) -> Path:
    lines = ["# Round Comparison", ""]
    lines.append("| round | val_f1 | val_loss | positive_rate | train_nodes | val_nodes | review_total | confirmed_positive | confirmed_negative | uncertain | resolution_rate | positive_rate_review |")
    lines.append("| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |")
    for item in report.rounds:
        lines.append(
            f"| {item.round_name} | {item.best_val_f1:.4f} | {item.best_val_loss:.6f} | {item.positive_rate:.4f} | {item.train_nodes} | {item.val_nodes} | {item.review_total} | {item.confirmed_positive} | {item.confirmed_negative} | {item.uncertain} | {item.review_resolution_rate:.4f} | {item.review_positive_rate:.4f} |"
        )
    return write_markdown_lines(output_path, lines)


def build_round_decision_sheet(
    round_name: str,
    metrics_json_path: str | Path,
    score_json_path: str | Path,
    review_csv_path: str | Path | None = None,
    label_json_paths: list[str] | None = None,
    thresholds: list[float] | None = None,
    reviewers: int = 1,
    daily_capacity_per_reviewer: int = 50,
    max_team_days: float | None = None,
    min_confirmed_positive_rate: float = 0.0,
    min_candidates: int = 1,
) -> RoundDecisionSheet:
    round_report = build_round_report(
        round_name=round_name,
        metrics_json_path=metrics_json_path,
        score_json_path=score_json_path,
        review_csv_path=review_csv_path,
        label_json_paths=label_json_paths,
    )
    sweep = score_threshold_sweep(
        score_json_path=score_json_path,
        review_csv_path=review_csv_path,
        thresholds=thresholds,
    )
    workload = review_workload_forecast(
        sweep,
        reviewers=reviewers,
        daily_capacity_per_reviewer=daily_capacity_per_reviewer,
    )
    recommendation = select_operating_threshold(
        workload,
        max_team_days=max_team_days,
        min_confirmed_positive_rate=min_confirmed_positive_rate,
        min_candidates=min_candidates,
    )
    next_actions = [
        f"Use threshold {recommendation.recommended_threshold:.2f} for the next review batch.",
        "Review top candidates under the selected threshold first.",
        "Import confirmed positives and negatives into the next round's label manifests.",
        "Re-run round comparison after the next training cycle.",
    ]
    return RoundDecisionSheet(
        round_name=round_name,
        round_report=round_report.to_dict(),
        threshold_recommendation=recommendation.to_dict(),
        workload_summary={
            "reviewers": workload.reviewers,
            "daily_capacity_per_reviewer": workload.daily_capacity_per_reviewer,
            "rows": [item.to_dict() for item in workload.rows],
        },
        next_actions=next_actions,
    )


def export_round_decision_sheet_json(report: RoundDecisionSheet, output_path: str | Path) -> Path:
    return write_json_file(output_path, report.to_dict())


def export_round_decision_sheet_markdown(report: RoundDecisionSheet, output_path: str | Path) -> Path:
    lines = [f"# Round Decision Sheet: {report.round_name}", ""]
    lines.append("## Training Snapshot")
    lines.append("")
    metrics = report.round_report.get("metrics", {})
    lines.append(f"- best_val_f1: {float(metrics.get('best_val_f1', 0.0)):.4f}")
    lines.append(f"- best_val_loss: {float(metrics.get('best_val_loss', 0.0)):.6f}")
    lines.append(f"- positive_rate: {float(metrics.get('positive_rate', 0.0)):.4f}")
    lines.append("")
    lines.append("## Recommended Threshold")
    lines.append("")
    recommendation = report.threshold_recommendation
    lines.append(f"- recommended_threshold: {float(recommendation.get('recommended_threshold', 0.0)):.4f}")
    lines.append(f"- reason: {recommendation.get('reason', '')}")
    lines.append("")
    lines.append("## Workload Summary")
    lines.append("")
    workload_summary = report.workload_summary
    lines.append(f"- reviewers: {int(workload_summary.get('reviewers', 0))}")
    lines.append(f"- daily_capacity_per_reviewer: {int(workload_summary.get('daily_capacity_per_reviewer', 0))}")
    selected_row = recommendation.get("row", {})
    for key, value in selected_row.items():
        lines.append(f"- {key}: {value}")
    lines.append("")
    lines.append("## Next Actions")
    lines.append("")
    for action in report.next_actions:
        lines.append(f"- {action}")
    return write_markdown_lines(output_path, lines)
