from __future__ import annotations

import csv
import json
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from statistics import mean
from typing import Any

from .excel import write_xlsx_table
from .graph_risk import GraphRiskModel
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
    support_rows: int
    unique_buyers: int
    bridge_buyers: int
    known_buyer_support: int
    unique_workbooks: int
    sample_counterparties: list[str]
    sample_workbooks: list[str]

    def to_dict(self) -> dict[str, Any]:
        return {
            "seller_account": self.seller_account,
            "score": round(self.score, 4),
            "avg_row_score": round(self.avg_row_score, 4),
            "support_rows": self.support_rows,
            "unique_buyers": self.unique_buyers,
            "bridge_buyers": self.bridge_buyers,
            "known_buyer_support": self.known_buyer_support,
            "unique_workbooks": self.unique_workbooks,
            "sample_counterparties": list(self.sample_counterparties),
            "sample_workbooks": list(self.sample_workbooks),
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
) -> dict[str, Any]:
    rows, _ = _collect_rows(
        root,
        manifests,
        role_annotation_path=role_annotation_path,
        owner_annotation_path=owner_annotation_path,
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
) -> GNNScoreReport:
    rows, workbook_examples = _collect_rows(
        root,
        manifests,
        role_annotation_path=role_annotation_path,
        owner_annotation_path=owner_annotation_path,
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
    seller_candidates = _build_seller_candidates(all_scored_rows, top_k=top_k)
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
        "top_workbook": selected_top_rows[0].workbook_path if selected_top_rows else "",
        "top_seller_candidate": seller_candidates[0].seller_account if seller_candidates else "",
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
    text = str(value or "").replace("\r", " ").replace("\n", " ").strip()
    text = text.replace(",", " | ")
    return " ".join(text.split())


def _known_seller_accounts(rows: list[GNNScoreRow]) -> set[str]:
    known: set[str] = set()
    for item in rows:
        seller = str(item.seller_account or "").strip()
        if not seller:
            continue
        if item.label_status == "positive" or item.extension_role in {"seller_anchor", "buyer_to_known_seller"}:
            known.add(seller)
    return known


def _build_seller_candidates(rows: list[GNNScoreRow], top_k: int) -> list[SellerCandidateRow]:
    known_sellers = _known_seller_accounts(rows)
    buyer_known_links: dict[str, set[str]] = defaultdict(set)
    for item in rows:
        buyer = str(item.buyer_account or "").strip()
        seller = str(item.seller_account or "").strip()
        if not buyer or not seller:
            continue
        if seller in known_sellers:
            buyer_known_links[buyer].add(seller)

    grouped: dict[str, list[GNNScoreRow]] = defaultdict(list)
    for item in rows:
        seller = str(item.seller_account or "").strip()
        if not seller or seller in known_sellers:
            continue
        grouped[seller].append(item)

    candidates: list[SellerCandidateRow] = []
    for seller, items in grouped.items():
        buyers = {str(item.buyer_account or "").strip() for item in items if item.buyer_account}
        bridge_buyers = {buyer for buyer in buyers if buyer_known_links.get(buyer)}
        counterparty_samples = sorted({item.counterparty_name or item.counterparty for item in items if item.counterparty_name or item.counterparty})[:3]
        workbook_samples = sorted({item.workbook_path for item in items if item.workbook_path})[:3]
        max_score = max(item.score for item in items)
        avg_score = mean(item.score for item in items)
        bridge_ratio = (len(bridge_buyers) / len(buyers)) if buyers else 0.0
        candidate_score = max_score + 0.08 * len(bridge_buyers) + 0.04 * len(buyers) + 0.02 * bridge_ratio
        candidates.append(
            SellerCandidateRow(
                seller_account=seller,
                score=candidate_score,
                avg_row_score=avg_score,
                support_rows=len(items),
                unique_buyers=len(buyers),
                bridge_buyers=len(bridge_buyers),
                known_buyer_support=sum(len(buyer_known_links.get(buyer, set())) for buyer in bridge_buyers),
                unique_workbooks=len({item.workbook_path for item in items}),
                sample_counterparties=counterparty_samples,
                sample_workbooks=workbook_samples,
            )
        )
    candidates.sort(
        key=lambda item: (
            -item.score,
            -item.bridge_buyers,
            -item.unique_buyers,
            -item.support_rows,
            item.seller_account,
        )
    )
    return candidates[:top_k]


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
    lines.append(f"- top_workbook: {report.summary.get('top_workbook', '')}")
    lines.append(f"- top_seller_candidate: {report.summary.get('top_seller_candidate', '')}")
    lines.append("")
    lines.append("## Recommendations")
    lines.append("")
    for item in report.recommendations:
        lines.append(f"- {item}")
    lines.append("")
    lines.append("## Seller Candidates")
    lines.append("")
    lines.append("| score | seller_account | bridge_buyers | unique_buyers | rows | workbooks |")
    lines.append("| --- | --- | ---: | ---: | ---: | ---: |")
    for item in report.seller_candidates:
        lines.append(
            f"| {item.score:.4f} | {item.seller_account} | {item.bridge_buyers} | {item.unique_buyers} | {item.support_rows} | {item.unique_workbooks} |"
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
        "bridge_buyers",
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
        if use_seller_candidates:
            reason_parts = [
                f"score={score:.4f}",
                f"bridge_buyers={int(item.get('bridge_buyers', 0))}",
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
                    "bridge_buyers": item.get("bridge_buyers", 0),
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
