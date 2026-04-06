from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from .analysis import analyze_transactions_from_path
from .annotations import (
    AnnotationRow,
    build_review_annotations,
    export_annotations_csv,
    export_annotations_jsonl,
    load_annotation_manifests,
)
from .catalog import export_label_catalog_json, export_label_catalog_markdown
from .gnn_pipeline import (
    build_round_report,
    build_round_decision_sheet,
    bootstrap_round,
    compare_round_metrics,
    export_round_decision_sheet_json,
    export_round_decision_sheet_markdown,
    export_operating_threshold_json,
    export_operating_threshold_markdown,
    export_gnn_score_json,
    export_gnn_score_markdown,
    export_graph_dataset_summary,
    export_round_bootstrap_json,
    export_round_bootstrap_markdown,
    export_round_report_json,
    export_round_report_markdown,
    export_round_comparison_json,
    export_round_comparison_markdown,
    export_review_workload_json,
    export_review_workload_markdown,
    export_threshold_sweep_json,
    export_threshold_sweep_markdown,
    export_normalized_ledgers,
    export_owner_review_csv,
    export_owner_review_xlsx,
    export_owner_summary_csv,
    export_owner_summary_json,
    export_review_candidates,
    build_owner_review_roles,
    build_owner_review_rows,
    review_workload_forecast,
    select_operating_threshold,
    score_threshold_sweep,
    score_gnn_directory,
    summarize_graph_dataset,
    summarize_owner_activity,
    train_gnn_model,
)
from .graph_risk import export_graph_triage_json, export_graph_triage_markdown, score_directory
from .labels import build_review_manifest, export_label_manifest, load_label_manifests, merge_label_manifests
from .model import BaselineTextClassifier, train_baseline_classifier
from .pdf_ingest import export_wechat_pdf_rows_csv, export_wechat_pdf_rows_jsonl, load_wechat_pdf_rows_from_path
from .roles import export_role_annotations_csv, export_role_annotations_jsonl
from .ledger_ops import OwnerSummaryReport, OwnerSummaryRow
from .ledger_ops import (
    build_ledger_review_rows,
    build_mirror_annotations,
    build_mirror_review_rows,
    build_rule_audit_rows,
    build_rule_review_summary,
    build_rule_summary,
    export_ledger_review_csv,
    export_ledger_review_xlsx,
    export_mirror_annotations_csv,
    export_mirror_annotations_jsonl,
    export_mirror_review_csv,
    export_mirror_review_xlsx,
    export_rule_audit_csv,
    export_rule_audit_xlsx,
    export_rule_review_summary_json,
    export_rule_review_summary_markdown,
    export_rule_summary_json,
    export_rule_summary_markdown,
    load_normalized_ledgers,
)
from .training import (
    build_positive_training_samples,
    build_training_examples,
    export_training_examples_csv,
    export_training_examples_jsonl,
    export_split_csv,
    export_split_jsonl,
    export_training_samples_csv,
    export_training_samples_jsonl,
    split_training_examples,
)
from .report import render_json_report, render_markdown_report
from .triage import export_triage_json, export_triage_markdown, scan_workbook_directory


def _existing_file(path: str | Path, label: str) -> Path:
    resolved = Path(path)
    if not resolved.exists():
        raise FileNotFoundError(f"{label} not found: {resolved}")
    if not resolved.is_file():
        raise ValueError(f"{label} must be a file: {resolved}")
    return resolved


def _existing_directory(path: str | Path, label: str) -> Path:
    resolved = Path(path)
    if not resolved.exists():
        raise FileNotFoundError(f"{label} not found: {resolved}")
    if not resolved.is_dir():
        raise ValueError(f"{label} must be a directory: {resolved}")
    return resolved


def _ratio_between_zero_and_one(value: float, label: str) -> None:
    if not 0.0 < float(value) < 1.0:
        raise ValueError(f"{label} must be between 0 and 1")


def _probability(value: float, label: str) -> None:
    if not 0.0 <= float(value) <= 1.0:
        raise ValueError(f"{label} must be between 0 and 1")


def _positive_int(value: int, label: str) -> None:
    if int(value) <= 0:
        raise ValueError(f"{label} must be greater than 0")


def _resolve_manifests(labels: list[str] | None = None, annotations: str | None = None, require_input: bool = True):
    label_paths = list(labels or [])
    if annotations:
        _existing_file(annotations, "annotations file")
        return load_annotation_manifests(annotations)
    if label_paths:
        return load_label_manifests(label_paths)
    if require_input:
        raise ValueError("provide either --labels or --annotations")
    return []


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Transaction network risk analysis toolkit")
    subparsers = parser.add_subparsers(dest="command", required=True)

    analyze_parser = subparsers.add_parser("analyze", help="Analyze one CSV/PDF file or a directory of CSV/PDF files")
    analyze_parser.add_argument("input", help="CSV/PDF file or directory")
    analyze_parser.add_argument("--format", choices=["markdown", "json"], default="markdown")
    analyze_parser.add_argument("--output", "-o", help="Output file")
    analyze_parser.add_argument("--ocr-command", help="Optional OCR command for image-based PDFs; use {image} as the input placeholder")
    analyze_parser.add_argument("--account-name", help="Optional account name override for payment PDFs")

    extract_pdf_parser = subparsers.add_parser("extract-payment-pdf", help="Extract WeChat or Alipay payment PDFs into normalized rows or annotations")
    extract_pdf_parser.add_argument("input", help="PDF file or directory")
    extract_pdf_parser.add_argument("--csv", help="Output normalized ledger CSV")
    extract_pdf_parser.add_argument("--jsonl", help="Output normalized ledger JSONL")
    extract_pdf_parser.add_argument("--annotations-csv", help="Output simplified annotation CSV")
    extract_pdf_parser.add_argument("--annotations-jsonl", help="Output simplified annotation JSONL")
    extract_pdf_parser.add_argument("--label", choices=["positive", "negative", "skip"], default="positive", help="Annotation label to assign when exporting annotations")
    extract_pdf_parser.add_argument("--note", default="", help="Optional note attached to exported annotations")
    extract_pdf_parser.add_argument("--ocr-command", help="Optional OCR command for image-based PDFs; use {image} as the input placeholder")
    extract_pdf_parser.add_argument("--account-name", help="Optional account name override for payment PDFs")

    export_parser = subparsers.add_parser("export-training", help="Export labeled samples from an xlsx file")
    export_parser.add_argument("--xlsx", required=True, help="Source xlsx file")
    export_parser.add_argument("--labels", nargs="+", required=True, help="Label manifest JSON files")
    export_parser.add_argument("--csv", help="Output CSV file")
    export_parser.add_argument("--jsonl", help="Output JSONL file")

    dataset_parser = subparsers.add_parser("export-dataset", help="Export a feature table for all xlsx rows")
    dataset_parser.add_argument("--xlsx", required=True, help="Source xlsx file")
    dataset_parser.add_argument("--labels", nargs="+", required=True, help="Label manifest JSON files")
    dataset_parser.add_argument("--csv", help="Output CSV file")
    dataset_parser.add_argument("--jsonl", help="Output JSONL file")

    catalog_parser = subparsers.add_parser("label-catalog", help="Export a summary catalog of label manifests")
    catalog_parser.add_argument("--labels", nargs="+", required=True, help="Label manifest JSON files")
    catalog_parser.add_argument("--json", help="Output JSON catalog")
    catalog_parser.add_argument("--md", help="Output Markdown catalog")

    split_parser = subparsers.add_parser("split-dataset", help="Split a labeled dataset into train and validation sets")
    split_parser.add_argument("--xlsx", required=True, help="Source xlsx file")
    split_parser.add_argument("--labels", nargs="+", required=True, help="Label manifest JSON files")
    split_parser.add_argument("--train-csv", help="Training CSV output")
    split_parser.add_argument("--train-jsonl", help="Training JSONL output")
    split_parser.add_argument("--validation-csv", help="Validation CSV output")
    split_parser.add_argument("--validation-jsonl", help="Validation JSONL output")
    split_parser.add_argument("--ratio", type=float, default=0.8, help="Training split ratio")
    split_parser.add_argument("--seed", type=int, default=42, help="Random seed")

    train_parser = subparsers.add_parser("train-baseline", help="Train a lightweight baseline classifier")
    train_parser.add_argument("--xlsx", required=True, help="Source xlsx file")
    train_parser.add_argument("--labels", nargs="*", default=[], help="Label manifest JSON files")
    train_parser.add_argument("--annotations", help="Simplified annotation file (.csv or .jsonl)")
    train_parser.add_argument("--roles", help="Optional role annotation file (.csv or .jsonl)")
    train_parser.add_argument("--owners", help="Optional owner annotation file (.csv or .jsonl)")
    train_parser.add_argument("--model", required=True, help="Output model JSON file")
    train_parser.add_argument("--metrics", required=True, help="Output metrics JSON file")
    train_parser.add_argument("--split-ratio", type=float, default=0.8, help="Training split ratio")
    train_parser.add_argument("--seed", type=int, default=42, help="Random seed")

    triage_parser = subparsers.add_parser("triage-workbooks", help="Score workbook files in a directory")
    triage_parser.add_argument("--root", required=True, help="Directory containing xlsx files")
    triage_parser.add_argument("--labels", nargs="+", required=True, help="Label manifest JSON files")
    triage_parser.add_argument("--json", help="Output JSON triage file")
    triage_parser.add_argument("--md", help="Output Markdown triage file")

    graph_parser = subparsers.add_parser("graph-triage", help="Rank workbook rows with graph propagation")
    graph_parser.add_argument("--root", required=True, help="Directory containing xlsx files")
    graph_parser.add_argument("--labels", nargs="+", required=True, help="Label manifest JSON files")
    graph_parser.add_argument("--json", help="Output JSON graph triage file")
    graph_parser.add_argument("--md", help="Output Markdown graph triage file")
    graph_parser.add_argument("--top-k", type=int, default=100, help="Number of top rows to keep")
    graph_parser.add_argument("--threshold", type=float, default=0.7, help="Optional display threshold")
    graph_parser.add_argument("--synthetic-warmup", type=int, default=0, help="Number of synthetic seller/buyer pairs for warmup")
    graph_parser.add_argument("--self-train-rounds", type=int, default=0, help="Number of self-training rounds on pseudo labels")
    graph_parser.add_argument("--pseudo-positive-threshold", type=float, default=0.9, help="Pseudo-label positive threshold")
    graph_parser.add_argument("--pseudo-negative-threshold", type=float, default=0.1, help="Pseudo-label negative threshold")
    graph_parser.add_argument("--pseudo-max-rows", type=int, default=500, help="Max pseudo-labeled rows per run")

    normalize_parser = subparsers.add_parser("normalize-ledgers", help="Normalize xlsx ledgers into a review-friendly table")
    normalize_parser.add_argument("--root", required=True, help="Directory containing xlsx files")
    normalize_parser.add_argument("--labels", nargs="*", default=[], help="Optional label manifest JSON files")
    normalize_parser.add_argument("--roles", help="Optional role annotation file (.csv or .jsonl)")
    normalize_parser.add_argument("--owners", help="Optional owner annotation file (.csv or .jsonl)")
    normalize_parser.add_argument("--mirror-annotations", help="Optional mirror annotation file (.csv or .jsonl)")
    normalize_parser.add_argument("--csv", help="Output CSV file")
    normalize_parser.add_argument("--jsonl", help="Output JSONL file")

    ledger_review_parser = subparsers.add_parser("export-ledger-review", help="Export a minimal manual-review ledger table from normalized rows")
    ledger_review_parser.add_argument("--normalized", required=True, help="Normalized ledger CSV or JSONL file")
    ledger_review_parser.add_argument("--csv", help="Output review CSV file")
    ledger_review_parser.add_argument("--xlsx", help="Output review XLSX file")

    rule_audit_parser = subparsers.add_parser("export-rule-audit", help="Export rule-hit audit rows from normalized ledgers")
    rule_audit_parser.add_argument("--normalized", required=True, help="Normalized ledger CSV or JSONL file")
    rule_audit_parser.add_argument("--csv", help="Output audit CSV file")
    rule_audit_parser.add_argument("--xlsx", help="Output audit XLSX file")
    rule_audit_parser.add_argument("--all-rows", action="store_true", help="Include rows without explicit rule hits")

    rule_summary_parser = subparsers.add_parser("build-rule-summary", help="Summarize rule-hit distribution from normalized ledgers")
    rule_summary_parser.add_argument("--normalized", required=True, help="Normalized ledger CSV or JSONL file")
    rule_summary_parser.add_argument("--json", help="Output JSON summary file")
    rule_summary_parser.add_argument("--md", help="Output Markdown summary file")

    rule_review_summary_parser = subparsers.add_parser("build-rule-review-summary", help="Summarize rule hit quality against reviewed ledger labels")
    rule_review_summary_parser.add_argument("--normalized", required=True, help="Normalized ledger CSV or JSONL file")
    rule_review_summary_parser.add_argument("--reviews", required=True, help="Ledger review CSV or XLSX file")
    rule_review_summary_parser.add_argument("--json", help="Output JSON summary file")
    rule_review_summary_parser.add_argument("--md", help="Output Markdown summary file")

    graph_dataset_parser = subparsers.add_parser("build-graph-dataset", help="Build a graph dataset summary for workbook directories")
    graph_dataset_parser.add_argument("--root", required=True, help="Directory containing xlsx files")
    graph_dataset_parser.add_argument("--labels", nargs="*", default=[], help="Optional label manifest JSON files")
    graph_dataset_parser.add_argument("--roles", help="Optional role annotation file (.csv or .jsonl)")
    graph_dataset_parser.add_argument("--owners", help="Optional owner annotation file (.csv or .jsonl)")
    graph_dataset_parser.add_argument("--mirror-annotations", help="Optional mirror annotation file (.csv or .jsonl)")
    graph_dataset_parser.add_argument("--json", required=True, help="Output JSON summary file")

    owner_summary_parser = subparsers.add_parser("build-owner-summary", help="Build an owner-level activity summary for workbook directories")
    owner_summary_parser.add_argument("--root", required=True, help="Directory containing xlsx files")
    owner_summary_parser.add_argument("--labels", nargs="*", default=[], help="Optional label manifest JSON files")
    owner_summary_parser.add_argument("--roles", help="Optional role annotation file (.csv or .jsonl)")
    owner_summary_parser.add_argument("--owners", help="Optional owner annotation file (.csv or .jsonl)")
    owner_summary_parser.add_argument("--mirror-annotations", help="Optional mirror annotation file (.csv or .jsonl)")
    owner_summary_parser.add_argument("--csv", help="Output CSV summary file")
    owner_summary_parser.add_argument("--json", help="Output JSON summary file")

    owner_review_parser = subparsers.add_parser("export-owner-review", help="Export owner summaries as a manual review template")
    owner_review_parser.add_argument("--summary", required=True, help="Owner summary JSON file produced by build-owner-summary")
    owner_review_parser.add_argument("--csv", help="Output owner review CSV file")
    owner_review_parser.add_argument("--xlsx", help="Output owner review XLSX file")

    mirror_review_parser = subparsers.add_parser("export-mirror-review", help="Export mirrored transaction candidates as a manual review template")
    mirror_review_parser.add_argument("--normalized", required=True, help="Normalized ledger CSV or JSONL file")
    mirror_review_parser.add_argument("--csv", help="Output mirror review CSV file")
    mirror_review_parser.add_argument("--xlsx", help="Output mirror review XLSX file")
    mirror_review_parser.add_argument("--confirmed", action="store_true", help="Only export confirmed mirrored rows")
    mirror_review_parser.add_argument("--possible", action="store_true", help="Only export possible mirrored rows")

    import_owner_review_parser = subparsers.add_parser("import-owner-review", help="Convert owner review results into role annotations")
    import_owner_review_parser.add_argument("--reviews", required=True, help="Reviewed owner CSV or XLSX file")
    import_owner_review_parser.add_argument("--roles-csv", help="Output role annotations CSV path")
    import_owner_review_parser.add_argument("--roles-jsonl", help="Output role annotations JSONL path")
    import_owner_review_parser.add_argument("--scene", default="owner_review", help="Scene name for imported owner roles")
    import_owner_review_parser.add_argument("--evidence", default="owner_manual_review", help="Evidence value for imported owner roles")

    import_mirror_review_parser = subparsers.add_parser("import-mirror-review", help="Convert mirror review results into mirror annotations")
    import_mirror_review_parser.add_argument("--reviews", required=True, help="Reviewed mirror CSV or XLSX file")
    import_mirror_review_parser.add_argument("--csv", help="Output mirror annotations CSV path")
    import_mirror_review_parser.add_argument("--jsonl", help="Output mirror annotations JSONL path")

    train_gnn_parser = subparsers.add_parser("train-gnn", help="Train the graph model on workbook directories")
    train_gnn_parser.add_argument("--root", required=True, help="Directory containing xlsx files")
    train_gnn_parser.add_argument("--labels", nargs="*", default=[], help="Label manifest JSON files")
    train_gnn_parser.add_argument("--annotations", help="Simplified annotation file (.csv or .jsonl)")
    train_gnn_parser.add_argument("--roles", help="Optional role annotation file (.csv or .jsonl)")
    train_gnn_parser.add_argument("--owners", help="Optional owner annotation file (.csv or .jsonl)")
    train_gnn_parser.add_argument("--model", required=True, help="Output torch model file")
    train_gnn_parser.add_argument("--metrics", required=True, help="Output metrics JSON file")
    train_gnn_parser.add_argument("--metadata", help="Optional metadata JSON file")
    train_gnn_parser.add_argument("--hidden-dim", type=int, default=64, help="Hidden dimension")
    train_gnn_parser.add_argument("--dropout", type=float, default=0.25, help="Dropout ratio")
    train_gnn_parser.add_argument("--epochs", type=int, default=120, help="Training epochs")
    train_gnn_parser.add_argument("--seed", type=int, default=42, help="Random seed")
    train_gnn_parser.add_argument("--split-ratio", type=float, default=0.8, help="Training split ratio")
    train_gnn_parser.add_argument("--synthetic-warmup", type=int, default=0, help="Synthetic warmup pairs")
    train_gnn_parser.add_argument("--self-train-rounds", type=int, default=0, help="Pseudo-label self-training rounds")
    train_gnn_parser.add_argument("--pseudo-positive-threshold", type=float, default=0.9, help="Pseudo-label positive threshold")
    train_gnn_parser.add_argument("--pseudo-negative-threshold", type=float, default=0.1, help="Pseudo-label negative threshold")
    train_gnn_parser.add_argument("--pseudo-max-rows", type=int, default=500, help="Max pseudo-labeled rows per run")

    score_gnn_parser = subparsers.add_parser("score-gnn", help="Run a trained graph model on workbook directories")
    score_gnn_parser.add_argument("--root", required=True, help="Directory containing xlsx files")
    score_gnn_parser.add_argument("--model", required=True, help="Saved torch model file")
    score_gnn_parser.add_argument("--labels", nargs="*", default=[], help="Optional label manifest JSON files")
    score_gnn_parser.add_argument("--annotations", help="Simplified annotation file (.csv or .jsonl)")
    score_gnn_parser.add_argument("--roles", help="Optional role annotation file (.csv or .jsonl)")
    score_gnn_parser.add_argument("--owners", help="Optional owner annotation file (.csv or .jsonl)")
    score_gnn_parser.add_argument("--json", help="Output JSON score file")
    score_gnn_parser.add_argument("--md", help="Output Markdown score file")
    score_gnn_parser.add_argument("--top-k", type=int, default=100, help="Number of top rows to keep")
    score_gnn_parser.add_argument("--include-labeled", action="store_true", help="Include labeled rows in top results")

    review_parser = subparsers.add_parser("export-review-candidates", help="Export score results for manual review")
    review_parser.add_argument("--scores", required=True, help="Score JSON file produced by score-gnn")
    review_parser.add_argument("--csv", help="Output review CSV file")
    review_parser.add_argument("--xlsx", help="Output review XLSX file with yellow high-risk fills")
    review_parser.add_argument("--md", help="Output review Markdown file")
    review_parser.add_argument("--threshold", type=float, default=0.7, help="Minimum score threshold")
    review_parser.add_argument("--limit", type=int, default=100, help="Max review candidates")
    review_parser.add_argument(
        "--entity-type",
        choices=["auto", "transaction", "seller_account"],
        default="transaction",
        help="Export transaction rows or aggregated seller-account candidates",
    )

    import_review_parser = subparsers.add_parser("import-review-labels", help="Convert manual review CSV or XLSX results into label manifests")
    import_review_parser.add_argument("--reviews", required=True, help="Reviewed CSV or XLSX file")
    import_review_parser.add_argument("--dataset-prefix", required=True, help="Prefix for generated dataset names")
    import_review_parser.add_argument("--positive-json", help="Output positive manifest JSON path")
    import_review_parser.add_argument("--negative-json", help="Output negative manifest JSON path")
    import_review_parser.add_argument("--annotations-csv", help="Output simplified annotations CSV path")
    import_review_parser.add_argument("--annotations-jsonl", help="Output simplified annotations JSONL path")
    import_review_parser.add_argument("--subject", default="reviewed_batch", help="Subject name for generated manifests")
    import_review_parser.add_argument("--verified-by", default="user", help="Verifier name")
    import_review_parser.add_argument("--source-file", default="", help="Optional shared source file for generated manifests")

    merge_parser = subparsers.add_parser("merge-label-manifests", help="Merge multiple manifests into positive and negative training sets")
    merge_parser.add_argument("--labels", nargs="+", required=True, help="Input manifest JSON files")
    merge_parser.add_argument("--dataset-prefix", required=True, help="Prefix for merged dataset names")
    merge_parser.add_argument("--positive-json", required=True, help="Output positive merged manifest")
    merge_parser.add_argument("--negative-json", required=True, help="Output negative merged manifest")
    merge_parser.add_argument("--subject", default="merged_batch", help="Subject name for merged manifests")
    merge_parser.add_argument("--verified-by", default="user", help="Verifier name")

    compare_parser = subparsers.add_parser("compare-round-metrics", help="Compare multiple training rounds and review outcomes")
    compare_parser.add_argument(
        "--round",
        dest="rounds",
        action="append",
        required=True,
        help="Round spec in the form round_name:metrics_json[:review_csv]",
    )
    compare_parser.add_argument("--json", help="Output JSON comparison file")
    compare_parser.add_argument("--md", help="Output Markdown comparison file")

    round_report_parser = subparsers.add_parser("make-round-report", help="Create a single-round report from training artifacts")
    round_report_parser.add_argument("--round-name", required=True, help="Round name")
    round_report_parser.add_argument("--metrics", required=True, help="Metrics JSON file")
    round_report_parser.add_argument("--scores", help="Score JSON file")
    round_report_parser.add_argument("--reviews", help="Review CSV file")
    round_report_parser.add_argument("--labels", nargs="*", default=[], help="Optional label manifest JSON files")
    round_report_parser.add_argument("--json", help="Output JSON report")
    round_report_parser.add_argument("--md", help="Output Markdown report")

    bootstrap_parser = subparsers.add_parser("bootstrap-round", help="Create a standard output workspace for one training round")
    bootstrap_parser.add_argument("--round-name", required=True, help="Round name")
    bootstrap_parser.add_argument("--base-dir", default="out", help="Base output directory")
    bootstrap_parser.add_argument("--train-root", default="/path/to/train_workbooks", help="Training workbook root placeholder")
    bootstrap_parser.add_argument("--score-root", default="/path/to/score_workbooks", help="Scoring workbook root placeholder")
    bootstrap_parser.add_argument("--label-glob", default="data/labels/*.json", help="Label glob placeholder")
    bootstrap_parser.add_argument("--json", help="Output JSON bootstrap file")
    bootstrap_parser.add_argument("--md", help="Output Markdown bootstrap file")

    sweep_parser = subparsers.add_parser("score-threshold-sweep", help="Compare candidate volume and review outcomes across score thresholds")
    sweep_parser.add_argument("--scores", required=True, help="Score JSON file")
    sweep_parser.add_argument("--reviews", help="Optional review CSV file")
    sweep_parser.add_argument("--threshold", dest="thresholds", action="append", type=float, help="Threshold to evaluate; may be repeated")
    sweep_parser.add_argument("--json", help="Output JSON sweep file")
    sweep_parser.add_argument("--md", help="Output Markdown sweep file")

    workload_parser = subparsers.add_parser("review-workload-forecast", help="Estimate review workload from score thresholds")
    workload_parser.add_argument("--scores", required=True, help="Score JSON file")
    workload_parser.add_argument("--reviews", help="Optional review CSV file")
    workload_parser.add_argument("--threshold", dest="thresholds", action="append", type=float, help="Threshold to evaluate; may be repeated")
    workload_parser.add_argument("--reviewers", type=int, default=1, help="Number of reviewers")
    workload_parser.add_argument("--daily-capacity", type=int, default=50, help="Items per reviewer per day")
    workload_parser.add_argument("--json", help="Output JSON forecast")
    workload_parser.add_argument("--md", help="Output Markdown forecast")

    threshold_select_parser = subparsers.add_parser("select-operating-threshold", help="Recommend a working score threshold under workload constraints")
    threshold_select_parser.add_argument("--scores", required=True, help="Score JSON file")
    threshold_select_parser.add_argument("--reviews", help="Optional review CSV file")
    threshold_select_parser.add_argument("--threshold", dest="thresholds", action="append", type=float, help="Threshold to evaluate; may be repeated")
    threshold_select_parser.add_argument("--reviewers", type=int, default=1, help="Number of reviewers")
    threshold_select_parser.add_argument("--daily-capacity", type=int, default=50, help="Items per reviewer per day")
    threshold_select_parser.add_argument("--max-team-days", type=float, help="Maximum acceptable team review days")
    threshold_select_parser.add_argument("--min-confirmed-positive-rate", type=float, default=0.0, help="Minimum acceptable confirmed positive rate")
    threshold_select_parser.add_argument("--min-candidates", type=int, default=1, help="Minimum acceptable candidate count")
    threshold_select_parser.add_argument("--json", help="Output JSON recommendation")
    threshold_select_parser.add_argument("--md", help="Output Markdown recommendation")

    decision_parser = subparsers.add_parser("round-decision-sheet", help="Build a one-page decision summary for a round")
    decision_parser.add_argument("--round-name", required=True, help="Round name")
    decision_parser.add_argument("--metrics", required=True, help="Metrics JSON file")
    decision_parser.add_argument("--scores", required=True, help="Score JSON file")
    decision_parser.add_argument("--reviews", help="Optional review CSV file")
    decision_parser.add_argument("--labels", nargs="*", default=[], help="Optional label manifest JSON files")
    decision_parser.add_argument("--threshold", dest="thresholds", action="append", type=float, help="Threshold to evaluate; may be repeated")
    decision_parser.add_argument("--reviewers", type=int, default=1, help="Number of reviewers")
    decision_parser.add_argument("--daily-capacity", type=int, default=50, help="Items per reviewer per day")
    decision_parser.add_argument("--max-team-days", type=float, help="Maximum acceptable team review days")
    decision_parser.add_argument("--min-confirmed-positive-rate", type=float, default=0.0, help="Minimum acceptable confirmed positive rate")
    decision_parser.add_argument("--min-candidates", type=int, default=1, help="Minimum acceptable candidate count")
    decision_parser.add_argument("--json", help="Output JSON decision sheet")
    decision_parser.add_argument("--md", help="Output Markdown decision sheet")

    return parser


def run_analyze(args: argparse.Namespace) -> str:
    _existing_file(args.input, "input file") if Path(args.input).is_file() else _existing_directory(args.input, "input directory")
    result = analyze_transactions_from_path(Path(args.input), ocr_command=args.ocr_command, account_name=args.account_name)
    if args.format == "json":
        return render_json_report(result)
    return render_markdown_report(result)


def run_extract_payment_pdf(args: argparse.Namespace) -> str:
    _existing_file(args.input, "input file") if Path(args.input).is_file() else _existing_directory(args.input, "input directory")
    if not any([args.csv, args.jsonl, args.annotations_csv, args.annotations_jsonl]):
        raise ValueError("provide at least one output path for --csv, --jsonl, --annotations-csv, or --annotations-jsonl")
    rows = load_wechat_pdf_rows_from_path(args.input, ocr_command=args.ocr_command, account_name=args.account_name)
    if args.csv:
        export_wechat_pdf_rows_csv(rows, args.csv)
    if args.jsonl:
        export_wechat_pdf_rows_jsonl(rows, args.jsonl)
    if args.annotations_csv or args.annotations_jsonl:
        note = args.note.strip() or "imported from payment pdf"
        annotations = [AnnotationRow(transaction_id=row.record_id, label_status=args.label, note=note) for row in rows]
        if args.annotations_csv:
            export_annotations_csv(annotations, args.annotations_csv)
        if args.annotations_jsonl:
            export_annotations_jsonl(annotations, args.annotations_jsonl)
    return f"extracted {len(rows)} payment-pdf rows"


def run_export_training(args: argparse.Namespace) -> str:
    _existing_file(args.xlsx, "xlsx file")
    manifests = load_label_manifests(args.labels)
    samples = build_positive_training_samples(args.xlsx, manifests)
    if args.csv:
        export_training_samples_csv(samples, args.csv)
    if args.jsonl:
        export_training_samples_jsonl(samples, args.jsonl)
    return f"exported {len(samples)} labeled samples"


def run_export_dataset(args: argparse.Namespace) -> str:
    _existing_file(args.xlsx, "xlsx file")
    manifests = load_label_manifests(args.labels)
    examples = build_training_examples(args.xlsx, manifests)
    if args.csv:
        export_training_examples_csv(examples, args.csv)
    if args.jsonl:
        export_training_examples_jsonl(examples, args.jsonl)
    labeled = sum(1 for item in examples if item.label_status != "unlabeled")
    return f"exported {len(examples)} rows ({labeled} labeled)"


def run_label_catalog(args: argparse.Namespace) -> str:
    manifests = load_label_manifests(args.labels)
    payload = None
    if args.json:
        export_label_catalog_json(manifests, args.json)
    if args.md:
        export_label_catalog_markdown(manifests, args.md)
    if args.json or args.md:
        payload = f"cataloged {len(manifests)} manifests"
    return payload or f"cataloged {len(manifests)} manifests"


def run_split_dataset(args: argparse.Namespace) -> str:
    _existing_file(args.xlsx, "xlsx file")
    _ratio_between_zero_and_one(args.ratio, "ratio")
    manifests = load_label_manifests(args.labels)
    examples = build_training_examples(args.xlsx, manifests)
    splits = split_training_examples(examples, train_ratio=args.ratio, seed=args.seed)
    split_map = {split.name: split for split in splits}
    train_split = split_map["train"]
    validation_split = split_map["validation"]
    if args.train_csv:
        export_split_csv(train_split, args.train_csv)
    if args.train_jsonl:
        export_split_jsonl(train_split, args.train_jsonl)
    if args.validation_csv:
        export_split_csv(validation_split, args.validation_csv)
    if args.validation_jsonl:
        export_split_jsonl(validation_split, args.validation_jsonl)
    return f"split {len(examples)} rows into {len(train_split.examples)} train and {len(validation_split.examples)} validation"


def run_train_baseline(args: argparse.Namespace) -> str:
    _existing_file(args.xlsx, "xlsx file")
    _ratio_between_zero_and_one(args.split_ratio, "split-ratio")
    manifests = _resolve_manifests(args.labels, args.annotations)
    examples = build_training_examples(
        args.xlsx,
        manifests,
        role_annotation_path=args.roles,
        owner_annotation_path=args.owners,
    )
    splits = split_training_examples(examples, train_ratio=args.split_ratio, seed=args.seed)
    split_map = {split.name: split for split in splits}
    train_examples = [item for item in split_map["train"].examples if item.label_status in {"positive", "negative"}]
    validation_examples = [item for item in split_map["validation"].examples if item.label_status in {"positive", "negative"}]
    model = train_baseline_classifier(train_examples)
    metrics = {
        "source": {
            "xlsx": str(Path(args.xlsx)),
            "label_manifest_count": len(manifests),
        },
        "config": {
            "split_ratio": args.split_ratio,
            "seed": args.seed,
        },
        "dataset": {
            "total_rows": len(examples),
            "labeled_rows": sum(1 for item in examples if item.label_status in {"positive", "negative"}),
            "unlabeled_rows": sum(1 for item in examples if item.label_status == "unlabeled"),
        },
        "train_size": len(train_examples),
        "validation_size": len(validation_examples),
        "train_label_counts": {label: sum(1 for item in train_examples if item.label_status == label) for label in {"positive", "negative"}},
        "validation_label_counts": {label: sum(1 for item in validation_examples if item.label_status == label) for label in {"positive", "negative"}},
        "model": model.to_dict(),
        "evaluation": model.evaluate(validation_examples),
    }
    next_steps: dict[str, list[str]] = {"data": [], "model": [], "review_ops": []}
    recommendations: list[str] = []
    if metrics["dataset"]["labeled_rows"] < 20:
        item = "Collect more labeled rows before trusting the baseline classifier on new ledgers."
        recommendations.append(item)
        next_steps["data"].append(item)
    if metrics["evaluation"]["f1"] < 0.6:
        item = "Review feature fields and label consistency before the next baseline run."
        recommendations.append(item)
        next_steps["model"].append(item)
    else:
        item = "Use the baseline as a quick triage reference before graph-based scoring."
        recommendations.append(item)
        next_steps["review_ops"].append(item)
    metrics["recommendations"] = recommendations
    metrics["next_steps"] = {key: value for key, value in next_steps.items() if value}
    model.save(args.model)
    Path(args.metrics).parent.mkdir(parents=True, exist_ok=True)
    Path(args.metrics).write_text(json.dumps(metrics, ensure_ascii=False, indent=2), encoding="utf-8")
    return f"trained baseline model on {len(train_examples)} rows; validation={len(validation_examples)}"


def run_triage_workbooks(args: argparse.Namespace) -> str:
    _existing_directory(args.root, "root directory")
    manifests = load_label_manifests(args.labels)
    results = scan_workbook_directory(args.root, manifests)
    if args.json:
        export_triage_json(results, args.json)
    if args.md:
        export_triage_markdown(results, args.md)
    return f"triaged {len(results)} workbooks"


def run_graph_triage(args: argparse.Namespace) -> str:
    _existing_directory(args.root, "root directory")
    _positive_int(args.top_k, "top-k")
    _probability(args.threshold, "threshold")
    _probability(args.pseudo_positive_threshold, "pseudo-positive-threshold")
    _probability(args.pseudo_negative_threshold, "pseudo-negative-threshold")
    manifests = load_label_manifests(args.labels)
    report = score_directory(
        args.root,
        manifests,
        top_k=args.top_k,
        synthetic_warmup=args.synthetic_warmup,
        self_training_rounds=args.self_train_rounds,
        pseudo_positive_threshold=args.pseudo_positive_threshold,
        pseudo_negative_threshold=args.pseudo_negative_threshold,
        pseudo_max_rows=args.pseudo_max_rows,
    )
    if args.json:
        export_graph_triage_json(report, args.json)
    if args.md:
        export_graph_triage_markdown(report, args.md)
    return f"graph triaged {report.total_workbooks} workbooks; top_rows={len(report.top_rows)}"


def run_normalize_ledgers(args: argparse.Namespace) -> str:
    _existing_directory(args.root, "root directory")
    if args.mirror_annotations:
        _existing_file(args.mirror_annotations, "mirror annotation file")
    manifests = load_label_manifests(args.labels)
    rows = export_normalized_ledgers(
        args.root,
        manifests,
        role_annotation_path=args.roles,
        owner_annotation_path=args.owners,
        mirror_annotation_path=args.mirror_annotations,
        csv_path=args.csv,
        jsonl_path=args.jsonl,
    )
    return f"normalized {len(rows)} rows"


def run_build_graph_dataset(args: argparse.Namespace) -> str:
    _existing_directory(args.root, "root directory")
    if args.mirror_annotations:
        _existing_file(args.mirror_annotations, "mirror annotation file")
    manifests = load_label_manifests(args.labels)
    summary = summarize_graph_dataset(
        args.root,
        manifests,
        role_annotation_path=args.roles,
        owner_annotation_path=args.owners,
        mirror_annotation_path=args.mirror_annotations,
    )
    export_graph_dataset_summary(summary, args.json)
    return f"summarized {summary.total_rows} rows across {summary.total_workbooks} workbooks"


def run_export_ledger_review(args: argparse.Namespace) -> str:
    _existing_file(args.normalized, "normalized ledger file")
    if not any([args.csv, args.xlsx]):
        raise ValueError("provide at least one output path for --csv or --xlsx")
    rows = build_ledger_review_rows(load_normalized_ledgers(args.normalized))
    if args.csv:
        export_ledger_review_csv(rows, args.csv)
    if args.xlsx:
        export_ledger_review_xlsx(rows, args.xlsx)
    return f"exported {len(rows)} ledger review rows"


def run_export_rule_audit(args: argparse.Namespace) -> str:
    _existing_file(args.normalized, "normalized ledger file")
    if not any([args.csv, args.xlsx]):
        raise ValueError("provide at least one output path for --csv or --xlsx")
    rows = build_rule_audit_rows(load_normalized_ledgers(args.normalized), include_all=args.all_rows)
    if args.csv:
        export_rule_audit_csv(rows, args.csv)
    if args.xlsx:
        export_rule_audit_xlsx(rows, args.xlsx)
    return f"exported {len(rows)} rule audit rows"


def run_build_rule_summary(args: argparse.Namespace) -> str:
    _existing_file(args.normalized, "normalized ledger file")
    if not any([args.json, args.md]):
        raise ValueError("provide at least one output path for --json or --md")
    summary = build_rule_summary(load_normalized_ledgers(args.normalized))
    if args.json:
        export_rule_summary_json(summary, args.json)
    if args.md:
        export_rule_summary_markdown(summary, args.md)
    return f"summarized {summary.rows_with_rule_hits} rule-hit rows out of {summary.total_rows}"


def run_build_rule_review_summary(args: argparse.Namespace) -> str:
    _existing_file(args.normalized, "normalized ledger file")
    _existing_file(args.reviews, "reviews file")
    if not any([args.json, args.md]):
        raise ValueError("provide at least one output path for --json or --md")
    summary = build_rule_review_summary(load_normalized_ledgers(args.normalized), args.reviews)
    if args.json:
        export_rule_review_summary_json(summary, args.json)
    if args.md:
        export_rule_review_summary_markdown(summary, args.md)
    return f"summarized {summary.matched_rows} reviewed rows against rule hits"


def run_build_owner_summary(args: argparse.Namespace) -> str:
    _existing_directory(args.root, "root directory")
    if args.mirror_annotations:
        _existing_file(args.mirror_annotations, "mirror annotation file")
    manifests = load_label_manifests(args.labels)
    if not any([args.csv, args.json]):
        raise ValueError("provide at least one output path for --csv or --json")
    summary = summarize_owner_activity(
        args.root,
        manifests,
        role_annotation_path=args.roles,
        owner_annotation_path=args.owners,
        mirror_annotation_path=args.mirror_annotations,
    )
    if args.csv:
        export_owner_summary_csv(summary, args.csv)
    if args.json:
        export_owner_summary_json(summary, args.json)
    return f"summarized {summary.covered_rows} owner-mapped rows across {summary.total_owners} owners"


def run_export_owner_review(args: argparse.Namespace) -> str:
    _existing_file(args.summary, "owner summary file")
    if not any([args.csv, args.xlsx]):
        raise ValueError("provide at least one output path for --csv or --xlsx")
    payload = json.loads(Path(args.summary).read_text(encoding="utf-8"))
    report = OwnerSummaryReport(
        total_rows=int(payload.get("total_rows", 0)),
        covered_rows=int(payload.get("covered_rows", 0)),
        skipped_rows=int(payload.get("skipped_rows", 0)),
        total_owners=int(payload.get("total_owners", 0)),
        owners=[
            OwnerSummaryRow(
                owner_id=str(item.get("owner_id", "")),
                owner_name=str(item.get("owner_name", "")),
                owner_confidence=str(item.get("owner_confidence", "")),
                dominant_role=str(item.get("dominant_role", "")),
                reviewed_role=str(item.get("reviewed_role", "")),
                reviewed_confidence=str(item.get("reviewed_confidence", "")),
                reviewed_note=str(item.get("reviewed_note", "")),
                role_counts={str(k): int(v) for k, v in dict(item.get("role_counts", {})).items()},
                pattern_tags=[str(tag) for tag in item.get("pattern_tags", [])],
                top_counterparties=[dict(entry) for entry in item.get("top_counterparties", [])],
                priority_score=float(item.get("priority_score", 0.0)),
                priority_rank=int(item.get("priority_rank", 0)),
                tx_count=int(item.get("tx_count", 0)),
                unique_counterparties=int(item.get("unique_counterparties", 0)),
                inflow_count=int(item.get("inflow_count", 0)),
                outflow_count=int(item.get("outflow_count", 0)),
                inflow_ratio=float(item.get("inflow_ratio", 0.0)),
                outflow_ratio=float(item.get("outflow_ratio", 0.0)),
                collect_and_split=bool(item.get("collect_and_split", False)),
                channel_count=int(item.get("channel_count", 0)),
                workbook_count=int(item.get("workbook_count", 0)),
                mirrored_rows=int(item.get("mirrored_rows", 0)),
                mirrored_groups=int(item.get("mirrored_groups", 0)),
                possible_mirrored_rows=int(item.get("possible_mirrored_rows", 0)),
                possible_mirrored_groups=int(item.get("possible_mirrored_groups", 0)),
                labeled_rows=int(item.get("labeled_rows", 0)),
                positive_rows=int(item.get("positive_rows", 0)),
                negative_rows=int(item.get("negative_rows", 0)),
                flagged_rows=int(item.get("flagged_rows", 0)),
            )
            for item in payload.get("owners", [])
        ],
    )
    rows = build_owner_review_rows(report)
    if args.csv:
        export_owner_review_csv(rows, args.csv)
    if args.xlsx:
        export_owner_review_xlsx(rows, args.xlsx)
    return f"exported {len(rows)} owner review rows"


def run_import_owner_review(args: argparse.Namespace) -> str:
    _existing_file(args.reviews, "owner review file")
    if not any([args.roles_csv, args.roles_jsonl]):
        raise ValueError("provide at least one output path for --roles-csv or --roles-jsonl")
    rows = build_owner_review_roles(args.reviews, scene=args.scene, evidence=args.evidence)
    if args.roles_csv:
        export_role_annotations_csv(rows, args.roles_csv)
    if args.roles_jsonl:
        export_role_annotations_jsonl(rows, args.roles_jsonl)
    return f"imported {len(rows)} owner review roles"


def run_export_mirror_review(args: argparse.Namespace) -> str:
    _existing_file(args.normalized, "normalized ledger file")
    if not any([args.csv, args.xlsx]):
        raise ValueError("provide at least one output path for --csv or --xlsx")
    rows = load_normalized_ledgers(args.normalized)
    include_confirmed = False
    include_possible = True
    if args.confirmed or args.possible:
        include_confirmed = args.confirmed
        include_possible = args.possible
    review_rows = build_mirror_review_rows(rows, include_confirmed=include_confirmed, include_possible=include_possible)
    if args.csv:
        export_mirror_review_csv(review_rows, args.csv)
    if args.xlsx:
        export_mirror_review_xlsx(review_rows, args.xlsx)
    return f"exported {len(review_rows)} mirror review rows"


def run_import_mirror_review(args: argparse.Namespace) -> str:
    _existing_file(args.reviews, "mirror review file")
    if not any([args.csv, args.jsonl]):
        raise ValueError("provide at least one output path for --csv or --jsonl")
    rows = build_mirror_annotations(args.reviews)
    if args.csv:
        export_mirror_annotations_csv(rows, args.csv)
    if args.jsonl:
        export_mirror_annotations_jsonl(rows, args.jsonl)
    return f"imported {len(rows)} mirror annotations"


def run_train_gnn(args: argparse.Namespace) -> str:
    _existing_directory(args.root, "root directory")
    _ratio_between_zero_and_one(args.split_ratio, "split-ratio")
    _probability(args.dropout, "dropout")
    _probability(args.pseudo_positive_threshold, "pseudo-positive-threshold")
    _probability(args.pseudo_negative_threshold, "pseudo-negative-threshold")
    _positive_int(args.epochs, "epochs")
    manifests = _resolve_manifests(args.labels, args.annotations)
    metrics = train_gnn_model(
        args.root,
        manifests,
        model_path=args.model,
        metrics_path=args.metrics,
        metadata_path=args.metadata,
        hidden_dim=args.hidden_dim,
        dropout=args.dropout,
        epochs=args.epochs,
        seed=args.seed,
        split_ratio=args.split_ratio,
        synthetic_warmup=args.synthetic_warmup,
        self_training_rounds=args.self_train_rounds,
        pseudo_positive_threshold=args.pseudo_positive_threshold,
        pseudo_negative_threshold=args.pseudo_negative_threshold,
        pseudo_max_rows=args.pseudo_max_rows,
        role_annotation_path=args.roles,
        owner_annotation_path=args.owners,
    )
    return f"trained gnn model; best_val_f1={float(metrics.get('best_val_f1', 0.0)):.4f}"


def run_score_gnn(args: argparse.Namespace) -> str:
    _existing_directory(args.root, "root directory")
    _existing_file(args.model, "model file")
    _positive_int(args.top_k, "top-k")
    manifests = _resolve_manifests(args.labels, args.annotations, require_input=False)
    report = score_gnn_directory(
        args.root,
        model_path=args.model,
        manifests=manifests,
        top_k=args.top_k,
        include_labeled=args.include_labeled,
        role_annotation_path=args.roles,
        owner_annotation_path=args.owners,
    )
    if args.json:
        export_gnn_score_json(report, args.json)
    if args.md:
        export_gnn_score_markdown(report, args.md)
    return f"scored {report.total_rows} rows; top_rows={len(report.top_rows)}; seller_candidates={len(report.seller_candidates)}"


def run_export_review_candidates(args: argparse.Namespace) -> str:
    _existing_file(args.scores, "scores file")
    _probability(args.threshold, "threshold")
    _positive_int(args.limit, "limit")
    rows = export_review_candidates(
        args.scores,
        csv_path=args.csv,
        xlsx_path=args.xlsx,
        md_path=args.md,
        threshold=args.threshold,
        limit=args.limit,
        entity_type=args.entity_type,
    )
    return f"exported {len(rows)} review candidates"


def run_import_review_labels(args: argparse.Namespace) -> str:
    _existing_file(args.reviews, "reviews file")
    if not any([args.positive_json, args.negative_json, args.annotations_csv, args.annotations_jsonl]):
        raise ValueError("provide at least one output path for manifests or annotations")
    positive = build_review_manifest(
        args.reviews,
        polarity="positive",
        dataset_name=f"{args.dataset_prefix}_positive",
        verified_by=args.verified_by,
        subject=args.subject,
        source_file=args.source_file,
    )
    negative = build_review_manifest(
        args.reviews,
        polarity="negative",
        dataset_name=f"{args.dataset_prefix}_negative",
        verified_by=args.verified_by,
        subject=args.subject,
        source_file=args.source_file,
    )
    if args.positive_json:
        export_label_manifest(positive, args.positive_json)
    if args.negative_json:
        export_label_manifest(negative, args.negative_json)
    annotation_rows = build_review_annotations(args.reviews)
    if args.annotations_csv:
        export_annotations_csv(annotation_rows, args.annotations_csv)
    if args.annotations_jsonl:
        export_annotations_jsonl(annotation_rows, args.annotations_jsonl)
    return (
        f"imported {len(positive.transaction_ids)} positive and {len(negative.transaction_ids)} negative reviewed labels"
        f"; annotations={len(annotation_rows)}"
    )


def run_merge_label_manifests(args: argparse.Namespace) -> str:
    manifests = load_label_manifests(args.labels)
    positive = merge_label_manifests(
        manifests,
        dataset_name=f"{args.dataset_prefix}_positive",
        polarity="positive",
        verified_by=args.verified_by,
        subject=args.subject,
    )
    negative = merge_label_manifests(
        manifests,
        dataset_name=f"{args.dataset_prefix}_negative",
        polarity="negative",
        verified_by=args.verified_by,
        subject=args.subject,
    )
    export_label_manifest(positive, args.positive_json)
    export_label_manifest(negative, args.negative_json)
    return f"merged {len(positive.transaction_ids)} positive and {len(negative.transaction_ids)} negative labels"


def run_compare_round_metrics(args: argparse.Namespace) -> str:
    round_specs: list[dict[str, str]] = []
    for item in args.rounds:
        parts = item.split(":")
        if len(parts) < 2 or len(parts) > 3:
            raise ValueError(f"invalid round spec: {item}")
        _existing_file(parts[1], "metrics file")
        spec = {"round_name": parts[0], "metrics": parts[1]}
        if len(parts) == 3 and parts[2]:
            _existing_file(parts[2], "review file")
            spec["reviews"] = parts[2]
        round_specs.append(spec)
    report = compare_round_metrics(round_specs)
    if args.json:
        export_round_comparison_json(report, args.json)
    if args.md:
        export_round_comparison_markdown(report, args.md)
    return f"compared {len(report.rounds)} rounds"


def run_make_round_report(args: argparse.Namespace) -> str:
    _existing_file(args.metrics, "metrics file")
    if args.scores:
        _existing_file(args.scores, "scores file")
    if args.reviews:
        _existing_file(args.reviews, "reviews file")
    report = build_round_report(
        round_name=args.round_name,
        metrics_json_path=args.metrics,
        score_json_path=args.scores,
        review_csv_path=args.reviews,
        label_json_paths=args.labels,
    )
    if args.json:
        export_round_report_json(report, args.json)
    if args.md:
        export_round_report_markdown(report, args.md)
    return f"built round report for {args.round_name}"


def run_bootstrap_round(args: argparse.Namespace) -> str:
    bootstrap = bootstrap_round(
        round_name=args.round_name,
        base_dir=args.base_dir,
        train_root=args.train_root,
        score_root=args.score_root,
        label_glob=args.label_glob,
    )
    if args.json:
        export_round_bootstrap_json(bootstrap, args.json)
    if args.md:
        export_round_bootstrap_markdown(bootstrap, args.md)
    return f"bootstrapped round workspace at {bootstrap.output_dir}"


def run_score_threshold_sweep(args: argparse.Namespace) -> str:
    _existing_file(args.scores, "scores file")
    if args.reviews:
        _existing_file(args.reviews, "reviews file")
    for threshold in args.thresholds or []:
        _probability(threshold, "threshold")
    report = score_threshold_sweep(
        score_json_path=args.scores,
        review_csv_path=args.reviews,
        thresholds=args.thresholds,
    )
    if args.json:
        export_threshold_sweep_json(report, args.json)
    if args.md:
        export_threshold_sweep_markdown(report, args.md)
    return f"swept {len(report.rows)} thresholds"


def run_review_workload_forecast(args: argparse.Namespace) -> str:
    _existing_file(args.scores, "scores file")
    if args.reviews:
        _existing_file(args.reviews, "reviews file")
    _positive_int(args.reviewers, "reviewers")
    _positive_int(args.daily_capacity, "daily-capacity")
    for threshold in args.thresholds or []:
        _probability(threshold, "threshold")
    sweep = score_threshold_sweep(
        score_json_path=args.scores,
        review_csv_path=args.reviews,
        thresholds=args.thresholds,
    )
    report = review_workload_forecast(
        sweep,
        reviewers=args.reviewers,
        daily_capacity_per_reviewer=args.daily_capacity,
    )
    if args.json:
        export_review_workload_json(report, args.json)
    if args.md:
        export_review_workload_markdown(report, args.md)
    return f"forecasted workload for {len(report.rows)} thresholds"


def run_select_operating_threshold(args: argparse.Namespace) -> str:
    _existing_file(args.scores, "scores file")
    if args.reviews:
        _existing_file(args.reviews, "reviews file")
    _positive_int(args.reviewers, "reviewers")
    _positive_int(args.daily_capacity, "daily-capacity")
    _probability(args.min_confirmed_positive_rate, "min-confirmed-positive-rate")
    _positive_int(args.min_candidates, "min-candidates")
    for threshold in args.thresholds or []:
        _probability(threshold, "threshold")
    sweep = score_threshold_sweep(
        score_json_path=args.scores,
        review_csv_path=args.reviews,
        thresholds=args.thresholds,
    )
    workload = review_workload_forecast(
        sweep,
        reviewers=args.reviewers,
        daily_capacity_per_reviewer=args.daily_capacity,
    )
    report = select_operating_threshold(
        workload,
        max_team_days=args.max_team_days,
        min_confirmed_positive_rate=args.min_confirmed_positive_rate,
        min_candidates=args.min_candidates,
    )
    if args.json:
        export_operating_threshold_json(report, args.json)
    if args.md:
        export_operating_threshold_markdown(report, args.md)
    return f"selected threshold {report.recommended_threshold:.4f}"


def run_round_decision_sheet(args: argparse.Namespace) -> str:
    _existing_file(args.metrics, "metrics file")
    _existing_file(args.scores, "scores file")
    if args.reviews:
        _existing_file(args.reviews, "reviews file")
    _positive_int(args.reviewers, "reviewers")
    _positive_int(args.daily_capacity, "daily-capacity")
    _probability(args.min_confirmed_positive_rate, "min-confirmed-positive-rate")
    _positive_int(args.min_candidates, "min-candidates")
    for threshold in args.thresholds or []:
        _probability(threshold, "threshold")
    report = build_round_decision_sheet(
        round_name=args.round_name,
        metrics_json_path=args.metrics,
        score_json_path=args.scores,
        review_csv_path=args.reviews,
        label_json_paths=args.labels,
        thresholds=args.thresholds,
        reviewers=args.reviewers,
        daily_capacity_per_reviewer=args.daily_capacity,
        max_team_days=args.max_team_days,
        min_confirmed_positive_rate=args.min_confirmed_positive_rate,
        min_candidates=args.min_candidates,
    )
    if args.json:
        export_round_decision_sheet_json(report, args.json)
    if args.md:
        export_round_decision_sheet_markdown(report, args.md)
    return f"built decision sheet for {args.round_name}"


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    try:
        if args.command == "analyze":
            content = run_analyze(args)
        elif args.command == "extract-payment-pdf":
            content = run_extract_payment_pdf(args)
        elif args.command == "export-training":
            content = run_export_training(args)
        elif args.command == "export-dataset":
            content = run_export_dataset(args)
        elif args.command == "label-catalog":
            content = run_label_catalog(args)
        elif args.command == "split-dataset":
            content = run_split_dataset(args)
        elif args.command == "train-baseline":
            content = run_train_baseline(args)
        elif args.command == "triage-workbooks":
            content = run_triage_workbooks(args)
        elif args.command == "graph-triage":
            content = run_graph_triage(args)
        elif args.command == "normalize-ledgers":
            content = run_normalize_ledgers(args)
        elif args.command == "export-ledger-review":
            content = run_export_ledger_review(args)
        elif args.command == "export-rule-audit":
            content = run_export_rule_audit(args)
        elif args.command == "build-rule-summary":
            content = run_build_rule_summary(args)
        elif args.command == "build-rule-review-summary":
            content = run_build_rule_review_summary(args)
        elif args.command == "build-graph-dataset":
            content = run_build_graph_dataset(args)
        elif args.command == "build-owner-summary":
            content = run_build_owner_summary(args)
        elif args.command == "export-owner-review":
            content = run_export_owner_review(args)
        elif args.command == "import-owner-review":
            content = run_import_owner_review(args)
        elif args.command == "export-mirror-review":
            content = run_export_mirror_review(args)
        elif args.command == "import-mirror-review":
            content = run_import_mirror_review(args)
        elif args.command == "train-gnn":
            content = run_train_gnn(args)
        elif args.command == "score-gnn":
            content = run_score_gnn(args)
        elif args.command == "export-review-candidates":
            content = run_export_review_candidates(args)
        elif args.command == "import-review-labels":
            content = run_import_review_labels(args)
        elif args.command == "merge-label-manifests":
            content = run_merge_label_manifests(args)
        elif args.command == "compare-round-metrics":
            content = run_compare_round_metrics(args)
        elif args.command == "make-round-report":
            content = run_make_round_report(args)
        elif args.command == "bootstrap-round":
            content = run_bootstrap_round(args)
        elif args.command == "score-threshold-sweep":
            content = run_score_threshold_sweep(args)
        elif args.command == "review-workload-forecast":
            content = run_review_workload_forecast(args)
        elif args.command == "select-operating-threshold":
            content = run_select_operating_threshold(args)
        elif args.command == "round-decision-sheet":
            content = run_round_decision_sheet(args)
        else:
            parser.error(f"unsupported command: {args.command}")
            return 2
    except (FileNotFoundError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2

    output_path = getattr(args, "output", None)
    if output_path:
        Path(output_path).write_text(content, encoding="utf-8")
    else:
        print(content, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
