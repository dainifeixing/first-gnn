from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from statistics import mean
from typing import Any

from .labels import LabelManifest
from .model import BaselineTextClassifier
from .training import TrainingExample, build_training_examples


@dataclass(frozen=True)
class RowTriage:
    row_index: int
    transaction_id: str
    label_status: str
    positive_probability: float
    label: str
    subject: str
    amount: str
    timestamp: str
    counterparty: str
    remark: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "row_index": self.row_index,
            "transaction_id": self.transaction_id,
            "label_status": self.label_status,
            "positive_probability": round(self.positive_probability, 4),
            "label": self.label,
            "subject": self.subject,
            "amount": self.amount,
            "timestamp": self.timestamp,
            "counterparty": self.counterparty,
            "remark": self.remark,
        }


@dataclass(frozen=True)
class WorkbookTriage:
    path: str
    total_rows: int
    labeled_rows: int
    positive_rows: int
    negative_rows: int
    unlabeled_rows: int
    avg_positive_probability: float
    max_positive_probability: float
    verdict: str
    row_hits: list[RowTriage]

    def to_dict(self) -> dict[str, Any]:
        return {
            "path": self.path,
            "total_rows": self.total_rows,
            "labeled_rows": self.labeled_rows,
            "positive_rows": self.positive_rows,
            "negative_rows": self.negative_rows,
            "unlabeled_rows": self.unlabeled_rows,
            "avg_positive_probability": round(self.avg_positive_probability, 4),
            "max_positive_probability": round(self.max_positive_probability, 4),
            "verdict": self.verdict,
            "row_hits": [hit.to_dict() for hit in self.row_hits],
        }


def _manifest_source_paths(manifests: list[LabelManifest]) -> list[Path]:
    paths: list[Path] = []
    for manifest in manifests:
        source = Path(manifest.source_file)
        if source.exists():
            paths.append(source)
    return sorted({path.resolve() for path in paths})


def build_global_training_examples(manifests: list[LabelManifest]) -> list[TrainingExample]:
    examples: list[TrainingExample] = []
    for source_path in _manifest_source_paths(manifests):
        for example in build_training_examples(source_path, manifests):
            if example.label_status in {"positive", "negative"}:
                examples.append(example)
    return examples


def _synthetic_example(label_status: str) -> TrainingExample:
    if label_status == "positive":
        return TrainingExample(
            row_index=0,
            transaction_id="synthetic-positive",
            label="high_risk_transaction",
            label_status="positive",
            subject="synthetic",
            source_file="",
            amount="388",
            timestamp="2026-03-12 23:11:00",
            hour=23,
            weekday=3,
            is_night=True,
            counterparty="夜间私单",
            direction="入账",
            channel="微信",
            remark="定金",
            raw={},
        )
    return TrainingExample(
        row_index=0,
        transaction_id="synthetic-negative",
        label="low_risk_transaction",
        label_status="negative",
        subject="synthetic",
        source_file="",
        amount="12",
        timestamp="2026-03-12 11:11:00",
        hour=11,
        weekday=3,
        is_night=False,
        counterparty="超市消费",
        direction="出账",
        channel="支付宝",
        remark="买菜",
        raw={},
    )


def train_global_classifier(manifests: list[LabelManifest]) -> BaselineTextClassifier:
    examples = build_global_training_examples(manifests)
    if not examples:
        raise ValueError("no labeled examples found in manifest source files")
    labels_present = {example.label_status for example in examples}
    if "positive" not in labels_present:
        examples.append(_synthetic_example("positive"))
    if "negative" not in labels_present:
        examples.append(_synthetic_example("negative"))
    return BaselineTextClassifier().fit(examples)


def triage_workbook(
    workbook_path: str | Path,
    manifests: list[LabelManifest],
    classifier: BaselineTextClassifier | None = None,
) -> WorkbookTriage:
    path = Path(workbook_path)
    examples = build_training_examples(path, manifests)
    model = classifier or train_global_classifier(manifests)

    row_hits: list[RowTriage] = []
    positive_probs: list[float] = []
    labeled_rows = positive_rows = negative_rows = unlabeled_rows = 0

    for example in examples:
        if example.label_status == "positive":
            labeled_rows += 1
            positive_rows += 1
            prob = 1.0
        elif example.label_status == "negative":
            labeled_rows += 1
            negative_rows += 1
            prob = 0.0
        else:
            unlabeled_rows += 1
            prob = model.predict_proba(example)["positive"]
        positive_probs.append(prob)
        row_hits.append(
            RowTriage(
                row_index=example.row_index,
                transaction_id=example.transaction_id,
                label_status=example.label_status,
                positive_probability=prob,
                label=example.label,
                subject=example.subject,
                amount=example.amount,
                timestamp=example.timestamp,
                counterparty=example.counterparty,
                remark=example.remark,
            )
        )

    avg_positive_probability = mean(positive_probs) if positive_probs else 0.0
    max_positive_probability = max(positive_probs) if positive_probs else 0.0
    if positive_rows > 0 or max_positive_probability >= 0.85:
        verdict = "high_confidence_positive"
    elif negative_rows > 0 and max_positive_probability <= 0.15:
        verdict = "high_confidence_negative"
    else:
        verdict = "needs_review"

    row_hits.sort(key=lambda item: (-item.positive_probability, item.label_status != "unlabeled", item.row_index))
    return WorkbookTriage(
        path=str(path),
        total_rows=len(examples),
        labeled_rows=labeled_rows,
        positive_rows=positive_rows,
        negative_rows=negative_rows,
        unlabeled_rows=unlabeled_rows,
        avg_positive_probability=avg_positive_probability,
        max_positive_probability=max_positive_probability,
        verdict=verdict,
        row_hits=row_hits,
    )


def scan_workbook_directory(
    root: str | Path,
    manifests: list[LabelManifest],
    classifier: BaselineTextClassifier | None = None,
) -> list[WorkbookTriage]:
    base = Path(root)
    model = classifier or train_global_classifier(manifests)
    results: list[WorkbookTriage] = []
    for path in sorted(base.rglob("*.xlsx")):
        if path.name.startswith("~$") or path.name.startswith(".~lock."):
            continue
        results.append(triage_workbook(path, manifests, classifier=model))
    results.sort(key=lambda item: (-item.max_positive_probability, item.verdict, item.path))
    return results


def export_triage_json(results: list[WorkbookTriage], output_path: str | Path) -> Path:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = [item.to_dict() for item in results]
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def export_triage_markdown(results: list[WorkbookTriage], output_path: str | Path) -> Path:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = ["# Workbook Triage", ""]
    lines.append(f"- workbooks: {len(results)}")
    lines.append("")
    lines.append("| verdict | path | total | labeled | positive | negative | unlabeled | avg_positive | max_positive |")
    lines.append("| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |")
    for item in results:
        lines.append(
            f"| {item.verdict} | {item.path} | {item.total_rows} | {item.labeled_rows} | {item.positive_rows} | {item.negative_rows} | {item.unlabeled_rows} | {item.avg_positive_probability:.4f} | {item.max_positive_probability:.4f} |"
        )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path
