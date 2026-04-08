from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

from .excel import load_xlsx_styled_rows


@dataclass(frozen=True)
class LabelManifest:
    dataset_name: str
    label: str
    subject: str
    status: str
    source_file: str
    transaction_ids: list[str]
    polarity: str = "positive"
    verified_by: str = ""
    verified_on: str = ""
    notes: str = ""

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "LabelManifest":
        return cls(
            dataset_name=str(payload.get("dataset_name", "")),
            label=str(payload.get("label", "")),
            subject=str(payload.get("subject", "")),
            status=str(payload.get("status", "")),
            source_file=str(payload.get("source_file", "")),
            transaction_ids=[str(item) for item in payload.get("transaction_ids", [])],
            polarity=str(payload.get("polarity", "positive")),
            verified_by=str(payload.get("verified_by", "")),
            verified_on=str(payload.get("verified_on", "")),
            notes=str(payload.get("notes", "")),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "dataset_name": self.dataset_name,
            "label": self.label,
            "subject": self.subject,
            "status": self.status,
            "source_file": self.source_file,
            "transaction_ids": list(self.transaction_ids),
            "polarity": self.polarity,
            "verified_by": self.verified_by,
            "verified_on": self.verified_on,
            "notes": self.notes,
        }


def load_label_manifest(path: str | Path) -> LabelManifest:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    return LabelManifest.from_dict(payload)


def load_label_manifests(paths: list[str | Path]) -> list[LabelManifest]:
    return [load_label_manifest(path) for path in paths]


def build_label_index(manifests: list[LabelManifest]) -> dict[str, list[dict[str, Any]]]:
    index: dict[str, list[dict[str, Any]]] = {}
    for manifest in manifests:
        for transaction_id in manifest.transaction_ids:
            index.setdefault(transaction_id, []).append(
                {
                    "dataset_name": manifest.dataset_name,
                    "label": manifest.label,
                    "subject": manifest.subject,
                    "status": manifest.status,
                    "source_file": manifest.source_file,
                    "polarity": manifest.polarity,
                }
            )
    return index


def annotate_transaction_ids(transaction_ids: list[str], manifests: list[LabelManifest]) -> list[dict[str, Any]]:
    index = build_label_index(manifests)
    return [
        {
            "transaction_id": transaction_id,
            "labels": index.get(transaction_id, []),
        }
        for transaction_id in transaction_ids
    ]


def export_label_manifest(manifest: LabelManifest, output_path: str | Path) -> Path:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(manifest.to_dict(), ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def build_review_manifest(
    review_csv_path: str | Path,
    polarity: str,
    dataset_name: str,
    verified_by: str = "user",
    subject: str = "reviewed_batch",
    source_file: str = "",
) -> LabelManifest:
    target_label = "confirmed_positive" if polarity == "positive" else "confirmed_negative"
    transaction_ids: list[str] = []
    workbook_paths: set[str] = set()
    review_path = Path(review_csv_path)
    if review_path.suffix.lower() == ".xlsx":
        for row in load_xlsx_styled_rows(review_path):
            review_label = str(row.values.get("review_label", "")).strip().lower()
            if not review_label:
                if row.fill_label == "red":
                    review_label = "confirmed_positive"
                elif row.fill_label == "yellow":
                    review_label = "uncertain"
            if review_label != target_label:
                continue
            transaction_id = str(row.values.get("transaction_id", "")).strip()
            if not transaction_id:
                continue
            transaction_ids.append(transaction_id)
            workbook_path = str(row.values.get("workbook_path", "")).strip()
            if workbook_path:
                workbook_paths.add(workbook_path)
    else:
        with review_path.open(encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                review_label = str(row.get("review_label", "")).strip().lower()
                if review_label != target_label:
                    continue
                transaction_id = str(row.get("transaction_id", "")).strip()
                if not transaction_id:
                    continue
                transaction_ids.append(transaction_id)
                workbook_path = str(row.get("workbook_path", "")).strip()
                if workbook_path:
                    workbook_paths.add(workbook_path)
    normalized_ids = sorted(dict.fromkeys(transaction_ids))
    resolved_source = source_file or (sorted(workbook_paths)[0] if len(workbook_paths) == 1 else "")
    label = "high_risk_transaction" if polarity == "positive" else "low_risk_transaction"
    notes = (
        "Generated from manual review results. Only confirmed review outcomes were included "
        "in this training manifest."
    )
    return LabelManifest(
        dataset_name=dataset_name,
        label=label,
        subject=subject,
        status="verified",
        source_file=resolved_source,
        transaction_ids=normalized_ids,
        polarity=polarity,
        verified_by=verified_by,
        verified_on=date.today().isoformat(),
        notes=notes,
    )


def merge_label_manifests(
    manifests: list[LabelManifest],
    dataset_name: str,
    polarity: str,
    verified_by: str = "user",
    subject: str = "merged_batch",
) -> LabelManifest:
    filtered = [manifest for manifest in manifests if manifest.polarity == polarity]
    transaction_ids: list[str] = []
    subjects: set[str] = set()
    source_files: set[str] = set()
    for manifest in filtered:
        transaction_ids.extend(manifest.transaction_ids)
        if manifest.subject:
            subjects.add(manifest.subject)
        if manifest.source_file:
            source_files.add(manifest.source_file)
    label = "high_risk_transaction" if polarity == "positive" else "low_risk_transaction"
    merged_ids = sorted(dict.fromkeys(transaction_ids))
    merged_subject = subject
    if not merged_subject and len(subjects) == 1:
        merged_subject = next(iter(subjects))
    source_file = sorted(source_files)[0] if len(source_files) == 1 else ""
    notes = "Merged from existing label manifests for iterative training."
    return LabelManifest(
        dataset_name=dataset_name,
        label=label,
        subject=merged_subject,
        status="verified",
        source_file=source_file,
        transaction_ids=merged_ids,
        polarity=polarity,
        verified_by=verified_by,
        verified_on=date.today().isoformat(),
        notes=notes,
    )
