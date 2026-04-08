from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from pathlib import Path

from .excel import load_xlsx_styled_rows
from .identifiers import choose_transaction_id
from .labels import LabelManifest


@dataclass(frozen=True)
class AnnotationRow:
    transaction_id: str
    label_status: str
    extension_role: str = ""
    anchor_subject: str = ""
    note: str = ""


def _normalize_label_status(value: str) -> str:
    normalized = str(value).strip().lower()
    mapping = {
        "positive": "positive",
        "negative": "negative",
        "skip": "skip",
        "confirmed_positive": "positive",
        "confirmed_negative": "negative",
        "high_risk_transaction": "positive",
        "low_risk_transaction": "negative",
    }
    if normalized not in mapping:
        raise ValueError(f"unsupported annotation label: {value}")
    return mapping[normalized]


def _annotation_rows_from_csv(path: Path) -> list[AnnotationRow]:
    rows: list[AnnotationRow] = []
    with path.open(encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            transaction_id = str(row.get("transaction_id", "")).strip()
            if not transaction_id:
                continue
            rows.append(
                AnnotationRow(
                    transaction_id=transaction_id,
                    label_status=_normalize_label_status(str(row.get("label", row.get("label_status", "")))),
                    extension_role=str(row.get("extension_role", "")).strip(),
                    anchor_subject=str(row.get("anchor_subject", "")).strip(),
                    note=str(row.get("note", "")).strip(),
                )
            )
    return rows


def _annotation_rows_from_jsonl(path: Path) -> list[AnnotationRow]:
    rows: list[AnnotationRow] = []
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            payload = json.loads(line)
            transaction_id = str(payload.get("transaction_id", "")).strip()
            if not transaction_id:
                continue
            rows.append(
                AnnotationRow(
                    transaction_id=transaction_id,
                    label_status=_normalize_label_status(str(payload.get("label", payload.get("label_status", "")))),
                    extension_role=str(payload.get("extension_role", "")).strip(),
                    anchor_subject=str(payload.get("anchor_subject", "")).strip(),
                    note=str(payload.get("note", "")).strip(),
                )
            )
    return rows


def _annotation_rows_from_xlsx(path: Path) -> list[AnnotationRow]:
    rows: list[AnnotationRow] = []
    for row_index, item in enumerate(load_xlsx_styled_rows(path), start=1):
        transaction_id = choose_transaction_id(item.values, row_index=row_index)
        if not transaction_id:
            continue
        review_label = str(item.values.get("review_label", "")).strip().lower()
        if review_label:
            label_status = _normalize_label_status(review_label)
        elif item.fill_label == "red":
            label_status = "positive"
        elif item.fill_label == "green":
            label_status = "negative"
        elif item.fill_label == "yellow":
            label_status = "skip"
        else:
            continue
        rows.append(
            AnnotationRow(
                transaction_id=transaction_id,
                label_status=label_status,
                extension_role=str(item.values.get("extension_role", "")).strip(),
                anchor_subject=str(item.values.get("anchor_subject", "")).strip(),
                note=str(item.values.get("review_note", "")).strip(),
            )
        )
    return rows


def load_annotation_rows(path: str | Path) -> list[AnnotationRow]:
    resolved = Path(path)
    suffix = resolved.suffix.lower()
    if suffix == ".csv":
        return _annotation_rows_from_csv(resolved)
    if suffix == ".jsonl":
        return _annotation_rows_from_jsonl(resolved)
    if suffix == ".xlsx":
        return _annotation_rows_from_xlsx(resolved)
    raise ValueError(f"annotation file must be .csv, .jsonl, or .xlsx: {resolved}")


def load_annotation_manifests(path: str | Path) -> list[LabelManifest]:
    resolved = Path(path)
    rows = load_annotation_rows(resolved)
    positive_ids: list[str] = []
    negative_ids: list[str] = []
    skipped_ids: list[str] = []
    for row in rows:
        if row.label_status == "positive":
            positive_ids.append(row.transaction_id)
        elif row.label_status == "negative":
            negative_ids.append(row.transaction_id)
        else:
            skipped_ids.append(row.transaction_id)
    manifests: list[LabelManifest] = []
    dataset_name = resolved.stem
    if positive_ids:
        manifests.append(
            LabelManifest(
                dataset_name=dataset_name,
                label="high_risk_transaction",
                subject="annotations",
                status="verified",
                source_file=str(resolved),
                transaction_ids=sorted(dict.fromkeys(positive_ids)),
                polarity="positive",
                notes="Loaded from simplified annotation file.",
            )
        )
    if negative_ids:
        manifests.append(
            LabelManifest(
                dataset_name=dataset_name,
                label="low_risk_transaction",
                subject="annotations",
                status="verified",
                source_file=str(resolved),
                transaction_ids=sorted(dict.fromkeys(negative_ids)),
                polarity="negative",
                notes="Loaded from simplified annotation file.",
            )
        )
    if not manifests and skipped_ids:
        raise ValueError("annotation file only contains skip rows; at least one positive or negative label is required")
    return manifests


def build_review_annotations(review_csv_path: str | Path) -> list[AnnotationRow]:
    resolved = Path(review_csv_path)
    if resolved.suffix.lower() == ".xlsx":
        return _annotation_rows_from_xlsx(resolved)
    rows: list[AnnotationRow] = []
    with resolved.open(encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            transaction_id = str(row.get("transaction_id", "")).strip()
            if not transaction_id:
                continue
            review_label = str(row.get("review_label", "")).strip().lower()
            if review_label == "confirmed_positive":
                label_status = "positive"
            elif review_label == "confirmed_negative":
                label_status = "negative"
            elif review_label == "uncertain":
                label_status = "skip"
            else:
                continue
            rows.append(
                AnnotationRow(
                    transaction_id=transaction_id,
                    label_status=label_status,
                    extension_role=str(row.get("extension_role", "")).strip(),
                    anchor_subject=str(row.get("anchor_subject", "")).strip(),
                    note=str(row.get("review_note", "")).strip(),
                )
            )
    return rows


def export_annotations_csv(rows: list[AnnotationRow], output_path: str | Path) -> Path:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["transaction_id", "label", "extension_role", "anchor_subject", "note"])
        writer.writeheader()
        for row in rows:
            writer.writerow(
                {
                    "transaction_id": row.transaction_id,
                    "label": row.label_status,
                    "extension_role": row.extension_role,
                    "anchor_subject": row.anchor_subject,
                    "note": row.note,
                }
            )
    return path


def export_annotations_jsonl(rows: list[AnnotationRow], output_path: str | Path) -> Path:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(
                json.dumps(
                    {
                        "transaction_id": row.transaction_id,
                        "label": row.label_status,
                        "extension_role": row.extension_role,
                        "anchor_subject": row.anchor_subject,
                        "note": row.note,
                    },
                    ensure_ascii=False,
                )
                + "\n"
            )
    return path
