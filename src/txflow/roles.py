from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class RoleAnnotation:
    target_type: str
    target_id: str
    scene: str
    role_label: str
    confidence: str
    evidence: str = ""
    note: str = ""

    def to_dict(self) -> dict[str, str]:
        return {
            "target_type": self.target_type,
            "target_id": self.target_id,
            "scene": self.scene,
            "role_label": self.role_label,
            "confidence": self.confidence,
            "evidence": self.evidence,
            "note": self.note,
        }


def _normalize_target_type(value: str) -> str:
    normalized = str(value).strip().lower()
    mapping = {
        "transaction": "transaction",
        "counterparty": "counterparty",
        "account": "counterparty",
        "owner": "owner",
    }
    if normalized not in mapping:
        raise ValueError(f"unsupported role target_type: {value}")
    return mapping[normalized]


def _normalize_role_label(value: str) -> str:
    normalized = str(value).strip().lower()
    allowed = {"buyer", "seller", "broker", "mixed", "unknown"}
    if normalized not in allowed:
        raise ValueError(f"unsupported role_label: {value}")
    return normalized


def _normalize_confidence(value: str) -> str:
    normalized = str(value).strip().lower()
    allowed = {"high", "medium", "low"}
    if normalized not in allowed:
        raise ValueError(f"unsupported role confidence: {value}")
    return normalized


def _normalize_key(value: str) -> str:
    return "".join(str(value or "").strip().lower().split())


def _role_from_row(row: dict[str, str]) -> RoleAnnotation | None:
    target_id = str(row.get("target_id", "")).strip()
    if not target_id:
        return None
    return RoleAnnotation(
        target_type=_normalize_target_type(str(row.get("target_type", ""))),
        target_id=target_id,
        scene=str(row.get("scene", "")).strip().lower(),
        role_label=_normalize_role_label(str(row.get("role_label", ""))),
        confidence=_normalize_confidence(str(row.get("confidence", ""))),
        evidence=str(row.get("evidence", "")).strip(),
        note=str(row.get("note", "")).strip(),
    )


def load_role_annotations(path: str | Path) -> list[RoleAnnotation]:
    resolved = Path(path)
    if resolved.suffix.lower() == ".csv":
        with resolved.open(encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            return [item for item in (_role_from_row(row) for row in reader) if item is not None]
    if resolved.suffix.lower() == ".jsonl":
        rows: list[RoleAnnotation] = []
        with resolved.open(encoding="utf-8") as handle:
            for line in handle:
                rows.append(_role_from_row(json.loads(line)))
        return [item for item in rows if item is not None]
    raise ValueError(f"role annotation file must be .csv or .jsonl: {resolved}")


class RoleLookup:
    def __init__(self, annotations: list[RoleAnnotation]) -> None:
        self.by_transaction: dict[str, RoleAnnotation] = {}
        self.by_counterparty: dict[str, RoleAnnotation] = {}
        self.by_owner: dict[str, RoleAnnotation] = {}
        for item in annotations:
            key = _normalize_key(item.target_id)
            if not key:
                continue
            if item.target_type == "transaction":
                self.by_transaction[key] = item
            elif item.target_type == "counterparty":
                self.by_counterparty[key] = item
            elif item.target_type == "owner":
                self.by_owner[key] = item

    def resolve(self, transaction_id: str, counterparty: str, owner_id: str = "") -> RoleAnnotation | None:
        tx_key = _normalize_key(transaction_id)
        if tx_key and tx_key in self.by_transaction:
            return self.by_transaction[tx_key]
        owner_key = _normalize_key(owner_id)
        if owner_key and owner_key in self.by_owner:
            return self.by_owner[owner_key]
        cp_key = _normalize_key(counterparty)
        if cp_key and cp_key in self.by_counterparty:
            return self.by_counterparty[cp_key]
        return None


def export_role_annotations_csv(rows: list[RoleAnnotation], output_path: str | Path) -> Path:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=["target_type", "target_id", "scene", "role_label", "confidence", "evidence", "note"],
        )
        writer.writeheader()
        for row in rows:
            writer.writerow(row.to_dict())
    return path


def export_role_annotations_jsonl(rows: list[RoleAnnotation], output_path: str | Path) -> Path:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row.to_dict(), ensure_ascii=False) + "\n")
    return path
