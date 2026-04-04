from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class OwnerAnnotation:
    target_type: str
    target_id: str
    owner_id: str
    owner_name: str
    confidence: str
    evidence: str = ""
    note: str = ""

    def to_dict(self) -> dict[str, str]:
        return {
            "target_type": self.target_type,
            "target_id": self.target_id,
            "owner_id": self.owner_id,
            "owner_name": self.owner_name,
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
        "owner": "counterparty",
    }
    if normalized not in mapping:
        raise ValueError(f"unsupported owner target_type: {value}")
    return mapping[normalized]


def _normalize_confidence(value: str) -> str:
    normalized = str(value).strip().lower()
    allowed = {"high", "medium", "low"}
    if normalized not in allowed:
        raise ValueError(f"unsupported owner confidence: {value}")
    return normalized


def _normalize_key(value: str) -> str:
    return "".join(str(value or "").strip().lower().split())


def _owner_from_row(row: dict[str, str]) -> OwnerAnnotation | None:
    target_id = str(row.get("target_id", "")).strip()
    owner_id = str(row.get("owner_id", "")).strip()
    if not target_id or not owner_id:
        return None
    return OwnerAnnotation(
        target_type=_normalize_target_type(str(row.get("target_type", ""))),
        target_id=target_id,
        owner_id=owner_id,
        owner_name=str(row.get("owner_name", "")).strip(),
        confidence=_normalize_confidence(str(row.get("confidence", ""))),
        evidence=str(row.get("evidence", "")).strip(),
        note=str(row.get("note", "")).strip(),
    )


def load_owner_annotations(path: str | Path) -> list[OwnerAnnotation]:
    resolved = Path(path)
    if resolved.suffix.lower() == ".csv":
        with resolved.open(encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            return [item for item in (_owner_from_row(row) for row in reader) if item is not None]
    if resolved.suffix.lower() == ".jsonl":
        rows: list[OwnerAnnotation] = []
        with resolved.open(encoding="utf-8") as handle:
            for line in handle:
                rows.append(_owner_from_row(json.loads(line)))
        return [item for item in rows if item is not None]
    raise ValueError(f"owner annotation file must be .csv or .jsonl: {resolved}")


class OwnerLookup:
    def __init__(self, annotations: list[OwnerAnnotation]) -> None:
        self.by_transaction: dict[str, OwnerAnnotation] = {}
        self.by_counterparty: dict[str, OwnerAnnotation] = {}
        for item in annotations:
            key = _normalize_key(item.target_id)
            if not key:
                continue
            if item.target_type == "transaction":
                self.by_transaction[key] = item
            elif item.target_type == "counterparty":
                self.by_counterparty[key] = item

    def resolve(self, transaction_id: str, counterparty: str) -> OwnerAnnotation | None:
        tx_key = _normalize_key(transaction_id)
        if tx_key and tx_key in self.by_transaction:
            return self.by_transaction[tx_key]
        cp_key = _normalize_key(counterparty)
        if cp_key and cp_key in self.by_counterparty:
            return self.by_counterparty[cp_key]
        return None
