from __future__ import annotations

import csv
import json
import re
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any

from .excel import load_xlsx_styled_rows, write_xlsx_table
from .labels import LabelManifest
from .report_io import write_json_file, write_markdown_lines
from .roles import RoleAnnotation, load_role_annotations
from .training import TrainingExample, build_training_examples


@dataclass(frozen=True)
class NormalizedTransaction:
    workbook_path: str
    row_index: int
    transaction_id: str
    amount: str
    timestamp: str
    counterparty: str
    counterparty_account: str
    counterparty_name: str
    direction: str
    channel: str
    remark: str
    subject_account: str
    tx_id_secondary: str
    balance_after: str
    payer_account: str
    payer_bank_name: str
    payer_bank_card: str
    payee_account: str
    payee_bank_name: str
    payee_bank_card: str
    merchant_id: str
    merchant_name: str
    flow_family: str
    trade_pattern: str
    buyer_account: str
    seller_account: str
    seller_proxy_name: str
    rule_reason: str
    is_qr_transfer: bool
    is_red_packet: bool
    is_merchant_consume: bool
    is_withdrawal_like: bool
    is_platform_settlement: bool
    is_failed_or_invalid: bool
    is_trade_like: bool
    label_status: str
    label: str
    subject: str
    role_label: str
    role_confidence: str
    role_scene: str
    owner_id: str
    owner_name: str
    owner_confidence: str
    owner_tx_count: int
    owner_unique_counterparties: int
    owner_inflow_ratio: float
    owner_outflow_ratio: float
    owner_collect_and_split: bool
    mirror_group_id: str
    mirror_match_count: int
    mirror_workbook_count: int
    mirror_has_opposite_direction: bool
    possible_mirror_group_id: str
    possible_mirror_match_count: int
    possible_mirror_workbook_count: int
    possible_mirror_score: float
    possible_mirror_confidence: str
    mirror_review_decision: str
    mirror_review_confidence: str
    mirror_review_note: str
    review_flags: list[str]

    def to_dict(self) -> dict[str, Any]:
        return {
            "workbook_path": self.workbook_path,
            "row_index": self.row_index,
            "transaction_id": self.transaction_id,
            "amount": self.amount,
            "timestamp": self.timestamp,
            "counterparty": self.counterparty,
            "counterparty_account": self.counterparty_account,
            "counterparty_name": self.counterparty_name,
            "direction": self.direction,
            "channel": self.channel,
            "remark": self.remark,
            "subject_account": self.subject_account,
            "tx_id_secondary": self.tx_id_secondary,
            "balance_after": self.balance_after,
            "payer_account": self.payer_account,
            "payer_bank_name": self.payer_bank_name,
            "payer_bank_card": self.payer_bank_card,
            "payee_account": self.payee_account,
            "payee_bank_name": self.payee_bank_name,
            "payee_bank_card": self.payee_bank_card,
            "merchant_id": self.merchant_id,
            "merchant_name": self.merchant_name,
            "flow_family": self.flow_family,
            "trade_pattern": self.trade_pattern,
            "buyer_account": self.buyer_account,
            "seller_account": self.seller_account,
            "seller_proxy_name": self.seller_proxy_name,
            "rule_reason": self.rule_reason,
            "is_qr_transfer": self.is_qr_transfer,
            "is_red_packet": self.is_red_packet,
            "is_merchant_consume": self.is_merchant_consume,
            "is_withdrawal_like": self.is_withdrawal_like,
            "is_platform_settlement": self.is_platform_settlement,
            "is_failed_or_invalid": self.is_failed_or_invalid,
            "is_trade_like": self.is_trade_like,
            "label_status": self.label_status,
            "label": self.label,
            "subject": self.subject,
            "role_label": self.role_label,
            "role_confidence": self.role_confidence,
            "role_scene": self.role_scene,
            "owner_id": self.owner_id,
            "owner_name": self.owner_name,
            "owner_confidence": self.owner_confidence,
            "owner_tx_count": self.owner_tx_count,
            "owner_unique_counterparties": self.owner_unique_counterparties,
            "owner_inflow_ratio": round(self.owner_inflow_ratio, 4),
            "owner_outflow_ratio": round(self.owner_outflow_ratio, 4),
            "owner_collect_and_split": self.owner_collect_and_split,
            "mirror_group_id": self.mirror_group_id,
            "mirror_match_count": self.mirror_match_count,
            "mirror_workbook_count": self.mirror_workbook_count,
            "mirror_has_opposite_direction": self.mirror_has_opposite_direction,
            "possible_mirror_group_id": self.possible_mirror_group_id,
            "possible_mirror_match_count": self.possible_mirror_match_count,
            "possible_mirror_workbook_count": self.possible_mirror_workbook_count,
            "possible_mirror_score": round(self.possible_mirror_score, 4),
            "possible_mirror_confidence": self.possible_mirror_confidence,
            "mirror_review_decision": self.mirror_review_decision,
            "mirror_review_confidence": self.mirror_review_confidence,
            "mirror_review_note": self.mirror_review_note,
            "review_flags": list(self.review_flags),
        }


@dataclass(frozen=True)
class GraphDatasetSummary:
    total_workbooks: int
    total_rows: int
    labeled_rows: int
    positive_rows: int
    negative_rows: int
    unlabeled_rows: int
    flagged_rows: int
    mirrored_rows: int
    mirrored_groups: int
    possible_mirrored_rows: int
    possible_mirrored_groups: int
    confirmed_mirror_rows: int
    rejected_mirror_rows: int
    uncertain_mirror_rows: int
    role_counts: dict[str, int]
    owner_counts: dict[str, int]
    workbooks: list[dict[str, Any]]

    def to_dict(self) -> dict[str, Any]:
        return {
            "total_workbooks": self.total_workbooks,
            "total_rows": self.total_rows,
            "labeled_rows": self.labeled_rows,
            "positive_rows": self.positive_rows,
            "negative_rows": self.negative_rows,
            "unlabeled_rows": self.unlabeled_rows,
            "flagged_rows": self.flagged_rows,
            "mirrored_rows": self.mirrored_rows,
            "mirrored_groups": self.mirrored_groups,
            "possible_mirrored_rows": self.possible_mirrored_rows,
            "possible_mirrored_groups": self.possible_mirrored_groups,
            "confirmed_mirror_rows": self.confirmed_mirror_rows,
            "rejected_mirror_rows": self.rejected_mirror_rows,
            "uncertain_mirror_rows": self.uncertain_mirror_rows,
            "role_counts": dict(self.role_counts),
            "owner_counts": dict(self.owner_counts),
            "workbooks": list(self.workbooks),
        }


@dataclass(frozen=True)
class OwnerSummaryRow:
    owner_id: str
    owner_name: str
    owner_confidence: str
    dominant_role: str
    reviewed_role: str
    reviewed_confidence: str
    reviewed_note: str
    role_counts: dict[str, int]
    pattern_tags: list[str]
    top_counterparties: list[dict[str, Any]]
    priority_score: float
    priority_rank: int
    tx_count: int
    unique_counterparties: int
    inflow_count: int
    outflow_count: int
    inflow_ratio: float
    outflow_ratio: float
    collect_and_split: bool
    channel_count: int
    workbook_count: int
    mirrored_rows: int
    mirrored_groups: int
    possible_mirrored_rows: int
    possible_mirrored_groups: int
    labeled_rows: int
    positive_rows: int
    negative_rows: int
    flagged_rows: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "owner_id": self.owner_id,
            "owner_name": self.owner_name,
            "owner_confidence": self.owner_confidence,
            "dominant_role": self.dominant_role,
            "reviewed_role": self.reviewed_role,
            "reviewed_confidence": self.reviewed_confidence,
            "reviewed_note": self.reviewed_note,
            "role_counts": dict(self.role_counts),
            "pattern_tags": list(self.pattern_tags),
            "top_counterparties": list(self.top_counterparties),
            "priority_score": round(self.priority_score, 4),
            "priority_rank": self.priority_rank,
            "tx_count": self.tx_count,
            "unique_counterparties": self.unique_counterparties,
            "inflow_count": self.inflow_count,
            "outflow_count": self.outflow_count,
            "inflow_ratio": round(self.inflow_ratio, 4),
            "outflow_ratio": round(self.outflow_ratio, 4),
            "collect_and_split": self.collect_and_split,
            "channel_count": self.channel_count,
            "workbook_count": self.workbook_count,
            "mirrored_rows": self.mirrored_rows,
            "mirrored_groups": self.mirrored_groups,
            "possible_mirrored_rows": self.possible_mirrored_rows,
            "possible_mirrored_groups": self.possible_mirrored_groups,
            "labeled_rows": self.labeled_rows,
            "positive_rows": self.positive_rows,
            "negative_rows": self.negative_rows,
            "flagged_rows": self.flagged_rows,
        }


@dataclass(frozen=True)
class OwnerSummaryReport:
    total_rows: int
    covered_rows: int
    skipped_rows: int
    total_owners: int
    owners: list[OwnerSummaryRow]

    def to_dict(self) -> dict[str, Any]:
        return {
            "total_rows": self.total_rows,
            "covered_rows": self.covered_rows,
            "skipped_rows": self.skipped_rows,
            "total_owners": self.total_owners,
            "owners": [item.to_dict() for item in self.owners],
        }


@dataclass(frozen=True)
class OwnerReviewRow:
    owner_id: str
    owner_name: str
    priority_rank: int
    priority_score: float
    dominant_role: str
    reviewed_role: str
    reviewed_confidence: str
    reviewed_note: str
    pattern_tags: list[str]
    top_counterparties: list[dict[str, Any]]
    review_role: str = ""
    review_confidence: str = ""
    review_note: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "owner_id": self.owner_id,
            "owner_name": self.owner_name,
            "priority_rank": self.priority_rank,
            "priority_score": round(self.priority_score, 4),
            "dominant_role": self.dominant_role,
            "reviewed_role": self.reviewed_role,
            "reviewed_confidence": self.reviewed_confidence,
            "reviewed_note": self.reviewed_note,
            "pattern_tags": list(self.pattern_tags),
            "top_counterparties": list(self.top_counterparties),
            "review_role": self.review_role or self.reviewed_role,
            "review_confidence": self.review_confidence or self.reviewed_confidence,
            "review_options": "seller|broker|buyer|mixed|unknown",
            "confidence_options": "high|medium|low",
            "review_note": self.review_note or self.reviewed_note,
        }


@dataclass(frozen=True)
class MirrorReviewRow:
    review_entity: str
    mirror_group_id: str
    transaction_id: str
    workbook_path: str
    row_index: int
    amount: str
    timestamp: str
    counterparty: str
    direction: str
    channel: str
    owner_id: str
    mirror_status: str
    mirror_match_count: int
    mirror_workbook_count: int
    mirror_score: float
    mirror_confidence: str
    review_decision: str = ""
    review_note: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "review_entity": self.review_entity,
            "mirror_group_id": self.mirror_group_id,
            "transaction_id": self.transaction_id,
            "workbook_path": self.workbook_path,
            "row_index": self.row_index,
            "amount": self.amount,
            "timestamp": self.timestamp,
            "counterparty": self.counterparty,
            "direction": self.direction,
            "channel": self.channel,
            "owner_id": self.owner_id,
            "mirror_status": self.mirror_status,
            "mirror_match_count": self.mirror_match_count,
            "mirror_workbook_count": self.mirror_workbook_count,
            "mirror_score": round(self.mirror_score, 4),
            "mirror_confidence": self.mirror_confidence,
            "review_decision": self.review_decision,
            "review_options": "confirmed_mirror|rejected_mirror|uncertain",
            "review_note": self.review_note,
        }


@dataclass(frozen=True)
class MirrorAnnotationRow:
    mirror_group_id: str
    transaction_id: str
    decision: str
    confidence: str
    note: str = ""

    def to_dict(self) -> dict[str, str]:
        return {
            "mirror_group_id": self.mirror_group_id,
            "transaction_id": self.transaction_id,
            "decision": self.decision,
            "confidence": self.confidence,
            "note": self.note,
        }


@dataclass(frozen=True)
class LedgerReviewRow:
    record_id: str
    tx_time: str
    amount: str
    direction: str
    trade_pattern: str
    is_qr_transfer: bool
    buyer_account: str
    seller_account: str
    counterparty_name: str
    merchant_name: str
    rule_reason: str
    remark_excerpt: str
    review_label: str = ""
    review_note: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "record_id": self.record_id,
            "tx_time": self.tx_time,
            "amount": self.amount,
            "direction": self.direction,
            "trade_pattern": self.trade_pattern,
            "is_qr_transfer": self.is_qr_transfer,
            "buyer_account": self.buyer_account,
            "seller_account": self.seller_account,
            "counterparty_name": self.counterparty_name,
            "merchant_name": self.merchant_name,
            "rule_reason": self.rule_reason,
            "remark_excerpt": self.remark_excerpt,
            "review_label": self.review_label,
            "review_options": "confirmed_positive|confirmed_negative|uncertain",
            "review_note": self.review_note,
        }


@dataclass(frozen=True)
class RuleAuditRow:
    record_id: str
    tx_time: str
    amount: str
    direction: str
    channel: str
    flow_family: str
    trade_pattern: str
    rule_reason: str
    rule_hit_count: int
    is_trade_like: bool
    is_qr_transfer: bool
    is_red_packet: bool
    is_platform_settlement: bool
    is_withdrawal_like: bool
    is_failed_or_invalid: bool
    buyer_account: str
    seller_account: str
    counterparty_name: str
    merchant_name: str
    remark_excerpt: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "record_id": self.record_id,
            "tx_time": self.tx_time,
            "amount": self.amount,
            "direction": self.direction,
            "channel": self.channel,
            "flow_family": self.flow_family,
            "trade_pattern": self.trade_pattern,
            "rule_reason": self.rule_reason,
            "rule_hit_count": self.rule_hit_count,
            "is_trade_like": self.is_trade_like,
            "is_qr_transfer": self.is_qr_transfer,
            "is_red_packet": self.is_red_packet,
            "is_platform_settlement": self.is_platform_settlement,
            "is_withdrawal_like": self.is_withdrawal_like,
            "is_failed_or_invalid": self.is_failed_or_invalid,
            "buyer_account": self.buyer_account,
            "seller_account": self.seller_account,
            "counterparty_name": self.counterparty_name,
            "merchant_name": self.merchant_name,
            "remark_excerpt": self.remark_excerpt,
        }


@dataclass(frozen=True)
class RuleSummaryBucket:
    key: str
    row_count: int
    ratio: float

    def to_dict(self) -> dict[str, Any]:
        return {
            "key": self.key,
            "row_count": self.row_count,
            "ratio": round(self.ratio, 4),
        }


@dataclass(frozen=True)
class RuleSummaryReport:
    total_rows: int
    rows_with_rule_hits: int
    trade_like_rows: int
    qr_rows: int
    platform_settlement_rows: int
    withdrawal_rows: int
    failed_or_invalid_rows: int
    by_channel: list[RuleSummaryBucket]
    by_trade_pattern: list[RuleSummaryBucket]
    by_rule_reason: list[RuleSummaryBucket]

    def to_dict(self) -> dict[str, Any]:
        return {
            "total_rows": self.total_rows,
            "rows_with_rule_hits": self.rows_with_rule_hits,
            "trade_like_rows": self.trade_like_rows,
            "qr_rows": self.qr_rows,
            "platform_settlement_rows": self.platform_settlement_rows,
            "withdrawal_rows": self.withdrawal_rows,
            "failed_or_invalid_rows": self.failed_or_invalid_rows,
            "by_channel": [item.to_dict() for item in self.by_channel],
            "by_trade_pattern": [item.to_dict() for item in self.by_trade_pattern],
            "by_rule_reason": [item.to_dict() for item in self.by_rule_reason],
        }


@dataclass(frozen=True)
class RuleReviewBucket:
    key: str
    reviewed_rows: int
    confirmed_positive: int
    confirmed_negative: int
    uncertain: int
    confirmed_positive_rate: float

    def to_dict(self) -> dict[str, Any]:
        return {
            "key": self.key,
            "reviewed_rows": self.reviewed_rows,
            "confirmed_positive": self.confirmed_positive,
            "confirmed_negative": self.confirmed_negative,
            "uncertain": self.uncertain,
            "confirmed_positive_rate": round(self.confirmed_positive_rate, 4),
        }


@dataclass(frozen=True)
class RuleReviewSummary:
    review_total: int
    matched_rows: int
    confirmed_positive: int
    confirmed_negative: int
    uncertain: int
    by_trade_pattern: list[RuleReviewBucket]
    by_rule_reason: list[RuleReviewBucket]

    def to_dict(self) -> dict[str, Any]:
        return {
            "review_total": self.review_total,
            "matched_rows": self.matched_rows,
            "confirmed_positive": self.confirmed_positive,
            "confirmed_negative": self.confirmed_negative,
            "uncertain": self.uncertain,
            "by_trade_pattern": [item.to_dict() for item in self.by_trade_pattern],
            "by_rule_reason": [item.to_dict() for item in self.by_rule_reason],
        }


def build_review_flags(example: TrainingExample, duplicate_ids: set[str]) -> list[str]:
    flags: list[str] = []
    if not example.transaction_id:
        flags.append("missing_transaction_id")
    elif example.transaction_id in duplicate_ids:
        flags.append("duplicate_transaction_id")
    if not example.timestamp:
        flags.append("missing_timestamp")
    if not example.amount:
        flags.append("missing_amount")
    if not example.counterparty:
        flags.append("missing_counterparty")
    if not example.remark:
        flags.append("missing_remark")
    if example.hour is not None and (example.hour >= 22 or example.hour < 6):
        flags.append("night_activity")
    return flags


def build_duplicate_transaction_ids(examples: list[TrainingExample]) -> set[str]:
    tx_id_counts: dict[str, int] = {}
    for example in examples:
        if example.transaction_id:
            tx_id_counts[example.transaction_id] = tx_id_counts.get(example.transaction_id, 0) + 1
    return {item for item, count in tx_id_counts.items() if count > 1}


def _direction_index(direction: str) -> int:
    value = str(direction or "").strip().lower()
    if any(token in value for token in {"inflow", "收入", "转入", "收款", "入账", "收"}):
        return 0
    if any(token in value for token in {"outflow", "支出", "转出", "付款", "消费", "支"}):
        return 1
    return 2


def _owner_aggregate_map(examples: list[TrainingExample]) -> dict[str, dict[str, Any]]:
    owner_examples: dict[str, list[TrainingExample]] = {}
    for example in examples:
        owner_id = str(example.owner_id or "").strip()
        if not owner_id:
            continue
        owner_examples.setdefault(owner_id, []).append(example)

    stats: dict[str, dict[str, Any]] = {}
    for owner_id, owner_rows in owner_examples.items():
        counterparties = {
            str(item.counterparty or "").strip().lower()
            for item in owner_rows
            if str(item.counterparty or "").strip()
        }
        inflow = sum(1 for item in owner_rows if _direction_index(item.direction) == 0)
        outflow = sum(1 for item in owner_rows if _direction_index(item.direction) == 1)
        total = inflow + outflow
        stats[owner_id] = {
            "owner_tx_count": len(owner_rows),
            "owner_unique_counterparties": len(counterparties),
            "owner_inflow_ratio": (inflow / total) if total else 0.0,
            "owner_outflow_ratio": (outflow / total) if total else 0.0,
            "owner_collect_and_split": inflow > 0 and outflow > 0,
        }
    return stats


def _mirror_group_map(rows: list[NormalizedTransaction]) -> dict[str, dict[str, Any]]:
    grouped: dict[str, list[NormalizedTransaction]] = {}
    for row in rows:
        transaction_id = str(row.transaction_id or "").strip()
        if not transaction_id:
            continue
        grouped.setdefault(transaction_id, []).append(row)

    mirror_map: dict[str, dict[str, Any]] = {}
    for transaction_id, items in grouped.items():
        if len(items) < 2:
            continue
        workbooks = {str(item.workbook_path or "").strip() for item in items if str(item.workbook_path or "").strip()}
        directions = {_direction_index(item.direction) for item in items}
        mirror_map[transaction_id] = {
            "mirror_group_id": f"txid:{transaction_id}",
            "mirror_match_count": len(items),
            "mirror_workbook_count": len(workbooks),
            "mirror_has_opposite_direction": 0 in directions and 1 in directions,
        }
    return mirror_map


def _apply_mirror_groups(rows: list[NormalizedTransaction]) -> list[NormalizedTransaction]:
    mirror_map = _mirror_group_map(rows)
    updated: list[NormalizedTransaction] = []
    for row in rows:
        transaction_id = str(row.transaction_id or "").strip()
        mirror = mirror_map.get(transaction_id)
        if mirror is None:
            updated.append(row)
            continue
        flags = list(row.review_flags)
        if "mirrored_transaction" not in flags:
            flags.append("mirrored_transaction")
        if "mirror_review_confirmed" not in flags:
            flags.append("mirror_review_confirmed")
        updated.append(
            replace(
                row,
                mirror_group_id=str(mirror["mirror_group_id"]),
                mirror_match_count=int(mirror["mirror_match_count"]),
                mirror_workbook_count=int(mirror["mirror_workbook_count"]),
                mirror_has_opposite_direction=bool(mirror["mirror_has_opposite_direction"]),
                mirror_review_decision="confirmed_mirror",
                mirror_review_confidence="high",
                mirror_review_note="matched_by_transaction_id",
                review_flags=flags,
            )
        )
    return updated


def _possible_mirror_candidate_key(row: NormalizedTransaction) -> tuple[str, str, str] | None:
    amount = str(row.amount or "").strip()
    timestamp = str(row.timestamp or "").strip()
    channel = str(row.channel or "").strip().lower()
    if not amount or not timestamp:
        return None
    return (amount, timestamp, channel)


def _normalize_match_text(value: str) -> str:
    text = re.sub(r"\s+", "", str(value or "")).strip().lower()
    return re.sub(r"[，,；;。.!！?？:：()（）\\[\\]{}'\"|/\\\\_-]+", "", text)


def _texts_related(left: str, right: str) -> bool:
    a = _normalize_match_text(left)
    b = _normalize_match_text(right)
    if not a or not b:
        return False
    return a == b or a in b or b in a


def _possible_mirror_strength(items: list[NormalizedTransaction]) -> tuple[float, str]:
    owner_assisted = any(str(item.owner_id or "").strip() for item in items)
    counterparty_overlap = any(
        _texts_related(left.counterparty, right.counterparty)
        for index, left in enumerate(items)
        for right in items[index + 1 :]
    )
    remark_overlap = any(
        _texts_related(left.remark, right.remark)
        for index, left in enumerate(items)
        for right in items[index + 1 :]
    )
    score = 0.55
    if owner_assisted:
        score += 0.15
    if counterparty_overlap:
        score += 0.15
    if remark_overlap:
        score += 0.15
    if score >= 0.85:
        return score, "high"
    if score >= 0.7:
        return score, "medium"
    return score, "low"


def _apply_possible_mirror_groups(rows: list[NormalizedTransaction]) -> list[NormalizedTransaction]:
    grouped: dict[tuple[str, str, str], list[NormalizedTransaction]] = {}
    for row in rows:
        if row.mirror_match_count > 1:
            continue
        key = _possible_mirror_candidate_key(row)
        if key is None:
            continue
        grouped.setdefault(key, []).append(row)

    candidate_map: dict[tuple[str, int], dict[str, Any]] = {}
    for key, items in grouped.items():
        if len(items) < 2:
            continue
        workbooks = {str(item.workbook_path or "").strip() for item in items if str(item.workbook_path or "").strip()}
        directions = {_direction_index(item.direction) for item in items}
        if len(workbooks) < 2 or not (0 in directions and 1 in directions):
            continue
        score, confidence = _possible_mirror_strength(items)
        group_id = f"candidate:{key[0]}|{key[1]}|{key[2]}"
        for item in items:
            candidate_map[(str(item.workbook_path), item.row_index)] = {
                "possible_mirror_group_id": group_id,
                "possible_mirror_match_count": len(items),
                "possible_mirror_workbook_count": len(workbooks),
                "possible_mirror_score": score,
                "possible_mirror_confidence": confidence,
            }

    updated: list[NormalizedTransaction] = []
    for row in rows:
        candidate = candidate_map.get((str(row.workbook_path), row.row_index))
        if candidate is None:
            updated.append(row)
            continue
        flags = list(row.review_flags)
        if "possible_mirror_transaction" not in flags:
            flags.append("possible_mirror_transaction")
        updated.append(
            replace(
                row,
                possible_mirror_group_id=str(candidate["possible_mirror_group_id"]),
                possible_mirror_match_count=int(candidate["possible_mirror_match_count"]),
                possible_mirror_workbook_count=int(candidate["possible_mirror_workbook_count"]),
                possible_mirror_score=float(candidate["possible_mirror_score"]),
                possible_mirror_confidence=str(candidate["possible_mirror_confidence"]),
                review_flags=flags,
            )
        )
    return updated


def load_mirror_annotations(path: str | Path) -> list[MirrorAnnotationRow]:
    resolved = Path(path)
    rows: list[dict[str, Any]]
    if resolved.suffix.lower() == ".csv":
        with resolved.open(encoding="utf-8", newline="") as handle:
            rows = list(csv.DictReader(handle))
    elif resolved.suffix.lower() == ".jsonl":
        with resolved.open(encoding="utf-8") as handle:
            rows = [json.loads(line) for line in handle]
    else:
        raise ValueError(f"mirror annotation file must be .csv or .jsonl: {resolved}")
    return [
        MirrorAnnotationRow(
            mirror_group_id=str(row.get("mirror_group_id", "")).strip(),
            transaction_id=str(row.get("transaction_id", "")).strip(),
            decision=str(row.get("decision", "")).strip(),
            confidence=str(row.get("confidence", "")).strip(),
            note=str(row.get("note", "")).strip(),
        )
        for row in rows
        if str(row.get("mirror_group_id", "")).strip() and str(row.get("decision", "")).strip()
    ]


def _aggregate_mirror_annotations(rows: list[MirrorAnnotationRow]) -> dict[str, MirrorAnnotationRow]:
    grouped: dict[str, list[MirrorAnnotationRow]] = {}
    for row in rows:
        grouped.setdefault(row.mirror_group_id, []).append(row)

    aggregated: dict[str, MirrorAnnotationRow] = {}
    for group_id, items in grouped.items():
        decisions = {str(item.decision or "").strip() for item in items if str(item.decision or "").strip()}
        if decisions == {"confirmed_mirror"}:
            decision = "confirmed_mirror"
        elif decisions == {"rejected_mirror"}:
            decision = "rejected_mirror"
        elif decisions == {"uncertain"}:
            decision = "uncertain"
        elif "confirmed_mirror" in decisions and "rejected_mirror" in decisions:
            decision = "uncertain"
        elif "confirmed_mirror" in decisions:
            decision = "confirmed_mirror"
        elif "rejected_mirror" in decisions:
            decision = "rejected_mirror"
        else:
            decision = "uncertain"
        confidence = max((str(item.confidence or "").strip().lower() for item in items), key=_confidence_rank, default="")
        note = " | ".join(dict.fromkeys(item.note for item in items if item.note))
        aggregated[group_id] = MirrorAnnotationRow(
            mirror_group_id=group_id,
            transaction_id=next((item.transaction_id for item in items if item.transaction_id), ""),
            decision=decision,
            confidence=confidence,
            note=note,
        )
    return aggregated


def _apply_mirror_annotations(
    rows: list[NormalizedTransaction],
    mirror_annotation_path: str | Path | None = None,
) -> list[NormalizedTransaction]:
    if not mirror_annotation_path:
        return rows
    annotations = load_mirror_annotations(mirror_annotation_path)
    if not annotations:
        return rows
    aggregated = _aggregate_mirror_annotations(annotations)
    updated: list[NormalizedTransaction] = []
    for row in rows:
        annotation = None
        if row.mirror_group_id:
            annotation = aggregated.get(row.mirror_group_id)
        if annotation is None and row.possible_mirror_group_id:
            annotation = aggregated.get(row.possible_mirror_group_id)
        if annotation is None:
            updated.append(row)
            continue
        flags = [
            item
            for item in row.review_flags
            if item not in {"mirror_review_confirmed", "mirror_review_rejected", "mirror_review_uncertain"}
        ]
        if annotation.decision == "confirmed_mirror":
            flags.append("mirror_review_confirmed")
        elif annotation.decision == "rejected_mirror":
            flags.append("mirror_review_rejected")
        elif annotation.decision == "uncertain":
            flags.append("mirror_review_uncertain")
        updated.append(
            replace(
                row,
                mirror_review_decision=annotation.decision,
                mirror_review_confidence=annotation.confidence,
                mirror_review_note=annotation.note,
                review_flags=flags,
            )
        )
    return updated


def _confidence_rank(value: str) -> int:
    lookup = {"low": 1, "medium": 2, "high": 3}
    return lookup.get(str(value or "").strip().lower(), 0)


def _owner_pattern_tags(
    tx_count: int,
    unique_counterparties: int,
    inflow_count: int,
    outflow_count: int,
) -> list[str]:
    tags: list[str] = []
    if unique_counterparties >= 3 and inflow_count > 0 and outflow_count == 0:
        tags.append("many_to_one_receiving")
    if unique_counterparties >= 3 and outflow_count > 0 and inflow_count == 0:
        tags.append("one_to_many_paying")
    if inflow_count > 0 and outflow_count > 0:
        tags.append("collect_then_split")
    if tx_count >= 5:
        tags.append("high_activity")
    return tags


def _owner_priority_score(
    dominant_role: str,
    owner_confidence: str,
    unique_counterparties: int,
    inflow_count: int,
    outflow_count: int,
    flagged_rows: int,
    positive_rows: int,
    pattern_tags: list[str],
) -> float:
    score = 0.0
    role_weights = {"broker": 0.35, "seller": 0.25, "mixed": 0.18, "buyer": 0.08, "unknown": 0.05}
    confidence_weights = {"high": 0.2, "medium": 0.1, "low": 0.04}
    pattern_weights = {
        "collect_then_split": 0.25,
        "many_to_one_receiving": 0.2,
        "one_to_many_paying": 0.15,
        "high_activity": 0.08,
    }
    score += role_weights.get(dominant_role, 0.0)
    score += confidence_weights.get(owner_confidence, 0.0)
    score += min(unique_counterparties / 10.0, 0.2)
    if inflow_count > 0 and outflow_count > 0:
        score += 0.1
    score += min(flagged_rows * 0.03, 0.15)
    score += min(positive_rows * 0.08, 0.24)
    for tag in pattern_tags:
        score += pattern_weights.get(tag, 0.0)
    return min(score, 1.0)


def summarize_owner_activity(
    root: str | Path,
    manifests: list[LabelManifest],
    role_annotation_path: str | Path | None = None,
    owner_annotation_path: str | Path | None = None,
    mirror_annotation_path: str | Path | None = None,
) -> OwnerSummaryReport:
    rows = export_normalized_ledgers(
        root,
        manifests,
        role_annotation_path=role_annotation_path,
        owner_annotation_path=owner_annotation_path,
        mirror_annotation_path=mirror_annotation_path,
    )
    owner_rows: dict[str, list[NormalizedTransaction]] = {}
    owner_role_annotations = {
        str(item.target_id or "").strip(): item
        for item in (load_role_annotations(role_annotation_path) if role_annotation_path else [])
        if str(item.target_type or "").strip() == "owner"
    }
    for row in rows:
        owner_id = str(row.owner_id or "").strip()
        if not owner_id:
            continue
        owner_rows.setdefault(owner_id, []).append(row)

    summaries: list[OwnerSummaryRow] = []
    for owner_id, items in sorted(owner_rows.items()):
        role_counts: dict[str, int] = {}
        counterparty_counts: dict[str, int] = {}
        channels = {
            str(item.channel or "").strip().lower()
            for item in items
            if str(item.channel or "").strip()
        }
        workbooks = {
            str(item.workbook_path or "").strip()
            for item in items
            if str(item.workbook_path or "").strip()
        }
        inflow_count = sum(1 for item in items if _direction_index(item.direction) == 0)
        outflow_count = sum(1 for item in items if _direction_index(item.direction) == 1)
        flow_total = inflow_count + outflow_count
        labeled_rows = sum(1 for item in items if item.label_status in {"positive", "negative"})
        positive_rows = sum(1 for item in items if item.label_status == "positive")
        negative_rows = sum(1 for item in items if item.label_status == "negative")
        flagged_rows = sum(1 for item in items if item.review_flags)
        mirrored_rows = sum(1 for item in items if item.mirror_match_count > 1)
        mirrored_groups = len({item.mirror_group_id for item in items if item.mirror_group_id})
        possible_mirrored_rows = sum(1 for item in items if item.possible_mirror_match_count > 1)
        possible_mirrored_groups = len({item.possible_mirror_group_id for item in items if item.possible_mirror_group_id})
        for item in items:
            role_key = str(item.role_label or "unknown").strip() or "unknown"
            role_counts[role_key] = role_counts.get(role_key, 0) + 1
            counterparty = str(item.counterparty or "").strip()
            if counterparty:
                counterparty_counts[counterparty] = counterparty_counts.get(counterparty, 0) + 1
        counterparties = {key.strip().lower() for key in counterparty_counts}
        dominant_role = max(sorted(role_counts), key=lambda key: role_counts[key]) if role_counts else "unknown"
        best_name = max(
            (str(item.owner_name or "").strip() for item in items),
            key=lambda value: (1 if value else 0, len(value)),
            default="",
        )
        best_confidence = max(
            (str(item.owner_confidence or "").strip().lower() for item in items),
            key=_confidence_rank,
            default="",
        )
        owner_role = owner_role_annotations.get(owner_id)
        pattern_tags = _owner_pattern_tags(
            tx_count=len(items),
            unique_counterparties=len(counterparties),
            inflow_count=inflow_count,
            outflow_count=outflow_count,
        )
        priority_score = _owner_priority_score(
            dominant_role=dominant_role,
            owner_confidence=best_confidence,
            unique_counterparties=len(counterparties),
            inflow_count=inflow_count,
            outflow_count=outflow_count,
            flagged_rows=flagged_rows,
            positive_rows=positive_rows,
            pattern_tags=pattern_tags,
        )
        top_counterparties = [
            {"counterparty": name, "tx_count": count}
            for name, count in sorted(counterparty_counts.items(), key=lambda item: (-item[1], item[0]))[:5]
        ]
        summaries.append(
            OwnerSummaryRow(
                owner_id=owner_id,
                owner_name=best_name,
                owner_confidence=best_confidence,
                dominant_role=dominant_role,
                reviewed_role=owner_role.role_label if owner_role else "",
                reviewed_confidence=owner_role.confidence if owner_role else "",
                reviewed_note=owner_role.note if owner_role else "",
                role_counts=role_counts,
                pattern_tags=pattern_tags,
                top_counterparties=top_counterparties,
                priority_score=priority_score,
                priority_rank=0,
                tx_count=len(items),
                unique_counterparties=len(counterparties),
                inflow_count=inflow_count,
                outflow_count=outflow_count,
                inflow_ratio=(inflow_count / flow_total) if flow_total else 0.0,
                outflow_ratio=(outflow_count / flow_total) if flow_total else 0.0,
                collect_and_split=inflow_count > 0 and outflow_count > 0,
                channel_count=len(channels),
                workbook_count=len(workbooks),
                mirrored_rows=mirrored_rows,
                mirrored_groups=mirrored_groups,
                possible_mirrored_rows=possible_mirrored_rows,
                possible_mirrored_groups=possible_mirrored_groups,
                labeled_rows=labeled_rows,
                positive_rows=positive_rows,
                negative_rows=negative_rows,
                flagged_rows=flagged_rows,
            )
        )
    ranked = sorted(
        summaries,
        key=lambda item: (
            -item.priority_score,
            -item.positive_rows,
            -item.flagged_rows,
            -item.tx_count,
            item.owner_id,
        ),
    )
    ranked = [
        OwnerSummaryRow(
            owner_id=item.owner_id,
            owner_name=item.owner_name,
            owner_confidence=item.owner_confidence,
            dominant_role=item.dominant_role,
            reviewed_role=item.reviewed_role,
            reviewed_confidence=item.reviewed_confidence,
            reviewed_note=item.reviewed_note,
            role_counts=item.role_counts,
            pattern_tags=item.pattern_tags,
            top_counterparties=item.top_counterparties,
            priority_score=item.priority_score,
            priority_rank=index,
            tx_count=item.tx_count,
            unique_counterparties=item.unique_counterparties,
            inflow_count=item.inflow_count,
            outflow_count=item.outflow_count,
            inflow_ratio=item.inflow_ratio,
            outflow_ratio=item.outflow_ratio,
            collect_and_split=item.collect_and_split,
            channel_count=item.channel_count,
            workbook_count=item.workbook_count,
            mirrored_rows=item.mirrored_rows,
            mirrored_groups=item.mirrored_groups,
            possible_mirrored_rows=item.possible_mirrored_rows,
            possible_mirrored_groups=item.possible_mirrored_groups,
            labeled_rows=item.labeled_rows,
            positive_rows=item.positive_rows,
            negative_rows=item.negative_rows,
            flagged_rows=item.flagged_rows,
        )
        for index, item in enumerate(ranked, start=1)
    ]
    return OwnerSummaryReport(
        total_rows=len(rows),
        covered_rows=sum(item.tx_count for item in ranked),
        skipped_rows=len(rows) - sum(item.tx_count for item in ranked),
        total_owners=len(ranked),
        owners=ranked,
    )


def build_owner_review_rows(summary: OwnerSummaryReport) -> list[OwnerReviewRow]:
    return [
        OwnerReviewRow(
            owner_id=item.owner_id,
            owner_name=item.owner_name,
            priority_rank=item.priority_rank,
            priority_score=item.priority_score,
            dominant_role=item.dominant_role,
            reviewed_role=item.reviewed_role,
            reviewed_confidence=item.reviewed_confidence,
            reviewed_note=item.reviewed_note,
            pattern_tags=item.pattern_tags,
            top_counterparties=item.top_counterparties,
        )
        for item in summary.owners
    ]


def normalize_workbook(
    xlsx_path: str | Path,
    manifests: list[LabelManifest],
    role_annotation_path: str | Path | None = None,
    owner_annotation_path: str | Path | None = None,
) -> list[NormalizedTransaction]:
    workbook = Path(xlsx_path)
    examples = build_training_examples(
        workbook,
        manifests,
        role_annotation_path=role_annotation_path,
        owner_annotation_path=owner_annotation_path,
    )
    duplicate_ids = build_duplicate_transaction_ids(examples)
    owner_stats = _owner_aggregate_map(examples)
    rows: list[NormalizedTransaction] = []
    for example in examples:
        aggregate = owner_stats.get(
            example.owner_id,
            {
                "owner_tx_count": 0,
                "owner_unique_counterparties": 0,
                "owner_inflow_ratio": 0.0,
                "owner_outflow_ratio": 0.0,
                "owner_collect_and_split": False,
            },
        )
        rows.append(
            NormalizedTransaction(
                workbook_path=str(workbook),
                row_index=example.row_index,
                transaction_id=example.transaction_id,
                amount=example.amount,
                timestamp=example.timestamp,
                counterparty=example.counterparty,
                counterparty_account=example.counterparty_account,
                counterparty_name=example.counterparty_name,
                direction=example.direction,
                channel=example.channel,
                remark=example.remark,
                subject_account=example.subject_account,
                tx_id_secondary=example.tx_id_secondary,
                balance_after=example.balance_after,
                payer_account=example.payer_account,
                payer_bank_name=example.payer_bank_name,
                payer_bank_card=example.payer_bank_card,
                payee_account=example.payee_account,
                payee_bank_name=example.payee_bank_name,
                payee_bank_card=example.payee_bank_card,
                merchant_id=example.merchant_id,
                merchant_name=example.merchant_name,
                flow_family=example.flow_family,
                trade_pattern=example.trade_pattern,
                buyer_account=example.buyer_account,
                seller_account=example.seller_account,
                seller_proxy_name=example.seller_proxy_name,
                rule_reason=example.rule_reason,
                is_qr_transfer=example.is_qr_transfer,
                is_red_packet=example.is_red_packet,
                is_merchant_consume=example.is_merchant_consume,
                is_withdrawal_like=example.is_withdrawal_like,
                is_platform_settlement=example.is_platform_settlement,
                is_failed_or_invalid=example.is_failed_or_invalid,
                is_trade_like=example.is_trade_like,
                label_status=example.label_status,
                label=example.label,
                subject=example.subject,
                role_label=example.role_label,
                role_confidence=example.role_confidence,
                role_scene=example.role_scene,
                owner_id=example.owner_id,
                owner_name=example.owner_name,
                owner_confidence=example.owner_confidence,
                owner_tx_count=int(aggregate["owner_tx_count"]),
                owner_unique_counterparties=int(aggregate["owner_unique_counterparties"]),
                owner_inflow_ratio=float(aggregate["owner_inflow_ratio"]),
                owner_outflow_ratio=float(aggregate["owner_outflow_ratio"]),
                owner_collect_and_split=bool(aggregate["owner_collect_and_split"]),
                mirror_group_id="",
                mirror_match_count=1,
                mirror_workbook_count=1,
                mirror_has_opposite_direction=False,
                possible_mirror_group_id="",
                possible_mirror_match_count=1,
                possible_mirror_workbook_count=1,
                possible_mirror_score=0.0,
                possible_mirror_confidence="",
                mirror_review_decision="",
                mirror_review_confidence="",
                mirror_review_note="",
                review_flags=build_review_flags(example, duplicate_ids),
            )
        )
    return rows


def export_normalized_ledgers(
    root: str | Path,
    manifests: list[LabelManifest],
    role_annotation_path: str | Path | None = None,
    owner_annotation_path: str | Path | None = None,
    mirror_annotation_path: str | Path | None = None,
    csv_path: str | Path | None = None,
    jsonl_path: str | Path | None = None,
) -> list[NormalizedTransaction]:
    transactions: list[NormalizedTransaction] = []
    for workbook in sorted(Path(root).rglob("*.xlsx")):
        if workbook.name.startswith("~$") or workbook.name.startswith(".~lock."):
            continue
        transactions.extend(
            normalize_workbook(
                workbook,
                manifests,
                role_annotation_path=role_annotation_path,
                owner_annotation_path=owner_annotation_path,
            )
        )
    transactions = _apply_mirror_groups(transactions)
    transactions = _apply_possible_mirror_groups(transactions)
    transactions = _apply_mirror_annotations(transactions, mirror_annotation_path=mirror_annotation_path)

    if csv_path:
        path = Path(csv_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8", newline="") as handle:
            fieldnames = [
                "workbook_path",
                "row_index",
                "transaction_id",
                "amount",
                "timestamp",
                "counterparty",
                "counterparty_account",
                "counterparty_name",
                "direction",
                "channel",
                "remark",
                "subject_account",
                "tx_id_secondary",
                "balance_after",
                "payer_account",
                "payer_bank_name",
                "payer_bank_card",
                "payee_account",
                "payee_bank_name",
                "payee_bank_card",
                "merchant_id",
                "merchant_name",
                "flow_family",
                "trade_pattern",
                "buyer_account",
                "seller_account",
                "seller_proxy_name",
                "rule_reason",
                "is_qr_transfer",
                "is_red_packet",
                "is_merchant_consume",
                "is_withdrawal_like",
                "is_platform_settlement",
                "is_failed_or_invalid",
                "is_trade_like",
                "label_status",
                "label",
                "subject",
                "role_label",
                "role_confidence",
                "role_scene",
                "owner_id",
                "owner_name",
                "owner_confidence",
                "owner_tx_count",
                "owner_unique_counterparties",
                "owner_inflow_ratio",
                "owner_outflow_ratio",
                "owner_collect_and_split",
                "mirror_group_id",
                "mirror_match_count",
                "mirror_workbook_count",
                "mirror_has_opposite_direction",
                "possible_mirror_group_id",
                "possible_mirror_match_count",
                "possible_mirror_workbook_count",
                "possible_mirror_score",
                "possible_mirror_confidence",
                "mirror_review_decision",
                "mirror_review_confidence",
                "mirror_review_note",
                "review_flags",
            ]
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            for item in transactions:
                row = item.to_dict()
                row["review_flags"] = "|".join(item.review_flags)
                writer.writerow(row)
    if jsonl_path:
        path = Path(jsonl_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8") as handle:
            for item in transactions:
                handle.write(json.dumps(item.to_dict(), ensure_ascii=False) + "\n")
    return transactions


def load_normalized_ledgers(path: str | Path) -> list[NormalizedTransaction]:
    resolved = Path(path)
    rows: list[dict[str, Any]] = []
    if resolved.suffix.lower() == ".csv":
        with resolved.open(encoding="utf-8", newline="") as handle:
            rows = list(csv.DictReader(handle))
    elif resolved.suffix.lower() == ".jsonl":
        with resolved.open(encoding="utf-8") as handle:
            rows = [json.loads(line) for line in handle]
    else:
        raise ValueError(f"normalized ledger file must be .csv or .jsonl: {resolved}")
    results: list[NormalizedTransaction] = []
    for row in rows:
        flags = str(row.get("review_flags", ""))
        review_flags = [item for item in flags.split("|") if item] if isinstance(flags, str) else list(row.get("review_flags", []))
        results.append(
            NormalizedTransaction(
                workbook_path=str(row.get("workbook_path", "")),
                row_index=int(row.get("row_index", 0)),
                transaction_id=str(row.get("transaction_id", "")),
                amount=str(row.get("amount", "")),
                timestamp=str(row.get("timestamp", "")),
                counterparty=str(row.get("counterparty", "")),
                counterparty_account=str(row.get("counterparty_account", "")),
                counterparty_name=str(row.get("counterparty_name", "")),
                direction=str(row.get("direction", "")),
                channel=str(row.get("channel", "")),
                remark=str(row.get("remark", "")),
                subject_account=str(row.get("subject_account", "")),
                tx_id_secondary=str(row.get("tx_id_secondary", "")),
                balance_after=str(row.get("balance_after", "")),
                payer_account=str(row.get("payer_account", "")),
                payer_bank_name=str(row.get("payer_bank_name", "")),
                payer_bank_card=str(row.get("payer_bank_card", "")),
                payee_account=str(row.get("payee_account", "")),
                payee_bank_name=str(row.get("payee_bank_name", "")),
                payee_bank_card=str(row.get("payee_bank_card", "")),
                merchant_id=str(row.get("merchant_id", "")),
                merchant_name=str(row.get("merchant_name", "")),
                flow_family=str(row.get("flow_family", "")),
                trade_pattern=str(row.get("trade_pattern", "")),
                buyer_account=str(row.get("buyer_account", "")),
                seller_account=str(row.get("seller_account", "")),
                seller_proxy_name=str(row.get("seller_proxy_name", "")),
                rule_reason=str(row.get("rule_reason", "")),
                is_qr_transfer=str(row.get("is_qr_transfer", "False")).lower() == "true",
                is_red_packet=str(row.get("is_red_packet", "False")).lower() == "true",
                is_merchant_consume=str(row.get("is_merchant_consume", "False")).lower() == "true",
                is_withdrawal_like=str(row.get("is_withdrawal_like", "False")).lower() == "true",
                is_platform_settlement=str(row.get("is_platform_settlement", "False")).lower() == "true",
                is_failed_or_invalid=str(row.get("is_failed_or_invalid", "False")).lower() == "true",
                is_trade_like=str(row.get("is_trade_like", "False")).lower() == "true",
                label_status=str(row.get("label_status", "")),
                label=str(row.get("label", "")),
                subject=str(row.get("subject", "")),
                role_label=str(row.get("role_label", "")),
                role_confidence=str(row.get("role_confidence", "")),
                role_scene=str(row.get("role_scene", "")),
                owner_id=str(row.get("owner_id", "")),
                owner_name=str(row.get("owner_name", "")),
                owner_confidence=str(row.get("owner_confidence", "")),
                owner_tx_count=int(row.get("owner_tx_count", 0)),
                owner_unique_counterparties=int(row.get("owner_unique_counterparties", 0)),
                owner_inflow_ratio=float(row.get("owner_inflow_ratio", 0.0)),
                owner_outflow_ratio=float(row.get("owner_outflow_ratio", 0.0)),
                owner_collect_and_split=str(row.get("owner_collect_and_split", "False")).lower() == "true",
                mirror_group_id=str(row.get("mirror_group_id", "")),
                mirror_match_count=int(row.get("mirror_match_count", 1)),
                mirror_workbook_count=int(row.get("mirror_workbook_count", 1)),
                mirror_has_opposite_direction=str(row.get("mirror_has_opposite_direction", "False")).lower() == "true",
                possible_mirror_group_id=str(row.get("possible_mirror_group_id", "")),
                possible_mirror_match_count=int(row.get("possible_mirror_match_count", 1)),
                possible_mirror_workbook_count=int(row.get("possible_mirror_workbook_count", 1)),
                possible_mirror_score=float(row.get("possible_mirror_score", 0.0)),
                possible_mirror_confidence=str(row.get("possible_mirror_confidence", "")),
                mirror_review_decision=str(row.get("mirror_review_decision", "")),
                mirror_review_confidence=str(row.get("mirror_review_confidence", "")),
                mirror_review_note=str(row.get("mirror_review_note", "")),
                review_flags=review_flags,
            )
        )
    return results


def build_mirror_review_rows(
    rows: list[NormalizedTransaction],
    include_confirmed: bool = True,
    include_possible: bool = True,
) -> list[MirrorReviewRow]:
    review_rows: list[MirrorReviewRow] = []
    for row in rows:
        if include_confirmed and row.mirror_match_count > 1:
            review_rows.append(
                MirrorReviewRow(
                    review_entity="confirmed_mirror",
                    mirror_group_id=row.mirror_group_id,
                    transaction_id=row.transaction_id,
                    workbook_path=row.workbook_path,
                    row_index=row.row_index,
                    amount=row.amount,
                    timestamp=row.timestamp,
                    counterparty=row.counterparty,
                    direction=row.direction,
                    channel=row.channel,
                    owner_id=row.owner_id,
                    mirror_status="confirmed",
                    mirror_match_count=row.mirror_match_count,
                    mirror_workbook_count=row.mirror_workbook_count,
                    mirror_score=1.0,
                    mirror_confidence="high",
                    review_decision=row.mirror_review_decision,
                    review_note=row.mirror_review_note,
                )
            )
        elif include_possible and row.possible_mirror_match_count > 1:
            review_rows.append(
                MirrorReviewRow(
                    review_entity="possible_mirror",
                    mirror_group_id=row.possible_mirror_group_id,
                    transaction_id=row.transaction_id,
                    workbook_path=row.workbook_path,
                    row_index=row.row_index,
                    amount=row.amount,
                    timestamp=row.timestamp,
                    counterparty=row.counterparty,
                    direction=row.direction,
                    channel=row.channel,
                    owner_id=row.owner_id,
                    mirror_status="possible",
                    mirror_match_count=row.possible_mirror_match_count,
                    mirror_workbook_count=row.possible_mirror_workbook_count,
                    mirror_score=row.possible_mirror_score,
                    mirror_confidence=row.possible_mirror_confidence,
                    review_decision=row.mirror_review_decision,
                    review_note=row.mirror_review_note,
                )
            )
    return review_rows


def export_mirror_review_csv(rows: list[MirrorReviewRow], output_path: str | Path) -> Path:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        fieldnames = [
            "mirror_group_id",
            "transaction_id",
            "mirror_status",
            "mirror_score",
            "mirror_confidence",
            "amount",
            "timestamp",
            "counterparty",
            "direction",
            "channel",
            "review_decision",
            "review_options",
            "review_note",
        ]
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            payload = row.to_dict()
            writer.writerow({key: payload.get(key, "") for key in fieldnames})
    return path


def export_mirror_review_xlsx(rows: list[MirrorReviewRow], output_path: str | Path) -> Path:
    headers = [
        "mirror_group_id",
        "transaction_id",
        "mirror_status",
        "mirror_score",
        "mirror_confidence",
        "amount",
        "timestamp",
        "counterparty",
        "direction",
        "channel",
        "review_decision",
        "review_options",
        "review_note",
    ]
    table_rows = []
    for item in rows:
        payload = item.to_dict()
        table_rows.append({key: payload.get(key, "") for key in headers})
    return write_xlsx_table(output_path, headers=headers, rows=table_rows, row_fills=["yellow"] * len(rows))


def build_mirror_annotations(review_path: str | Path) -> list[MirrorAnnotationRow]:
    path = Path(review_path)
    source_rows: list[dict[str, str]]
    if path.suffix.lower() == ".xlsx":
        source_rows = [item.values for item in load_xlsx_styled_rows(path)]
    else:
        with path.open(encoding="utf-8", newline="") as handle:
            source_rows = list(csv.DictReader(handle))
    rows: list[MirrorAnnotationRow] = []
    for row in source_rows:
        decision = str(row.get("review_decision", "")).strip().lower()
        if decision not in {"confirmed_mirror", "rejected_mirror", "uncertain"}:
            continue
        confidence = "high" if decision == "confirmed_mirror" else "low" if decision == "rejected_mirror" else "medium"
        rows.append(
            MirrorAnnotationRow(
                mirror_group_id=str(row.get("mirror_group_id", "")).strip(),
                transaction_id=str(row.get("transaction_id", "")).strip(),
                decision=decision,
                confidence=confidence,
                note=str(row.get("review_note", "")).strip(),
            )
        )
    return rows


def build_ledger_review_rows(rows: list[NormalizedTransaction]) -> list[LedgerReviewRow]:
    review_rows: list[LedgerReviewRow] = []
    for row in rows:
        remark_excerpt = str(row.remark or "").strip()
        if len(remark_excerpt) > 120:
            remark_excerpt = remark_excerpt[:117] + "..."
        review_rows.append(
            LedgerReviewRow(
                record_id=f"{row.workbook_path}:{row.row_index}",
                tx_time=row.timestamp,
                amount=row.amount,
                direction=row.direction,
                trade_pattern=row.trade_pattern,
                is_qr_transfer=row.is_qr_transfer,
                buyer_account=row.buyer_account,
                seller_account=row.seller_account,
                counterparty_name=row.counterparty_name or row.counterparty,
                merchant_name=row.merchant_name,
                rule_reason=row.rule_reason,
                remark_excerpt=remark_excerpt,
            )
        )
    return review_rows


def _remark_excerpt(value: str, limit: int = 120) -> str:
    text = str(value or "").strip()
    if len(text) > limit:
        return text[: limit - 3] + "..."
    return text


def _has_rule_hits(row: NormalizedTransaction) -> bool:
    return any(
        [
            str(row.rule_reason or "").strip(),
            row.is_qr_transfer,
            row.is_red_packet,
            row.is_platform_settlement,
            row.is_withdrawal_like,
            row.is_failed_or_invalid,
            row.is_trade_like,
        ]
    )


def build_rule_audit_rows(rows: list[NormalizedTransaction], include_all: bool = False) -> list[RuleAuditRow]:
    audit_rows: list[RuleAuditRow] = []
    for row in rows:
        if not include_all and not _has_rule_hits(row):
            continue
        reason_tags = [item for item in str(row.rule_reason or "").split("|") if item]
        audit_rows.append(
            RuleAuditRow(
                record_id=f"{row.workbook_path}:{row.row_index}",
                tx_time=row.timestamp,
                amount=row.amount,
                direction=row.direction,
                channel=row.channel,
                flow_family=row.flow_family,
                trade_pattern=row.trade_pattern,
                rule_reason=row.rule_reason,
                rule_hit_count=len(reason_tags),
                is_trade_like=row.is_trade_like,
                is_qr_transfer=row.is_qr_transfer,
                is_red_packet=row.is_red_packet,
                is_platform_settlement=row.is_platform_settlement,
                is_withdrawal_like=row.is_withdrawal_like,
                is_failed_or_invalid=row.is_failed_or_invalid,
                buyer_account=row.buyer_account,
                seller_account=row.seller_account,
                counterparty_name=row.counterparty_name or row.counterparty,
                merchant_name=row.merchant_name,
                remark_excerpt=_remark_excerpt(row.remark),
            )
        )
    return audit_rows


def _summary_buckets(counts: dict[str, int], total: int) -> list[RuleSummaryBucket]:
    return [
        RuleSummaryBucket(key=key, row_count=count, ratio=(count / total) if total else 0.0)
        for key, count in sorted(counts.items(), key=lambda item: (-item[1], item[0]))
    ]


def build_rule_summary(rows: list[NormalizedTransaction]) -> RuleSummaryReport:
    channel_counts: dict[str, int] = {}
    trade_pattern_counts: dict[str, int] = {}
    rule_reason_counts: dict[str, int] = {}
    rows_with_hits = 0
    trade_like_rows = 0
    qr_rows = 0
    platform_rows = 0
    withdrawal_rows = 0
    failed_rows = 0
    for row in rows:
        if row.is_trade_like:
            trade_like_rows += 1
        if row.is_qr_transfer:
            qr_rows += 1
        if row.is_platform_settlement:
            platform_rows += 1
        if row.is_withdrawal_like:
            withdrawal_rows += 1
        if row.is_failed_or_invalid:
            failed_rows += 1
        hit = _has_rule_hits(row)
        if not hit:
            continue
        rows_with_hits += 1
        channel = str(row.channel or "").strip() or "unknown"
        trade_pattern = str(row.trade_pattern or "").strip() or "unknown"
        channel_counts[channel] = channel_counts.get(channel, 0) + 1
        trade_pattern_counts[trade_pattern] = trade_pattern_counts.get(trade_pattern, 0) + 1
        tags = [item for item in str(row.rule_reason or "").split("|") if item]
        if not tags:
            tags = ["implicit_rule_signal"]
        for tag in tags:
            rule_reason_counts[tag] = rule_reason_counts.get(tag, 0) + 1
    return RuleSummaryReport(
        total_rows=len(rows),
        rows_with_rule_hits=rows_with_hits,
        trade_like_rows=trade_like_rows,
        qr_rows=qr_rows,
        platform_settlement_rows=platform_rows,
        withdrawal_rows=withdrawal_rows,
        failed_or_invalid_rows=failed_rows,
        by_channel=_summary_buckets(channel_counts, rows_with_hits),
        by_trade_pattern=_summary_buckets(trade_pattern_counts, rows_with_hits),
        by_rule_reason=_summary_buckets(rule_reason_counts, rows_with_hits),
    )


def _load_review_rows(review_path: str | Path) -> list[dict[str, str]]:
    path = Path(review_path)
    if path.suffix.lower() == ".xlsx":
        return [{str(k): str(v) for k, v in item.values.items()} for item in load_xlsx_styled_rows(path)]
    with path.open(encoding="utf-8", newline="") as handle:
        return [{str(k): str(v) for k, v in row.items()} for row in csv.DictReader(handle)]


def _review_bucket_rows(items: list[dict[str, str]]) -> RuleReviewBucket:
    reviewed_rows = len(items)
    confirmed_positive = sum(1 for item in items if item["review_label"] == "confirmed_positive")
    confirmed_negative = sum(1 for item in items if item["review_label"] == "confirmed_negative")
    uncertain = sum(1 for item in items if item["review_label"] == "uncertain")
    return RuleReviewBucket(
        key=items[0]["bucket_key"],
        reviewed_rows=reviewed_rows,
        confirmed_positive=confirmed_positive,
        confirmed_negative=confirmed_negative,
        uncertain=uncertain,
        confirmed_positive_rate=(confirmed_positive / reviewed_rows) if reviewed_rows else 0.0,
    )


def build_rule_review_summary(rows: list[NormalizedTransaction], review_path: str | Path) -> RuleReviewSummary:
    review_rows = _load_review_rows(review_path)
    normalized_by_record = {f"{item.workbook_path}:{item.row_index}": item for item in rows}
    matched: list[tuple[NormalizedTransaction, str]] = []
    for review in review_rows:
        review_label = str(review.get("review_label", "")).strip().lower()
        if review_label not in {"confirmed_positive", "confirmed_negative", "uncertain"}:
            continue
        record_id = str(review.get("record_id", "")).strip()
        normalized = normalized_by_record.get(record_id)
        if normalized is None:
            continue
        matched.append((normalized, review_label))

    trade_pattern_groups: dict[str, list[dict[str, str]]] = {}
    rule_reason_groups: dict[str, list[dict[str, str]]] = {}
    confirmed_positive = confirmed_negative = uncertain = 0
    for row, review_label in matched:
        if review_label == "confirmed_positive":
            confirmed_positive += 1
        elif review_label == "confirmed_negative":
            confirmed_negative += 1
        else:
            uncertain += 1
        trade_key = str(row.trade_pattern or "").strip() or "unknown"
        trade_pattern_groups.setdefault(trade_key, []).append({"bucket_key": trade_key, "review_label": review_label})
        reason_tags = [item for item in str(row.rule_reason or "").split("|") if item]
        if not reason_tags:
            reason_tags = ["implicit_rule_signal"]
        for tag in reason_tags:
            rule_reason_groups.setdefault(tag, []).append({"bucket_key": tag, "review_label": review_label})

    by_trade_pattern = sorted(
        (_review_bucket_rows(items) for items in trade_pattern_groups.values()),
        key=lambda item: (-item.confirmed_positive_rate, -item.reviewed_rows, item.key),
    )
    by_rule_reason = sorted(
        (_review_bucket_rows(items) for items in rule_reason_groups.values()),
        key=lambda item: (-item.confirmed_positive_rate, -item.reviewed_rows, item.key),
    )
    return RuleReviewSummary(
        review_total=len(review_rows),
        matched_rows=len(matched),
        confirmed_positive=confirmed_positive,
        confirmed_negative=confirmed_negative,
        uncertain=uncertain,
        by_trade_pattern=by_trade_pattern,
        by_rule_reason=by_rule_reason,
    )


def export_ledger_review_csv(rows: list[LedgerReviewRow], output_path: str | Path) -> Path:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "record_id",
        "tx_time",
        "amount",
        "direction",
        "trade_pattern",
        "is_qr_transfer",
        "buyer_account",
        "seller_account",
        "counterparty_name",
        "merchant_name",
        "rule_reason",
        "remark_excerpt",
        "review_label",
        "review_options",
        "review_note",
    ]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            payload = row.to_dict()
            writer.writerow({key: payload.get(key, "") for key in fieldnames})
    return path


def export_ledger_review_xlsx(rows: list[LedgerReviewRow], output_path: str | Path) -> Path:
    headers = [
        "record_id",
        "tx_time",
        "amount",
        "direction",
        "trade_pattern",
        "is_qr_transfer",
        "buyer_account",
        "seller_account",
        "counterparty_name",
        "merchant_name",
        "rule_reason",
        "remark_excerpt",
        "review_label",
        "review_options",
        "review_note",
    ]
    return write_xlsx_table(output_path, headers=headers, rows=[item.to_dict() for item in rows], row_fills=["yellow"] * len(rows))


def export_rule_audit_csv(rows: list[RuleAuditRow], output_path: str | Path) -> Path:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "record_id",
        "tx_time",
        "amount",
        "direction",
        "channel",
        "flow_family",
        "trade_pattern",
        "rule_reason",
        "rule_hit_count",
        "is_trade_like",
        "is_qr_transfer",
        "is_red_packet",
        "is_platform_settlement",
        "is_withdrawal_like",
        "is_failed_or_invalid",
        "buyer_account",
        "seller_account",
        "counterparty_name",
        "merchant_name",
        "remark_excerpt",
    ]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            payload = row.to_dict()
            writer.writerow({key: payload.get(key, "") for key in fieldnames})
    return path


def export_rule_audit_xlsx(rows: list[RuleAuditRow], output_path: str | Path) -> Path:
    headers = [
        "record_id",
        "tx_time",
        "amount",
        "direction",
        "channel",
        "flow_family",
        "trade_pattern",
        "rule_reason",
        "rule_hit_count",
        "is_trade_like",
        "is_qr_transfer",
        "is_red_packet",
        "is_platform_settlement",
        "is_withdrawal_like",
        "is_failed_or_invalid",
        "buyer_account",
        "seller_account",
        "counterparty_name",
        "merchant_name",
        "remark_excerpt",
    ]
    return write_xlsx_table(output_path, headers=headers, rows=[item.to_dict() for item in rows], row_fills=["yellow"] * len(rows))


def export_rule_summary_json(summary: RuleSummaryReport, output_path: str | Path) -> Path:
    return write_json_file(output_path, summary.to_dict())


def export_rule_summary_markdown(summary: RuleSummaryReport, output_path: str | Path) -> Path:
    lines = [
        "# Rule Summary",
        "",
        f"- total_rows: {summary.total_rows}",
        f"- rows_with_rule_hits: {summary.rows_with_rule_hits}",
        f"- trade_like_rows: {summary.trade_like_rows}",
        f"- qr_rows: {summary.qr_rows}",
        f"- platform_settlement_rows: {summary.platform_settlement_rows}",
        f"- withdrawal_rows: {summary.withdrawal_rows}",
        f"- failed_or_invalid_rows: {summary.failed_or_invalid_rows}",
        "",
        "## By Channel",
    ]
    if summary.by_channel:
        lines.extend([f"- {item.key}: {item.row_count} ({item.ratio:.2%})" for item in summary.by_channel])
    else:
        lines.append("- none")
    lines.append("")
    lines.append("## By Trade Pattern")
    if summary.by_trade_pattern:
        lines.extend([f"- {item.key}: {item.row_count} ({item.ratio:.2%})" for item in summary.by_trade_pattern])
    else:
        lines.append("- none")
    lines.append("")
    lines.append("## By Rule Reason")
    if summary.by_rule_reason:
        lines.extend([f"- {item.key}: {item.row_count} ({item.ratio:.2%})" for item in summary.by_rule_reason])
    else:
        lines.append("- none")
    return write_markdown_lines(output_path, lines)


def export_rule_review_summary_json(summary: RuleReviewSummary, output_path: str | Path) -> Path:
    return write_json_file(output_path, summary.to_dict())


def export_rule_review_summary_markdown(summary: RuleReviewSummary, output_path: str | Path) -> Path:
    lines = [
        "# Rule Review Summary",
        "",
        f"- review_total: {summary.review_total}",
        f"- matched_rows: {summary.matched_rows}",
        f"- confirmed_positive: {summary.confirmed_positive}",
        f"- confirmed_negative: {summary.confirmed_negative}",
        f"- uncertain: {summary.uncertain}",
        "",
        "## By Trade Pattern",
    ]
    if summary.by_trade_pattern:
        lines.extend(
            [
                f"- {item.key}: reviewed={item.reviewed_rows}, positive={item.confirmed_positive}, negative={item.confirmed_negative}, uncertain={item.uncertain}, positive_rate={item.confirmed_positive_rate:.2%}"
                for item in summary.by_trade_pattern
            ]
        )
    else:
        lines.append("- none")
    lines.append("")
    lines.append("## By Rule Reason")
    if summary.by_rule_reason:
        lines.extend(
            [
                f"- {item.key}: reviewed={item.reviewed_rows}, positive={item.confirmed_positive}, negative={item.confirmed_negative}, uncertain={item.uncertain}, positive_rate={item.confirmed_positive_rate:.2%}"
                for item in summary.by_rule_reason
            ]
        )
    else:
        lines.append("- none")
    return write_markdown_lines(output_path, lines)


def export_mirror_annotations_csv(rows: list[MirrorAnnotationRow], output_path: str | Path) -> Path:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["mirror_group_id", "transaction_id", "decision", "confidence", "note"])
        writer.writeheader()
        for row in rows:
            writer.writerow(row.to_dict())
    return path


def export_mirror_annotations_jsonl(rows: list[MirrorAnnotationRow], output_path: str | Path) -> Path:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row.to_dict(), ensure_ascii=False) + "\n")
    return path


def summarize_graph_dataset(
    root: str | Path,
    manifests: list[LabelManifest],
    role_annotation_path: str | Path | None = None,
    owner_annotation_path: str | Path | None = None,
    mirror_annotation_path: str | Path | None = None,
) -> GraphDatasetSummary:
    workbooks: list[dict[str, Any]] = []
    total_rows = labeled_rows = positive_rows = negative_rows = unlabeled_rows = flagged_rows = 0
    mirrored_rows = mirrored_groups = possible_mirrored_rows = possible_mirrored_groups = 0
    confirmed_mirror_rows = rejected_mirror_rows = uncertain_mirror_rows = 0
    role_counts: dict[str, int] = {}
    owner_counts: dict[str, int] = {}
    all_rows = export_normalized_ledgers(
        root,
        manifests,
        role_annotation_path=role_annotation_path,
        owner_annotation_path=owner_annotation_path,
        mirror_annotation_path=mirror_annotation_path,
    )
    mirrored_rows = sum(1 for item in all_rows if item.mirror_match_count > 1)
    mirrored_groups = len({item.mirror_group_id for item in all_rows if item.mirror_group_id})
    possible_mirrored_rows = sum(1 for item in all_rows if item.possible_mirror_match_count > 1)
    possible_mirrored_groups = len({item.possible_mirror_group_id for item in all_rows if item.possible_mirror_group_id})
    confirmed_mirror_rows = sum(1 for item in all_rows if item.mirror_review_decision == "confirmed_mirror")
    rejected_mirror_rows = sum(1 for item in all_rows if item.mirror_review_decision == "rejected_mirror")
    uncertain_mirror_rows = sum(1 for item in all_rows if item.mirror_review_decision == "uncertain")
    workbook_map: dict[str, list[NormalizedTransaction]] = {}
    for row in all_rows:
        workbook_map.setdefault(row.workbook_path, []).append(row)
    for workbook, rows in sorted(workbook_map.items()):
        total_rows += len(rows)
        labeled_rows += sum(1 for item in rows if item.label_status in {"positive", "negative"})
        positive_rows += sum(1 for item in rows if item.label_status == "positive")
        negative_rows += sum(1 for item in rows if item.label_status == "negative")
        unlabeled_rows += sum(1 for item in rows if item.label_status == "unlabeled")
        flagged = sum(1 for item in rows if item.review_flags)
        flagged_rows += flagged
        for item in rows:
            role_key = item.role_label or "unknown"
            role_counts[role_key] = role_counts.get(role_key, 0) + 1
            owner_key = item.owner_id or "unknown"
            owner_counts[owner_key] = owner_counts.get(owner_key, 0) + 1
        workbooks.append(
            {
                "path": str(workbook),
                "rows": len(rows),
                "labeled_rows": sum(1 for item in rows if item.label_status in {"positive", "negative"}),
                "flagged_rows": flagged,
                "mirrored_rows": sum(1 for item in rows if item.mirror_match_count > 1),
                "possible_mirrored_rows": sum(1 for item in rows if item.possible_mirror_match_count > 1),
                "confirmed_mirror_rows": sum(1 for item in rows if item.mirror_review_decision == "confirmed_mirror"),
                "rejected_mirror_rows": sum(1 for item in rows if item.mirror_review_decision == "rejected_mirror"),
                "uncertain_mirror_rows": sum(1 for item in rows if item.mirror_review_decision == "uncertain"),
                "role_counts": {
                    key: sum(1 for item in rows if (item.role_label or "unknown") == key)
                    for key in sorted({item.role_label or "unknown" for item in rows})
                },
                "owner_counts": {
                    key: sum(1 for item in rows if (item.owner_id or "unknown") == key)
                    for key in sorted({item.owner_id or "unknown" for item in rows})
                },
            }
        )
    return GraphDatasetSummary(
        total_workbooks=len(workbooks),
        total_rows=total_rows,
        labeled_rows=labeled_rows,
        positive_rows=positive_rows,
        negative_rows=negative_rows,
        unlabeled_rows=unlabeled_rows,
        flagged_rows=flagged_rows,
        mirrored_rows=mirrored_rows,
        mirrored_groups=mirrored_groups,
        possible_mirrored_rows=possible_mirrored_rows,
        possible_mirrored_groups=possible_mirrored_groups,
        confirmed_mirror_rows=confirmed_mirror_rows,
        rejected_mirror_rows=rejected_mirror_rows,
        uncertain_mirror_rows=uncertain_mirror_rows,
        role_counts=role_counts,
        owner_counts=owner_counts,
        workbooks=workbooks,
    )


def export_graph_dataset_summary(summary: GraphDatasetSummary, output_path: str | Path) -> Path:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(summary.to_dict(), ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def export_owner_summary_csv(summary: OwnerSummaryReport, output_path: str | Path) -> Path:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        fieldnames = [
            "owner_id",
            "owner_name",
            "owner_confidence",
            "dominant_role",
            "reviewed_role",
            "reviewed_confidence",
            "reviewed_note",
            "role_counts",
            "pattern_tags",
            "top_counterparties",
            "priority_score",
            "priority_rank",
            "tx_count",
            "unique_counterparties",
            "inflow_count",
            "outflow_count",
            "inflow_ratio",
            "outflow_ratio",
            "collect_and_split",
            "channel_count",
            "workbook_count",
            "mirrored_rows",
            "mirrored_groups",
            "possible_mirrored_rows",
            "possible_mirrored_groups",
            "labeled_rows",
            "positive_rows",
            "negative_rows",
            "flagged_rows",
        ]
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for item in summary.owners:
            row = item.to_dict()
            row["role_counts"] = json.dumps(item.role_counts, ensure_ascii=False, sort_keys=True)
            row["pattern_tags"] = "|".join(item.pattern_tags)
            row["top_counterparties"] = json.dumps(item.top_counterparties, ensure_ascii=False)
            writer.writerow(row)
    return path


def export_owner_summary_json(summary: OwnerSummaryReport, output_path: str | Path) -> Path:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(summary.to_dict(), ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def export_owner_review_csv(rows: list[OwnerReviewRow], output_path: str | Path) -> Path:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        fieldnames = [
            "owner_id",
            "owner_name",
            "priority_rank",
            "priority_score",
            "dominant_role",
            "reviewed_role",
            "reviewed_confidence",
            "reviewed_note",
            "pattern_tags",
            "top_counterparties",
            "review_role",
            "review_confidence",
            "review_options",
            "confidence_options",
            "review_note",
        ]
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for item in rows:
            row = item.to_dict()
            row["pattern_tags"] = "|".join(item.pattern_tags)
            row["top_counterparties"] = json.dumps(item.top_counterparties, ensure_ascii=False)
            writer.writerow(row)
    return path


def export_owner_review_xlsx(rows: list[OwnerReviewRow], output_path: str | Path) -> Path:
    headers = [
        "owner_id",
        "owner_name",
        "priority_rank",
        "priority_score",
        "dominant_role",
        "reviewed_role",
        "reviewed_confidence",
        "reviewed_note",
        "pattern_tags",
        "top_counterparties",
        "review_role",
        "review_confidence",
        "review_options",
        "confidence_options",
        "review_note",
    ]
    table_rows: list[dict[str, Any]] = []
    for item in rows:
        row = item.to_dict()
        row["pattern_tags"] = "|".join(item.pattern_tags)
        row["top_counterparties"] = json.dumps(item.top_counterparties, ensure_ascii=False)
        table_rows.append(row)
    return write_xlsx_table(output_path, headers=headers, rows=table_rows, row_fills=["yellow"] * len(table_rows))


def _build_owner_review_role(row: dict[str, str], scene: str, evidence: str) -> RoleAnnotation | None:
    owner_id = str(row.get("owner_id", "")).strip()
    review_role = str(row.get("review_role", "")).strip().lower()
    review_confidence = str(row.get("review_confidence", "")).strip().lower()
    if not owner_id or not review_role or not review_confidence:
        return None
    return RoleAnnotation(
        target_type="owner",
        target_id=owner_id,
        scene=scene,
        role_label=review_role,
        confidence=review_confidence,
        evidence=evidence,
        note=str(row.get("review_note", "")).strip(),
    )


def build_owner_review_roles(review_path: str | Path, scene: str = "owner_review", evidence: str = "owner_manual_review") -> list[RoleAnnotation]:
    path = Path(review_path)
    rows: list[RoleAnnotation] = []
    if path.suffix.lower() == ".xlsx":
        source_rows = [item.values for item in load_xlsx_styled_rows(path)]
    else:
        with path.open(encoding="utf-8", newline="") as handle:
            source_rows = list(csv.DictReader(handle))
    for row in source_rows:
        annotation = _build_owner_review_role({str(k): str(v) for k, v in row.items()}, scene=scene, evidence=evidence)
        if annotation is not None:
            rows.append(annotation)
    return rows
