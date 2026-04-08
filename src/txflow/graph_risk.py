from __future__ import annotations

import hashlib
import json
import math
import random
import re
from collections import Counter, defaultdict
from dataclasses import dataclass, replace
from datetime import datetime
from pathlib import Path
from statistics import mean
from typing import Any

import torch
import torch.nn as nn
import torch.nn.functional as F

from .labels import LabelManifest
from .model import example_to_tokens
from .training import TrainingExample, build_training_examples


def _row_key(example: TrainingExample, source_path: str) -> str:
    return f"{source_path}::{example.row_index}::{example.transaction_id or 'unknown'}"


def _stable_bucket(token: str, buckets: int) -> int:
    digest = hashlib.blake2b(token.encode("utf-8"), digest_size=8).digest()
    return int.from_bytes(digest, "big") % buckets


def _normalize_text(value: str) -> str:
    text = re.sub(r"\s+", "", str(value or "")).strip().lower()
    text = text.replace("【", "").replace("】", "")
    if text in {"", "unknown", "none", "null", "nan", "外部", "external", "unknowncounterparty"}:
        return ""
    if any(token in text for token in {"未知", "不详", "未知账户", "暂无", "空白"}):
        return ""
    return text


def _parse_amount(value: str) -> float:
    text = str(value or "").strip().replace(",", "").replace("￥", "").replace("元", "")
    match = re.search(r"(-?\d+(?:\.\d+)?)", text)
    if not match:
        return 0.0
    try:
        return float(match.group(1))
    except ValueError:
        return 0.0


def _parse_datetime(value: str) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    text = text.replace("年", "-").replace("月", "-").replace("日", " ")
    text = text.replace("时", ":").replace("分", "").replace("T", " ").strip()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y/%m/%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y/%m/%d %H:%M", "%Y-%m-%d", "%Y/%m/%d", "%Y%m%d%H%M%S", "%Y%m%d"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    match = re.search(r"(\d{4})[-/](\d{1,2})[-/](\d{1,2})(?:[^\d](\d{1,2}):(\d{1,2})(?::(\d{1,2}))?)?", text)
    if not match:
        return None
    year, month, day = int(match.group(1)), int(match.group(2)), int(match.group(3))
    hour = int(match.group(4) or 0)
    minute = int(match.group(5) or 0)
    second = int(match.group(6) or 0)
    try:
        return datetime(year, month, day, hour, minute, second)
    except ValueError:
        return None


def _parse_timestamp_components(value: str) -> tuple[int | None, int | None]:
    text = str(value or "").strip()
    if not text:
        return None, None
    text = text.replace("年", "-").replace("月", "-").replace("日", " ")
    text = text.replace("时", ":").replace("分", "").replace("T", " ").strip()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y/%m/%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y/%m/%d %H:%M", "%Y-%m-%d", "%Y/%m/%d", "%Y%m%d%H%M%S", "%Y%m%d"):
        try:
            dt = datetime.strptime(text, fmt)
            return dt.hour, dt.weekday()
        except Exception:
            continue
    match = re.search(r"(\d{4})[-/](\d{1,2})[-/](\d{1,2})(?:[^\d](\d{1,2}):(\d{1,2})(?::(\d{1,2}))?)?", text)
    if not match:
        return None, None
    year, month, day = int(match.group(1)), int(match.group(2)), int(match.group(3))
    hour = int(match.group(4) or 0)
    minute = int(match.group(5) or 0)
    second = int(match.group(6) or 0)
    try:
        dt = datetime(year, month, day, hour, minute, second)
        return dt.hour, dt.weekday()
    except Exception:
        return None, None


def _direction_index(direction: str) -> int:
    value = _normalize_text(direction)
    if any(token in value for token in {"inflow", "收入", "转入", "收款", "入账", "收"}):
        return 0
    if any(token in value for token in {"outflow", "支出", "转出", "付款", "消费", "支"}):
        return 1
    return 2


def _night_prior(hour: int | None) -> float:
    if hour is None:
        return 0.5
    return 0.8 if (hour >= 22 or hour < 6) else 0.2


def _token_features(example: TrainingExample, buckets: int = 128) -> list[float]:
    features = [0.0] * buckets
    for token in example_to_tokens(example):
        bucket = _stable_bucket(token, buckets)
        features[bucket] += 1.0
    return [math.log1p(value) for value in features]


def _numeric_features(example: TrainingExample) -> list[float]:
    amount = _parse_amount(example.amount)
    hour = example.hour
    weekday = example.weekday
    if hour is None:
        hour_sin = 0.0
        hour_cos = 0.0
    else:
        hour_sin = math.sin(2.0 * math.pi * hour / 24.0)
        hour_cos = math.cos(2.0 * math.pi * hour / 24.0)
    if weekday is None:
        weekday_sin = 0.0
        weekday_cos = 0.0
    else:
        weekday_sin = math.sin(2.0 * math.pi * weekday / 7.0)
        weekday_cos = math.cos(2.0 * math.pi * weekday / 7.0)
    role_map = {"buyer": 0.0, "seller": 1.0, "broker": 2.0, "mixed": 3.0, "unknown": 4.0}
    confidence_map = {"low": 1.0 / 3.0, "medium": 2.0 / 3.0, "high": 1.0}
    owner_present = 1.0 if example.owner_id else 0.0
    trade_pattern_map = {
        "p2p_transfer": 0.0,
        "qr_p2p_transfer": 1.0,
        "merchant_consume": 2.0,
        "platform_settlement": 3.0,
        "withdraw_to_bank": 4.0,
        "recharge": 5.0,
        "red_packet": 6.0,
        "failed_or_invalid": 7.0,
        "other": 8.0,
    }
    return [
        math.log1p(abs(amount)),
        1.0 if amount < 0 else 0.0,
        hour_sin,
        hour_cos,
        weekday_sin,
        weekday_cos,
        _night_prior(example.hour),
        float(_direction_index(example.direction)),
        role_map.get(example.role_label or "unknown", 4.0) / 4.0,
        confidence_map.get(example.role_confidence, 0.0),
        owner_present,
        confidence_map.get(example.owner_confidence, 0.0),
        trade_pattern_map.get(example.trade_pattern or "other", 8.0) / 8.0,
        1.0 if example.is_qr_transfer else 0.0,
        1.0 if example.is_red_packet else 0.0,
        1.0 if example.is_platform_settlement else 0.0,
        1.0 if example.is_withdrawal_like else 0.0,
        1.0 if example.is_trade_like else 0.0,
    ]


def _extension_aggregate_features(examples: list[TrainingExample]) -> list[list[float]]:
    buyer_rows: dict[str, list[int]] = defaultdict(list)
    buyer_sellers: dict[str, set[str]] = defaultdict(set)
    buyer_known_sellers: dict[str, set[str]] = defaultdict(set)
    seller_rows: dict[str, list[int]] = defaultdict(list)
    seller_buyers: dict[str, set[str]] = defaultdict(set)
    seller_workbooks: dict[str, set[str]] = defaultdict(set)
    seller_known_buyer_support: dict[str, set[str]] = defaultdict(set)

    for index, example in enumerate(examples):
        buyer_account = _normalize_text(example.buyer_account)
        seller_account = _normalize_text(example.seller_account)
        workbook = str(example.source_file or "").strip() or "__unknown__"
        if buyer_account:
            buyer_rows[buyer_account].append(index)
            if seller_account:
                buyer_sellers[buyer_account].add(seller_account)
                if example.extension_role == "buyer_to_known_seller":
                    buyer_known_sellers[buyer_account].add(seller_account)
        if seller_account:
            seller_rows[seller_account].append(index)
            seller_workbooks[seller_account].add(workbook)
            if buyer_account:
                seller_buyers[seller_account].add(buyer_account)
                if buyer_account in buyer_known_sellers:
                    seller_known_buyer_support[seller_account].add(buyer_account)

    feature_rows: list[list[float]] = []
    for example in examples:
        buyer_account = _normalize_text(example.buyer_account)
        seller_account = _normalize_text(example.seller_account)
        buyer_tx_count = len(buyer_rows.get(buyer_account, [])) if buyer_account else 0
        buyer_unique_sellers = len(buyer_sellers.get(buyer_account, set())) if buyer_account else 0
        buyer_known_support = len(buyer_known_sellers.get(buyer_account, set())) if buyer_account else 0
        seller_tx_count = len(seller_rows.get(seller_account, [])) if seller_account else 0
        seller_unique_buyers = len(seller_buyers.get(seller_account, set())) if seller_account else 0
        seller_workbook_count = len(seller_workbooks.get(seller_account, set())) if seller_account else 0
        seller_known_buyer_count = len(seller_known_buyer_support.get(seller_account, set())) if seller_account else 0
        feature_rows.append(
            [
                math.log1p(buyer_tx_count),
                math.log1p(buyer_unique_sellers),
                math.log1p(buyer_known_support),
                math.log1p(seller_tx_count),
                math.log1p(seller_unique_buyers),
                math.log1p(seller_workbook_count),
                math.log1p(seller_known_buyer_count),
                1.0 if seller_unique_buyers >= 2 else 0.0,
            ]
        )
    return feature_rows


def _graph_tokens(example: TrainingExample) -> list[str]:
    tokens: list[str] = []
    counterparty = _normalize_text(example.counterparty)
    remark = _normalize_text(example.remark)
    if counterparty:
        tokens.append(f"counterparty:{counterparty}")
    if remark:
        tokens.extend([f"remark:{piece}" for piece in re.split(r"[，,；;。.|/\\\s]+", remark) if piece])
    tokens.append(f"direction:{_direction_index(example.direction)}")
    if example.channel:
        tokens.append(f"channel:{_normalize_text(example.channel)}")
    if example.hour is not None:
        tokens.append(f"hour:{example.hour // 3}")
    if example.weekday is not None:
        tokens.append(f"weekday:{example.weekday}")
    if example.role_label:
        tokens.append(f"role:{example.role_label}")
    if example.role_scene:
        tokens.append(f"role_scene:{example.role_scene}")
    if example.flow_family:
        tokens.append(f"flow_family:{example.flow_family}")
    if example.trade_pattern:
        tokens.append(f"trade_pattern:{example.trade_pattern}")
    if example.rule_reason:
        tokens.extend([f"rule_reason:{piece}" for piece in re.split(r"[|]+", example.rule_reason) if piece])
    if example.is_qr_transfer:
        tokens.append("qr_transfer")
    if example.is_red_packet:
        tokens.append("red_packet")
    if example.is_platform_settlement:
        tokens.append("platform_settlement")
    if example.is_failed_or_invalid:
        tokens.append("failed_or_invalid")
    if example.is_trade_like:
        tokens.append("trade_like")
    if example.extension_role:
        tokens.append(f"extension_role:{example.extension_role}")
    if example.buyer_account:
        tokens.append("has_buyer_account")
    if example.seller_account:
        tokens.append("has_seller_account")
    if example.seller_proxy_name:
        tokens.append(f"seller_proxy:{_normalize_text(example.seller_proxy_name)}")
    return [token for token in tokens if token]


def _synthetic_example(label_status: str) -> TrainingExample:
    if label_status == "positive":
        return TrainingExample(
            row_index=0,
            transaction_id="synthetic-positive",
            label="high_risk_transaction",
            label_status="positive",
            subject="synthetic",
            source_file="__synthetic__",
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
        source_file="__synthetic__",
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


def build_synthetic_graph_rows(warmup_pairs: int = 4) -> list[tuple[str, TrainingExample]]:
    count = max(int(warmup_pairs), 0)
    rows: list[tuple[str, TrainingExample]] = []
    for index in range(count):
        amount = 188 + (index % 5) * 12
        hour = 22 + (index % 3)
        minute = (index * 11) % 60
        seller_path = f"__synthetic__/seller_{index}.xlsx"
        buyer_path = f"__synthetic__/buyer_{index}.xlsx"
        shared_counterparty = f"synthetic_shared_{index}"
        seller = TrainingExample(
            row_index=index * 2 + 1,
            transaction_id=f"synthetic-seller-{index}",
            label="high_risk_transaction",
            label_status="positive",
            subject="synthetic",
            source_file=seller_path,
            amount=f"{amount:.2f}",
            timestamp=f"2026-03-{12 + (index % 5):02d} {hour:02d}:{minute:02d}:00",
            hour=hour,
            weekday=3,
            is_night=True,
            counterparty=shared_counterparty,
            direction="入账",
            channel="微信",
            remark="夜间定金",
            raw={},
        )
        buyer = TrainingExample(
            row_index=index * 2 + 2,
            transaction_id=f"synthetic-buyer-{index}",
            label="low_risk_transaction",
            label_status="negative",
            subject="synthetic",
            source_file=buyer_path,
            amount=f"{amount:.2f}",
            timestamp=f"2026-03-{12 + (index % 5):02d} {hour:02d}:{(minute + 7) % 60:02d}:00",
            hour=hour,
            weekday=3,
            is_night=True,
            counterparty=shared_counterparty,
            direction="出账",
            channel="微信",
            remark="夜间转账",
            raw={},
        )
        rows.append((seller_path, seller))
        rows.append((buyer_path, buyer))

    if not rows:
        rows.extend([
            ("__synthetic__/seller_0.xlsx", _synthetic_example("positive")),
            ("__synthetic__/buyer_0.xlsx", _synthetic_example("negative")),
        ])
    return rows


@dataclass(frozen=True)
class GraphRowScore:
    workbook_path: str
    row_index: int
    transaction_id: str
    label_status: str
    score: float
    label: str
    subject: str
    amount: str
    timestamp: str
    counterparty: str
    remark: str
    direction: str
    channel: str
    tokens: list[str]

    def to_dict(self) -> dict[str, Any]:
        return {
            "workbook_path": self.workbook_path,
            "row_index": self.row_index,
            "transaction_id": self.transaction_id,
            "label_status": self.label_status,
            "score": round(self.score, 4),
            "label": self.label,
            "subject": self.subject,
            "amount": self.amount,
            "timestamp": self.timestamp,
            "counterparty": self.counterparty,
            "remark": self.remark,
            "direction": self.direction,
            "channel": self.channel,
            "tokens": list(self.tokens),
        }


@dataclass(frozen=True)
class GraphWorkbookSummary:
    path: str
    total_rows: int
    labeled_rows: int
    positive_rows: int
    negative_rows: int
    unlabeled_rows: int
    avg_score: float
    max_score: float
    top_rows: list[GraphRowScore]

    def to_dict(self) -> dict[str, Any]:
        return {
            "path": self.path,
            "total_rows": self.total_rows,
            "labeled_rows": self.labeled_rows,
            "positive_rows": self.positive_rows,
            "negative_rows": self.negative_rows,
            "unlabeled_rows": self.unlabeled_rows,
            "avg_score": round(self.avg_score, 4),
            "max_score": round(self.max_score, 4),
            "top_rows": [item.to_dict() for item in self.top_rows],
        }


@dataclass(frozen=True)
class GraphPairScore:
    left_workbook_path: str
    left_row_index: int
    left_transaction_id: str
    left_score: float
    left_amount: str
    left_direction: str
    left_counterparty: str
    left_timestamp: str
    right_workbook_path: str
    right_row_index: int
    right_transaction_id: str
    right_score: float
    right_amount: str
    right_direction: str
    right_counterparty: str
    right_timestamp: str
    amount_gap_ratio: float
    time_gap_minutes: float
    same_counterparty: bool
    opposite_direction: bool
    cross_workbook: bool
    pair_type: str
    pair_score: float

    def to_dict(self) -> dict[str, Any]:
        return {
            "left_workbook_path": self.left_workbook_path,
            "left_row_index": self.left_row_index,
            "left_transaction_id": self.left_transaction_id,
            "left_score": round(self.left_score, 4),
            "left_amount": self.left_amount,
            "left_direction": self.left_direction,
            "left_counterparty": self.left_counterparty,
            "left_timestamp": self.left_timestamp,
            "right_workbook_path": self.right_workbook_path,
            "right_row_index": self.right_row_index,
            "right_transaction_id": self.right_transaction_id,
            "right_score": round(self.right_score, 4),
            "right_amount": self.right_amount,
            "right_direction": self.right_direction,
            "right_counterparty": self.right_counterparty,
            "right_timestamp": self.right_timestamp,
            "amount_gap_ratio": round(self.amount_gap_ratio, 4),
            "time_gap_minutes": round(self.time_gap_minutes, 2),
            "same_counterparty": self.same_counterparty,
            "opposite_direction": self.opposite_direction,
            "cross_workbook": self.cross_workbook,
            "pair_type": self.pair_type,
            "pair_score": round(self.pair_score, 4),
        }


@dataclass(frozen=True)
class GraphTrainingSummary:
    model_name: str
    feature_dim: int
    total_nodes: int
    labeled_nodes: int
    synthetic_rows: int
    pseudo_labeled_rows: int
    self_training_rounds: int
    train_nodes: int
    val_nodes: int
    epochs: int
    best_epoch: int
    best_val_loss: float
    best_val_f1: float
    positive_rate: float

    def to_dict(self) -> dict[str, Any]:
        return {
            "model_name": self.model_name,
            "feature_dim": self.feature_dim,
            "total_nodes": self.total_nodes,
            "labeled_nodes": self.labeled_nodes,
            "synthetic_rows": self.synthetic_rows,
            "pseudo_labeled_rows": self.pseudo_labeled_rows,
            "self_training_rounds": self.self_training_rounds,
            "train_nodes": self.train_nodes,
            "val_nodes": self.val_nodes,
            "epochs": self.epochs,
            "best_epoch": self.best_epoch,
            "best_val_loss": round(self.best_val_loss, 6),
            "best_val_f1": round(self.best_val_f1, 4),
            "positive_rate": round(self.positive_rate, 4),
        }


@dataclass(frozen=True)
class GraphTriageReport:
    total_workbooks: int
    total_rows: int
    labeled_rows: int
    positive_rows: int
    negative_rows: int
    unlabeled_rows: int
    top_rows: list[GraphRowScore]
    workbooks: list[GraphWorkbookSummary]
    pairs: list[GraphPairScore]
    training: GraphTrainingSummary

    def to_dict(self) -> dict[str, Any]:
        return {
            "total_workbooks": self.total_workbooks,
            "total_rows": self.total_rows,
            "labeled_rows": self.labeled_rows,
            "positive_rows": self.positive_rows,
            "negative_rows": self.negative_rows,
            "unlabeled_rows": self.unlabeled_rows,
            "top_rows": [item.to_dict() for item in self.top_rows],
            "workbooks": [item.to_dict() for item in self.workbooks],
            "pairs": [item.to_dict() for item in self.pairs],
            "training": self.training.to_dict(),
        }


class RowGNNLayer(nn.Module):
    def __init__(self, input_dim: int, output_dim: int) -> None:
        super().__init__()
        self.self_proj = nn.Linear(input_dim, output_dim)
        self.neigh_proj = nn.Linear(input_dim, output_dim)

    def forward(self, x: torch.Tensor, edge_index: torch.Tensor, edge_weight: torch.Tensor | None = None) -> torch.Tensor:
        if edge_index.numel() == 0:
            neighbor = torch.zeros_like(x)
        else:
            src = edge_index[0]
            dst = edge_index[1]
            if edge_weight is None:
                weight = torch.ones(src.shape[0], device=x.device, dtype=x.dtype)
            else:
                weight = edge_weight.to(device=x.device, dtype=x.dtype)
            neighbor = torch.zeros_like(x)
            weighted_messages = x[src] * weight.unsqueeze(-1)
            neighbor.index_add_(0, dst, weighted_messages)
            degree = torch.zeros(x.size(0), device=x.device, dtype=x.dtype)
            degree.index_add_(0, dst, weight)
            neighbor = neighbor / degree.clamp_min(1.0).unsqueeze(-1)
        return self.self_proj(x) + self.neigh_proj(neighbor)


class RowGNNClassifier(nn.Module):
    def __init__(self, input_dim: int, hidden_dim: int = 64, dropout: float = 0.2) -> None:
        super().__init__()
        self.layer1 = RowGNNLayer(input_dim, hidden_dim)
        self.layer2 = RowGNNLayer(hidden_dim, hidden_dim)
        self.output = nn.Linear(hidden_dim, 1)
        self.dropout = dropout

    def forward(self, x: torch.Tensor, edge_index: torch.Tensor, edge_weight: torch.Tensor | None = None) -> torch.Tensor:
        x = self.layer1(x, edge_index, edge_weight)
        x = F.relu(x)
        x = F.dropout(x, p=self.dropout, training=self.training)
        x = self.layer2(x, edge_index, edge_weight)
        x = F.relu(x)
        x = F.dropout(x, p=self.dropout, training=self.training)
        return self.output(x).squeeze(-1)


class FocalLoss(nn.Module):
    def __init__(self, alpha: float = 0.75, gamma: float = 2.0) -> None:
        super().__init__()
        self.alpha = alpha
        self.gamma = gamma

    def forward(self, logits: torch.Tensor, targets: torch.Tensor) -> torch.Tensor:
        targets = targets.float()
        probs = torch.sigmoid(logits)
        pt = torch.where(targets > 0.5, probs, 1.0 - probs)
        alpha_t = torch.where(targets > 0.5, torch.full_like(targets, self.alpha), torch.full_like(targets, 1.0 - self.alpha))
        loss = -alpha_t * torch.pow(1.0 - pt, self.gamma) * torch.log(pt.clamp_min(1e-8))
        return loss.mean()


@dataclass(frozen=True)
class GraphDataset:
    features: torch.Tensor
    labels: torch.Tensor
    labeled_mask: torch.Tensor
    train_mask: torch.Tensor
    val_mask: torch.Tensor
    edge_index: torch.Tensor
    edge_weight: torch.Tensor
    examples: list[TrainingExample]
    node_keys: list[str]
    workbook_paths: list[str]


def _build_feature_matrix(examples: list[TrainingExample], token_buckets: int = 128) -> torch.Tensor:
    numeric_rows = [_numeric_features(example) for example in examples]
    extension_rows = _extension_aggregate_features(examples)
    token_rows = [_token_features(example, token_buckets) for example in examples]
    features = torch.tensor(
        [numeric + extension + token for numeric, extension, token in zip(numeric_rows, extension_rows, token_rows)],
        dtype=torch.float32,
    )
    if features.numel() == 0:
        return features
    numeric_dim = len(numeric_rows[0]) if numeric_rows else 0
    if numeric_dim:
        numeric = features[:, :numeric_dim]
        mean_ = numeric.mean(dim=0, keepdim=True)
        std_ = numeric.std(dim=0, keepdim=True).clamp_min(1e-6)
        features[:, :numeric_dim] = (numeric - mean_) / std_
    return features


def _add_edge(edge_map: dict[tuple[int, int], float], left: int, right: int, weight: float) -> None:
    if left == right:
        return
    edge_map[(left, right)] = edge_map.get((left, right), 0.0) + weight


def _build_graph(rows: list[tuple[str, TrainingExample]]) -> tuple[torch.Tensor, torch.Tensor, list[str]]:
    edge_map: dict[tuple[int, int], float] = {}
    examples = [example for _, example in rows]
    node_keys = [_row_key(example, source_path) for source_path, example in rows]

    workbook_groups: dict[str, list[int]] = defaultdict(list)
    counterparty_groups: dict[str, list[int]] = defaultdict(list)
    owner_groups: dict[str, list[int]] = defaultdict(list)
    buyer_groups: dict[str, list[int]] = defaultdict(list)
    seller_groups: dict[str, list[int]] = defaultdict(list)
    token_groups: dict[str, list[int]] = defaultdict(list)

    for index, (source_path, example) in enumerate(rows):
        workbook_groups[source_path or "__unknown__"].append(index)
        counterparty = _normalize_text(example.counterparty)
        if counterparty:
            counterparty_groups[counterparty].append(index)
        owner_id = str(example.owner_id or "").strip()
        if owner_id:
            owner_groups[owner_id].append(index)
        buyer_account = _normalize_text(example.buyer_account)
        if buyer_account:
            buyer_groups[buyer_account].append(index)
        seller_account = _normalize_text(example.seller_account)
        if seller_account:
            seller_groups[seller_account].append(index)
        for token in _graph_tokens(example):
            token_groups[token].append(index)

    for indices in workbook_groups.values():
        indices.sort(key=lambda item: examples[item].row_index)
        for i, left in enumerate(indices):
            for offset, weight in ((1, 1.0), (2, 0.7)):
                if i + offset >= len(indices):
                    continue
                right = indices[i + offset]
                _add_edge(edge_map, left, right, weight * 0.35)
                _add_edge(edge_map, right, left, weight * 0.35)

    for indices in counterparty_groups.values():
        indices.sort(key=lambda item: examples[item].row_index)
        if len(indices) > 1:
            for left, right in zip(indices, indices[1:]):
                _add_edge(edge_map, left, right, 1.6)
                _add_edge(edge_map, right, left, 1.6)

    for indices in owner_groups.values():
        indices.sort(key=lambda item: (examples[item].source_file, examples[item].row_index))
        if len(indices) > 1:
            for left, right in zip(indices, indices[1:]):
                _add_edge(edge_map, left, right, 0.65)
                _add_edge(edge_map, right, left, 0.65)

    for indices in buyer_groups.values():
        indices.sort(key=lambda item: (examples[item].source_file, examples[item].row_index))
        if len(indices) > 1:
            for left, right in zip(indices, indices[1:]):
                _add_edge(edge_map, left, right, 1.8)
                _add_edge(edge_map, right, left, 1.8)

    for indices in seller_groups.values():
        indices.sort(key=lambda item: (examples[item].source_file, examples[item].row_index))
        if len(indices) > 1:
            for left, right in zip(indices, indices[1:]):
                _add_edge(edge_map, left, right, 1.9)
                _add_edge(edge_map, right, left, 1.9)

    bridge_edges: dict[tuple[int, int], float] = {}
    for indices in buyer_groups.values():
        if len(indices) < 2:
            continue
        known_indices = [index for index in indices if examples[index].extension_role == "buyer_to_known_seller"]
        candidate_indices = [
            index
            for index in indices
            if examples[index].seller_account and examples[index].extension_role != "seller_anchor"
        ]
        for left in known_indices:
            for right in candidate_indices:
                if left == right:
                    continue
                bridge_edges[(left, right)] = max(bridge_edges.get((left, right), 0.0), 2.1)
                bridge_edges[(right, left)] = max(bridge_edges.get((right, left), 0.0), 2.1)
    for (left, right), weight in bridge_edges.items():
        _add_edge(edge_map, left, right, weight)

    token_frequency = {token: len(indices) for token, indices in token_groups.items()}
    candidate_tokens = {token for token, freq in token_frequency.items() if 2 <= freq <= 40}
    candidate_edges: dict[tuple[int, int], float] = {}
    for token in candidate_tokens:
        indices = token_groups[token]
        if len(indices) < 2:
            continue
        for left, right in zip(indices, indices[1:]):
            if left == right:
                continue
            weight = 1.0 + min(0.6, 0.1 * len(_graph_tokens(examples[left])) / 10.0)
            candidate_edges[(left, right)] = max(candidate_edges.get((left, right), 0.0), weight)
            candidate_edges[(right, left)] = max(candidate_edges.get((right, left), 0.0), weight)
    for (left, right), weight in candidate_edges.items():
        _add_edge(edge_map, left, right, weight)

    if edge_map:
        edge_index = torch.tensor(list(edge_map.keys()), dtype=torch.long).t().contiguous()
        edge_weight = torch.tensor(list(edge_map.values()), dtype=torch.float32)
    else:
        edge_index = torch.empty((2, 0), dtype=torch.long)
        edge_weight = torch.empty((0,), dtype=torch.float32)
    return edge_index, edge_weight, node_keys


def _split_group_key(example: TrainingExample, workbook_path: str) -> str:
    owner_id = str(example.owner_id or "").strip()
    if owner_id:
        return f"owner:{owner_id}"
    subject_account = _normalize_text(example.subject_account)
    if subject_account:
        return f"subject_account:{subject_account}"
    buyer_account = _normalize_text(example.buyer_account)
    seller_account = _normalize_text(example.seller_account)
    if buyer_account or seller_account:
        return f"trade:{buyer_account}->{seller_account}"
    return f"workbook:{workbook_path or '__unknown__'}"


def _grouped_stratified_split(
    label_indices: list[int],
    labels: torch.Tensor,
    examples: list[TrainingExample],
    workbook_paths: list[str],
    seed: int,
    split_ratio: float,
) -> tuple[list[int], list[int]]:
    rng = random.Random(seed)
    grouped: dict[str, list[int]] = defaultdict(list)
    for index in label_indices:
        grouped[_split_group_key(examples[index], workbook_paths[index])].append(index)

    group_items: list[tuple[str, list[int], int, int]] = []
    total_positive = total_negative = 0
    for key, indices in grouped.items():
        positives = sum(1 for index in indices if int(labels[index].item()) == 1)
        negatives = sum(1 for index in indices if int(labels[index].item()) == 0)
        total_positive += positives
        total_negative += negatives
        group_items.append((key, indices, positives, negatives))

    if len(group_items) <= 1:
        return list(label_indices), list(label_indices)

    rng.shuffle(group_items)
    group_items.sort(key=lambda item: (max(item[2], item[3]), len(item[1])), reverse=True)
    target_positive = max(1, int(round(total_positive * (1.0 - split_ratio)))) if total_positive > 1 else total_positive
    target_negative = max(1, int(round(total_negative * (1.0 - split_ratio)))) if total_negative > 1 else total_negative

    val_groups: list[tuple[str, list[int], int, int]] = []
    train_groups: list[tuple[str, list[int], int, int]] = []
    val_positive = val_negative = 0

    for item in group_items:
        _, _, positives, negatives = item
        still_need_positive = val_positive < target_positive
        still_need_negative = val_negative < target_negative
        add_to_val = False
        if still_need_positive and positives:
            add_to_val = True
        elif still_need_negative and negatives:
            add_to_val = True
        elif not val_groups:
            add_to_val = True
        if add_to_val:
            val_groups.append(item)
            val_positive += positives
            val_negative += negatives
        else:
            train_groups.append(item)

    if not train_groups:
        train_groups.append(val_groups.pop())
    if not val_groups:
        val_groups.append(train_groups.pop())

    def _flatten(items: list[tuple[str, list[int], int, int]]) -> list[int]:
        return [index for _, indices, _, _ in items for index in indices]

    train = _flatten(train_groups)
    val = _flatten(val_groups)

    train_positive = sum(1 for index in train if int(labels[index].item()) == 1)
    train_negative = sum(1 for index in train if int(labels[index].item()) == 0)
    val_positive = sum(1 for index in val if int(labels[index].item()) == 1)
    val_negative = sum(1 for index in val if int(labels[index].item()) == 0)

    def _move_group(
        source: list[tuple[str, list[int], int, int]],
        target: list[tuple[str, list[int], int, int]],
        need_positive: bool,
    ) -> bool:
        for idx, item in enumerate(source):
            positives = item[2]
            negatives = item[3]
            if need_positive and positives:
                target.append(source.pop(idx))
                return True
            if not need_positive and negatives:
                target.append(source.pop(idx))
                return True
        return False

    if val_positive == 0 and total_positive > 0:
        _move_group(train_groups, val_groups, need_positive=True)
    if val_negative == 0 and total_negative > 0:
        _move_group(train_groups, val_groups, need_positive=False)
    if train_positive == 0 and total_positive > 0:
        _move_group(val_groups, train_groups, need_positive=True)
    if train_negative == 0 and total_negative > 0:
        _move_group(val_groups, train_groups, need_positive=False)

    train = _flatten(train_groups)
    val = _flatten(val_groups)
    if not val:
        val = train[:]
    return train, val


def _metrics_from_predictions(y_true: torch.Tensor, y_prob: torch.Tensor, threshold: float = 0.5) -> dict[str, Any]:
    if y_true.numel() == 0:
        return {
            "total": 0,
            "accuracy": 0.0,
            "precision": 0.0,
            "recall": 0.0,
            "f1": 0.0,
            "confusion_matrix": {"tp": 0, "fp": 0, "tn": 0, "fn": 0},
        }
    y_pred = (y_prob >= threshold).long()
    tp = int(((y_pred == 1) & (y_true == 1)).sum().item())
    fp = int(((y_pred == 1) & (y_true == 0)).sum().item())
    tn = int(((y_pred == 0) & (y_true == 0)).sum().item())
    fn = int(((y_pred == 0) & (y_true == 1)).sum().item())
    total = int(y_true.numel())
    accuracy = (tp + tn) / total if total else 0.0
    precision = tp / (tp + fp) if tp + fp else 0.0
    recall = tp / (tp + fn) if tp + fn else 0.0
    f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
    return {
        "total": total,
        "accuracy": round(accuracy, 4),
        "precision": round(precision, 4),
        "recall": round(recall, 4),
        "f1": round(f1, 4),
        "confusion_matrix": {"tp": tp, "fp": fp, "tn": tn, "fn": fn},
    }


def _pseudo_label_rows(
    rows: list[tuple[str, TrainingExample]],
    scores: dict[str, float],
    positive_threshold: float,
    negative_threshold: float,
    max_rows: int,
) -> list[tuple[str, TrainingExample]]:
    candidates: list[tuple[float, str, TrainingExample]] = []
    for source_path, example in rows:
        if source_path.startswith("__synthetic__"):
            continue
        if example.label_status != "unlabeled":
            continue
        key = _row_key(example, source_path)
        score = scores.get(key, 0.5)
        if score >= positive_threshold:
            confidence = score
            pseudo = replace(
                example,
                label="high_risk_transaction",
                label_status="positive",
                subject=example.subject or "pseudo",
            )
            candidates.append((confidence, source_path, pseudo))
        elif score <= negative_threshold:
            confidence = 1.0 - score
            pseudo = replace(
                example,
                label="low_risk_transaction",
                label_status="negative",
                subject=example.subject or "pseudo",
            )
            candidates.append((confidence, source_path, pseudo))

    candidates.sort(key=lambda item: (-item[0], item[1], item[2].row_index))
    if max_rows > 0:
        candidates = candidates[:max_rows]
    return [(source_path, example) for _, source_path, example in candidates]


def _pair_candidates(rows: list[GraphRowScore], limit: int = 20) -> list[GraphPairScore]:
    def _make_pair(left: GraphRowScore, right: GraphRowScore, pair_type: str) -> GraphPairScore | None:
        left_dt = _parse_datetime(left.timestamp)
        right_dt = _parse_datetime(right.timestamp)
        if left_dt is None or right_dt is None:
            return None
        left_amount = _parse_amount(left.amount)
        right_amount = _parse_amount(right.amount)
        if left_amount <= 0 or right_amount <= 0:
            return None

        time_gap_minutes = abs((left_dt - right_dt).total_seconds()) / 60.0
        amount_gap_ratio = abs(left_amount - right_amount) / max(left_amount, right_amount)

        same_counterparty = bool(_normalize_text(left.counterparty)) and _normalize_text(left.counterparty) == _normalize_text(right.counterparty)
        left_direction = _direction_index(left.direction)
        right_direction = _direction_index(right.direction)
        opposite_direction = {left_direction, right_direction} == {0, 1}
        cross_workbook = left.workbook_path != right.workbook_path

        if cross_workbook:
            if time_gap_minutes > 720:
                return None
            if amount_gap_ratio > 0.35:
                return None
        else:
            if time_gap_minutes > 240:
                return None
            if amount_gap_ratio > 0.2:
                return None

        if cross_workbook:
            if not (same_counterparty or opposite_direction or amount_gap_ratio <= 0.05):
                return None
        elif not (same_counterparty or opposite_direction or amount_gap_ratio <= 0.05):
            return None

        left_night = _night_prior(left_dt.hour)
        right_night = _night_prior(right_dt.hour)
        pair_score = (
            0.45 * max(left.score, right.score)
            + 0.20 * min(left.score, right.score)
            + 0.15 * (1.0 - amount_gap_ratio)
            + 0.10 * max(0.0, 1.0 - time_gap_minutes / 240.0)
            + 0.10 * ((left_night + right_night) / 2.0)
        )
        if same_counterparty:
            pair_score += 0.05
        if opposite_direction:
            pair_score += 0.05
        if cross_workbook:
            pair_score += 0.10
        return GraphPairScore(
            left_workbook_path=left.workbook_path,
            left_row_index=left.row_index,
            left_transaction_id=left.transaction_id,
            left_score=left.score,
            left_amount=left.amount,
            left_direction=left.direction,
            left_counterparty=left.counterparty,
            left_timestamp=left.timestamp,
            right_workbook_path=right.workbook_path,
            right_row_index=right.row_index,
            right_transaction_id=right.transaction_id,
            right_score=right.score,
            right_amount=right.amount,
            right_direction=right.direction,
            right_counterparty=right.counterparty,
            right_timestamp=right.timestamp,
            amount_gap_ratio=amount_gap_ratio,
            time_gap_minutes=time_gap_minutes,
            same_counterparty=same_counterparty,
            opposite_direction=opposite_direction,
            cross_workbook=cross_workbook,
            pair_type=pair_type,
            pair_score=pair_score,
        )

    pairs: list[GraphPairScore] = []
    candidates = [item for item in rows if item.score >= 0.7 or item.label_status != "negative"]
    candidates.sort(key=lambda item: (-item.score, item.workbook_path, item.row_index))

    counterparty_groups: dict[str, list[GraphRowScore]] = defaultdict(list)
    for item in candidates:
        counterparty = _normalize_text(item.counterparty)
        if counterparty:
            counterparty_groups[counterparty].append(item)

    for group in counterparty_groups.values():
        group.sort(key=lambda item: (-item.score, item.workbook_path, item.row_index))
        if len(group) > 40:
            group = group[:40]
        for index, left in enumerate(group):
            for right in group[index + 1 :]:
                if left.workbook_path == right.workbook_path:
                    continue
                pair = _make_pair(left, right, "cross_workbook")
                if pair is not None:
                    pairs.append(pair)

    amount_groups: dict[int, list[GraphRowScore]] = defaultdict(list)
    for item in candidates:
        amount = _parse_amount(item.amount)
        if amount <= 0:
            continue
        amount_groups[int(round(amount))].append(item)

    for group in amount_groups.values():
        group.sort(key=lambda item: (-item.score, item.workbook_path, item.row_index))
        if len(group) > 30:
            group = group[:30]
        for index, left in enumerate(group):
            for right in group[index + 1 :]:
                if left.workbook_path == right.workbook_path:
                    continue
                left_dir = _direction_index(left.direction)
                right_dir = _direction_index(right.direction)
                if {left_dir, right_dir} != {0, 1}:
                    continue
                pair = _make_pair(left, right, "cross_workbook")
                if pair is not None:
                    pairs.append(pair)

    high_score_anchors = [item for item in candidates if item.score >= 0.85]
    opposite_index: dict[tuple[int, int], list[GraphRowScore]] = defaultdict(list)
    for item in rows:
        amount = _parse_amount(item.amount)
        if amount <= 0:
            continue
        opposite_index[(int(round(amount)), _direction_index(item.direction))].append(item)
    for group in opposite_index.values():
        group.sort(key=lambda item: (-item.score, item.workbook_path, item.row_index))
        if len(group) > 60:
            del group[60:]

    for anchor in high_score_anchors:
        anchor_amount = int(round(_parse_amount(anchor.amount)))
        anchor_dir = _direction_index(anchor.direction)
        if anchor_amount <= 0 or anchor_dir not in {0, 1}:
            continue
        target_dir = 1 if anchor_dir == 0 else 0
        for right in opposite_index.get((anchor_amount, target_dir), []):
            if right.workbook_path == anchor.workbook_path:
                continue
            pair = _make_pair(anchor, right, "cross_workbook")
            if pair is not None:
                pairs.append(pair)

    if len(pairs) < limit:
        for index, left in enumerate(candidates):
            for right in candidates[index + 1 :]:
                if left.workbook_path != right.workbook_path:
                    continue
                pair = _make_pair(left, right, "same_workbook")
                if pair is not None:
                    pairs.append(pair)

    pairs.sort(key=lambda item: (-item.cross_workbook, -item.pair_score, item.amount_gap_ratio, item.time_gap_minutes))
    return pairs[:limit]


class GraphRiskModel:
    def __init__(
        self,
        epochs: int = 120,
        hidden_dim: int = 64,
        dropout: float = 0.25,
        lr: float = 0.01,
        seed: int = 42,
        split_ratio: float = 0.8,
        patience: int = 20,
    ) -> None:
        self.epochs = epochs
        self.hidden_dim = hidden_dim
        self.dropout = dropout
        self.lr = lr
        self.seed = seed
        self.split_ratio = split_ratio
        self.patience = patience
        self.model: RowGNNClassifier | None = None
        self.dataset: GraphDataset | None = None
        self.training_summary = GraphTrainingSummary(
            model_name="row_gnn",
            feature_dim=0,
            total_nodes=0,
            labeled_nodes=0,
            synthetic_rows=0,
            pseudo_labeled_rows=0,
            self_training_rounds=0,
            train_nodes=0,
            val_nodes=0,
            epochs=0,
            best_epoch=0,
            best_val_loss=0.0,
            best_val_f1=0.0,
            positive_rate=0.0,
        )
        self.node_scores: dict[str, float] = {}
        self.node_examples: dict[str, TrainingExample] = {}
        self.node_keys: list[str] = []
        self.edge_index = torch.empty((2, 0), dtype=torch.long)
        self.edge_weight = torch.empty((0,), dtype=torch.float32)

    def _predict_dataset_probs(self, dataset: GraphDataset) -> torch.Tensor:
        if self.model is None:
            raise ValueError("model has not been fit")
        self.model.eval()
        with torch.no_grad():
            logits = self.model(dataset.features, dataset.edge_index, dataset.edge_weight)
            probs = torch.sigmoid(logits).detach().cpu()

        if dataset.edge_index.numel() > 0:
            smoothed = probs.clone()
            labeled_targets = dataset.labels.clone().float().cpu()
            for _ in range(3):
                neighbor = torch.zeros_like(smoothed)
                degree = torch.zeros(smoothed.size(0), dtype=smoothed.dtype)
                src = dataset.edge_index[0].cpu()
                dst = dataset.edge_index[1].cpu()
                weight = dataset.edge_weight.cpu() if dataset.edge_weight.numel() else torch.ones(src.shape[0], dtype=smoothed.dtype)
                neighbor.index_add_(0, dst, smoothed[src] * weight)
                degree.index_add_(0, dst, weight)
                neighbor = neighbor / degree.clamp_min(1.0)
                smoothed = 0.65 * smoothed + 0.35 * neighbor
                labeled_mask = dataset.labeled_mask.cpu()
                smoothed[labeled_mask] = labeled_targets[labeled_mask]
            probs = 0.65 * probs + 0.35 * smoothed
        return probs

    def _build_dataset(self, rows: list[tuple[str, TrainingExample]]) -> GraphDataset:
        examples = [example for _, example in rows]
        features = _build_feature_matrix(examples)
        labels = torch.tensor([1 if example.label_status == "positive" else 0 if example.label_status == "negative" else -1 for example in examples], dtype=torch.long)
        labeled_mask = labels >= 0
        labeled_indices = [index for index, flag in enumerate(labeled_mask.tolist()) if flag]
        train_indices, val_indices = _grouped_stratified_split(
            labeled_indices,
            labels,
            examples,
            workbook_paths=[source for source, _ in rows],
            seed=self.seed,
            split_ratio=self.split_ratio,
        )
        train_mask = torch.zeros_like(labeled_mask)
        val_mask = torch.zeros_like(labeled_mask)
        if train_indices:
            train_mask[train_indices] = True
        if val_indices:
            val_mask[val_indices] = True
        edge_index, edge_weight, node_keys = _build_graph(rows)
        return GraphDataset(
            features=features,
            labels=labels,
            labeled_mask=labeled_mask,
            train_mask=train_mask,
            val_mask=val_mask,
            edge_index=edge_index,
            edge_weight=edge_weight,
            examples=examples,
            node_keys=node_keys,
            workbook_paths=[source for source, _ in rows],
        )

    def fit(
        self,
        rows: list[tuple[str, TrainingExample]],
        synthetic_warmup: int = 0,
        self_training_rounds: int = 0,
        pseudo_positive_threshold: float = 0.9,
        pseudo_negative_threshold: float = 0.1,
        pseudo_max_rows: int = 500,
    ) -> "GraphRiskModel":
        torch.manual_seed(self.seed)
        random.seed(self.seed)

        base_rows = list(rows)
        synthetic_rows = build_synthetic_graph_rows(synthetic_warmup)
        if not any(example.label_status == "positive" for _, example in base_rows):
            synthetic_rows.append(("__synthetic__", _synthetic_example("positive")))
        if not any(example.label_status == "negative" for _, example in base_rows):
            synthetic_rows.append(("__synthetic__", _synthetic_example("negative")))

        working_rows = list(base_rows) + synthetic_rows
        all_pseudo_rows: list[tuple[str, TrainingExample]] = []
        rounds_completed = 0
        best_global = {
            "state": None,
            "val_loss": float("inf"),
            "val_f1": -1.0,
            "epoch": 0,
            "dataset": None,
            "probs": None,
            "labeled_count": 0,
            "train_count": 0,
            "val_count": 0,
        }
        last_epoch = 0

        total_rounds = max(0, int(self_training_rounds))
        for round_index in range(total_rounds + 1):
            dataset = self._build_dataset(working_rows)
            if dataset.features.numel() == 0:
                raise ValueError("no rows available for graph training")
            labeled_count = int(dataset.labeled_mask.sum().item())
            if labeled_count == 0:
                raise ValueError("no labeled examples available for graph training")

            positive_count = int(((dataset.labels == 1) & dataset.train_mask).sum().item())
            negative_count = int(((dataset.labels == 0) & dataset.train_mask).sum().item())
            self.model = RowGNNClassifier(dataset.features.size(1), hidden_dim=self.hidden_dim, dropout=self.dropout)
            optimizer = torch.optim.Adam(self.model.parameters(), lr=self.lr, weight_decay=1e-4)
            positive_ratio = positive_count / max(positive_count + negative_count, 1)
            alpha = min(0.9, max(0.35, 1.0 - positive_ratio))
            focal_loss = FocalLoss(alpha=alpha)

            best_state = None
            best_val_loss = float("inf")
            best_val_f1 = -1.0
            best_epoch = 0
            patience_left = self.patience
            train_mask = dataset.train_mask
            val_mask = dataset.val_mask if int(dataset.val_mask.sum().item()) > 0 else dataset.train_mask

            for epoch in range(1, self.epochs + 1):
                self.model.train()
                optimizer.zero_grad()
                logits = self.model(dataset.features, dataset.edge_index, dataset.edge_weight)
                train_logits = logits[train_mask]
                train_targets = dataset.labels[train_mask].float()
                loss = focal_loss(train_logits, train_targets)
                loss.backward()
                optimizer.step()

                self.model.eval()
                with torch.no_grad():
                    logits = self.model(dataset.features, dataset.edge_index, dataset.edge_weight)
                    val_logits = logits[val_mask]
                    val_targets = dataset.labels[val_mask].long()
                    val_loss = focal_loss(val_logits, val_targets.float()).item() if val_logits.numel() else float(loss.item())
                    val_prob = torch.sigmoid(val_logits) if val_logits.numel() else torch.empty(0)
                    metrics = _metrics_from_predictions(val_targets, val_prob) if val_logits.numel() else {"f1": 0.0}
                    val_f1 = float(metrics.get("f1", 0.0))

                if val_loss < best_val_loss or (math.isclose(val_loss, best_val_loss) and val_f1 > best_val_f1):
                    best_val_loss = val_loss
                    best_val_f1 = val_f1
                    best_epoch = epoch
                    best_state = {
                        key: value.detach().cpu().clone()
                        for key, value in self.model.state_dict().items()
                    }
                    patience_left = self.patience
                else:
                    patience_left -= 1
                    if patience_left <= 0:
                        break

            if best_state is not None:
                self.model.load_state_dict(best_state)

            probs = self._predict_dataset_probs(dataset)

            if best_val_loss < best_global["val_loss"] or (math.isclose(best_val_loss, best_global["val_loss"]) and best_val_f1 > best_global["val_f1"]):
                best_global = {
                    "state": {
                        key: value.detach().cpu().clone()
                        for key, value in self.model.state_dict().items()
                    },
                    "val_loss": best_val_loss,
                    "val_f1": best_val_f1,
                    "epoch": best_epoch,
                    "dataset": dataset,
                    "probs": probs.clone(),
                    "labeled_count": labeled_count,
                    "train_count": int(train_mask.sum().item()),
                    "val_count": int(val_mask.sum().item()),
                }

            last_epoch = epoch
            rounds_completed = round_index + 1
            if round_index >= total_rounds:
                break

            pseudo_rows = _pseudo_label_rows(
                base_rows,
                {key: float(probs[index].item()) for index, key in enumerate(dataset.node_keys)},
                pseudo_positive_threshold,
                pseudo_negative_threshold,
                pseudo_max_rows,
            )
            if not pseudo_rows:
                break
            all_pseudo_rows = pseudo_rows
            working_rows = list(base_rows) + synthetic_rows + all_pseudo_rows

        if best_global["state"] is None or best_global["dataset"] is None or best_global["probs"] is None:
            raise ValueError("graph training failed")

        self.model.load_state_dict(best_global["state"])
        dataset = best_global["dataset"]
        probs = best_global["probs"]
        self.dataset = dataset
        self.node_examples = {key: example for key, example in zip(dataset.node_keys, dataset.examples)}
        self.node_keys = list(dataset.node_keys)
        self.edge_index = dataset.edge_index
        self.edge_weight = dataset.edge_weight
        self.node_scores = {key: float(probs[index].item()) for index, key in enumerate(dataset.node_keys)}
        self.training_summary = GraphTrainingSummary(
            model_name="row_gnn",
            feature_dim=int(dataset.features.size(1)),
            total_nodes=len(dataset.examples),
            labeled_nodes=best_global["labeled_count"],
            synthetic_rows=len(synthetic_rows),
            pseudo_labeled_rows=len(all_pseudo_rows),
            self_training_rounds=total_rounds,
            train_nodes=best_global["train_count"],
            val_nodes=best_global["val_count"],
            epochs=last_epoch,
            best_epoch=best_global["epoch"],
            best_val_loss=best_global["val_loss"],
            best_val_f1=best_global["val_f1"],
            positive_rate=sum(1 for _, example in base_rows if example.label_status == "positive") / max(
                sum(1 for _, example in base_rows if example.label_status in {"positive", "negative"}),
                1,
            ),
        )
        return self

    def score_rows(self, rows: list[tuple[str, TrainingExample]]) -> dict[str, float]:
        if self.model is None:
            raise ValueError("model has not been fit")
        dataset = self._build_dataset(rows)
        if dataset.features.numel() == 0:
            return {}
        probs = self._predict_dataset_probs(dataset)
        return {key: float(probs[index].item()) for index, key in enumerate(dataset.node_keys)}

    def score(self, example: TrainingExample) -> float:
        if self.model is None or self.dataset is None:
            raise ValueError("model has not been fit")
        tmp_rows = [("inference", example)]
        scores = self.score_rows(tmp_rows)
        return scores.get(_row_key(example, "inference"), 0.5)

    def save(self, model_path: str | Path, metadata_path: str | Path | None = None) -> Path:
        if self.model is None:
            raise ValueError("model has not been fit")
        path = Path(model_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "state_dict": {key: value.detach().cpu() for key, value in self.model.state_dict().items()},
            "hidden_dim": self.hidden_dim,
            "dropout": self.dropout,
            "lr": self.lr,
            "seed": self.seed,
            "split_ratio": self.split_ratio,
            "patience": self.patience,
            "feature_dim": int(self.training_summary.feature_dim),
            "training_summary": self.training_summary.to_dict(),
        }
        torch.save(payload, path)
        if metadata_path:
            meta_path = Path(metadata_path)
            meta_path.parent.mkdir(parents=True, exist_ok=True)
            meta_path.write_text(json.dumps(payload["training_summary"], ensure_ascii=False, indent=2), encoding="utf-8")
        return path

    @classmethod
    def load(cls, model_path: str | Path) -> "GraphRiskModel":
        payload = torch.load(Path(model_path), map_location="cpu")
        model = cls(
            hidden_dim=int(payload.get("hidden_dim", 64)),
            dropout=float(payload.get("dropout", 0.25)),
            lr=float(payload.get("lr", 0.01)),
            seed=int(payload.get("seed", 42)),
            split_ratio=float(payload.get("split_ratio", 0.8)),
            patience=int(payload.get("patience", 20)),
        )
        feature_dim = int(payload.get("feature_dim", 0))
        if feature_dim <= 0:
            raise ValueError("invalid saved feature_dim")
        model.model = RowGNNClassifier(feature_dim, hidden_dim=model.hidden_dim, dropout=model.dropout)
        model.model.load_state_dict(payload["state_dict"])
        training_summary = payload.get("training_summary", {})
        model.training_summary = GraphTrainingSummary(
            model_name=str(training_summary.get("model_name", "row_gnn")),
            feature_dim=int(training_summary.get("feature_dim", feature_dim)),
            total_nodes=int(training_summary.get("total_nodes", 0)),
            labeled_nodes=int(training_summary.get("labeled_nodes", 0)),
            synthetic_rows=int(training_summary.get("synthetic_rows", 0)),
            pseudo_labeled_rows=int(training_summary.get("pseudo_labeled_rows", 0)),
            self_training_rounds=int(training_summary.get("self_training_rounds", 0)),
            train_nodes=int(training_summary.get("train_nodes", 0)),
            val_nodes=int(training_summary.get("val_nodes", 0)),
            epochs=int(training_summary.get("epochs", 0)),
            best_epoch=int(training_summary.get("best_epoch", 0)),
            best_val_loss=float(training_summary.get("best_val_loss", 0.0)),
            best_val_f1=float(training_summary.get("best_val_f1", 0.0)),
            positive_rate=float(training_summary.get("positive_rate", 0.0)),
        )
        return model


def score_directory(
    root: str | Path,
    manifests: list[LabelManifest],
    top_k: int = 100,
    include_labeled: bool = False,
    seed: int = 42,
    synthetic_warmup: int = 0,
    self_training_rounds: int = 0,
    pseudo_positive_threshold: float = 0.9,
    pseudo_negative_threshold: float = 0.1,
    pseudo_max_rows: int = 500,
) -> GraphTriageReport:
    base = Path(root)
    rows: list[tuple[str, TrainingExample]] = []
    workbook_examples: dict[str, list[TrainingExample]] = {}

    for workbook in sorted(base.rglob("*.xlsx")):
        if workbook.name.startswith("~$") or workbook.name.startswith(".~lock."):
            continue
        examples = build_training_examples(workbook, manifests)
        workbook_examples[str(workbook)] = examples
        for example in examples:
            rows.append((str(workbook), example))

    model = GraphRiskModel(seed=seed, split_ratio=0.8)
    model.fit(
        rows,
        synthetic_warmup=synthetic_warmup,
        self_training_rounds=self_training_rounds,
        pseudo_positive_threshold=pseudo_positive_threshold,
        pseudo_negative_threshold=pseudo_negative_threshold,
        pseudo_max_rows=pseudo_max_rows,
    )

    scored_rows: list[GraphRowScore] = []
    workbook_summaries: list[GraphWorkbookSummary] = []
    total_rows = labeled_rows = positive_rows = negative_rows = unlabeled_rows = 0

    for workbook_path, examples in workbook_examples.items():
        workbook_scores: list[GraphRowScore] = []
        for example in examples:
            key = _row_key(example, workbook_path)
            score = model.node_scores.get(key, 0.5)
            total_rows += 1
            if example.label_status == "positive":
                labeled_rows += 1
                positive_rows += 1
            elif example.label_status == "negative":
                labeled_rows += 1
                negative_rows += 1
            else:
                unlabeled_rows += 1
            item = GraphRowScore(
                workbook_path=workbook_path,
                row_index=example.row_index,
                transaction_id=example.transaction_id,
                label_status=example.label_status,
                score=score,
                label=example.label,
                subject=example.subject,
                amount=example.amount,
                timestamp=example.timestamp,
                counterparty=example.counterparty,
                remark=example.remark,
                direction=example.direction,
                channel=example.channel,
                tokens=example_to_tokens(example),
            )
            workbook_scores.append(item)
            if include_labeled or example.label_status == "unlabeled":
                scored_rows.append(item)

        workbook_scores.sort(key=lambda item: (-item.score, item.label_status != "unlabeled", item.row_index))
        workbook_summaries.append(
            GraphWorkbookSummary(
                path=workbook_path,
                total_rows=len(examples),
                labeled_rows=sum(1 for item in examples if item.label_status in {"positive", "negative"}),
                positive_rows=sum(1 for item in examples if item.label_status == "positive"),
                negative_rows=sum(1 for item in examples if item.label_status == "negative"),
                unlabeled_rows=sum(1 for item in examples if item.label_status == "unlabeled"),
                avg_score=mean(item.score for item in workbook_scores) if workbook_scores else 0.0,
                max_score=max((item.score for item in workbook_scores), default=0.0),
                top_rows=workbook_scores[:5],
            )
        )

    scored_rows.sort(key=lambda item: (-item.score, item.workbook_path, item.row_index))
    top_rows = scored_rows[:top_k]
    workbook_summaries.sort(key=lambda item: (-item.max_score, item.path))
    pairs = _pair_candidates(top_rows, limit=20)
    return GraphTriageReport(
        total_workbooks=len(workbook_summaries),
        total_rows=total_rows,
        labeled_rows=labeled_rows,
        positive_rows=positive_rows,
        negative_rows=negative_rows,
        unlabeled_rows=unlabeled_rows,
        top_rows=top_rows,
        workbooks=workbook_summaries,
        pairs=pairs,
        training=model.training_summary,
    )


def export_graph_triage_json(report: GraphTriageReport, output_path: str | Path) -> Path:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report.to_dict(), ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def export_graph_triage_markdown(report: GraphTriageReport, output_path: str | Path) -> Path:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = ["# Graph Triage", ""]
    lines.append(f"- workbooks: {report.total_workbooks}")
    lines.append(f"- rows: {report.total_rows}")
    lines.append(f"- labeled: {report.labeled_rows}")
    lines.append(f"- positive: {report.positive_rows}")
    lines.append(f"- negative: {report.negative_rows}")
    lines.append(f"- unlabeled: {report.unlabeled_rows}")
    lines.append(f"- model: {report.training.model_name}")
    lines.append(f"- feature_dim: {report.training.feature_dim}")
    lines.append(f"- synthetic_rows: {report.training.synthetic_rows}")
    lines.append(f"- pseudo_labeled_rows: {report.training.pseudo_labeled_rows}")
    lines.append(f"- self_training_rounds: {report.training.self_training_rounds}")
    lines.append(f"- best_epoch: {report.training.best_epoch}")
    lines.append(f"- best_val_f1: {report.training.best_val_f1:.4f}")
    lines.append("")
    lines.append("## Top Rows")
    lines.append("")
    lines.append("| score | workbook | row | transaction_id | status | amount | counterparty | remark |")
    lines.append("| --- | --- | ---: | --- | --- | --- | --- | --- |")
    for item in report.top_rows:
        lines.append(
            f"| {item.score:.4f} | {item.workbook_path} | {item.row_index} | {item.transaction_id} | {item.label_status} | {item.amount} | {item.counterparty} | {item.remark} |"
        )
    lines.append("")
    lines.append("## Workbooks")
    lines.append("")
    lines.append("| score | workbook | total | labeled | positive | negative | unlabeled |")
    lines.append("| --- | --- | ---: | ---: | ---: | ---: | ---: |")
    for item in report.workbooks:
        lines.append(
            f"| {item.max_score:.4f} | {item.path} | {item.total_rows} | {item.labeled_rows} | {item.positive_rows} | {item.negative_rows} | {item.unlabeled_rows} |"
        )
    lines.append("")
    lines.append("## Pair Candidates")
    lines.append("")
    lines.append("| pair_score | pair_type | left workbook | left row | left amount | right workbook | right row | right amount | same_counterparty | opposite_direction | gap_min |")
    lines.append("| --- | --- | --- | ---: | --- | --- | ---: | --- | --- | --- | ---: |")
    for item in report.pairs:
        lines.append(
            f"| {item.pair_score:.4f} | {item.pair_type} | {item.left_workbook_path} | {item.left_row_index} | {item.left_amount} | {item.right_workbook_path} | {item.right_row_index} | {item.right_amount} | {str(item.same_counterparty)} | {str(item.opposite_direction)} | {item.time_gap_minutes:.1f} |"
        )
    lines.append("")
    lines.append("## Training")
    lines.append("")
    lines.append(f"- train_nodes: {report.training.train_nodes}")
    lines.append(f"- val_nodes: {report.training.val_nodes}")
    lines.append(f"- epochs: {report.training.epochs}")
    lines.append(f"- best_val_loss: {report.training.best_val_loss:.6f}")
    lines.append(f"- positive_rate: {report.training.positive_rate:.4f}")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path
