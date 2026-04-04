from __future__ import annotations

import json
import math
import re
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .training import TrainingExample


TOKEN_RE = re.compile(r"[A-Za-z0-9\u4e00-\u9fff]+")


def _normalize_text(value: str) -> str:
    return str(value or "").strip().lower()


def _tokenize(value: str, prefix: str) -> list[str]:
    text = str(value or "").strip()
    if not text:
        return []
    return [f"{prefix}:{token.lower()}" for token in TOKEN_RE.findall(text)]


def _amount_bucket(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return "unknown"
    match = re.search(r"(-?\d+(?:\.\d+)?)", text.replace(",", ""))
    if not match:
        return "unknown"
    amount = abs(float(match.group(1)))
    if amount < 10:
        return "lt_10"
    if amount < 100:
        return "lt_100"
    if amount < 500:
        return "lt_500"
    if amount < 1000:
        return "lt_1000"
    if amount < 5000:
        return "lt_5000"
    if amount < 10000:
        return "lt_10000"
    return "gte_10000"


def _hour_bucket(hour: int | None) -> str:
    if hour is None:
        return "unknown"
    if hour < 6:
        return "night_0_5"
    if hour < 10:
        return "morning_6_9"
    if hour < 14:
        return "midday_10_13"
    if hour < 18:
        return "afternoon_14_17"
    if hour < 22:
        return "evening_18_21"
    return "night_22_23"


def example_to_tokens(example: TrainingExample) -> list[str]:
    tokens = [
        f"is_night:{int(example.is_night)}",
        f"amount_bucket:{_amount_bucket(example.amount)}",
        f"hour_bucket:{_hour_bucket(example.hour)}",
        f"weekday:{example.weekday if example.weekday is not None else 'unknown'}",
    ]
    if example.flow_family:
        tokens.append(f"flow_family:{example.flow_family}")
    if example.trade_pattern:
        tokens.append(f"trade_pattern:{example.trade_pattern}")
    if example.rule_reason:
        tokens.extend(_tokenize(example.rule_reason, "rule_reason"))
    if example.is_qr_transfer:
        tokens.append("is_qr_transfer:1")
    if example.is_red_packet:
        tokens.append("is_red_packet:1")
    if example.is_platform_settlement:
        tokens.append("is_platform_settlement:1")
    if example.is_withdrawal_like:
        tokens.append("is_withdrawal_like:1")
    if example.is_failed_or_invalid:
        tokens.append("is_failed_or_invalid:1")
    if example.is_trade_like:
        tokens.append("is_trade_like:1")
    if example.role_label:
        tokens.append(f"role:{example.role_label}")
    if example.role_confidence:
        tokens.append(f"role_confidence:{example.role_confidence}")
    if example.role_scene:
        tokens.append(f"role_scene:{example.role_scene}")
    if example.owner_id:
        tokens.append(f"owner_id:{example.owner_id}")
    if example.owner_confidence:
        tokens.append(f"owner_confidence:{example.owner_confidence}")
    tokens.extend(_tokenize(example.counterparty, "counterparty"))
    tokens.extend(_tokenize(example.counterparty_account, "counterparty_account"))
    tokens.extend(_tokenize(example.counterparty_name, "counterparty_name"))
    tokens.extend(_tokenize(example.remark, "remark"))
    tokens.extend(_tokenize(example.direction, "direction"))
    tokens.extend(_tokenize(example.channel, "channel"))
    tokens.extend(_tokenize(example.subject_account, "subject_account"))
    tokens.extend(_tokenize(example.payer_account, "payer_account"))
    tokens.extend(_tokenize(example.payee_account, "payee_account"))
    tokens.extend(_tokenize(example.merchant_name, "merchant_name"))
    tokens.extend(_tokenize(example.buyer_account, "buyer_account"))
    tokens.extend(_tokenize(example.seller_account, "seller_account"))
    tokens.extend(_tokenize(example.seller_proxy_name, "seller_proxy"))
    tokens.extend(_tokenize(example.owner_name, "owner_name"))
    if not tokens:
        tokens.append("bias:1")
    return tokens


def _log_softmax(scores: dict[str, float]) -> dict[str, float]:
    max_score = max(scores.values())
    denom = sum(math.exp(value - max_score) for value in scores.values())
    return {label: value - max_score - math.log(denom) for label, value in scores.items()}


@dataclass
class BaselineTextClassifier:
    alpha: float = 1.0
    positive_label: str = "positive"
    negative_label: str = "negative"
    label_counts: dict[str, int] = field(default_factory=dict)
    token_counts: dict[str, dict[str, int]] = field(default_factory=dict)
    token_totals: dict[str, int] = field(default_factory=dict)
    vocabulary: set[str] = field(default_factory=set)

    def fit(self, examples: list[TrainingExample]) -> "BaselineTextClassifier":
        label_counts: Counter[str] = Counter()
        token_counts: dict[str, Counter[str]] = defaultdict(Counter)
        vocabulary: set[str] = set()

        for example in examples:
            if example.label_status not in {self.positive_label, self.negative_label}:
                continue
            label_counts[example.label_status] += 1
            tokens = example_to_tokens(example)
            vocabulary.update(tokens)
            token_counts[example.label_status].update(tokens)

        if not label_counts:
            raise ValueError("no labeled examples available for training")
        if self.positive_label not in label_counts or self.negative_label not in label_counts:
            raise ValueError("both positive and negative examples are required")

        self.label_counts = dict(label_counts)
        self.token_counts = {label: dict(counter) for label, counter in token_counts.items()}
        self.token_totals = {label: sum(counter.values()) for label, counter in token_counts.items()}
        self.vocabulary = vocabulary
        return self

    @property
    def vocabulary_size(self) -> int:
        return max(len(self.vocabulary), 1)

    @property
    def classes(self) -> tuple[str, str]:
        return (self.negative_label, self.positive_label)

    def _class_log_prob(self, label: str, tokens: list[str]) -> float:
        total_examples = sum(self.label_counts.values())
        class_count = self.label_counts.get(label, 0)
        log_prob = math.log((class_count + self.alpha) / (total_examples + self.alpha * len(self.classes)))
        token_total = self.token_totals.get(label, 0)
        class_token_counts = self.token_counts.get(label, {})
        denom = token_total + self.alpha * self.vocabulary_size
        for token in tokens:
            token_count = class_token_counts.get(token, 0)
            log_prob += math.log((token_count + self.alpha) / denom)
        return log_prob

    def predict_proba(self, example: TrainingExample) -> dict[str, float]:
        tokens = example_to_tokens(example)
        scores = {label: self._class_log_prob(label, tokens) for label in self.classes}
        log_probs = _log_softmax(scores)
        return {label: math.exp(value) for label, value in log_probs.items()}

    def predict(self, example: TrainingExample) -> str:
        proba = self.predict_proba(example)
        return max(proba.items(), key=lambda item: item[1])[0]

    def predict_batch(self, examples: list[TrainingExample]) -> list[str]:
        return [self.predict(example) for example in examples]

    def evaluate(self, examples: list[TrainingExample]) -> dict[str, Any]:
        labeled = [example for example in examples if example.label_status in {self.positive_label, self.negative_label}]
        if not labeled:
            return {
                "total": 0,
                "accuracy": 0.0,
                "precision": 0.0,
                "recall": 0.0,
                "f1": 0.0,
                "confusion_matrix": {"tp": 0, "fp": 0, "tn": 0, "fn": 0},
            }

        tp = fp = tn = fn = 0
        for example in labeled:
            predicted = self.predict(example)
            actual = example.label_status
            if predicted == self.positive_label and actual == self.positive_label:
                tp += 1
            elif predicted == self.positive_label and actual == self.negative_label:
                fp += 1
            elif predicted == self.negative_label and actual == self.negative_label:
                tn += 1
            elif predicted == self.negative_label and actual == self.positive_label:
                fn += 1

        total = len(labeled)
        accuracy = (tp + tn) / total if total else 0.0
        precision = tp / (tp + fp) if (tp + fp) else 0.0
        recall = tp / (tp + fn) if (tp + fn) else 0.0
        f1 = (2 * precision * recall / (precision + recall)) if (precision + recall) else 0.0
        return {
            "total": total,
            "accuracy": round(accuracy, 4),
            "precision": round(precision, 4),
            "recall": round(recall, 4),
            "f1": round(f1, 4),
            "confusion_matrix": {"tp": tp, "fp": fp, "tn": tn, "fn": fn},
        }

    def to_dict(self) -> dict[str, Any]:
        return {
            "alpha": self.alpha,
            "positive_label": self.positive_label,
            "negative_label": self.negative_label,
            "label_counts": self.label_counts,
            "token_counts": self.token_counts,
            "token_totals": self.token_totals,
            "vocabulary": sorted(self.vocabulary),
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "BaselineTextClassifier":
        model = cls(
            alpha=float(payload.get("alpha", 1.0)),
            positive_label=str(payload.get("positive_label", "positive")),
            negative_label=str(payload.get("negative_label", "negative")),
        )
        model.label_counts = {str(key): int(value) for key, value in payload.get("label_counts", {}).items()}
        model.token_counts = {
            str(label): {str(token): int(count) for token, count in counts.items()}
            for label, counts in payload.get("token_counts", {}).items()
        }
        model.token_totals = {str(label): int(value) for label, value in payload.get("token_totals", {}).items()}
        model.vocabulary = {str(token) for token in payload.get("vocabulary", [])}
        return model

    def save(self, output_path: str | Path) -> Path:
        path = Path(output_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(self.to_dict(), ensure_ascii=False, indent=2), encoding="utf-8")
        return path

    @classmethod
    def load(cls, input_path: str | Path) -> "BaselineTextClassifier":
        payload = json.loads(Path(input_path).read_text(encoding="utf-8"))
        return cls.from_dict(payload)


def train_baseline_classifier(examples: list[TrainingExample]) -> BaselineTextClassifier:
    return BaselineTextClassifier().fit(examples)
