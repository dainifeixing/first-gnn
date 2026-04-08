from __future__ import annotations

import csv
import json
import random
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any
import re

from .identifiers import choose_transaction_id
from .excel import load_xlsx_rows
from .labels import LabelManifest, build_label_index, load_label_manifests
from .owners import OwnerLookup, load_owner_annotations
from .rule_config import detect_rule_signals, derive_trade_pattern, flow_family_for_trade_pattern
from .roles import RoleLookup, load_role_annotations

TIMESTAMP_FORMATS = [
    "%Y-%m-%d %H:%M:%S",
    "%Y/%m/%d %H:%M:%S",
    "%Y-%m-%d %H:%M",
    "%Y/%m/%d %H:%M",
    "%Y-%m-%d",
    "%Y/%m/%d",
    "%Y%m%d%H%M%S",
    "%Y%m%d",
]


@dataclass(frozen=True)
class TrainingSample:
    transaction_id: str
    label: str
    subject: str
    source_file: str
    amount: str
    timestamp: str
    counterparty: str
    remark: str
    raw: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "transaction_id": self.transaction_id,
            "label": self.label,
            "subject": self.subject,
            "source_file": self.source_file,
            "amount": self.amount,
            "timestamp": self.timestamp,
            "counterparty": self.counterparty,
            "remark": self.remark,
            "raw": self.raw,
        }


@dataclass(frozen=True)
class TrainingExample:
    row_index: int
    transaction_id: str
    label: str
    label_status: str
    subject: str
    source_file: str
    amount: str
    timestamp: str
    hour: int | None
    weekday: int | None
    is_night: bool
    counterparty: str
    direction: str
    channel: str
    remark: str
    raw: dict[str, Any] = field(default_factory=dict)
    counterparty_account: str = ""
    counterparty_name: str = ""
    subject_account: str = ""
    tx_id_secondary: str = ""
    balance_after: str = ""
    payer_account: str = ""
    payer_bank_name: str = ""
    payer_bank_card: str = ""
    payee_account: str = ""
    payee_bank_name: str = ""
    payee_bank_card: str = ""
    merchant_id: str = ""
    merchant_name: str = ""
    flow_family: str = ""
    trade_pattern: str = ""
    buyer_account: str = ""
    seller_account: str = ""
    seller_proxy_name: str = ""
    rule_reason: str = ""
    is_qr_transfer: bool = False
    is_red_packet: bool = False
    is_merchant_consume: bool = False
    is_withdrawal_like: bool = False
    is_platform_settlement: bool = False
    is_failed_or_invalid: bool = False
    is_trade_like: bool = False
    role_label: str = ""
    role_confidence: str = ""
    role_scene: str = ""
    role_evidence: str = ""
    owner_id: str = ""
    owner_name: str = ""
    owner_confidence: str = ""
    owner_evidence: str = ""
    extension_role: str = ""
    anchor_subject: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "row_index": self.row_index,
            "transaction_id": self.transaction_id,
            "label": self.label,
            "label_status": self.label_status,
            "subject": self.subject,
            "source_file": self.source_file,
            "amount": self.amount,
            "timestamp": self.timestamp,
            "hour": self.hour,
            "weekday": self.weekday,
            "is_night": self.is_night,
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
            "raw": self.raw,
            "role_label": self.role_label,
            "role_confidence": self.role_confidence,
            "role_scene": self.role_scene,
            "role_evidence": self.role_evidence,
            "owner_id": self.owner_id,
            "owner_name": self.owner_name,
            "owner_confidence": self.owner_confidence,
            "owner_evidence": self.owner_evidence,
            "extension_role": self.extension_role,
            "anchor_subject": self.anchor_subject,
        }


@dataclass(frozen=True)
class DatasetSplit:
    name: str
    examples: list[TrainingExample]

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "size": len(self.examples),
            "label_counts": _count_by_label_status(self.examples),
        }


def _pick_field(row: dict[str, str], candidates: list[str]) -> str:
    normalized = {str(key).strip().lower().replace(" ", ""): value for key, value in row.items()}
    for candidate in candidates:
        key = candidate.strip().lower().replace(" ", "")
        if key in normalized and normalized[key]:
            return str(normalized[key]).strip()
    return ""


def _parse_timestamp(text: str) -> datetime | None:
    value = str(text or "").strip()
    if not value:
        return None
    normalized = value.replace("年", "-").replace("月", "-").replace("日", " ")
    normalized = normalized.replace("时", ":").replace("分", "").replace("T", " ").strip()
    for fmt in TIMESTAMP_FORMATS:
        try:
            return datetime.strptime(normalized, fmt)
        except ValueError:
            continue
    return None


def _pick_any(row: dict[str, str], candidates: list[str]) -> str:
    return _pick_field(row, candidates)


def _normalize_text(value: str) -> str:
    return "".join(str(value or "").strip().lower().split())


def _subject_account_from_path(path: str | Path) -> str:
    name = Path(path).stem.strip()
    match = re.search(r"([A-Za-z0-9._%+-]+@wx\.tenpay\.com)", name, flags=re.IGNORECASE)
    if match:
        return match.group(1)
    match = re.search(r"([A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,})", name)
    if match:
        return match.group(1)
    match = re.search(r"(?<!\d)(1\d{10})(?!\d)", name)
    if match:
        return match.group(1)
    return name


def _channel_from_row(row: dict[str, str], path: str | Path) -> str:
    explicit = _pick_any(row, ["渠道", "平台", "支付方式", "交易渠道"])
    if explicit:
        return explicit
    haystack = " ".join(str(value or "") for value in row.values()) + " " + str(Path(path).name)
    lowered = haystack.lower()
    if "@wx.tenpay.com" in lowered or "微信" in haystack:
        return "微信"
    if "alipay" in lowered or "支付宝" in haystack:
        return "支付宝"
    return ""


def _secondary_transaction_id(row: dict[str, str], primary: str) -> str:
    for candidate in ["银行外部渠道交易流水号", "外部流水号", "网银流水号", "商户订单号", "订单号"]:
        value = _pick_any(row, [candidate])
        if value and value != primary and value != "-":
            return value
    return ""


def _derive_trade_fields(row: dict[str, str], xlsx_path: str | Path, transaction_id: str) -> dict[str, Any]:
    subject_account = _subject_account_from_path(xlsx_path)
    payer_account = _pick_any(row, ["付款支付帐号", "付款账号", "付款账户", "付款方账号"])
    payer_bank_name = _pick_any(row, ["付款银行卡银行名称"])
    payer_bank_card = _pick_any(row, ["付款银行卡号"])
    payee_account = _pick_any(row, ["收款支付帐号", "收款账号", "收款账户", "收款方账号"])
    payee_bank_name = _pick_any(row, ["收款银行卡银行名称"])
    payee_bank_card = _pick_any(row, ["收款银行卡号"])
    merchant_id = _pick_any(row, ["收款方的商户号", "商户号"])
    merchant_name = _pick_any(row, ["收款方的商户名称", "商户名称", "交易对方", "对方名称"])
    remark = _pick_any(row, ["备注", "摘要", "附言", "用途"])
    tx_type = _pick_any(row, ["交易类型", "类型"])
    direction = _pick_any(row, ["交易主体的出入账标识", "方向", "收/支", "收支"])
    channel = _channel_from_row(row, xlsx_path)
    balance_after = _pick_any(row, ["交易余额", "余额"])
    tx_id_secondary = _secondary_transaction_id(row, transaction_id)

    counterparty_account = payee_account if "出" in direction else payer_account if "入" in direction else (payee_account or payer_account)
    counterparty_name = merchant_name
    if not counterparty_name and counterparty_account:
        counterparty_name = counterparty_account

    signals = detect_rule_signals(
        channel=channel,
        tx_type=tx_type,
        direction=direction,
        remark=remark,
        merchant_name=merchant_name,
        payer_account=payer_account,
        counterparty_account=counterparty_account,
    )
    trade_pattern = derive_trade_pattern(
        tx_type=tx_type,
        is_qr_transfer=signals.is_qr_transfer,
        is_red_packet=signals.is_red_packet,
        is_failed_or_invalid=signals.is_failed_or_invalid,
        is_withdrawal_like=signals.is_withdrawal_like,
        is_merchant_consume=signals.is_merchant_consume,
        is_platform_settlement=signals.is_platform_settlement,
    )

    buyer_account = ""
    seller_account = ""
    seller_proxy_name = ""
    is_trade_like = trade_pattern in {"p2p_transfer", "qr_p2p_transfer", "merchant_consume"}
    if trade_pattern in {"p2p_transfer", "qr_p2p_transfer"}:
        buyer_account = payer_account
        seller_account = payee_account
    elif trade_pattern == "merchant_consume":
        buyer_account = payer_account
        seller_account = payee_account if payee_account and payee_account != subject_account else ""
        seller_proxy_name = merchant_name

    flow_family = flow_family_for_trade_pattern(trade_pattern)

    return {
        "subject_account": subject_account,
        "tx_id_secondary": tx_id_secondary,
        "balance_after": balance_after,
        "payer_account": payer_account,
        "payer_bank_name": payer_bank_name,
        "payer_bank_card": payer_bank_card,
        "payee_account": payee_account,
        "payee_bank_name": payee_bank_name,
        "payee_bank_card": payee_bank_card,
        "merchant_id": merchant_id,
        "merchant_name": merchant_name,
        "counterparty_account": counterparty_account,
        "counterparty_name": counterparty_name,
        "flow_family": flow_family,
        "trade_pattern": trade_pattern,
        "buyer_account": buyer_account,
        "seller_account": seller_account,
        "seller_proxy_name": seller_proxy_name,
        "rule_reason": "|".join(signals.reason_tags),
        "is_qr_transfer": signals.is_qr_transfer,
        "is_red_packet": signals.is_red_packet,
        "is_merchant_consume": signals.is_merchant_consume,
        "is_withdrawal_like": signals.is_withdrawal_like,
        "is_platform_settlement": signals.is_platform_settlement,
        "is_failed_or_invalid": signals.is_failed_or_invalid,
        "is_trade_like": is_trade_like,
        "channel": channel,
        "direction": direction,
        "remark": remark,
    }


def _weekday_index(timestamp: str) -> int | None:
    parsed = _parse_timestamp(timestamp)
    if parsed is None:
        return None
    return parsed.weekday()


def _hour_index(timestamp: str) -> int | None:
    parsed = _parse_timestamp(timestamp)
    if parsed is None:
        return None
    return parsed.hour


def _is_night_hour(hour: int | None) -> bool:
    if hour is None:
        return False
    return hour >= 22 or hour < 6


def _manifest_signature(manifest: LabelManifest) -> tuple[str, str, str]:
    return (manifest.label, manifest.polarity, manifest.subject)


def _infer_extension_role(
    manifest: LabelManifest | None,
    derived: dict[str, Any],
    role_label: str,
) -> tuple[str, str]:
    if manifest is None:
        return "", ""
    subject = str(manifest.subject or "").strip()
    label = str(manifest.label or "").strip().lower()
    subject_text = _normalize_text(subject)
    seller_account = _normalize_text(str(derived.get("seller_account", "")))
    buyer_account = _normalize_text(str(derived.get("buyer_account", "")))

    if manifest.polarity == "positive":
        if "嫖客" in subject or "payment" in label or role_label == "buyer":
            return "buyer_to_known_seller", subject
        if seller_account and ("卖淫女" in subject or "income" in label or role_label == "seller"):
            return "seller_anchor", subject
        if buyer_account and seller_account:
            return "buyer_to_known_seller", subject
        return ("seller_anchor" if subject_text else "positive_transaction"), subject

    if manifest.polarity == "negative":
        if derived.get("is_platform_settlement") or derived.get("is_withdrawal_like"):
            return "non_extension_negative", subject
        if derived.get("trade_pattern") == "merchant_consume":
            return "non_extension_negative", subject
        return "negative_transaction", subject

    return "", subject


def _build_manifest_lookup(manifests: list[LabelManifest]) -> dict[str, LabelManifest]:
    manifest_by_id: dict[str, LabelManifest] = {}
    conflicts: list[str] = []
    for manifest in manifests:
        for transaction_id in manifest.transaction_ids:
            existing = manifest_by_id.get(transaction_id)
            if existing is None:
                manifest_by_id[transaction_id] = manifest
                continue
            if _manifest_signature(existing) != _manifest_signature(manifest):
                conflicts.append(
                    f"{transaction_id}: "
                    f"{existing.label}/{existing.polarity}/{existing.subject} "
                    f"vs {manifest.label}/{manifest.polarity}/{manifest.subject}"
                )
    if conflicts:
        preview = "; ".join(conflicts[:5])
        suffix = " ..." if len(conflicts) > 5 else ""
        raise ValueError(f"conflicting label manifests detected for transaction ids: {preview}{suffix}")
    return manifest_by_id


def build_positive_training_samples(
    xlsx_path: str | Path,
    manifests: list[LabelManifest],
) -> list[TrainingSample]:
    rows = load_xlsx_rows(xlsx_path)
    label_index = build_label_index(manifests)
    manifest_by_id = _build_manifest_lookup(manifests)

    samples: list[TrainingSample] = []
    for row_index, row in enumerate(rows, start=1):
        transaction_id = choose_transaction_id(row, row_index=row_index)
        if transaction_id not in label_index:
            continue
        manifest = manifest_by_id[transaction_id]
        samples.append(
            TrainingSample(
                transaction_id=transaction_id,
                label=manifest.label,
                subject=manifest.subject,
                source_file=manifest.source_file,
                amount=_pick_field(row, ["交易金额", "金额", "收款金额", "支出金额", "交易额"]),
                timestamp=_pick_field(row, ["交易时间", "时间", "发生时间", "日期时间"]),
                counterparty=_pick_field(row, ["收款方", "付款方", "交易对方", "收款支付帐号", "付款支付帐号", "收款方的商户名称"]),
                remark=_pick_field(row, ["备注", "摘要", "附言", "用途"]),
                raw={str(k): str(v) for k, v in row.items() if v is not None},
            )
        )
    return samples


def build_training_examples(
    xlsx_path: str | Path,
    manifests: list[LabelManifest],
    role_annotation_path: str | Path | None = None,
    owner_annotation_path: str | Path | None = None,
) -> list[TrainingExample]:
    rows = load_xlsx_rows(xlsx_path)
    label_index = build_label_index(manifests)
    manifest_by_id = _build_manifest_lookup(manifests)
    role_lookup = RoleLookup(load_role_annotations(role_annotation_path)) if role_annotation_path else RoleLookup([])
    owner_lookup = OwnerLookup(load_owner_annotations(owner_annotation_path)) if owner_annotation_path else OwnerLookup([])

    examples: list[TrainingExample] = []
    for index, row in enumerate(rows, start=1):
        transaction_id = choose_transaction_id(row, row_index=index)
        manifest = manifest_by_id.get(transaction_id)
        timestamp = _pick_any(row, ["交易时间", "时间", "发生时间", "日期时间"])
        hour = _hour_index(timestamp)
        derived = _derive_trade_fields(row, xlsx_path, transaction_id)
        counterparty = derived["counterparty_account"] or derived["counterparty_name"] or _pick_any(
            row, ["收款方", "付款方", "交易对方", "收款支付帐号", "付款支付帐号", "收款方的商户名称"]
        )
        owner = owner_lookup.resolve(transaction_id, counterparty)
        role = role_lookup.resolve(transaction_id, counterparty, owner.owner_id if owner else "")
        extension_role, anchor_subject = _infer_extension_role(
            manifest,
            derived,
            role.role_label if role else "",
        )
        examples.append(
            TrainingExample(
                row_index=index,
                transaction_id=transaction_id,
                label=manifest.label if transaction_id in label_index and manifest else "",
                label_status=manifest.polarity if transaction_id in label_index and manifest else "unlabeled",
                subject=manifest.subject if manifest else "",
                source_file=manifest.source_file if manifest else "",
                amount=_pick_any(row, ["交易金额", "金额", "收款金额", "支出金额", "交易额"]),
                timestamp=timestamp,
                hour=hour,
                weekday=_weekday_index(timestamp),
                is_night=_is_night_hour(hour),
                counterparty=counterparty,
                counterparty_account=str(derived["counterparty_account"]),
                counterparty_name=str(derived["counterparty_name"]),
                direction=str(derived["direction"]),
                channel=str(derived["channel"]),
                remark=str(derived["remark"]),
                subject_account=str(derived["subject_account"]),
                tx_id_secondary=str(derived["tx_id_secondary"]),
                balance_after=str(derived["balance_after"]),
                payer_account=str(derived["payer_account"]),
                payer_bank_name=str(derived["payer_bank_name"]),
                payer_bank_card=str(derived["payer_bank_card"]),
                payee_account=str(derived["payee_account"]),
                payee_bank_name=str(derived["payee_bank_name"]),
                payee_bank_card=str(derived["payee_bank_card"]),
                merchant_id=str(derived["merchant_id"]),
                merchant_name=str(derived["merchant_name"]),
                flow_family=str(derived["flow_family"]),
                trade_pattern=str(derived["trade_pattern"]),
                buyer_account=str(derived["buyer_account"]),
                seller_account=str(derived["seller_account"]),
                seller_proxy_name=str(derived["seller_proxy_name"]),
                rule_reason=str(derived["rule_reason"]),
                is_qr_transfer=bool(derived["is_qr_transfer"]),
                is_red_packet=bool(derived["is_red_packet"]),
                is_merchant_consume=bool(derived["is_merchant_consume"]),
                is_withdrawal_like=bool(derived["is_withdrawal_like"]),
                is_platform_settlement=bool(derived["is_platform_settlement"]),
                is_failed_or_invalid=bool(derived["is_failed_or_invalid"]),
                is_trade_like=bool(derived["is_trade_like"]),
                raw={str(k): str(v) for k, v in row.items() if v is not None},
                role_label=role.role_label if role else "",
                role_confidence=role.confidence if role else "",
                role_scene=role.scene if role else "",
                role_evidence=role.evidence if role else "",
                owner_id=owner.owner_id if owner else "",
                owner_name=owner.owner_name if owner else "",
                owner_confidence=owner.confidence if owner else "",
                owner_evidence=owner.evidence if owner else "",
                extension_role=extension_role,
                anchor_subject=anchor_subject,
            )
        )
    return examples


def export_training_samples_csv(samples: list[TrainingSample], output_path: str | Path) -> Path:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=["transaction_id", "label", "subject", "source_file", "amount", "timestamp", "counterparty", "remark"],
        )
        writer.writeheader()
        for sample in samples:
            row = sample.to_dict()
            writer.writerow({key: row.get(key, "") for key in writer.fieldnames})
    return path


def export_training_samples_jsonl(samples: list[TrainingSample], output_path: str | Path) -> Path:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for sample in samples:
            handle.write(json.dumps(sample.to_dict(), ensure_ascii=False) + "\n")
    return path


def export_training_examples_csv(examples: list[TrainingExample], output_path: str | Path) -> Path:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "row_index",
                "transaction_id",
                "label",
                "label_status",
                "subject",
                "source_file",
                "amount",
                "timestamp",
                "hour",
                "weekday",
                "is_night",
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
                "payee_account",
                "merchant_name",
                "flow_family",
                "trade_pattern",
                "buyer_account",
                "seller_account",
                "seller_proxy_name",
                "rule_reason",
                "is_qr_transfer",
                "is_red_packet",
                "is_platform_settlement",
                "is_failed_or_invalid",
                "is_trade_like",
                "extension_role",
                "anchor_subject",
            ],
        )
        writer.writeheader()
        for example in examples:
            row = example.to_dict()
            writer.writerow({key: row.get(key, "") for key in writer.fieldnames})
    return path


def export_training_examples_jsonl(examples: list[TrainingExample], output_path: str | Path) -> Path:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for example in examples:
            handle.write(json.dumps(example.to_dict(), ensure_ascii=False) + "\n")
    return path


def _count_by_label_status(examples: list[TrainingExample]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for example in examples:
        counts[example.label_status] = counts.get(example.label_status, 0) + 1
    return counts


def split_training_examples(
    examples: list[TrainingExample],
    train_ratio: float = 0.8,
    seed: int = 42,
) -> list[DatasetSplit]:
    if not 0 < train_ratio < 1:
        raise ValueError("train_ratio must be between 0 and 1")
    grouped: dict[tuple[str, str], list[TrainingExample]] = {}
    for example in examples:
        key = (example.label_status, example.label or "__unlabeled__")
        grouped.setdefault(key, []).append(example)
    rng = random.Random(seed)
    train: list[TrainingExample] = []
    val: list[TrainingExample] = []
    for items in grouped.values():
        shuffled = list(items)
        rng.shuffle(shuffled)
        if len(shuffled) == 1:
            train.extend(shuffled)
            continue
        cut = max(1, min(len(shuffled) - 1, int(round(len(shuffled) * train_ratio))))
        train.extend(shuffled[:cut])
        val.extend(shuffled[cut:])
    return [DatasetSplit(name="train", examples=train), DatasetSplit(name="validation", examples=val)]


def export_split_jsonl(split: DatasetSplit, output_path: str | Path) -> Path:
    return export_training_examples_jsonl(split.examples, output_path)


def export_split_csv(split: DatasetSplit, output_path: str | Path) -> Path:
    return export_training_examples_csv(split.examples, output_path)
