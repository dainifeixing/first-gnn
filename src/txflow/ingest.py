from __future__ import annotations

import csv
import re
from dataclasses import replace
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Iterable

from .models import TransactionRecord


HEADER_ALIASES = {
    "record_id": ["交易单号", "订单号", "流水号", "编号", "记录编号", "id"],
    "timestamp": ["时间", "交易时间", "发生时间", "日期时间", "datetime", "time", "日期"],
    "amount": ["金额", "交易金额", "收款金额", "支出金额", "amount", "交易额"],
    "source": ["付款方", "转出方", "付款账户", "转出账户", "来源", "发送方", "交易账户"],
    "target": ["收款方", "转入方", "收款账户", "转入账户", "去向", "接收方", "交易对方"],
    "direction": ["方向", "收/支", "收支", "交易类型", "类型"],
    "channel": ["渠道", "平台", "支付方式", "交易渠道"],
    "remark": ["备注", "摘要", "附言", "用途", "说明"],
    "account": ["本方账户", "账户", "账户名", "主体账户", "我方账户"],
}

DATETIME_FORMATS = [
    "%Y-%m-%d %H:%M:%S",
    "%Y/%m/%d %H:%M:%S",
    "%Y-%m-%d %H:%M",
    "%Y/%m/%d %H:%M",
    "%Y-%m-%d",
    "%Y/%m/%d",
    "%Y%m%d%H%M%S",
    "%Y%m%d",
]


def _normalize_header(name: str) -> str:
    return re.sub(r"\s+", "", str(name or "")).strip().lower()


def _match_field(header: str, field: str) -> bool:
    normalized = _normalize_header(header)
    return any(normalized == _normalize_header(alias) for alias in HEADER_ALIASES[field])


def _parse_decimal(value: str | int | float | Decimal | None) -> Decimal:
    if value is None:
        return Decimal("0")
    if isinstance(value, Decimal):
        return value
    if isinstance(value, (int, float)):
        return Decimal(str(value))
    text = str(value).strip()
    if not text:
        return Decimal("0")
    text = text.replace(",", "").replace("￥", "").replace("元", "")
    match = re.search(r"(-?\d+(?:\.\d+)?)", text)
    if not match:
        return Decimal("0")
    try:
        return Decimal(match.group(1))
    except InvalidOperation:
        return Decimal("0")


def _parse_timestamp(value: str | None) -> datetime | None:
    if not value:
        return None
    text = str(value).strip()
    if not text:
        return None
    text = text.replace("年", "-").replace("月", "-").replace("日", " ").replace("时", ":").replace("分", "")
    text = text.replace("T", " ").strip()
    for fmt in DATETIME_FORMATS:
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


def _read_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", errors="ignore", newline="") as handle:
        reader = csv.DictReader(handle)
        rows = [dict(row) for row in reader]
        if rows:
            return rows
    with path.open("r", encoding="gb18030", errors="ignore", newline="") as handle:
        reader = csv.DictReader(handle)
        return [dict(row) for row in reader]


def _field_value(row: dict[str, str], field: str) -> str:
    for key, value in row.items():
        if _match_field(key, field) and value is not None:
            return str(value).strip()
    return ""


def _infer_direction(text: str) -> str:
    normalized = str(text or "").strip()
    if any(token in normalized for token in ["支出", "转出", "付款", "消费", "支"]):
        return "outflow"
    if any(token in normalized for token in ["收入", "转入", "收款", "入账", "收"]):
        return "inflow"
    return "unknown"


def _build_record(row: dict[str, str], index: int) -> TransactionRecord:
    record_id = _field_value(row, "record_id") or f"row-{index + 1}"
    timestamp = _parse_timestamp(_field_value(row, "timestamp"))
    amount = _parse_decimal(_field_value(row, "amount"))
    source = _field_value(row, "source")
    target = _field_value(row, "target")
    direction = _field_value(row, "direction") or _infer_direction(row.get("方向", "") or row.get("收/支", "") or row.get("类型", ""))
    channel = _field_value(row, "channel")
    remark = _field_value(row, "remark")
    account = _field_value(row, "account")

    if not source and account and direction == "inflow":
        source = "external"
        target = account
    elif not target and account and direction == "outflow":
        source = account
        target = "external"

    if not source:
        source = _field_value(row, "target") or "unknown"
    if not target:
        target = _field_value(row, "source") or "unknown"

    return TransactionRecord(
        record_id=record_id,
        timestamp=timestamp,
        amount=amount,
        source=source or "unknown",
        target=target or "unknown",
        direction=direction or "unknown",
        channel=channel,
        remark=remark,
        raw={k: str(v) for k, v in row.items() if v is not None},
    )


def load_transactions(rows: Iterable[dict[str, str]]) -> list[TransactionRecord]:
    return [_build_record(dict(row), index) for index, row in enumerate(rows)]


def load_transactions_from_path(path: str | Path) -> list[TransactionRecord]:
    source = Path(path)
    if source.is_dir():
        records: list[TransactionRecord] = []
        for item in sorted(source.rglob("*.csv")):
            records.extend(load_transactions_from_path(item))
        return records
    if source.suffix.lower() != ".csv":
        raise ValueError(f"unsupported input file: {source}")
    return load_transactions(_read_csv_rows(source))

