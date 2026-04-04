from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path


DEFAULT_RULE_PATTERNS = {
    "qr_hint_tokens": {
        "common": ["二维码", "扫码", "收款码", "付款码", "当面付"],
        "wechat": ["扫二维码付款"],
        "alipay": ["碰一下"],
    },
    "red_packet_tokens": ["红包"],
    "failed_or_invalid_tokens": ["失败", "手续费", "撤销", "关闭", "退款关闭"],
    "platform_account_tokens": {
        "common": [
            "@meituan.com",
            "@bytedance.com",
            "@service.aliyun.com",
            "@alibaba-inc.com",
            "@ccbfund.cn",
            "@fund123.cn",
            "@kuaishou.com",
            "@yiran.com",
        ],
        "wechat": ["pddzhifubao", "zxpayment", "shgwzz", "hnjrtt", "bjztbl", "gtbzhuanqia", "xiaoxuelei"],
        "alipay": ["pddzhifubao", "zxpayment", "shgwzz", "hnjrtt", "bjztbl", "gtbzhuanqia", "xiaoxuelei"],
    },
}


@dataclass(frozen=True)
class RuleSignals:
    is_qr_transfer: bool
    is_red_packet: bool
    is_failed_or_invalid: bool
    is_withdrawal_like: bool
    is_merchant_consume: bool
    is_platform_settlement: bool
    reason_tags: list[str]


def _rule_config_path() -> Path:
    return Path(__file__).resolve().parents[2] / "config" / "rule_patterns.json"


def _normalize_token_config(value: object, default: object) -> object:
    if isinstance(default, dict):
        default_dict = dict(default)
        if not isinstance(value, dict):
            return {key: list(items) for key, items in default_dict.items()}
        normalized: dict[str, list[str]] = {}
        for key, items in default_dict.items():
            current = value.get(key, items)
            if not isinstance(current, list):
                normalized[key] = list(items)
            else:
                normalized[key] = [str(item) for item in current if str(item).strip()]
        return normalized
    if not isinstance(value, list):
        return list(default)
    return [str(item) for item in value if str(item).strip()]


def load_rule_patterns() -> dict[str, object]:
    path = _rule_config_path()
    if not path.exists():
        return {key: list(value) for key, value in DEFAULT_RULE_PATTERNS.items()}
    payload = json.loads(path.read_text(encoding="utf-8"))
    patterns: dict[str, object] = {}
    for key, default in DEFAULT_RULE_PATTERNS.items():
        patterns[key] = _normalize_token_config(payload.get(key, default), default)
    return patterns


RULE_PATTERNS = load_rule_patterns()
QR_HINT_TOKENS = RULE_PATTERNS["qr_hint_tokens"]
RED_PACKET_TOKENS = RULE_PATTERNS["red_packet_tokens"]
FAILED_OR_INVALID_TOKENS = RULE_PATTERNS["failed_or_invalid_tokens"]
PLATFORM_ACCOUNT_TOKENS = RULE_PATTERNS["platform_account_tokens"]


def contains_any(text: str, tokens: list[str]) -> bool:
    value = str(text or "")
    return any(token in value for token in tokens)


def channel_tokens(config: dict[str, list[str]], channel: str) -> list[str]:
    normalized = str(channel or "").strip().lower()
    tokens = list(config.get("common", []))
    if normalized in config:
        tokens.extend(config.get(normalized, []))
    return list(dict.fromkeys(tokens))


def is_platform_account(value: str, channel: str = "") -> bool:
    normalized = str(value or "").strip().lower()
    if not normalized or normalized == "-":
        return False
    tokens = channel_tokens(PLATFORM_ACCOUNT_TOKENS, channel)
    return any(token in normalized for token in tokens)


def detect_rule_signals(
    *,
    channel: str,
    tx_type: str,
    direction: str,
    remark: str,
    merchant_name: str,
    payer_account: str,
    counterparty_account: str,
) -> RuleSignals:
    qr_text = " ".join([remark, merchant_name, tx_type])
    qr_tokens = channel_tokens(QR_HINT_TOKENS, channel)
    is_qr_transfer = contains_any(qr_text, qr_tokens)
    is_red_packet = contains_any(qr_text, RED_PACKET_TOKENS)
    is_failed_or_invalid = contains_any(qr_text, FAILED_OR_INVALID_TOKENS)
    is_withdrawal_like = "提现" in tx_type or "转账至银行卡" in tx_type
    is_merchant_consume = "消费" in tx_type
    is_platform_settlement = ("入" in direction and "消费" in tx_type and is_platform_account(payer_account, channel)) or is_platform_account(counterparty_account, channel)
    reason_tags: list[str] = []
    if is_qr_transfer:
        reason_tags.append("qr_hint")
    if is_red_packet:
        reason_tags.append("red_packet_hint")
    if is_failed_or_invalid:
        reason_tags.append("failed_or_invalid_hint")
    if is_withdrawal_like:
        reason_tags.append("withdrawal_type")
    if is_merchant_consume:
        reason_tags.append("consume_type")
    if is_platform_settlement:
        reason_tags.append("platform_account")
    return RuleSignals(
        is_qr_transfer=is_qr_transfer,
        is_red_packet=is_red_packet,
        is_failed_or_invalid=is_failed_or_invalid,
        is_withdrawal_like=is_withdrawal_like,
        is_merchant_consume=is_merchant_consume,
        is_platform_settlement=is_platform_settlement,
        reason_tags=reason_tags,
    )


def derive_trade_pattern(
    *,
    tx_type: str,
    is_qr_transfer: bool,
    is_red_packet: bool,
    is_failed_or_invalid: bool,
    is_withdrawal_like: bool,
    is_merchant_consume: bool,
    is_platform_settlement: bool,
) -> str:
    if is_failed_or_invalid:
        return "failed_or_invalid"
    if is_withdrawal_like:
        return "withdraw_to_bank"
    if "充值" in tx_type:
        return "recharge"
    if "红包" in tx_type or is_red_packet:
        return "red_packet"
    if is_qr_transfer and ("转账" in tx_type or "消费" in tx_type):
        return "qr_p2p_transfer"
    if "支付账户对支付账户转账" in tx_type or "转账" in tx_type:
        return "p2p_transfer"
    if is_platform_settlement:
        return "platform_settlement"
    if is_merchant_consume:
        return "merchant_consume"
    if "其他" in tx_type:
        return "other"
    return "other"


def flow_family_for_trade_pattern(trade_pattern: str) -> str:
    return {
        "p2p_transfer": "account_transfer",
        "qr_p2p_transfer": "account_transfer",
        "merchant_consume": "merchant_consume",
        "platform_settlement": "platform_settlement",
        "withdraw_to_bank": "withdraw_to_bank",
        "recharge": "recharge",
        "red_packet": "red_packet",
        "failed_or_invalid": "fee_or_adjustment",
        "other": "other",
    }.get(trade_pattern, "other")
