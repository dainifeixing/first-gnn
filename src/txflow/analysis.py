from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import asdict
from datetime import date
from decimal import Decimal
from pathlib import Path
from statistics import pstdev
from typing import Iterable

from .ingest import load_transactions_from_path
from .models import AnalysisResult, EdgeProfile, NodeProfile, RiskFinding, TransactionRecord


NIGHT_START = 22
NIGHT_END = 6


def _counterparty(record: TransactionRecord) -> str:
    if record.direction == "inflow":
        return record.source
    if record.direction == "outflow":
        return record.target
    return record.target if record.target != "unknown" else record.source


def _active_day_key(record: TransactionRecord) -> str:
    if not record.timestamp:
        return "unknown"
    return record.timestamp.date().isoformat()


def _is_night(record: TransactionRecord) -> bool:
    if not record.timestamp:
        return False
    hour = record.timestamp.hour
    return hour >= NIGHT_START or hour < NIGHT_END


def _std_decimal(values: list[Decimal]) -> Decimal:
    if len(values) < 2:
        return Decimal("0")
    try:
        return Decimal(str(pstdev([float(v) for v in values])))
    except Exception:
        return Decimal("0")


def build_network(records: Iterable[TransactionRecord]) -> dict[str, object]:
    node_totals = defaultdict(lambda: {"sent_count": 0, "received_count": 0, "sent_total": Decimal("0"), "received_total": Decimal("0")})
    node_counterparties = defaultdict(set)
    node_sources = defaultdict(set)
    edge_map: dict[tuple[str, str], dict[str, object]] = {}

    for record in records:
        source = record.source or "unknown"
        target = record.target or "unknown"
        amount = record.amount
        key = (source, target)
        if source not in {"unknown", "external"}:
            node_totals[source]["sent_count"] += 1
            node_totals[source]["sent_total"] += amount
            node_counterparties[source].add(target)
        if target not in {"unknown", "external"}:
            node_totals[target]["received_count"] += 1
            node_totals[target]["received_total"] += amount
            node_sources[target].add(source)
        edge = edge_map.setdefault(
            key,
            {
                "source": source,
                "target": target,
                "count": 0,
                "total_amount": Decimal("0"),
                "first_seen": record.timestamp,
                "last_seen": record.timestamp,
            },
        )
        edge["count"] += 1
        edge["total_amount"] += amount
        if record.timestamp and (edge["first_seen"] is None or record.timestamp < edge["first_seen"]):
            edge["first_seen"] = record.timestamp
        if record.timestamp and (edge["last_seen"] is None or record.timestamp > edge["last_seen"]):
            edge["last_seen"] = record.timestamp

    nodes = []
    for account, totals in node_totals.items():
        sent = totals["sent_total"]
        received = totals["received_total"]
        nodes.append(
            {
                "account": account,
                "sent_count": totals["sent_count"],
                "received_count": totals["received_count"],
                "sent_total": sent,
                "received_total": received,
                "unique_targets": len(node_counterparties[account]),
                "unique_sources": len(node_sources[account]),
                "net_flow": received - sent,
            }
        )

    edges = list(edge_map.values())
    return {"nodes": nodes, "edges": edges}


def profile_nodes(records: Iterable[TransactionRecord]) -> list[NodeProfile]:
    grouped: defaultdict[str, list[TransactionRecord]] = defaultdict(list)
    for record in records:
        if record.source and record.source != "unknown":
            grouped[record.source].append(record)
        if record.target and record.target != "unknown":
            grouped[record.target].append(record)

    profiles: list[NodeProfile] = []
    for account, items in grouped.items():
        sent_records = [item for item in items if item.source == account]
        received_records = [item for item in items if item.target == account]
        amounts = [item.amount for item in items]
        active_days = len({day for day in (_active_day_key(item) for item in items) if day != "unknown"})
        night_count = sum(1 for item in items if _is_night(item))
        counterparties = {_counterparty(item) for item in items if _counterparty(item) not in {"unknown", "external"}}
        amount_counter = Counter(amounts)
        concentration = 0.0
        if amounts:
            top = max(amount_counter.values())
            concentration = round(top / len(amounts), 4)
        profiles.append(
            NodeProfile(
                account=account,
                sent_count=len(sent_records),
                received_count=len(received_records),
                sent_total=sum((item.amount for item in sent_records), Decimal("0")),
                received_total=sum((item.amount for item in received_records), Decimal("0")),
                unique_targets=len({_counterparty(item) for item in sent_records if _counterparty(item) not in {"unknown", "external"}}),
                unique_sources=len({_counterparty(item) for item in received_records if _counterparty(item) not in {"unknown", "external"}}),
                active_days=active_days,
                night_count=night_count,
                night_ratio=round(night_count / max(len(items), 1), 4),
                average_amount=sum(amounts, Decimal("0")) / max(len(amounts), 1),
                amount_std=_std_decimal(amounts),
                concentration=concentration,
            )
        )
    profiles.sort(key=lambda item: (-(item.sent_count + item.received_count), item.account))
    return profiles


def build_edge_profiles(records: Iterable[TransactionRecord]) -> list[EdgeProfile]:
    network = build_network(records)
    edges: list[EdgeProfile] = []
    for item in network["edges"]:
        edges.append(
            EdgeProfile(
                source=str(item["source"]),
                target=str(item["target"]),
                count=int(item["count"]),
                total_amount=Decimal(item["total_amount"]),
                first_seen=item["first_seen"],
                last_seen=item["last_seen"],
            )
        )
    edges.sort(key=lambda item: (-item.count, -item.total_amount, item.source, item.target))
    return edges


def detect_risks(records: list[TransactionRecord]) -> list[RiskFinding]:
    findings: list[RiskFinding] = []
    profiles = profile_nodes(records)
    by_account = {item.account: item for item in profiles}

    for profile in profiles:
        total_count = profile.sent_count + profile.received_count
        total_amount = profile.sent_total + profile.received_total
        if total_count >= 12 and profile.unique_targets >= 8 and profile.night_ratio >= 0.35:
            findings.append(
                RiskFinding(
                    rule_id="R-01",
                    severity="high",
                    subject=profile.account,
                    description="High activity count, broad counterparties, and elevated night ratio.",
                    evidence=[
                        f"count={total_count}",
                        f"unique_targets={profile.unique_targets}",
                        f"night_ratio={profile.night_ratio}",
                    ],
                    score=0.78,
                )
            )
        if total_count >= 8 and profile.concentration >= 0.35 and total_amount >= Decimal("5000"):
            findings.append(
                RiskFinding(
                    rule_id="R-02",
                    severity="medium",
                    subject=profile.account,
                    description="Repeated amount pattern suggests templated or clustered behavior.",
                    evidence=[f"concentration={profile.concentration}", f"total_amount={total_amount}"],
                    score=0.62,
                )
            )
        if profile.sent_count >= 5 and profile.received_count >= 5 and abs(profile.sent_total - profile.received_total) <= max(profile.received_total, profile.sent_total, Decimal("1")) * Decimal("0.25"):
            findings.append(
                RiskFinding(
                    rule_id="R-03",
                    severity="medium",
                    subject=profile.account,
                    description="Balanced in/out flow may indicate pass-through behavior and should be reviewed.",
                    evidence=[
                        f"sent_total={profile.sent_total}",
                        f"received_total={profile.received_total}",
                        f"sent_count={profile.sent_count}",
                        f"received_count={profile.received_count}",
                    ],
                    score=0.58,
                )
            )

    if len(records) >= 20:
        total_night = sum(1 for record in records if _is_night(record))
        if total_night / len(records) >= 0.4:
            findings.append(
                RiskFinding(
                    rule_id="R-04",
                    severity="medium",
                    subject="dataset",
                    description="Dataset has an elevated share of night activity.",
                    evidence=[f"night_ratio={round(total_night / len(records), 4)}"],
                    score=0.55,
                )
            )

    findings.sort(key=lambda item: (-item.score, item.subject, item.rule_id))
    return findings


def summarize_records(records: list[TransactionRecord]) -> AnalysisResult:
    network = build_network(records)
    nodes = profile_nodes(records)
    edges = build_edge_profiles(records)
    findings = detect_risks(records)
    summary = {
        "total_records": len(records),
        "total_accounts": len(nodes),
        "total_edges": len(edges),
        "night_records": sum(1 for record in records if _is_night(record)),
    }
    return AnalysisResult(
        total_records=len(records),
        nodes=nodes,
        edges=edges,
        findings=findings,
        summary=summary,
        network=network,
    )


def analyze_transactions_from_path(path: str | Path) -> AnalysisResult:
    records = load_transactions_from_path(path)
    return summarize_records(records)


def analyze_transactions(records: Iterable[TransactionRecord]) -> AnalysisResult:
    return summarize_records(list(records))

