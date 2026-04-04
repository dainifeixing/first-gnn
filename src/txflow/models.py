from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from typing import Any


@dataclass(frozen=True)
class TransactionRecord:
    record_id: str
    timestamp: datetime | None
    amount: Decimal
    source: str
    target: str
    direction: str = "unknown"
    channel: str = ""
    remark: str = ""
    raw: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class NodeProfile:
    account: str
    sent_count: int
    received_count: int
    sent_total: Decimal
    received_total: Decimal
    unique_targets: int
    unique_sources: int
    active_days: int
    night_count: int
    night_ratio: float
    average_amount: Decimal
    amount_std: Decimal
    concentration: float


@dataclass(frozen=True)
class EdgeProfile:
    source: str
    target: str
    count: int
    total_amount: Decimal
    first_seen: datetime | None
    last_seen: datetime | None


@dataclass(frozen=True)
class RiskFinding:
    rule_id: str
    severity: str
    subject: str
    description: str
    evidence: list[str]
    score: float


@dataclass
class AnalysisResult:
    total_records: int
    nodes: list[NodeProfile]
    edges: list[EdgeProfile]
    findings: list[RiskFinding]
    summary: dict[str, Any]
    network: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "total_records": self.total_records,
            "nodes": [node.__dict__ for node in self.nodes],
            "edges": [
                {
                    **edge.__dict__,
                    "total_amount": str(edge.total_amount),
                    "first_seen": edge.first_seen.isoformat() if edge.first_seen else None,
                    "last_seen": edge.last_seen.isoformat() if edge.last_seen else None,
                }
                for edge in self.edges
            ],
            "findings": [
                {**finding.__dict__, "score": round(finding.score, 4)} for finding in self.findings
            ],
            "summary": self.summary,
            "network": self.network,
        }

