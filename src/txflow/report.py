from __future__ import annotations

import json
from decimal import Decimal

from .models import AnalysisResult


def _json_default(value):
    if isinstance(value, Decimal):
        return str(value)
    return str(value)


def render_json_report(result: AnalysisResult) -> str:
    return json.dumps(result.to_dict(), ensure_ascii=False, indent=2, default=_json_default)


def render_markdown_report(result: AnalysisResult) -> str:
    lines: list[str] = []
    lines.append("# Transaction Network Risk Report")
    lines.append("")
    lines.append(f"- Records: {result.total_records}")
    lines.append(f"- Accounts: {result.summary.get('total_accounts', 0)}")
    lines.append(f"- Edges: {result.summary.get('total_edges', 0)}")
    lines.append(f"- Night records: {result.summary.get('night_records', 0)}")
    lines.append("")

    lines.append("## Findings")
    lines.append("")
    if result.findings:
        for finding in result.findings[:12]:
            lines.append(
                f"- [{finding.severity.upper()}] {finding.rule_id} {finding.subject}: {finding.description}"
            )
            for item in finding.evidence[:4]:
                lines.append(f"  - {item}")
    else:
        lines.append("- No high-confidence findings.")
    lines.append("")

    lines.append("## Top Accounts")
    lines.append("")
    for node in result.nodes[:12]:
        lines.append(
            f"- {node.account}: sent {node.sent_count}, received {node.received_count}, night_ratio {node.night_ratio}"
        )
    lines.append("")

    lines.append("## Top Edges")
    lines.append("")
    for edge in result.edges[:12]:
        lines.append(
            f"- {edge.source} -> {edge.target}: {edge.count} tx, total {edge.total_amount}"
        )
    lines.append("")

    return "\n".join(lines).rstrip() + "\n"

