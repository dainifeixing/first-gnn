from __future__ import annotations

import csv
import json
from collections import defaultdict
from datetime import datetime
from html import escape
from pathlib import Path
from statistics import mean
from typing import Any

from .excel import load_xlsx_styled_rows


def _load_json(path: str | Path | None) -> dict[str, Any]:
    if not path:
        return {}
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    return payload if isinstance(payload, dict) else {}


def _load_seller_review_states(path: str | Path | None) -> dict[str, dict[str, str]]:
    if not path:
        return {}
    resolved = Path(path)
    if not resolved.exists():
        return {}
    suffix = resolved.suffix.lower()
    if suffix == ".csv":
        return _load_seller_review_states_from_csv(resolved)
    if suffix == ".xlsx":
        return _load_seller_review_states_from_xlsx(resolved)
    return {}


def _normalize_review_label(value: Any, fill_label: str = "") -> str:
    normalized = str(value or "").strip().lower()
    if normalized in {"confirmed_positive", "confirmed_negative", "uncertain"}:
        return normalized
    if fill_label == "red":
        return "confirmed_positive"
    if fill_label == "green":
        return "confirmed_negative"
    if fill_label == "yellow":
        return "uncertain"
    return ""


def _load_seller_review_states_from_csv(path: Path) -> dict[str, dict[str, str]]:
    states: dict[str, dict[str, str]] = {}
    with path.open(encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            seller_account = str(row.get("seller_account", "")).strip()
            if not seller_account:
                continue
            review_label = _normalize_review_label(row.get("review_label", ""))
            review_note = str(row.get("review_note", "")).strip()
            if review_label or review_note:
                states[seller_account] = {"review_label": review_label, "review_note": review_note}
    return states


def _load_seller_review_states_from_xlsx(path: Path) -> dict[str, dict[str, str]]:
    states: dict[str, dict[str, str]] = {}
    for row in load_xlsx_styled_rows(path):
        seller_account = str(row.values.get("seller_account", "")).strip()
        if not seller_account:
            continue
        review_label = _normalize_review_label(row.values.get("review_label", ""), fill_label=row.fill_label)
        review_note = str(row.values.get("review_note", "")).strip()
        if review_label or review_note:
            states[seller_account] = {"review_label": review_label, "review_note": review_note}
    return states


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _short_path(path: str, keep: int = 2) -> str:
    parts = [part for part in str(path or "").replace("\\", "/").split("/") if part]
    if len(parts) <= keep:
        return "/".join(parts)
    return ".../" + "/".join(parts[-keep:])


def _trim_text(value: Any, limit: int = 80) -> str:
    text = " ".join(str(value or "").replace("\r", " ").replace("\n", " ").split())
    if len(text) <= limit:
        return text
    return text[: limit - 1].rstrip() + "..."


def _review_label_badge(review_label: str) -> str:
    normalized = str(review_label or "").strip().lower()
    if not normalized:
        return "<span class='review-badge review-empty'>-</span>"
    class_name = {
        "confirmed_positive": "review-positive",
        "confirmed_negative": "review-negative",
        "uncertain": "review-uncertain",
    }.get(normalized, "review-other")
    return f"<span class='review-badge {class_name}'>{escape(normalized)}</span>"


def _parse_timestamp(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    normalized = text.replace("/", "-").replace("T", " ")
    normalized = " ".join(normalized.split())
    compact = "".join(ch for ch in text if ch.isdigit())
    candidates = [
        normalized,
        normalized[:19],
        normalized[:16],
        normalized[:10],
    ]
    if compact:
        candidates.extend([compact, compact[:14], compact[:12], compact[:8]])
    formats = [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d",
        "%Y%m%d%H%M%S",
        "%Y%m%d%H%M",
        "%Y%m%d",
    ]
    for candidate in candidates:
        for fmt in formats:
            try:
                return datetime.strptime(candidate, fmt)
            except ValueError:
                continue
    return None


def _normalize_top_row(item: dict[str, Any]) -> dict[str, Any]:
    row = dict(item)
    row["score"] = _safe_float(row.get("score"))
    row["row_index"] = _safe_int(row.get("row_index"))
    row["bridge_buyer"] = bool(row.get("bridge_buyer"))
    row["known_seller_links"] = _safe_int(row.get("known_seller_links"))
    row["review_flags"] = [str(flag) for flag in row.get("review_flags", [])]
    return row


def _derive_seller_candidates(top_rows: list[dict[str, Any]], limit: int) -> list[dict[str, Any]]:
    known_sellers: set[str] = set()
    buyer_known_links: dict[str, set[str]] = defaultdict(set)
    for item in top_rows:
        seller = str(item.get("seller_account") or "").strip()
        if not seller:
            continue
        if item.get("label_status") == "positive" or item.get("extension_role") in {"seller_anchor", "buyer_to_known_seller"}:
            known_sellers.add(seller)
    for item in top_rows:
        buyer = str(item.get("buyer_account") or "").strip()
        seller = str(item.get("seller_account") or "").strip()
        if buyer and seller and seller in known_sellers:
            buyer_known_links[buyer].add(seller)

    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for item in top_rows:
        seller = str(item.get("seller_account") or "").strip()
        if not seller or seller in known_sellers:
            continue
        grouped[seller].append(item)

    candidates: list[dict[str, Any]] = []
    for seller, items in grouped.items():
        buyers = {str(item.get("buyer_account") or "").strip() for item in items if item.get("buyer_account")}
        bridge_buyers = {buyer for buyer in buyers if buyer_known_links.get(buyer)}
        avg_score = mean(_safe_float(item.get("score")) for item in items)
        max_score = max(_safe_float(item.get("score")) for item in items)
        bridge_ratio = (len(bridge_buyers) / len(buyers)) if buyers else 0.0
        candidate_score = max_score + 0.08 * len(bridge_buyers) + 0.04 * len(buyers) + 0.02 * bridge_ratio
        candidates.append(
            {
                "seller_account": seller,
                "score": round(candidate_score, 4),
                "avg_row_score": round(avg_score, 4),
                "support_rows": len(items),
                "unique_buyers": len(buyers),
                "bridge_buyers": len(bridge_buyers),
                "known_buyer_support": sum(len(buyer_known_links.get(buyer, set())) for buyer in bridge_buyers),
                "unique_workbooks": len({str(item.get("workbook_path") or "") for item in items if item.get("workbook_path")}),
                "sample_counterparties": sorted(
                    {
                        str(item.get("counterparty_name") or item.get("counterparty") or "").strip()
                        for item in items
                        if str(item.get("counterparty_name") or item.get("counterparty") or "").strip()
                    }
                )[:3],
                "sample_workbooks": sorted({_short_path(str(item.get("workbook_path") or "")) for item in items if item.get("workbook_path")})[:3],
            }
        )
    candidates.sort(
        key=lambda item: (
            -_safe_float(item.get("score")),
            -_safe_int(item.get("bridge_buyers")),
            -_safe_int(item.get("unique_buyers")),
            -_safe_int(item.get("support_rows")),
            str(item.get("seller_account") or ""),
        )
    )
    return candidates[:limit]


def _build_candidate_views(
    score_payload: dict[str, Any],
    review_states: dict[str, dict[str, str]],
    max_candidates: int,
    max_support_rows: int,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    top_rows = [_normalize_top_row(item) for item in score_payload.get("top_rows", []) if isinstance(item, dict)]
    seller_candidates = [
        dict(item)
        for item in score_payload.get("seller_candidates", [])
        if isinstance(item, dict) and str(item.get("seller_account") or "").strip()
    ]
    if not seller_candidates:
        seller_candidates = _derive_seller_candidates(top_rows, max_candidates)

    rows_by_seller: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in top_rows:
        seller = str(row.get("seller_account") or "").strip()
        if seller:
            rows_by_seller[seller].append(row)

    candidate_views: list[dict[str, Any]] = []
    for item in seller_candidates[:max_candidates]:
        seller = str(item.get("seller_account") or "").strip()
        support_examples_payload = item.get("support_examples", [])
        if isinstance(support_examples_payload, list) and support_examples_payload:
            support_rows = [_normalize_top_row(row) for row in support_examples_payload if isinstance(row, dict)][:max_support_rows]
        else:
            support_rows = sorted(
                rows_by_seller.get(seller, []),
                key=lambda row: (-_safe_float(row.get("score")), str(row.get("timestamp") or ""), _safe_int(row.get("row_index"))),
            )[:max_support_rows]
        buyer_groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in support_rows:
            buyer = str(row.get("buyer_account") or "").strip() or "(missing)"
            buyer_groups[buyer].append(row)
        buyer_support: list[dict[str, Any]] = []
        for buyer, buyer_rows in buyer_groups.items():
            buyer_support.append(
                {
                    "buyer_account": buyer,
                    "rows": len(buyer_rows),
                    "max_score": max(_safe_float(row.get("score")) for row in buyer_rows),
                    "bridge_buyer": any(bool(row.get("bridge_buyer")) for row in buyer_rows),
                    "known_seller_links": max(_safe_int(row.get("known_seller_links")) for row in buyer_rows),
                    "workbooks": sorted({_short_path(str(row.get("workbook_path") or "")) for row in buyer_rows if row.get("workbook_path")})[:3],
                    "sample_counterparties": sorted(
                        {
                            str(row.get("counterparty_name") or row.get("counterparty") or "").strip()
                            for row in buyer_rows
                            if str(row.get("counterparty_name") or row.get("counterparty") or "").strip()
                        }
                    )[:3],
                }
            )
        buyer_support.sort(key=lambda entry: (not entry["bridge_buyer"], -entry["max_score"], -entry["rows"], entry["buyer_account"]))
        candidate_views.append(
            {
                "seller_account": seller,
                "score": _safe_float(item.get("score")),
                "avg_row_score": _safe_float(item.get("avg_row_score")),
                "bridge_uplift": _safe_float(item.get("bridge_uplift")),
                "support_rows": _safe_int(item.get("support_rows")),
                "unique_buyers": _safe_int(item.get("unique_buyers")),
                "bridge_buyers": _safe_int(item.get("bridge_buyers")),
                "bridge_support_ratio": _safe_float(item.get("bridge_support_ratio")),
                "known_buyer_support": _safe_int(item.get("known_buyer_support")),
                "candidate_tier": str(item.get("candidate_tier") or ""),
                "unique_workbooks": _safe_int(item.get("unique_workbooks")),
                "sample_counterparties": [str(value) for value in item.get("sample_counterparties", []) if str(value).strip()],
                "sample_workbooks": [str(value) for value in item.get("sample_workbooks", []) if str(value).strip()],
                "review_label": str(review_states.get(seller, {}).get("review_label", "")).strip(),
                "review_note": str(review_states.get(seller, {}).get("review_note", "")).strip(),
                "buyer_support": buyer_support,
                "support_examples": support_rows,
            }
        )
    return candidate_views, top_rows


def _build_bridge_graph(candidate_views: list[dict[str, Any]], limit: int = 5) -> dict[str, Any]:
    selected = candidate_views[:limit]
    buyer_ids: list[str] = []
    seller_ids: list[str] = []
    edges: list[dict[str, Any]] = []
    seen_buyers: set[str] = set()
    for candidate in selected:
        seller = str(candidate.get("seller_account") or "")
        if not seller:
            continue
        seller_ids.append(seller)
        for buyer in candidate.get("buyer_support", [])[:5]:
            buyer_account = str(buyer.get("buyer_account") or "")
            if not buyer_account:
                continue
            if buyer_account not in seen_buyers:
                buyer_ids.append(buyer_account)
                seen_buyers.add(buyer_account)
            edges.append(
                {
                    "buyer": buyer_account,
                    "seller": seller,
                    "rows": _safe_int(buyer.get("rows")),
                    "max_score": _safe_float(buyer.get("max_score")),
                }
            )
    return {"buyers": buyer_ids, "sellers": seller_ids, "edges": edges}


def _render_cards(cards: list[dict[str, str]]) -> str:
    rendered: list[str] = []
    for item in cards:
        rendered.append(
            "<article class='metric-card'>"
            f"<div class='metric-label'>{escape(item['label'])}</div>"
            f"<div class='metric-value'>{escape(item['value'])}</div>"
            f"<div class='metric-note'>{escape(item['note'])}</div>"
            "</article>"
        )
    return "\n".join(rendered)


def _render_recommendations(score_payload: dict[str, Any], report_payload: dict[str, Any]) -> str:
    items = [str(item) for item in score_payload.get("recommendations", []) if str(item).strip()]
    items.extend(str(item) for item in report_payload.get("metrics", {}).get("recommendations", []) if str(item).strip())
    deduped: list[str] = []
    seen: set[str] = set()
    for item in items:
        if item not in seen:
            deduped.append(item)
            seen.add(item)
    if not deduped:
        return "<p class='muted'>No recommendations in the current artifacts.</p>"
    return "<ul class='bullet-list'>" + "".join(f"<li>{escape(item)}</li>" for item in deduped[:8]) + "</ul>"


def _render_seller_candidates(candidate_views: list[dict[str, Any]]) -> str:
    if not candidate_views:
        return "<p class='muted'>No seller-account candidates were generated in this round.</p>"
    rows: list[str] = []
    for index, item in enumerate(candidate_views, start=1):
        review_label = str(item.get("review_label") or "").strip() or "-"
        rows.append(
            f"<tr data-seller='{escape(item['seller_account'])}' data-review='{escape(str(item.get('review_label') or '').strip().lower())}' data-tier='{escape(str(item.get('candidate_tier') or '').strip().lower())}'>"
            f"<td>{index}</td>"
            f"<td><a href='#candidate-{index}'>{escape(item['seller_account'])}</a></td>"
            f"<td>{escape(item['candidate_tier'] or '-')}</td>"
            f"<td>{item['score']:.4f}</td>"
            f"<td>{item['bridge_uplift']:.4f}</td>"
            f"<td>{_review_label_badge(review_label if review_label != '-' else '')}</td>"
            f"<td>{item['bridge_buyers']}</td>"
            f"<td>{item['bridge_support_ratio']:.2f}</td>"
            f"<td>{item['known_buyer_support']}</td>"
            f"<td>{item['unique_buyers']}</td>"
            f"<td>{item['support_rows']}</td>"
            f"<td>{item['unique_workbooks']}</td>"
            f"<td>{escape(', '.join(item['sample_counterparties']) or '-')}</td>"
            "</tr>"
        )
    return (
        "<div class='toolbar'>"
        "<input id='seller-filter' class='filter-input' placeholder='Filter seller account or counterparty' />"
        "<div class='filter-chip-row'>"
        "<button type='button' class='filter-chip active' data-review-filter='all'>All</button>"
        "<button type='button' class='filter-chip' data-review-filter='confirmed_positive'>Positive</button>"
        "<button type='button' class='filter-chip' data-review-filter='confirmed_negative'>Negative</button>"
        "<button type='button' class='filter-chip' data-review-filter='uncertain'>Uncertain</button>"
        "<button type='button' class='filter-chip' data-review-filter='unreviewed'>Unreviewed</button>"
        "</div>"
        "<div class='filter-chip-row'>"
        "<button type='button' class='filter-chip active' data-tier-filter='all'>All Tiers</button>"
        "<button type='button' class='filter-chip' data-tier-filter='strong_bridge_unknown_seller'>Strong Bridge</button>"
        "<button type='button' class='filter-chip' data-tier-filter='weak_bridge_high_score'>Weak Bridge</button>"
        "<button type='button' class='filter-chip' data-tier-filter='high_support_non_bridge'>High Support</button>"
        "<button type='button' class='filter-chip' data-tier-filter='score_only'>Score Only</button>"
        "</div>"
        "</div>"
        "<div class='table-wrap'>"
        "<table id='seller-table'><thead><tr>"
        "<th>#</th><th>Seller Account</th><th>Tier</th><th>Score</th><th>Uplift</th><th>Review</th><th>Bridge Buyers</th><th>Bridge Ratio</th><th>Known Links</th><th>Unique Buyers</th><th>Rows</th><th>Workbooks</th><th>Samples</th>"
        "</tr></thead><tbody>"
        + "".join(rows)
        + "</tbody></table></div>"
    )


def _render_bridge_graph(graph: dict[str, Any]) -> str:
    buyers = graph.get("buyers", [])
    sellers = graph.get("sellers", [])
    edges = graph.get("edges", [])
    if not buyers or not sellers or not edges:
        return "<p class='muted'>Bridge graph needs seller candidates plus supporting buyer rows.</p>"

    width = 980
    left_x = 150
    right_x = 830
    top_pad = 70
    row_gap = 90
    height = max(420, top_pad + max(len(buyers), len(sellers)) * row_gap)
    buyer_pos = {buyer: (left_x, top_pad + index * row_gap) for index, buyer in enumerate(buyers)}
    seller_pos = {seller: (right_x, top_pad + index * row_gap) for index, seller in enumerate(sellers)}

    parts = [f"<svg viewBox='0 0 {width} {height}' class='bridge-graph' role='img' aria-label='buyer seller bridge graph'>"]
    for edge in edges:
        buyer = edge["buyer"]
        seller = edge["seller"]
        if buyer not in buyer_pos or seller not in seller_pos:
            continue
        x1, y1 = buyer_pos[buyer]
        x2, y2 = seller_pos[seller]
        stroke_width = 1.5 + min(5.0, edge["rows"] * 0.7)
        opacity = min(0.95, 0.35 + edge["max_score"] * 0.55)
        parts.append(
            f"<path data-buyer='{escape(buyer)}' data-seller='{escape(seller)}' d='M{x1 + 80},{y1} C {x1 + 250},{y1} {x2 - 250},{y2} {x2 - 80},{y2}' "
            f"stroke='rgba(224, 92, 52, {opacity:.3f})' stroke-width='{stroke_width:.2f}' fill='none' />"
        )
    for buyer, (x, y) in buyer_pos.items():
        parts.append(f"<circle cx='{x}' cy='{y}' r='34' class='node buyer-node' data-buyer='{escape(buyer)}' />")
        parts.append(f"<text x='{x}' y='{y - 4}' text-anchor='middle' class='node-label'>{escape(_trim_text(buyer, 16))}</text>")
        parts.append(f"<text x='{x}' y='{y + 16}' text-anchor='middle' class='node-sub'>buyer</text>")
    for seller, (x, y) in seller_pos.items():
        parts.append(f"<rect x='{x - 82}' y='{y - 34}' width='164' height='68' rx='18' class='node seller-node' data-seller='{escape(seller)}' />")
        parts.append(f"<text x='{x}' y='{y - 4}' text-anchor='middle' class='node-label'>{escape(_trim_text(seller, 18))}</text>")
        parts.append(f"<text x='{x}' y='{y + 16}' text-anchor='middle' class='node-sub'>candidate seller</text>")
    parts.append("</svg>")
    return "".join(parts)


def _render_candidate_details(candidate_views: list[dict[str, Any]]) -> str:
    if not candidate_views:
        return ""
    sections: list[str] = []
    for index, item in enumerate(candidate_views, start=1):
        buyer_rows = item["buyer_support"]
        buyer_table = "<p class='muted'>No supporting buyers inside current top rows.</p>"
        if buyer_rows:
            buyer_table = (
                "<div class='table-wrap'><table><thead><tr>"
                "<th>Buyer Account</th><th>Bridge</th><th>Known Links</th><th>Rows</th><th>Max Score</th><th>Workbooks</th><th>Counterparties</th>"
                "</tr></thead><tbody>"
                + "".join(
                    "<tr>"
                    f"<td>{escape(entry['buyer_account'])}</td>"
                    f"<td>{'yes' if entry['bridge_buyer'] else '-'}</td>"
                    f"<td>{entry['known_seller_links']}</td>"
                    f"<td>{entry['rows']}</td>"
                    f"<td>{entry['max_score']:.4f}</td>"
                    f"<td>{escape(', '.join(entry['workbooks']) or '-')}</td>"
                    f"<td>{escape(', '.join(entry['sample_counterparties']) or '-')}</td>"
                    "</tr>"
                    for entry in buyer_rows
                )
                + "</tbody></table></div>"
            )
        support_rows = item["support_examples"]
        support_table = "<p class='muted'>No supporting transaction rows in current score export.</p>"
        support_timeline = _render_support_timeline(support_rows)
        if support_rows:
            support_table = (
                "<div class='table-wrap'><table><thead><tr>"
                "<th>Score</th><th>Bridge</th><th>Workbook</th><th>Row</th><th>Buyer</th><th>Amount</th><th>Time</th><th>Counterparty</th><th>Remark</th>"
                "</tr></thead><tbody>"
                + "".join(
                    "<tr>"
                    f"<td>{_safe_float(row.get('score')):.4f}</td>"
                    f"<td>{'yes' if bool(row.get('bridge_buyer')) else '-'}</td>"
                    f"<td>{escape(_short_path(str(row.get('workbook_path') or '')))}</td>"
                    f"<td>{_safe_int(row.get('row_index'))}</td>"
                    f"<td>{escape(str(row.get('buyer_account') or '-'))}</td>"
                    f"<td>{escape(str(row.get('amount') or ''))}</td>"
                    f"<td>{escape(str(row.get('timestamp') or ''))}</td>"
                    f"<td>{escape(_trim_text(row.get('counterparty_name') or row.get('counterparty') or '', 24) or '-')}</td>"
                    f"<td>{escape(_trim_text(row.get('remark') or '', 40) or '-')}</td>"
                    "</tr>"
                    for row in support_rows
                )
                + "</tbody></table></div>"
            )
        chips = [
            f"<span class='chip'>tier {escape(item['candidate_tier'] or 'unknown')}</span>",
            f"<span class='chip'>score {item['score']:.4f}</span>",
            f"<span class='chip'>bridge uplift {item['bridge_uplift']:.4f}</span>",
            f"<span class='chip'>bridge buyers {item['bridge_buyers']}</span>",
            f"<span class='chip'>bridge ratio {item['bridge_support_ratio']:.2f}</span>",
            f"<span class='chip'>known links {item['known_buyer_support']}</span>",
            f"<span class='chip'>unique buyers {item['unique_buyers']}</span>",
            f"<span class='chip'>support rows {item['support_rows']}</span>",
            f"<span class='chip'>workbooks {item['unique_workbooks']}</span>",
        ]
        review_label = str(item.get("review_label") or "").strip()
        review_note = str(item.get("review_note") or "").strip()
        if review_label:
            chips.append(_review_label_badge(review_label))
        review_line = (
            f"<p class='muted'><strong>Review:</strong> {escape(review_label)} | <strong>Note:</strong> {escape(review_note or '-')}</p>"
            if review_label or review_note
            else ""
        )
        sections.append(
            f"<section class='candidate-section' id='candidate-{index}' data-seller='{escape(item['seller_account'])}' data-review='{escape(str(item.get('review_label') or '').strip().lower())}' data-tier='{escape(str(item.get('candidate_tier') or '').strip().lower())}'>"
            f"<h3>{index}. {escape(item['seller_account'])}</h3>"
            f"<div class='chip-row'>{''.join(chips)}</div>"
            f"<p class='muted'>Samples: {escape(', '.join(item['sample_counterparties']) or '-')} | Workbooks: {escape(', '.join(item['sample_workbooks']) or '-')}</p>"
            f"{review_line}"
            "<div><h4>Support Timeline</h4>"
            f"{support_timeline}</div>"
            "<div class='candidate-grid'>"
            "<div><h4>Buyer Support</h4>"
            f"{buyer_table}</div>"
            "<div><h4>Supporting Rows</h4>"
            f"{support_table}</div>"
            "</div></section>"
        )
    return "".join(sections)


def _render_support_timeline(support_rows: list[dict[str, Any]]) -> str:
    if not support_rows:
        return "<p class='muted'>No support timeline available.</p>"

    points: list[dict[str, Any]] = []
    for index, row in enumerate(support_rows):
        parsed = _parse_timestamp(row.get("timestamp"))
        points.append(
            {
                "index": index,
                "parsed": parsed,
                "timestamp": str(row.get("timestamp") or ""),
                "score": _safe_float(row.get("score")),
                "buyer": str(row.get("buyer_account") or "-"),
                "amount": str(row.get("amount") or ""),
                "counterparty": str(row.get("counterparty_name") or row.get("counterparty") or ""),
            }
        )
    points.sort(key=lambda item: (item["parsed"] is None, item["parsed"] or datetime.max, item["index"]))

    width = 760
    height = 160
    left = 34
    right = 22
    top = 24
    bottom = 38
    chart_width = width - left - right
    chart_height = height - top - bottom
    count = len(points)
    denom = max(1, count - 1)
    max_score = max(0.01, max(item["score"] for item in points))
    min_score = min(item["score"] for item in points)
    score_span = max(0.01, max_score - min_score)

    circles: list[str] = []
    labels: list[str] = []
    poly_points: list[str] = []
    for index, item in enumerate(points):
        x = left + (chart_width * index / denom if count > 1 else chart_width / 2)
        norm = (item["score"] - min_score) / score_span if score_span else 0.5
        y = top + chart_height - chart_height * norm
        poly_points.append(f"{x:.1f},{y:.1f}")
        radius = 4 + 4 * min(1.0, item["score"] / max_score)
        tooltip = " | ".join(
            part
            for part in [
                item["timestamp"] or "no time",
                f"score={item['score']:.4f}",
                f"buyer={item['buyer']}",
                f"amount={item['amount']}" if item["amount"] else "",
                f"counterparty={_trim_text(item['counterparty'], 24)}" if item["counterparty"] else "",
            ]
            if part
        )
        circles.append(
            f"<circle cx='{x:.1f}' cy='{y:.1f}' r='{radius:.1f}' class='timeline-point'>"
            f"<title>{escape(tooltip)}</title></circle>"
        )
        if index in {0, count - 1} or count <= 4:
            labels.append(
                f"<text x='{x:.1f}' y='{height - 12}' text-anchor='middle' class='timeline-label'>"
                f"{escape(_trim_text(item['timestamp'] or f'#{index + 1}', 14))}</text>"
            )
    avg_score = mean(item["score"] for item in points)
    avg_norm = (avg_score - min_score) / score_span if score_span else 0.5
    avg_y = top + chart_height - chart_height * avg_norm
    tick_lines = []
    for ratio in (0.0, 0.5, 1.0):
        y = top + chart_height - chart_height * ratio
        tick_score = min_score + score_span * ratio
        tick_lines.append(
            f"<line x1='{left}' y1='{y:.1f}' x2='{width - right}' y2='{y:.1f}' class='timeline-grid' />"
            f"<text x='{left - 8}' y='{y + 4:.1f}' text-anchor='end' class='timeline-axis'>{tick_score:.2f}</text>"
        )

    return (
        "<div class='timeline-wrap'>"
        f"<svg viewBox='0 0 {width} {height}' class='support-timeline' role='img' aria-label='support row timeline'>"
        + "".join(tick_lines)
        + f"<line x1='{left}' y1='{avg_y:.1f}' x2='{width - right}' y2='{avg_y:.1f}' class='timeline-average' />"
        + f"<polyline points='{' '.join(poly_points)}' class='timeline-line' />"
        + "".join(circles)
        + "".join(labels)
        + "</svg>"
        f"<div class='muted timeline-note'>support rows {count} | avg score {avg_score:.4f} | score range {min_score:.4f} - {max_score:.4f}</div>"
        "</div>"
    )


def _render_top_rows(top_rows: list[dict[str, Any]], limit: int) -> str:
    selected = top_rows[:limit]
    if not selected:
        return "<p class='muted'>No top-row output available.</p>"
    return (
        "<div class='table-wrap'><table><thead><tr>"
        "<th>Score</th><th>Workbook</th><th>Row</th><th>Status</th><th>Extension Role</th><th>Buyer</th><th>Seller</th><th>Counterparty</th><th>Remark</th>"
        "</tr></thead><tbody>"
        + "".join(
            "<tr>"
            f"<td>{_safe_float(row.get('score')):.4f}</td>"
            f"<td>{escape(_short_path(str(row.get('workbook_path') or '')))}</td>"
            f"<td>{_safe_int(row.get('row_index'))}</td>"
            f"<td>{escape(str(row.get('label_status') or 'unlabeled'))}</td>"
            f"<td>{escape(str(row.get('extension_role') or '-'))}</td>"
            f"<td>{escape(str(row.get('buyer_account') or '-'))}</td>"
            f"<td>{escape(str(row.get('seller_account') or '-'))}</td>"
            f"<td>{escape(_trim_text(row.get('counterparty_name') or row.get('counterparty') or '', 20) or '-')}</td>"
            f"<td>{escape(_trim_text(row.get('remark') or '', 36) or '-')}</td>"
            "</tr>"
            for row in selected
        )
        + "</tbody></table></div>"
    )


def _render_round_comparison(comparison_payload: dict[str, Any]) -> str:
    rounds = comparison_payload.get("rounds", [])
    if not rounds:
        return "<p class='muted'>No round-comparison file attached.</p>"
    return (
        "<div class='table-wrap'><table><thead><tr>"
        "<th>Round</th><th>Val F1</th><th>Val Loss</th><th>Positive Rate</th><th>Train Nodes</th><th>Val Nodes</th><th>Review Total</th><th>Confirmed +</th><th>Confirmed -</th>"
        "</tr></thead><tbody>"
        + "".join(
            "<tr>"
            f"<td>{escape(str(item.get('round_name') or ''))}</td>"
            f"<td>{_safe_float(item.get('best_val_f1')):.4f}</td>"
            f"<td>{_safe_float(item.get('best_val_loss')):.6f}</td>"
            f"<td>{_safe_float(item.get('positive_rate')):.4f}</td>"
            f"<td>{_safe_int(item.get('train_nodes'))}</td>"
            f"<td>{_safe_int(item.get('val_nodes'))}</td>"
            f"<td>{_safe_int(item.get('review_total'))}</td>"
            f"<td>{_safe_int(item.get('confirmed_positive'))}</td>"
            f"<td>{_safe_int(item.get('confirmed_negative'))}</td>"
            "</tr>"
            for item in rounds
        )
        + "</tbody></table></div>"
    )


def _render_extension_role_summary(frozen_eval_payload: dict[str, Any]) -> str:
    rows = frozen_eval_payload.get("extension_role_summary", [])
    if not rows:
        return "<p class='muted'>No extension-role breakdown in frozen eval.</p>"
    return (
        "<div class='table-wrap'><table><thead><tr>"
        "<th>Extension Role</th><th>Rows</th><th>Positive</th><th>Negative</th><th>Precision</th><th>Recall</th>"
        "</tr></thead><tbody>"
        + "".join(
            "<tr>"
            f"<td>{escape(str(item.get('extension_role') or ''))}</td>"
            f"<td>{_safe_int(item.get('rows'))}</td>"
            f"<td>{_safe_int(item.get('positive_rows'))}</td>"
            f"<td>{_safe_int(item.get('negative_rows'))}</td>"
            f"<td>{_safe_float(item.get('precision')):.4f}</td>"
            f"<td>{_safe_float(item.get('recall')):.4f}</td>"
            "</tr>"
            for item in rows
        )
        + "</tbody></table></div>"
    )


def build_round_visualization_html(
    score_json_path: str | Path,
    *,
    report_json_path: str | Path | None = None,
    frozen_eval_json_path: str | Path | None = None,
    comparison_json_path: str | Path | None = None,
    comparison_payload: dict[str, Any] | None = None,
    reviews_path: str | Path | None = None,
    title: str | None = None,
    max_candidates: int = 20,
    max_support_rows: int = 8,
    max_top_rows: int = 60,
) -> str:
    score_payload = _load_json(score_json_path)
    report_payload = _load_json(report_json_path)
    frozen_eval_payload = _load_json(frozen_eval_json_path)
    resolved_comparison_payload = dict(comparison_payload or {})
    if not resolved_comparison_payload:
        resolved_comparison_payload = _load_json(comparison_json_path)
    review_states = _load_seller_review_states(reviews_path)
    candidate_views, top_rows = _build_candidate_views(
        score_payload,
        review_states,
        max_candidates=max_candidates,
        max_support_rows=max_support_rows,
    )
    bridge_graph = _build_bridge_graph(candidate_views)

    current_title = title or str(report_payload.get("round_name") or Path(score_json_path).stem)
    score_summary = score_payload.get("summary", {})
    model_summary = score_payload.get("model", {})
    report_metrics = report_payload.get("metrics", {})
    frozen_metrics = frozen_eval_payload.get("metrics", {})
    seller_recovery = frozen_eval_payload.get("seller_candidate_recovery", {})
    review_counts = {
        "confirmed_positive": sum(1 for item in candidate_views if item.get("review_label") == "confirmed_positive"),
        "confirmed_negative": sum(1 for item in candidate_views if item.get("review_label") == "confirmed_negative"),
        "uncertain": sum(1 for item in candidate_views if item.get("review_label") == "uncertain"),
    }
    strong_bridge_candidates = sum(1 for item in candidate_views if item.get("candidate_tier") == "strong_bridge_unknown_seller")
    weak_bridge_candidates = sum(1 for item in candidate_views if item.get("candidate_tier") == "weak_bridge_high_score")
    bridge_backed_candidates = sum(1 for item in candidate_views if _safe_int(item.get("bridge_buyers")) > 0)
    bridge_candidate_rate = (bridge_backed_candidates / len(candidate_views)) if candidate_views else 0.0
    avg_bridge_buyers = mean(_safe_int(item.get("bridge_buyers")) for item in candidate_views) if candidate_views else 0.0
    avg_bridge_ratio = mean(_safe_float(item.get("bridge_support_ratio")) for item in candidate_views) if candidate_views else 0.0
    avg_bridge_uplift = mean(_safe_float(item.get("bridge_uplift")) for item in candidate_views) if candidate_views else 0.0
    cards = [
        {
            "label": "Seller Candidates",
            "value": str(len(candidate_views)),
            "note": f"top candidate {candidate_views[0]['seller_account']}" if candidate_views else "none",
        },
        {
            "label": "Bridge-Backed Sellers",
            "value": str(bridge_backed_candidates),
            "note": f"coverage {bridge_candidate_rate:.2%} | avg buyers {avg_bridge_buyers:.2f}",
        },
        {
            "label": "Bridge Ratio",
            "value": f"{avg_bridge_ratio:.4f}",
            "note": f"top bridge {str(score_summary.get('top_bridge_seller_candidate') or '-')}",
        },
        {
            "label": "Strong Bridge Tier",
            "value": str(strong_bridge_candidates),
            "note": f"weak tier {weak_bridge_candidates} | avg uplift {avg_bridge_uplift:.4f}",
        },
        {
            "label": "Top Row Score",
            "value": f"{_safe_float(score_summary.get('max_top_score')):.4f}",
            "note": f"avg {_safe_float(score_summary.get('avg_top_score')):.4f}",
        },
        {
            "label": "Best Val F1",
            "value": f"{_safe_float(report_metrics.get('best_val_f1', model_summary.get('best_val_f1'))):.4f}",
            "note": f"val nodes {_safe_int(report_metrics.get('val_nodes', model_summary.get('val_nodes')))}",
        },
        {
            "label": "Frozen Eval F1",
            "value": f"{_safe_float(frozen_metrics.get('f1')):.4f}",
            "note": f"rows {_safe_int(frozen_eval_payload.get('total_rows'))}",
        },
        {
            "label": "Seller Recovery",
            "value": f"{_safe_float(seller_recovery.get('recovery_rate')):.4f}",
            "note": f"matched {_safe_int(seller_recovery.get('recovered_positive_sellers'))}/{_safe_int(seller_recovery.get('holdout_positive_seller_count'))}",
        },
        {
            "label": "Total Rows",
            "value": str(_safe_int(score_payload.get('total_rows'))),
            "note": f"workbooks {_safe_int(score_payload.get('total_workbooks'))}",
        },
        {
            "label": "Reviewed Candidates",
            "value": str(sum(review_counts.values())),
            "note": f"+ {review_counts['confirmed_positive']} / - {review_counts['confirmed_negative']} / ? {review_counts['uncertain']}",
        },
    ]

    css = """
body {
  margin: 0;
  font-family: "Segoe UI", "PingFang SC", "Noto Sans SC", sans-serif;
  color: #1f2328;
  background:
    radial-gradient(circle at top left, rgba(248, 205, 145, 0.45), transparent 30%),
    radial-gradient(circle at top right, rgba(215, 233, 255, 0.65), transparent 35%),
    linear-gradient(180deg, #fcfbf7 0%, #f4f0e8 100%);
}
.page {
  max-width: 1320px;
  margin: 0 auto;
  padding: 32px 28px 80px;
}
.hero {
  display: grid;
  grid-template-columns: 1.4fr 1fr;
  gap: 24px;
  align-items: start;
  margin-bottom: 24px;
}
.hero-panel, .side-panel, .panel {
  background: rgba(255, 255, 255, 0.82);
  border: 1px solid rgba(108, 84, 45, 0.12);
  border-radius: 24px;
  box-shadow: 0 14px 40px rgba(86, 63, 33, 0.08);
  backdrop-filter: blur(8px);
}
.hero-panel {
  padding: 28px;
}
.hero-panel h1 {
  margin: 0 0 10px;
  font-size: 38px;
  line-height: 1.05;
  letter-spacing: -0.04em;
}
.hero-panel p {
  margin: 0;
  max-width: 74ch;
  color: #4e5561;
}
.side-panel {
  padding: 24px;
}
.section {
  margin-top: 24px;
}
.section h2 {
  margin: 0 0 14px;
  font-size: 22px;
  letter-spacing: -0.03em;
}
.metrics-grid {
  display: grid;
  grid-template-columns: repeat(3, minmax(0, 1fr));
  gap: 14px;
}
.metric-card {
  background: linear-gradient(180deg, rgba(255,255,255,0.95), rgba(248,243,233,0.95));
  border: 1px solid rgba(122, 95, 47, 0.12);
  border-radius: 18px;
  padding: 18px;
}
.metric-label {
  font-size: 12px;
  text-transform: uppercase;
  letter-spacing: 0.08em;
  color: #7c6340;
}
.metric-value {
  margin-top: 6px;
  font-size: 32px;
  font-weight: 700;
  letter-spacing: -0.04em;
}
.metric-note, .muted {
  color: #66707a;
}
.metric-note {
  margin-top: 8px;
  font-size: 13px;
}
.bullet-list {
  margin: 0;
  padding-left: 18px;
}
.bullet-list li + li {
  margin-top: 8px;
}
.split-grid {
  display: grid;
  grid-template-columns: 1.05fr 0.95fr;
  gap: 24px;
}
.panel {
  padding: 22px;
}
.table-wrap {
  overflow: auto;
  border-radius: 16px;
  border: 1px solid rgba(117, 95, 49, 0.1);
  background: rgba(255,255,255,0.76);
}
table {
  width: 100%;
  border-collapse: collapse;
  min-width: 720px;
}
th, td {
  padding: 11px 12px;
  border-bottom: 1px solid rgba(124, 100, 60, 0.1);
  text-align: left;
  vertical-align: top;
}
th {
  position: sticky;
  top: 0;
  background: #f6efe2;
  z-index: 1;
  font-size: 12px;
  letter-spacing: 0.06em;
  text-transform: uppercase;
  color: #765d37;
}
tr:hover td {
  background: rgba(240, 231, 212, 0.35);
}
.toolbar {
  margin-bottom: 12px;
}
.filter-input {
  width: 100%;
  max-width: 360px;
  padding: 12px 14px;
  border-radius: 999px;
  border: 1px solid rgba(117, 95, 49, 0.18);
  background: rgba(255,255,255,0.9);
  font: inherit;
}
.filter-chip-row {
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
  margin-top: 10px;
}
.filter-chip {
  border: 1px solid rgba(117, 95, 49, 0.18);
  background: rgba(255,255,255,0.86);
  color: #6a5231;
  border-radius: 999px;
  padding: 8px 12px;
  font: inherit;
  cursor: pointer;
}
.filter-chip.active {
  background: #a34b27;
  color: #fff8f1;
  border-color: #a34b27;
}
.candidate-section {
  margin-top: 20px;
  padding: 22px;
  border-radius: 22px;
  background: rgba(255,255,255,0.8);
  border: 1px solid rgba(117, 95, 49, 0.12);
}
.candidate-section h3 {
  margin: 0 0 10px;
  font-size: 24px;
  letter-spacing: -0.03em;
}
.candidate-grid {
  display: grid;
  grid-template-columns: 0.95fr 1.05fr;
  gap: 18px;
}
.chip-row {
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
  margin-bottom: 10px;
}
.chip {
  display: inline-flex;
  align-items: center;
  padding: 7px 10px;
  border-radius: 999px;
  background: rgba(221, 137, 67, 0.14);
  color: #85451f;
  font-size: 13px;
  font-weight: 600;
}
.review-badge {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  min-width: 92px;
  padding: 6px 10px;
  border-radius: 999px;
  font-size: 12px;
  font-weight: 700;
  letter-spacing: 0.02em;
}
.review-positive {
  background: rgba(197, 54, 48, 0.14);
  color: #9b1f1a;
}
.review-negative {
  background: rgba(35, 141, 86, 0.16);
  color: #13693d;
}
.review-uncertain {
  background: rgba(224, 168, 36, 0.2);
  color: #8b5c00;
}
.review-empty,
.review-other {
  background: rgba(116, 126, 140, 0.12);
  color: #5a6572;
}
.timeline-wrap {
  margin: 12px 0 18px;
  padding: 14px 14px 10px;
  border-radius: 18px;
  background: rgba(248, 243, 233, 0.72);
  border: 1px solid rgba(117, 95, 49, 0.1);
}
.support-timeline {
  width: 100%;
  min-height: 160px;
}
.timeline-grid {
  stroke: rgba(114, 93, 63, 0.18);
  stroke-dasharray: 4 4;
}
.timeline-axis,
.timeline-label {
  fill: #746853;
  font-size: 10px;
}
.timeline-line {
  fill: none;
  stroke: #c76637;
  stroke-width: 2.5;
}
.timeline-average {
  stroke: rgba(80, 112, 154, 0.52);
  stroke-width: 1.6;
}
.timeline-point {
  fill: #d14f24;
  stroke: rgba(255,255,255,0.82);
  stroke-width: 1.6;
}
.timeline-note {
  margin-top: 8px;
}
.bridge-graph {
  width: 100%;
  min-height: 420px;
}
.buyer-node {
  fill: #f3e3c2;
  stroke: #c48b3f;
  stroke-width: 2;
}
.seller-node {
  fill: #db6b48;
  stroke: #983d20;
  stroke-width: 2;
}
.node.active {
  filter: drop-shadow(0 0 10px rgba(168, 57, 19, 0.55));
}
.seller-node.active {
  fill: #b9411b;
}
.buyer-node.active {
  fill: #f2c46c;
}
.bridge-graph path.active {
  stroke: rgba(168, 57, 19, 0.82);
}
.bridge-graph path.dim,
.node.dim {
  opacity: 0.14;
}
.candidate-section.active,
#seller-table tbody tr.active td {
  outline: 2px solid rgba(170, 70, 35, 0.3);
  background: rgba(255, 240, 220, 0.56);
}
.node-label {
  fill: #1f2328;
  font-size: 12px;
  font-weight: 700;
}
.node-sub {
  fill: rgba(31, 35, 40, 0.68);
  font-size: 10px;
}
a {
  color: #8f3e21;
  text-decoration: none;
}
a:hover {
  text-decoration: underline;
}
@media (max-width: 1080px) {
  .hero, .split-grid, .candidate-grid, .metrics-grid {
    grid-template-columns: 1fr;
  }
  .page {
    padding: 18px 14px 56px;
  }
  .hero-panel h1 {
    font-size: 32px;
  }
}
"""
    js = """
const filterInput = document.getElementById('seller-filter');
const sellerTable = document.getElementById('seller-table');
const reviewFilterButtons = Array.from(document.querySelectorAll('.filter-chip[data-review-filter]'));
const tierFilterButtons = Array.from(document.querySelectorAll('.filter-chip[data-tier-filter]'));
let activeReviewFilter = 'all';
let activeTierFilter = 'all';

function rowMatchesReview(row) {
  const review = (row.dataset.review || '').trim().toLowerCase();
  if (activeReviewFilter === 'all') {
    return true;
  }
  if (activeReviewFilter === 'unreviewed') {
    return !review;
  }
  return review === activeReviewFilter;
}

function rowMatchesTier(row) {
  const tier = (row.dataset.tier || '').trim().toLowerCase();
  if (activeTierFilter === 'all') {
    return true;
  }
  return tier === activeTierFilter;
}

function applyTableFilters() {
  const keyword = filterInput ? filterInput.value.trim().toLowerCase() : '';
  const rows = sellerTable ? sellerTable.querySelectorAll('tbody tr') : [];
  rows.forEach((row) => {
    const text = row.innerText.toLowerCase();
    const textOk = !keyword || text.includes(keyword);
    const reviewOk = rowMatchesReview(row);
    const tierOk = rowMatchesTier(row);
    row.style.display = textOk && reviewOk && tierOk ? '' : 'none';
  });
  const sections = document.querySelectorAll('.candidate-section[data-seller]');
  sections.forEach((section) => {
    const text = section.innerText.toLowerCase();
    const textOk = !keyword || text.includes(keyword);
    const reviewOk = rowMatchesReview(section);
    const tierOk = rowMatchesTier(section);
    section.style.display = textOk && reviewOk && tierOk ? '' : 'none';
  });
}

if (filterInput && sellerTable) {
  filterInput.addEventListener('input', applyTableFilters);
}
reviewFilterButtons.forEach((button) => {
  button.addEventListener('click', () => {
    activeReviewFilter = button.dataset.reviewFilter || 'all';
    reviewFilterButtons.forEach((item) => item.classList.toggle('active', item === button));
    applyTableFilters();
  });
});
tierFilterButtons.forEach((button) => {
  button.addEventListener('click', () => {
    activeTierFilter = button.dataset.tierFilter || 'all';
    tierFilterButtons.forEach((item) => item.classList.toggle('active', item === button));
    applyTableFilters();
  });
});
const sellerRows = Array.from(document.querySelectorAll('#seller-table tbody tr[data-seller]'));
const sellerSections = Array.from(document.querySelectorAll('.candidate-section[data-seller]'));
const sellerNodes = Array.from(document.querySelectorAll('.seller-node[data-seller]'));
const buyerNodes = Array.from(document.querySelectorAll('.buyer-node[data-buyer]'));
const edgePaths = Array.from(document.querySelectorAll('.bridge-graph path[data-seller][data-buyer]'));
let activeSeller = '';

function applySellerFocus(seller) {
  activeSeller = seller === activeSeller ? '' : seller;
  sellerRows.forEach((row) => {
    row.classList.toggle('active', !!activeSeller && row.dataset.seller === activeSeller);
  });
  sellerSections.forEach((section) => {
    section.classList.toggle('active', !!activeSeller && section.dataset.seller === activeSeller);
  });
  const activeBuyers = new Set();
  edgePaths.forEach((edge) => {
    const isActive = !!activeSeller && edge.dataset.seller === activeSeller;
    edge.classList.toggle('active', isActive);
    edge.classList.toggle('dim', !!activeSeller && !isActive);
    if (isActive) {
      activeBuyers.add(edge.dataset.buyer);
    }
  });
  sellerNodes.forEach((node) => {
    const isActive = !!activeSeller && node.dataset.seller === activeSeller;
    node.classList.toggle('active', isActive);
    node.classList.toggle('dim', !!activeSeller && !isActive);
  });
  buyerNodes.forEach((node) => {
    const isActive = !!activeSeller && activeBuyers.has(node.dataset.buyer);
    node.classList.toggle('active', isActive);
    node.classList.toggle('dim', !!activeSeller && !isActive);
  });
}

sellerRows.forEach((row) => row.addEventListener('click', () => applySellerFocus(row.dataset.seller || '')));
sellerSections.forEach((section) => section.addEventListener('click', (event) => {
  if (event.target.closest('a')) {
    return;
  }
  applySellerFocus(section.dataset.seller || '');
}));
sellerNodes.forEach((node) => node.addEventListener('click', () => applySellerFocus(node.dataset.seller || '')));
applyTableFilters();
"""

    html = f"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>{escape(current_title)} - txflow-risk</title>
  <style>{css}</style>
</head>
<body>
  <main class="page">
    <section class="hero">
      <div class="hero-panel">
        <div class="muted">txflow-risk / minimal visualization</div>
        <h1>{escape(current_title)}</h1>
        <p>这版页面只解决三件事：先看账号级扩线候选，再看 buyer 到 seller 的桥接结构，最后看这一轮训练和冻结评估有没有往正确方向移动。</p>
      </div>
      <aside class="side-panel">
        <div><strong>Source</strong></div>
        <div class="muted">scores: {escape(_short_path(str(score_json_path), keep=3))}</div>
        <div class="muted">report: {escape(_short_path(str(report_json_path or '-'), keep=3))}</div>
        <div class="muted">frozen eval: {escape(_short_path(str(frozen_eval_json_path or '-'), keep=3))}</div>
        <div class="muted">comparison: {escape(_short_path(str(comparison_json_path or '-'), keep=3))}</div>
        <div class="muted">reviews: {escape(_short_path(str(reviews_path or '-'), keep=3))}</div>
        <div class="muted" style="margin-top:12px;">generated at {escape(datetime.now().isoformat(timespec='seconds'))}</div>
      </aside>
    </section>

    <section class="section">
      <div class="metrics-grid">
        {_render_cards(cards)}
      </div>
    </section>

    <section class="section split-grid">
      <div class="panel">
        <h2>Recommendations</h2>
        {_render_recommendations(score_payload, report_payload)}
      </div>
      <div class="panel">
        <h2>Round Comparison</h2>
        {_render_round_comparison(resolved_comparison_payload)}
      </div>
    </section>

    <section class="section">
      <div class="panel">
        <h2>Seller Candidates</h2>
        {_render_seller_candidates(candidate_views)}
      </div>
    </section>

    <section class="section split-grid">
      <div class="panel">
        <h2>Buyer to Seller Bridge Graph</h2>
        {_render_bridge_graph(bridge_graph)}
      </div>
      <div class="panel">
        <h2>Frozen Eval Breakdown</h2>
        {_render_extension_role_summary(frozen_eval_payload)}
      </div>
    </section>

    <section class="section">
      <div class="panel">
        <h2>Candidate Details</h2>
        {_render_candidate_details(candidate_views)}
      </div>
    </section>

    <section class="section">
      <div class="panel">
        <h2>Top Rows</h2>
        {_render_top_rows(top_rows, max_top_rows)}
      </div>
    </section>
  </main>
  <script>{js}</script>
</body>
</html>
"""
    return html


def export_round_visualization_html(
    output_path: str | Path,
    *,
    score_json_path: str | Path,
    report_json_path: str | Path | None = None,
    frozen_eval_json_path: str | Path | None = None,
    comparison_json_path: str | Path | None = None,
    comparison_payload: dict[str, Any] | None = None,
    reviews_path: str | Path | None = None,
    title: str | None = None,
    max_candidates: int = 20,
    max_support_rows: int = 8,
    max_top_rows: int = 60,
) -> Path:
    html = build_round_visualization_html(
        score_json_path,
        report_json_path=report_json_path,
        frozen_eval_json_path=frozen_eval_json_path,
        comparison_json_path=comparison_json_path,
        comparison_payload=comparison_payload,
        reviews_path=reviews_path,
        title=title,
        max_candidates=max_candidates,
        max_support_rows=max_support_rows,
        max_top_rows=max_top_rows,
    )
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(html, encoding="utf-8")
    return path
