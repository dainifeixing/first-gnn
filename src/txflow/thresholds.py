from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .report_io import write_json_file, write_markdown_lines


@dataclass(frozen=True)
class ThresholdSweepRow:
    threshold: float
    candidate_count: int
    reviewed_count: int
    confirmed_positive: int
    confirmed_negative: int
    uncertain: int
    resolution_rate: float
    confirmed_positive_rate: float

    def to_dict(self) -> dict[str, Any]:
        return {
            "threshold": round(self.threshold, 4),
            "candidate_count": self.candidate_count,
            "reviewed_count": self.reviewed_count,
            "confirmed_positive": self.confirmed_positive,
            "confirmed_negative": self.confirmed_negative,
            "uncertain": self.uncertain,
            "resolution_rate": round(self.resolution_rate, 4),
            "confirmed_positive_rate": round(self.confirmed_positive_rate, 4),
        }


@dataclass(frozen=True)
class ThresholdSweepReport:
    rows: list[ThresholdSweepRow]

    def to_dict(self) -> dict[str, Any]:
        return {"rows": [item.to_dict() for item in self.rows]}


@dataclass(frozen=True)
class ReviewWorkloadRow:
    threshold: float
    candidate_count: int
    estimated_reviewer_days: float
    estimated_team_days: float
    estimated_confirmed_positive: float
    estimated_confirmed_negative: float
    estimated_uncertain: float

    def to_dict(self) -> dict[str, Any]:
        return {
            "threshold": round(self.threshold, 4),
            "candidate_count": self.candidate_count,
            "estimated_reviewer_days": round(self.estimated_reviewer_days, 2),
            "estimated_team_days": round(self.estimated_team_days, 2),
            "estimated_confirmed_positive": round(self.estimated_confirmed_positive, 2),
            "estimated_confirmed_negative": round(self.estimated_confirmed_negative, 2),
            "estimated_uncertain": round(self.estimated_uncertain, 2),
        }


@dataclass(frozen=True)
class ReviewWorkloadForecast:
    rows: list[ReviewWorkloadRow]
    reviewers: int
    daily_capacity_per_reviewer: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "reviewers": self.reviewers,
            "daily_capacity_per_reviewer": self.daily_capacity_per_reviewer,
            "rows": [item.to_dict() for item in self.rows],
        }


@dataclass(frozen=True)
class OperatingThresholdRecommendation:
    recommended_threshold: float
    reason: str
    constraints: dict[str, Any]
    row: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "recommended_threshold": round(self.recommended_threshold, 4),
            "reason": self.reason,
            "constraints": dict(self.constraints),
            "row": dict(self.row),
        }


def _load_review_labels_by_record(review_csv_path: str | Path | None) -> dict[str, str]:
    labels: dict[str, str] = {}
    if not review_csv_path:
        return labels
    path = Path(review_csv_path)
    if not path.exists():
        return labels
    with path.open(encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            record_id = str(row.get("record_id", "")).strip()
            review_label = str(row.get("review_label", "")).strip().lower()
            if record_id:
                labels[record_id] = review_label
    return labels


def score_threshold_sweep(
    score_json_path: str | Path,
    review_csv_path: str | Path | None = None,
    thresholds: list[float] | None = None,
) -> ThresholdSweepReport:
    payload = json.loads(Path(score_json_path).read_text(encoding="utf-8"))
    top_rows = payload.get("top_rows", [])
    review_labels = _load_review_labels_by_record(review_csv_path)
    threshold_values = thresholds or [0.5, 0.6, 0.7, 0.75, 0.8, 0.85, 0.9]

    rows: list[ThresholdSweepRow] = []
    for threshold in threshold_values:
        candidate_rows = [item for item in top_rows if float(item.get("score", 0.0)) >= threshold]
        reviewed_count = confirmed_positive = confirmed_negative = uncertain = 0
        for item in candidate_rows:
            record_id = f"{item.get('workbook_path', '')}:{item.get('row_index', '')}"
            review_label = review_labels.get(record_id, "")
            if not review_label:
                continue
            reviewed_count += 1
            if review_label == "confirmed_positive":
                confirmed_positive += 1
            elif review_label == "confirmed_negative":
                confirmed_negative += 1
            elif review_label == "uncertain":
                uncertain += 1
        resolved = confirmed_positive + confirmed_negative
        rows.append(
            ThresholdSweepRow(
                threshold=threshold,
                candidate_count=len(candidate_rows),
                reviewed_count=reviewed_count,
                confirmed_positive=confirmed_positive,
                confirmed_negative=confirmed_negative,
                uncertain=uncertain,
                resolution_rate=(resolved / reviewed_count) if reviewed_count else 0.0,
                confirmed_positive_rate=(confirmed_positive / resolved) if resolved else 0.0,
            )
        )
    return ThresholdSweepReport(rows=rows)


def export_threshold_sweep_json(report: ThresholdSweepReport, output_path: str | Path) -> Path:
    return write_json_file(output_path, report.to_dict())


def export_threshold_sweep_markdown(report: ThresholdSweepReport, output_path: str | Path) -> Path:
    lines = ["# Threshold Sweep", ""]
    lines.append("| threshold | candidates | reviewed | confirmed_positive | confirmed_negative | uncertain | resolution_rate | confirmed_positive_rate |")
    lines.append("| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |")
    for item in report.rows:
        lines.append(
            f"| {item.threshold:.2f} | {item.candidate_count} | {item.reviewed_count} | {item.confirmed_positive} | {item.confirmed_negative} | {item.uncertain} | {item.resolution_rate:.4f} | {item.confirmed_positive_rate:.4f} |"
        )
    return write_markdown_lines(output_path, lines)


def review_workload_forecast(
    sweep_report: ThresholdSweepReport,
    reviewers: int = 1,
    daily_capacity_per_reviewer: int = 50,
) -> ReviewWorkloadForecast:
    safe_reviewers = max(int(reviewers), 1)
    safe_capacity = max(int(daily_capacity_per_reviewer), 1)
    rows: list[ReviewWorkloadRow] = []
    for item in sweep_report.rows:
        resolved_rate = item.resolution_rate
        positive_rate = item.confirmed_positive_rate
        uncertain_rate = 1.0 - resolved_rate
        estimated_resolved = item.candidate_count * resolved_rate
        estimated_positive = estimated_resolved * positive_rate
        estimated_negative = estimated_resolved - estimated_positive
        estimated_uncertain = item.candidate_count * uncertain_rate
        reviewer_days = item.candidate_count / safe_capacity
        team_days = item.candidate_count / (safe_reviewers * safe_capacity)
        rows.append(
            ReviewWorkloadRow(
                threshold=item.threshold,
                candidate_count=item.candidate_count,
                estimated_reviewer_days=reviewer_days,
                estimated_team_days=team_days,
                estimated_confirmed_positive=estimated_positive,
                estimated_confirmed_negative=estimated_negative,
                estimated_uncertain=estimated_uncertain,
            )
        )
    return ReviewWorkloadForecast(
        rows=rows,
        reviewers=safe_reviewers,
        daily_capacity_per_reviewer=safe_capacity,
    )


def export_review_workload_json(report: ReviewWorkloadForecast, output_path: str | Path) -> Path:
    return write_json_file(output_path, report.to_dict())


def export_review_workload_markdown(report: ReviewWorkloadForecast, output_path: str | Path) -> Path:
    lines = ["# Review Workload Forecast", ""]
    lines.append(f"- reviewers: {report.reviewers}")
    lines.append(f"- daily_capacity_per_reviewer: {report.daily_capacity_per_reviewer}")
    lines.append("")
    lines.append("| threshold | candidates | reviewer_days | team_days | est_confirmed_positive | est_confirmed_negative | est_uncertain |")
    lines.append("| ---: | ---: | ---: | ---: | ---: | ---: | ---: |")
    for item in report.rows:
        lines.append(
            f"| {item.threshold:.2f} | {item.candidate_count} | {item.estimated_reviewer_days:.2f} | {item.estimated_team_days:.2f} | {item.estimated_confirmed_positive:.2f} | {item.estimated_confirmed_negative:.2f} | {item.estimated_uncertain:.2f} |"
        )
    return write_markdown_lines(output_path, lines)


def select_operating_threshold(
    workload_report: ReviewWorkloadForecast,
    max_team_days: float | None = None,
    min_confirmed_positive_rate: float = 0.0,
    min_candidates: int = 1,
) -> OperatingThresholdRecommendation:
    eligible: list[tuple[ReviewWorkloadRow, float]] = []
    for item in workload_report.rows:
        resolved = item.estimated_confirmed_positive + item.estimated_confirmed_negative
        confirmed_positive_rate = (item.estimated_confirmed_positive / resolved) if resolved else 0.0
        if item.candidate_count < min_candidates:
            continue
        if max_team_days is not None and item.estimated_team_days > max_team_days:
            continue
        if confirmed_positive_rate < min_confirmed_positive_rate:
            continue
        eligible.append((item, confirmed_positive_rate))

    constraints = {
        "max_team_days": max_team_days,
        "min_confirmed_positive_rate": min_confirmed_positive_rate,
        "min_candidates": min_candidates,
    }

    if not eligible:
        fallback = min(workload_report.rows, key=lambda item: (item.estimated_team_days, -item.threshold))
        return OperatingThresholdRecommendation(
            recommended_threshold=fallback.threshold,
            reason="No threshold satisfied all constraints; selected the lightest workload option.",
            constraints=constraints,
            row=fallback.to_dict(),
        )

    best_item, best_rate = max(
        eligible,
        key=lambda pair: (pair[1], pair[0].candidate_count, pair[0].threshold),
    )
    return OperatingThresholdRecommendation(
        recommended_threshold=best_item.threshold,
        reason="Selected the highest-yield threshold within the workload constraints.",
        constraints=constraints,
        row={**best_item.to_dict(), "estimated_confirmed_positive_rate": round(best_rate, 4)},
    )


def export_operating_threshold_json(report: OperatingThresholdRecommendation, output_path: str | Path) -> Path:
    return write_json_file(output_path, report.to_dict())


def export_operating_threshold_markdown(report: OperatingThresholdRecommendation, output_path: str | Path) -> Path:
    lines = ["# Operating Threshold Recommendation", ""]
    lines.append(f"- recommended_threshold: {report.recommended_threshold:.4f}")
    lines.append(f"- reason: {report.reason}")
    lines.append("")
    lines.append("## Constraints")
    lines.append("")
    for key, value in report.constraints.items():
        lines.append(f"- {key}: {value}")
    lines.append("")
    lines.append("## Selected Row")
    lines.append("")
    for key, value in report.row.items():
        lines.append(f"- {key}: {value}")
    return write_markdown_lines(output_path, lines)
