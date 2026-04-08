from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

from .labels import LabelManifest


@dataclass(frozen=True)
class LabelCatalogEntry:
    dataset_name: str
    label: str
    polarity: str
    subject: str
    source_file: str
    status: str
    transaction_count: int

    def to_dict(self) -> dict[str, object]:
        return {
            "dataset_name": self.dataset_name,
            "label": self.label,
            "polarity": self.polarity,
            "subject": self.subject,
            "source_file": self.source_file,
            "status": self.status,
            "transaction_count": self.transaction_count,
        }


def build_label_catalog(manifests: list[LabelManifest]) -> dict[str, object]:
    entries = [
        LabelCatalogEntry(
            dataset_name=manifest.dataset_name,
            label=manifest.label,
            polarity=manifest.polarity,
            subject=manifest.subject,
            source_file=manifest.source_file,
            status=manifest.status,
            transaction_count=len(manifest.transaction_ids),
        ).to_dict()
        for manifest in sorted(manifests, key=lambda item: (item.subject, item.source_file, item.dataset_name))
    ]
    label_counts: dict[str, int] = {}
    polarity_counts: dict[str, int] = {}
    subject_counts: dict[str, int] = {}
    for entry in entries:
        label = str(entry["label"])
        polarity = str(entry["polarity"])
        subject = str(entry["subject"])
        transaction_count = int(entry["transaction_count"])
        label_counts[label] = label_counts.get(label, 0) + transaction_count
        polarity_counts[polarity] = polarity_counts.get(polarity, 0) + transaction_count
        subject_counts[subject] = subject_counts.get(subject, 0) + transaction_count
    return {
        "manifest_count": len(entries),
        "entry_count": len(entries),
        "label_counts": label_counts,
        "polarity_counts": polarity_counts,
        "subject_counts": subject_counts,
        "entries": entries,
    }


def export_label_catalog_json(manifests: list[LabelManifest], output_path: str | Path) -> Path:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = build_label_catalog(manifests)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def export_label_catalog_markdown(manifests: list[LabelManifest], output_path: str | Path) -> Path:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = build_label_catalog(manifests)
    lines = ["# Label Catalog", ""]
    lines.append(f"- manifests: {payload['manifest_count']}")
    lines.append(f"- labels: {len(payload['label_counts'])}")
    lines.append(f"- subjects: {len(payload['subject_counts'])}")
    lines.append("")
    lines.append("## Entries")
    lines.append("")
    lines.append("| dataset_name | label | polarity | subject | source_file | transactions |")
    lines.append("| --- | --- | --- | --- | --- | ---: |")
    for entry in payload["entries"]:
        lines.append(
            f"| {entry['dataset_name']} | {entry['label']} | {entry['polarity']} | {entry['subject']} | {entry['source_file']} | {entry['transaction_count']} |"
        )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path
