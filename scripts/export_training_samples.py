#!/usr/bin/env python3
from __future__ import annotations

import argparse

from txflow.labels import load_label_manifests
from txflow.training import build_positive_training_samples, export_training_samples_csv, export_training_samples_jsonl


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Export labeled training samples from transaction spreadsheets")
    parser.add_argument("--xlsx", required=True, help="Source xlsx file")
    parser.add_argument("--labels", nargs="+", required=True, help="Label manifest JSON files")
    parser.add_argument("--csv", help="Output CSV path")
    parser.add_argument("--jsonl", help="Output JSONL path")
    return parser


def run_from_args(args: argparse.Namespace) -> int:
    manifests = load_label_manifests(args.labels)
    samples = build_positive_training_samples(args.xlsx, manifests)
    if args.csv:
        export_training_samples_csv(samples, args.csv)
    if args.jsonl:
        export_training_samples_jsonl(samples, args.jsonl)
    print(f"exported {len(samples)} labeled samples")
    return 0


def main() -> int:
    args = build_parser().parse_args()
    return run_from_args(args)


if __name__ == "__main__":
    raise SystemExit(main())

