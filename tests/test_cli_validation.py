from __future__ import annotations

import io
import tempfile
import unittest
from contextlib import redirect_stderr
from pathlib import Path

from txflow.cli import main


class CliValidationTests(unittest.TestCase):
    def test_score_threshold_sweep_reports_missing_scores_file(self) -> None:
        stderr = io.StringIO()
        with redirect_stderr(stderr):
            rc = main(["score-threshold-sweep", "--scores", "/tmp/does-not-exist.json"])

        self.assertEqual(rc, 2)
        self.assertIn("error: scores file not found", stderr.getvalue())

    def test_train_gnn_rejects_invalid_split_ratio(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            stderr = io.StringIO()
            with redirect_stderr(stderr):
                rc = main(
                    [
                        "train-gnn",
                        "--root",
                        str(root),
                        "--labels",
                        str(root / "missing.json"),
                        "--model",
                        str(root / "model.pt"),
                        "--metrics",
                        str(root / "metrics.json"),
                        "--split-ratio",
                        "1.2",
                    ]
                )

        self.assertEqual(rc, 2)
        self.assertIn("error: split-ratio must be between 0 and 1", stderr.getvalue())

    def test_compare_round_metrics_rejects_invalid_round_spec(self) -> None:
        stderr = io.StringIO()
        with redirect_stderr(stderr):
            rc = main(["compare-round-metrics", "--round", "broken-spec"])

        self.assertEqual(rc, 2)
        self.assertIn("error: invalid round spec", stderr.getvalue())

    def test_train_gnn_requires_labels_or_annotations(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            stderr = io.StringIO()
            with redirect_stderr(stderr):
                rc = main(
                    [
                        "train-gnn",
                        "--root",
                        str(root),
                        "--model",
                        str(root / "model.pt"),
                        "--metrics",
                        str(root / "metrics.json"),
                    ]
                )

        self.assertEqual(rc, 2)
        self.assertIn("error: provide either --labels or --annotations", stderr.getvalue())

    def test_import_review_labels_requires_at_least_one_output(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            review_csv = Path(temp_dir) / "review.csv"
            review_csv.write_text("transaction_id,review_label\nTX-1,confirmed_positive\n", encoding="utf-8")
            stderr = io.StringIO()
            with redirect_stderr(stderr):
                rc = main(
                    [
                        "import-review-labels",
                        "--reviews",
                        str(review_csv),
                        "--dataset-prefix",
                        "round_01",
                    ]
                )

        self.assertEqual(rc, 2)
        self.assertIn("error: provide at least one output path for manifests or annotations", stderr.getvalue())

    def test_build_owner_summary_requires_at_least_one_output(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            stderr = io.StringIO()
            with redirect_stderr(stderr):
                rc = main(["build-owner-summary", "--root", str(root)])

        self.assertEqual(rc, 2)
        self.assertIn("error: provide at least one output path for --csv or --json", stderr.getvalue())

    def test_export_owner_review_requires_at_least_one_output(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            summary_json = Path(temp_dir) / "owner_summary.json"
            summary_json.write_text('{"total_rows":0,"covered_rows":0,"skipped_rows":0,"total_owners":0,"owners":[]}', encoding="utf-8")
            stderr = io.StringIO()
            with redirect_stderr(stderr):
                rc = main(["export-owner-review", "--summary", str(summary_json)])

        self.assertEqual(rc, 2)
        self.assertIn("error: provide at least one output path for --csv or --xlsx", stderr.getvalue())

    def test_import_owner_review_requires_at_least_one_output(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            review_csv = Path(temp_dir) / "owner_review.csv"
            review_csv.write_text("owner_id,review_role,review_confidence\nowner_001,broker,high\n", encoding="utf-8")
            stderr = io.StringIO()
            with redirect_stderr(stderr):
                rc = main(["import-owner-review", "--reviews", str(review_csv)])

        self.assertEqual(rc, 2)
        self.assertIn("error: provide at least one output path for --roles-csv or --roles-jsonl", stderr.getvalue())

    def test_export_mirror_review_requires_at_least_one_output(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            normalized_csv = Path(temp_dir) / "normalized.csv"
            normalized_csv.write_text("transaction_id\nTX-1\n", encoding="utf-8")
            stderr = io.StringIO()
            with redirect_stderr(stderr):
                rc = main(["export-mirror-review", "--normalized", str(normalized_csv)])

        self.assertEqual(rc, 2)
        self.assertIn("error: provide at least one output path for --csv or --xlsx", stderr.getvalue())

    def test_import_mirror_review_requires_at_least_one_output(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            review_csv = Path(temp_dir) / "mirror_review.csv"
            review_csv.write_text("mirror_group_id,transaction_id,review_decision\nm1,TX-1,confirmed_mirror\n", encoding="utf-8")
            stderr = io.StringIO()
            with redirect_stderr(stderr):
                rc = main(["import-mirror-review", "--reviews", str(review_csv)])

        self.assertEqual(rc, 2)
        self.assertIn("error: provide at least one output path for --csv or --jsonl", stderr.getvalue())

    def test_export_ledger_review_requires_at_least_one_output(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            normalized_csv = Path(temp_dir) / "normalized.csv"
            normalized_csv.write_text("transaction_id\nTX-1\n", encoding="utf-8")
            stderr = io.StringIO()
            with redirect_stderr(stderr):
                rc = main(["export-ledger-review", "--normalized", str(normalized_csv)])

        self.assertEqual(rc, 2)
        self.assertIn("error: provide at least one output path for --csv or --xlsx", stderr.getvalue())

    def test_export_rule_audit_requires_at_least_one_output(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            normalized_csv = Path(temp_dir) / "normalized.csv"
            normalized_csv.write_text("transaction_id\nTX-1\n", encoding="utf-8")
            stderr = io.StringIO()
            with redirect_stderr(stderr):
                rc = main(["export-rule-audit", "--normalized", str(normalized_csv)])

        self.assertEqual(rc, 2)
        self.assertIn("error: provide at least one output path for --csv or --xlsx", stderr.getvalue())

    def test_build_rule_summary_requires_at_least_one_output(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            normalized_csv = Path(temp_dir) / "normalized.csv"
            normalized_csv.write_text("transaction_id\nTX-1\n", encoding="utf-8")
            stderr = io.StringIO()
            with redirect_stderr(stderr):
                rc = main(["build-rule-summary", "--normalized", str(normalized_csv)])

        self.assertEqual(rc, 2)
        self.assertIn("error: provide at least one output path for --json or --md", stderr.getvalue())


if __name__ == "__main__":
    unittest.main()
