from __future__ import annotations

import csv
import tempfile
import unittest
from pathlib import Path

from txflow.analysis import analyze_transactions_from_path


class AnalysisTests(unittest.TestCase):
    def test_night_activity_generates_a_finding(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "sample.csv"
            with path.open("w", encoding="utf-8", newline="") as handle:
                writer = csv.writer(handle)
                writer.writerow(["交易时间", "金额", "付款方", "收款方", "方向"])
                for i in range(12):
                    writer.writerow([f"2026-03-30 23:{i:02d}:00", "100", "A账户", f"收款{i}", "支出"])

            result = analyze_transactions_from_path(path)

        self.assertGreaterEqual(result.summary["night_records"], 12)
        self.assertTrue(result.findings)
        self.assertTrue(any(finding.rule_id == "R-01" for finding in result.findings))


if __name__ == "__main__":
    unittest.main()
