from __future__ import annotations

import csv
import tempfile
import unittest
from pathlib import Path

from txflow.ingest import load_transactions_from_path


class IngestTests(unittest.TestCase):
    def test_common_csv_headers_are_normalized(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "sample.csv"
            with path.open("w", encoding="utf-8", newline="") as handle:
                writer = csv.writer(handle)
                writer.writerow(["交易时间", "金额", "付款方", "收款方", "方向", "备注"])
                writer.writerow(["2026-03-30 23:11:00", "120.50", "A账户", "B账户", "支出", "测试"])

            records = load_transactions_from_path(path)

        self.assertEqual(len(records), 1)
        self.assertEqual(records[0].source, "A账户")
        self.assertEqual(records[0].target, "B账户")
        self.assertEqual(str(records[0].amount), "120.50")
        self.assertEqual(records[0].remark, "测试")


if __name__ == "__main__":
    unittest.main()

