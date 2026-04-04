from __future__ import annotations

import csv
import tempfile
import unittest
from pathlib import Path

from txflow.excel import write_xlsx_table
from txflow.labels import annotate_transaction_ids, build_review_manifest, load_label_manifest, merge_label_manifests


PROJECT_ROOT = Path(__file__).resolve().parents[1]
LABEL_PATH = PROJECT_ROOT / "data" / "labels" / "wechat_yang_qianqian_verified.json"
ALT_LABEL_PATH = PROJECT_ROOT / "data" / "labels" / "wechat_yang_qianqian_wxid_4ry9iy64yip312_verified.json"
LI_SHAOYUN_POSITIVE_PATH = PROJECT_ROOT / "data" / "labels" / "lixiaoxiao168_li_shaoyun_verified_positive.json"
LI_SHAOYUN_NEGATIVE_PATH = PROJECT_ROOT / "data" / "labels" / "lixiaoxiao168_li_shaoyun_verified_negative.json"
A688_POSITIVE_PATH = PROJECT_ROOT / "data" / "labels" / "a68853039_lu_ping_zhi_verified_positive.json"
A688_NEGATIVE_PATH = PROJECT_ROOT / "data" / "labels" / "a68853039_lu_ping_zhi_verified_negative.json"


class LabelTests(unittest.TestCase):
    def test_verified_label_manifest_loads(self) -> None:
        manifest = load_label_manifest(LABEL_PATH)

        self.assertEqual(manifest.subject, "杨欠欠")
        self.assertEqual(manifest.label, "sex_work_related_income")
        self.assertGreater(len(manifest.transaction_ids), 40)

    def test_all_provided_transaction_ids_are_positive_labels(self) -> None:
        manifest = load_label_manifest(LABEL_PATH)
        annotations = annotate_transaction_ids(
            [
                "2026031264155009238164850111202",
                "2026012490235290035302710110405",
                "2025101230235026430803660210401",
            ],
            [manifest],
        )

        self.assertTrue(all(item["labels"] for item in annotations))
        self.assertTrue(all(label["label"] == "sex_work_related_income" for item in annotations for label in item["labels"]))

    def test_alternate_manifest_loads(self) -> None:
        manifest = load_label_manifest(ALT_LABEL_PATH)

        self.assertEqual(manifest.subject, "杨欠欠")
        self.assertEqual(manifest.source_file, "/home/doudougou/下载/3.29/wxid_4ry9iy64yip312.xlsx")
        self.assertIn("2026011028175005132814410101409", manifest.transaction_ids)

    def test_li_shaoyun_manifests_load_with_polarity(self) -> None:
        positive = load_label_manifest(LI_SHAOYUN_POSITIVE_PATH)
        negative = load_label_manifest(LI_SHAOYUN_NEGATIVE_PATH)

        self.assertEqual(positive.polarity, "positive")
        self.assertEqual(negative.polarity, "negative")
        self.assertEqual(positive.subject, "李少云")
        self.assertEqual(negative.subject, "李少云")

    def test_a688_manifests_load_with_polarity(self) -> None:
        positive = load_label_manifest(A688_POSITIVE_PATH)
        negative = load_label_manifest(A688_NEGATIVE_PATH)

        self.assertEqual(positive.subject, "陆平枝")
        self.assertEqual(negative.subject, "陆平枝")
        self.assertEqual(positive.polarity, "positive")
        self.assertEqual(negative.polarity, "negative")

    def test_build_review_manifest_filters_confirmed_rows(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            review_csv = Path(temp_dir) / "review.csv"
            with review_csv.open("w", encoding="utf-8", newline="") as handle:
                writer = csv.DictWriter(
                    handle,
                    fieldnames=[
                        "record_id",
                        "entity_type",
                        "pred_score",
                        "review_label",
                        "review_note",
                        "workbook_path",
                        "row_index",
                        "transaction_id",
                        "counterparty",
                        "remark",
                    ],
                )
                writer.writeheader()
                writer.writerow(
                    {
                        "record_id": "a:1",
                        "entity_type": "transaction",
                        "pred_score": "0.91",
                        "review_label": "confirmed_positive",
                        "review_note": "confirmed",
                        "workbook_path": "/tmp/a.xlsx",
                        "row_index": "1",
                        "transaction_id": "TX-1",
                        "counterparty": "A",
                        "remark": "r1",
                    }
                )
                writer.writerow(
                    {
                        "record_id": "a:2",
                        "entity_type": "transaction",
                        "pred_score": "0.20",
                        "review_label": "confirmed_negative",
                        "review_note": "confirmed",
                        "workbook_path": "/tmp/a.xlsx",
                        "row_index": "2",
                        "transaction_id": "TX-2",
                        "counterparty": "B",
                        "remark": "r2",
                    }
                )
                writer.writerow(
                    {
                        "record_id": "a:3",
                        "entity_type": "transaction",
                        "pred_score": "0.50",
                        "review_label": "uncertain",
                        "review_note": "skip",
                        "workbook_path": "/tmp/a.xlsx",
                        "row_index": "3",
                        "transaction_id": "TX-3",
                        "counterparty": "C",
                        "remark": "r3",
                    }
                )

            positive = build_review_manifest(review_csv, "positive", "round_x_positive", verified_by="analyst")
            negative = build_review_manifest(review_csv, "negative", "round_x_negative", verified_by="analyst")

            self.assertEqual(positive.label, "high_risk_transaction")
            self.assertEqual(negative.label, "low_risk_transaction")
            self.assertEqual(positive.transaction_ids, ["TX-1"])
            self.assertEqual(negative.transaction_ids, ["TX-2"])
            self.assertEqual(positive.verified_by, "analyst")

    def test_build_review_manifest_reads_red_fill_from_xlsx(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            review_xlsx = Path(temp_dir) / "review.xlsx"
            write_xlsx_table(
                review_xlsx,
                headers=[
                    "record_id",
                    "review_label",
                    "review_note",
                    "workbook_path",
                    "row_index",
                    "transaction_id",
                ],
                rows=[
                    {
                        "record_id": "a:1",
                        "review_label": "",
                        "review_note": "confirmed",
                        "workbook_path": "/tmp/a.xlsx",
                        "row_index": "1",
                        "transaction_id": "TX-1",
                    },
                    {
                        "record_id": "a:2",
                        "review_label": "",
                        "review_note": "high risk",
                        "workbook_path": "/tmp/a.xlsx",
                        "row_index": "2",
                        "transaction_id": "TX-2",
                    },
                ],
                row_fills=["red", "yellow"],
            )

            positive = build_review_manifest(review_xlsx, "positive", "round_x_positive", verified_by="analyst")
            negative = build_review_manifest(review_xlsx, "negative", "round_x_negative", verified_by="analyst")

            self.assertEqual(positive.transaction_ids, ["TX-1"])
            self.assertEqual(negative.transaction_ids, [])

    def test_merge_label_manifests_deduplicates_transaction_ids(self) -> None:
        positive_a = load_label_manifest(LI_SHAOYUN_POSITIVE_PATH)
        positive_b = load_label_manifest(LI_SHAOYUN_POSITIVE_PATH)
        negative = load_label_manifest(LI_SHAOYUN_NEGATIVE_PATH)

        merged_positive = merge_label_manifests(
            [positive_a, positive_b, negative],
            dataset_name="merged_positive",
            polarity="positive",
            verified_by="analyst",
            subject="merged_batch",
        )
        merged_negative = merge_label_manifests(
            [positive_a, negative],
            dataset_name="merged_negative",
            polarity="negative",
            verified_by="analyst",
            subject="merged_batch",
        )

        self.assertEqual(merged_positive.label, "high_risk_transaction")
        self.assertEqual(merged_negative.label, "low_risk_transaction")
        self.assertEqual(len(merged_positive.transaction_ids), len(set(positive_a.transaction_ids)))
        self.assertEqual(merged_negative.transaction_ids, sorted(dict.fromkeys(negative.transaction_ids)))


if __name__ == "__main__":
    unittest.main()
