from __future__ import annotations

import csv
import json
import tempfile
import unittest
from pathlib import Path
from zipfile import ZipFile

from txflow.labels import LabelManifest
from txflow.ledger_ops import (
    build_duplicate_transaction_ids,
    build_review_flags,
    export_normalized_ledgers,
    normalize_workbook,
    summarize_graph_dataset,
    summarize_owner_activity,
)
from txflow.owners import OwnerLookup, load_owner_annotations
from txflow.roles import RoleLookup, load_role_annotations
from txflow.round_ops import build_round_report, load_review_stats
from txflow.thresholds import review_workload_forecast, score_threshold_sweep, select_operating_threshold
from txflow.training import TrainingExample


def _write_xlsx(path: Path, rows: list[dict[str, str]]) -> None:
    headers = ["交易流水号", "交易金额", "交易时间", "收款方的商户名称", "备注", "方向", "渠道"]
    row_xml = []
    for row_index, row in enumerate(rows, start=2):
        cells = []
        for col_index, header in enumerate(headers, start=1):
            ref = f"{chr(64 + col_index)}{row_index}"
            value = row.get(header, "")
            cells.append(f'<c r="{ref}" t="inlineStr"><is><t>{value}</t></is></c>')
        row_xml.append(f'<row r="{row_index}">{"".join(cells)}</row>')

    workbook_xml = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"
          xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">
  <sheets>
    <sheet name="Sheet1" sheetId="1" r:id="rId1"/>
  </sheets>
</workbook>
"""
    rels_xml = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/>
</Relationships>
"""
    header_cells = []
    for index, header in enumerate(headers, start=1):
        ref = f"{chr(64 + index)}1"
        header_cells.append(f'<c r="{ref}" t="inlineStr"><is><t>{header}</t></is></c>')
    sheet_rows = [f'<row r="1">{"".join(header_cells)}</row>'] + row_xml
    sheet_xml = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">
  <sheetData>
    {rows}
  </sheetData>
</worksheet>
""".format(rows="\n".join(sheet_rows))
    content_types = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">
  <Default Extension="xml" ContentType="application/xml"/>
  <Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>
  <Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>
</Types>
"""
    root_rels = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/>
</Relationships>
"""
    with ZipFile(path, "w") as archive:
        archive.writestr("[Content_Types].xml", content_types)
        archive.writestr("_rels/.rels", root_rels)
        archive.writestr("xl/workbook.xml", workbook_xml)
        archive.writestr("xl/_rels/workbook.xml.rels", rels_xml)
        archive.writestr("xl/worksheets/sheet1.xml", sheet_xml)


def _write_roles_csv(path: Path, rows: list[dict[str, str]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=["target_type", "target_id", "scene", "role_label", "confidence", "evidence", "note"],
        )
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _write_owners_csv(path: Path, rows: list[dict[str, str]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=["target_type", "target_id", "owner_id", "owner_name", "confidence", "evidence", "note"],
        )
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _write_mirror_annotations_csv(path: Path, rows: list[dict[str, str]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["mirror_group_id", "transaction_id", "decision", "confidence", "note"])
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


class ThresholdModuleTests(unittest.TestCase):
    def test_threshold_sweep_and_workload_forecast_use_review_labels(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            base = Path(temp_dir)
            score_json = base / "scores.json"
            review_csv = base / "reviews.csv"
            score_json.write_text(
                json.dumps(
                    {
                        "top_rows": [
                            {"workbook_path": "wb1.xlsx", "row_index": 1, "score": 0.95},
                            {"workbook_path": "wb1.xlsx", "row_index": 2, "score": 0.72},
                            {"workbook_path": "wb2.xlsx", "row_index": 1, "score": 0.61},
                        ]
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            with review_csv.open("w", encoding="utf-8", newline="") as handle:
                writer = csv.DictWriter(handle, fieldnames=["record_id", "review_label"])
                writer.writeheader()
                writer.writerow({"record_id": "wb1.xlsx:1", "review_label": "confirmed_positive"})
                writer.writerow({"record_id": "wb1.xlsx:2", "review_label": "confirmed_negative"})

            sweep = score_threshold_sweep(score_json, review_csv, thresholds=[0.7, 0.9])
            forecast = review_workload_forecast(sweep, reviewers=2, daily_capacity_per_reviewer=10)
            recommendation = select_operating_threshold(forecast, max_team_days=1.0, min_confirmed_positive_rate=0.4)

        self.assertEqual(len(sweep.rows), 2)
        self.assertEqual(sweep.rows[0].candidate_count, 2)
        self.assertEqual(sweep.rows[0].reviewed_count, 2)
        self.assertEqual(sweep.rows[0].confirmed_positive, 1)
        self.assertEqual(forecast.rows[0].estimated_team_days, 0.1)
        self.assertEqual(recommendation.recommended_threshold, 0.9)


class LedgerModuleTests(unittest.TestCase):
    def test_owner_lookup_prefers_transaction_and_falls_back_to_counterparty(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            owner_csv = Path(temp_dir) / "owners.csv"
            _write_owners_csv(
                owner_csv,
                [
                    {
                        "target_type": "counterparty",
                        "target_id": "夜间私单",
                        "owner_id": "owner_seller_01",
                        "owner_name": "张三",
                        "confidence": "high",
                        "evidence": "manual_review",
                        "note": "",
                    },
                    {
                        "target_type": "transaction",
                        "target_id": "TX-100",
                        "owner_id": "owner_broker_01",
                        "owner_name": "李四",
                        "confidence": "medium",
                        "evidence": "pattern",
                        "note": "",
                    },
                ],
            )
            lookup = OwnerLookup(load_owner_annotations(owner_csv))

        self.assertEqual(lookup.resolve("TX-100", "夜间私单").owner_id, "owner_broker_01")
        self.assertEqual(lookup.resolve("UNKNOWN", "夜间私单").owner_id, "owner_seller_01")

    def test_role_lookup_prefers_transaction_and_falls_back_to_counterparty(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            role_csv = Path(temp_dir) / "roles.csv"
            _write_roles_csv(
                role_csv,
                [
                    {
                        "target_type": "counterparty",
                        "target_id": "夜间私单",
                        "scene": "vice",
                        "role_label": "seller",
                        "confidence": "high",
                        "evidence": "manual_review",
                        "note": "",
                    },
                    {
                        "target_type": "transaction",
                        "target_id": "TX-100",
                        "scene": "vice",
                        "role_label": "broker",
                        "confidence": "medium",
                        "evidence": "pattern",
                        "note": "",
                    },
                ],
            )
            lookup = RoleLookup(load_role_annotations(role_csv))

        self.assertEqual(lookup.resolve("TX-100", "夜间私单").role_label, "broker")
        self.assertEqual(lookup.resolve("UNKNOWN", "夜间私单").role_label, "seller")

    def test_role_lookup_supports_owner_level_roles(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            role_csv = Path(temp_dir) / "roles.csv"
            _write_roles_csv(
                role_csv,
                [
                    {
                        "target_type": "owner",
                        "target_id": "owner_001",
                        "scene": "owner_review",
                        "role_label": "broker",
                        "confidence": "high",
                        "evidence": "owner_manual_review",
                        "note": "",
                    }
                ],
            )
            lookup = RoleLookup(load_role_annotations(role_csv))

        self.assertEqual(lookup.resolve("UNKNOWN", "未命中", "owner_001").role_label, "broker")

    def test_duplicate_ids_and_review_flags_are_detected(self) -> None:
        examples = [
            TrainingExample(1, "TX-1", "", "unlabeled", "", "", "12", "", 23, None, True, "", "", "", "", {}),
            TrainingExample(2, "TX-1", "", "unlabeled", "", "", "", "", None, None, False, "商户", "", "", "备注", {}),
        ]
        duplicate_ids = build_duplicate_transaction_ids(examples)
        flags = build_review_flags(examples[0], duplicate_ids)

        self.assertEqual(duplicate_ids, {"TX-1"})
        self.assertIn("duplicate_transaction_id", flags)
        self.assertIn("missing_timestamp", flags)
        self.assertIn("missing_counterparty", flags)
        self.assertIn("missing_remark", flags)
        self.assertIn("night_activity", flags)

    def test_export_normalized_ledgers_detects_mirrored_transactions_across_workbooks(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            _write_xlsx(
                root / "a.xlsx",
                [
                    {
                        "交易流水号": "TX-MIRROR-1",
                        "交易金额": "388.00",
                        "交易时间": "2026-03-12 23:11:00",
                        "收款方的商户名称": "B账户",
                        "备注": "A到账单",
                        "方向": "收入",
                        "渠道": "微信",
                    }
                ],
            )
            _write_xlsx(
                root / "b.xlsx",
                [
                    {
                        "交易流水号": "TX-MIRROR-1",
                        "交易金额": "388.00",
                        "交易时间": "2026-03-12 23:11:00",
                        "收款方的商户名称": "A账户",
                        "备注": "B到账单",
                        "方向": "支出",
                        "渠道": "微信",
                    }
                ],
            )

            rows = export_normalized_ledgers(root, [])
            summary = summarize_graph_dataset(root, [])

        self.assertEqual(len(rows), 2)
        self.assertTrue(all(item.mirror_match_count == 2 for item in rows))
        self.assertTrue(all(item.mirror_workbook_count == 2 for item in rows))
        self.assertTrue(all(item.mirror_has_opposite_direction for item in rows))
        self.assertTrue(all("mirrored_transaction" in item.review_flags for item in rows))
        self.assertTrue(all(item.mirror_review_decision == "confirmed_mirror" for item in rows))
        self.assertTrue(all(item.mirror_review_confidence == "high" for item in rows))
        self.assertEqual(summary.mirrored_rows, 2)
        self.assertEqual(summary.mirrored_groups, 1)
        self.assertEqual(summary.confirmed_mirror_rows, 2)

    def test_export_normalized_ledgers_detects_possible_mirror_candidates(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            _write_xlsx(
                root / "a.xlsx",
                [
                    {
                        "交易流水号": "TX-A-1",
                        "交易金额": "388.00",
                        "交易时间": "2026-03-12 23:11:00",
                        "收款方的商户名称": "B账户",
                        "备注": "A到账单",
                        "方向": "收入",
                        "渠道": "微信",
                    }
                ],
            )
            _write_xlsx(
                root / "b.xlsx",
                [
                    {
                        "交易流水号": "TX-B-1",
                        "交易金额": "388.00",
                        "交易时间": "2026-03-12 23:11:00",
                        "收款方的商户名称": "A账户",
                        "备注": "B到账单",
                        "方向": "支出",
                        "渠道": "微信",
                    }
                ],
            )

            rows = export_normalized_ledgers(root, [])
            summary = summarize_graph_dataset(root, [])

        self.assertEqual(len(rows), 2)
        self.assertTrue(all(item.mirror_match_count == 1 for item in rows))
        self.assertTrue(all(item.possible_mirror_match_count == 2 for item in rows))
        self.assertTrue(all(item.possible_mirror_confidence == "low" for item in rows))
        self.assertTrue(all(item.possible_mirror_score > 0.0 for item in rows))
        self.assertTrue(all("possible_mirror_transaction" in item.review_flags for item in rows))
        self.assertEqual(summary.mirrored_rows, 0)
        self.assertEqual(summary.possible_mirrored_rows, 2)
        self.assertEqual(summary.possible_mirrored_groups, 1)

    def test_mirror_annotations_are_applied_to_normalized_ledgers_and_summary(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            mirror_annotations_csv = root / "mirror_annotations.csv"
            _write_xlsx(
                root / "a.xlsx",
                [
                    {
                        "交易流水号": "TX-MIRROR-1",
                        "交易金额": "388.00",
                        "交易时间": "2026-03-12 23:11:00",
                        "收款方的商户名称": "B账户",
                        "备注": "A到账单",
                        "方向": "收入",
                        "渠道": "微信",
                    }
                ],
            )
            _write_xlsx(
                root / "b.xlsx",
                [
                    {
                        "交易流水号": "TX-MIRROR-1",
                        "交易金额": "388.00",
                        "交易时间": "2026-03-12 23:11:00",
                        "收款方的商户名称": "A账户",
                        "备注": "B到账单",
                        "方向": "支出",
                        "渠道": "微信",
                    }
                ],
            )
            _write_xlsx(
                root / "c.xlsx",
                [
                    {
                        "交易流水号": "TX-A-1",
                        "交易金额": "188.00",
                        "交易时间": "2026-03-13 10:00:00",
                        "收款方的商户名称": "D账户",
                        "备注": "候选镜像",
                        "方向": "收入",
                        "渠道": "支付宝",
                    }
                ],
            )
            _write_xlsx(
                root / "d.xlsx",
                [
                    {
                        "交易流水号": "TX-B-1",
                        "交易金额": "188.00",
                        "交易时间": "2026-03-13 10:00:00",
                        "收款方的商户名称": "C账户",
                        "备注": "候选镜像",
                        "方向": "支出",
                        "渠道": "支付宝",
                    }
                ],
            )
            _write_mirror_annotations_csv(
                mirror_annotations_csv,
                [
                    {
                        "mirror_group_id": "candidate:188.00|2026-03-13 10:00:00|支付宝",
                        "transaction_id": "TX-A-1",
                        "decision": "rejected_mirror",
                        "confidence": "low",
                        "note": "金额相同但并非同笔",
                    },
                ],
            )

            rows = export_normalized_ledgers(root, [], mirror_annotation_path=mirror_annotations_csv)
            summary = summarize_graph_dataset(root, [], mirror_annotation_path=mirror_annotations_csv)

        confirmed_rows = [item for item in rows if item.transaction_id == "TX-MIRROR-1"]
        rejected_rows = [item for item in rows if item.transaction_id in {"TX-A-1", "TX-B-1"}]
        self.assertEqual({item.mirror_review_decision for item in confirmed_rows}, {"confirmed_mirror"})
        self.assertTrue(all("mirror_review_confirmed" in item.review_flags for item in confirmed_rows))
        self.assertEqual({item.mirror_review_note for item in confirmed_rows}, {"matched_by_transaction_id"})
        self.assertEqual({item.mirror_review_decision for item in rejected_rows}, {"rejected_mirror"})
        self.assertTrue(all("mirror_review_rejected" in item.review_flags for item in rejected_rows))
        self.assertEqual(summary.confirmed_mirror_rows, 2)
        self.assertEqual(summary.rejected_mirror_rows, 2)
        self.assertEqual(summary.uncertain_mirror_rows, 0)

    def test_possible_mirror_strength_uses_owner_and_remark_signals(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            owner_csv = root / "owners.csv"
            _write_xlsx(
                root / "a.xlsx",
                [
                    {
                        "交易流水号": "TX-A-1",
                        "交易金额": "388.00",
                        "交易时间": "2026-03-12 23:11:00",
                        "收款方的商户名称": "B账户",
                        "备注": "包夜定金",
                        "方向": "收入",
                        "渠道": "微信",
                    }
                ],
            )
            _write_xlsx(
                root / "b.xlsx",
                [
                    {
                        "交易流水号": "TX-B-1",
                        "交易金额": "388.00",
                        "交易时间": "2026-03-12 23:11:00",
                        "收款方的商户名称": "A账户",
                        "备注": "包夜定金",
                        "方向": "支出",
                        "渠道": "微信",
                    }
                ],
            )
            _write_owners_csv(
                owner_csv,
                [
                    {
                        "target_type": "transaction",
                        "target_id": "TX-A-1",
                        "owner_id": "owner_a",
                        "owner_name": "甲",
                        "confidence": "high",
                        "evidence": "manual_review",
                        "note": "",
                    }
                ],
            )

            rows = export_normalized_ledgers(root, [], owner_annotation_path=owner_csv)

        self.assertTrue(all(item.possible_mirror_confidence == "high" for item in rows))
        self.assertTrue(all(item.possible_mirror_score >= 0.85 for item in rows))

    def test_normalize_and_summarize_graph_dataset(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            workbook = root / "sample.xlsx"
            role_csv = root / "roles.csv"
            owner_csv = root / "owners.csv"
            _write_xlsx(
                workbook,
                [
                    {
                        "交易流水号": "TX-100",
                        "交易金额": "388.00",
                        "交易时间": "2026-03-12 23:11:00",
                        "收款方的商户名称": "夜间私单",
                        "备注": "定金",
                        "方向": "收入",
                        "渠道": "微信",
                    },
                    {
                        "交易流水号": "TX-100",
                        "交易金额": "388.00",
                        "交易时间": "",
                        "收款方的商户名称": "",
                        "备注": "",
                        "方向": "收入",
                        "渠道": "微信",
                    },
                ],
            )
            _write_roles_csv(
                role_csv,
                [
                    {
                        "target_type": "transaction",
                        "target_id": "TX-100",
                        "scene": "vice",
                        "role_label": "seller",
                        "confidence": "high",
                        "evidence": "manual_review",
                        "note": "",
                    },
                    {
                        "target_type": "owner",
                        "target_id": "owner_001",
                        "scene": "vice",
                        "role_label": "broker",
                        "confidence": "high",
                        "evidence": "owner_manual_review",
                        "note": "主体已人工确认中间商",
                    },
                ],
            )
            _write_owners_csv(
                owner_csv,
                [
                    {
                        "target_type": "transaction",
                        "target_id": "TX-100",
                        "owner_id": "owner_001",
                        "owner_name": "张三",
                        "confidence": "high",
                        "evidence": "manual_review",
                        "note": "",
                    }
                ],
            )
            manifest = LabelManifest(
                dataset_name="demo_positive",
                label="high_risk_transaction",
                subject="demo",
                status="verified",
                source_file=str(workbook),
                transaction_ids=["TX-100"],
                polarity="positive",
            )

            rows = normalize_workbook(
                workbook,
                [manifest],
                role_annotation_path=role_csv,
                owner_annotation_path=owner_csv,
            )
            summary = summarize_graph_dataset(
                root,
                [manifest],
                role_annotation_path=role_csv,
                owner_annotation_path=owner_csv,
            )

        self.assertEqual(len(rows), 2)
        self.assertIn("duplicate_transaction_id", rows[0].review_flags)
        self.assertEqual(summary.total_workbooks, 1)
        self.assertEqual(summary.total_rows, 2)
        self.assertEqual(summary.positive_rows, 2)
        self.assertGreaterEqual(summary.flagged_rows, 2)
        self.assertEqual(rows[0].role_label, "seller")
        self.assertEqual(rows[0].role_confidence, "high")
        self.assertEqual(rows[0].owner_id, "owner_001")
        self.assertEqual(rows[0].owner_name, "张三")
        self.assertEqual(rows[0].owner_tx_count, 2)
        self.assertEqual(rows[0].owner_unique_counterparties, 1)
        self.assertTrue(rows[0].owner_inflow_ratio > 0.0)
        self.assertEqual(summary.role_counts["seller"], 2)
        self.assertEqual(summary.owner_counts["owner_001"], 2)

    def test_summarize_owner_activity_builds_owner_level_report(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            workbook = root / "sample.xlsx"
            role_csv = root / "roles.csv"
            owner_csv = root / "owners.csv"
            _write_xlsx(
                workbook,
                [
                    {
                        "交易流水号": "TX-100",
                        "交易金额": "388.00",
                        "交易时间": "2026-03-12 23:11:00",
                        "收款方的商户名称": "夜间私单",
                        "备注": "定金",
                        "方向": "收入",
                        "渠道": "微信",
                    },
                    {
                        "交易流水号": "TX-101",
                        "交易金额": "188.00",
                        "交易时间": "2026-03-13 00:11:00",
                        "收款方的商户名称": "夜间私单",
                        "备注": "加钟",
                        "方向": "支出",
                        "渠道": "支付宝",
                    },
                    {
                        "交易流水号": "TX-999",
                        "交易金额": "28.00",
                        "交易时间": "2026-03-13 08:11:00",
                        "收款方的商户名称": "早餐店",
                        "备注": "早餐",
                        "方向": "支出",
                        "渠道": "微信",
                    },
                ],
            )
            _write_roles_csv(
                role_csv,
                [
                    {
                        "target_type": "counterparty",
                        "target_id": "夜间私单",
                        "scene": "vice",
                        "role_label": "seller",
                        "confidence": "high",
                        "evidence": "manual_review",
                        "note": "",
                    },
                    {
                        "target_type": "owner",
                        "target_id": "owner_001",
                        "scene": "vice",
                        "role_label": "broker",
                        "confidence": "high",
                        "evidence": "owner_manual_review",
                        "note": "主体已人工确认中间商",
                    }
                ],
            )
            _write_owners_csv(
                owner_csv,
                [
                    {
                        "target_type": "transaction",
                        "target_id": "TX-100",
                        "owner_id": "owner_001",
                        "owner_name": "张三",
                        "confidence": "high",
                        "evidence": "manual_review",
                        "note": "",
                    },
                    {
                        "target_type": "transaction",
                        "target_id": "TX-101",
                        "owner_id": "owner_001",
                        "owner_name": "张三",
                        "confidence": "medium",
                        "evidence": "pattern",
                        "note": "",
                    },
                ],
            )
            manifest = LabelManifest(
                dataset_name="demo_positive",
                label="high_risk_transaction",
                subject="demo",
                status="verified",
                source_file=str(workbook),
                transaction_ids=["TX-100"],
                polarity="positive",
            )

            summary = summarize_owner_activity(
                root,
                [manifest],
                role_annotation_path=role_csv,
                owner_annotation_path=owner_csv,
            )

        self.assertEqual(summary.total_rows, 3)
        self.assertEqual(summary.covered_rows, 2)
        self.assertEqual(summary.skipped_rows, 1)
        self.assertEqual(summary.total_owners, 1)
        owner = summary.owners[0]
        self.assertEqual(owner.owner_id, "owner_001")
        self.assertEqual(owner.priority_rank, 1)
        self.assertGreater(owner.priority_score, 0.0)
        self.assertEqual(owner.owner_name, "张三")
        self.assertEqual(owner.owner_confidence, "high")
        self.assertEqual(owner.dominant_role, "broker")
        self.assertEqual(owner.reviewed_role, "broker")
        self.assertEqual(owner.reviewed_confidence, "high")
        self.assertEqual(owner.reviewed_note, "主体已人工确认中间商")
        self.assertEqual(owner.tx_count, 2)
        self.assertEqual(owner.unique_counterparties, 1)
        self.assertTrue(owner.collect_and_split)
        self.assertIn("collect_then_split", owner.pattern_tags)
        self.assertEqual(owner.top_counterparties[0]["counterparty"], "夜间私单")
        self.assertEqual(owner.top_counterparties[0]["tx_count"], 2)
        self.assertEqual(owner.channel_count, 2)
        self.assertEqual(owner.workbook_count, 1)
        self.assertEqual(owner.positive_rows, 1)


class RoundOpsModuleTests(unittest.TestCase):
    def test_load_review_stats_and_build_round_report(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            base = Path(temp_dir)
            metrics_json = base / "metrics.json"
            score_json = base / "scores.json"
            review_csv = base / "reviews.csv"
            label_json = base / "label.json"
            metrics_json.write_text(
                json.dumps({"best_val_f1": 0.88, "best_val_loss": 0.12, "positive_rate": 0.4, "train_nodes": 10, "val_nodes": 4}),
                encoding="utf-8",
            )
            score_json.write_text(
                json.dumps({"total_rows": 5, "top_rows": [{"score": 0.9}], "workbooks": [{"path": "wb1.xlsx"}]}),
                encoding="utf-8",
            )
            with review_csv.open("w", encoding="utf-8", newline="") as handle:
                writer = csv.DictWriter(handle, fieldnames=["review_label"])
                writer.writeheader()
                writer.writerow({"review_label": "confirmed_positive"})
                writer.writerow({"review_label": "uncertain"})
            label_json.write_text(
                json.dumps(
                    LabelManifest(
                        dataset_name="demo_positive",
                        label="high_risk_transaction",
                        subject="demo",
                        status="verified",
                        source_file="sample.xlsx",
                        transaction_ids=["TX-1", "TX-2"],
                        polarity="positive",
                    ).to_dict(),
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )

            stats = load_review_stats(review_csv)
            report = build_round_report("round_01", metrics_json, score_json, review_csv, [str(label_json)])

        self.assertEqual(stats["review_total"], 2)
        self.assertEqual(stats["confirmed_positive"], 1)
        self.assertEqual(report.round_name, "round_01")
        self.assertEqual(report.score_summary["top_rows"], 1)
        self.assertEqual(report.label_summary["positive_transaction_count"], 2)


if __name__ == "__main__":
    unittest.main()
