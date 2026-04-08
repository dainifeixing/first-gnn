from __future__ import annotations

import csv
import json
import tempfile
import unittest
from pathlib import Path
from zipfile import ZipFile

from txflow.cli import main
from txflow.excel import load_xlsx_styled_rows, write_xlsx_table
from txflow.labels import load_label_manifest


PROJECT_ROOT = Path(__file__).resolve().parents[1]
LABEL_PATH = PROJECT_ROOT / "data" / "labels" / "wechat_yang_qianqian_verified.json"
LI_SHAOYUN_POSITIVE_PATH = PROJECT_ROOT / "data" / "labels" / "lixiaoxiao168_li_shaoyun_verified_positive.json"
LI_SHAOYUN_NEGATIVE_PATH = PROJECT_ROOT / "data" / "labels" / "lixiaoxiao168_li_shaoyun_verified_negative.json"


def _write_minimal_xlsx(path: Path) -> None:
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
    sheet_xml = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">
  <sheetData>
    <row r="1">
      <c r="A1" t="inlineStr"><is><t>交易流水号</t></is></c>
      <c r="B1" t="inlineStr"><is><t>交易金额</t></is></c>
    </row>
    <row r="2">
      <c r="A2" t="inlineStr"><is><t>2026031264155009238164850111202</t></is></c>
      <c r="B2" t="inlineStr"><is><t>388.00</t></is></c>
    </row>
  </sheetData>
</worksheet>
"""
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


def _write_xlsx_with_rows(path: Path, rows: list[dict[str, str]]) -> None:
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
    header = ["交易流水号", "交易金额", "交易时间", "收款方的商户名称", "备注", "方向", "渠道"]
    lines = [
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>',
        '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">',
        "  <sheetData>",
        '    <row r="1">',
    ]
    for col, name in zip(["A", "B", "C", "D", "E", "F", "G"], header):
        lines.append(f'      <c r="{col}1" t="inlineStr"><is><t>{name}</t></is></c>')
    lines.append("    </row>")
    for index, row in enumerate(rows, start=2):
        lines.append(f'    <row r="{index}">')
        for col, name in zip(["A", "B", "C", "D", "E", "F", "G"], header):
            value = row.get(name, "")
            lines.append(f'      <c r="{col}{index}" t="inlineStr"><is><t>{value}</t></is></c>')
        lines.append("    </row>")
    lines.extend(["  </sheetData>", "</worksheet>"])
    sheet_xml = "\n".join(lines)
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


def _write_annotations_csv(path: Path, rows: list[dict[str, str]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["transaction_id", "label", "note"])
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


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


class CliTests(unittest.TestCase):
    def test_markdown_output_is_generated(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "sample.csv"
            with path.open("w", encoding="utf-8", newline="") as handle:
                writer = csv.writer(handle)
                writer.writerow(["交易时间", "金额", "付款方", "收款方", "方向"])
                writer.writerow(["2026-03-30 23:11:00", "120.50", "A账户", "B账户", "支出"])
            rc = main(["analyze", str(path), "--format", "markdown", "--output", str(Path(temp_dir) / "report.md")])

        self.assertEqual(rc, 0)

    def test_export_training_command_writes_outputs(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            xlsx_path = Path(temp_dir) / "sample.xlsx"
            csv_path = Path(temp_dir) / "samples.csv"
            jsonl_path = Path(temp_dir) / "samples.jsonl"
            _write_minimal_xlsx(xlsx_path)

            rc = main(
                [
                    "export-training",
                    "--xlsx",
                    str(xlsx_path),
                    "--labels",
                    str(LABEL_PATH),
                    "--csv",
                    str(csv_path),
                    "--jsonl",
                    str(jsonl_path),
                ]
            )

            self.assertEqual(rc, 0)
            self.assertTrue(csv_path.exists())
            self.assertTrue(jsonl_path.exists())

    def test_export_dataset_command_writes_outputs(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            xlsx_path = Path(temp_dir) / "sample.xlsx"
            csv_path = Path(temp_dir) / "dataset.csv"
            jsonl_path = Path(temp_dir) / "dataset.jsonl"
            _write_minimal_xlsx(xlsx_path)

            rc = main(
                [
                    "export-dataset",
                    "--xlsx",
                    str(xlsx_path),
                    "--labels",
                    str(LABEL_PATH),
                    "--csv",
                    str(csv_path),
                    "--jsonl",
                    str(jsonl_path),
                ]
            )

            self.assertEqual(rc, 0)
            self.assertTrue(csv_path.exists())
            self.assertTrue(jsonl_path.exists())

    def test_label_catalog_command_writes_outputs(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            json_path = Path(temp_dir) / "catalog.json"
            md_path = Path(temp_dir) / "catalog.md"

            rc = main(
                [
                    "label-catalog",
                    "--labels",
                    str(LABEL_PATH),
                    "--json",
                    str(json_path),
                    "--md",
                    str(md_path),
                ]
            )

            self.assertEqual(rc, 0)
            self.assertTrue(json_path.exists())
            self.assertTrue(md_path.exists())

    def test_split_dataset_command_writes_outputs(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            xlsx_path = Path(temp_dir) / "sample.xlsx"
            train_csv = Path(temp_dir) / "train.csv"
            val_csv = Path(temp_dir) / "validation.csv"
            _write_minimal_xlsx(xlsx_path)

            rc = main(
                [
                    "split-dataset",
                    "--xlsx",
                    str(xlsx_path),
                    "--labels",
                    str(LABEL_PATH),
                    "--train-csv",
                    str(train_csv),
                    "--validation-csv",
                    str(val_csv),
                    "--ratio",
                    "0.5",
                ]
            )

            self.assertEqual(rc, 0)
            self.assertTrue(train_csv.exists())
            self.assertTrue(val_csv.exists())

    def test_train_baseline_command_writes_model_and_metrics(self) -> None:
        positive_manifest = load_label_manifest(LI_SHAOYUN_POSITIVE_PATH)
        negative_manifest = load_label_manifest(LI_SHAOYUN_NEGATIVE_PATH)
        with tempfile.TemporaryDirectory() as temp_dir:
            xlsx_path = Path(temp_dir) / "sample.xlsx"
            model_path = Path(temp_dir) / "model.json"
            metrics_path = Path(temp_dir) / "metrics.json"
            _write_xlsx_with_rows(
                xlsx_path,
                [
                    {
                        "交易流水号": positive_manifest.transaction_ids[0],
                        "交易金额": "388.00",
                        "交易时间": "2026-03-12 23:11:00",
                        "收款方的商户名称": "夜间私单",
                        "备注": "定金",
                    },
                    {
                        "交易流水号": positive_manifest.transaction_ids[1],
                        "交易金额": "280.00",
                        "交易时间": "2026-03-13 22:40:00",
                        "收款方的商户名称": "包夜订单",
                        "备注": "加钟",
                    },
                    {
                        "交易流水号": negative_manifest.transaction_ids[0],
                        "交易金额": "12.00",
                        "交易时间": "2026-03-12 11:11:00",
                        "收款方的商户名称": "超市消费",
                        "备注": "买菜",
                    },
                    {
                        "交易流水号": negative_manifest.transaction_ids[1],
                        "交易金额": "18.00",
                        "交易时间": "2026-03-13 09:40:00",
                        "收款方的商户名称": "便利店",
                        "备注": "早餐",
                    },
                ],
            )

            rc = main(
                [
                    "train-baseline",
                    "--xlsx",
                    str(xlsx_path),
                    "--labels",
                    str(LI_SHAOYUN_POSITIVE_PATH),
                    str(LI_SHAOYUN_NEGATIVE_PATH),
                    "--model",
                    str(model_path),
                    "--metrics",
                    str(metrics_path),
                    "--split-ratio",
                    "0.5",
                    "--seed",
                    "7",
                ]
            )

            self.assertEqual(rc, 0)
            self.assertTrue(model_path.exists())
            self.assertTrue(metrics_path.exists())
            metrics = json.loads(metrics_path.read_text(encoding="utf-8"))
            self.assertIn("evaluation", metrics)
            self.assertIn("accuracy", metrics["evaluation"])
            self.assertEqual(metrics["source"]["label_manifest_count"], 2)
            self.assertEqual(metrics["config"]["seed"], 7)
            self.assertEqual(metrics["dataset"]["total_rows"], 4)
            self.assertIn("recommendations", metrics)
            self.assertGreaterEqual(len(metrics["recommendations"]), 1)

    def test_graph_triage_command_writes_outputs(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "root"
            root.mkdir()
            xlsx_path = root / "sample.xlsx"
            json_path = Path(temp_dir) / "graph.json"
            md_path = Path(temp_dir) / "graph.md"
            _write_xlsx_with_rows(
                xlsx_path,
                [
                    {
                        "交易流水号": "2025102055205830178285410301304",
                        "交易金额": "388.00",
                        "交易时间": "2026-03-12 23:11:00",
                        "收款方的商户名称": "夜间私单",
                        "备注": "定金",
                    },
                    {
                        "交易流水号": "20260225712258976106235S0211308",
                        "交易金额": "12.00",
                        "交易时间": "2026-03-12 11:11:00",
                        "收款方的商户名称": "超市消费",
                        "备注": "买菜",
                    },
                    {
                        "交易流水号": "UNLABELED-ROW",
                        "交易金额": "420.00",
                        "交易时间": "2026-03-14 23:20:00",
                        "收款方的商户名称": "夜间约单",
                        "备注": "加钟",
                    },
                ],
            )

            rc = main(
                [
                    "graph-triage",
                    "--root",
                    str(root),
                    "--labels",
                    str(LI_SHAOYUN_POSITIVE_PATH),
                    str(LI_SHAOYUN_NEGATIVE_PATH),
                    "--json",
                    str(json_path),
                    "--md",
                    str(md_path),
                    "--top-k",
                    "5",
                ]
            )

            self.assertEqual(rc, 0)
            self.assertTrue(json_path.exists())
            self.assertTrue(md_path.exists())

    def test_normalize_train_score_and_export_review_commands(self) -> None:
        positive_manifest = load_label_manifest(LI_SHAOYUN_POSITIVE_PATH)
        negative_manifest = load_label_manifest(LI_SHAOYUN_NEGATIVE_PATH)
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "root"
            root.mkdir()
            xlsx_path = root / "sample.xlsx"
            normalized_csv = Path(temp_dir) / "normalized.csv"
            normalized_jsonl = Path(temp_dir) / "normalized.jsonl"
            dataset_json = Path(temp_dir) / "graph_dataset.json"
            model_path = Path(temp_dir) / "gnn_model.pt"
            metrics_path = Path(temp_dir) / "gnn_metrics.json"
            metadata_path = Path(temp_dir) / "gnn_metadata.json"
            score_json = Path(temp_dir) / "gnn_scores.json"
            score_md = Path(temp_dir) / "gnn_scores.md"
            review_csv = Path(temp_dir) / "review.csv"
            review_md = Path(temp_dir) / "review.md"
            review_xlsx = Path(temp_dir) / "review.xlsx"
            _write_xlsx_with_rows(
                xlsx_path,
                [
                    {
                        "交易流水号": positive_manifest.transaction_ids[0],
                        "交易金额": "388.00",
                        "交易时间": "2026-03-12 23:11:00",
                        "收款方的商户名称": "夜间私单",
                        "备注": "定金",
                    },
                    {
                        "交易流水号": negative_manifest.transaction_ids[0],
                        "交易金额": "12.00",
                        "交易时间": "2026-03-12 11:11:00",
                        "收款方的商户名称": "超市消费",
                        "备注": "买菜",
                    },
                    {
                        "交易流水号": "UNLABELED-ROW",
                        "交易金额": "388.00",
                        "交易时间": "2026-03-12 23:16:00",
                        "收款方的商户名称": "夜间私单",
                        "备注": "夜间定金",
                    },
                ],
            )

            rc = main(
                [
                    "normalize-ledgers",
                    "--root",
                    str(root),
                    "--labels",
                    str(LI_SHAOYUN_POSITIVE_PATH),
                    str(LI_SHAOYUN_NEGATIVE_PATH),
                    "--csv",
                    str(normalized_csv),
                    "--jsonl",
                    str(normalized_jsonl),
                ]
            )
            self.assertEqual(rc, 0)
            self.assertTrue(normalized_csv.exists())
            self.assertTrue(normalized_jsonl.exists())

            rc = main(
                [
                    "build-graph-dataset",
                    "--root",
                    str(root),
                    "--labels",
                    str(LI_SHAOYUN_POSITIVE_PATH),
                    str(LI_SHAOYUN_NEGATIVE_PATH),
                    "--json",
                    str(dataset_json),
                ]
            )
            self.assertEqual(rc, 0)
            summary = json.loads(dataset_json.read_text(encoding="utf-8"))
            self.assertEqual(summary["total_rows"], 3)

            rc = main(
                [
                    "train-gnn",
                    "--root",
                    str(root),
                    "--labels",
                    str(LI_SHAOYUN_POSITIVE_PATH),
                    str(LI_SHAOYUN_NEGATIVE_PATH),
                    "--model",
                    str(model_path),
                    "--metrics",
                    str(metrics_path),
                    "--metadata",
                    str(metadata_path),
                    "--epochs",
                    "5",
                    "--split-ratio",
                    "0.5",
                ]
            )
            self.assertEqual(rc, 0)
            self.assertTrue(model_path.exists())
            self.assertTrue(metrics_path.exists())
            self.assertTrue(metadata_path.exists())
            gnn_metrics = json.loads(metrics_path.read_text(encoding="utf-8"))
            self.assertIn("recommendations", gnn_metrics)
            self.assertGreaterEqual(len(gnn_metrics["recommendations"]), 1)

            rc = main(
                [
                    "score-gnn",
                    "--root",
                    str(root),
                    "--model",
                    str(model_path),
                    "--labels",
                    str(LI_SHAOYUN_POSITIVE_PATH),
                    str(LI_SHAOYUN_NEGATIVE_PATH),
                    "--json",
                    str(score_json),
                    "--md",
                    str(score_md),
                    "--top-k",
                    "5",
                    "--include-labeled",
                ]
            )
            self.assertEqual(rc, 0)
            self.assertTrue(score_json.exists())
            self.assertTrue(score_md.exists())
            score_payload = json.loads(score_json.read_text(encoding="utf-8"))
            self.assertIn("summary", score_payload)
            self.assertEqual(score_payload["summary"]["top_k"], 5)
            self.assertEqual(score_payload["summary"]["returned_top_rows"], len(score_payload["top_rows"]))
            self.assertIn("recommendations", score_payload)
            self.assertGreaterEqual(len(score_payload["recommendations"]), 1)

            rc = main(
                [
                    "export-review-candidates",
                    "--scores",
                    str(score_json),
                    "--csv",
                    str(review_csv),
                    "--xlsx",
                    str(review_xlsx),
                    "--md",
                    str(review_md),
                    "--threshold",
                    "0.0",
                    "--limit",
                    "10",
                ]
            )
            self.assertEqual(rc, 0)
            self.assertTrue(review_csv.exists())
            self.assertTrue(review_xlsx.exists())
            self.assertTrue(review_md.exists())
            with review_csv.open(encoding="utf-8", newline="") as handle:
                rows = list(csv.DictReader(handle))
            self.assertGreaterEqual(len(rows), 1)
            self.assertIn("review_note", rows[0])
            self.assertEqual(rows[0]["review_label"], "")
            self.assertEqual(rows[0]["review_options"], "confirmed_positive|confirmed_negative|uncertain")
            self.assertNotIn("workbook_path", rows[0])
            self.assertNotIn("row_index", rows[0])
            self.assertIn("amount", rows[0])
            self.assertIn("timestamp", rows[0])
            styled_rows = load_xlsx_styled_rows(review_xlsx)
            self.assertGreaterEqual(len(styled_rows), 1)
            self.assertEqual(styled_rows[0].fill_label, "yellow")

    def test_train_and_score_gnn_accept_simplified_annotations(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "root"
            root.mkdir()
            xlsx_path = root / "sample.xlsx"
            annotations_path = Path(temp_dir) / "annotations.csv"
            model_path = Path(temp_dir) / "gnn_model.pt"
            metrics_path = Path(temp_dir) / "gnn_metrics.json"
            score_json = Path(temp_dir) / "gnn_scores.json"
            _write_xlsx_with_rows(
                xlsx_path,
                [
                    {
                        "交易流水号": "POS-1",
                        "交易金额": "388.00",
                        "交易时间": "2026-03-12 23:11:00",
                        "收款方的商户名称": "夜间私单",
                        "备注": "定金",
                    },
                    {
                        "交易流水号": "NEG-1",
                        "交易金额": "12.00",
                        "交易时间": "2026-03-12 11:11:00",
                        "收款方的商户名称": "超市消费",
                        "备注": "买菜",
                    },
                    {
                        "交易流水号": "UNLABELED-ROW",
                        "交易金额": "420.00",
                        "交易时间": "2026-03-14 23:20:00",
                        "收款方的商户名称": "夜间约单",
                        "备注": "加钟",
                    },
                ],
            )
            _write_annotations_csv(
                annotations_path,
                [
                    {"transaction_id": "POS-1", "label": "positive", "note": "confirmed"},
                    {"transaction_id": "NEG-1", "label": "negative", "note": "normal"},
                    {"transaction_id": "SKIP-1", "label": "skip", "note": "ignored"},
                ],
            )

            rc = main(
                [
                    "train-gnn",
                    "--root",
                    str(root),
                    "--annotations",
                    str(annotations_path),
                    "--model",
                    str(model_path),
                    "--metrics",
                    str(metrics_path),
                    "--epochs",
                    "5",
                    "--split-ratio",
                    "0.5",
                ]
            )
            self.assertEqual(rc, 0)
            metrics = json.loads(metrics_path.read_text(encoding="utf-8"))
            self.assertIn("recommendations", metrics)
            self.assertGreaterEqual(len(metrics["recommendations"]), 1)

            rc = main(
                [
                    "score-gnn",
                    "--root",
                    str(root),
                    "--model",
                    str(model_path),
                    "--annotations",
                    str(annotations_path),
                    "--json",
                    str(score_json),
                    "--top-k",
                    "5",
                    "--include-labeled",
                ]
            )
            self.assertEqual(rc, 0)
            score_payload = json.loads(score_json.read_text(encoding="utf-8"))
            self.assertEqual(score_payload["labeled_rows"], 2)
            self.assertIn("recommendations", score_payload)

    def test_train_gnn_accepts_highlighted_xlsx_annotations(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "root"
            root.mkdir()
            xlsx_path = root / "sample.xlsx"
            highlighted_xlsx = Path(temp_dir) / "highlighted.xlsx"
            model_path = Path(temp_dir) / "gnn_model.pt"
            metrics_path = Path(temp_dir) / "gnn_metrics.json"
            _write_xlsx_with_rows(
                xlsx_path,
                [
                    {
                        "交易流水号": "POS-1",
                        "交易金额": "388.00",
                        "交易时间": "2026-03-12 23:11:00",
                        "收款方的商户名称": "夜间私单",
                        "备注": "定金",
                    },
                    {
                        "交易流水号": "YELLOW-1",
                        "交易金额": "420.00",
                        "交易时间": "2026-03-14 23:20:00",
                        "收款方的商户名称": "夜间约单",
                        "备注": "加钟",
                    },
                ],
            )
            write_xlsx_table(
                highlighted_xlsx,
                headers=["交易流水号", "交易金额", "交易时间", "收款方的商户名称", "备注"],
                rows=[
                    {
                        "交易流水号": "POS-1",
                        "交易金额": "388.00",
                        "交易时间": "2026-03-12 23:11:00",
                        "收款方的商户名称": "夜间私单",
                        "备注": "定金",
                    },
                    {
                        "交易流水号": "YELLOW-1",
                        "交易金额": "420.00",
                        "交易时间": "2026-03-14 23:20:00",
                        "收款方的商户名称": "夜间约单",
                        "备注": "加钟",
                    },
                ],
                row_fills=["red", "yellow"],
            )

            rc = main(
                [
                    "train-gnn",
                    "--root",
                    str(root),
                    "--annotations",
                    str(highlighted_xlsx),
                    "--model",
                    str(model_path),
                    "--metrics",
                    str(metrics_path),
                    "--epochs",
                    "5",
                    "--split-ratio",
                    "0.5",
                ]
            )

            self.assertEqual(rc, 0)
            metrics = json.loads(metrics_path.read_text(encoding="utf-8"))
            self.assertIn("recommendations", metrics)

    def test_export_rule_audit_outputs_rule_focused_fields(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "root"
            root.mkdir()
            xlsx_path = root / "085e9858e0b33a470bda9eb28@wx.tenpay.com.xlsx"
            normalized_csv = Path(temp_dir) / "normalized.csv"
            audit_csv = Path(temp_dir) / "rule_audit.csv"
            audit_xlsx = Path(temp_dir) / "rule_audit.xlsx"
            _write_xlsx_with_rows(
                xlsx_path,
                [
                    {
                        "交易流水号": "QR-1",
                        "交易金额": "388.00",
                        "交易时间": "2026-03-12 23:11:00",
                        "收款方的商户名称": "夜间私单",
                        "备注": "扫二维码付款-给夜间私单",
                        "方向": "支出",
                        "渠道": "微信",
                    },
                    {
                        "交易流水号": "OTHER-1",
                        "交易金额": "12.00",
                        "交易时间": "2026-03-12 11:11:00",
                        "收款方的商户名称": "普通商户",
                        "备注": "普通备注",
                        "方向": "收入",
                        "渠道": "微信",
                    },
                ],
            )

            rc = main(["normalize-ledgers", "--root", str(root), "--csv", str(normalized_csv)])
            self.assertEqual(rc, 0)

            rc = main(
                [
                    "export-rule-audit",
                    "--normalized",
                    str(normalized_csv),
                    "--csv",
                    str(audit_csv),
                    "--xlsx",
                    str(audit_xlsx),
                ]
            )
            self.assertEqual(rc, 0)
            self.assertTrue(audit_csv.exists())
            self.assertTrue(audit_xlsx.exists())
            with audit_csv.open(encoding="utf-8", newline="") as handle:
                rows = list(csv.DictReader(handle))
            self.assertGreaterEqual(len(rows), 1)
            self.assertIn("flow_family", rows[0])
            self.assertIn("trade_pattern", rows[0])
            self.assertIn("rule_reason", rows[0])
            self.assertIn("rule_hit_count", rows[0])
            self.assertIn("is_platform_settlement", rows[0])
            self.assertNotIn("payer_bank_card", rows[0])
            self.assertEqual(rows[0]["is_qr_transfer"], "True")
            self.assertIn("qr_hint", rows[0]["rule_reason"])
            styled_rows = load_xlsx_styled_rows(audit_xlsx)
            self.assertGreaterEqual(len(styled_rows), 1)
            self.assertEqual(styled_rows[0].fill_label, "yellow")

    def test_build_rule_summary_outputs_distribution_reports(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "root"
            root.mkdir()
            xlsx_path = root / "085e9858e0b33a470bda9eb28@wx.tenpay.com.xlsx"
            normalized_csv = Path(temp_dir) / "normalized.csv"
            summary_json = Path(temp_dir) / "rule_summary.json"
            summary_md = Path(temp_dir) / "rule_summary.md"
            _write_xlsx_with_rows(
                xlsx_path,
                [
                    {
                        "交易流水号": "QR-1",
                        "交易金额": "388.00",
                        "交易时间": "2026-03-12 23:11:00",
                        "收款方的商户名称": "夜间私单",
                        "备注": "扫二维码付款-给夜间私单",
                        "方向": "支出",
                        "渠道": "微信",
                    },
                    {
                        "交易流水号": "FAIL-1",
                        "交易金额": "20.00",
                        "交易时间": "2026-03-12 12:00:00",
                        "收款方的商户名称": "普通商户",
                        "备注": "付款失败",
                        "方向": "支出",
                        "渠道": "支付宝",
                    },
                ],
            )

            rc = main(["normalize-ledgers", "--root", str(root), "--csv", str(normalized_csv)])
            self.assertEqual(rc, 0)
            rc = main(
                [
                    "build-rule-summary",
                    "--normalized",
                    str(normalized_csv),
                    "--json",
                    str(summary_json),
                    "--md",
                    str(summary_md),
                ]
            )
            self.assertEqual(rc, 0)
            payload = json.loads(summary_json.read_text(encoding="utf-8"))
            self.assertEqual(payload["total_rows"], 2)
            self.assertGreaterEqual(payload["rows_with_rule_hits"], 2)
            self.assertIn("by_channel", payload)
            self.assertIn("by_trade_pattern", payload)
            self.assertIn("by_rule_reason", payload)
            self.assertTrue(any(item["key"] == "微信" for item in payload["by_channel"]))
            self.assertTrue(any(item["key"] == "qr_hint" for item in payload["by_rule_reason"]))
            self.assertIn("By Rule Reason", summary_md.read_text(encoding="utf-8"))

    def test_normalize_ledgers_marks_mirrored_transactions_across_workbooks(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "root"
            root.mkdir()
            normalized_csv = Path(temp_dir) / "normalized.csv"
            dataset_json = Path(temp_dir) / "graph_dataset.json"
            _write_xlsx_with_rows(
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
            _write_xlsx_with_rows(
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

            rc = main(["normalize-ledgers", "--root", str(root), "--csv", str(normalized_csv)])
            self.assertEqual(rc, 0)
            with normalized_csv.open(encoding="utf-8", newline="") as handle:
                rows = list(csv.DictReader(handle))
            self.assertEqual(len(rows), 2)
            self.assertEqual(rows[0]["mirror_match_count"], "2")
            self.assertEqual(rows[0]["mirror_workbook_count"], "2")
            self.assertEqual(rows[0]["mirror_has_opposite_direction"], "True")
            self.assertEqual(rows[0]["mirror_review_decision"], "confirmed_mirror")
            self.assertEqual(rows[0]["mirror_review_confidence"], "high")
            self.assertIn("mirrored_transaction", rows[0]["review_flags"])

            rc = main(["build-graph-dataset", "--root", str(root), "--json", str(dataset_json)])
            self.assertEqual(rc, 0)
            payload = json.loads(dataset_json.read_text(encoding="utf-8"))
            self.assertEqual(payload["mirrored_rows"], 2)
            self.assertEqual(payload["mirrored_groups"], 1)

    def test_export_ledger_review_outputs_minimal_fields(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "root"
            root.mkdir()
            normalized_csv = Path(temp_dir) / "normalized.csv"
            ledger_review_csv = Path(temp_dir) / "ledger_review.csv"
            ledger_review_xlsx = Path(temp_dir) / "ledger_review.xlsx"
            _write_xlsx_with_rows(
                root / "a.xlsx",
                [
                    {
                        "交易流水号": "TX-A-1",
                        "交易金额": "23.00",
                        "交易时间": "2026-03-19 12:52:22",
                        "收款方的商户名称": "王卫英(个人)",
                        "备注": "备注：扫二维码付款-给芸;微信转账",
                        "方向": "出账",
                        "渠道": "微信",
                    }
                ],
            )

            rc = main(["normalize-ledgers", "--root", str(root), "--csv", str(normalized_csv)])
            self.assertEqual(rc, 0)
            rc = main(
                [
                    "export-ledger-review",
                    "--normalized",
                    str(normalized_csv),
                    "--csv",
                    str(ledger_review_csv),
                    "--xlsx",
                    str(ledger_review_xlsx),
                ]
            )
            self.assertEqual(rc, 0)
            with ledger_review_csv.open(encoding="utf-8", newline="") as handle:
                rows = list(csv.DictReader(handle))
            self.assertEqual(len(rows), 1)
            self.assertIn("trade_pattern", rows[0])
            self.assertIn("is_qr_transfer", rows[0])
            self.assertIn("buyer_account", rows[0])
            self.assertIn("seller_account", rows[0])
            self.assertIn("rule_reason", rows[0])
            self.assertNotIn("device_ip", rows[0])
            self.assertNotIn("payer_bank_card", rows[0])
            styled_rows = load_xlsx_styled_rows(ledger_review_xlsx)
            self.assertEqual(styled_rows[0].fill_label, "yellow")

    def test_normalize_ledgers_marks_possible_mirror_candidates(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "root"
            root.mkdir()
            normalized_csv = Path(temp_dir) / "normalized.csv"
            dataset_json = Path(temp_dir) / "graph_dataset.json"
            _write_xlsx_with_rows(
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
            _write_xlsx_with_rows(
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

            rc = main(["normalize-ledgers", "--root", str(root), "--csv", str(normalized_csv)])
            self.assertEqual(rc, 0)
            with normalized_csv.open(encoding="utf-8", newline="") as handle:
                rows = list(csv.DictReader(handle))
            self.assertEqual(len(rows), 2)
            self.assertEqual(rows[0]["mirror_match_count"], "1")
            self.assertEqual(rows[0]["possible_mirror_match_count"], "2")
            self.assertEqual(rows[0]["possible_mirror_workbook_count"], "2")
            self.assertEqual(rows[0]["possible_mirror_confidence"], "low")
            self.assertTrue(float(rows[0]["possible_mirror_score"]) > 0.0)
            self.assertIn("possible_mirror_transaction", rows[0]["review_flags"])

            rc = main(["build-graph-dataset", "--root", str(root), "--json", str(dataset_json)])
            self.assertEqual(rc, 0)
            payload = json.loads(dataset_json.read_text(encoding="utf-8"))
            self.assertEqual(payload["mirrored_rows"], 0)
            self.assertEqual(payload["possible_mirrored_rows"], 2)
            self.assertEqual(payload["possible_mirrored_groups"], 1)

    def test_export_and_import_mirror_review_round_trip(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "root"
            root.mkdir()
            normalized_csv = Path(temp_dir) / "normalized.csv"
            mirror_review_csv = Path(temp_dir) / "mirror_review.csv"
            mirror_review_xlsx = Path(temp_dir) / "mirror_review.xlsx"
            mirror_annotations_csv = Path(temp_dir) / "mirror_annotations.csv"
            _write_xlsx_with_rows(
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
            _write_xlsx_with_rows(
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

            rc = main(["normalize-ledgers", "--root", str(root), "--csv", str(normalized_csv)])
            self.assertEqual(rc, 0)

            rc = main(
                [
                    "export-mirror-review",
                    "--normalized",
                    str(normalized_csv),
                    "--csv",
                    str(mirror_review_csv),
                    "--xlsx",
                    str(mirror_review_xlsx),
                ]
            )
            self.assertEqual(rc, 0)
            with mirror_review_csv.open(encoding="utf-8", newline="") as handle:
                rows = list(csv.DictReader(handle))
            self.assertEqual(len(rows), 2)
            self.assertEqual(rows[0]["mirror_status"], "possible")
            self.assertEqual(rows[0]["review_decision"], "")
            styled_rows = load_xlsx_styled_rows(mirror_review_xlsx)
            self.assertEqual(styled_rows[0].fill_label, "yellow")

            with mirror_review_csv.open("w", encoding="utf-8", newline="") as handle:
                writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
                writer.writeheader()
                rows[0]["review_decision"] = "confirmed_mirror"
                rows[0]["review_note"] = "人工确认同笔"
                writer.writerow(rows[0])

            rc = main(
                [
                    "import-mirror-review",
                    "--reviews",
                    str(mirror_review_csv),
                    "--csv",
                    str(mirror_annotations_csv),
                ]
            )
            self.assertEqual(rc, 0)
            with mirror_annotations_csv.open(encoding="utf-8", newline="") as handle:
                annotations = list(csv.DictReader(handle))
            self.assertEqual(annotations[0]["decision"], "confirmed_mirror")
            self.assertEqual(annotations[0]["confidence"], "high")

    def test_export_mirror_review_defaults_to_possible_candidates_only(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "root"
            root.mkdir()
            normalized_csv = Path(temp_dir) / "normalized.csv"
            mirror_review_csv = Path(temp_dir) / "mirror_review.csv"
            _write_xlsx_with_rows(
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
            _write_xlsx_with_rows(
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

            rc = main(["normalize-ledgers", "--root", str(root), "--csv", str(normalized_csv)])
            self.assertEqual(rc, 0)
            rc = main(["export-mirror-review", "--normalized", str(normalized_csv), "--csv", str(mirror_review_csv)])
            self.assertEqual(rc, 0)
            with mirror_review_csv.open(encoding="utf-8", newline="") as handle:
                rows = list(csv.DictReader(handle))

        self.assertEqual(rows, [])

    def test_normalize_and_graph_dataset_accept_mirror_annotations(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "root"
            root.mkdir()
            normalized_csv = Path(temp_dir) / "normalized.csv"
            dataset_json = Path(temp_dir) / "graph_dataset.json"
            owner_summary_json = Path(temp_dir) / "owner_summary.json"
            mirror_annotations_csv = Path(temp_dir) / "mirror_annotations.csv"
            _write_xlsx_with_rows(
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
            _write_xlsx_with_rows(
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
            _write_owners_csv(
                Path(temp_dir) / "owners.csv",
                [
                    {
                        "target_type": "transaction",
                        "target_id": "TX-MIRROR-1",
                        "owner_id": "owner_001",
                        "owner_name": "张三",
                        "confidence": "high",
                        "evidence": "manual_review",
                        "note": "",
                    }
                ],
            )
            owners_path = Path(temp_dir) / "owners.csv"
            _write_mirror_annotations_csv(
                mirror_annotations_csv,
                [
                    {
                        "mirror_group_id": "txid:TX-MIRROR-1",
                        "transaction_id": "TX-MIRROR-1",
                        "decision": "confirmed_mirror",
                        "confidence": "high",
                        "note": "人工复核确认",
                    }
                ],
            )

            rc = main(
                [
                    "normalize-ledgers",
                    "--root",
                    str(root),
                    "--owners",
                    str(owners_path),
                    "--mirror-annotations",
                    str(mirror_annotations_csv),
                    "--csv",
                    str(normalized_csv),
                ]
            )
            self.assertEqual(rc, 0)
            with normalized_csv.open(encoding="utf-8", newline="") as handle:
                rows = list(csv.DictReader(handle))
            self.assertEqual(rows[0]["mirror_review_decision"], "confirmed_mirror")
            self.assertEqual(rows[0]["mirror_review_confidence"], "high")
            self.assertEqual(rows[0]["mirror_review_note"], "人工复核确认")
            self.assertIn("mirror_review_confirmed", rows[0]["review_flags"])

            rc = main(
                [
                    "build-graph-dataset",
                    "--root",
                    str(root),
                    "--owners",
                    str(owners_path),
                    "--mirror-annotations",
                    str(mirror_annotations_csv),
                    "--json",
                    str(dataset_json),
                ]
            )
            self.assertEqual(rc, 0)
            payload = json.loads(dataset_json.read_text(encoding="utf-8"))
            self.assertEqual(payload["confirmed_mirror_rows"], 2)
            self.assertEqual(payload["rejected_mirror_rows"], 0)
            self.assertEqual(payload["uncertain_mirror_rows"], 0)

            rc = main(
                [
                    "build-owner-summary",
                    "--root",
                    str(root),
                    "--owners",
                    str(owners_path),
                    "--mirror-annotations",
                    str(mirror_annotations_csv),
                    "--json",
                    str(owner_summary_json),
                ]
            )
            self.assertEqual(rc, 0)
            owner_payload = json.loads(owner_summary_json.read_text(encoding="utf-8"))
            self.assertEqual(owner_payload["owners"][0]["mirrored_rows"], 2)

    def test_normalize_and_graph_dataset_accept_role_annotations(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "root"
            root.mkdir()
            xlsx_path = root / "sample.xlsx"
            roles_path = Path(temp_dir) / "roles.csv"
            owners_path = Path(temp_dir) / "owners.csv"
            normalized_csv = Path(temp_dir) / "normalized.csv"
            dataset_json = Path(temp_dir) / "graph_dataset.json"
            owner_summary_csv = Path(temp_dir) / "owner_summary.csv"
            owner_summary_json = Path(temp_dir) / "owner_summary.json"
            _write_xlsx_with_rows(
                xlsx_path,
                [
                    {
                        "交易流水号": "ROLE-1",
                        "交易金额": "388.00",
                        "交易时间": "2026-03-12 23:11:00",
                        "收款方的商户名称": "夜间私单",
                        "备注": "定金",
                    },
                    {
                        "交易流水号": "ROLE-2",
                        "交易金额": "188.00",
                        "交易时间": "2026-03-12 11:11:00",
                        "收款方的商户名称": "夜间私单",
                        "备注": "分账",
                    },
                ],
            )
            _write_roles_csv(
                roles_path,
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
                        "note": "已人工复核主体角色",
                    },
                ],
            )
            _write_owners_csv(
                owners_path,
                [
                    {
                        "target_type": "counterparty",
                        "target_id": "夜间私单",
                        "owner_id": "owner_001",
                        "owner_name": "张三",
                        "confidence": "high",
                        "evidence": "manual_review",
                        "note": "",
                    }
                ],
            )

            rc = main(
                [
                    "normalize-ledgers",
                    "--root",
                    str(root),
                    "--roles",
                    str(roles_path),
                    "--owners",
                    str(owners_path),
                    "--csv",
                    str(normalized_csv),
                ]
            )
            self.assertEqual(rc, 0)
            with normalized_csv.open(encoding="utf-8", newline="") as handle:
                rows = list(csv.DictReader(handle))
            self.assertEqual(rows[0]["role_label"], "broker")
            self.assertEqual(rows[0]["role_confidence"], "high")
            self.assertEqual(rows[0]["owner_id"], "owner_001")
            self.assertEqual(rows[0]["owner_name"], "张三")
            self.assertEqual(rows[0]["owner_tx_count"], "2")
            self.assertEqual(rows[0]["owner_unique_counterparties"], "1")
            self.assertEqual(rows[0]["owner_collect_and_split"], "False")

            rc = main(
                [
                    "build-graph-dataset",
                    "--root",
                    str(root),
                    "--roles",
                    str(roles_path),
                    "--owners",
                    str(owners_path),
                    "--json",
                    str(dataset_json),
                ]
            )
            self.assertEqual(rc, 0)
            payload = json.loads(dataset_json.read_text(encoding="utf-8"))
            self.assertEqual(payload["role_counts"]["broker"], 2)
            self.assertEqual(payload["owner_counts"]["owner_001"], 2)

            rc = main(
                [
                    "build-owner-summary",
                    "--root",
                    str(root),
                    "--roles",
                    str(roles_path),
                    "--owners",
                    str(owners_path),
                    "--csv",
                    str(owner_summary_csv),
                    "--json",
                    str(owner_summary_json),
                ]
            )
            self.assertEqual(rc, 0)
            with owner_summary_csv.open(encoding="utf-8", newline="") as handle:
                owner_rows = list(csv.DictReader(handle))
            self.assertEqual(len(owner_rows), 1)
            self.assertEqual(owner_rows[0]["owner_id"], "owner_001")
            self.assertEqual(owner_rows[0]["tx_count"], "2")
            self.assertEqual(owner_rows[0]["dominant_role"], "broker")
            self.assertEqual(owner_rows[0]["reviewed_role"], "broker")
            self.assertEqual(owner_rows[0]["reviewed_confidence"], "high")
            self.assertEqual(owner_rows[0]["priority_rank"], "1")
            self.assertTrue(float(owner_rows[0]["priority_score"]) > 0.0)
            self.assertEqual(owner_rows[0]["pattern_tags"], "")
            self.assertIn("夜间私单", owner_rows[0]["top_counterparties"])
            owner_payload = json.loads(owner_summary_json.read_text(encoding="utf-8"))
            self.assertEqual(owner_payload["covered_rows"], 2)
            self.assertEqual(owner_payload["skipped_rows"], 0)
            self.assertEqual(owner_payload["owners"][0]["owner_name"], "张三")
            self.assertEqual(owner_payload["owners"][0]["reviewed_role"], "broker")
            self.assertEqual(owner_payload["owners"][0]["priority_rank"], 1)
            self.assertEqual(owner_payload["owners"][0]["top_counterparties"][0]["counterparty"], "夜间私单")

    def test_import_review_labels_command_writes_manifests(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            review_csv = Path(temp_dir) / "review.csv"
            positive_json = Path(temp_dir) / "positive.json"
            negative_json = Path(temp_dir) / "negative.json"
            annotations_csv = Path(temp_dir) / "annotations.csv"
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
                        "record_id": "sample:1",
                        "entity_type": "transaction",
                        "pred_score": "0.91",
                        "review_label": "confirmed_positive",
                        "review_note": "confirmed",
                        "workbook_path": "/tmp/sample.xlsx",
                        "row_index": "1",
                        "transaction_id": "TX-P",
                        "counterparty": "A",
                        "remark": "r1",
                    }
                )
                writer.writerow(
                    {
                        "record_id": "sample:2",
                        "entity_type": "transaction",
                        "pred_score": "0.11",
                        "review_label": "confirmed_negative",
                        "review_note": "confirmed",
                        "workbook_path": "/tmp/sample.xlsx",
                        "row_index": "2",
                        "transaction_id": "TX-N",
                        "counterparty": "B",
                        "remark": "r2",
                    }
                )

            rc = main(
                [
                    "import-review-labels",
                    "--reviews",
                    str(review_csv),
                    "--dataset-prefix",
                    "round_02",
                    "--positive-json",
                    str(positive_json),
                    "--negative-json",
                    str(negative_json),
                    "--annotations-csv",
                    str(annotations_csv),
                    "--subject",
                    "reviewed_batch",
                    "--verified-by",
                    "analyst",
                ]
            )

            self.assertEqual(rc, 0)
            self.assertTrue(positive_json.exists())
            self.assertTrue(negative_json.exists())
            self.assertTrue(annotations_csv.exists())
            positive_payload = json.loads(positive_json.read_text(encoding="utf-8"))
            negative_payload = json.loads(negative_json.read_text(encoding="utf-8"))
            self.assertEqual(positive_payload["label"], "high_risk_transaction")
            self.assertEqual(negative_payload["label"], "low_risk_transaction")
            self.assertEqual(positive_payload["transaction_ids"], ["TX-P"])
            self.assertEqual(negative_payload["transaction_ids"], ["TX-N"])
            with annotations_csv.open(encoding="utf-8", newline="") as handle:
                rows = list(csv.DictReader(handle))
            self.assertEqual(rows[0]["transaction_id"], "TX-P")
            self.assertEqual(rows[0]["label"], "positive")
            self.assertEqual(rows[1]["transaction_id"], "TX-N")
            self.assertEqual(rows[1]["label"], "negative")

    def test_import_review_labels_accepts_filled_xlsx(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            review_xlsx = Path(temp_dir) / "review.xlsx"
            annotations_csv = Path(temp_dir) / "annotations.csv"
            positive_json = Path(temp_dir) / "positive.json"
            negative_json = Path(temp_dir) / "negative.json"
            write_xlsx_table(
                review_xlsx,
                headers=[
                    "record_id",
                    "entity_type",
                    "pred_score",
                    "review_label",
                    "review_options",
                    "review_note",
                    "workbook_path",
                    "row_index",
                    "transaction_id",
                    "counterparty",
                    "remark",
                ],
                rows=[
                    {
                        "record_id": "sample:1",
                        "entity_type": "transaction",
                        "pred_score": "0.91",
                        "review_label": "",
                        "review_options": "confirmed_positive|confirmed_negative|uncertain",
                        "review_note": "confirmed true",
                        "workbook_path": "/tmp/sample.xlsx",
                        "row_index": "1",
                        "transaction_id": "TX-P",
                        "counterparty": "A",
                        "remark": "r1",
                    },
                    {
                        "record_id": "sample:2",
                        "entity_type": "transaction",
                        "pred_score": "0.81",
                        "review_label": "",
                        "review_options": "confirmed_positive|confirmed_negative|uncertain",
                        "review_note": "high risk",
                        "workbook_path": "/tmp/sample.xlsx",
                        "row_index": "2",
                        "transaction_id": "TX-Y",
                        "counterparty": "B",
                        "remark": "r2",
                    },
                ],
                row_fills=["red", "yellow"],
            )

            rc = main(
                [
                    "import-review-labels",
                    "--reviews",
                    str(review_xlsx),
                    "--dataset-prefix",
                    "round_03",
                    "--positive-json",
                    str(positive_json),
                    "--negative-json",
                    str(negative_json),
                    "--annotations-csv",
                    str(annotations_csv),
                ]
            )

            self.assertEqual(rc, 0)
            positive_payload = json.loads(positive_json.read_text(encoding="utf-8"))
            negative_payload = json.loads(negative_json.read_text(encoding="utf-8"))
            self.assertEqual(positive_payload["transaction_ids"], ["TX-P"])
            self.assertEqual(negative_payload["transaction_ids"], [])
            with annotations_csv.open(encoding="utf-8", newline="") as handle:
                rows = list(csv.DictReader(handle))
            self.assertEqual(rows[0]["transaction_id"], "TX-P")
            self.assertEqual(rows[0]["label"], "positive")
            self.assertEqual(rows[1]["transaction_id"], "TX-Y")
            self.assertEqual(rows[1]["label"], "skip")

    def test_export_and_import_owner_review_round_trip(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "root"
            root.mkdir()
            xlsx_path = root / "sample.xlsx"
            owners_path = Path(temp_dir) / "owners.csv"
            owner_summary_json = Path(temp_dir) / "owner_summary.json"
            owner_review_csv = Path(temp_dir) / "owner_review.csv"
            owner_review_xlsx = Path(temp_dir) / "owner_review.xlsx"
            imported_roles_csv = Path(temp_dir) / "imported_roles.csv"
            normalized_csv = Path(temp_dir) / "normalized.csv"
            _write_xlsx_with_rows(
                xlsx_path,
                [
                    {
                        "交易流水号": "OWN-1",
                        "交易金额": "388.00",
                        "交易时间": "2026-03-12 23:11:00",
                        "收款方的商户名称": "夜间私单A",
                        "备注": "定金",
                    },
                    {
                        "交易流水号": "OWN-2",
                        "交易金额": "188.00",
                        "交易时间": "2026-03-12 11:11:00",
                        "收款方的商户名称": "夜间私单B",
                        "备注": "分账",
                    },
                ],
            )
            _write_owners_csv(
                owners_path,
                [
                    {
                        "target_type": "transaction",
                        "target_id": "OWN-1",
                        "owner_id": "owner_001",
                        "owner_name": "张三",
                        "confidence": "high",
                        "evidence": "manual_review",
                        "note": "",
                    },
                    {
                        "target_type": "transaction",
                        "target_id": "OWN-2",
                        "owner_id": "owner_001",
                        "owner_name": "张三",
                        "confidence": "high",
                        "evidence": "manual_review",
                        "note": "",
                    },
                ],
            )

            rc = main(
                [
                    "build-owner-summary",
                    "--root",
                    str(root),
                    "--owners",
                    str(owners_path),
                    "--json",
                    str(owner_summary_json),
                ]
            )
            self.assertEqual(rc, 0)

            rc = main(
                [
                    "export-owner-review",
                    "--summary",
                    str(owner_summary_json),
                    "--csv",
                    str(owner_review_csv),
                    "--xlsx",
                    str(owner_review_xlsx),
                ]
            )
            self.assertEqual(rc, 0)
            with owner_review_csv.open(encoding="utf-8", newline="") as handle:
                review_rows = list(csv.DictReader(handle))
            self.assertEqual(review_rows[0]["owner_id"], "owner_001")
            self.assertEqual(review_rows[0]["review_role"], "")
            styled_rows = load_xlsx_styled_rows(owner_review_xlsx)
            self.assertEqual(styled_rows[0].fill_label, "yellow")

            with owner_review_csv.open("w", encoding="utf-8", newline="") as handle:
                writer = csv.DictWriter(handle, fieldnames=list(review_rows[0].keys()))
                writer.writeheader()
                review_rows[0]["review_role"] = "broker"
                review_rows[0]["review_confidence"] = "high"
                review_rows[0]["review_note"] = "主体复核确认中间商"
                writer.writerow(review_rows[0])

            rc = main(
                [
                    "import-owner-review",
                    "--reviews",
                    str(owner_review_csv),
                    "--roles-csv",
                    str(imported_roles_csv),
                    "--scene",
                    "vice",
                ]
            )
            self.assertEqual(rc, 0)
            with imported_roles_csv.open(encoding="utf-8", newline="") as handle:
                role_rows = list(csv.DictReader(handle))
            self.assertEqual(role_rows[0]["target_type"], "owner")
            self.assertEqual(role_rows[0]["target_id"], "owner_001")
            self.assertEqual(role_rows[0]["role_label"], "broker")

            rc = main(
                [
                    "normalize-ledgers",
                    "--root",
                    str(root),
                    "--owners",
                    str(owners_path),
                    "--roles",
                    str(imported_roles_csv),
                    "--csv",
                    str(normalized_csv),
                ]
            )
            self.assertEqual(rc, 0)
            with normalized_csv.open(encoding="utf-8", newline="") as handle:
                normalized_rows = list(csv.DictReader(handle))
            self.assertEqual(normalized_rows[0]["role_label"], "broker")
            self.assertEqual(normalized_rows[0]["role_confidence"], "high")

    def test_merge_label_manifests_command_writes_merged_manifests(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            positive_json = Path(temp_dir) / "merged_positive.json"
            negative_json = Path(temp_dir) / "merged_negative.json"

            rc = main(
                [
                    "merge-label-manifests",
                    "--labels",
                    str(LI_SHAOYUN_POSITIVE_PATH),
                    str(LI_SHAOYUN_POSITIVE_PATH),
                    str(LI_SHAOYUN_NEGATIVE_PATH),
                    "--dataset-prefix",
                    "merged_round",
                    "--positive-json",
                    str(positive_json),
                    "--negative-json",
                    str(negative_json),
                    "--subject",
                    "merged_batch",
                    "--verified-by",
                    "analyst",
                ]
            )

            self.assertEqual(rc, 0)
            self.assertTrue(positive_json.exists())
            self.assertTrue(negative_json.exists())
            positive_payload = json.loads(positive_json.read_text(encoding="utf-8"))
            negative_payload = json.loads(negative_json.read_text(encoding="utf-8"))
            self.assertEqual(positive_payload["label"], "high_risk_transaction")
            self.assertEqual(negative_payload["label"], "low_risk_transaction")
            self.assertEqual(len(positive_payload["transaction_ids"]), len(set(positive_payload["transaction_ids"])))
            self.assertGreater(len(negative_payload["transaction_ids"]), 0)

    def test_compare_round_metrics_command_writes_outputs(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            metrics_one = Path(temp_dir) / "round1_metrics.json"
            metrics_two = Path(temp_dir) / "round2_metrics.json"
            review_one = Path(temp_dir) / "round1_review.csv"
            review_two = Path(temp_dir) / "round2_review.csv"
            output_json = Path(temp_dir) / "comparison.json"
            output_md = Path(temp_dir) / "comparison.md"

            metrics_one.write_text(
                json.dumps(
                    {
                        "best_val_f1": 0.61,
                        "best_val_loss": 0.42,
                        "positive_rate": 0.3,
                        "train_nodes": 12,
                        "val_nodes": 4,
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )
            metrics_two.write_text(
                json.dumps(
                    {
                        "best_val_f1": 0.74,
                        "best_val_loss": 0.31,
                        "positive_rate": 0.35,
                        "train_nodes": 18,
                        "val_nodes": 6,
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )

            for review_path, rows in (
                (
                    review_one,
                    [
                        {"review_label": "confirmed_positive"},
                        {"review_label": "confirmed_negative"},
                        {"review_label": "uncertain"},
                    ],
                ),
                (
                    review_two,
                    [
                        {"review_label": "confirmed_positive"},
                        {"review_label": "confirmed_positive"},
                        {"review_label": "confirmed_negative"},
                    ],
                ),
            ):
                with review_path.open("w", encoding="utf-8", newline="") as handle:
                    writer = csv.DictWriter(handle, fieldnames=["review_label"])
                    writer.writeheader()
                    for row in rows:
                        writer.writerow(row)

            rc = main(
                [
                    "compare-round-metrics",
                    "--round",
                    f"round_01:{metrics_one}:{review_one}",
                    "--round",
                    f"round_02:{metrics_two}:{review_two}",
                    "--json",
                    str(output_json),
                    "--md",
                    str(output_md),
                ]
            )

            self.assertEqual(rc, 0)
            self.assertTrue(output_json.exists())
            self.assertTrue(output_md.exists())
            payload = json.loads(output_json.read_text(encoding="utf-8"))
            self.assertEqual(len(payload["rounds"]), 2)
            self.assertEqual(payload["rounds"][0]["round_name"], "round_01")
            self.assertEqual(payload["rounds"][1]["review_total"], 3)

    def test_make_round_report_command_writes_outputs(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            metrics_path = Path(temp_dir) / "metrics.json"
            scores_path = Path(temp_dir) / "scores.json"
            reviews_path = Path(temp_dir) / "review.csv"
            positive_label = Path(temp_dir) / "positive.json"
            negative_label = Path(temp_dir) / "negative.json"
            output_json = Path(temp_dir) / "round_report.json"
            output_md = Path(temp_dir) / "round_report.md"

            metrics_path.write_text(
                json.dumps(
                    {
                        "best_val_f1": 0.77,
                        "best_val_loss": 0.29,
                        "positive_rate": 0.33,
                        "train_nodes": 20,
                        "val_nodes": 6,
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )
            scores_path.write_text(
                json.dumps(
                    {
                        "total_rows": 25,
                        "top_rows": [
                            {"score": 0.91},
                            {"score": 0.82},
                        ],
                        "workbooks": [
                            {"path": "/tmp/top.xlsx"},
                        ],
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )
            with reviews_path.open("w", encoding="utf-8", newline="") as handle:
                writer = csv.DictWriter(handle, fieldnames=["review_label"])
                writer.writeheader()
                writer.writerow({"review_label": "confirmed_positive"})
                writer.writerow({"review_label": "confirmed_negative"})
                writer.writerow({"review_label": "uncertain"})

            positive_label.write_text(
                json.dumps(
                    {
                        "dataset_name": "round_positive",
                        "label": "high_risk_transaction",
                        "subject": "merged_batch",
                        "status": "verified",
                        "source_file": "",
                        "transaction_ids": ["TX-1", "TX-2"],
                        "polarity": "positive",
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )
            negative_label.write_text(
                json.dumps(
                    {
                        "dataset_name": "round_negative",
                        "label": "low_risk_transaction",
                        "subject": "merged_batch",
                        "status": "verified",
                        "source_file": "",
                        "transaction_ids": ["TX-3"],
                        "polarity": "negative",
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )

            rc = main(
                [
                    "make-round-report",
                    "--round-name",
                    "round_02",
                    "--metrics",
                    str(metrics_path),
                    "--scores",
                    str(scores_path),
                    "--reviews",
                    str(reviews_path),
                    "--labels",
                    str(positive_label),
                    str(negative_label),
                    "--json",
                    str(output_json),
                    "--md",
                    str(output_md),
                ]
            )

            self.assertEqual(rc, 0)
            self.assertTrue(output_json.exists())
            self.assertTrue(output_md.exists())
            payload = json.loads(output_json.read_text(encoding="utf-8"))
            self.assertEqual(payload["round_name"], "round_02")
            self.assertEqual(payload["score_summary"]["top_rows"], 2)
            self.assertEqual(payload["review_summary"]["review_total"], 3)
            self.assertEqual(payload["label_summary"]["manifest_count"], 2)

    def test_bootstrap_round_command_writes_outputs(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            output_json = Path(temp_dir) / "round_03" / "bootstrap.json"
            output_md = Path(temp_dir) / "round_03" / "bootstrap.md"

            rc = main(
                [
                    "bootstrap-round",
                    "--round-name",
                    "round_03",
                    "--base-dir",
                    str(Path(temp_dir)),
                    "--train-root",
                    "/tmp/train_workbooks",
                    "--score-root",
                    "/tmp/score_workbooks",
                    "--label-glob",
                    "data/labels/*.json",
                    "--json",
                    str(output_json),
                    "--md",
                    str(output_md),
                ]
            )

            self.assertEqual(rc, 0)
            self.assertTrue(output_json.exists())
            self.assertTrue(output_md.exists())
            payload = json.loads(output_json.read_text(encoding="utf-8"))
            self.assertEqual(payload["round_name"], "round_03")
            self.assertIn("model_pt", payload["files"])
            self.assertGreaterEqual(len(payload["commands"]), 5)
            self.assertTrue((Path(temp_dir) / "round_03" / "README.md").exists())

    def test_score_threshold_sweep_command_writes_outputs(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            scores_path = Path(temp_dir) / "scores.json"
            reviews_path = Path(temp_dir) / "review.csv"
            output_json = Path(temp_dir) / "threshold_sweep.json"
            output_md = Path(temp_dir) / "threshold_sweep.md"

            scores_path.write_text(
                json.dumps(
                    {
                        "top_rows": [
                            {"workbook_path": "/tmp/a.xlsx", "row_index": 1, "score": 0.92},
                            {"workbook_path": "/tmp/a.xlsx", "row_index": 2, "score": 0.81},
                            {"workbook_path": "/tmp/b.xlsx", "row_index": 3, "score": 0.66},
                        ]
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )
            with reviews_path.open("w", encoding="utf-8", newline="") as handle:
                writer = csv.DictWriter(handle, fieldnames=["record_id", "review_label"])
                writer.writeheader()
                writer.writerow({"record_id": "/tmp/a.xlsx:1", "review_label": "confirmed_positive"})
                writer.writerow({"record_id": "/tmp/a.xlsx:2", "review_label": "confirmed_negative"})
                writer.writerow({"record_id": "/tmp/b.xlsx:3", "review_label": "uncertain"})

            rc = main(
                [
                    "score-threshold-sweep",
                    "--scores",
                    str(scores_path),
                    "--reviews",
                    str(reviews_path),
                    "--threshold",
                    "0.60",
                    "--threshold",
                    "0.80",
                    "--threshold",
                    "0.90",
                    "--json",
                    str(output_json),
                    "--md",
                    str(output_md),
                ]
            )

            self.assertEqual(rc, 0)
            self.assertTrue(output_json.exists())
            self.assertTrue(output_md.exists())
            payload = json.loads(output_json.read_text(encoding="utf-8"))
            self.assertEqual(len(payload["rows"]), 3)
            self.assertEqual(payload["rows"][0]["candidate_count"], 3)
            self.assertEqual(payload["rows"][1]["candidate_count"], 2)
            self.assertEqual(payload["rows"][2]["confirmed_positive"], 1)

    def test_review_workload_forecast_command_writes_outputs(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            scores_path = Path(temp_dir) / "scores.json"
            reviews_path = Path(temp_dir) / "review.csv"
            output_json = Path(temp_dir) / "workload_forecast.json"
            output_md = Path(temp_dir) / "workload_forecast.md"

            scores_path.write_text(
                json.dumps(
                    {
                        "top_rows": [
                            {"workbook_path": "/tmp/a.xlsx", "row_index": 1, "score": 0.92},
                            {"workbook_path": "/tmp/a.xlsx", "row_index": 2, "score": 0.81},
                            {"workbook_path": "/tmp/b.xlsx", "row_index": 3, "score": 0.66},
                        ]
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )
            with reviews_path.open("w", encoding="utf-8", newline="") as handle:
                writer = csv.DictWriter(handle, fieldnames=["record_id", "review_label"])
                writer.writeheader()
                writer.writerow({"record_id": "/tmp/a.xlsx:1", "review_label": "confirmed_positive"})
                writer.writerow({"record_id": "/tmp/a.xlsx:2", "review_label": "confirmed_negative"})
                writer.writerow({"record_id": "/tmp/b.xlsx:3", "review_label": "uncertain"})

            rc = main(
                [
                    "review-workload-forecast",
                    "--scores",
                    str(scores_path),
                    "--reviews",
                    str(reviews_path),
                    "--threshold",
                    "0.60",
                    "--threshold",
                    "0.80",
                    "--reviewers",
                    "2",
                    "--daily-capacity",
                    "40",
                    "--json",
                    str(output_json),
                    "--md",
                    str(output_md),
                ]
            )

            self.assertEqual(rc, 0)
            self.assertTrue(output_json.exists())
            self.assertTrue(output_md.exists())
            payload = json.loads(output_json.read_text(encoding="utf-8"))
            self.assertEqual(payload["reviewers"], 2)
            self.assertEqual(payload["daily_capacity_per_reviewer"], 40)
            self.assertEqual(len(payload["rows"]), 2)
            self.assertEqual(payload["rows"][0]["candidate_count"], 3)
            self.assertGreater(payload["rows"][0]["estimated_team_days"], 0)

    def test_select_operating_threshold_command_writes_outputs(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            scores_path = Path(temp_dir) / "scores.json"
            reviews_path = Path(temp_dir) / "review.csv"
            output_json = Path(temp_dir) / "operating_threshold.json"
            output_md = Path(temp_dir) / "operating_threshold.md"

            scores_path.write_text(
                json.dumps(
                    {
                        "top_rows": [
                            {"workbook_path": "/tmp/a.xlsx", "row_index": 1, "score": 0.92},
                            {"workbook_path": "/tmp/a.xlsx", "row_index": 2, "score": 0.81},
                            {"workbook_path": "/tmp/b.xlsx", "row_index": 3, "score": 0.66},
                        ]
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )
            with reviews_path.open("w", encoding="utf-8", newline="") as handle:
                writer = csv.DictWriter(handle, fieldnames=["record_id", "review_label"])
                writer.writeheader()
                writer.writerow({"record_id": "/tmp/a.xlsx:1", "review_label": "confirmed_positive"})
                writer.writerow({"record_id": "/tmp/a.xlsx:2", "review_label": "confirmed_negative"})
                writer.writerow({"record_id": "/tmp/b.xlsx:3", "review_label": "uncertain"})

            rc = main(
                [
                    "select-operating-threshold",
                    "--scores",
                    str(scores_path),
                    "--reviews",
                    str(reviews_path),
                    "--threshold",
                    "0.60",
                    "--threshold",
                    "0.80",
                    "--threshold",
                    "0.90",
                    "--reviewers",
                    "2",
                    "--daily-capacity",
                    "40",
                    "--max-team-days",
                    "0.05",
                    "--min-confirmed-positive-rate",
                    "0.40",
                    "--min-candidates",
                    "1",
                    "--json",
                    str(output_json),
                    "--md",
                    str(output_md),
                ]
            )

            self.assertEqual(rc, 0)
            self.assertTrue(output_json.exists())
            self.assertTrue(output_md.exists())
            payload = json.loads(output_json.read_text(encoding="utf-8"))
            self.assertEqual(payload["recommended_threshold"], 0.9)
            self.assertIn("reason", payload)
            self.assertEqual(payload["row"]["candidate_count"], 1)

    def test_round_decision_sheet_command_writes_outputs(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            metrics_path = Path(temp_dir) / "metrics.json"
            scores_path = Path(temp_dir) / "scores.json"
            reviews_path = Path(temp_dir) / "review.csv"
            positive_label = Path(temp_dir) / "positive.json"
            negative_label = Path(temp_dir) / "negative.json"
            output_json = Path(temp_dir) / "decision_sheet.json"
            output_md = Path(temp_dir) / "decision_sheet.md"

            metrics_path.write_text(
                json.dumps(
                    {
                        "best_val_f1": 0.77,
                        "best_val_loss": 0.29,
                        "positive_rate": 0.33,
                        "train_nodes": 20,
                        "val_nodes": 6,
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )
            scores_path.write_text(
                json.dumps(
                    {
                        "total_rows": 25,
                        "top_rows": [
                            {"workbook_path": "/tmp/a.xlsx", "row_index": 1, "score": 0.92},
                            {"workbook_path": "/tmp/a.xlsx", "row_index": 2, "score": 0.81},
                            {"workbook_path": "/tmp/b.xlsx", "row_index": 3, "score": 0.66},
                        ],
                        "workbooks": [{"path": "/tmp/a.xlsx"}],
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )
            with reviews_path.open("w", encoding="utf-8", newline="") as handle:
                writer = csv.DictWriter(handle, fieldnames=["record_id", "review_label"])
                writer.writeheader()
                writer.writerow({"record_id": "/tmp/a.xlsx:1", "review_label": "confirmed_positive"})
                writer.writerow({"record_id": "/tmp/a.xlsx:2", "review_label": "confirmed_negative"})
                writer.writerow({"record_id": "/tmp/b.xlsx:3", "review_label": "uncertain"})

            positive_label.write_text(
                json.dumps(
                    {
                        "dataset_name": "round_positive",
                        "label": "high_risk_transaction",
                        "subject": "merged_batch",
                        "status": "verified",
                        "source_file": "",
                        "transaction_ids": ["TX-1", "TX-2"],
                        "polarity": "positive",
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )
            negative_label.write_text(
                json.dumps(
                    {
                        "dataset_name": "round_negative",
                        "label": "low_risk_transaction",
                        "subject": "merged_batch",
                        "status": "verified",
                        "source_file": "",
                        "transaction_ids": ["TX-3"],
                        "polarity": "negative",
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )

            rc = main(
                [
                    "round-decision-sheet",
                    "--round-name",
                    "round_03",
                    "--metrics",
                    str(metrics_path),
                    "--scores",
                    str(scores_path),
                    "--reviews",
                    str(reviews_path),
                    "--labels",
                    str(positive_label),
                    str(negative_label),
                    "--threshold",
                    "0.60",
                    "--threshold",
                    "0.80",
                    "--threshold",
                    "0.90",
                    "--reviewers",
                    "2",
                    "--daily-capacity",
                    "40",
                    "--max-team-days",
                    "0.05",
                    "--min-confirmed-positive-rate",
                    "0.40",
                    "--json",
                    str(output_json),
                    "--md",
                    str(output_md),
                ]
            )

            self.assertEqual(rc, 0)
            self.assertTrue(output_json.exists())
            self.assertTrue(output_md.exists())
            payload = json.loads(output_json.read_text(encoding="utf-8"))
            self.assertEqual(payload["round_name"], "round_03")
            self.assertEqual(payload["threshold_recommendation"]["recommended_threshold"], 0.9)
            self.assertGreaterEqual(len(payload["next_actions"]), 3)

    def test_triage_workbooks_command_writes_outputs(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "root"
            root.mkdir()
            xlsx_path = root / "triage.xlsx"
            json_path = Path(temp_dir) / "triage.json"
            md_path = Path(temp_dir) / "triage.md"
            _write_xlsx_with_rows(
                xlsx_path,
                [
                    {
                        "交易流水号": "2026031264155009238164850111202",
                        "交易金额": "388.00",
                        "交易时间": "2026-03-12 23:11:00",
                        "收款方的商户名称": "夜间私单",
                        "备注": "定金",
                    },
                    {
                        "交易流水号": "NEG-001",
                        "交易金额": "12.00",
                        "交易时间": "2026-03-12 11:11:00",
                        "收款方的商户名称": "超市消费",
                        "备注": "买菜",
                    },
                ],
            )

            rc = main(
                [
                    "triage-workbooks",
                    "--root",
                    str(root),
                    "--labels",
                    str(LI_SHAOYUN_POSITIVE_PATH),
                    str(LI_SHAOYUN_NEGATIVE_PATH),
                    "--json",
                    str(json_path),
                    "--md",
                    str(md_path),
                ]
            )

            self.assertEqual(rc, 0)
            self.assertTrue(json_path.exists())
            self.assertTrue(md_path.exists())


if __name__ == "__main__":
    unittest.main()
