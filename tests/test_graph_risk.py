from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from datetime import datetime
from zipfile import ZipFile

from txflow.graph_risk import score_directory
from txflow.labels import load_label_manifest


PROJECT_ROOT = Path(__file__).resolve().parents[1]
POSITIVE_PATH = PROJECT_ROOT / "data" / "labels" / "lixiaoxiao168_li_shaoyun_verified_positive.json"
NEGATIVE_PATH = PROJECT_ROOT / "data" / "labels" / "lixiaoxiao168_li_shaoyun_verified_negative.json"


def _write_xlsx(path: Path, rows: list[dict[str, str]]) -> None:
    headers = ["交易流水号", "交易金额", "交易时间", "收款方的商户名称", "备注", "方向", "渠道"]
    row_xml = []
    for row_index, row in enumerate(rows, start=2):
        cells = []
        for col_index, header in enumerate(headers, start=1):
            value = row.get(header, "")
            if col_index <= 26:
                ref = f"{chr(64 + col_index)}{row_index}"
            else:
                ref = f"A{row_index}"
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


class GraphRiskTests(unittest.TestCase):
    def test_score_directory_finds_positive_like_unlabeled_row(self) -> None:
        manifests = [
            load_label_manifest(POSITIVE_PATH),
            load_label_manifest(NEGATIVE_PATH),
        ]
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            workbook = root / "sample.xlsx"
            _write_xlsx(
                workbook,
                [
                    {
                        "交易流水号": "2025102055205830178285410301304",
                        "交易金额": "388.00",
                        "交易时间": "2026-03-12 23:11:00",
                        "收款方的商户名称": "夜间私单",
                        "备注": "定金",
                        "方向": "收入",
                        "渠道": "微信",
                    },
                    {
                        "交易流水号": "20260225712258976106235S0211308",
                        "交易金额": "12.00",
                        "交易时间": "2026-03-12 11:11:00",
                        "收款方的商户名称": "超市消费",
                        "备注": "买菜",
                        "方向": "支出",
                        "渠道": "支付宝",
                    },
                    {
                        "交易流水号": "UNLABELED-ROW",
                        "交易金额": "420.00",
                        "交易时间": "2026-03-14 23:20:00",
                        "收款方的商户名称": "夜间约单",
                        "备注": "加钟",
                        "方向": "收入",
                        "渠道": "微信",
                    },
                ],
            )

            report = score_directory(root, manifests, top_k=5, include_labeled=False)

        self.assertEqual(report.total_workbooks, 1)
        self.assertGreaterEqual(report.total_rows, 3)
        self.assertTrue(any(item.transaction_id == "UNLABELED-ROW" for item in report.top_rows))
        self.assertGreater(
            next(item.score for item in report.top_rows if item.transaction_id == "UNLABELED-ROW"),
            0.5,
        )

    def test_cross_workbook_pair_candidates_are_prioritized(self) -> None:
        positive_manifest = load_label_manifest(POSITIVE_PATH)
        negative_manifest = load_label_manifest(NEGATIVE_PATH)
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            _write_xlsx(
                root / "seller.xlsx",
                [
                    {
                        "交易流水号": positive_manifest.transaction_ids[0],
                        "交易金额": "388.00",
                        "交易时间": "2026-03-12 23:11:00",
                        "收款方的商户名称": "夜间私单",
                        "备注": "定金",
                        "方向": "收入",
                        "渠道": "微信",
                    },
                    {
                        "交易流水号": "SELLER-CANDIDATE",
                        "交易金额": "200.00",
                        "交易时间": "2026-03-12 23:20:00",
                        "收款方的商户名称": "同一对手",
                        "备注": "微信转账",
                        "方向": "收入",
                        "渠道": "微信",
                    },
                ],
            )
            _write_xlsx(
                root / "buyer.xlsx",
                [
                    {
                        "交易流水号": negative_manifest.transaction_ids[0],
                        "交易金额": "12.00",
                        "交易时间": "2026-03-12 11:11:00",
                        "收款方的商户名称": "超市消费",
                        "备注": "买菜",
                        "方向": "支出",
                        "渠道": "支付宝",
                    },
                    {
                        "交易流水号": "BUYER-CANDIDATE",
                        "交易金额": "200.00",
                        "交易时间": "2026-03-12 23:29:00",
                        "收款方的商户名称": "同一对手",
                        "备注": "微信转账",
                        "方向": "支出",
                        "渠道": "微信",
                    },
                ],
            )

            report = score_directory(root, [positive_manifest, negative_manifest], top_k=10, include_labeled=False)

        self.assertTrue(any(pair.cross_workbook for pair in report.pairs))
        self.assertEqual(report.pairs[0].pair_type, "cross_workbook")
        self.assertTrue(
            {report.pairs[0].left_workbook_path, report.pairs[0].right_workbook_path}
            == {str(root / "seller.xlsx"), str(root / "buyer.xlsx")}
            or report.pairs[0].cross_workbook
        )

    def test_synthetic_warmup_and_self_training_update_summary(self) -> None:
        manifests = [
            load_label_manifest(POSITIVE_PATH),
            load_label_manifest(NEGATIVE_PATH),
        ]
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            workbook = root / "iterative.xlsx"
            _write_xlsx(
                workbook,
                [
                    {
                        "交易流水号": "2025102055205830178285410301304",
                        "交易金额": "388.00",
                        "交易时间": "2026-03-12 23:11:00",
                        "收款方的商户名称": "夜间私单",
                        "备注": "定金",
                        "方向": "收入",
                        "渠道": "微信",
                    },
                    {
                        "交易流水号": "20260225712258976106235S0211308",
                        "交易金额": "12.00",
                        "交易时间": "2026-03-12 11:11:00",
                        "收款方的商户名称": "超市消费",
                        "备注": "买菜",
                        "方向": "支出",
                        "渠道": "支付宝",
                    },
                    {
                        "交易流水号": "UNLABELED-ITERATIVE",
                        "交易金额": "388.00",
                        "交易时间": "2026-03-12 23:16:00",
                        "收款方的商户名称": "夜间私单",
                        "备注": "夜间定金",
                        "方向": "收入",
                        "渠道": "微信",
                    },
                ],
            )

            report = score_directory(
                root,
                manifests,
                top_k=5,
                include_labeled=False,
                synthetic_warmup=2,
                self_training_rounds=1,
                pseudo_positive_threshold=0.75,
                pseudo_negative_threshold=0.25,
                pseudo_max_rows=10,
            )

        self.assertGreater(report.training.synthetic_rows, 0)
        self.assertGreaterEqual(report.training.self_training_rounds, 1)
        self.assertGreaterEqual(report.training.pseudo_labeled_rows, 1)
        self.assertTrue(any(item.transaction_id == "UNLABELED-ITERATIVE" for item in report.top_rows))


if __name__ == "__main__":
    unittest.main()
