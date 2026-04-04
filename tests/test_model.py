from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from txflow.model import BaselineTextClassifier, example_to_tokens
from txflow.triage import train_global_classifier, triage_workbook
from txflow.training import TrainingExample


def _example(
    row_index: int,
    transaction_id: str,
    label: str,
    label_status: str,
    amount: str,
    timestamp: str,
    hour: int | None,
    weekday: int | None,
    is_night: bool,
    counterparty: str,
    direction: str,
    channel: str,
    remark: str,
) -> TrainingExample:
    return TrainingExample(
        row_index=row_index,
        transaction_id=transaction_id,
        label=label,
        label_status=label_status,
        subject="",
        source_file="",
        amount=amount,
        timestamp=timestamp,
        hour=hour,
        weekday=weekday,
        is_night=is_night,
        counterparty=counterparty,
        direction=direction,
        channel=channel,
        remark=remark,
        raw={},
    )


class BaselineModelTests(unittest.TestCase):
    def test_example_to_tokens_uses_feature_fields(self) -> None:
        tokens = example_to_tokens(
            _example(1, "p1", "sex_work_related_income", "positive", "388", "2026-03-12 23:11:00", 23, 3, True, "夜间私单", "入账", "微信", "定金")
        )

        self.assertIn("counterparty:夜间私单", tokens)
        self.assertIn("remark:定金", tokens)
        self.assertIn("amount_bucket:lt_500", tokens)
        self.assertIn("hour_bucket:night_22_23", tokens)
        self.assertNotIn("label_status:positive", tokens)

    def test_classifier_trains_predicts_and_serializes(self) -> None:
        train_examples = [
            _example(1, "p1", "sex_work_related_income", "positive", "388", "2026-03-12 23:11:00", 23, 3, True, "夜间私单", "入账", "微信", "定金"),
            _example(2, "p2", "sex_work_related_income", "positive", "280", "2026-03-13 22:40:00", 22, 4, True, "包夜订单", "入账", "微信", "加钟"),
            _example(3, "n1", "not_sex_work_related_income", "negative", "12", "2026-03-12 11:11:00", 11, 3, False, "超市消费", "出账", "微信", "买菜"),
            _example(4, "n2", "not_sex_work_related_income", "negative", "18", "2026-03-13 09:40:00", 9, 4, False, "便利店", "出账", "支付宝", "早餐"),
        ]
        model = BaselineTextClassifier().fit(train_examples)

        positive_probe = _example(5, "probe-p", "", "unlabeled", "399", "2026-03-14 23:00:00", 23, 5, True, "夜间私单", "入账", "微信", "定金")
        negative_probe = _example(6, "probe-n", "", "unlabeled", "15", "2026-03-14 10:00:00", 10, 5, False, "超市消费", "出账", "支付宝", "买菜")

        self.assertEqual(model.predict(positive_probe), "positive")
        self.assertEqual(model.predict(negative_probe), "negative")

        metrics = model.evaluate([
            _example(7, "v1", "sex_work_related_income", "positive", "320", "2026-03-15 23:20:00", 23, 6, True, "包夜订单", "入账", "微信", "加钟"),
            _example(8, "v2", "not_sex_work_related_income", "negative", "20", "2026-03-15 09:20:00", 9, 6, False, "便利店", "出账", "支付宝", "早餐"),
        ])

        self.assertEqual(metrics["total"], 2)
        self.assertEqual(metrics["accuracy"], 1.0)
        self.assertEqual(metrics["confusion_matrix"]["tp"], 1)
        self.assertEqual(metrics["confusion_matrix"]["tn"], 1)

        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "baseline.json"
            model.save(path)
            loaded = BaselineTextClassifier.load(path)

        self.assertEqual(loaded.predict(positive_probe), "positive")
        self.assertEqual(loaded.predict(negative_probe), "negative")

    def test_global_classifier_and_triage_workbook_run(self) -> None:
        from txflow.labels import load_label_manifest

        project_root = Path(__file__).resolve().parents[1]
        manifests = [
            load_label_manifest(project_root / "data" / "labels" / "lixiaoxiao168_li_shaoyun_verified_positive.json"),
            load_label_manifest(project_root / "data" / "labels" / "lixiaoxiao168_li_shaoyun_verified_negative.json"),
        ]
        classifier = train_global_classifier(manifests)

        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "triage.xlsx"
            from zipfile import ZipFile

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
      <c r="C1" t="inlineStr"><is><t>交易时间</t></is></c>
      <c r="D1" t="inlineStr"><is><t>收款方的商户名称</t></is></c>
      <c r="E1" t="inlineStr"><is><t>备注</t></is></c>
    </row>
    <row r="2">
      <c r="A2" t="inlineStr"><is><t>2026031264155009238164850111202</t></is></c>
      <c r="B2" t="inlineStr"><is><t>388.00</t></is></c>
      <c r="C2" t="inlineStr"><is><t>2026-03-12 23:11:00</t></is></c>
      <c r="D2" t="inlineStr"><is><t>夜间私单</t></is></c>
      <c r="E2" t="inlineStr"><is><t>定金</t></is></c>
    </row>
    <row r="3">
      <c r="A3" t="inlineStr"><is><t>NEG-001</t></is></c>
      <c r="B3" t="inlineStr"><is><t>12.00</t></is></c>
      <c r="C3" t="inlineStr"><is><t>2026-03-12 11:11:00</t></is></c>
      <c r="D3" t="inlineStr"><is><t>超市消费</t></is></c>
      <c r="E3" t="inlineStr"><is><t>买菜</t></is></c>
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

            triage = triage_workbook(path, manifests, classifier=classifier)

        self.assertEqual(triage.total_rows, 2)
        self.assertIn(triage.verdict, {"high_confidence_positive", "needs_review", "high_confidence_negative"})
        self.assertEqual(len(triage.row_hits), 2)


if __name__ == "__main__":
    unittest.main()
