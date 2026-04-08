from __future__ import annotations

import csv
import json
import tempfile
import unittest
from pathlib import Path
from zipfile import ZipFile

from txflow.labels import load_label_manifest, load_label_manifests
from txflow.labels import LabelManifest
from txflow.training import (
    DatasetSplit,
    TrainingExample,
    build_positive_training_samples,
    build_training_examples,
    export_training_examples_csv,
    export_training_examples_jsonl,
    export_split_csv,
    export_split_jsonl,
    export_training_samples_csv,
    export_training_samples_jsonl,
    split_training_examples,
)
from txflow.excel import load_xlsx_rows
from txflow.rule_config import (
    DEFAULT_RULE_PATTERNS,
    detect_rule_signals,
    derive_trade_pattern,
    flow_family_for_trade_pattern,
    load_rule_patterns,
)


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
      <c r="C1" t="inlineStr"><is><t>交易时间</t></is></c>
      <c r="D1" t="inlineStr"><is><t>收款方的商户名称</t></is></c>
      <c r="E1" t="inlineStr"><is><t>备注</t></is></c>
    </row>
    <row r="2">
      <c r="A2" t="inlineStr"><is><t>2026031264155009238164850111202</t></is></c>
      <c r="B2" t="inlineStr"><is><t>388.00</t></is></c>
      <c r="C2" t="inlineStr"><is><t>2026-03-12 16:41:55</t></is></c>
      <c r="D2" t="inlineStr"><is><t>杨欠欠</t></is></c>
      <c r="E2" t="inlineStr"><is><t>测试备注</t></is></c>
    </row>
    <row r="3">
      <c r="A3" t="inlineStr"><is><t>UNLABELED-001</t></is></c>
      <c r="B3" t="inlineStr"><is><t>12.00</t></is></c>
      <c r="C3" t="inlineStr"><is><t>2026-03-12 17:00:00</t></is></c>
      <c r="D3" t="inlineStr"><is><t>其他商户</t></is></c>
      <c r="E3" t="inlineStr"><is><t>无</t></is></c>
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


class TrainingTests(unittest.TestCase):
    def test_rule_patterns_load_from_config_file(self) -> None:
        patterns = load_rule_patterns()

        self.assertIn("qr_hint_tokens", patterns)
        self.assertIn("platform_account_tokens", patterns)
        self.assertTrue(set(DEFAULT_RULE_PATTERNS).issubset(patterns))
        self.assertIn("扫码", patterns["qr_hint_tokens"]["common"])
        self.assertIn("碰一下", patterns["qr_hint_tokens"]["alipay"])

    def test_rule_config_detects_qr_and_platform_patterns(self) -> None:
        qr_signals = detect_rule_signals(
            channel="微信",
            tx_type="支付账户对支付账户转账",
            direction="出账",
            remark="备注：扫二维码付款-给芸;微信转账",
            merchant_name="王卫英(个人)",
            payer_account="085e9858e0b33a470bda9eb28@wx.tenpay.com",
            counterparty_account="085e9858e89ad7a7562b51b87@wx.tenpay.com",
        )
        platform_signals = detect_rule_signals(
            channel="支付宝",
            tx_type="支付账户消费",
            direction="入账",
            remark="-",
            merchant_name="陆平枝",
            payer_account="zxpayment@meituan.com",
            counterparty_account="zxpayment@meituan.com",
        )

        self.assertTrue(qr_signals.is_qr_transfer)
        self.assertIn("qr_hint", qr_signals.reason_tags)
        self.assertEqual(
            derive_trade_pattern(
                tx_type="支付账户对支付账户转账",
                is_qr_transfer=qr_signals.is_qr_transfer,
                is_red_packet=qr_signals.is_red_packet,
                is_failed_or_invalid=qr_signals.is_failed_or_invalid,
                is_withdrawal_like=qr_signals.is_withdrawal_like,
                is_merchant_consume=qr_signals.is_merchant_consume,
                is_platform_settlement=qr_signals.is_platform_settlement,
            ),
            "qr_p2p_transfer",
        )
        self.assertTrue(platform_signals.is_platform_settlement)
        self.assertIn("platform_account", platform_signals.reason_tags)
        self.assertEqual(
            derive_trade_pattern(
                tx_type="支付账户消费",
                is_qr_transfer=platform_signals.is_qr_transfer,
                is_red_packet=platform_signals.is_red_packet,
                is_failed_or_invalid=platform_signals.is_failed_or_invalid,
                is_withdrawal_like=platform_signals.is_withdrawal_like,
                is_merchant_consume=platform_signals.is_merchant_consume,
                is_platform_settlement=platform_signals.is_platform_settlement,
            ),
            "platform_settlement",
        )
        self.assertEqual(flow_family_for_trade_pattern("platform_settlement"), "platform_settlement")

    def test_xlsx_rows_are_loaded(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "sample.xlsx"
            _write_minimal_xlsx(path)

            rows = load_xlsx_rows(path)

        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["交易流水号"], "2026031264155009238164850111202")
        self.assertEqual(rows[1]["备注"], "无")

    def test_positive_samples_export_for_verified_manifest(self) -> None:
        manifest = load_label_manifest(LABEL_PATH)
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "sample.xlsx"
            csv_path = Path(temp_dir) / "samples.csv"
            jsonl_path = Path(temp_dir) / "samples.jsonl"
            _write_minimal_xlsx(path)

            samples = build_positive_training_samples(path, [manifest])
            export_training_samples_csv(samples, csv_path)
            export_training_samples_jsonl(samples, jsonl_path)

            with csv_path.open("r", encoding="utf-8", newline="") as handle:
                csv_rows = list(csv.DictReader(handle))
            json_rows = [json.loads(line) for line in jsonl_path.read_text(encoding="utf-8").splitlines() if line.strip()]

        self.assertEqual(len(samples), 1)
        self.assertEqual(samples[0].transaction_id, "2026031264155009238164850111202")
        self.assertEqual(samples[0].label, "sex_work_related_income")
        self.assertEqual(csv_rows[0]["subject"], "杨欠欠")
        self.assertEqual(json_rows[0]["remark"], "测试备注")

    def test_dataset_export_includes_unlabeled_rows(self) -> None:
        manifest = load_label_manifest(LABEL_PATH)
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "sample.xlsx"
            csv_path = Path(temp_dir) / "dataset.csv"
            jsonl_path = Path(temp_dir) / "dataset.jsonl"
            _write_minimal_xlsx(path)

            examples = build_training_examples(path, [manifest])
            export_training_examples_csv(examples, csv_path)
            export_training_examples_jsonl(examples, jsonl_path)

            with csv_path.open("r", encoding="utf-8", newline="") as handle:
                csv_rows = list(csv.DictReader(handle))
            json_rows = [json.loads(line) for line in jsonl_path.read_text(encoding="utf-8").splitlines() if line.strip()]

        self.assertEqual(len(examples), 2)
        self.assertEqual(examples[0].label_status, "positive")
        self.assertEqual(examples[1].label_status, "unlabeled")
        self.assertEqual(csv_rows[0]["label_status"], "positive")
        self.assertEqual(json_rows[1]["transaction_id"], "UNLABELED-001")

    def test_positive_and_negative_examples_are_both_labeled(self) -> None:
        positive_manifest = load_label_manifest(LI_SHAOYUN_POSITIVE_PATH)
        negative_manifest = load_label_manifest(LI_SHAOYUN_NEGATIVE_PATH)
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "sample.xlsx"
            _write_minimal_xlsx(path)

            # overwrite the second row so it matches the negative manifest
            with ZipFile(path, "w") as archive:
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
                sheet_xml = f"""<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
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
      <c r="A2" t="inlineStr"><is><t>{positive_manifest.transaction_ids[0]}</t></is></c>
      <c r="B2" t="inlineStr"><is><t>388.00</t></is></c>
      <c r="C2" t="inlineStr"><is><t>2026-03-12 16:41:55</t></is></c>
      <c r="D2" t="inlineStr"><is><t>杨欠欠</t></is></c>
      <c r="E2" t="inlineStr"><is><t>测试备注</t></is></c>
    </row>
    <row r="3">
      <c r="A3" t="inlineStr"><is><t>{negative_manifest.transaction_ids[0]}</t></is></c>
      <c r="B3" t="inlineStr"><is><t>12.00</t></is></c>
      <c r="C3" t="inlineStr"><is><t>2026-03-12 17:00:00</t></is></c>
      <c r="D3" t="inlineStr"><is><t>其他商户</t></is></c>
      <c r="E3" t="inlineStr"><is><t>无</t></is></c>
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
                archive.writestr("[Content_Types].xml", content_types)
                archive.writestr("_rels/.rels", root_rels)
                archive.writestr("xl/workbook.xml", workbook_xml)
                archive.writestr("xl/_rels/workbook.xml.rels", rels_xml)
                archive.writestr("xl/worksheets/sheet1.xml", sheet_xml)

            examples = build_training_examples(path, [positive_manifest, negative_manifest])

        self.assertEqual(len(examples), 2)

    def test_build_training_examples_derives_trade_pattern_fields(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "085e9858e0b33a470bda9eb28@wx.tenpay.com.xlsx"
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
      <c r="B1" t="inlineStr"><is><t>付款支付帐号</t></is></c>
      <c r="C1" t="inlineStr"><is><t>收款支付帐号</t></is></c>
      <c r="D1" t="inlineStr"><is><t>交易金额</t></is></c>
      <c r="E1" t="inlineStr"><is><t>交易时间</t></is></c>
      <c r="F1" t="inlineStr"><is><t>交易类型</t></is></c>
      <c r="G1" t="inlineStr"><is><t>交易主体的出入账标识</t></is></c>
      <c r="H1" t="inlineStr"><is><t>收款方的商户名称</t></is></c>
      <c r="I1" t="inlineStr"><is><t>备注</t></is></c>
    </row>
    <row r="2">
      <c r="A2" t="inlineStr"><is><t>TX-QR-1</t></is></c>
      <c r="B2" t="inlineStr"><is><t>085e9858e0b33a470bda9eb28@wx.tenpay.com</t></is></c>
      <c r="C2" t="inlineStr"><is><t>085e9858e89ad7a7562b51b87@wx.tenpay.com</t></is></c>
      <c r="D2" t="inlineStr"><is><t>23.00</t></is></c>
      <c r="E2" t="inlineStr"><is><t>2026-03-19 12:52:22</t></is></c>
      <c r="F2" t="inlineStr"><is><t>支付账户对支付账户转账</t></is></c>
      <c r="G2" t="inlineStr"><is><t>出账</t></is></c>
      <c r="H2" t="inlineStr"><is><t>王卫英(个人)</t></is></c>
      <c r="I2" t="inlineStr"><is><t>备注：扫二维码付款-给芸;微信转账</t></is></c>
    </row>
    <row r="3">
      <c r="A3" t="inlineStr"><is><t>TX-WD-1</t></is></c>
      <c r="B3" t="inlineStr"><is><t>085e9858e0b33a470bda9eb28@wx.tenpay.com</t></is></c>
      <c r="C3" t="inlineStr"><is><t>085e9858e0b33a470bda9eb28@wx.tenpay.com</t></is></c>
      <c r="D3" t="inlineStr"><is><t>225.00</t></is></c>
      <c r="E3" t="inlineStr"><is><t>2026-03-12 22:05:52</t></is></c>
      <c r="F3" t="inlineStr"><is><t>支付账户提现/转账至银行卡</t></is></c>
      <c r="G3" t="inlineStr"><is><t>出账</t></is></c>
      <c r="H3" t="inlineStr"><is><t>陶照亮(个人)</t></is></c>
      <c r="I3" t="inlineStr"><is><t>网银联单号类型：网联</t></is></c>
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

            examples = build_training_examples(path, [])

        self.assertEqual(len(examples), 2)
        self.assertEqual(examples[0].subject_account, "085e9858e0b33a470bda9eb28@wx.tenpay.com")
        self.assertEqual(examples[0].trade_pattern, "qr_p2p_transfer")
        self.assertTrue(examples[0].is_qr_transfer)
        self.assertTrue(examples[0].is_trade_like)
        self.assertEqual(examples[0].buyer_account, "085e9858e0b33a470bda9eb28@wx.tenpay.com")
        self.assertEqual(examples[0].seller_account, "085e9858e89ad7a7562b51b87@wx.tenpay.com")
        self.assertEqual(examples[1].trade_pattern, "withdraw_to_bank")
        self.assertTrue(examples[1].is_withdrawal_like)
        self.assertFalse(examples[1].is_trade_like)

    def test_conflicting_manifests_raise_error(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "sample.xlsx"
            _write_minimal_xlsx(path)
            positive_manifest = LabelManifest(
                dataset_name="conflict_positive",
                label="high_risk_transaction",
                subject="subject_a",
                status="verified",
                source_file=str(path),
                transaction_ids=["2026031264155009238164850111202"],
                polarity="positive",
            )
            negative_manifest = LabelManifest(
                dataset_name="conflict_negative",
                label="low_risk_transaction",
                subject="subject_b",
                status="verified",
                source_file=str(path),
                transaction_ids=["2026031264155009238164850111202"],
                polarity="negative",
            )

            with self.assertRaisesRegex(ValueError, "conflicting label manifests detected"):
                build_training_examples(path, [positive_manifest, negative_manifest])

    def test_split_training_examples_partitions_deterministically(self) -> None:
        examples = [
            TrainingExample(1, "a", "x", "positive", "S", "F", "1", "t", 1, 1, False, "c", "d", "ch", "r", {}),
            TrainingExample(2, "b", "", "unlabeled", "", "", "", "", None, None, False, "", "", "", "", {}),
            TrainingExample(3, "c", "y", "negative", "S", "F", "2", "t", 2, 1, False, "c", "d", "ch", "r", {}),
            TrainingExample(4, "d", "", "unlabeled", "", "", "", "", None, None, False, "", "", "", "", {}),
        ]
        splits = split_training_examples(examples, train_ratio=0.5, seed=7)
        split_map = {split.name: split for split in splits}

        self.assertEqual(sum(len(split.examples) for split in splits), 4)
        self.assertEqual({item.label_status for item in split_map["train"].examples} | {item.label_status for item in split_map["validation"].examples}, {"positive", "negative", "unlabeled"})
        self.assertGreaterEqual(len(split_map["train"].examples), 2)

    def test_split_export_helpers_write_files(self) -> None:
        examples = [
            TrainingExample(1, "a", "x", "positive", "S", "F", "1", "t", 1, 1, False, "c", "d", "ch", "r", {}),
            TrainingExample(2, "b", "", "unlabeled", "", "", "", "", None, None, False, "", "", "", "", {}),
        ]
        split = DatasetSplit(name="train", examples=examples)
        with tempfile.TemporaryDirectory() as temp_dir:
            csv_path = Path(temp_dir) / "train.csv"
            jsonl_path = Path(temp_dir) / "train.jsonl"
            export_split_csv(split, csv_path)
            export_split_jsonl(split, jsonl_path)

            self.assertTrue(csv_path.exists())
            self.assertTrue(jsonl_path.exists())


if __name__ == "__main__":
    unittest.main()
