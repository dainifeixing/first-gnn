from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any
from xml.etree import ElementTree as ET
from zipfile import ZipFile
from xml.sax.saxutils import escape


NS = {"a": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
REL_NS = "{http://schemas.openxmlformats.org/officeDocument/2006/relationships}"


@dataclass(frozen=True)
class StyledRow:
    values: dict[str, str]
    fill_label: str = ""


def _col_to_index(cell_ref: str) -> int:
    letters = "".join(ch for ch in cell_ref if ch.isalpha()).upper()
    total = 0
    for ch in letters:
        total = total * 26 + (ord(ch) - ord("A") + 1)
    return total - 1


def _index_to_col(index: int) -> str:
    value = index + 1
    letters: list[str] = []
    while value > 0:
        value, remainder = divmod(value - 1, 26)
        letters.append(chr(65 + remainder))
    return "".join(reversed(letters))


def _cell_text(cell: ET.Element, shared_strings: list[str]) -> str:
    cell_type = cell.attrib.get("t", "")
    if cell_type == "inlineStr":
        return "".join(node.text or "" for node in cell.findall(".//a:t", NS))
    value = cell.find("a:v", NS)
    if value is None or value.text is None:
        return ""
    if cell_type == "s" and value.text.isdigit():
        index = int(value.text)
        if 0 <= index < len(shared_strings):
            return shared_strings[index]
    return value.text


def _load_shared_strings(archive: ZipFile) -> list[str]:
    shared_strings: list[str] = []
    if "xl/sharedStrings.xml" not in archive.namelist():
        return shared_strings
    shared_root = ET.fromstring(archive.read("xl/sharedStrings.xml"))
    for item in shared_root.findall(".//a:si", NS):
        shared_strings.append("".join(node.text or "" for node in item.findall(".//a:t", NS)))
    return shared_strings


def _resolve_sheet_path(archive: ZipFile, sheet_name: str | None = None) -> str:
    workbook = ET.fromstring(archive.read("xl/workbook.xml"))
    rels = ET.fromstring(archive.read("xl/_rels/workbook.xml.rels"))
    rel_map: dict[str, str] = {}
    for rel in rels:
        rel_map[rel.attrib["Id"]] = rel.attrib["Target"]
    for sheet in workbook.findall(".//a:sheets/a:sheet", NS):
        name = sheet.attrib.get("name", "")
        rel_id = sheet.attrib.get(REL_NS + "id", "")
        if sheet_name is None or name == sheet_name:
            target = rel_map.get(rel_id)
            if target:
                return f"xl/{target.lstrip('/')}"
    raise FileNotFoundError(f"sheet not found: {sheet_name or 'first sheet'}")


def _classify_rgb(rgb: str) -> str:
    normalized = str(rgb).strip().lstrip("#").upper()
    if len(normalized) == 8:
        normalized = normalized[2:]
    if len(normalized) != 6:
        return ""
    try:
        red = int(normalized[0:2], 16)
        green = int(normalized[2:4], 16)
        blue = int(normalized[4:6], 16)
    except ValueError:
        return ""
    if red >= 200 and green < 160 and blue < 160:
        return "red"
    if red >= 200 and green >= 200 and blue < 170:
        return "yellow"
    return ""


def _load_style_fill_labels(archive: ZipFile) -> dict[int, str]:
    if "xl/styles.xml" not in archive.namelist():
        return {}
    root = ET.fromstring(archive.read("xl/styles.xml"))
    fills = root.find("a:fills", NS)
    fill_labels: dict[int, str] = {}
    if fills is not None:
        for fill_index, fill in enumerate(fills.findall("a:fill", NS)):
            pattern_fill = fill.find("a:patternFill", NS)
            if pattern_fill is None:
                continue
            fg = pattern_fill.find("a:fgColor", NS)
            bg = pattern_fill.find("a:bgColor", NS)
            label = ""
            if fg is not None:
                label = _classify_rgb(fg.attrib.get("rgb", ""))
            if not label and bg is not None:
                label = _classify_rgb(bg.attrib.get("rgb", ""))
            if label:
                fill_labels[fill_index] = label

    style_labels: dict[int, str] = {}
    cell_xfs = root.find("a:cellXfs", NS)
    if cell_xfs is None:
        return style_labels
    for style_index, xf in enumerate(cell_xfs.findall("a:xf", NS)):
        fill_id = int(xf.attrib.get("fillId", "0"))
        label = fill_labels.get(fill_id, "")
        if label:
            style_labels[style_index] = label
    return style_labels


def load_xlsx_styled_rows(path: str | Path, sheet_name: str | None = None) -> list[StyledRow]:
    source = Path(path)
    with ZipFile(source) as archive:
        shared_strings = _load_shared_strings(archive)
        style_labels = _load_style_fill_labels(archive)
        sheet_path = _resolve_sheet_path(archive, sheet_name=sheet_name)
        root = ET.fromstring(archive.read(sheet_path))

        rows: list[StyledRow] = []
        headers: list[str] = []
        for row in root.findall(".//a:sheetData/a:row", NS):
            cells: dict[int, str] = {}
            row_fill = ""
            for cell in row.findall("a:c", NS):
                ref = cell.attrib.get("r", "")
                if not ref:
                    continue
                cells[_col_to_index(ref)] = _cell_text(cell, shared_strings).strip()
                style_index = int(cell.attrib.get("s", "0"))
                fill_label = style_labels.get(style_index, "")
                if fill_label == "red":
                    row_fill = "red"
                elif fill_label == "yellow" and not row_fill:
                    row_fill = "yellow"

            if not headers:
                max_index = max(cells.keys(), default=-1)
                headers = [cells.get(i, "") for i in range(max_index + 1)]
                continue

            row_data: dict[str, str] = {}
            max_index = max(max(cells.keys(), default=-1), len(headers) - 1)
            for index in range(max_index + 1):
                header = headers[index] if index < len(headers) else f"col_{index + 1}"
                if not header:
                    header = f"col_{index + 1}"
                row_data[header] = cells.get(index, "")
            if any(value for value in row_data.values()):
                rows.append(StyledRow(values=row_data, fill_label=row_fill))
        return rows


def load_xlsx_rows(path: str | Path, sheet_name: str | None = None) -> list[dict[str, str]]:
    return [item.values for item in load_xlsx_styled_rows(path, sheet_name=sheet_name)]


def write_xlsx_table(
    path: str | Path,
    headers: list[str],
    rows: list[dict[str, Any]],
    row_fills: list[str] | None = None,
    sheet_name: str = "Sheet1",
) -> Path:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    normalized_fills = list(row_fills or [])
    while len(normalized_fills) < len(rows):
        normalized_fills.append("")

    style_map = {"": "", "yellow": ' s="1"', "red": ' s="2"'}
    row_xml: list[str] = []
    for row_index, row in enumerate(rows, start=2):
        fill_style = style_map.get(normalized_fills[row_index - 2], "")
        cells: list[str] = []
        for col_index, header in enumerate(headers):
            ref = f"{_index_to_col(col_index)}{row_index}"
            value = escape(str(row.get(header, "")))
            cells.append(f'<c r="{ref}" t="inlineStr"{fill_style}><is><t>{value}</t></is></c>')
        row_xml.append(f'<row r="{row_index}">{"".join(cells)}</row>')

    header_cells = []
    for index, header in enumerate(headers):
        ref = f"{_index_to_col(index)}1"
        header_cells.append(f'<c r="{ref}" t="inlineStr"><is><t>{escape(header)}</t></is></c>')
    sheet_rows = [f'<row r="1">{"".join(header_cells)}</row>'] + row_xml

    workbook_xml = f"""<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"
          xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">
  <sheets>
    <sheet name="{escape(sheet_name)}" sheetId="1" r:id="rId1"/>
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
    {rows}
  </sheetData>
</worksheet>
""".format(rows="\n".join(sheet_rows))
    styles_xml = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<styleSheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">
  <fonts count="1">
    <font><sz val="11"/><name val="Calibri"/></font>
  </fonts>
  <fills count="4">
    <fill><patternFill patternType="none"/></fill>
    <fill><patternFill patternType="gray125"/></fill>
    <fill><patternFill patternType="solid"><fgColor rgb="FFFFFF00"/><bgColor indexed="64"/></patternFill></fill>
    <fill><patternFill patternType="solid"><fgColor rgb="FFFF0000"/><bgColor indexed="64"/></patternFill></fill>
  </fills>
  <borders count="1">
    <border><left/><right/><top/><bottom/><diagonal/></border>
  </borders>
  <cellStyleXfs count="1">
    <xf numFmtId="0" fontId="0" fillId="0" borderId="0"/>
  </cellStyleXfs>
  <cellXfs count="3">
    <xf numFmtId="0" fontId="0" fillId="0" borderId="0" xfId="0"/>
    <xf numFmtId="0" fontId="0" fillId="2" borderId="0" xfId="0" applyFill="1"/>
    <xf numFmtId="0" fontId="0" fillId="3" borderId="0" xfId="0" applyFill="1"/>
  </cellXfs>
</styleSheet>
"""
    content_types = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">
  <Default Extension="xml" ContentType="application/xml"/>
  <Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>
  <Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>
  <Override PartName="/xl/styles.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml"/>
</Types>
"""
    root_rels = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/>
</Relationships>
"""
    with ZipFile(target, "w") as archive:
        archive.writestr("[Content_Types].xml", content_types)
        archive.writestr("_rels/.rels", root_rels)
        archive.writestr("xl/workbook.xml", workbook_xml)
        archive.writestr("xl/_rels/workbook.xml.rels", rels_xml)
        archive.writestr("xl/worksheets/sheet1.xml", sheet_xml)
        archive.writestr("xl/styles.xml", styles_xml)
    return target
