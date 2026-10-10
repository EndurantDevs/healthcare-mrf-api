# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Decode the inspected HIOS workbook layout into bounded native input JSON.

This is an input decoder, not issuer/company resolution or database admission.
The caller supplies the independently verified enclosing artifact digest and
must exhaust the iterator before accepting a complete edition. Dates and source
company identifiers remain raw evidence; labels confer no ownership authority.
"""

import hashlib
import io
import json
import re
import zipfile
from decimal import Decimal, InvalidOperation
from pathlib import Path
from xml.etree import ElementTree

HEADERS = (
    "hios_issuer_id",
    "issr_lgl_name",
    "marketingname",
    "state",
    "individualmarket",
    "smallgroupmarket",
    "unknownmarket",
    "largemarket",
    "federal_ein",
    "active",
    "datecreated",
    "lastmodifieddate",
    "databasecompanyid",
    "org_adr1",
    "org_adr2",
    "org_city",
    "org_state",
    "org_zip",
    "org_zip4",
)
LAYOUT = "hios-2026-08-06-issuer-v1"
MAX_WORKBOOK_BYTES = 16 * 1024 * 1024
MAX_XML_BYTES = 8 * 1024 * 1024
MAX_BATCH_BYTES = 8 * 1024 * 1024
MAX_SHARED_STRINGS = 128_000
MAX_ROWS = 100_000
_NS = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"
_REL_NS = "{http://schemas.openxmlformats.org/package/2006/relationships}"
_DOCUMENT_REL = "{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id"
_DIGEST = re.compile(r"[0-9a-f]{64}\Z")
_CELL = re.compile(r"([A-S])([1-9][0-9]*)\Z")
_IDENTIFIER_WIDTHS = {0: 5, 8: 9, 12: None, 17: 5, 18: 4}


class PlanFinderWorkbookError(ValueError):
    """The complete workbook cannot be safely decoded under the pinned layout."""


def _require(condition, message):
    if not condition:
        raise PlanFinderWorkbookError(message)


def _checked_xml(archive, name, *, maximum=MAX_XML_BYTES):
    """Validate bounded XML bytes before parsing, including entity declarations."""
    _require(name in archive.namelist(), "Required workbook XML is missing")
    _require(archive.getinfo(name).file_size <= maximum, "Workbook XML exceeds its limit")
    tail = b""
    with archive.open(name) as stream:
        while chunk := stream.read(64 * 1024):
            scan = tail + chunk
            _require(b"\0" not in scan, "Only UTF8 workbook XML is supported")
            _require(b"<!DOCTYPE" not in scan and b"<!ENTITY" not in scan, "XML declarations are forbidden")
            tail = scan[-16:]


def _document(archive, name):
    _checked_xml(archive, name, maximum=1024 * 1024)
    return ElementTree.fromstring(archive.read(name))


def _issuer_sheet(archive):
    workbook = _document(archive, "xl/workbook.xml")
    settings = workbook.find(_NS + "workbookPr")
    _require(settings is None or settings.get("date1904", "0") in ("0", "false"), "Unexpected date system")
    sheets = [sheet for sheet in workbook.findall(_NS + "sheets/" + _NS + "sheet") if sheet.get("name") == "ISSUER_1"]
    _require(len(sheets) == 1, "Exact ISSUER_1 sheet is required")
    relationships = _document(archive, "xl/_rels/workbook.xml.rels")
    matches = [
        item
        for item in relationships.findall(_REL_NS + "Relationship")
        if item.get("Id") == sheets[0].get(_DOCUMENT_REL)
    ]
    _require(len(matches) == 1, "Issuer worksheet relationship is ambiguous")
    relationship = matches[0]
    _require(relationship.get("TargetMode") is None, "External worksheet relationships are forbidden")
    _require(relationship.get("Type", "").endswith("/worksheet"), "Issuer relationship is not a worksheet")
    _require(relationship.get("Target") == "worksheets/sheet1.xml", "Unexpected issuer worksheet layout")
    styles = _document(archive, "xl/styles.xml")
    formats = [item.get("numFmtId") for item in styles.findall(_NS + "cellXfs/" + _NS + "xf")]
    _require(formats == ["0", "14", "6"], "Unexpected workbook numeric/date styles")
    return "xl/worksheets/sheet1.xml"


def _shared_strings(archive):
    name = "xl/sharedStrings.xml"
    _checked_xml(archive, name)
    strings = []
    text_bytes = 0
    with archive.open(name) as stream:
        events = ElementTree.iterparse(stream, events=("start", "end"))
        _, root = next(events)
        for event, item in events:
            if event != "end" or item.tag != _NS + "si":
                continue
            value = "".join(element.text or "" for element in item.iter(_NS + "t"))
            encoded_bytes = len(value.encode("utf-8"))
            text_bytes += encoded_bytes
            _require(encoded_bytes <= 32_768 and text_bytes <= MAX_XML_BYTES, "Shared string text exceeds its limit")
            _require(len(strings) < MAX_SHARED_STRINGS, "Too many workbook shared strings")
            strings.append(value)
            root.clear()
    return strings


def _cell_value(cell, shared):
    _require(cell.find(_NS + "f") is None, "Formula cells are unsupported")
    cell_type = cell.get("t", "n")
    _require(cell_type in ("s", "n", "inlineStr"), "Unsupported issuer cell type")
    style = cell.get("s", "0")
    _require(style in ("0", "1", "2"), "Unsupported issuer cell style")
    value = cell.findtext(_NS + "v")
    if cell_type == "s":
        _require(
            value is not None and len(value) <= 6 and value.isascii() and value.isdecimal(),
            "Invalid shared string reference",
        )
        _require(int(value) < len(shared), "Shared string reference is outside the workbook")
        value = shared[int(value)]
    elif cell_type == "inlineStr":
        inline = cell.find(_NS + "is")
        _require(inline is not None, "Inline string content is missing")
        value = "".join(element.text or "" for element in inline.iter(_NS + "t"))
    elif value is not None:
        try:
            _require(Decimal(value).is_finite(), "Nonfinite numeric cell")
        except InvalidOperation as exc:
            raise PlanFinderWorkbookError("Malformed numeric cell") from exc
    _require(value is None or len(value.encode("utf-8")) <= 32_768, "Issuer cell text exceeds its limit")
    return value, cell_type, int(style)


def _numeric_identifier(value, width):
    if value is None:
        return None
    number = Decimal(value)
    if number < 0 or number != number.to_integral_value() or number >= 10**20:
        return value
    result = str(int(number))
    return result.zfill(width) if width is not None else result


def _decode_row(row, shared, expected_row):
    _require(row.get("r") == str(expected_row), "Noncontiguous issuer row positions")
    _require(all(child.tag == _NS + "c" for child in row), "Unsupported issuer row contents")
    raw_values = [None] * len(HEADERS)
    cell_types = [None] * len(HEADERS)
    style_ids = [None] * len(HEADERS)
    seen_columns = set()
    for cell in row.findall(_NS + "c"):
        coordinate = _CELL.fullmatch(cell.get("r", ""))
        _require(coordinate is not None and int(coordinate[2]) == expected_row, "Invalid issuer cell coordinate")
        index = ord(coordinate[1]) - ord("A")
        _require(index not in seen_columns, "Duplicate issuer cell coordinate")
        seen_columns.add(index)
        raw_values[index], cell_types[index], style_ids[index] = _cell_value(cell, shared)
    if expected_row == 1:
        _require(tuple(raw_values) == HEADERS, "Issuer header does not match the inspected layout")
        return None
    values = raw_values.copy()
    for index, width in _IDENTIFIER_WIDTHS.items():
        if cell_types[index] == "n":
            values[index] = _numeric_identifier(raw_values[index], width)
    return {
        "source_row": expected_row,
        "values": values,
        "raw_values": raw_values,
        "cell_types": cell_types,
        "style_ids": style_ids,
    }


def _issuer_rows(archive, shared, sheet):
    _checked_xml(archive, sheet)
    expected_row = 1
    sheet_data = None
    parent_tags = []
    with archive.open(sheet) as stream:
        for event, item in ElementTree.iterparse(stream, events=("start", "end")):
            if event == "start":
                parent_tags.append(item.tag)
            if event == "start" and item.tag == _NS + "sheetData":
                _require(sheet_data is None, "Duplicate issuer sheet data")
                sheet_data = item
            if event != "end":
                continue
            parent_tags.pop()
            if item.tag != _NS + "row":
                continue
            _require(parent_tags == [_NS + "worksheet", _NS + "sheetData"], "Unexpected issuer row location")
            _require(sheet_data is not None and expected_row <= MAX_ROWS + 1, "Issuer row limit exceeded")
            decoded = _decode_row(item, shared, expected_row)
            expected_row += 1
            if decoded is not None:
                yield decoded
            sheet_data.clear()
    _require(expected_row > 1, "Issuer header is missing")


def _workbook_bytes(workbook_path):
    chunks = []
    size = 0
    with Path(workbook_path).open("rb") as stream:
        while chunk := stream.read(64 * 1024):
            size += len(chunk)
            _require(size <= MAX_WORKBOOK_BYTES, "Workbook exceeds its byte limit")
            chunks.append(chunk)
    return b"".join(chunks)


def _batch_prefix(workbook_sha256, artifact_sha256):
    metadata = {
        "component": "cms_planfinder_workbook_input",
        "revision": 1,
        "layout": LAYOUT,
        "artifact_sha256": artifact_sha256,
        "workbook_sha256": workbook_sha256,
        "sheet": "ISSUER_1",
        "headers": HEADERS,
    }
    return (
        json.dumps(metadata, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")[:-1]
        + b',"rows":['
    )


def iter_cms_planfinder_issuer_batches(workbook_path, *, expected_workbook_sha256, artifact_sha256, batch_size=5000):
    """Yield <=5000 rows and <=8 MiB JSON per batch, retaining exact cell evidence.

    Numeric identifier presentation is normalized without removing raw lexical
    values or cell types. Invalid identifier semantics remain for the native
    normalizer to reject with row accounting. PRODUCT_1 is never decoded.
    """
    _require(
        isinstance(batch_size, int) and not isinstance(batch_size, bool) and 1 <= batch_size <= 5000,
        "Batch size must be between 1 and 5000",
    )
    for digest in (expected_workbook_sha256, artifact_sha256):
        _require(isinstance(digest, str) and _DIGEST.fullmatch(digest), "Canonical SHA256 pins are required")
    workbook = _workbook_bytes(workbook_path)
    _require(hashlib.sha256(workbook).hexdigest() == expected_workbook_sha256, "Workbook SHA256 does not match its pin")
    prefix = _batch_prefix(expected_workbook_sha256, artifact_sha256)
    try:
        with zipfile.ZipFile(io.BytesIO(workbook)) as archive:
            members = archive.infolist()
            _require(
                len(members) <= 256 and len({member.filename for member in members}) == len(members),
                "Workbook ZIP membership is invalid",
            )
            _require(
                sum(member.file_size for member in members) <= 64 * 1024 * 1024,
                "Workbook expanded size exceeds its limit",
            )
            _require(all(not member.flag_bits & 1 for member in members), "Encrypted workbook members are forbidden")
            sheet = _issuer_sheet(archive)
            shared = _shared_strings(archive)
            encoded_records = []
            size = len(prefix) + 2
            for issuer_record in _issuer_rows(archive, shared, sheet):
                encoded = json.dumps(issuer_record, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode(
                    "utf-8"
                )
                _require(len(prefix) + len(encoded) + 2 <= MAX_BATCH_BYTES, "One issuer row exceeds the batch limit")
                if encoded_records and (
                    len(encoded_records) >= batch_size or size + len(encoded) + 1 > MAX_BATCH_BYTES
                ):
                    yield prefix + b",".join(encoded_records) + b"]}"
                    encoded_records = []
                    size = len(prefix) + 2
                size += len(encoded) + bool(encoded_records)
                encoded_records.append(encoded)
            if encoded_records:
                yield prefix + b",".join(encoded_records) + b"]}"
    except (zipfile.BadZipFile, ElementTree.ParseError, KeyError, RuntimeError, EOFError) as exc:
        raise PlanFinderWorkbookError("Workbook container or XML is malformed") from exc
