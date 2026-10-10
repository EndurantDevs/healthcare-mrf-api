# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Synthetic workbooks retain the inspected issuer sheet/header/cell layout."""

import hashlib
import io
import json
import tracemalloc
import zipfile
from xml.etree import ElementTree

import pytest

from process import cms_planfinder_workbook_input as decoder

_NS = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
_REL = "http://schemas.openxmlformats.org/package/2006/relationships"
_DOCUMENT_REL = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"
_ARTIFACT = "a" * 64


def _add_issuer_row(sheet_data, number, overrides):
    issuer_row = ElementTree.SubElement(sheet_data, f"{{{_NS}}}row", r=str(number))
    cells_by_column = {
        0: ("n", "1234", "0"),
        1: ("s", "19", "0"),
        2: ("s", "20", "0"),
        3: ("s", "21", "0"),
        4: ("s", "22", "0"),
        5: ("s", "23", "0"),
        8: ("n", "12345678", "0"),
        9: ("s", "22", "0"),
        10: ("n", "46000.5", "1"),
        12: ("n", "37.0", "0"),
        17: ("n", "1234", "0"),
        18: ("n", "12", "0"),
    }
    cells_by_column.update(overrides or {})
    for index, (cell_type, cell_text, style) in cells_by_column.items():
        cell = ElementTree.SubElement(issuer_row, f"{{{_NS}}}c", r=f"{chr(65 + index)}{number}", t=cell_type, s=style)
        if cell_type == "inlineStr":
            ElementTree.SubElement(ElementTree.SubElement(cell, f"{{{_NS}}}is"), f"{{{_NS}}}t").text = cell_text
        elif cell_text is not None:
            ElementTree.SubElement(cell, f"{{{_NS}}}v").text = cell_text


def _synthetic_documents(count, overrides):
    shared_strings = list(decoder.HEADERS) + ["Example Legal Company", "Example Label", "CA", "YES", "NO"]
    worksheet = ElementTree.Element(f"{{{_NS}}}worksheet")
    sheet_data = ElementTree.SubElement(worksheet, f"{{{_NS}}}sheetData")
    header = ElementTree.SubElement(sheet_data, f"{{{_NS}}}row", r="1")
    for index in range(19):
        cell = ElementTree.SubElement(header, f"{{{_NS}}}c", r=f"{chr(65 + index)}1", t="s")
        ElementTree.SubElement(cell, f"{{{_NS}}}v").text = str(index)
    for number in range(2, count + 2):
        _add_issuer_row(sheet_data, number, overrides)
    shared = ElementTree.Element(f"{{{_NS}}}sst")
    for shared_text in shared_strings:
        ElementTree.SubElement(ElementTree.SubElement(shared, f"{{{_NS}}}si"), f"{{{_NS}}}t").text = shared_text
    workbook = ElementTree.Element(f"{{{_NS}}}workbook")
    ElementTree.SubElement(workbook, f"{{{_NS}}}workbookPr", date1904="0")
    sheets = ElementTree.SubElement(workbook, f"{{{_NS}}}sheets")
    ElementTree.SubElement(
        sheets, f"{{{_NS}}}sheet", name="ISSUER_1", sheetId="1", attrib={f"{{{_DOCUMENT_REL}}}id": "rId1"}
    )
    relationships = ElementTree.Element(f"{{{_REL}}}Relationships")
    ElementTree.SubElement(
        relationships,
        f"{{{_REL}}}Relationship",
        Id="rId1",
        Type=f"{_DOCUMENT_REL}/worksheet",
        Target="worksheets/sheet1.xml",
    )
    styles = ElementTree.Element(f"{{{_NS}}}styleSheet")
    formats = ElementTree.SubElement(styles, f"{{{_NS}}}cellXfs")
    for number in ("0", "14", "6"):
        ElementTree.SubElement(formats, f"{{{_NS}}}xf", numFmtId=number)
    return {
        "xl/workbook.xml": workbook,
        "xl/_rels/workbook.xml.rels": relationships,
        "xl/styles.xml": styles,
        "xl/sharedStrings.xml": shared,
        "xl/worksheets/sheet1.xml": worksheet,
    }


def _workbook(tmp_path, *, count=3, overrides=None, mutation=None):
    documents_by_path = _synthetic_documents(count, overrides)
    if mutation is not None:
        mutation(documents_by_path)
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for name, document in documents_by_path.items():
            archive.writestr(name, document if isinstance(document, bytes) else ElementTree.tostring(document))
        # An unrelated sheet is never parsed, including its invalid XML.
        archive.writestr("xl/worksheets/sheet2.xml", b"not issuer XML")
    workbook_bytes = output.getvalue()
    path = tmp_path / "synthetic-hios.xlsx"
    path.write_bytes(workbook_bytes)
    return path, hashlib.sha256(workbook_bytes).hexdigest()


def _batches(path, digest, **kwargs):
    return list(
        decoder.iter_cms_planfinder_issuer_batches(
            path, expected_workbook_sha256=digest, artifact_sha256=_ARTIFACT, **kwargs
        )
    )


def test_exact_layout_bounded_batches_and_raw_identifier_date_evidence(tmp_path):
    path, digest = _workbook(tmp_path, count=5)
    encoded = _batches(path, digest, batch_size=2)
    batches = [json.loads(batch) for batch in encoded]
    assert [len(batch["rows"]) for batch in batches] == [2, 2, 1]
    assert [row["source_row"] for batch in batches for row in batch["rows"]] == [2, 3, 4, 5, 6]
    for batch in batches:
        assert set(batch) == {
            "component",
            "revision",
            "layout",
            "artifact_sha256",
            "workbook_sha256",
            "sheet",
            "headers",
            "rows",
        }
        assert batch["headers"] == list(decoder.HEADERS) and batch["sheet"] == "ISSUER_1"
        assert batch["workbook_sha256"] == digest and batch["artifact_sha256"] == _ARTIFACT
        assert batch["layout"] == decoder.LAYOUT and batch["revision"] == 1
    row = batches[0]["rows"][0]
    assert set(row) == {"source_row", "values", "raw_values", "cell_types", "style_ids"}
    assert row["values"][0] == "01234" and row["raw_values"][0] == "1234" and row["cell_types"][0] == "n"
    assert row["values"][8] == "012345678" and row["values"][17:19] == ["01234", "0012"]
    assert row["values"][12] == "37" and row["raw_values"][12] == "37.0"
    assert row["values"][10] == "46000.5" and row["style_ids"][10] == 1
    assert row["values"][11] is None and row["cell_types"][11] is None
    assert encoded == _batches(path, digest, batch_size=2)


@pytest.mark.parametrize(
    "value,expected",
    [("1.234E3", "01234"), ("1234.0", "01234"), ("1234.5", "1234.5"), ("-12", "-12"), ("123456", "123456")],
)
def test_numeric_semantics_preserve_invalid_rows_for_native_accounting(tmp_path, value, expected):
    path, digest = _workbook(tmp_path, count=1, overrides={0: ("n", value, "0")})
    row = json.loads(_batches(path, digest)[0])["rows"][0]
    assert row["values"][0] == expected and row["raw_values"][0] == value


def test_text_leading_zero_and_sparse_missing_field_evidence(tmp_path):
    path, digest = _workbook(tmp_path, count=1, overrides={0: ("inlineStr", "00123", "0"), 8: ("n", None, "0")})
    row = json.loads(_batches(path, digest)[0])["rows"][0]
    assert row["values"][0] == row["raw_values"][0] == "00123" and row["cell_types"][0] == "inlineStr"
    assert row["values"][8] is None and row["values"][13] is None


@pytest.mark.parametrize("batch_size", [0, 5001, True, 1.5])
def test_invalid_batch_size_is_rejected(tmp_path, batch_size):
    path, digest = _workbook(tmp_path)
    with pytest.raises(decoder.PlanFinderWorkbookError, match="Batch size"):
        _batches(path, digest, batch_size=batch_size)


def test_digest_and_complete_container_limits(tmp_path, monkeypatch):
    path, digest = _workbook(tmp_path)
    with pytest.raises(decoder.PlanFinderWorkbookError, match="SHA256"):
        _batches(path, "b" * 64)
    with pytest.raises(decoder.PlanFinderWorkbookError, match="SHA256"):
        _batches(path, digest.upper())
    monkeypatch.setattr(decoder, "MAX_WORKBOOK_BYTES", 8)
    with pytest.raises(decoder.PlanFinderWorkbookError, match="byte limit"):
        _batches(path, digest)


@pytest.mark.parametrize(
    "cell",
    [
        ("b", "1", "0"),
        ("e", "#N/A", "0"),
        ("n", "NaN", "0"),
        ("n", "not-numeric", "0"),
        ("s", "999", "0"),
        ("n", "1", "3"),
    ],
)
def test_unsupported_cell_evidence_fails_whole_decode(tmp_path, cell):
    path, digest = _workbook(tmp_path, count=1, overrides={0: cell})
    with pytest.raises(decoder.PlanFinderWorkbookError):
        _batches(path, digest)


def _change_header(members):
    members["xl/worksheets/sheet1.xml"].find(f".//{{{_NS}}}c/{{{_NS}}}v").text = "1"


def _change_date_system(members):
    members["xl/workbook.xml"].find(f"{{{_NS}}}workbookPr").set("date1904", "1")


def _external_relationship(members):
    members["xl/_rels/workbook.xml.rels"][0].set("TargetMode", "External")


def _add_formula(members):
    cell = members["xl/worksheets/sheet1.xml"].findall(f".//{{{_NS}}}row")[1][0]
    ElementTree.SubElement(cell, f"{{{_NS}}}f").text = "1+1"


def _duplicate_cell(members):
    row = members["xl/worksheets/sheet1.xml"].findall(f".//{{{_NS}}}row")[1]
    row.append(ElementTree.fromstring(ElementTree.tostring(row[0])))


def _skip_row(members):
    members["xl/worksheets/sheet1.xml"].findall(f".//{{{_NS}}}row")[1].set("r", "3")


@pytest.mark.parametrize(
    "mutation", [_change_header, _change_date_system, _external_relationship, _add_formula, _duplicate_cell, _skip_row]
)
def test_layout_formula_coordinate_and_temporal_drift_fails_closed(tmp_path, mutation):
    path, digest = _workbook(tmp_path, mutation=mutation)
    with pytest.raises(decoder.PlanFinderWorkbookError):
        _batches(path, digest)


def test_entity_declarations_are_rejected_before_parsing(tmp_path):
    def entity(members):
        members["xl/sharedStrings.xml"] = b'<!DOCTYPE sst [<!ENTITY x "example">]><sst>&x;</sst>'

    path, digest = _workbook(tmp_path, mutation=entity)
    with pytest.raises(decoder.PlanFinderWorkbookError, match="declarations"):
        _batches(path, digest)


def test_utf16_declaration_cannot_bypass_entity_rejection(tmp_path):
    def entity(members):
        members["xl/sharedStrings.xml"] = '<!DOCTYPE sst [<!ENTITY x "example">]><sst>&x;</sst>'.encode("utf-16")

    path, digest = _workbook(tmp_path, mutation=entity)
    with pytest.raises(decoder.PlanFinderWorkbookError, match="UTF8"):
        _batches(path, digest)


def test_xml_limit_and_empty_header_only_accounting(tmp_path, monkeypatch):
    path, digest = _workbook(tmp_path, count=0)
    assert _batches(path, digest) == []
    monkeypatch.setattr(decoder, "MAX_XML_BYTES", 4)
    with zipfile.ZipFile(path) as archive:
        with pytest.raises(decoder.PlanFinderWorkbookError, match="XML exceeds"):
            decoder._checked_xml(archive, "xl/sharedStrings.xml", maximum=4)


def test_rows_outside_sheet_data_are_rejected(tmp_path):
    def move_row(members):
        worksheet = members["xl/worksheets/sheet1.xml"]
        sheet_data = worksheet[0]
        issuer_row = sheet_data[1]
        sheet_data.remove(issuer_row)
        worksheet.append(issuer_row)

    path, digest = _workbook(tmp_path, count=1, mutation=move_row)
    with pytest.raises(decoder.PlanFinderWorkbookError, match="row location"):
        _batches(path, digest)


def test_malformed_and_duplicate_zip_members_fail_closed(tmp_path):
    path, digest = _workbook(tmp_path)
    with pytest.warns(UserWarning, match="Duplicate name"):
        with zipfile.ZipFile(path, "a") as archive:
            archive.writestr("xl/workbook.xml", b"duplicate")
    with pytest.raises(decoder.PlanFinderWorkbookError, match="membership"):
        _batches(path, hashlib.sha256(path.read_bytes()).hexdigest())
    path.write_bytes(b"not a ZIP")
    with pytest.raises(decoder.PlanFinderWorkbookError, match="malformed"):
        _batches(path, hashlib.sha256(path.read_bytes()).hexdigest())


def test_byte_limit_splits_without_losing_or_repeating_rows(tmp_path, monkeypatch):
    path, digest = _workbook(tmp_path, count=23)
    monkeypatch.setattr(decoder, "MAX_BATCH_BYTES", 1600)
    batches = _batches(path, digest)
    assert all(len(batch) <= 1600 for batch in batches)
    assert [row["source_row"] for batch in batches for row in json.loads(batch)["rows"]] == list(range(2, 25))
    monkeypatch.setattr(decoder, "MAX_BATCH_BYTES", 100)
    with pytest.raises(decoder.PlanFinderWorkbookError, match="One issuer row"):
        _batches(path, digest)


def test_shared_string_and_row_limits_fail_instead_of_truncating(tmp_path, monkeypatch):
    path, digest = _workbook(tmp_path, count=5)
    monkeypatch.setattr(decoder, "MAX_SHARED_STRINGS", 1)
    with pytest.raises(decoder.PlanFinderWorkbookError, match="shared strings"):
        _batches(path, digest)
    monkeypatch.setattr(decoder, "MAX_SHARED_STRINGS", 128_000)
    monkeypatch.setattr(decoder, "MAX_ROWS", 3)
    with pytest.raises(decoder.PlanFinderWorkbookError, match="row limit"):
        _batches(path, digest, batch_size=1)


def test_streaming_memory_and_full_row_accounting(tmp_path):
    path, digest = _workbook(tmp_path, count=10_000)
    tracemalloc.start()
    try:
        count = 0
        maximum = 0
        for batch in decoder.iter_cms_planfinder_issuer_batches(
            path, expected_workbook_sha256=digest, artifact_sha256=_ARTIFACT, batch_size=100
        ):
            count += len(json.loads(batch)["rows"])
            maximum = max(maximum, len(batch))
        _, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
    assert count == 10_000 and maximum < decoder.MAX_BATCH_BYTES
    assert peak < 8 * 1024 * 1024
