# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Synthetic workbooks retain the inspected issuer sheet/header/cell layout."""

import hashlib
import io
import json
import struct
import tracemalloc
import zipfile
import zlib
from xml.etree import ElementTree

import pytest

from process import cms_planfinder_rbis_input as plan_decoder
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


def _plan_row(number=1, **changes):
    values_by_header = {
        "HIOS Plan ID": f"00123CA{number:07d}".encode(),
        "Plan Marketing Name": b"Example Plan",
        "HIOS Product ID": b"00123CA000",
        "Network ID": b"CAN001",
        "Plan Effective Date": b"2026-01-01",
        "Plan Expiration Date": b"2026-12-31",
        **changes,
    }
    return b"|".join(values_by_header.get(field, b"\xff") for field in plan_decoder.HEADERS) + b"\r\n"


def _plan_artifact(tmp_path, rows=None, *, raw=None, missing=None, duplicate=False, damage=None):
    if raw is None:
        raw = b"|".join(field.encode() for field in plan_decoder.HEADERS) + b"\r\n"
        raw += b"".join(rows if rows is not None else [_plan_row()])
    digests_by_member = {"member_sha256": hashlib.sha256(raw).hexdigest()}
    contents = raw
    for level, member in enumerate(
        (plan_decoder.PLAN_MEMBER, plan_decoder.PLAN_ARCHIVE_MEMBER, plan_decoder.RBIS_MEMBER)
    ):
        output = io.BytesIO()
        with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_DEFLATED) as archive:
            archive.writestr("missing.dat" if missing == level else member, contents)
            if duplicate and level == 0:
                with pytest.warns(UserWarning, match="Duplicate name"):
                    archive.writestr(member, contents)
        contents = output.getvalue()
        if damage is not None and damage[0] == level:
            contents = _damaged_plan_member(contents, damage[1])
        digests_by_member[("plan_archive_sha256", "rbis_sha256", "artifact_sha256")[level]] = hashlib.sha256(
            contents
        ).hexdigest()
    path = tmp_path / "synthetic-plan-input.zip"
    path.write_bytes(contents)
    return path, digests_by_member


def _plan_batches(path, digests_by_member, **options):
    return list(
        decoder.iter_cms_planfinder_plan_batches(
            path, expected_artifact_sha256=digests_by_member["artifact_sha256"], **options
        )
    )


def test_plan_batches_retain_every_source_position_hash_and_exact_field(tmp_path):
    path, digests_by_member = _plan_artifact(tmp_path, [_plan_row(number) for number in range(1, 6)])
    encoded = _plan_batches(path, digests_by_member, batch_size=2)
    batches = [json.loads(batch) for batch in encoded]
    assert [len(batch["rows"]) for batch in batches] == [2, 2, 1]
    rows = [row for batch in batches for row in batch["rows"]]
    assert [row["source_row"] for row in rows] == [2, 3, 4, 5, 6]
    assert rows[0] == {
        "source_row": 2,
        "values": ["00123CA0000001", "Example Plan", "00123CA000", "CAN001", "2026-01-01", "2026-12-31"],
    }
    for batch in batches:
        assert all(batch[key] == value for key, value in digests_by_member.items())
        assert batch["headers"] == list(plan_decoder.SELECTED_HEADERS)
        assert batch["member"] == plan_decoder.PLAN_MEMBER and batch["layout"] == plan_decoder.LAYOUT
        assert "plan_year" not in batch and "checksum_network" not in batch and "network_id" not in batch
    assert encoded == _plan_batches(path, digests_by_member, batch_size=2)
    assert _plan_batches(*_plan_artifact(tmp_path, [])) == []


@pytest.mark.parametrize(
    "field,value",
    [
        ("HIOS Plan ID", b"00123CA000000"),
        ("HIOS Plan ID", b"00000CA0000001"),
        ("HIOS Product ID", b"00124CA000"),
        ("Network ID", b"42"),
        ("Plan Marketing Name", b"\xff"),
        ("Plan Marketing Name", b" "),
        ("Plan Marketing Name", b"x" * 513),
        ("Plan Marketing Name", b"bad\tlabel"),
        ("Plan Effective Date", b"2026-02-30"),
        ("Plan Effective Date", b"01/01/2026"),
        ("Plan Expiration Date", b"2025-12-31"),
        ("Plan Expiration Date", b""),
    ],
)
def test_plan_selected_fields_refuse_malformed_identity_label_and_dates(tmp_path, field, value):
    path, digests_by_member = _plan_artifact(tmp_path, [_plan_row(**{field: value})])
    with pytest.raises(decoder.PlanFinderPlanInputError):
        _plan_batches(path, digests_by_member)


@pytest.mark.parametrize(
    "raw", [b"", b"wrong header\n", b"|".join(field.encode() for field in plan_decoder.HEADERS) + b"\nshort|row\n"]
)
def test_plan_header_and_fixed_record_width_are_required(tmp_path, raw):
    path, digests_by_member = _plan_artifact(tmp_path, raw=raw)
    with pytest.raises(decoder.PlanFinderPlanInputError):
        _plan_batches(path, digests_by_member)


@pytest.mark.parametrize("missing", [0, 1, 2])
def test_plan_container_requires_every_exact_nested_member(tmp_path, missing):
    path, digests_by_member = _plan_artifact(tmp_path, missing=missing)
    with pytest.raises(decoder.PlanFinderPlanInputError, match="missing"):
        _plan_batches(path, digests_by_member)


def test_plan_duplicate_zip_members_and_bad_artifact_pin_fail_closed(tmp_path):
    path, digests_by_member = _plan_artifact(tmp_path, duplicate=True)
    with pytest.raises(decoder.PlanFinderPlanInputError, match="membership"):
        _plan_batches(path, digests_by_member)
    path, digests_by_member = _plan_artifact(tmp_path)
    for digest in ("b" * 64, "A" * 64):
        with pytest.raises(decoder.PlanFinderPlanInputError, match="SHA256"):
            _plan_batches(path, {**digests_by_member, "artifact_sha256": digest})
    path.write_bytes(b"not a ZIP")
    with pytest.raises(decoder.PlanFinderPlanInputError, match="malformed"):
        _plan_batches(path, {"artifact_sha256": hashlib.sha256(path.read_bytes()).hexdigest()})


@pytest.mark.parametrize(
    "limit",
    ["MAX_ARTIFACT_BYTES", "MAX_RBIS_BYTES", "MAX_PLAN_ARCHIVE_BYTES", "MAX_PLAN_BYTES", "MAX_LINE_BYTES", "MAX_ROWS"],
)
def test_plan_byte_record_and_row_bounds_refuse_instead_of_truncating(tmp_path, monkeypatch, limit):
    path, digests_by_member = _plan_artifact(tmp_path, [_plan_row(1), _plan_row(2)])
    monkeypatch.setattr(plan_decoder, limit, 1)
    with pytest.raises(decoder.PlanFinderPlanInputError):
        _plan_batches(path, digests_by_member, batch_size=1)


@pytest.mark.parametrize("batch_size", [0, 5001, True, 1.5])
def test_plan_batch_size_is_closed(tmp_path, batch_size):
    path, digests_by_member = _plan_artifact(tmp_path)
    with pytest.raises(decoder.PlanFinderPlanInputError, match="Batch size"):
        _plan_batches(path, digests_by_member, batch_size=batch_size)


def test_plan_byte_boundary_and_duplicate_conflict_across_batches(tmp_path, monkeypatch):
    path, digests_by_member = _plan_artifact(tmp_path, [_plan_row(1), _plan_row(2), _plan_row(3)])
    one_row_size = len(_plan_batches(path, digests_by_member, batch_size=1)[0])
    monkeypatch.setattr(plan_decoder, "MAX_BATCH_BYTES", one_row_size)
    batches = _plan_batches(path, digests_by_member)
    assert [row["source_row"] for batch in batches for row in json.loads(batch)["rows"]] == [2, 3, 4]
    assert all(len(batch) <= one_row_size for batch in batches)
    monkeypatch.setattr(plan_decoder, "MAX_BATCH_BYTES", 1)
    with pytest.raises(decoder.PlanFinderPlanInputError, match="One plan row"):
        _plan_batches(path, digests_by_member)
    monkeypatch.setattr(plan_decoder, "MAX_BATCH_BYTES", 8 * 1024 * 1024)
    path, digests_by_member = _plan_artifact(tmp_path, [_plan_row(1), _plan_row(1)])
    assert [
        row["source_row"] for batch in _plan_batches(path, digests_by_member) for row in json.loads(batch)["rows"]
    ] == [2, 3]
    path, digests_by_member = _plan_artifact(
        tmp_path, [_plan_row(1), _plan_row(2), _plan_row(1, **{"Plan Marketing Name": b"Other Plan"})]
    )
    iterator = decoder.iter_cms_planfinder_plan_batches(
        path, expected_artifact_sha256=digests_by_member["artifact_sha256"], batch_size=1
    )
    assert json.loads(next(iterator))["rows"][0]["source_row"] == 2
    with pytest.raises(decoder.PlanFinderPlanInputError, match="Conflicting duplicate"):
        list(iterator)


@pytest.mark.parametrize("terminal", ["cancel", "conflict"])
def test_plan_candidate_exit_closes_all_retained_temporary_files(tmp_path, monkeypatch, terminal):
    rows = [_plan_row(1), _plan_row(2), _plan_row(1, **{"Plan Marketing Name": b"Other Plan"})]
    path, digests_by_member = _plan_artifact(tmp_path, rows)
    temporary_file = plan_decoder.tempfile.TemporaryFile
    files = []

    def capture_file():
        opened = temporary_file()
        files.append(opened)
        return opened

    monkeypatch.setattr(plan_decoder.tempfile, "TemporaryFile", capture_file)
    iterator = decoder.iter_cms_planfinder_plan_batches(
        path, expected_artifact_sha256=digests_by_member["artifact_sha256"], batch_size=1
    )
    next(iterator)
    assert len(files) == 4 and not any(opened.closed for opened in files)
    if terminal == "cancel":
        iterator.close()
    else:
        with pytest.raises(decoder.PlanFinderPlanInputError, match="Conflicting duplicate"):
            list(iterator)
    assert all(opened.closed for opened in files)


def _damaged_plan_member(contents, failure):
    archive = bytearray(contents)
    if failure == "unsupported":
        central = archive.index(b"PK\x01\x02")
        archive[8:10] = archive[central + 10 : central + 12] = (99).to_bytes(2, "little")
    elif failure in {"central_utf8", "local_utf8"}:
        central = archive.index(b"PK\x01\x02")
        flags_offset = central + 8 if failure == "central_utf8" else 6
        flags = int.from_bytes(archive[flags_offset : flags_offset + 2], "little") | 0x800
        archive[flags_offset : flags_offset + 2] = flags.to_bytes(2, "little")
        archive[central + 46 if failure == "central_utf8" else 30] = 255
    else:
        name_size = int.from_bytes(archive[26:28], "little")
        extra_size = int.from_bytes(archive[28:30], "little")
        archive[30 + name_size + extra_size] = 7
    return bytes(archive)


@pytest.mark.parametrize("level", [0, 1, 2])
@pytest.mark.parametrize("failure,expected_cause", [("deflate", zlib.error), ("unsupported", NotImplementedError)])
def test_plan_decompression_errors_wrap_the_original_cause(tmp_path, level, failure, expected_cause):
    path, digests_by_member = _plan_artifact(tmp_path, damage=(level, failure))
    with pytest.raises(plan_decoder.PlanFinderPlanInputError, match="Plan container is malformed") as error:
        _plan_batches(path, digests_by_member)
    assert isinstance(error.value.__cause__, expected_cause)


def _directory_fixture(count=1):
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w") as archive:
        for index in range(count):
            archive.writestr(f"example-{index}.dat", b"payload")
    return output.getvalue()


def _with_zip64_directory(contents, *, extension=b""):
    offset = contents.rindex(b"PK\x05\x06")
    footer_fields = list(struct.unpack(zipfile.structEndArchive, contents[offset : offset + zipfile.sizeEndCentDir]))
    record = struct.pack(
        zipfile.structEndArchive64,
        b"PK\x06\x06",
        44 + len(extension),
        45,
        45,
        0,
        0,
        footer_fields[3],
        footer_fields[4],
        footer_fields[5],
        footer_fields[6],
    )
    locator = struct.pack(zipfile.structEndArchive64Locator, b"PK\x06\x07", 0, offset, 1)
    footer_fields[3:7] = [65535, 65535, 0xFFFFFFFF, 0xFFFFFFFF]
    return contents[:offset] + record + extension + locator + struct.pack(zipfile.structEndArchive, *footer_fields)


def _write_directory_artifact(tmp_path, contents):
    path = tmp_path / "synthetic-directory.zip"
    path.write_bytes(contents)
    return path, {"artifact_sha256": hashlib.sha256(contents).hexdigest()}


@pytest.mark.parametrize("zip64", [False, True])
@pytest.mark.parametrize("advertised_count", [1, 257])
def test_plan_member_limit_precedes_zipfile_even_when_footer_lies(tmp_path, monkeypatch, zip64, advertised_count):
    contents = bytearray(_directory_fixture(257))
    end = contents.rindex(b"PK\x05\x06")
    contents[end + 8 : end + 12] = advertised_count.to_bytes(2, "little") * 2
    contents = _with_zip64_directory(bytes(contents)) if zip64 else bytes(contents)
    artifact = _write_directory_artifact(tmp_path, contents)

    def refuse_construction(*_args, **_kwargs):
        pytest.fail("ZipFile was constructed before the member limit")

    monkeypatch.setattr(plan_decoder.zipfile, "ZipFile", refuse_construction)
    with pytest.raises(plan_decoder.PlanFinderPlanInputError, match="membership"):
        _plan_batches(*artifact)


@pytest.mark.parametrize("zip64", [False, True])
def test_plan_directory_byte_limit_precedes_zipfile_allocation(tmp_path, monkeypatch, zip64):
    contents = _directory_fixture()
    if zip64:
        contents = _with_zip64_directory(contents)
    artifact = _write_directory_artifact(tmp_path, contents)
    monkeypatch.setattr(plan_decoder, "MAX_ZIP_DIRECTORY_BYTES", 46)

    def refuse_construction(*_args, **_kwargs):
        pytest.fail("ZipFile was constructed before the directory byte limit")

    monkeypatch.setattr(plan_decoder.zipfile, "ZipFile", refuse_construction)
    with pytest.raises(plan_decoder.PlanFinderPlanInputError, match="directory exceeds its byte limit"):
        _plan_batches(*artifact)


def test_plan_oversized_advertised_directory_is_refused_without_reading_it(tmp_path, monkeypatch):
    contents = bytearray(_directory_fixture())
    end = contents.rindex(b"PK\x05\x06")
    contents[end + 12 : end + 16] = (plan_decoder.MAX_ZIP_DIRECTORY_BYTES + 1).to_bytes(4, "little")
    artifact = _write_directory_artifact(tmp_path, bytes(contents))

    def refuse_construction(*_args, **_kwargs):
        pytest.fail("ZipFile was constructed for an oversized advertised directory")

    monkeypatch.setattr(plan_decoder.zipfile, "ZipFile", refuse_construction)
    with pytest.raises(plan_decoder.PlanFinderPlanInputError, match="directory exceeds its byte limit"):
        _plan_batches(*artifact)


@pytest.mark.parametrize("failure", ["count", "length", "offset", "truncated", "signature", "zip64_locator"])
def test_plan_inconsistent_directory_metadata_refuses_before_construction(tmp_path, monkeypatch, failure):
    contents = bytearray(_directory_fixture())
    central = contents.index(b"PK\x01\x02")
    end = contents.rindex(b"PK\x05\x06")
    if failure == "zip64_locator":
        contents = bytearray(_with_zip64_directory(contents))
        offset, size, replacement = contents.rindex(b"PK\x06\x07") + 8, 8, (len(contents) + 1).to_bytes(8, "little")
    else:
        mutations_by_failure = {
            "count": (end + 8, 4, b"\0" * 4),
            "length": (central + 28, 2, (65535).to_bytes(2, "little")),
            "offset": (end + 16, 4, (end + 1).to_bytes(4, "little")),
            "truncated": (end + 12, 4, (1).to_bytes(4, "little")),
            "signature": (central, 1, b"\0"),
        }
        offset, size, replacement = mutations_by_failure[failure]
    contents[offset : offset + size] = replacement
    artifact = _write_directory_artifact(tmp_path, bytes(contents))

    def refuse_construction(*_args, **_kwargs):
        pytest.fail("ZipFile was constructed for inconsistent directory metadata")

    monkeypatch.setattr(plan_decoder.zipfile, "ZipFile", refuse_construction)
    with pytest.raises(plan_decoder.PlanFinderPlanInputError, match="directory|membership|malformed"):
        _plan_batches(*artifact)


@pytest.mark.parametrize("level", [0, 1, 2])
@pytest.mark.parametrize("failure", ["central_utf8", "local_utf8"])
def test_plan_actual_malformed_utf8_filenames_wrap_the_original_cause(tmp_path, level, failure):
    artifact = _plan_artifact(tmp_path, damage=(level, failure))
    with pytest.raises(plan_decoder.PlanFinderPlanInputError, match="Plan container is malformed") as error:
        _plan_batches(*artifact)
    assert isinstance(error.value.__cause__, UnicodeDecodeError)


@pytest.mark.parametrize("variant", ["ordinary", "comment", "prefix", "zip64", "zip64_prefix", "zip64_extension"])
def test_plan_directory_preflight_preserves_supported_containers_and_nested_batch_bytes(tmp_path, variant):
    path, digests = _plan_artifact(tmp_path, [_plan_row(1), _plan_row(2)])
    expected_batches = [json.loads(batch) for batch in _plan_batches(path, digests, batch_size=1)]
    contents = path.read_bytes()
    if variant.startswith("zip64"):
        contents = _with_zip64_directory(
            contents, extension=b"example extension" if variant == "zip64_extension" else b""
        )
    if variant in {"prefix", "zip64_prefix"}:
        contents = b"synthetic executable prefix" + contents
    elif variant == "comment":
        contents = contents[:-2] + (65535).to_bytes(2, "little") + b"x" * 65535
    artifact = _write_directory_artifact(tmp_path, contents)
    actual_batches = [json.loads(batch) for batch in _plan_batches(*artifact, batch_size=1)]
    for batch in expected_batches:
        batch["artifact_sha256"] = artifact[1]["artifact_sha256"]
    assert actual_batches == expected_batches


def test_plan_directory_preflight_bounds_reads_and_accepts_the_exact_member_limit():
    class BoundedReads(io.BytesIO):
        def read(self, size=-1):
            assert 0 <= size <= zipfile.ZIP_MAX_COMMENT + zipfile.sizeEndCentDir
            return super().read(size)

    stream = BoundedReads(_with_zip64_directory(_directory_fixture(256)))
    plan_decoder._check_zip_directory(stream)
