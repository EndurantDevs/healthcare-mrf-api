# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import csv
import hashlib
import io

import pytest

from process import rhode_island_profile_roster as roster


def source_row(license_number="MD00001", **changes):
    raw_fields = dict.fromkeys(roster.ROSTER_COLUMNS, "")
    raw_fields.update(
        {
            "Name": "EXAMPLE ALEX",
            "First": "Alex",
            "Last": "Example",
            "License No": license_number,
            "Profession": "Physician",
            "License Type": roster.LICENSE_TYPES[license_number[:2]],
            "Status": "Active",
            "Issue Date": "01/01/2001",
            "Specialty": "Example Specialty",
            "License Address Line 1": 'Example Building, "Suite 1"',
        }
    )
    raw_fields.update(changes)
    return raw_fields


def csv_bytes(source_rows, columns=roster.ROSTER_COLUMNS):
    stream = io.StringIO(newline="")
    writer = csv.writer(stream, lineterminator="\n")
    writer.writerow(columns)
    writer.writerows([row[column] for column in columns] for row in source_rows)
    return stream.getvalue().encode()


def evidence(payload):
    return {
        "run_id": "synthetic-run",
        "artifact_id": "synthetic-artifact",
        "source_url": roster.SOURCE_URL,
        "downloaded_at": "2026-09-09T00:00:00Z",
        "content_sha256": hashlib.sha256(payload).hexdigest(),
    }


def parse(payload, *, expected_rows=1, license_type=roster.LICENSE_TYPES["MD"]):
    return roster.parse_roster(
        payload, license_type=license_type, expected_rows=expected_rows, evidence=evidence(payload)
    )


@pytest.mark.parametrize("prefix", ["MD", "DO"])
def test_roots_retain_every_source_occurrence(prefix):
    first = source_row(prefix + "00002", **{"Status": "Active Probation"})
    second_fields_by_name = {**first, "Specialty": "Second Specialty"}
    third = source_row(prefix + "00001", **{"Status": "Active Restricted", "Specialty": ""})
    source_rows = [first, second_fields_by_name, first.copy(), third]
    payload = csv_bytes(source_rows)
    field_limit = csv.field_size_limit()
    result = parse(payload, expected_rows=4, license_type=roster.LICENSE_TYPES[prefix])
    assert csv.field_size_limit() == field_limit
    assert result["input_row_count"] == result["expected_row_count"] == 4
    assert result["unique_license_count"] == 2
    assert [root["license_number"] for root in result["roots"]] == [prefix + "00001", prefix + "00002"]
    root = result["roots"][1]
    assert root["specialties"] == ["Example Specialty", "Second Specialty"]
    assert [original["raw_payload"] for original in root["originals"]] == source_rows[:3]
    assert [original["row_number"] for original in root["originals"]] == [1, 2, 3]
    assert [original["line_number"] for original in root["originals"]] == [2, 3, 4]
    assert all(original["raw_payload"]["Status"] == "Active Probation" for original in root["originals"])
    assert "facts" not in result and "matched_npi" not in root


def test_multiline_unicode_fields_preserve_exact_values():
    raw_fields = source_row(**{"Name": "EXAMPLE ÁLEX", "License Address Line 2": "Floor 2\nWest wing"})
    result = parse(csv_bytes([raw_fields]).rstrip(b"\n"))
    assert result["roots"][0]["originals"] == [{"row_number": 1, "line_number": 3, "raw_payload": raw_fields}]


@pytest.mark.parametrize(
    "change",
    [
        {"License No": "MD1"},
        {"License No": "MD００００１"},
        {"License No": " MD00001"},
        {"License No": "DO00001"},
        {"Profession": "Other"},
        {"License Type": roster.LICENSE_TYPES["DO"]},
    ],
)
def test_every_row_must_match_selected_cohort(change):
    with pytest.raises(ValueError, match="rhode_island_roster_cohort_identity_mismatch"):
        parse(csv_bytes([source_row(), source_row(**change)]), expected_rows=2)


@pytest.mark.parametrize("field", ["Name", "First", "Last", "Status", "Issue Date", "License Address Line 1"])
def test_repeated_license_conflicts_reject_whole_input(field):
    with pytest.raises(ValueError, match="rhode_island_roster_conflicting_license_rows"):
        parse(csv_bytes([source_row(), source_row(**{field: "Conflicting value"})]), expected_rows=2)


@pytest.mark.parametrize(
    "tail",
    [b"\n", b"trailer\n", b'"unfinished', b'"bad"suffix\n', b",".join([b"field"] * 27), b",".join([b"field"] * 25)],
)
def test_unexplained_or_malformed_tail_is_rejected(tail):
    with pytest.raises(ValueError, match="rhode_island_roster_"):
        parse(csv_bytes([source_row()]) + tail)


@pytest.mark.parametrize(
    "columns",
    [
        roster.ROSTER_COLUMNS[:-1],
        tuple(reversed(roster.ROSTER_COLUMNS)),
        (*roster.ROSTER_COLUMNS[:12], "Address Line 1", *roster.ROSTER_COLUMNS[13:]),
    ],
)
def test_header_requires_exact_download_columns(columns):
    with pytest.raises(ValueError, match="rhode_island_roster_column_schema_mismatch"):
        parse(csv_bytes([], columns=columns))


@pytest.mark.parametrize("expected_rows", [True, False, 0, -1, 1.0, "1", None, roster.MAX_ROSTER_ROWS + 1])
def test_source_count_is_bounded_integer(expected_rows):
    with pytest.raises(ValueError, match="rhode_island_roster_invalid_expected_rows"):
        parse(csv_bytes([source_row()]), expected_rows=expected_rows)


def test_source_count_mismatch_has_no_partial_result():
    with pytest.raises(ValueError, match="rhode_island_roster_source_row_count_mismatch"):
        parse(csv_bytes([source_row()]), expected_rows=2)
    with pytest.raises(ValueError, match="rhode_island_roster_source_row_count_mismatch"):
        parse(csv_bytes([]))


def test_byte_row_and_field_limits(monkeypatch):
    payload = csv_bytes([source_row()])
    monkeypatch.setattr(roster, "MAX_ROSTER_BYTES", len(payload) - 1)
    with pytest.raises(ValueError, match="rhode_island_roster_invalid_csv_bytes"):
        parse(payload)
    monkeypatch.setattr(roster, "MAX_ROSTER_BYTES", 16 * 1024 * 1024)
    monkeypatch.setattr(roster, "MAX_ROSTER_ROWS", 1)
    with pytest.raises(ValueError, match="rhode_island_roster_row_limit_exceeded"):
        parse(csv_bytes([source_row(), source_row()]))
    with pytest.raises(ValueError, match="rhode_island_roster_field_limit_exceeded"):
        parse(csv_bytes([source_row(**{"Phone": "1" * (roster.MAX_FIELD_CHARS + 1)})]))


@pytest.mark.parametrize("payload", [b"", b"\xff", b"\x00", b"\xef\xbb\xbf" + csv_bytes([source_row()])])
def test_unobserved_encoding_or_nul_is_rejected(payload):
    with pytest.raises(ValueError, match="rhode_island_roster_"):
        parse(payload)


@pytest.mark.parametrize(
    "field, value",
    [
        ("content_sha256", "0" * 64),
        ("source_url", "https://example.test"),
        ("downloaded_at", "2026-09-09"),
        ("downloaded_at", "not-a-timestamp"),
        ("artifact_id", ""),
    ],
)
def test_provenance_is_bound_to_retained_bytes(field, value):
    payload = csv_bytes([source_row()])
    metadata = {**evidence(payload), field: value}
    with pytest.raises(ValueError, match="rhode_island_roster_"):
        roster.parse_roster(payload, license_type=roster.LICENSE_TYPES["MD"], expected_rows=1, evidence=metadata)
