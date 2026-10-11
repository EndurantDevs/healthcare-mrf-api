# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Canonical input boundaries for provider-enrichment source rows."""

from __future__ import annotations

import importlib
from datetime import date, datetime

import pytest

enrichment = importlib.import_module("process.provider_enrichment")


@pytest.mark.parametrize(
    ("raw_value", "expected"),
    ((None, 7), ("", 7), ("5", 5), ("0", 7), ("bad", 7)),
)
def test_positive_integer_environment_is_bounded(monkeypatch, raw_value, expected) -> None:
    variable = "TEST_PROVIDER_ENRICHMENT_POSITIVE"
    if raw_value is None:
        monkeypatch.delenv(variable, raising=False)
    else:
        monkeypatch.setenv(variable, raw_value)
    assert enrichment._env_positive_int(variable, 7) == expected


@pytest.mark.parametrize(
    ("raw_value", "expected"),
    ((None, 9), (" ", 9), ("5", 5), ("0", None), ("-2", None), ("bad", 9)),
)
def test_optional_limit_distinguishes_unbounded_from_invalid(monkeypatch, raw_value, expected) -> None:
    variable = "TEST_PROVIDER_ENRICHMENT_LIMIT"
    if raw_value is None:
        monkeypatch.delenv(variable, raising=False)
    else:
        monkeypatch.setenv(variable, raw_value)
    assert enrichment._env_optional_limit(variable, 9) == expected


def test_short_archive_identifiers_remain_readable() -> None:
    assert enrichment._archived_identifier("provider_table") == "provider_table_old"


@pytest.mark.parametrize(
    ("raw_value", "expected"),
    (
        ("run-2026/07", "run202607"),
        ("!@#", None),
        (None, None),
    ),
)
def test_import_identifier_accepts_only_alphanumeric_coordinates(monkeypatch, raw_value, expected) -> None:
    if expected is None:
        monkeypatch.setattr(
            enrichment.datetime,
            "datetime",
            type(
                "FrozenDatetime",
                (),
                {"now": staticmethod(lambda: datetime(2026, 7, 30))},
            ),
        )
        expected = "20260730"
    assert enrichment._normalize_import_id(raw_value) == expected


@pytest.mark.parametrize(
    ("raw_value", "default", "expected"),
    (
        (None, 4, 4),
        (" ", 4, 4),
        ("abc", 4, 4),
        ("NPI 123", None, 123),
    ),
)
def test_safe_integer_extracts_only_decimal_digits(raw_value, default, expected) -> None:
    assert enrichment._safe_int(raw_value, default) == expected


def test_text_state_and_postal_normalization_preserve_fail_closed_values() -> None:
    assert enrichment._safe_text(None) is None
    assert enrichment._safe_text("   ") is None
    assert enrichment._safe_text(" value ") == "value"
    assert enrichment._safe_state(None) is None
    assert enrichment._safe_state(" t ") == "T"
    assert enrichment._safe_state("texas") == "TE"
    assert enrichment._safe_zip(None) is None
    assert enrichment._safe_zip(" 12-345-6789 ") == "12345"
    assert enrichment._safe_zip("AB-1") == "AB-1"


def test_date_normalization_handles_empty_invalid_and_timezone_inputs() -> None:
    assert enrichment._safe_date(None) is None
    assert enrichment._safe_date("not a date") is None
    assert enrichment._safe_date("2026-07-30") == date(2026, 7, 30)
    assert enrichment._safe_datetime(None) is None
    assert enrichment._safe_datetime("not a date") is None
    assert enrichment._safe_datetime("2026-07-30T02:00:00+02:00") == datetime(2026, 7, 30)


def test_sql_array_literal_is_typed_and_escapes_quotes() -> None:
    assert enrichment._sql_varchar_array_literal(["", " "]) == "ARRAY[]::varchar[]"
    assert enrichment._sql_varchar_array_literal(["A", "O'Reilly"]) == ("ARRAY['A', 'O''Reilly']::varchar[]")


@pytest.mark.parametrize(
    ("distribution", "expected"),
    (
        ({"mediaType": "text/csv"}, True),
        ({"format": "CSV"}, True),
        ({"mediaType": "application/json", "format": "JSON"}, False),
    ),
)
def test_distribution_type_requires_csv_evidence(distribution, expected) -> None:
    assert enrichment._is_csv_distribution(distribution) is expected


def test_period_year_and_header_selection_are_deterministic() -> None:
    assert enrichment._extract_period_bounds(None) == (None, None)
    assert enrichment._extract_period_bounds("2026-07-01") == (None, None)
    assert enrichment._extract_period_bounds("2026-07-01/2026-07-31") == (
        date(2026, 7, 1),
        date(2026, 7, 31),
    )
    assert enrichment._extract_year(None, "report 2026") == 2026
    assert enrichment._extract_year(None, "no year") is None
    assert enrichment._resolve_header({"NPI": "", "npi": "123"}, ("NPI", "npi")) == "123"
    assert enrichment._resolve_header({}, ("NPI",)) is None
    assert enrichment._normalize_nppes_header(" Provider Name (Legal) / DBA ") == ("provider_name_dba")


def test_header_validation_accepts_aliases_and_reports_every_missing_field() -> None:
    specification_map = {
        "fields": [
            {"name": "npi", "aliases": ("NPI",), "required": True},
            {"name": "state", "aliases": ("STATE", "STATE_CD"), "required": True},
            {"name": "optional", "aliases": ("OPTIONAL",), "required": False},
        ]
    }
    enrichment._validate_headers(["NPI", "STATE_CD"], specification_map, "source")
    with pytest.raises(RuntimeError, match="npi, state"):
        enrichment._validate_headers([], specification_map, "source")


@pytest.mark.parametrize(
    "title",
    [
        "Provider Directory",
        "Medicare Program Statistics Enrollment",
        "Monthly Enrollment",
        "Medicare Advantage Enrollment",
        "Part D Enrollment",
    ],
)
def test_beneficiary_catalogs_are_not_provider_enrollment(title):
    assert not enrichment._is_provider_enrollment_title(title)
    assert enrichment._is_provider_enrollment_title("New Provider Enrollments")


@pytest.mark.parametrize(
    ("content", "encoding", "headers"),
    [
        ("\ufeffNPI,Café\n1234567890,value\n".encode(), "utf-8-sig", ["NPI", "Café"]),
        ("NPI,Café\n1234567890,value\n".encode("cp1252"), "cp1252", ["NPI", "Café"]),
        (b"", "utf-8-sig", []),
    ],
)
def test_csv_encoding_and_headers_survive_chunk_boundaries(monkeypatch, tmp_path, content, encoding, headers):
    csv_path = tmp_path / "enrollment.csv"
    csv_path.write_bytes(content)
    monkeypatch.setattr(enrichment, "CSV_PROBE_CHUNK_SIZE", 2)

    selected_encoding = enrichment._select_csv_encoding(str(csv_path))

    assert selected_encoding == encoding
    assert enrichment._read_csv_header(str(csv_path), selected_encoding) == headers


@pytest.fixture
def enrollment_source():
    return {
        "dataset_title": "Synthetic Provider Enrollment",
        "distribution_title": "Quarter 1 2026",
        "download_url": "https://example.com/enrollment.csv",
        "reporting_year": 2026,
    }


@pytest.mark.parametrize(
    ("spec_key", "source_row", "reason"),
    [
        ("ffs_address", {"ZIP_CD": "01234"}, "missing_enrollment_id"),
        ("ffs_address", {"ENRLMT_ID": "E-1", "ZIP_CD": " "}, "missing_zip_code"),
        ("ffs_secondary_specialty", {"PROVIDER_TYPE_CD": "01"}, "missing_enrollment_id"),
        ("ffs_secondary_specialty", {"ENRLMT_ID": "E-1"}, "missing_provider_type_code"),
        ("ffs_additional_npi", {"NPI": "1234567890"}, "missing_enrollment_id"),
        ("ffs_additional_npi", {"ENRLMT_ID": "E-1", "NPI": "invalid"}, "missing_npi"),
        ("ffs_reassignment", {"RCV_BNFT_ENRLMT_ID": "E-2"}, "missing_reassigning_enrollment_id"),
        ("ffs_reassignment", {"REASGN_BNFT_ENRLMT_ID": "E-1"}, "missing_receiving_enrollment_id"),
    ],
)
def test_subfile_rows_require_their_identity_fields(enrollment_source, spec_key, source_row, reason):
    specification = enrichment.SPEC_BY_KEY[spec_key]
    model_columns = enrichment._model_columns(specification["model"])

    assert enrichment._build_row_payload(source_row, specification, enrollment_source, model_columns) == (None, reason)


@pytest.mark.parametrize(
    ("spec_key", "source_row", "expected", "identity_field", "replacement"),
    [
        (
            "ffs_address",
            {"ENROLLMENT ID": " E-1 ", "CITY": " Sample City ", "STATE": "ma", "ZIP CODE": "01234-5678"},
            {"enrollment_id": "E-1", "city": "Sample City", "state": "MA", "zip_code": "01234"},
            "ZIP CODE",
            "01235",
        ),
        (
            "ffs_secondary_specialty",
            {"ENROLLMENT ID": " E-1 ", "PROVIDER_TYPE_CD": " 01 ", "PROVIDER_TYPE_DESC": " Specialty "},
            {"enrollment_id": "E-1", "provider_type_code": "01", "provider_type_text": "Specialty"},
            "PROVIDER_TYPE_CD",
            "02",
        ),
    ],
)
def test_subfile_payload_normalization_preserves_record_identity(
    enrollment_source, spec_key, source_row, expected, identity_field, replacement
):
    specification = enrichment.SPEC_BY_KEY[spec_key]
    model_columns = enrichment._model_columns(specification["model"])
    normalized, reason = enrichment._build_row_payload(source_row, specification, enrollment_source, model_columns)
    repeated, _ = enrichment._build_row_payload(
        {**source_row, "UNMAPPED": "ignored"}, specification, enrollment_source, model_columns
    )
    distinct, _ = enrichment._build_row_payload(
        {**source_row, identity_field: replacement}, specification, enrollment_source, model_columns
    )

    assert reason is None
    assert {field: normalized[field] for field in expected} == expected
    assert set(normalized) <= model_columns
    assert normalized["source_url"] == "https://example.com/enrollment.csv"
    assert normalized["reporting_year"] == 2026
    assert normalized["record_hash"] == repeated["record_hash"]
    assert normalized["record_hash"] != distinct["record_hash"]


@pytest.mark.parametrize(
    ("spec_key", "additional_fields", "expected"),
    [
        (
            "hospital",
            {"SUBGROUP - GENERAL": " Y ", "REH CONVERSION DATE": "2026-02-03"},
            {"subgroup_general": "Y", "reh_conversion_date": date(2026, 2, 3)},
        ),
        (
            "snf",
            {"NURSING HOME PROVIDER NAME": " Sample Home ", "AFFILIATION ENTITY ID": " A-1 "},
            {"nursing_home_provider_name": "Sample Home", "affiliation_entity_id": "A-1"},
        ),
    ],
)
def test_enrollment_rows_keep_model_specific_fields(enrollment_source, spec_key, additional_fields, expected):
    specification = enrichment.SPEC_BY_KEY[spec_key]
    source_row_map = {"NPI": "1234567890", "ENROLLMENT ID": "E-1", **additional_fields}
    model_columns = enrichment._model_columns(specification["model"])

    normalized, reason = enrichment._build_row_payload(source_row_map, specification, enrollment_source, model_columns)

    assert reason is None
    assert normalized["npi"] == 1234567890
    assert {field: normalized[field] for field in expected} == expected
    assert set(normalized) <= model_columns
