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
def test_positive_integer_environment_is_bounded(
    monkeypatch, raw_value, expected
) -> None:
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
def test_optional_limit_distinguishes_unbounded_from_invalid(
    monkeypatch, raw_value, expected
) -> None:
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
def test_import_identifier_accepts_only_alphanumeric_coordinates(
    monkeypatch, raw_value, expected
) -> None:
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
    assert enrichment._safe_datetime("2026-07-30T02:00:00+02:00") == datetime(
        2026, 7, 30
    )


def test_sql_array_literal_is_typed_and_escapes_quotes() -> None:
    assert enrichment._sql_varchar_array_literal(["", " "]) == "ARRAY[]::varchar[]"
    assert enrichment._sql_varchar_array_literal(["A", "O'Reilly"]) == (
        "ARRAY['A', 'O''Reilly']::varchar[]"
    )


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
    assert enrichment._normalize_nppes_header(" Provider Name (Legal) / DBA ") == (
        "provider_name_dba"
    )


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
