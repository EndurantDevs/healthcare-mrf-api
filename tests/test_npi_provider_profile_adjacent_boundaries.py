from __future__ import annotations

import importlib
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

npi = importlib.import_module("api.endpoint.npi")


def test_public_taxonomy_rows_drop_internal_empty_and_duplicate_entries():
    rows = npi._public_nested_taxonomy_rows(
        [
            "malformed",
            {"npi": 1000000004, "checksum": 7},
            {"code": "207Q00000X", "npi": 1000000004},
            {"code": "207Q00000X", "checksum": 9},
        ]
    )

    assert rows == [{"code": "207Q00000X"}]


def test_query_normalizers_cover_absence_invalidity_and_human_fallbacks(
    monkeypatch,
):
    monkeypatch.delenv("PROFILE_FLAG_MISSING", raising=False)
    monkeypatch.setenv("PROFILE_FLAG_BLANK", " ")
    assert npi._is_environment_flag_enabled(
        "PROFILE_FLAG_MISSING",
        "PROFILE_FLAG_BLANK",
        default=True,
    )
    monkeypatch.setenv("PROFILE_FLAG_TRUE", "yes")
    assert npi._is_environment_flag_enabled("PROFILE_FLAG_TRUE") is True

    assert npi._parse_optional_bounded_int(
        None,
        param_name="page",
        minimum=1,
        maximum=10,
    ) is None
    assert npi._parse_optional_bounded_int(
        "5",
        param_name="page",
        minimum=1,
        maximum=10,
    ) == 5
    with pytest.raises(npi.sanic.exceptions.InvalidUsage):
        npi._parse_optional_bounded_int(
            "11",
            param_name="page",
            minimum=1,
            maximum=10,
        )

    assert npi._normalize_text_filter(" ", param_name="name") is None
    assert npi._normalize_state_filter(None) is None
    assert npi._normalize_state_filter("ca") == "CA"
    with pytest.raises(npi.sanic.exceptions.InvalidUsage):
        npi._normalize_state_filter("x")
    assert npi._normalize_ccn_filter(None) is None
    with pytest.raises(npi.sanic.exceptions.InvalidUsage):
        npi._normalize_ccn_filter("bad value!")

    assert npi._provider_display_name_from_mapping(
        {
            "entity_type_code": "1",
            "provider_organization_name": "Example Clinic",
        }
    ) == "Example Clinic"
    assert npi._provider_display_name_from_mapping(
        {
            "entity_type_code": "2",
            "provider_first_name": "Alex",
            "provider_last_name": "Example",
        }
    ) == "Alex Example"
    assert npi._provider_display_name_from_mapping({}) == "Unknown"


def test_optional_bounded_int_rejects_non_numeric_values():
    with pytest.raises(npi.sanic.exceptions.InvalidUsage, match="must be an integer"):
        npi._parse_optional_bounded_int(
            "not-a-number",
            param_name="page",
            minimum=1,
            maximum=10,
        )


@pytest.mark.asyncio
async def test_classification_lookups_handle_empty_cache_and_row_variants(
    monkeypatch,
):
    monkeypatch.setattr(npi, "_CLASSIFICATION_TAXONOMY_CODES_CACHE", {})
    monkeypatch.setattr(npi, "_CLASSIFICATION_NPI_CACHE", {})
    assert await npi._get_taxonomy_codes_for_classification("") == []
    assert await npi._get_classification_npi_list("") == []

    monkeypatch.setattr(
        npi,
        "_get_taxonomy_codes_for_classification",
        AsyncMock(return_value=[]),
    )
    assert await npi._get_classification_npi_list("Unknown") == []

    monkeypatch.setattr(
        npi,
        "_get_taxonomy_codes_for_classification",
        AsyncMock(return_value=["207Q00000X"]),
    )

    class QueryResult:
        def all(self):
            return [
                SimpleNamespace(_mapping={"npi": "1000000004"}),
                ("1000000005",),
                ("invalid",),
                (),
            ]

    session = SimpleNamespace(execute=AsyncMock(return_value=QueryResult()))
    assert await npi._get_classification_npi_list(
        "Family Medicine",
        session=session,
    ) == [1000000004, 1000000005]


def test_schema_caches_fail_closed_and_expire_deterministically(monkeypatch):
    cache_by_key = {}
    monkeypatch.setattr(npi, "ENABLE_NPI_SCHEMA_CACHE", False)
    assert npi._cache_get(cache_by_key, "missing") is None
    assert npi._cache_set(cache_by_key, "key", "value") == "value"
    assert cache_by_key == {}
    assert npi._filter_cache_get() is None
    assert npi._filter_cache_set({"city": True}) == {"city": True}
    assert npi._primary_total_cache_get("publication-1") is None
    assert npi._primary_total_cache_set("publication-1", 3) == 3

    monkeypatch.setattr(npi, "ENABLE_NPI_SCHEMA_CACHE", True)
    monkeypatch.setattr(npi.time, "monotonic", lambda: 1_000.0)
    monkeypatch.setattr(npi, "_NPI_SCHEMA_CACHE_TTL_SECONDS", 100.0)
    assert npi._cache_get({}, "missing") is None
    stale_cache_by_key = {"stale": (0.0, "value")}
    assert npi._cache_get(stale_cache_by_key, "stale") is None
    assert stale_cache_by_key == {}
    fresh_cache_by_key = {"fresh": (950.0, "value")}
    assert npi._cache_get(fresh_cache_by_key, "fresh") == "value"
    npi._cache_set(fresh_cache_by_key, "written", 7)
    assert fresh_cache_by_key["written"] == (1_000.0, 7)

    monkeypatch.setattr(
        npi,
        "_NPI_FILTER_CAPABILITIES_CACHE_STATE",
        {"entry": None},
    )
    assert npi._filter_cache_get() is None
    npi._filter_cache_set({"city": True})
    assert npi._filter_cache_get() == {"city": True}
    npi._NPI_FILTER_CAPABILITIES_CACHE_STATE["entry"] = (
        0.0,
        "mrf",
        {"city": True},
    )
    assert npi._filter_cache_get() is None
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "custom")
    npi._NPI_FILTER_CAPABILITIES_CACHE_STATE["entry"] = (
        950.0,
        "mrf",
        {"city": True},
    )
    assert npi._filter_cache_get() is None



def test_primary_total_cache_is_publication_scoped_and_expires(monkeypatch):
    monkeypatch.setattr(npi, "ENABLE_NPI_SCHEMA_CACHE", True)
    monkeypatch.setattr(npi.time, "monotonic", lambda: 1_000.0)
    monkeypatch.setattr(npi, "_NPI_SCHEMA_CACHE_TTL_SECONDS", 100.0)
    monkeypatch.setattr(npi, "_NPI_PRIMARY_TOTAL_CACHE_STATE", {"entry": None})

    assert npi._primary_total_cache_get("publication-1") is None
    npi._primary_total_cache_set("publication-1", 4)
    assert npi._primary_total_cache_get("publication-1") == 4
    assert npi._primary_total_cache_get("publication-2") is None
    npi._NPI_PRIMARY_TOTAL_CACHE_STATE["entry"] = (
        0.0,
        "publication-1",
        4,
    )
    assert npi._primary_total_cache_get("publication-1") is None


def test_model_table_and_limited_cache_boundaries(monkeypatch):
    assert npi._model_table_columns(object()) == set()
    cache_by_key = {
        "older": (1.0, "old"),
        "new": (2.0, "new"),
    }
    monkeypatch.setattr(npi, "_CLASSIFICATION_CACHE_MAX_KEYS", 1)

    npi._set_limited_classification_cache(
        cache_by_key,
        "new",
        "replacement",
        3.0,
    )

    assert cache_by_key == {"new": (3.0, "replacement")}
