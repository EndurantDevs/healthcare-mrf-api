from __future__ import annotations

import importlib
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from api import provider_geo_sql as geo_sql
from tests.test_custom_import_provider_geo_sql import _context, _imported_geo_query

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

    assert npi._parse_optional_bounded_int(None, param_name="page", minimum=1, maximum=10) is None
    assert npi._parse_optional_bounded_int("5", param_name="page", minimum=1, maximum=10) == 5
    with pytest.raises(npi.sanic.exceptions.InvalidUsage):
        npi._parse_optional_bounded_int(
            "11",
            param_name="page",
            minimum=1,
            maximum=10,
        )

    assert npi._normalize_text_filter(" ", param_name="name") is None
    assert npi._normalize_state_filter(None) is None
    assert npi._normalize_state_filter(" \t") is None
    assert npi._normalize_state_filter("ca") == "CA"
    with pytest.raises(npi.sanic.exceptions.InvalidUsage):
        npi._normalize_state_filter("x")
    assert npi._normalize_ccn_filter(None) is None
    with pytest.raises(npi.sanic.exceptions.InvalidUsage):
        npi._normalize_ccn_filter("bad value!")

    assert (
        npi._provider_display_name_from_mapping(
            {
                "entity_type_code": "1",
                "provider_organization_name": "Example Clinic",
            }
        )
        == "Example Clinic"
    )
    assert (
        npi._provider_display_name_from_mapping(
            {
                "entity_type_code": "2",
                "provider_first_name": "Alex",
                "provider_last_name": "Example",
            }
        )
        == "Alex Example"
    )
    assert npi._provider_display_name_from_mapping({}) == "Unknown"


def test_optional_bounded_int_rejects_non_numeric_values():
    with pytest.raises(npi.sanic.exceptions.InvalidUsage, match="must be an integer"):
        npi._parse_optional_bounded_int(
            "not-a-number",
            param_name="page",
            minimum=1,
            maximum=10,
        )


@pytest.mark.parametrize("invalid_part", ["context", "query", "ordering"])
def test_imported_geo_rejects_untyped_or_unordered_input(invalid_part):
    """The builder requires typed input and a defined imported ordering or match."""
    context = _context(direction=None if invalid_part == "ordering" else "asc")
    query = _imported_geo_query()
    if invalid_part == "context":
        context = object()
    elif invalid_part == "query":
        query = object()
    with pytest.raises(ValueError, match="custom-import geo"):
        geo_sql.build_imported_geo_statements(context, query)


@pytest.mark.parametrize("limit", [True, False, 0, 51, 1.5])
def test_imported_geo_page_limit_requires_a_bounded_integer(limit):
    """Invalid limits fail without modifying the caller's native bind values."""
    parameters_by_name = {"in_long": 0.0, "in_lat": 0.0, "radius": 10.0}
    query = _imported_geo_query(native_parameters=parameters_by_name)
    with pytest.raises(ValueError, match="page limit is invalid"):
        geo_sql.build_imported_geo_statements(_context(), replace(query, limit=limit))
    assert parameters_by_name == {"in_long": 0.0, "in_lat": 0.0, "radius": 10.0}


@pytest.mark.parametrize("npi_value", [1000000491, "100000049", "100000049x", "١٠٠٠٠٠٠٤٩١"])
def test_imported_geo_cursor_requires_ten_ascii_digits(npi_value):
    """Cursor identities cannot be coerced from numeric, short or non-ASCII values."""
    query = _imported_geo_query(anchor=(npi_value, "00000000-0000-0000-0000-000000000001"))
    with pytest.raises(ValueError, match="cursor anchor is invalid"):
        geo_sql.build_imported_geo_statements(_context(), query)


@pytest.mark.parametrize("limit", [1, 50])
@pytest.mark.parametrize("require_match", [False, True])
def test_imported_geo_valid_limits_keep_native_binds_and_fetch_one_sentinel(limit, require_match):
    """Both ordered and match-only queries preserve caller binds at inclusive limits."""
    parameters_by_name = {"in_long": 0.0, "in_lat": 0.0, "radius": 10.0}
    query = replace(_imported_geo_query(native_parameters=parameters_by_name), limit=limit)
    context = _context(direction=None if require_match else "asc", require_match=require_match)
    statements = geo_sql.build_imported_geo_statements(context, query)
    assert statements.parameters[geo_sql._PAGE_LIMIT_PARAMETER] == limit + 1
    assert parameters_by_name == {"in_long": 0.0, "in_lat": 0.0, "radius": 10.0}


def test_nearby_identity_without_distance_preserves_the_last_native_cursor():
    """A row without distance still contributes its identity but cannot advance paging."""
    first_by_field = dict(npi_code=1000000491, address_key="ABC", cursor_distance_meters=1.5)
    second_by_field = dict(npi=1000000004, address_key="DEF")
    identities, cursor = geo_sql.nearby_batch_identities(
        [SimpleNamespace(_mapping=first_by_field), SimpleNamespace(_mapping=second_by_field)]
    )
    assert identities == {(1000000491, "abc"), (1000000004, "def")}
    assert cursor == (1.5, 1000000491, "ABC")


@pytest.mark.asyncio
async def test_classification_lookups_handle_empty_cache_and_row_variants(
    monkeypatch,
):
    monkeypatch.setattr(npi, "_CLASSIFICATION_TAXONOMY_CODES_CACHE", {})
    monkeypatch.setattr(npi, "_CLASSIFICATION_NPI_CACHE", {})
    monkeypatch.setattr(npi, "_npi_canonical_publication_identity", AsyncMock(return_value="publication-1"))
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
    assert "healthcare_provider_primary_taxonomy_switch = 'Y'" in str(session.execute.await_args.args[0])

    await npi._get_classification_npi_list(
        "Family Medicine",
        primary_only=False,
        session=session,
    )
    assert "healthcare_provider_primary_taxonomy_switch" not in str(session.execute.await_args.args[0])
    npi._npi_canonical_publication_identity.assert_awaited_with(session=session)


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
