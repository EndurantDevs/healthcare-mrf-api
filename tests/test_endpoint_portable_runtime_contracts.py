# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Portable endpoint helper contracts shared by local and Linux test runs."""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock

import pytest

from api.endpoint import npi as npi_endpoint
from api.endpoint import pricing


class _Result:
    def __init__(
        self,
        rows: list[Any] | None = None,
        *,
        scalar_value: Any = None,
    ) -> None:
        self._rows = list(rows or [])
        self._scalar_value = scalar_value

    def scalar(self) -> Any:
        return self._scalar_value

    def fetchall(self) -> list[Any]:
        return list(self._rows)

    def all(self) -> list[Any]:
        return list(self._rows)

    def __iter__(self):
        return iter(self._rows)


class _QueueSession:
    def __init__(self, results: list[_Result]) -> None:
        self._results = list(results)
        self.executions: list[tuple[tuple[Any, ...], dict[str, Any]]] = []

    async def execute(self, *args: Any, **kwargs: Any) -> _Result:
        self.executions.append((args, kwargs))
        return self._results.pop(0)


class _AcquireContext:
    def __init__(self, connection: Any) -> None:
        self._connection = connection

    async def __aenter__(self) -> Any:
        return self._connection

    async def __aexit__(self, *_exc_info: Any) -> None:
        return None


class _NpiConnection:
    def __init__(self, rows_by_call: list[list[Any]]) -> None:
        self._rows_by_call = list(rows_by_call)

    async def all(self, *_args: Any, **_kwargs: Any) -> list[Any]:
        return self._rows_by_call.pop(0)


class _MappingRow:
    def __init__(self, npi: Any) -> None:
        self._mapping = {"npi": npi}


def test_zip_radius_cache_fails_closed_when_disabled_or_expired(monkeypatch):
    """Disable cache reads/writes and evict stale rows deterministically."""

    cache_key = ("00001", 10.0, "AA", 8)
    pricing._ZIP_RADIUS_ROWS_CACHE.clear()
    monkeypatch.setattr(pricing, "_ZIP_RADIUS_ROWS_CACHE_MAX_KEYS", 8)
    monkeypatch.setattr(pricing, "_ZIP_RADIUS_ROWS_CACHE_TTL_SECONDS", 0.0)

    assert pricing._zip_radius_rows_cache_get(cache_key) is None
    pricing._zip_radius_rows_cache_put(cache_key, [{"zip5": "00001"}])
    assert cache_key not in pricing._ZIP_RADIUS_ROWS_CACHE

    monkeypatch.setattr(pricing, "_ZIP_RADIUS_ROWS_CACHE_TTL_SECONDS", 5.0)
    monkeypatch.setattr(pricing.time, "monotonic", lambda: 20.0)
    pricing._ZIP_RADIUS_ROWS_CACHE[cache_key] = (
        10.0,
        ({"zip5": "00001"},),
    )
    assert pricing._zip_radius_rows_cache_get(cache_key) is None
    assert cache_key not in pricing._ZIP_RADIUS_ROWS_CACHE


@pytest.mark.asyncio
async def test_pricing_helpers_reject_non_queryable_inputs():
    """Return bounded empty results for invalid ZIP, code, and payload inputs."""

    assert await pricing._zip_radius_rows(
        object(),
        zip5="",
        radius_miles=10.0,
    ) == []
    assert pricing._is_broad_office_visit_cpt("CPT", "not-a-code") is False
    assert (
        pricing._annotate_ptg2_query_payload(
            None,
            plan_id_type="group",
            year=2025,
            has_plan_scope=True,
        )
        is None
    )


@pytest.mark.asyncio
async def test_resolve_year_uses_configured_default(monkeypatch):
    """Prefer the configured year without querying the database."""

    monkeypatch.setattr(pricing, "PRICING_DEFAULT_YEAR", 2025)
    assert await pricing._resolve_year(object(), object(), None) == (
        2025,
        "env",
    )


@pytest.mark.asyncio
async def test_table_columns_filters_rows_and_populates_cache(monkeypatch):
    """Retain valid column names and cache an immutable tuple of them."""

    pricing._PRICING_TABLE_COLUMNS_CACHE.clear()
    monkeypatch.setattr(pricing, "ENABLE_PRICING_SCHEMA_CACHE", True)
    session = _QueueSession(
        [_Result(rows=[("npi",), (), (None,), ("state",)])]
    )

    assert await pricing._table_columns(session, "portable_table") == {
        "npi",
        "state",
    }
    assert "mrf.portable_table" in pricing._PRICING_TABLE_COLUMNS_CACHE


@pytest.mark.asyncio
async def test_unified_address_source_requires_complete_schema(monkeypatch):
    """Keep legacy reads unless every required unified column is present."""

    monkeypatch.setenv(
        pricing.ADDRESS_SERVING_SOURCE_ENV,
        pricing.ADDRESS_SERVING_SOURCE_UNIFIED,
    )
    incomplete_columns = (
        set(pricing.GROUP_PLAN_UNIFIED_BASE_ADDRESS_COLUMNS) - {"npi"}
    )
    monkeypatch.setattr(
        pricing,
        "_table_columns",
        AsyncMock(return_value=incomplete_columns),
    )

    assert await pricing._group_plan_provider_address_source(object()) == (
        "mrf.npi_address",
        False,
        False,
        False,
    )


@pytest.mark.asyncio
async def test_unified_address_source_reports_optional_capabilities(monkeypatch):
    """Expose coverage arrays and the plan bridge only when both are proven."""

    monkeypatch.setenv(
        pricing.ADDRESS_SERVING_SOURCE_ENV,
        pricing.ADDRESS_SERVING_SOURCE_UNIFIED,
    )
    complete_columns = set(pricing.GROUP_PLAN_UNIFIED_BASE_ADDRESS_COLUMNS)
    complete_columns.update(
        {"group_plan_array", "ptg_plan_array", "location_key"}
    )
    monkeypatch.setattr(
        pricing,
        "_table_columns",
        AsyncMock(return_value=complete_columns),
    )
    monkeypatch.setattr(
        pricing,
        "_is_table_available",
        AsyncMock(return_value=True),
    )

    assert await pricing._group_plan_provider_address_source(object()) == (
        "mrf.entity_address_unified",
        True,
        True,
        True,
    )


@pytest.mark.asyncio
async def test_terminology_query_applies_fuzzy_scope_and_public_shape(
    monkeypatch,
):
    """Apply fuzzy, system, and broad-term filters before shaping matches."""

    monkeypatch.setattr(
        pricing,
        "_is_terminology_available",
        AsyncMock(return_value=True),
    )
    session = _QueueSession(
        [
            _Result(
                rows=[
                    {
                        "domain": "procedure",
                        "synonym": "Synthetic service",
                        "term_key": "synthetic service",
                        "target_system": "CPT",
                        "target_code": "00000",
                        "confidence": "0.9",
                        "metadata_json": '{"scope":"portable"}',
                    }
                ]
            )
        ]
    )

    matches = await pricing._query_terminology(
        session,
        domain="procedure",
        term="Synthetic service",
        target_systems=("cpt",),
        include_broad=False,
        limit=5,
    )

    assert matches[0]["target_code"] == "00000"
    assert matches[0]["metadata"] == {"scope": "portable"}
    assert len(session.executions) == 1


@pytest.mark.asyncio
async def test_rx_crosswalk_preserves_internal_and_external_matches():
    """Combine direct internal codes with every resolved external code."""

    session = _QueueSession(
        [_Result(rows=[("RX-002",), {"to_code": "RX-003"}])]
    )
    matches = await pricing._internal_rx_codes_from_terminology(
        session,
        [
            {
                "target_system": pricing.INTERNAL_RX_CODE_SYSTEM,
                "target_code": "RX-001",
            },
            {"target_system": "RXNORM", "target_code": "12345"},
        ],
    )

    assert matches == ["RX-001", "RX-002", "RX-003"]


@pytest.mark.asyncio
async def test_npi_classification_queries_cover_connection_and_mapping_rows(
    monkeypatch,
):
    """Support pooled classification reads and mapping-backed NPI rows."""

    npi_endpoint._CLASSIFICATION_TAXONOMY_CODES_CACHE.clear()
    npi_endpoint._CLASSIFICATION_NPI_CACHE.clear()
    connection = _NpiConnection(
        [[("0000000000",)], [_MappingRow("1234567890")]]
    )
    monkeypatch.setattr(
        npi_endpoint.db,
        "acquire",
        lambda: _AcquireContext(connection),
    )

    assert await npi_endpoint._get_taxonomy_codes_for_classification(
        "Synthetic specialty"
    ) == ["0000000000"]
    monkeypatch.setattr(
        npi_endpoint,
        "_get_taxonomy_codes_for_classification",
        AsyncMock(return_value=["0000000000"]),
    )
    assert await npi_endpoint._get_classification_npi_list(
        "Synthetic specialty"
    ) == [1234567890]


@pytest.mark.asyncio
async def test_npi_table_availability_caches_resolved_presence(monkeypatch):
    """Cache a successful table lookup after one database execution."""

    npi_endpoint._TABLE_EXISTS_CACHE.clear()
    monkeypatch.setattr(npi_endpoint, "ENABLE_NPI_SCHEMA_CACHE", True)
    execute_statement = AsyncMock(
        return_value=_Result(rows=[("mrf.synthetic_table",)])
    )
    monkeypatch.setattr(npi_endpoint, "_execute_stmt", execute_statement)

    assert await npi_endpoint._is_table_available(
        "synthetic_table",
        session=object(),
    )
    assert await npi_endpoint._is_table_available(
        "synthetic_table",
        session=object(),
    )
    assert execute_statement.await_count == 1


def test_npi_cache_set_preserves_state_when_cache_is_disabled(monkeypatch):
    """Disabling schema caching must not retain a newly observed value."""

    cached_values_by_key: dict[str, tuple[float, object]] = {}
    value = object()
    monkeypatch.setattr(npi_endpoint, "ENABLE_NPI_SCHEMA_CACHE", False)

    assert (
        npi_endpoint._cache_set(
            cached_values_by_key,
            "synthetic",
            value,
        )
        is value
    )
    assert cached_values_by_key == {}
