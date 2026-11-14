# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native NPI caches stay disabled without portable ordinary-write invalidation."""

import datetime as dt
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from api.endpoint import npi
from api.npi_native_cache_identity import npi_native_cache_identity


def revision_row():
    lineage = str(uuid4())
    return {
        "singleton": True,
        "local_lineage_id": lineage,
        "local_generation": 4,
        "origin_lineage_id": lineage,
        "origin_generation": 4,
        "published_at": dt.datetime(2026, 1, 1, tzinfo=dt.UTC),
        "relation_oids": [10, 11, 12, 13, 14, 15],
        "canonical_publication_ref": "nppub1_" + "a" * 43,
        "canonical_publication_generation": 2,
        "canonical_chain_ref": "penpc1_" + "b" * 43,
        "canonical_import_date": dt.date(2026, 1, 1),
    }


def native_rows(rows):
    return SimpleNamespace(mappings=lambda: SimpleNamespace(all=lambda: rows))


@pytest.mark.asyncio
async def test_current_logical_revision_stays_uncached_without_invalidation_contract(monkeypatch):
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "mrf")
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    row = revision_row()
    execute = AsyncMock(return_value=native_rows([row]))
    monkeypatch.setattr(npi, "_execute_stmt", execute)
    assert await npi._npi_canonical_publication_identity() is None
    execute.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("rows", [[], [revision_row(), revision_row()]])
async def test_absent_or_multiple_native_revisions_cannot_key_cache(rows):
    assert await npi_native_cache_identity(AsyncMock(return_value=native_rows(rows)), "mrf") is None


@pytest.mark.asyncio
async def test_tracked_generation_without_a_receipt_stays_uncached():
    row = revision_row()
    for field in (
        "canonical_publication_ref",
        "canonical_publication_generation",
        "canonical_chain_ref",
        "canonical_import_date",
    ):
        row[field] = None
    assert await npi_native_cache_identity(AsyncMock(return_value=native_rows([row])), "mrf") is None
    # Origin adoption alone cannot establish ordinary-write invalidation.
    adopted_by_field = {**row, "origin_generation": 5, "relation_oids": [20, 21, 22, 23, 24, 25]}
    later = await npi_native_cache_identity(AsyncMock(return_value=native_rows([adopted_by_field])), "mrf")
    assert later is None


@pytest.mark.asyncio
async def test_untracked_legacy_counter_cannot_key_cache():
    row = revision_row()
    for field in (
        "origin_lineage_id",
        "origin_generation",
        "published_at",
        "relation_oids",
        "canonical_publication_ref",
        "canonical_publication_generation",
        "canonical_chain_ref",
        "canonical_import_date",
    ):
        row[field] = None
    assert await npi_native_cache_identity(AsyncMock(return_value=native_rows([row])), "mrf") is None


@pytest.mark.asyncio
@pytest.mark.parametrize("changed", ["generation", "origin", "singleton", "lineage"])
async def test_malformed_native_revision_fails_closed_at_api_boundary(monkeypatch, changed):
    row = revision_row()
    field, value = {
        "generation": ("local_generation", True),
        "origin": ("origin_generation", 0),
        "singleton": ("singleton", False),
        "lineage": ("local_lineage_id", "not-a-lineage"),
    }[changed]
    row[field] = value
    execute = AsyncMock(return_value=native_rows([row]))
    monkeypatch.setattr(npi, "_execute_stmt", execute)
    assert await npi._npi_canonical_publication_identity() is None


@pytest.mark.asyncio
async def test_native_counter_unavailable_bypasses_cache(monkeypatch):
    execute = AsyncMock(side_effect=RuntimeError("native counter unavailable"))
    monkeypatch.setattr(npi, "_execute_stmt", execute)
    assert await npi._npi_canonical_publication_identity() is None


@pytest.mark.asyncio
@pytest.mark.parametrize("schema", [None, "bad;schema", "x" * 64])
async def test_invalid_schema_is_refused_before_query(schema):
    execute = AsyncMock()
    with pytest.raises(ValueError, match="schema"):
        await npi_native_cache_identity(execute, schema)
    execute.assert_not_awaited()


@pytest.mark.parametrize("legacy", [None, [], [10] * 6, [20, 21, 22, 23, 24, 25]])
async def test_copied_metadata_ignores_legacy_physical_ids(legacy):
    row = revision_row()
    row["relation_oids"] = legacy
    key = await npi_native_cache_identity(AsyncMock(return_value=native_rows([row])), "mrf")
    assert key is None


@pytest.mark.asyncio
async def test_uncached_native_primary_count_reads_current_result_each_time(monkeypatch):
    monkeypatch.setattr(npi, "ENABLE_NPI_SCHEMA_CACHE", True)
    monkeypatch.setattr(npi, "_NPI_PRIMARY_TOTAL_CACHE_STATE", {"entry": None})
    monkeypatch.setattr(npi, "_address_serving_model", AsyncMock(return_value=npi.NPIAddress))
    execute = AsyncMock(side_effect=AssertionError("native cache metadata must not be queried"))
    monkeypatch.setattr(npi, "_execute_stmt", execute)
    scalar = AsyncMock(side_effect=[11, 22])
    monkeypatch.setattr(npi.db, "scalar", scalar)

    assert await npi._fast_primary_npi_count() == 11
    assert await npi._fast_primary_npi_count() == 22
    assert scalar.await_count == 2
    execute.assert_not_awaited()
