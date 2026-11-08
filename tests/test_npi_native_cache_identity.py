# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Legacy publication and native revision identities remain separate cache keys."""

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
async def test_missing_legacy_match_uses_actual_native_revision_without_changing_legacy_priority(monkeypatch):
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "mrf")
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    row = revision_row()
    execute = AsyncMock(side_effect=[SimpleNamespace(all=lambda: []), native_rows([row])])
    monkeypatch.setattr(npi, "_execute_stmt", execute)
    expected = f"native:{row['local_lineage_id']}:4:{row['canonical_publication_ref']}"
    assert await npi._npi_canonical_publication_identity() == expected
    assert execute.await_count == 2
    legacy = "nppub1_" + "c" * 43
    execute.reset_mock(side_effect=True)
    execute.return_value = SimpleNamespace(all=lambda: [(7, legacy)])
    assert await npi._npi_canonical_publication_identity() == "7:" + legacy
    execute.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("rows", [[], [revision_row(), revision_row()]])
async def test_absent_or_multiple_native_revisions_cannot_key_cache(rows):
    assert await npi_native_cache_identity(AsyncMock(return_value=native_rows(rows)), "mrf") is None


@pytest.mark.asyncio
async def test_generation_without_canonical_provenance_cannot_key_cache():
    row = revision_row()
    for field in (
        "canonical_publication_ref",
        "canonical_publication_generation",
        "canonical_chain_ref",
        "canonical_import_date",
    ):
        row[field] = None
    assert await npi_native_cache_identity(AsyncMock(return_value=native_rows([row])), "mrf") is None


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
@pytest.mark.parametrize("changed", ["generation", "oids", "singleton", "provenance"])
async def test_malformed_native_revision_fails_closed_at_api_boundary(monkeypatch, changed):
    row = revision_row()
    field, value = {
        "generation": ("local_generation", True),
        "oids": ("relation_oids", [10] * 6),
        "singleton": ("singleton", False),
        "provenance": ("canonical_publication_ref", "not-a-receipt"),
    }[changed]
    row[field] = value
    execute = AsyncMock(side_effect=[SimpleNamespace(all=lambda: []), native_rows([row])])
    monkeypatch.setattr(npi, "_execute_stmt", execute)
    assert await npi._npi_canonical_publication_identity() is None


@pytest.mark.asyncio
async def test_native_counter_unavailable_bypasses_cache(monkeypatch):
    execute = AsyncMock(side_effect=[SimpleNamespace(all=lambda: []), RuntimeError("native counter unavailable")])
    monkeypatch.setattr(npi, "_execute_stmt", execute)
    assert await npi._npi_canonical_publication_identity() is None


@pytest.mark.asyncio
@pytest.mark.parametrize("schema", [None, "bad;schema", "x" * 64])
async def test_invalid_schema_is_refused_before_query(schema):
    execute = AsyncMock()
    with pytest.raises(ValueError, match="schema"):
        await npi_native_cache_identity(execute, schema)
    execute.assert_not_awaited()
