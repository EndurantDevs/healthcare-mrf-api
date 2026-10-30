# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Normal catalog pruning must preserve the aliases required by served evidence."""

import importlib
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

fhir = importlib.import_module("process.provider_directory_fhir")


@pytest.mark.asyncio
@pytest.mark.parametrize("native_rows", [False, True])
async def test_catalog_pruning_locks_exact_candidates_before_retention_read(monkeypatch, native_rows):
    calls = []

    async def all_rows(statement, **parameters):
        calls.append((statement, parameters))
        if "FOR UPDATE" in statement:
            rows = [{"source_id": value} for value in ("source_old", "source_unused", "source_old")]
        else:
            rows = [{"source_id": "source_old"}]
        return [SimpleNamespace(_mapping=row) for row in rows] if native_rows else rows

    monkeypatch.setattr(fhir.db, "scalar", AsyncMock(return_value="read committed"))
    monkeypatch.setattr(fhir.db, "all", all_rows)
    assert await fhir._catalog_deletable_source_ids(["source_current"]) == ["source_unused"]
    assert len(calls) == 2
    assert "ORDER BY source_id FOR UPDATE" in calls[0][0]
    assert calls[0][1] == {"source_ids": ["source_current"]}
    assert "unknown_membership" in calls[1][0]
    assert calls[1][1] == {"source_ids": ["source_old", "source_unused"]}


@pytest.mark.asyncio
@pytest.mark.parametrize("isolation", ["repeatable read", "serializable", "read uncommitted"])
async def test_catalog_pruning_refuses_a_stale_statement_snapshot(monkeypatch, isolation):
    all_rows = AsyncMock()
    monkeypatch.setattr(fhir.db, "scalar", AsyncMock(return_value=isolation))
    monkeypatch.setattr(fhir.db, "all", all_rows)
    with pytest.raises(RuntimeError, match="catalog_cleanup_read_committed_required"):
        await fhir._catalog_deletable_source_ids(["source_current"])
    all_rows.assert_not_awaited()


@pytest.mark.asyncio
async def test_catalog_pruning_skips_retention_read_without_locked_candidates(monkeypatch):
    all_rows = AsyncMock(return_value=[])
    monkeypatch.setattr(fhir.db, "scalar", AsyncMock(return_value="read committed"))
    monkeypatch.setattr(fhir.db, "all", all_rows)
    assert await fhir._catalog_deletable_source_ids(["source_current"]) == []
    all_rows.assert_awaited_once()


def test_retained_catalog_query_uses_header_membership_and_indexed_overlay_existence():
    statement = fhir._retained_catalog_source_ids_sql()
    assert "WHERE published_at IS NOT NULL" in statement
    assert "publication_metadata_summary_json::jsonb -> 'source_ids'" in statement
    assert "publication_metadata_json::jsonb -> 'source_ids'" in statement
    assert "overlay.source_id = source.source_id" in statement
    assert "publications.source_ids ? source.source_id" in statement
    assert "jsonb_typeof(source_ids) IS DISTINCT FROM 'array'" in statement
    assert "source_ids = '[]'::jsonb" in statement
    assert "jsonb_typeof(value) IS DISTINCT FROM 'string'" in statement
    assert "is_current" not in statement
    assert "provider_directory_dataset_resource" not in statement
    assert "DISTINCT overlay" not in statement


@pytest.mark.asyncio
async def test_catalog_pruning_rolls_back_all_dependent_deletes_on_failure(monkeypatch):
    events = []

    @asynccontextmanager
    async def transaction():
        events.append("begin")
        try:
            yield
        except Exception:
            events.append("rollback")
            raise
        events.append("commit")

    deletable = AsyncMock(return_value=["source_unused"])
    status = AsyncMock(side_effect=[1, RuntimeError("delete refused")])
    monkeypatch.setattr(fhir.db, "transaction", transaction)
    monkeypatch.setattr(fhir, "_catalog_deletable_source_ids", deletable)
    monkeypatch.setattr(fhir.db, "status", status)
    with pytest.raises(RuntimeError, match="delete refused"):
        await fhir._delete_source_catalog_rows_except(["source_current"])
    assert events == ["begin", "rollback"]
    assert status.await_count == 2
    for call in status.await_args_list:
        assert call.kwargs["source_ids"] == ["source_unused"]
        assert "WHERE source_id = ANY" in call.args[0]


@pytest.mark.asyncio
async def test_catalog_pruning_keeps_all_rows_when_every_alias_is_referenced(monkeypatch):
    @asynccontextmanager
    async def transaction():
        yield

    monkeypatch.setattr(fhir.db, "transaction", transaction)
    monkeypatch.setattr(fhir, "_catalog_deletable_source_ids", AsyncMock(return_value=[]))
    status = AsyncMock()
    monkeypatch.setattr(fhir.db, "status", status)
    assert await fhir._delete_source_catalog_rows_except(["source_current"]) == {}
    status.assert_not_awaited()
