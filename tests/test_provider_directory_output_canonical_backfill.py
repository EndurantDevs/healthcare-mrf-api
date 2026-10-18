# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Fixed canonical projections and orchestration boundaries, not native authority mocks."""

import asyncio
import importlib
from contextvars import ContextVar, copy_context
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_output_canonical_backfill as repair

directory = importlib.import_module("process.provider_directory_fhir")


def _directory():
    return SimpleNamespace(
        _schema=lambda: "mrf",
        _selected_resources=directory._selected_resources,
        _sql_string_literal=directory._sql_string_literal,
        RESOURCE_MODELS_BY_TYPE=directory.RESOURCE_MODELS_BY_TYPE,
        _PROVIDER_DIRECTORY_ARTIFACT_RELATION_OVERRIDES=ContextVar("canonical_override", default={}),
    )


@pytest.mark.parametrize("root", [*repair.TARGETS, *repair.CONTROLS, "provider_directory_location"])
def test_raw_scratch_mapping_refuses_before_database(root):
    owner = _directory()
    owner._PROVIDER_DIRECTORY_ARTIFACT_RELATION_OVERRIDES.set({root: "ordinary_scratch"})
    with pytest.raises(RuntimeError, match="canonical_scratch_custody_required"):
        repair._selected(owner, "Location")


@pytest.mark.parametrize("selection", ["Location,Location", ["Location", "Location"], '["Location","Location"]'])
def test_selection_preserves_duplicates_and_fixed_model_identity(selection):
    assert repair._selected(_directory(), selection) == ["Location", "Location"]


def test_model_substitution_refuses():
    owner = _directory()
    owner.RESOURCE_MODELS_BY_TYPE = {**owner.RESOURCE_MODELS_BY_TYPE, "Location": object()}
    with pytest.raises(RuntimeError, match="canonical_model_changed"):
        repair._selected(owner, "Location")


@pytest.mark.parametrize("target", repair.TARGETS)
def test_projection_is_complete_native_merge_without_live_dml(target):
    sql = repair.canonical_projection(_directory(), target, ["Location", "Practitioner", "Location"])
    assert "INSERT " not in sql and "UPDATE " not in sql and "ON CONFLICT" not in sql
    assert "incoming AS MATERIALIZED" in sql and "NOT EXISTS" in sql
    assert "COALESCE(NULLIF(src.canonical_api_base,''),NULLIF(src.api_base,''))" in sql
    assert "NULLIF(r.source_id,'') IS NOT NULL" in sql and "NULLIF(r.resource_id,'') IS NOT NULL" in sql
    assert 'JOIN "mrf"."provider_directory_source" src USING(source_id)' in sql
    assert sql.count('FROM "mrf"."provider_directory_location" r') == 1
    if target == repair.TARGETS[0]:
        assert "DISTINCT ON" in sql
        assert "r.updated_at DESC NULLS LAST,r.observed_at DESC NULLS LAST,r.source_id" in sql
        assert "to_jsonb(r)-'source_id'-'last_seen_run_id'-'observed_at'-'updated_at'" in sql
        assert "md5(" in sql and "COALESCE(old.first_seen_run_id,i.first_seen_run_id)" in sql
    else:
        assert "SELECT DISTINCT r.source_id" in sql and "DISTINCT ON" not in sql
        assert "first_seen_run_id" not in sql
    for column in repair.output_models()[target].__table__.columns:
        assert ' AS "' + str(column.name) + '"' in sql


def test_counts_keep_updated_even_when_equal_and_repeated_resource_totals():
    counts = [
        dict(resource_type="Location", canonical_rows=2, source_edge_rows=3),
        dict(resource_type="Practitioner", canonical_rows=1, source_edge_rows=1),
    ]
    assert repair._summary(["Location", "Practitioner", "Location"], counts) == dict(
        resources={
            "Location": {"canonical_rows": 2, "source_edge_rows": 3},
            "Practitioner": {"canonical_rows": 1, "source_edge_rows": 1},
        },
        canonical_rows=5,
        source_edge_rows=7,
    )
    sql = repair.count_projection(_directory(), ["Location", "Practitioner", "Location"])
    assert sql.count("AS resource_type") == 6
    assert "IS DISTINCT FROM" not in sql


@pytest.mark.asyncio
@pytest.mark.parametrize("failure_at", [0, 1])
async def test_control_cut_rights_are_required_without_grants(failure_at):
    connection = SimpleNamespace(fetchval=AsyncMock(side_effect=[True] * failure_at + [False]))
    with pytest.raises(RuntimeError, match="canonical_control_cut_fence_required"):
        await repair._require_control_fence_rights(connection, "mrf")
    assert connection.fetchval.await_count == failure_at + 1
    assert all("GRANT" not in call.args[0] for call in connection.fetchval.await_args_list)


@pytest.mark.asyncio
async def test_dispatch_preserves_request_and_stable_summary(monkeypatch, capsys):
    summary_by_name = {
        "resources": {"Location": {"canonical_rows": 1, "source_edge_rows": 2}},
        "canonical_rows": 1,
        "source_edge_rows": 2,
    }
    backfill = AsyncMock(return_value=summary_by_name)
    ensure_tables = AsyncMock()
    monkeypatch.setattr(repair, "backfill_canonical_resources", backfill)
    monkeypatch.setattr(directory, "_ensure_provider_directory_tables", ensure_tables)
    assert await directory.backfill_provider_directory_canonical_resources(resources="Location") == summary_by_name
    backfill.assert_awaited_once_with(directory, resources="Location")
    ensure_tables.assert_not_awaited()
    assert capsys.readouterr().out.startswith("PROVIDER_DIRECTORY_CANONICAL_BACKFILL_DONE\t")


def test_control_catalog_scopes_index_constraint_without_omitting_other_native_metadata():
    added = "WHERE k.conindid=i.indexrelid AND k.conrelid=c.oid)"
    assert repair.CONTROL_CATALOG_SQL.count(added) == 1
    assert repair.CONTROL_CATALOG_SQL.replace(added, "WHERE k.conindid=i.indexrelid)") == repair.OUTPUT_CATALOG_SQL


@pytest.mark.asyncio
@pytest.mark.parametrize("changed", [False, True])
async def test_final_fence_uses_fresh_factory_context_then_rechecks_all_inputs(monkeypatch, changed):
    mode = ContextVar("canonical_test_context", default="factory")
    factory_context = copy_context()
    mode.set("original-owner")
    calls = []

    async def fresh(directory, controls, connection):
        calls.append(("fresh", mode.get(), controls))

    async def lock(sql):
        calls.append(("lock", sql))

    expected_by_root = {"input": object()}
    capture = AsyncMock(return_value={} if changed else expected_by_root)
    monkeypatch.setattr(repair, "_fresh_controls", fresh)
    monkeypatch.setattr(repair.custody, "_capture", capture)
    capability = SimpleNamespace(policy=SimpleNamespace(schema="mrf"), connection=SimpleNamespace(execute=lock))
    if changed:
        with pytest.raises(RuntimeError, match="canonical_input_changed"):
            await repair._final_fence(None, capability, {"closed": "controls"}, factory_context, expected_by_root)
    else:
        await repair._final_fence(None, capability, {"closed": "controls"}, factory_context, expected_by_root)
    assert calls[0][0] == "lock" and "IN SHARE MODE NOWAIT" in calls[0][1]
    assert calls[1] == ("fresh", "factory", {"closed": "controls"})
    capture.assert_awaited_once_with(capability, {"input": "input"})


@pytest.mark.asyncio
async def test_final_fence_cancellation_joins_fresh_reader_cleanup(monkeypatch):
    started, cleaned = asyncio.Event(), asyncio.Event()

    async def fresh(*args):
        try:
            started.set()
            await asyncio.Future()
        finally:
            await asyncio.sleep(0)
            cleaned.set()

    monkeypatch.setattr(repair, "_fresh_controls", fresh)
    capture = AsyncMock()
    monkeypatch.setattr(repair.custody, "_capture", capture)
    capability = SimpleNamespace(policy=SimpleNamespace(schema="mrf"), connection=SimpleNamespace(execute=AsyncMock()))
    task = asyncio.create_task(repair._final_fence(None, capability, {}, copy_context(), {}))
    await asyncio.wait_for(started.wait(), 1)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert cleaned.is_set()
    capture.assert_not_awaited()
