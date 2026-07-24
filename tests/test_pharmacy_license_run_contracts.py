"""Pharmacy-license run lifecycle and database index contracts."""

import importlib
import io
import json
import zipfile
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest


pharmacy_license = importlib.import_module("process.pharmacy_license")


def _state(state_code: str = "TX") -> pharmacy_license.StateSource:
    return pharmacy_license.StateSource(
        state_code=state_code,
        state_name={"TX": "Texas", "FL": "Florida", "CO": "Colorado"}.get(
            state_code,
            "Sample State",
        ),
        board_url=f"https://licenses.example.test/{state_code.lower()}",
    )


@pytest.mark.asyncio
async def test_index_builder_creates_primary_filtered_and_method_indexes(monkeypatch):
    class IndexedModel:
        __tablename__ = "sample_table"
        __my_index_elements__ = ("id", "kind")
        __my_additional_indexes__ = (
            {},
            {"index_elements": ("state",)},
            {
                "name": "sample_gin_idx",
                "index_elements": ("tokens",),
                "using": "gin",
                "where": "tokens IS NOT NULL",
            },
        )

    statements = []

    async def record_status(statement):
        statements.append(statement)

    monkeypatch.setattr(pharmacy_license.db, "status", record_status)
    await pharmacy_license._ensure_indexes(IndexedModel, "mrf")

    assert any("CREATE UNIQUE INDEX" in statement for statement in statements)
    assert any("sample_table_state_idx" in statement for statement in statements)
    assert any("USING gin" in statement and "WHERE tokens IS NOT NULL" in statement for statement in statements)

    statements.clear()
    await pharmacy_license._ensure_indexes(IndexedModel, "mrf", include_additional=False)
    assert len(statements) == 1


class _RunEvents:
    def __init__(self):
        self.run_updates = []
        self.snapshot_updates = []
        self.coverage_updates = []
        self.control_updates = []
        self.progress_updates = []
        self.cleanup_calls = []


class _RunClientSession:
    def __init__(self, **options_by_name):
        self.options_by_name = options_by_name

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_exc):
        return False


async def _import_state_with_recoverable_failure(_session, state_source, **_kwargs):
    if state_source.state_code == "CO":
        raise ValueError("source shape changed")
    return pharmacy_license.StateImportStats(
        supported=True,
        status="completed",
        source_url="https://files.example.test/tx.csv",
        unsupported_reason=None,
        error_text=None,
        row_count_parsed=2,
        row_count_matched=1,
        row_count_dropped=1,
        row_count_inserted=1,
        metadata={"adapter": "direct"},
    )


def _configure_import_run_harness(monkeypatch, run_events, state_sources):
    """Record import-run state transitions while replacing external effects."""
    async def cleanup(name):
        run_events.cleanup_calls.append(name)

    async def record_run(payload):
        run_events.run_updates.append(payload)

    async def record_snapshot(payload):
        run_events.snapshot_updates.append(payload)

    async def record_coverage(payload):
        run_events.coverage_updates.append(payload)

    async def mark_control(run_id, **payload):
        run_events.control_updates.append({"run_id": run_id, **payload})

    monkeypatch.setattr(pharmacy_license, "PHARM_LICENSE_DEFER_ADDITIONAL_INDEXES", True)
    monkeypatch.setattr(pharmacy_license, "PHARM_LICENSE_DROP_ADDITIONAL_INDEXES_BEFORE_IMPORT", True)
    monkeypatch.setattr(pharmacy_license, "ensure_database", AsyncMock())
    monkeypatch.setattr(pharmacy_license, "_ensure_tables", AsyncMock(return_value="mrf"))
    monkeypatch.setattr(pharmacy_license, "_truncate_stage_table", lambda _schema: cleanup("truncate"))
    monkeypatch.setattr(pharmacy_license, "_drop_secondary_indexes", lambda _schema: cleanup("drop_indexes"))
    monkeypatch.setattr(pharmacy_license, "_ensure_secondary_indexes", lambda _schema: cleanup("ensure_indexes"))
    monkeypatch.setattr(pharmacy_license, "_analyze_tables", lambda _schema: cleanup("analyze"))
    monkeypatch.setattr(pharmacy_license, "download_it", AsyncMock(return_value="<html />"))
    monkeypatch.setattr(pharmacy_license, "_parse_fda_state_sources", lambda _html: state_sources)
    monkeypatch.setattr(pharmacy_license, "_import_state_source", _import_state_with_recoverable_failure)
    monkeypatch.setattr(pharmacy_license, "_materialize_snapshot", AsyncMock(return_value=1))
    monkeypatch.setattr(pharmacy_license, "_upsert_run", record_run)
    monkeypatch.setattr(pharmacy_license, "_upsert_snapshot", record_snapshot)
    monkeypatch.setattr(pharmacy_license, "_upsert_coverage", record_coverage)
    monkeypatch.setattr(pharmacy_license, "mark_control_run", mark_control)
    monkeypatch.setattr(
        pharmacy_license,
        "enqueue_live_progress",
        lambda **payload: run_events.progress_updates.append(payload),
    )
    monkeypatch.setattr(pharmacy_license.aiohttp, "ClientSession", _RunClientSession)


@pytest.mark.asyncio
async def test_import_run_completes_healthy_states_while_recording_a_recoverable_state_failure(monkeypatch):
    """A recoverable state failure remains visible without aborting healthy states."""
    run_events = _RunEvents()
    _configure_import_run_harness(monkeypatch, run_events, [_state("TX"), _state("CO")])

    await pharmacy_license.pharmacy_license_start(
        {},
        {"run_id": "run-contract", "import_id": "import-contract", "test_mode": False},
    )

    completed_run = run_events.run_updates[-1]
    assert completed_run["status"] == "completed"
    assert completed_run["source_summary"] == {
        "test_mode": False,
        "states": 2,
        "supported_states": 1,
        "unsupported_states": 1,
        "parsed_rows": 2,
        "matched_rows": 1,
        "dropped_rows": 1,
        "inserted_rows": 1,
    }
    assert run_events.control_updates[-1]["status"] == "succeeded"
    assert any(
        update["status"] == "failed" and update["state_code"] == "CO"
        for update in run_events.snapshot_updates
    )
    assert any(
        update["unsupported_reason"] == "state_import_failed"
        for update in run_events.coverage_updates
    )
    assert {"drop_indexes", "ensure_indexes", "analyze"}.issubset(run_events.cleanup_calls)
    assert any(
        update["phase"] == "pharmacy-license state failed"
        for update in run_events.progress_updates
    )


@pytest.mark.asyncio
async def test_table_setup_defers_only_large_serving_indexes(monkeypatch):
    created_tables = []
    index_modes = []

    async def create_table(table, **_kwargs):
        created_tables.append(table.name)

    async def ensure_indexes(model, _schema, *, include_additional=True):
        index_modes.append((model.__tablename__, include_additional))

    monkeypatch.setattr(pharmacy_license, "PHARM_LICENSE_DEFER_ADDITIONAL_INDEXES", True)
    monkeypatch.setattr(pharmacy_license.db, "status", AsyncMock())
    monkeypatch.setattr(pharmacy_license.db, "create_table", create_table)
    monkeypatch.setattr(pharmacy_license, "_ensure_indexes", ensure_indexes)

    schema = await pharmacy_license._ensure_tables()

    assert schema == "mrf"
    assert len(created_tables) == 6
    deferred_tables = {name for name, include in index_modes if not include}
    assert deferred_tables == {
        pharmacy_license.PharmacyLicenseRecord.__tablename__,
        pharmacy_license.PharmacyLicenseRecordHistory.__tablename__,
    }


@pytest.mark.asyncio
async def test_additional_index_drop_skips_empty_definitions_and_derives_default_name(monkeypatch):
    class IndexedModel:
        __tablename__ = "sample_table"
        __my_additional_indexes__ = (
            {},
            {"index_elements": ("state",)},
            {"name": "named_idx", "index_elements": ("kind",)},
        )

    statements = []
    monkeypatch.setattr(pharmacy_license.db, "status", lambda statement: statements.append(statement) or AsyncMock()())

    await pharmacy_license._drop_additional_indexes(IndexedModel, "mrf")

    assert statements == [
        "DROP INDEX IF EXISTS mrf.sample_table_state_idx;",
        "DROP INDEX IF EXISTS mrf.named_idx;",
    ]


@pytest.mark.asyncio
async def test_materialization_without_address_canon_still_promotes_and_clears_stage(monkeypatch):
    statements = []
    monkeypatch.setattr(pharmacy_license, "source_enabled", lambda _source: False)
    monkeypatch.setattr(pharmacy_license.db, "status", lambda statement, **_params: statements.append(str(statement)) or AsyncMock()())
    monkeypatch.setattr(pharmacy_license.db, "all", AsyncMock(return_value=[(2,)]))

    row_count = await pharmacy_license._materialize_snapshot("mrf", "snapshot", "run")

    assert row_count == 2
    assert len(statements) == 3
    assert statements[-1].startswith("DELETE FROM mrf.pharmacy_license_record_stage")


@pytest.mark.asyncio
async def test_empty_source_catalog_fails_without_unneeded_index_restore(monkeypatch):
    run_updates = []
    control_updates = []
    truncate = AsyncMock()
    ensure_indexes = AsyncMock()

    monkeypatch.setattr(pharmacy_license, "PHARM_LICENSE_DEFER_ADDITIONAL_INDEXES", False)
    monkeypatch.setattr(pharmacy_license, "PHARM_LICENSE_DROP_ADDITIONAL_INDEXES_BEFORE_IMPORT", False)
    monkeypatch.setattr(pharmacy_license, "ensure_database", AsyncMock())
    monkeypatch.setattr(pharmacy_license, "_ensure_tables", AsyncMock(return_value="mrf"))
    monkeypatch.setattr(pharmacy_license, "_truncate_stage_table", truncate)
    monkeypatch.setattr(pharmacy_license, "_ensure_secondary_indexes", ensure_indexes)
    monkeypatch.setattr(pharmacy_license, "download_it", AsyncMock(return_value="<html />"))
    monkeypatch.setattr(pharmacy_license, "_parse_fda_state_sources", lambda _html: [])

    async def record_run(payload):
        run_updates.append(payload)

    async def record_control(run_id, **payload):
        control_updates.append({"run_id": run_id, **payload})

    monkeypatch.setattr(pharmacy_license, "_upsert_run", record_run)
    monkeypatch.setattr(pharmacy_license, "mark_control_run", record_control)

    with pytest.raises(RuntimeError, match="No state board sources"):
        await pharmacy_license.pharmacy_license_start({}, {"run_id": "run-empty"})

    assert run_updates[-1]["status"] == "failed"
    assert control_updates[-1]["status"] == "failed"
    assert truncate.await_count == 2
    ensure_indexes.assert_not_awaited()


@pytest.mark.asyncio
async def test_empty_batch_and_models_without_index_metadata_are_noops(monkeypatch):
    push = AsyncMock()
    status = AsyncMock()
    monkeypatch.setattr(pharmacy_license, "push_objects", push)
    monkeypatch.setattr(pharmacy_license.db, "status", status)

    await pharmacy_license._flush_stage_batch([])
    batch_rows = [{"id": 1}]
    await pharmacy_license._flush_stage_batch(batch_rows)

    class PlainModel:
        __tablename__ = "plain"

    assert pharmacy_license._iter_additional_indexes(PlainModel) == []
    await pharmacy_license._ensure_indexes(PlainModel, "mrf")
    push.assert_awaited_once()
    assert batch_rows == []
    status.assert_not_awaited()


@pytest.mark.asyncio
async def test_secondary_index_helpers_visit_both_serving_models(monkeypatch):
    dropped_indexes = []
    ensured_indexes = []

    async def drop_indexes(model, schema):
        dropped_indexes.append((model.__tablename__, schema))

    async def ensure_indexes(model, schema, *, include_additional=True):
        ensured_indexes.append((model.__tablename__, schema, include_additional))

    monkeypatch.setattr(pharmacy_license, "_drop_additional_indexes", drop_indexes)
    monkeypatch.setattr(pharmacy_license, "_ensure_indexes", ensure_indexes)

    await pharmacy_license._drop_secondary_indexes("mrf")
    await pharmacy_license._ensure_secondary_indexes("mrf")

    assert [name for name, _schema in dropped_indexes] == [
        pharmacy_license.PharmacyLicenseRecord.__tablename__,
        pharmacy_license.PharmacyLicenseRecordHistory.__tablename__,
    ]
    assert all(include_additional for _name, _schema, include_additional in ensured_indexes)
