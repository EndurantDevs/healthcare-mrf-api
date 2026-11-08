# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bound control statements to one issued NPI run without changing shared routing."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import select, text, update
from sqlalchemy.dialects import postgresql

from api import control_imports
from db.models import ImportRun, NPIData
from process import control_lifecycle
from process import npi_isolated_execution as isolated
from tests.test_npi_isolated_execution import RUN_ID, execution

CONTROL_SCHEMA = "npi_control_" + "a" * 32


def rendered(statement):
    translation = statement.get_execution_options()["schema_translate_map"]
    compiled = statement.compile(
        dialect=postgresql.dialect(), schema_translate_map=translation, render_schema_translate=True
    )
    assert f"{CONTROL_SCHEMA}.import_run" in str(compiled)
    assert "mrf.import_run" not in str(compiled)
    assert RUN_ID in compiled.params.values() and "npi" in compiled.params.values()
    return compiled


@pytest.mark.parametrize("schema", ["mrf", CONTROL_SCHEMA])
def test_control_namespace_preserves_legacy_and_fixed_private_names(schema):
    assert execution(control_schema=schema).control_schema == schema


@pytest.mark.parametrize(
    "schema", [None, "public", "pg_catalog", "npi_control_short", "npi_control_" + "A" * 32, CONTROL_SCHEMA + "x"]
)
def test_control_namespace_rejects_other_coordinates(schema):
    with pytest.raises(RuntimeError, match="authority differs"):
        execution(control_schema=schema)


@pytest.mark.parametrize("field", ["output_schema", "source_schema"])
def test_private_control_namespace_cannot_replace_candidate_storage(field):
    with pytest.raises(RuntimeError, match="authority differs"):
        execution(control_schema=CONTROL_SCHEMA, **{field: CONTROL_SCHEMA})


def test_unbound_statements_keep_their_original_target_and_options():
    for statement in (select(ImportRun), update(ImportRun).values(status="running")):
        assert isolated.npi_control_run_statement(statement) is statement
        assert "schema_translate_map" not in statement.get_execution_options()


@pytest.mark.asyncio
async def test_bound_routing_retains_claim_predicates_and_returning():
    statement = (
        update(ImportRun)
        .where(ImportRun.run_id == RUN_ID, ImportRun.status == "queued")
        .where(ImportRun.progress["attempt_id"].as_string() == RUN_ID + ":attempt")
        .values(status="running")
        .returning(ImportRun.run_id)
    )
    async with isolated.bind_npi_isolated_execution(execution(control_schema=CONTROL_SCHEMA)):
        routed = isolated.npi_control_run_statement(statement)
        compiled = rendered(routed)
        assert {"queued", "running", RUN_ID + ":attempt"} <= set(compiled.params.values())
        assert "RETURNING" in str(compiled) and "progress ->>" in str(compiled)
        rendered(isolated.npi_control_run_statement(routed))
    assert "schema_translate_map" not in statement.get_execution_options()


@pytest.mark.asyncio
async def test_bound_routing_refuses_other_models_runs_and_schema_maps():
    statements = (
        select(NPIData),
        update(NPIData),
        select(ImportRun, NPIData),
        select(ImportRun).where(select(NPIData.npi).exists()),
        text("SELECT 1"),
        select(ImportRun).execution_options(schema_translate_map={"mrf": "other"}),
    )
    async with isolated.bind_npi_isolated_execution(execution(control_schema=CONTROL_SCHEMA)):
        for statement in statements:
            with pytest.raises(RuntimeError, match="authority differs"):
                isolated.npi_control_run_statement(statement)
        with pytest.raises(RuntimeError, match="authority differs"):
            isolated.npi_control_run_statement(select(ImportRun), run_id="another_run")


@pytest.mark.asyncio
@pytest.mark.parametrize("bound_transaction", [False, True])
async def test_lifecycle_routes_both_existing_transaction_paths(monkeypatch, bound_transaction):
    session = SimpleNamespace(in_transaction=lambda: True, in_nested_transaction=lambda: False)
    binding = SimpleNamespace(session=session) if bound_transaction else None
    database = SimpleNamespace(
        _reader_binding=lambda: None,
        _transaction_binding=lambda: binding,
        _session_database_name=lambda _session: "sample_database",
        _database_override=None,
        connect=AsyncMock(),
        execute=AsyncMock(return_value=SimpleNamespace(all=lambda: [(RUN_ID,)])),
    )
    monkeypatch.setenv("HLTHPRT_DB_DATABASE", "sample_database")
    monkeypatch.setattr(control_lifecycle, "db", database)
    statement = update(ImportRun).where(ImportRun.run_id == RUN_ID).values(status="running")
    async with isolated.bind_npi_isolated_execution(execution(control_schema=CONTROL_SCHEMA)):
        assert await control_lifecycle._execute_control_run_update(statement) == 1
    rendered(database.execute.await_args.args[0])
    assert database._database_override is None


@pytest.mark.asyncio
async def test_get_run_routes_the_owned_row_and_preserves_normalization(monkeypatch):
    durable = SimpleNamespace(run_id=RUN_ID, importer="npi", status="queued", metrics={}, params={})
    database = SimpleNamespace(execute=AsyncMock(return_value=SimpleNamespace(scalar_one_or_none=lambda: durable)))
    monkeypatch.setattr(control_imports, "db", database)
    async with isolated.bind_npi_isolated_execution(execution(control_schema=CONTROL_SCHEMA)):
        result = await control_imports.get_import_run(RUN_ID)
        assert result["run_id"] == RUN_ID and result["status"] == "queued"
        with pytest.raises(RuntimeError, match="authority differs"):
            await control_imports.get_import_run("another_run")
    database.execute.assert_awaited_once()
    rendered(database.execute.await_args.args[0])


@pytest.mark.asyncio
@pytest.mark.parametrize("rowcount", [0, 1])
async def test_worker_failure_routes_update_and_lost_race_readback(monkeypatch, rowcount):
    run_by_field = {"run_id": RUN_ID, "importer": "npi", "status": "running", "progress": {}, "metrics": {}}
    latest_by_field = {**run_by_field, "status": "finalizing"}
    database = SimpleNamespace(
        execute=AsyncMock(
            side_effect=[
                SimpleNamespace(rowcount=rowcount),
                SimpleNamespace(scalar_one_or_none=lambda: latest_by_field),
            ]
        )
    )
    monkeypatch.setattr(control_imports, "db", database)
    monkeypatch.setattr(
        control_imports,
        "_active_worker_state",
        AsyncMock(
            return_value={
                "status": "failed",
                "items": [{"job_status": "failed", "failure": {"reason": "Error", "exitCode": 1}}],
            }
        ),
    )
    async with isolated.bind_npi_isolated_execution(execution(control_schema=CONTROL_SCHEMA)):
        result = await control_imports._sync_terminal_worker_failure(run_by_field)
    assert result["status"] == ("failed" if rowcount else "finalizing")
    assert database.execute.await_count == (1 if rowcount else 2)
    for call in database.execute.await_args_list:
        rendered(call.args[0])


@pytest.mark.asyncio
@pytest.mark.parametrize("already_ensured", [False, True])
async def test_scoped_ensure_attests_without_shared_ddl_or_state(monkeypatch, already_ensured):
    monkeypatch.setattr(control_imports._IMPORT_RUN_ENSURE_STATE, "ensured", already_ensured)
    create = AsyncMock(side_effect=AssertionError("shared DDL is forbidden"))
    monkeypatch.setattr(control_imports, "_ensure_import_run_table_once", create)
    scope = execution(control_schema=CONTROL_SCHEMA)
    async with isolated.bind_npi_isolated_execution(scope):
        await control_imports.ensure_import_run_table()
        scope.verify.assert_any_await(None, phase="control")
        scope.verify.side_effect = RuntimeError("synthetic view custody changed")
        with pytest.raises(RuntimeError, match="custody changed"):
            await control_imports.ensure_import_run_table()
    assert control_imports._IMPORT_RUN_ENSURE_STATE.ensured is already_ensured
    create.assert_not_awaited()


@pytest.mark.asyncio
async def test_unbound_ensure_retains_shared_once_semantics(monkeypatch):
    monkeypatch.setattr(control_imports._IMPORT_RUN_ENSURE_STATE, "ensured", False)
    create = AsyncMock()
    monkeypatch.setattr(control_imports, "_ensure_import_run_table_once", create)
    await control_imports.ensure_import_run_table()
    await control_imports.ensure_import_run_table()
    create.assert_awaited_once()
    assert control_imports._IMPORT_RUN_ENSURE_STATE.ensured is True
