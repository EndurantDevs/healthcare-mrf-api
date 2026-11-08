# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Synthetic routing tests; native custody and full-size acceptance remain separate."""

import importlib
from contextlib import asynccontextmanager
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import MetaData

from process import control_lifecycle
from process import npi_isolated_execution as isolated

RUN_ID = "run_synthetic"
OUTPUT_SCHEMA = "npi_source_synthetic"
npi = importlib.import_module("process.npi")


def context():
    return {
        "import_date": "20261029",
        "control_run_id": RUN_ID,
        "context": {
            "run": 1,
            "control_run_id": RUN_ID,
            "_control_attempt_id": RUN_ID + ":attempt",
            "_control_attempt_started_at": "2026-10-29T00:00:00.000000+00:00",
        },
    }


def stage_model(model, _date):
    table = model.__table__.to_metadata(MetaData(schema=OUTPUT_SCHEMA), schema=OUTPUT_SCHEMA)
    return SimpleNamespace(
        __table__=table,
        __tablename__=model.__main_table__,
        __main_table__=model.__main_table__,
        **{column.name: column for column in table.c},
    )


def execution(**changes):
    inputs_by_name = {
        name: (isolated.NpiInputRelation("captured_inputs", name, index + 100, index + 200),)
        for index, name in enumerate(sorted(isolated.INPUT_NAMES))
    }
    canonical = isolated.NpiInputRelation(OUTPUT_SCHEMA, "npi_canonical_address", 500, 501)
    settings_by_name = dict(
        run_id=RUN_ID,
        output_schema=OUTPUT_SCHEMA,
        inputs=inputs_by_name,
        stage_model=stage_model,
        verify=AsyncMock(),
        startup=AsyncMock(),
        resolve_addresses=AsyncMock(return_value=isolated.NpiAddressPreparation(canonical, SimpleNamespace(rows=1))),
        handoff=AsyncMock(return_value={"status": "prepared", "operation_id": "synthetic"}),
    )
    return isolated.NpiIsolatedExecution(**{**settings_by_name, **changes})


def prepared(ctx):
    state = ctx["context"]
    return isolated.NpiPreparedOutput(
        RUN_ID,
        state["_control_attempt_id"],
        state["_control_attempt_started_at"],
        OUTPUT_SCHEMA,
        "mrf",
        "20261029",
        tuple((name, name) for name in isolated.NPI_TABLES),
        tuple((name, 1) for name in isolated.NPI_TABLES),
        isolated.NpiInputRelation(OUTPUT_SCHEMA, "npi_canonical_address", 500, 501),
        object(),
        {},
    )


@pytest.mark.asyncio
async def test_source_and_output_namespaces_route_independently():
    scope = execution(source_schema="captured_source")
    async with isolated.bind_npi_isolated_execution(scope):
        assert npi._runtime_db_schema() == OUTPUT_SCHEMA
        assert npi._nppes_source_schema(OUTPUT_SCHEMA) == "captured_source"
        query = npi._nppes_canonical_parity_query(
            "captured_source", "npi", ("source_members",), output_schema=OUTPUT_SCHEMA
        )
    assert 'FROM "captured_source"."source_members"' in query
    assert f'LEFT JOIN "{OUTPUT_SCHEMA}"."npi" AS stage' in query
    assert '"captured_source"."npi"' not in query


@pytest.mark.parametrize("schema", ["mrf", "public", "pg_catalog", OUTPUT_SCHEMA])
def test_isolated_source_namespace_cannot_select_shared_or_output_storage(schema):
    with pytest.raises(RuntimeError, match="authority differs"):
        execution(source_schema=schema)


@pytest.mark.asyncio
async def test_native_verification_refusal_cannot_bind_coordinates():
    scope = execution(verify=AsyncMock(side_effect=RuntimeError("synthetic native custody refusal")))
    with pytest.raises(RuntimeError, match="custody refusal"):
        async with isolated.bind_npi_isolated_execution(scope):
            pytest.fail("unverified scope was bound")
    assert isolated.current_npi_isolated_execution() is None


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["run", "schema", "inventory", "canonical"])
async def test_handoff_rejects_changed_original_identity_or_private_closure(change):
    scope = execution()
    ctx = context()
    candidate = prepared(ctx)
    changes_by_case = {
        "run": {"run_id": "other"},
        "schema": {"output_schema": "mrf"},
        "inventory": {"stage_tables": candidate.stage_tables[:-1]},
        "canonical": {"canonical_relation": isolated.NpiInputRelation("mrf", "npi_canonical_address", 500, 501)},
    }
    async with isolated.bind_npi_isolated_execution(scope):
        with pytest.raises(RuntimeError, match="authority differs"):
            await isolated.handoff_npi_prepared_output(ctx, replace(candidate, **changes_by_case[change]))
        assert isolated.committed_npi_isolated_handoff(ctx) is None
        assert not isolated.has_unknown_npi_isolated_handoff(ctx)
    scope.handoff.assert_not_awaited()


@pytest.mark.asyncio
async def test_scope_requires_issuer_and_exact_attempt_and_inventory():
    scope = execution()
    with pytest.raises(RuntimeError, match="authority differs"):
        async with isolated.bind_npi_isolated_execution({"output_schema": OUTPUT_SCHEMA}):
            pytest.fail("serialized scope was accepted")
    with pytest.raises(RuntimeError):
        execution(output_schema="mrf")
    with pytest.raises(RuntimeError):
        execution(inputs={})
    async with isolated.bind_npi_isolated_execution(scope):
        ctx = context()
        ctx["context"]["_control_attempt_id"] = "another:attempt"
        with pytest.raises(RuntimeError):
            await isolated.verify_npi_isolated_execution(ctx, phase="process")
        assert npi._runtime_db_schema() == OUTPUT_SCHEMA
        model = npi._npi_staging_model(npi.NPIAddress, "20261029")
        assert model.__table__.schema == OUTPUT_SCHEMA and model.__tablename__ == "npi_address"
        assert npi._npi_input_relation("nucc_taxonomy", OUTPUT_SCHEMA) == '"captured_inputs"."nucc_taxonomy"'
    assert isolated.current_npi_isolated_execution() is None
    scope.verify.assert_awaited_once_with(None, phase="bind")


@pytest.mark.asyncio
async def test_optional_absence_never_falls_back_and_invalid_stage_is_refused():
    scope = execution()
    scope = replace(scope, inputs={**scope.inputs, "pricing_provider_procedure": ()})
    async with isolated.bind_npi_isolated_execution(scope):
        assert not await npi._npi_has_input_table("pricing_provider_procedure", OUTPUT_SCHEMA)
        with pytest.raises(npi.NPIPrerequisiteError, match="absent"):
            npi._npi_input_relation("pricing_provider_procedure", OUTPUT_SCHEMA)
    invalid = replace(scope, stage_model=lambda model, _date: model)
    async with isolated.bind_npi_isolated_execution(invalid):
        with pytest.raises(npi.NPIPrerequisiteError, match="stage model"):
            npi._npi_staging_model(npi.NPIAddress, "20261029")


@pytest.mark.asyncio
async def test_startup_and_staging_do_not_run_legacy_ddl(monkeypatch):
    scope = execution()
    monkeypatch.setattr(npi, "my_init_db", AsyncMock())
    monkeypatch.setattr(npi, "ensure_database", AsyncMock())
    legacy = AsyncMock(side_effect=AssertionError("legacy DDL"))
    monkeypatch.setattr(npi, "_ensure_required_extensions", legacy)
    monkeypatch.setattr(npi.db, "create_table", legacy)
    monkeypatch.setattr(npi.db, "status", legacy)
    startup_context_map = {}
    async with isolated.bind_npi_isolated_execution(scope):
        await npi.startup(startup_context_map)
        await npi._prepare_npi_staging("20261029", OUTPUT_SCHEMA)
        await npi.finalize_npi_import_attempt(startup_context_map)
    scope.startup.assert_awaited_once_with(startup_context_map)
    legacy.assert_not_awaited()


def _prepare_finalizer_runtime(monkeypatch, ctx):
    """Record synthetic finalizer IO while rejecting shared publication and vacuum."""
    statements = []

    async def scalar(statement):
        statements.append(str(statement))
        return 6_000_001

    async def status(statement):
        statements.append(str(statement))

    async def fetchval(statement):
        statements.append(statement)
        return False if "SELECT EXISTS" in statement else 6_000_001

    @asynccontextmanager
    async def transaction():
        yield object()

    monkeypatch.setenv("HLTHPRT_ADDRESS_CANON_SOURCES", "nppes")
    monkeypatch.setenv("HLTHPRT_ADDRESS_ARCHIVE_CUTOVER", "1")
    monkeypatch.setenv("HLTHPRT_NPI_OPENADDRESSES_BACKFILL", "1")
    monkeypatch.setattr(npi, "ensure_database", AsyncMock())
    monkeypatch.setattr(npi, "raise_if_cancelled", AsyncMock())
    monkeypatch.setattr(npi, "_assert_npi_import_lease", AsyncMock())
    monkeypatch.setattr(npi, "_release_npi_import_lease", AsyncMock())
    monkeypatch.setattr(npi, "_nppes_evidence_runtime_config", lambda _ctx: SimpleNamespace(required=True))
    evidence = object()
    monkeypatch.setattr(npi, "_required_nppes_evidence_receipt", lambda _ctx: evidence)
    monkeypatch.setattr(npi, "_nppes_evidence_metrics", lambda _receipt: {"status": "admitted"})
    monkeypatch.setattr(npi.db, "scalar", scalar)
    monkeypatch.setattr(npi.db, "status", status)
    monkeypatch.setattr(npi.db, "transaction", transaction)
    legacy = AsyncMock(side_effect=AssertionError("shared publication"))
    for name in (
        "resolve_npi_address_archive",
        "refresh_archive_geocodes_from_openaddresses_sharded",
        "_rotate_npi_canonical_table",
    ):
        monkeypatch.setattr(npi, name, legacy)
    monkeypatch.setattr(npi.db, "execute_ddl", legacy)
    ctx["context"][npi._NPI_IMPORT_LEASE_KEY] = npi._NpiImportLease(object(), SimpleNamespace(fetchval=fetchval), 1)

    return statements, evidence, legacy


@pytest.mark.asyncio
@pytest.mark.parametrize("canonical_archive", [True, False])
async def test_finalizer_reuses_all_enrichments_before_private_seven_table_handoff(monkeypatch, canonical_archive):
    """Exercise normal enrichment routing while refusing every shared mutation seam."""
    scope = execution()
    ctx = context()
    statements, evidence, legacy = _prepare_finalizer_runtime(monkeypatch, ctx)
    monkeypatch.setenv("HLTHPRT_ADDRESS_ARCHIVE_CUTOVER", "1" if canonical_archive else "0")

    async with isolated.bind_npi_isolated_execution(scope):
        handoff_result = await npi.finalize_npi_import_attempt(ctx)
        assert isolated.committed_npi_isolated_handoff(ctx) is handoff_result

    [args] = scope.handoff.await_args_list
    candidate = args.args[1]
    assert candidate.evidence_receipt is evidence
    assert dict(candidate.row_counts)["npi_address"] == 6_000_001
    assert dict(candidate.stage_tables) == {name: name for name in isolated.NPI_TABLES}
    assert candidate.canonical_relation.table_name == "npi_canonical_address"
    assert scope.resolve_addresses.await_args.kwargs["openaddresses_backfill"] is True
    for dependency in ("nucc_taxonomy", "plan_npi_raw", "pricing_provider_procedure", "pricing_provider_prescription"):
        assert any(f'"captured_inputs"."{dependency}"' in statement for statement in statements)
    for output in (
        "do_business_as",
        "search_taxonomy_codes",
        "taxonomy_array",
        "plans_network_array",
        "procedures_array",
        "medications_array",
    ):
        assert any(output in statement for statement in statements)
    assert any(f'FROM "{OUTPUT_SCHEMA}"."npi_canonical_address"' in statement for statement in statements)
    assert not any(npi.ADDRESS_FORMAT_FUNCTION in statement for statement in statements)
    assert not ctx["context"].get("control_run_terminal_committed")
    legacy.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("has_handoff", [False, True])
async def test_wrapper_never_reports_candidate_preparation_as_success(monkeypatch, has_handoff):
    scope = execution()
    writes = []

    async def is_marked(_run_id, **values):
        writes.append(values["status"])
        return True

    async def target(ctx, _task):
        if has_handoff:
            return await isolated.handoff_npi_prepared_output(ctx, prepared(ctx))
        return {"status": "prepared", "_npi_isolated_handoff": {"scope": "serialized"}}

    monkeypatch.setenv("HLTHPRT_IMPORT_LIVE_PROGRESS_HEARTBEAT_SECONDS", "0")
    monkeypatch.setattr(control_lifecycle, "mark_control_run", is_marked)
    monkeypatch.setattr(control_lifecycle, "_flush_terminal_status_events", AsyncMock())
    monkeypatch.setattr(control_lifecycle, "import_module", lambda _name: SimpleNamespace(process_data=target))
    task_by_field = dict(
        run_id=RUN_ID, importer="npi", target_module="process.npi", target_function="process_data", task={}
    )
    async with isolated.bind_npi_isolated_execution(scope):
        if has_handoff:
            result = await control_lifecycle.control_single_job_start({"context": {}}, task_by_field)
            assert result["status"] == "finalizing" and writes == ["running"]
        else:
            with pytest.raises(RuntimeError, match="authenticated handoff"):
                await control_lifecycle.control_single_job_start({"context": {}}, task_by_field)
            assert writes == ["running", "failed"]


@pytest.mark.asyncio
async def test_uncertain_handoff_does_not_fabricate_terminal_result():
    scope = execution(handoff=AsyncMock(side_effect=RuntimeError("synthetic commit acknowledgement loss")))
    ctx = context()
    async with isolated.bind_npi_isolated_execution(scope):
        with pytest.raises(RuntimeError, match="acknowledgement loss"):
            await isolated.handoff_npi_prepared_output(ctx, prepared(ctx))
        assert control_lifecycle._has_unknown_native_commit(ctx, "process.npi")
        assert isolated.committed_npi_isolated_handoff(ctx) is None
