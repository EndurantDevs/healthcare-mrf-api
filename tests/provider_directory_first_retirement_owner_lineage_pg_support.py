# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native proof for first-acquisition owners followed by terminal retries."""

import asyncio
from contextlib import asynccontextmanager, nullcontext
from dataclasses import replace
from hashlib import sha256

import pytest

from db import migration_provider_directory_first_acquisition_retirement as first
from process.provider_directory_terminal_root_retirement_contract import TerminalRootRetirementError
from process.provider_directory_terminal_root_retirement_operator import (
    apply_terminal_root_retirement,
    preview_terminal_root_retirement,
)
from tests.formulary_fhir_twin_admission_pg_support import load_migration
from tests.provider_directory_retirement_native_pg_support import _convert_storage
from tests.provider_directory_terminal_root_retirement_pg_support import SqlCapture
from tests.provider_directory_terminal_root_retirement_v2_pg_support import retirement_trigger_state
from tests.tin_npi_connector_postgres_support import open_test_connection


def _base():
    from tests import test_provider_directory_terminal_root_retirement_postgres

    return test_provider_directory_terminal_root_retirement_postgres


async def _migrate(scenario, action="upgrade"):
    path = _base().REPOSITORY_ROOT / "alembic/versions"
    migration = load_migration(
        path / "20261024050000_provider_directory_first_retirement_owner_lineage.py", "first_owner_lineage_native"
    )
    migration.op = capture = SqlCapture()
    getattr(migration, action)()
    async with scenario.connection.transaction():
        for statement in capture.statements:
            await scenario.connection.execute(statement)


async def _state(scenario):
    functions = await scenario.connection.fetch(
        "SELECT p.oid,p.prosrc FROM pg_proc p WHERE p.pronamespace=$1::regnamespace ORDER BY p.oid",
        scenario.schema_name,
    )
    relations = await scenario.connection.fetch(
        "SELECT oid,relowner,relacl::text FROM pg_class WHERE relnamespace=$1::regnamespace ORDER BY oid",
        scenario.schema_name,
    )
    return (
        await _base()._function_identities(scenario),
        await retirement_trigger_state(scenario),
        tuple(map(tuple, relations)),
        tuple(map(tuple, functions)),
        await scenario.snapshot(),
    )


@asynccontextmanager
async def _scenario(monkeypatch, count=3):
    base = _base()
    async with base._scenario(monkeypatch) as scenario:
        runs = [base.RUN, *(f"run-first-retry-{index:03d}" for index in range(1, count))]
        await scenario.connection.executemany(
            f"INSERT INTO {scenario.schema}.import_run "
            "(run_id,importer,status,retry_of_run_id,created_at,started_at,finished_at) "
            "VALUES($1,'provider-directory-fhir',$3,$2,now()-interval '19 minutes',"
            "now()-interval '18 minutes',now()-interval '16 minutes')",
            [(run, runs[index - 1], "failed" if index % 2 else "canceled") for index, run in enumerate(runs) if index],
        )
        await scenario.connection.execute(
            f"UPDATE {scenario.schema}.provider_directory_endpoint_dataset SET import_run_id=$1 WHERE dataset_id=$2",
            runs[-2],
            base.ORPHAN_DATASET_ID,
        )

        async def stopped(lineage):
            assert len(lineage) == count and all(row["status"] in {"failed", "canceled"} for row in lineage)

        monkeypatch.setattr(
            "process.provider_directory_terminal_root_retirement_runtime.require_stopped_lineage", stopped
        )
        await _migrate(scenario)
        yield scenario, replace(base._request(), owner_run_id=runs[-2]), runs


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "administrator,native", [(False, False), (True, False), (True, True)], ids=["source", "administrator", "native"]
)
async def test_first_owner_ancestor_retains_complete_evidence_and_replays(monkeypatch, administrator, native):
    base = _base()
    async with _scenario(monkeypatch, 82) as (scenario, request, runs):
        monkeypatch.setattr(
            base, "_request", lambda evidence_sha256=None: replace(request, expected_evidence_sha256=evidence_sha256)
        )
        context = (
            base._standalone_retirement_database(scenario, administrator=True)
            if administrator
            else nullcontext(scenario.database)
        )
        async with context as database:
            if native:
                await _convert_storage(scenario, database.engine)
            await base._assert_first_retirement_retains_evidence(scenario, database)
        target_by_field = base.dataset_rows(await scenario.snapshot())[base.ORPHAN_DATASET_ID]
        assert target_by_field["import_run_id"] == runs[80]
        assert target_by_field["acquisition_root_run_id"] == runs[0]
        evidence_by_field = target_by_field["publication_metadata_json"][base.RETIREMENT_METADATA_KEY]["evidence"]
        assert evidence_by_field["terminal_run_count"] == 82
        async with scenario.connection.transaction():
            await scenario.connection.execute("SET LOCAL TimeZone='UTC'")
            rows = await scenario.connection.fetch(
                f"SELECT to_jsonb(row)::text FROM {scenario.schema}.import_run row "
                "WHERE run_id=ANY($1::varchar[]) ORDER BY created_at,run_id,to_jsonb(row)::text",
                runs,
            )
        digest_input = "".join(sha256(row[0].encode()).hexdigest() for row in rows)
        assert evidence_by_field["lineage_sha256"] == sha256(digest_input.encode()).hexdigest()


@pytest.mark.asyncio
@pytest.mark.parametrize("case", ["wrong-importer", "root-parent", "running", "unfinished", "young", "branching"])
async def test_first_owner_ancestor_rejects_invalid_history(monkeypatch, case):
    async with _scenario(monkeypatch) as (scenario, request, runs):
        mutations_by_case = {
            "wrong-importer": "importer='other'",
            "root-parent": "retry_of_run_id='missing-parent'",
            "running": "status='running'",
            "unfinished": "finished_at=NULL",
            "young": "finished_at=now()",
        }
        if case == "branching":
            await scenario.connection.execute(
                f"INSERT INTO {scenario.schema}.import_run SELECT 'run-first-branch',importer,status,"
                "retry_of_run_id,created_at,started_at,finished_at,summary_json "
                f"FROM {scenario.schema}.import_run WHERE run_id=$1",
                runs[-1],
            )
        else:
            await scenario.connection.execute(
                f"UPDATE {scenario.schema}.import_run SET {mutations_by_case[case]} WHERE run_id=$1",
                runs[0] if case == "root-parent" else runs[-1],
            )
        before = await _state(scenario)
        assert not await scenario.connection.fetchval(
            f"SELECT {scenario.schema}.provider_directory_terminal_root_retirement_eligible($1,900)",
            request.dataset_id,
        )
        with pytest.raises(TerminalRootRetirementError, match="evidence_invalid"):
            await preview_terminal_root_retirement(request, database=scenario.database)
        assert await _state(scenario) == before


@pytest.mark.asyncio
async def test_published_predecessor_still_requires_leaf_owner(monkeypatch):
    base = _base()
    async with base._scenario(monkeypatch) as scenario:
        await _migrate(scenario)
        await scenario.connection.execute(
            f"INSERT INTO {scenario.schema}.import_run SELECT 'run-published-later',importer,'failed',run_id,"
            f"created_at,started_at,finished_at,summary_json FROM {scenario.schema}.import_run WHERE run_id=$1",
            base.OWNER_RUN_ID,
        )
        before = await _state(scenario)
        assert not await scenario.connection.fetchval(
            f"SELECT {scenario.schema}.provider_directory_terminal_root_retirement_eligible($1,900)",
            base.TARGET_DATASET_ID,
        )
        with pytest.raises(TerminalRootRetirementError, match="evidence_invalid"):
            await preview_terminal_root_retirement(base.request(), database=scenario.database)
        assert await _state(scenario) == before


@pytest.mark.asyncio
async def test_owner_lineage_migration_roundtrip_preserves_catalog_and_data(monkeypatch):
    async with _base()._scenario(monkeypatch) as scenario:
        before = await _state(scenario)
        transaction = scenario.connection.transaction()
        await transaction.start()
        try:
            await _migrate(scenario)
            upgraded = await _state(scenario)
            assert upgraded[:3] == before[:3] and upgraded[4] == before[4]
            assert sum(old != new for old, new in zip(before[3], upgraded[3], strict=True)) == 2
        finally:
            await transaction.rollback()
        assert await _state(scenario) == before
        for action in ("upgrade", "upgrade", "downgrade", "upgrade"):
            await _migrate(scenario, action)
            assert await _state(scenario) == (before if action == "downgrade" else upgraded)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "routine,case", [("evidence", "body"), ("eligible", "body"), ("evidence", "settings"), ("eligible", "overload")]
)
async def test_owner_lineage_migration_refuses_unknown_controls(monkeypatch, routine, case):
    async with _base()._scenario(monkeypatch) as scenario:
        before = await _state(scenario)
        function = f"{scenario.schema}.provider_directory_terminal_root_retirement_{routine}"
        signature = function + ("(text)" if routine == "evidence" else "(text,integer)")
        if case == "body":
            mutation = getattr(first, routine + "_sql")(scenario.schema_name)
            mutation = mutation.replace("CREATE FUNCTION", "CREATE OR REPLACE FUNCTION", 1)
            mutation = mutation.replace("AS $function$\n", "AS $function$\n -- unrecognized body\n", 1)
        elif case == "settings":
            mutation = f"ALTER FUNCTION {signature} SET search_path=public"
        else:
            mutation = f"CREATE FUNCTION {function}(text) RETURNS boolean LANGUAGE sql AS 'SELECT true'"
        transaction = scenario.connection.transaction()
        await transaction.start()
        try:
            await scenario.connection.execute(mutation)
            drifted = await _state(scenario)
            refused = "first_retirement_function_changed|owner_lineage_control_changed"
            with pytest.raises(Exception, match=refused) as error:
                await _migrate(scenario)
            assert error.value.sqlstate == "55000"
            assert await _state(scenario) == drifted
        finally:
            await transaction.rollback()
        assert await _state(scenario) == before


@pytest.mark.asyncio
async def test_owner_lineage_downgrade_serializes_header_and_refuses_retired_ancestor(monkeypatch):
    async with _scenario(monkeypatch) as (scenario, request, runs):
        other = await open_test_connection()
        try:
            async with other.transaction():
                await other.fetchval(f"SELECT 1 FROM {scenario.schema}.import_run WHERE run_id=$1 FOR UPDATE", runs[-1])
                before = await _state(scenario)
                with pytest.raises(TerminalRootRetirementError, match="busy"):
                    await asyncio.wait_for(preview_terminal_root_retirement(request, database=scenario.database), 3)
                assert await _state(scenario) == before
            async with other.transaction():
                await other.execute(
                    f"UPDATE {scenario.schema}.provider_directory_endpoint_dataset SET resource_count=resource_count "
                    "WHERE dataset_id=$1",
                    request.dataset_id,
                )
                before = await _state(scenario)
                with pytest.raises(Exception, match="lock timeout") as error:
                    await asyncio.wait_for(_migrate(scenario, "downgrade"), timeout=3)
                assert error.value.sqlstate == "55P03"
                assert await _state(scenario) == before
        finally:
            await other.close()
        token = await preview_terminal_root_retirement(request, database=scenario.database)
        retirement_result = await apply_terminal_root_retirement(
            replace(request, expected_evidence_sha256=token), database=scenario.database
        )
        assert retirement_result.retired
        before = await _state(scenario)
        with pytest.raises(Exception, match="owner_lineage_downgrade_blocked") as error:
            await _migrate(scenario, "downgrade")
        assert error.value.sqlstate == "55000"
        assert await _state(scenario) == before
