# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Real-PostgreSQL release proof for exact terminal-root retirement."""

from __future__ import annotations

import asyncio
import importlib.util
import json
import os
import uuid
from contextlib import asynccontextmanager
from hashlib import sha256
from typing import Any

import pytest
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from db.connection import Database
from process import provider_directory_generic_source_policy as source_policy
from process.provider_directory_generic_source_policy import source_transaction
from process.provider_directory_terminal_root_retirement_contract import (
    REQUIRED_CHILD_RELATIONS,
    RETIREMENT_EVIDENCE_FUNCTION,
    RETIREMENT_METADATA_KEY,
    RETIREMENT_STATUS,
    TerminalRootRetirementError,
    TerminalRootRetirementRequest,
)
from process.provider_directory_terminal_root_retirement_operator import (
    apply_terminal_root_retirement,
    preview_terminal_root_retirement,
)
from process.provider_directory_terminal_root_retirement_profile import SEMANTIC_V4_RETIREMENT_PROFILE
from process.provider_directory_terminal_root_retirement_selection import (
    selected_terminal_root_retirement,
)
from tests.provider_directory_first_retirement_owner_lineage_pg_support import _migrate as _migrate_owner_lineage
from tests.provider_directory_first_retirement_owner_lineage_pg_support import (
    test_first_owner_ancestor_rejects_invalid_history as test_first_owner_ancestor_rejects_invalid_history,
)
from tests.provider_directory_first_retirement_owner_lineage_pg_support import (
    test_first_owner_ancestor_retains_complete_evidence_and_replays as test_first_owner_ancestor_retains_complete_evidence_and_replays,
)
from tests.provider_directory_first_retirement_owner_lineage_pg_support import (
    test_owner_lineage_downgrade_serializes_header_and_refuses_retired_ancestor as test_owner_lineage_downgrade_serializes_header_and_refuses_retired_ancestor,
)
from tests.provider_directory_first_retirement_owner_lineage_pg_support import (
    test_owner_lineage_migration_refuses_unknown_controls as test_owner_lineage_migration_refuses_unknown_controls,
)
from tests.provider_directory_first_retirement_owner_lineage_pg_support import (
    test_owner_lineage_migration_roundtrip_preserves_catalog_and_data as test_owner_lineage_migration_roundtrip_preserves_catalog_and_data,
)
from tests.provider_directory_first_retirement_owner_lineage_pg_support import (
    test_published_predecessor_still_requires_leaf_owner as test_published_predecessor_still_requires_leaf_owner,
)
from tests.provider_directory_terminal_root_retirement_pg_support import (
    CURRENT_DATASET_ID,
    ENDPOINT_ID,
    ORPHAN_DATASET_ID,
    OWNER_RUN_ID,
    ROOT_RUN_ID,
    SOURCE_ID,
    TARGET_DATASET_ID,
    RetirementPostgres,
    SqlCapture,
    quote,
    retirement_postgres,
    seed_ineligible,
)
from tests.provider_directory_terminal_root_retirement_pg_support import ROOT as REPOSITORY_ROOT
from tests.provider_directory_terminal_root_retirement_v2_pg_support import (
    load_v2_migration,
    retirement_trigger_state,
    run_v2_migration,
)
from tests.tin_npi_connector_postgres_support import POSTGRES_DSN_ENV, open_test_connection


def request(
    *,
    expected_evidence_sha256: str | None = None,
    values: dict[str, str] | None = None,
) -> TerminalRootRetirementRequest:
    selectors = values or {
        "source_id": SOURCE_ID,
        "endpoint_id": ENDPOINT_ID,
        "dataset_id": TARGET_DATASET_ID,
        "acquisition_root_run_id": ROOT_RUN_ID,
        "owner_run_id": OWNER_RUN_ID,
        "expected_current_dataset_id": CURRENT_DATASET_ID,
    }
    return TerminalRootRetirementRequest(
        **selectors,
        expected_evidence_sha256=expected_evidence_sha256,
    )


async def target_json(scenario: RetirementPostgres) -> dict[str, Any]:
    value = await scenario.connection.fetchval(
        f"SELECT pg_catalog.to_jsonb(row)::text FROM "
        f"{scenario.schema}.provider_directory_endpoint_dataset AS row "
        "WHERE dataset_id = $1",
        TARGET_DATASET_ID,
    )
    return json.loads(value)


def dataset_rows(snapshot: dict[str, tuple[str, ...]]) -> dict[str, dict[str, Any]]:
    return {
        decoded["dataset_id"]: decoded
        for row in snapshot["provider_directory_endpoint_dataset"]
        for decoded in (json.loads(row),)
    }


async def require_runtime_error_without_writes(
    scenario: RetirementPostgres,
    expected_code: str,
    operation,
) -> None:
    before = await scenario.snapshot()
    with pytest.raises(TerminalRootRetirementError, match=expected_code):
        await operation()
    assert await scenario.snapshot() == before


async def require_database_guard(
    scenario: RetirementPostgres,
    statement: str,
    expected_text: str,
) -> None:
    with pytest.raises(Exception) as error:
        async with scenario.connection.transaction():
            await scenario.connection.execute(statement)
    assert getattr(error.value, "sqlstate", None) == "55000"
    assert expected_text in str(error.value)


def assert_only_parent_seal_changed(
    before: dict[str, tuple[str, ...]],
    after: dict[str, tuple[str, ...]],
) -> dict[str, Any]:
    for relation in before:
        if relation != "provider_directory_endpoint_dataset":
            assert after[relation] == before[relation]
    before_rows = dataset_rows(before)
    after_rows = dataset_rows(after)
    for dataset_id in (CURRENT_DATASET_ID, ORPHAN_DATASET_ID):
        assert after_rows[dataset_id] == before_rows[dataset_id]
    expected_target = before_rows[TARGET_DATASET_ID]
    actual_target = after_rows[TARGET_DATASET_ID]
    assert actual_target["status"] == RETIREMENT_STATUS
    marker = actual_target["publication_metadata_json"].pop(RETIREMENT_METADATA_KEY)
    expected_target["status"] = RETIREMENT_STATUS
    assert actual_target == expected_target
    return marker


async def drift_replay_inputs(scenario: RetirementPostgres) -> None:
    schema = scenario.schema
    await scenario.connection.execute(
        f"UPDATE {schema}.provider_directory_source "
        "SET metadata_json = '{\"reconfigured\":true}' WHERE source_id = $1",
        SOURCE_ID,
    )


async def preview_in_timezone(
    scenario: RetirementPostgres,
    timezone_name: str,
) -> tuple[str, dict[str, Any]]:
    """Return one preview token and SQL evidence under a session timezone."""

    async with source_transaction(scenario.database, isolation="read committed"):
        await scenario.database.scalar(
            "SELECT pg_catalog.set_config('TimeZone', :timezone_name, true)",
            timezone_name=timezone_name,
        )
        evidence_sha256 = await preview_terminal_root_retirement(request(), database=scenario.database)
        evidence_by_field = await scenario.database.scalar(
            f"SELECT {scenario.schema}.{RETIREMENT_EVIDENCE_FUNCTION}(:dataset_id)",
            dataset_id=TARGET_DATASET_ID,
        )
    return evidence_sha256, evidence_by_field


async def apply_in_timezone(
    scenario: RetirementPostgres,
    timezone_name: str,
    evidence_sha256: str,
):
    """Apply or replay while returning the truthful UTC transaction instant."""

    async with source_transaction(scenario.database, isolation="read committed"):
        await scenario.database.scalar(
            "SELECT pg_catalog.set_config('TimeZone', :timezone_name, true)",
            timezone_name=timezone_name,
        )
        expected_retired_at = await scenario.database.scalar(
            """SELECT pg_catalog.to_char(
                       pg_catalog.transaction_timestamp() AT TIME ZONE 'UTC',
                       'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'
                   )"""
        )
        apply_result = await apply_terminal_root_retirement(
            request(expected_evidence_sha256=evidence_sha256),
            database=scenario.database,
        )
    return apply_result, expected_retired_at


@pytest.mark.asyncio
async def test_exact_retirement_preview_apply_and_drifted_replay(monkeypatch) -> None:
    """Prove token binding, one parent seal, independent counts, and replay."""

    async with retirement_postgres(monkeypatch) as scenario:
        seeded = await scenario.snapshot()
        token = await preview_terminal_root_retirement(request(), database=scenario.database)
        assert len(token) == 64
        assert "resource_hash_contract" not in (await target_json(scenario))["publication_metadata_json"]
        assert await scenario.snapshot() == seeded
        await require_runtime_error_without_writes(
            scenario,
            "evidence_changed",
            lambda: apply_terminal_root_retirement(
                request(expected_evidence_sha256="0" * 64),
                database=scenario.database,
            ),
        )
        apply_result = await apply_terminal_root_retirement(
            request(expected_evidence_sha256=token),
            database=scenario.database,
        )
        assert apply_result.retired is True
        applied = await scenario.snapshot()
        marker = assert_only_parent_seal_changed(seeded, applied)
        evidence = marker["evidence"]
        assert evidence["actual_resource_count"] == 3
        assert evidence["resource_counts"] == {"Organization": 2, "Practitioner": 1}
        assert evidence["parent_resource_count"] == 3
        assert evidence["proof_row_count"] == 4
        assert evidence["terminal_run_count"] == 4
        assert set(evidence["child_relations"]) == REQUIRED_CHILD_RELATIONS
        assert evidence["child_relations"]["provider_directory_endpoint_dataset_previous_reference"]["row_count"] == 0
        await drift_replay_inputs(scenario)
        replay_input = await scenario.snapshot()
        assert await preview_terminal_root_retirement(request(), database=scenario.database) == token
        replay = await apply_terminal_root_retirement(
            request(expected_evidence_sha256=token),
            database=scenario.database,
        )
        assert replay.retired is False
        assert await scenario.snapshot() == replay_input
        with pytest.raises(Exception) as downgrade_error:
            await scenario.migrate("downgrade")
        assert getattr(downgrade_error.value, "sqlstate", None) == "55000"
        assert "downgrade_blocked" in str(downgrade_error.value)


@pytest.mark.asyncio
async def test_timezone_invariant_evidence_apply_and_replay(monkeypatch) -> None:
    """Prove UTC evidence and marker truth across unlike session timezones."""

    async with retirement_postgres(monkeypatch) as scenario:
        utc_token, utc_evidence = await preview_in_timezone(scenario, "UTC")
        ny_token, ny_evidence = await preview_in_timezone(scenario, "America/New_York")
        assert ny_token == utc_token
        assert ny_evidence == utc_evidence
        apply_result, expected_retired_at = await apply_in_timezone(scenario, "America/New_York", utc_token)
        assert apply_result.retired is True
        marker = (await target_json(scenario))["publication_metadata_json"][RETIREMENT_METADATA_KEY]
        assert marker["retired_at"] == expected_retired_at
        assert marker["retired_at"].endswith("Z")
        for timezone_name in ("UTC", "America/New_York"):
            replay_token, replay_evidence = await preview_in_timezone(scenario, timezone_name)
            replay_result, _ = await apply_in_timezone(scenario, timezone_name, utc_token)
            assert replay_token == utc_token
            assert replay_evidence == utc_evidence
            assert replay_result.retired is False


@pytest.mark.asyncio
async def test_retired_parent_children_lineage_and_reference_are_guarded(
    monkeypatch,
) -> None:
    """Prove frozen evidence surfaces and unrelated-row mutability."""

    async with retirement_postgres(monkeypatch) as scenario:
        token = await preview_terminal_root_retirement(request(), database=scenario.database)
        await apply_terminal_root_retirement(
            request(expected_evidence_sha256=token),
            database=scenario.database,
        )
        schema = scenario.schema
        for statement, expected_text in retired_mutation_cases(schema):
            await require_database_guard(scenario, statement, expected_text)
        await mutate_unrelated_rows(scenario)
        assert (
            await scenario.connection.fetchval(f"SELECT status FROM {schema}.import_run WHERE run_id = 'run-unrelated'")
            == "running"
        )


def retired_mutation_cases(schema: str) -> tuple[tuple[str, str], ...]:
    """Return each protected evidence mutation and its stable DB error."""

    return (
        (
            f"UPDATE {schema}.provider_directory_dataset_resource SET payload_json = "
            f"'{{\"drift\":true}}' WHERE dataset_id = '{TARGET_DATASET_ID}'",
            "child_immutable",
        ),
        (
            f"UPDATE {schema}.provider_directory_dataset_proof_shard SET "
            f"resource_count = 4 WHERE dataset_id = '{TARGET_DATASET_ID}'",
            "child_immutable",
        ),
        (
            f"UPDATE {schema}.provider_directory_pagination_checkpoint SET "
            f"state = 'drift' WHERE dataset_id = '{TARGET_DATASET_ID}'",
            "child_immutable",
        ),
        (
            f"UPDATE {schema}.import_run SET summary_json = '{{\"drift\":true}}' WHERE run_id = '{ROOT_RUN_ID}'",
            "run_immutable",
        ),
        (
            f"INSERT INTO {schema}.import_run (run_id, importer, status, "
            "retry_of_run_id, created_at) VALUES ('run-late-retry', "
            f"'provider-directory-fhir', 'queued', '{OWNER_RUN_ID}', now())",
            "run_immutable",
        ),
        (
            f"INSERT INTO {schema}.provider_directory_endpoint_dataset "
            "(dataset_id, endpoint_id, previous_dataset_id, status, is_current, "
            "resource_count) VALUES ('dataset-forbidden-reference', "
            f"'{ENDPOINT_ID}', '{TARGET_DATASET_ID}', 'acquiring', false, 0)",
            "reference_forbidden",
        ),
    )


async def mutate_unrelated_rows(scenario: RetirementPostgres) -> None:
    """Prove ordinary resource and import-run rows remain mutable."""

    schema = scenario.schema
    await scenario.connection.execute(
        f"""UPDATE {schema}.provider_directory_dataset_resource
               SET payload_hash = $1,
                   payload_json = '{{"resourceType":"Organization","active":false}}'
             WHERE dataset_id = $2""",
        "9" * 64,
        CURRENT_DATASET_ID,
    )
    await scenario.connection.execute(
        f"""INSERT INTO {schema}.import_run
            (run_id, importer, status, created_at)
            VALUES ('run-unrelated', 'provider-directory-fhir', 'queued', now());
        UPDATE {schema}.import_run SET status = 'running'
         WHERE run_id = 'run-unrelated';"""
    )


async def direct_transition_marker_json(scenario: RetirementPostgres) -> str:
    """Build the valid pre-drift marker used to exercise the SQL trigger."""

    async with scenario.database.transaction():
        selection = await selected_terminal_root_retirement(scenario.database, request())
    return json.dumps(selection.marker_by_field, sort_keys=True, separators=(",", ":"))


async def insert_lineage_child(
    scenario: RetirementPostgres,
    run_id: str,
    retry_of_run_id: str,
    importer: str,
) -> None:
    """Insert one terminal child without bypassing installed run guards."""

    await scenario.connection.execute(
        f"""INSERT INTO {scenario.schema}.import_run
            (run_id, importer, status, retry_of_run_id, created_at, started_at,
             finished_at) VALUES ($1, $2, 'failed', $3,
             now() - interval '23 minutes', now() - interval '22 minutes',
             now() - interval '20 minutes')""",
        run_id,
        importer,
        retry_of_run_id,
    )


def direct_transition_sql(scenario: RetirementPostgres, marker_json: str) -> str:
    """Return the exact parent-only direct transition attempted by the test."""

    marker_literal = marker_json.replace("'", "''")
    return (
        f"UPDATE {scenario.schema}.provider_directory_endpoint_dataset SET "
        f"status = '{RETIREMENT_STATUS}', publication_metadata_json = "
        "publication_metadata_json || jsonb_build_object("
        f"'{RETIREMENT_METADATA_KEY}', '{marker_literal}'::jsonb) "
        f"WHERE dataset_id = '{TARGET_DATASET_ID}'"
    )


@pytest.mark.asyncio
async def test_non_linear_or_foreign_descendant_blocks_python_and_sql(
    monkeypatch,
) -> None:
    """Reject branches and a foreign child beyond the claimed owner."""

    async with retirement_postgres(monkeypatch) as scenario:
        marker_json = await direct_transition_marker_json(scenario)
        lineage_cases = (
            ("run-terminal-branch", "run-terminal-1", "provider-directory-fhir"),
            ("run-terminal-foreign", OWNER_RUN_ID, "synthetic-foreign-importer"),
        )
        for run_id, retry_of_run_id, importer in lineage_cases:
            await insert_lineage_child(scenario, run_id, retry_of_run_id, importer)
            drifted_snapshot = await scenario.snapshot()
            await require_runtime_error_without_writes(
                scenario,
                "evidence_invalid",
                lambda: preview_terminal_root_retirement(request(), database=scenario.database),
            )
            await require_database_guard(
                scenario,
                direct_transition_sql(scenario, marker_json),
                "transition_invalid",
            )
            assert await scenario.snapshot() == drifted_snapshot
            await scenario.connection.execute(f"DELETE FROM {scenario.schema}.import_run WHERE run_id = $1", run_id)


@pytest.mark.asyncio
async def test_too_young_nonterminal_v4_and_predecessorless_roots_fail_closed(
    monkeypatch,
) -> None:
    """Prove every excluded acquisition shape is write-free."""

    async with retirement_postgres(monkeypatch) as scenario:
        for slug, kind in (
            ("young", "young"),
            ("nonterminal", "nonterminal"),
            ("v4", "v4"),
        ):
            selectors = await seed_ineligible(scenario, slug, kind)
            await require_runtime_error_without_writes(
                scenario,
                "evidence_invalid",
                lambda selectors=selectors: preview_terminal_root_retirement(
                    request(values=selectors), database=scenario.database
                ),
            )
        orphan_before = await scenario.rows("provider_directory_endpoint_dataset")
        orphan_selector_by_field = {
            "source_id": "source-predecessorless",
            "endpoint_id": "endpoint-predecessorless",
            "dataset_id": ORPHAN_DATASET_ID,
            "acquisition_root_run_id": "run-predecessorless",
            "owner_run_id": "run-predecessorless",
            "expected_current_dataset_id": CURRENT_DATASET_ID,
        }
        with pytest.raises(TerminalRootRetirementError, match="evidence_invalid"):
            await preview_terminal_root_retirement(request(values=orphan_selector_by_field), database=scenario.database)
        assert await scenario.rows("provider_directory_endpoint_dataset") == orphan_before


ENDPOINT = "endpoint-predecessorless"
SOURCE = "source-predecessorless"
RUN = "run-predecessorless"
API_BASE = "https://orphan.example.invalid/fhir"
REVISION = "20261022120000_provider_directory_first_retirement"


def _migration():
    migration_path = REPOSITORY_ROOT / "alembic/versions" / f"{REVISION}.py"
    module_spec = importlib.util.spec_from_file_location("first_retirement_postgres_migration", migration_path)
    assert module_spec is not None and module_spec.loader is not None
    migration = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(migration)
    return migration


def _request(evidence_sha256=None):
    return TerminalRootRetirementRequest(
        source_id=SOURCE,
        endpoint_id=ENDPOINT,
        dataset_id=ORPHAN_DATASET_ID,
        acquisition_root_run_id=RUN,
        owner_run_id=RUN,
        expected_current_dataset_id=None,
        expect_no_current_dataset=True,
        expected_evidence_sha256=evidence_sha256,
    )


async def _seed_retained_rows(scenario):
    await scenario.connection.execute(
        f"INSERT INTO {scenario.schema}.provider_directory_dataset_resource VALUES "
        "($1,'Organization','retained-org',$2,'{\"resourceType\":\"Organization\"}')",
        ORPHAN_DATASET_ID,
        "e" * 64,
    )
    await scenario.connection.execute(
        f"INSERT INTO {scenario.schema}.provider_directory_dataset_proof_shard VALUES ($1,0,decode('010203','hex'),1)",
        ORPHAN_DATASET_ID,
    )
    await scenario.connection.execute(
        f"UPDATE {scenario.schema}.provider_directory_endpoint_dataset SET resource_count=1 WHERE dataset_id=$1",
        ORPHAN_DATASET_ID,
    )
    source_ids = json.dumps([SOURCE], separators=(",", ":"))
    await scenario.connection.execute(
        f"INSERT INTO {scenario.schema}.provider_directory_pagination_checkpoint "
        "(dataset_id,canonical_api_base,resource_type,source_scope_hash,source_ids,owner_run_id,"
        "acquisition_root_run_id,start_url_hash,state) VALUES ($1,$2,'Organization',$3,$4::jsonb,$5,$5,$6,'active')",
        ORPHAN_DATASET_ID,
        API_BASE,
        sha256(source_ids.encode()).hexdigest(),
        source_ids,
        RUN,
        sha256((API_BASE + "/Organization").encode()).hexdigest(),
    )


async def _function_identities(scenario):
    function_rows = await scenario.connection.fetch(
        "SELECT p.oid,p.proname,p.proowner,p.proacl::text,p.prosecdef,p.provolatile,p.proconfig "
        "FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace "
        "WHERE n.nspname=$1 AND (p.proname LIKE '%terminal_root%' "
        "OR p.proname LIKE 'guard_provider_directory_terminal_root%') ORDER BY p.oid",
        scenario.schema_name,
    )
    return tuple(tuple(function_row) for function_row in function_rows)


@asynccontextmanager
async def _scenario(monkeypatch):
    async def stopped(lineage):
        assert lineage and all(lineage_run["status"] == "failed" for lineage_run in lineage)

    monkeypatch.setattr("process.provider_directory_terminal_root_retirement_runtime.require_stopped_lineage", stopped)
    async with retirement_postgres(monkeypatch) as scenario:
        await run_v2_migration(scenario, load_v2_migration(), "upgrade")
        await _seed_retained_rows(scenario)
        function_identities = await _function_identities(scenario)
        trigger_identities = await retirement_trigger_state(scenario)
        await run_v2_migration(scenario, _migration(), "upgrade")
        assert await _function_identities(scenario) == function_identities
        assert await retirement_trigger_state(scenario) == trigger_identities
        yield scenario


async def _publish_fresh(scenario):
    await scenario.connection.execute(
        f"INSERT INTO {scenario.schema}.provider_directory_endpoint_dataset "
        "(dataset_id,endpoint_id,import_run_id,acquisition_root_run_id,status,is_current,resource_count,"
        "dataset_hash,validated_at,published_at,publication_metadata_json) VALUES "
        "('dataset-fresh-first',$1,'run-fresh-first','run-fresh-first','published',true,0,$2,now(),now(),"
        "jsonb_build_object('source_ids',jsonb_build_array($3::text)))",
        ENDPOINT,
        "d" * 64,
        SOURCE,
    )


@asynccontextmanager
async def _standalone_retirement_database(scenario, *, administrator):
    engine = (
        create_async_engine(make_url(os.environ[POSTGRES_DSN_ENV]).set(drivername="postgresql+asyncpg"))
        if administrator
        else scenario.database.engine
    )
    database = Database(engine=engine, session_factory=async_sessionmaker(engine, expire_on_commit=False))
    database._database_override = database._database_name = engine.url.database
    token = source_policy._FACTORY.set(None)
    try:
        yield database
    finally:
        source_policy._FACTORY.reset(token)
        if administrator:
            await engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("administrator", [False, True], ids=["source", "standalone-administrator"])
async def test_first_retirement_retains_evidence_and_replays_after_publication(monkeypatch, administrator):
    async with _scenario(monkeypatch) as scenario:
        if administrator:
            async with _standalone_retirement_database(scenario, administrator=True) as database:
                await _assert_first_retirement_retains_evidence(scenario, database)
        else:
            await _assert_first_retirement_retains_evidence(scenario, scenario.database)
        published_snapshot = await scenario.snapshot()
        with pytest.raises(Exception, match="first_retirement_downgrade_blocked"):
            await run_v2_migration(scenario, _migration(), "downgrade")
        assert await scenario.snapshot() == published_snapshot


async def _assert_first_retirement_retains_evidence(scenario, database):
    snapshot_before = await scenario.snapshot()
    evidence_sha256 = await preview_terminal_root_retirement(_request(), database=database)
    assert await scenario.snapshot() == snapshot_before
    retirement_result = await apply_terminal_root_retirement(_request(evidence_sha256), database=database)
    assert retirement_result.retired is True
    snapshot_after = await scenario.snapshot()
    for relation in snapshot_before:
        if relation != "provider_directory_endpoint_dataset":
            assert snapshot_after[relation] == snapshot_before[relation]
    original_datasets = dataset_rows(snapshot_before)
    retired_datasets = dataset_rows(snapshot_after)
    for dataset_id in original_datasets:
        if dataset_id != ORPHAN_DATASET_ID:
            assert retired_datasets[dataset_id] == original_datasets[dataset_id]
    retired_target = retired_datasets[ORPHAN_DATASET_ID]
    retirement_marker = retired_target["publication_metadata_json"].pop(RETIREMENT_METADATA_KEY)
    expected_target = original_datasets[ORPHAN_DATASET_ID]
    expected_target["status"] = RETIREMENT_STATUS
    assert retired_target == expected_target
    assert retirement_marker["evidence"]["predecessor_identity_sha256"] == sha256(b"null").hexdigest()
    assert retirement_marker["evidence"]["resource_counts"] == {"Organization": 1}
    await _publish_fresh(scenario)
    published_snapshot = await scenario.snapshot()
    assert await preview_terminal_root_retirement(_request(), database=database) == evidence_sha256
    replay_result = await apply_terminal_root_retirement(_request(evidence_sha256), database=database)
    assert replay_result.retired is False
    assert await scenario.snapshot() == published_snapshot


@pytest.mark.asyncio
@pytest.mark.parametrize("administrator", [False, True], ids=["ordinary-publisher", "administrator-set-role"])
async def test_standalone_first_retirement_rejects_ordinary_and_switched_sessions(monkeypatch, administrator):
    async with _scenario(monkeypatch) as scenario:
        async with _standalone_retirement_database(scenario, administrator=administrator) as database:
            async with database.transaction():
                if administrator:
                    publisher = scenario.database.engine.url.username
                    await database.status(f"SET LOCAL ROLE {quote(publisher)}")
                await require_runtime_error_without_writes(
                    scenario,
                    "administrator_required",
                    lambda: preview_terminal_root_retirement(_request(), database=database),
                )
                await require_runtime_error_without_writes(
                    scenario,
                    "administrator_required",
                    lambda: apply_terminal_root_retirement(_request("0" * 64), database=database),
                )


@pytest.mark.asyncio
@pytest.mark.parametrize("changed_control", ["disabled-trigger", "rewired-trigger", "function-body", "overload"])
async def test_standalone_first_retirement_rejects_changed_guards(monkeypatch, changed_control):
    from db import migration_provider_directory_first_acquisition_retirement as first

    async with (
        _scenario(monkeypatch) as scenario,
        _standalone_retirement_database(scenario, administrator=True) as database,
    ):
        token = await preview_terminal_root_retirement(_request(), database=database)
        original_function = first.run_sql(scenario.schema_name).replace("CREATE FUNCTION", "CREATE OR REPLACE FUNCTION")
        original_trigger = (
            f"CREATE OR REPLACE TRIGGER pd_trr_import_run_row BEFORE INSERT OR UPDATE OR DELETE "
            f"ON {scenario.schema}.import_run FOR EACH ROW EXECUTE FUNCTION "
            f"{scenario.schema}.guard_provider_directory_terminal_root_retirement_run()"
        )
        if changed_control == "disabled-trigger":
            statement = f"ALTER TABLE {scenario.schema}.import_run DISABLE TRIGGER pd_trr_import_run_row"
        elif changed_control == "rewired-trigger":
            statement = original_trigger.replace("retirement_run()", "retirement_child()")
        elif changed_control == "overload":
            statement = (
                f"CREATE FUNCTION {scenario.schema}.provider_directory_terminal_root_retirement_eligible"
                "(varchar,integer) RETURNS boolean LANGUAGE sql AS 'SELECT true'"
            )
        else:
            statement = original_function.replace("AS $function$\n", "AS $function$\n -- changed body\n", 1)
        try:
            await scenario.connection.execute(statement)
            await require_runtime_error_without_writes(
                scenario, "state_invalid", lambda: preview_terminal_root_retirement(_request(), database=database)
            )
            await require_runtime_error_without_writes(
                scenario,
                "state_invalid",
                lambda: apply_terminal_root_retirement(_request(token), database=database),
            )
        finally:
            if changed_control == "overload":
                await scenario.connection.execute(
                    f"DROP FUNCTION {scenario.schema}.provider_directory_terminal_root_retirement_eligible"
                    "(varchar,integer)"
                )
            await scenario.connection.execute(original_function)
            await scenario.connection.execute(original_trigger)
            await scenario.connection.execute(
                f"ALTER TABLE {scenario.schema}.import_run ENABLE ALWAYS TRIGGER pd_trr_import_run_row"
            )


@pytest.mark.asyncio
@pytest.mark.parametrize("owner_lineage", [False, True], ids=["original-lineage", "owner-lineage"])
@pytest.mark.parametrize("semantic_v4", [False, True], ids=["v1", "v2"])
async def test_published_predecessor_retirement_still_applies_and_replays(monkeypatch, semantic_v4, owner_lineage):
    async with _scenario(monkeypatch) as scenario:
        if owner_lineage:
            await _migrate_owner_lineage(scenario)
        if semantic_v4:
            await scenario.connection.execute(
                f"UPDATE {scenario.schema}.provider_directory_endpoint_dataset SET resource_count=0,"
                "publication_metadata_json=publication_metadata_json || "
                "jsonb_build_object('resource_hash_contract','semantic_content_v4') WHERE dataset_id=$1",
                TARGET_DATASET_ID,
            )
        retirement_request = request()
        snapshot_before = await scenario.snapshot()
        evidence_sha256 = await preview_terminal_root_retirement(retirement_request, database=scenario.database)
        retirement_result = await apply_terminal_root_retirement(
            request(expected_evidence_sha256=evidence_sha256), database=scenario.database
        )
        assert retirement_result.retired is True
        snapshot_after = await scenario.snapshot()
        for relation_name in snapshot_before:
            if relation_name != "provider_directory_endpoint_dataset":
                assert snapshot_after[relation_name] == snapshot_before[relation_name]
        original_datasets = dataset_rows(snapshot_before)
        retired_datasets = dataset_rows(snapshot_after)
        expected_target = original_datasets.pop(retirement_request.dataset_id)
        retired_target = retired_datasets.pop(retirement_request.dataset_id)
        marker_key = SEMANTIC_V4_RETIREMENT_PROFILE.metadata_key if semantic_v4 else RETIREMENT_METADATA_KEY
        retirement_marker = retired_target["publication_metadata_json"].pop(marker_key)
        assert retirement_marker["evidence"]["predecessor_identity_sha256"] != sha256(b"null").hexdigest()
        expected_target["status"] = RETIREMENT_STATUS
        assert retired_target == expected_target
        assert retired_datasets == original_datasets
        assert await preview_terminal_root_retirement(retirement_request, database=scenario.database) == evidence_sha256
        replay_result = await apply_terminal_root_retirement(
            request(expected_evidence_sha256=evidence_sha256), database=scenario.database
        )
        assert replay_result.retired is False
        assert await scenario.snapshot() == snapshot_after


@pytest.mark.asyncio
async def test_unused_first_retirement_migration_downgrades_without_catalog_replacement(monkeypatch):
    async with _scenario(monkeypatch) as scenario:
        snapshot_before = await scenario.snapshot()
        function_identities = await _function_identities(scenario)
        trigger_identities = await retirement_trigger_state(scenario)
        await run_v2_migration(scenario, _migration(), "upgrade")
        await run_v2_migration(scenario, _migration(), "downgrade")
        assert await _function_identities(scenario) == function_identities
        assert await retirement_trigger_state(scenario) == trigger_identities
        assert await scenario.snapshot() == snapshot_before
        assert (
            await scenario.connection.fetchval(
                f"SELECT {scenario.schema}.provider_directory_terminal_root_retirement_eligible($1,900)",
                ORPHAN_DATASET_ID,
            )
            is False
        )
        await require_runtime_error_without_writes(
            scenario,
            "evidence_invalid",
            lambda: preview_terminal_root_retirement(_request(), database=scenario.database),
        )


@pytest.mark.asyncio
async def test_first_retirement_migration_rejects_unknown_installed_function_body(monkeypatch):
    from db import migration_provider_directory_first_acquisition_retirement as first

    async with _scenario(monkeypatch) as scenario:
        snapshot_before = await scenario.snapshot()
        function_identities = await _function_identities(scenario)
        migration = _migration()
        capture = SqlCapture()
        migration.op = capture
        migration.upgrade()
        drifted_sql = (
            first.eligible_sql(scenario.schema_name)
            .replace("CREATE FUNCTION", "CREATE OR REPLACE FUNCTION", 1)
            .replace("AS $function$\n", "AS $function$\n    -- unexpected installed body\n", 1)
        )
        transaction = scenario.connection.transaction()
        await transaction.start()
        try:
            await scenario.connection.execute(drifted_sql)
            with pytest.raises(Exception, match="first_retirement_function_changed") as migration_error:
                for statement in capture.statements:
                    await scenario.connection.execute(statement)
            assert getattr(migration_error.value, "sqlstate", None) == "55000"
        finally:
            await transaction.rollback()
        assert await _function_identities(scenario) == function_identities
        assert await scenario.snapshot() == snapshot_before


@pytest.mark.asyncio
async def test_first_retirement_rejects_current_pointer_drift_and_wrong_token(monkeypatch):
    async with _scenario(monkeypatch) as scenario:
        evidence_sha256 = await preview_terminal_root_retirement(_request(), database=scenario.database)
        await require_runtime_error_without_writes(
            scenario,
            "evidence_changed",
            lambda: apply_terminal_root_retirement(_request("0" * 64), database=scenario.database),
        )
        await _publish_fresh(scenario)
        await require_runtime_error_without_writes(
            scenario,
            "evidence_changed",
            lambda: apply_terminal_root_retirement(_request(evidence_sha256), database=scenario.database),
        )


@pytest.mark.asyncio
async def test_first_retirement_rejects_noncurrent_publication_history(monkeypatch):
    async with _scenario(monkeypatch) as scenario:
        await _publish_fresh(scenario)
        await scenario.connection.execute(
            f"UPDATE {scenario.schema}.provider_directory_endpoint_dataset "
            "SET is_current=false,status='superseded',superseded_at=now() WHERE dataset_id='dataset-fresh-first'"
        )
        await require_runtime_error_without_writes(
            scenario,
            "evidence_changed",
            lambda: preview_terminal_root_retirement(_request(), database=scenario.database),
        )


@pytest.mark.asyncio
async def test_first_retirement_rejects_semantic_v4_and_malformed_direct_marker(monkeypatch):
    async with _scenario(monkeypatch) as scenario:
        await scenario.connection.execute(
            f"UPDATE {scenario.schema}.provider_directory_endpoint_dataset SET publication_metadata_json="
            'publication_metadata_json || \'{"resource_hash_contract":"semantic_content_v4"}\' WHERE dataset_id=$1',
            ORPHAN_DATASET_ID,
        )
        await require_runtime_error_without_writes(
            scenario,
            "evidence_invalid",
            lambda: preview_terminal_root_retirement(_request(), database=scenario.database),
        )
        await require_database_guard(
            scenario,
            f"UPDATE {scenario.schema}.provider_directory_endpoint_dataset SET status='{RETIREMENT_STATUS}', "
            f"publication_metadata_json=publication_metadata_json || '{{\"{RETIREMENT_METADATA_KEY}\":{{}}}}'::jsonb "
            f"WHERE dataset_id='{ORPHAN_DATASET_ID}'",
            "transition_invalid",
        )


@pytest.mark.asyncio
async def test_first_retirement_fences_payload_checkpoint_parent_and_retry_resurrection(monkeypatch):
    async with _scenario(monkeypatch) as scenario:
        evidence_sha256 = await preview_terminal_root_retirement(_request(), database=scenario.database)
        await apply_terminal_root_retirement(_request(evidence_sha256), database=scenario.database)
        snapshot_before = await scenario.snapshot()
        for statement, error in (
            (
                f"UPDATE {scenario.schema}.provider_directory_endpoint_dataset SET status='acquiring' "
                f"WHERE dataset_id='{ORPHAN_DATASET_ID}'",
                "retirement_immutable",
            ),
            (
                f"UPDATE {scenario.schema}.provider_directory_dataset_resource SET payload_json='{{}}' "
                f"WHERE dataset_id='{ORPHAN_DATASET_ID}'",
                "child_immutable",
            ),
            (
                f"UPDATE {scenario.schema}.provider_directory_pagination_checkpoint SET state='active' "
                f"WHERE dataset_id='{ORPHAN_DATASET_ID}'",
                "child_immutable",
            ),
            (f"UPDATE {scenario.schema}.import_run SET status='queued' WHERE run_id='{RUN}'", "run_immutable"),
            (
                f"INSERT INTO {scenario.schema}.import_run (run_id,importer,status,retry_of_run_id,created_at) "
                f"VALUES ('run-late-first-retry','provider-directory-fhir','queued','{RUN}',now())",
                "run_immutable",
            ),
        ):
            await require_database_guard(scenario, statement, error)
        assert await scenario.snapshot() == snapshot_before


@asynccontextmanager
async def _unprivileged_run_guard(scenario):
    """Exercise the row trigger with no direct table rights on its definer."""
    role_name = "first_retirement_guard_" + uuid.uuid4().hex
    role_sql = quote(role_name)
    guard_signature = f"{scenario.schema}.guard_provider_directory_terminal_root_retirement_run()"
    helper_signature = f"{scenario.schema}.provider_directory_terminal_root_run_retired(text)"
    prior_owner = await scenario.connection.fetchval(
        "SELECT pg_get_userbyid(proowner) FROM pg_proc WHERE oid=to_regprocedure($1)", guard_signature
    )
    is_role_created = False
    try:
        await scenario.connection.execute(f"CREATE ROLE {role_sql} NOLOGIN")
        is_role_created = True
        await scenario.connection.execute(f"GRANT USAGE ON SCHEMA {scenario.schema} TO {role_sql}")
        await scenario.connection.execute(f"GRANT EXECUTE ON FUNCTION {helper_signature} TO {role_sql}")
        await scenario.connection.execute(f"ALTER FUNCTION {guard_signature} OWNER TO {role_sql}")
        for table in ("import_run", "provider_directory_endpoint_dataset"):
            table_name = f"{scenario.schema_name}.{table}"
            assert not await scenario.connection.fetchval(
                "SELECT has_table_privilege($1,$2,'SELECT')", role_name, table_name
            )
            assert not await scenario.connection.fetchval(
                "SELECT has_table_privilege($1,$2,'UPDATE')", role_name, table_name
            )
        yield
    finally:
        if is_role_created:
            await scenario.connection.execute(f"ALTER FUNCTION {guard_signature} OWNER TO {quote(prior_owner)}")
            await scenario.connection.execute(f"REVOKE EXECUTE ON FUNCTION {helper_signature} FROM {role_sql}")
            await scenario.connection.execute(f"REVOKE USAGE ON SCHEMA {scenario.schema} FROM {role_sql}")
            await scenario.connection.execute(f"DROP ROLE {role_sql}")


@pytest.mark.asyncio
async def test_first_retirement_run_guard_needs_no_direct_table_privileges(monkeypatch):
    async with _scenario(monkeypatch) as scenario:
        async with _unprivileged_run_guard(scenario):
            await scenario.connection.execute(
                f"INSERT INTO {scenario.schema}.import_run (run_id,importer,status,created_at) "
                "VALUES ('run-ordinary-readonly-guard','provider-directory-fhir','queued',now())"
            )
            await scenario.connection.execute(
                f"UPDATE {scenario.schema}.import_run SET status='running' WHERE run_id='run-ordinary-readonly-guard'"
            )
            assert (
                await scenario.connection.fetchval(
                    f"SELECT status FROM {scenario.schema}.import_run WHERE run_id='run-ordinary-readonly-guard'"
                )
                == "running"
            )
            # Match an actual acquisition root before its terminal transition.
            await scenario.connection.execute(
                f"UPDATE {scenario.schema}.import_run SET status=status WHERE run_id=$1", RUN
            )
            evidence_sha256 = await preview_terminal_root_retirement(_request(), database=scenario.database)
            retirement_result = await apply_terminal_root_retirement(
                _request(evidence_sha256), database=scenario.database
            )
            assert retirement_result.retired is True
            await require_database_guard(
                scenario,
                f"INSERT INTO {scenario.schema}.import_run (run_id,importer,status,retry_of_run_id,created_at) "
                f"VALUES ('run-readonly-late-retry','provider-directory-fhir','queued','{RUN}',now())",
                "run_immutable",
            )


@pytest.mark.asyncio
@pytest.mark.parametrize("lock_kind", ["endpoint", "pagination", "lineage", "worker-action", "root-retirement"])
async def test_first_retirement_rejects_exact_competing_locks(monkeypatch, lock_kind):
    async with _scenario(monkeypatch) as scenario:
        other = await open_test_connection()
        try:
            async with other.transaction():
                if lock_kind == "lineage":
                    await other.fetchrow(f"SELECT 1 FROM {scenario.schema}.import_run WHERE run_id=$1 FOR UPDATE", RUN)
                else:
                    lock_identity = {
                        "endpoint": ENDPOINT,
                        "pagination": f"provider-directory-pagination:{API_BASE}",
                        "worker-action": f"control-run-worker-action:v1:{RUN}",
                        "root-retirement": f"provider-directory-terminal-root-retirement:{RUN}",
                    }[lock_kind]
                    assert await other.fetchval(
                        "SELECT pg_try_advisory_xact_lock(hashtextextended($1,0))", lock_identity
                    )
                snapshot_before = await scenario.snapshot()
                with pytest.raises(TerminalRootRetirementError, match="busy"):
                    await asyncio.wait_for(
                        preview_terminal_root_retirement(_request(), database=scenario.database), timeout=3
                    )
                assert await scenario.snapshot() == snapshot_before
        finally:
            await other.close()


@pytest.mark.asyncio
async def test_migrated_standalone_retirement_preserves_rows_and_native_guards(monkeypatch):
    from tests.provider_directory_retirement_native_pg_support import assert_migrated_retirement

    await assert_migrated_retirement(monkeypatch)


@pytest.mark.asyncio
async def test_retired_parent_rejects_valid_admission_receipt_and_future_column_updates(monkeypatch):
    from tests.provider_directory_retirement_native_pg_support import assert_retired_parent_update_coverage

    await assert_retired_parent_update_coverage(monkeypatch)


@pytest.mark.asyncio
@pytest.mark.parametrize("native", [False, True], ids=["legacy", "native"])
async def test_parent_update_migration_preserves_rows_and_control_identity(monkeypatch, native):
    from tests.provider_directory_retirement_native_pg_support import assert_parent_update_migration_roundtrip

    await assert_parent_update_migration_roundtrip(monkeypatch, native)


@pytest.mark.asyncio
@pytest.mark.parametrize("native", [False, True], ids=["legacy", "native"])
async def test_parent_update_migration_cannot_remove_retired_row_protection(monkeypatch, native):
    from tests.provider_directory_retirement_native_pg_support import (
        assert_retired_parent_downgrade_refused,
    )

    await assert_retired_parent_downgrade_refused(monkeypatch, native)


@pytest.mark.asyncio
@pytest.mark.parametrize("case", ["body", "settings", "overload", "disabled", "partial-columns"])
async def test_parent_update_migration_refuses_changed_controls(monkeypatch, case):
    from tests.provider_directory_retirement_native_pg_support import assert_parent_update_migration_refuses_drift

    await assert_parent_update_migration_refuses_drift(monkeypatch, case)


@pytest.mark.asyncio
async def test_parent_update_migration_bounds_lock_wait(monkeypatch):
    from tests.provider_directory_retirement_native_pg_support import assert_parent_update_migration_bounds_locks

    await assert_parent_update_migration_bounds_locks(monkeypatch)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "case",
    [
        "drop-root",
        "disable-root",
        "drop-leaf",
        "disable-leaf",
        "rewire",
        "arguments",
        "transition",
        "condition",
        "parent-columns",
        "legacy-parent-columns",
        "extra-parent-column",
        "plan",
        "partial",
        "overload",
        "body",
        "truncate-grant",
        "trigger-grant",
        "row-security",
        "sealed-disabled",
        "replica",
        "foreign-owner",
        "split-replica",
    ],
)
async def test_migrated_retirement_rejects_changed_write_boundaries(monkeypatch, case):
    from tests.provider_directory_retirement_native_pg_support import assert_changed_native_boundary

    await assert_changed_native_boundary(monkeypatch, case)


@pytest.mark.asyncio
async def test_first_retirement_allows_unrelated_work_and_serializes_late_retry(monkeypatch):
    async with _scenario(monkeypatch) as scenario:
        other = await open_test_connection()
        retry_task = None
        try:
            async with source_transaction(scenario.database, isolation="read committed"):
                selection = await selected_terminal_root_retirement(scenario.database, _request())
                await asyncio.wait_for(
                    other.execute(
                        f"UPDATE {scenario.schema}.provider_directory_endpoint_dataset SET resource_count=resource_count "
                        "WHERE dataset_id='dataset-current'"
                    ),
                    timeout=3,
                )
                retry_task = asyncio.create_task(
                    other.execute(
                        f"INSERT INTO {scenario.schema}.import_run (run_id,importer,status,retry_of_run_id,created_at) "
                        f"VALUES ('run-concurrent-first-retry','provider-directory-fhir','queued','{RUN}',now())"
                    )
                )
                with pytest.raises(TimeoutError):
                    await asyncio.wait_for(asyncio.shield(retry_task), timeout=0.15)
                evidence_sha256 = sha256(
                    json.dumps(
                        selection.marker_by_field["evidence"], sort_keys=True, separators=(",", ":"), ensure_ascii=False
                    ).encode()
                ).hexdigest()
                retirement_result = await apply_terminal_root_retirement(
                    _request(evidence_sha256), database=scenario.database
                )
                assert retirement_result.retired is True
            with pytest.raises(Exception, match="run_immutable"):
                await asyncio.wait_for(retry_task, timeout=3)
            assert (
                await scenario.connection.fetchval(
                    f"SELECT count(*) FROM {scenario.schema}.import_run WHERE run_id='run-concurrent-first-retry'"
                )
                == 0
            )
        finally:
            if retry_task is not None and not retry_task.done():
                retry_task.cancel()
                await asyncio.gather(retry_task, return_exceptions=True)
            await other.close()
