# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Disposable PostgreSQL proof for the terminal-window subset profile."""

from __future__ import annotations

from copy import deepcopy
import importlib.util
import json
from pathlib import Path

import asyncpg
import pytest

from process import (
    provider_directory_fhir_subset_terminal_disposition_v4_selection
    as v4_selection,
)
from process.provider_directory_fhir_subset_completion import (
    build_subset_completion_proof,
)
from process.provider_directory_fhir_subset_terminal_disposition_store import (
    sync_v4_terminal_disposition,
)
from tests.provider_directory_fhir_subset_abandonment_pg_support import (
    close_abandonment_scenario,
    runtime_database,
)
from process.provider_directory_fhir_subset_identity import (
    SERVER_ISSUED_SUBSET_COMPLETION_SCOPES,
    SERVER_ISSUED_SUBSET_STRATEGY_VERSION,
)
from tests.provider_directory_fhir_subset_activation_support import (
    single_root_activation_inputs,
)
from tests.provider_directory_fhir_subset_completion_support import (
    PAGE_COUNT,
    build_subset_contract,
)
from tests.provider_directory_reviewed_root_policy_pg import (
    _activate_policy_source,
    _install_policy_predecessors,
    _insert_policy_source,
    _terminalize_candidate,
)
from tests.provider_directory_reviewed_subset_activation_pg_support import (
    flush_deferred_fixture_events,
)
from tests.provider_directory_fhir_subset_terminal_disposition_v4_pg_support import (
    seed_direct_v4_terminal_root,
)
from tests.provider_directory_subset_completion_pg_setup import (
    MigrationSqlCapture,
    insert_subset_candidate,
    insert_valid_subset_resources,
)
from tests.tin_npi_connector_postgres_support import TransactionalSchema
from tests.test_provider_directory_reviewed_subset_direct_v4_disposition_postgres import (
    SYNTHETIC_MARKER_SHA256,
    _install_terminal_stack,
    _object_identity_by_kind as _direct_object_identity,
)
from tests.test_provider_directory_reviewed_subset_terminal_disposition_postgres import (
    _load_migration as _load_terminal_disposition_migration,
    _load_scope_binding_migration,
)
from tests.provider_directory_subset_completion_pg_concurrency import (
    create_committed_subset_schema,
)


MIGRATION_DIRECTORY = Path(__file__).resolve().parents[1] / "alembic/versions"
BOUNDED_MIGRATION_PATH = MIGRATION_DIRECTORY / (
    "20260810000000_provider_directory_reviewed_subset_bounded_drift.py"
)
MIGRATION_PATH = MIGRATION_DIRECTORY / (
    "20260810130000_provider_directory_reviewed_subset_terminal_window.py"
)
DIRECT_DISPOSITION_MIGRATION_PATH = MIGRATION_DIRECTORY / (
    "20260810110000_provider_directory_reviewed_subset_direct_v4_disposition.py"
)
def _load_migration(path, module_name):
    module_spec = importlib.util.spec_from_file_location(module_name, path)
    assert module_spec is not None and module_spec.loader is not None
    migration = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(migration)
    return migration


async def _run_migration(scenario, migration, action: str) -> None:
    capture = MigrationSqlCapture()
    migration.op = capture
    if hasattr(migration, "_bounded"):
        migration._bounded().op = capture
    getattr(migration, action)()
    for statement_index, statement in enumerate(capture.statements):
        try:
            await scenario.connection.execute(statement)
        except Exception as error:
            raise AssertionError(
                f"failed migration {migration.revision} {action} "
                f"statement {statement_index} at "
                f"{getattr(error, 'position', None)}: {error}"
            ) from error


async def _install_direct_predecessor(scenario):
    await scenario.connection.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {scenario.quoted_schema}.import_run (
            run_id varchar(64) PRIMARY KEY,
            retry_of_run_id varchar(64)
        )
        """
    )
    direct = _load_migration(
        DIRECT_DISPOSITION_MIGRATION_PATH,
        "provider_directory_terminal_window_direct_predecessor",
    )
    for predecessor in (
        _load_terminal_disposition_migration(),
        _load_scope_binding_migration(),
        direct,
    ):
        await _run_migration(scenario, predecessor, "upgrade")
    return direct


def _current_contract():
    return build_subset_contract(
        strategy_version=SERVER_ISSUED_SUBSET_STRATEGY_VERSION,
        completion_scopes=SERVER_ISSUED_SUBSET_COMPLETION_SCOPES,
        campaign_id="synthetic-terminal-window-profile",
    )


def _function_names(migration) -> tuple[str, ...]:
    subset = migration._subset()
    activation = migration._activation()
    return (
        subset._PROOF_SHAPE_VALID_FUNCTION,
        subset._ENDPOINT_DATASET_GUARD,
        subset._SOURCE_GUARD,
        activation._ACTIVATION_VALID_FUNCTION,
    )


async def _function_identity_by_name(scenario, migration) -> dict[str, tuple]:
    rows = await scenario.connection.fetch(
        """
        SELECT function_row.proname, function_row.oid,
               function_row.prosecdef, function_row.proconfig,
               pg_catalog.has_function_privilege(
                   'public', function_row.oid, 'EXECUTE'
               ) AS public_execute
          FROM pg_catalog.pg_proc AS function_row
          JOIN pg_catalog.pg_namespace AS namespace_row
            ON namespace_row.oid = function_row.pronamespace
         WHERE namespace_row.nspname = $1
           AND function_row.proname = ANY($2::text[])
        """,
        scenario.schema,
        _function_names(migration),
    )
    return {
        row["proname"]: (
            row["oid"],
            row["prosecdef"],
            tuple(row["proconfig"] or ()),
            row["public_execute"],
        )
        for row in rows
    }


def _assert_function_security(function_identity_by_name) -> None:
    assert all(
        function_identity[1:] == (
            True,
            ("search_path=pg_catalog",),
            False,
        )
        for function_identity in function_identity_by_name.values()
    )


async def _function_definitions(scenario, migration) -> str:
    return await scenario.connection.fetchval(
        """
        SELECT pg_catalog.string_agg(
                   pg_catalog.pg_get_functiondef(function_row.oid), E'\n'
                   ORDER BY function_row.proname)
          FROM pg_catalog.pg_proc AS function_row
          JOIN pg_catalog.pg_namespace AS namespace_row
            ON namespace_row.oid = function_row.pronamespace
         WHERE namespace_row.nspname = $1
           AND function_row.proname = ANY($2::text[])
        """,
        scenario.schema,
        _function_names(migration),
    )


async def _is_proof_valid(scenario, migration, proof) -> bool:
    subset = migration._subset()
    function_ref = subset._qf(
        scenario.schema,
        subset._PROOF_SHAPE_VALID_FUNCTION,
    )
    return bool(
        await scenario.connection.fetchval(
            f"SELECT {function_ref}($1::jsonb, $2, $3)",
            json.dumps(proof),
            proof["dataset"]["hash"],
            proof["dataset"]["count"],
        )
    )


def _profile_proof_by_field(
    *,
    advertised_pre: int = 512_034,
    advertised_post: int = 507_034,
    checkpoint_pages: int = 2_048,
):
    contract = _current_contract()
    execution_proof_by_field = {
        "verified": True,
        "advertised_pre": advertised_pre,
        "advertised_post": advertised_post,
        "returned_unique": 0,
        "deficit": advertised_pre,
        "terminal_reason": "source_no_next",
        "page_entry_counts": [0] * (checkpoint_pages + 1),
        "continuation_hop_sha256": ["a" * 64] * checkpoint_pages,
        "continuation_shape_sha256": ["b" * 64] * checkpoint_pages,
        "terminal_page_geometry": {
            "version": 2,
            "page_count": PAGE_COUNT,
            "pages_processed": checkpoint_pages + 1,
            "processed_rows": 0,
            "terminal_page_start_offset": checkpoint_pages * PAGE_COUNT,
            "logical_window_end_offset": (
                checkpoint_pages + 1
            ) * PAGE_COUNT,
            "terminal_page_entries": 0,
            "sparse_pages": checkpoint_pages + 1,
            "empty_pages": checkpoint_pages + 1,
        },
    }
    resources = contract.resources
    proof, _proof_sha256 = build_subset_completion_proof(
        contract=contract,
        resource_proof_by_type=dict.fromkeys(
            resources,
            execution_proof_by_field,
        ),
        dataset_hash="c" * 64,
        resource_count=0,
        resource_hash_by_type=dict.fromkeys(resources, "d" * 64),
        acquired_resource_hash_by_type=dict.fromkeys(resources, "e" * 64),
        resource_count_by_type=dict.fromkeys(resources, 0),
    )
    return proof


async def _prove_profile_boundaries(scenario, migration) -> None:
    proof = _profile_proof_by_field()
    assert await _is_proof_valid(scenario, migration, proof)

    over_bound = deepcopy(proof)
    over_bound_resource = over_bound["resources"]["PractitionerRole"]
    over_bound_resource["advertised_post"] -= 1
    assert not await _is_proof_valid(scenario, migration, over_bound)

    percentage_bound = _profile_proof_by_field(
        advertised_pre=502,
        advertised_post=496,
        checkpoint_pages=2,
    )
    assert await _is_proof_valid(scenario, migration, percentage_bound)
    percentage_over_bound = deepcopy(percentage_bound)
    percentage_over_bound["resources"]["PractitionerRole"][
        "advertised_post"
    ] -= 1
    assert not await _is_proof_valid(
        scenario,
        migration,
        percentage_over_bound,
    )

    for envelope_edge in (512_000, 512_250):
        edge_proof = _profile_proof_by_field(
            advertised_pre=envelope_edge,
            advertised_post=envelope_edge,
        )
        assert await _is_proof_valid(scenario, migration, edge_proof)

    below_envelope = deepcopy(proof)
    below_envelope["resources"]["PractitionerRole"].update(
        advertised_pre=511_999,
        advertised_post=511_999,
        deficit=511_999,
    )
    assert not await _is_proof_valid(scenario, migration, below_envelope)

    early_terminal = deepcopy(proof)
    early_resource = early_terminal["resources"]["PractitionerRole"]
    early_resource.update(
        advertised_pre=512_251,
        advertised_post=512_251,
        deficit=512_251,
    )
    assert not await _is_proof_valid(scenario, migration, early_terminal)

    increased = deepcopy(proof)
    increased["resources"]["PractitionerRole"]["advertised_post"] = 512_035
    assert not await _is_proof_valid(scenario, migration, increased)


async def _prove_profile_guarded_lifecycle(scenario, migration) -> None:
    lifecycle_inputs = single_root_activation_inputs(contract=_current_contract())
    source_record, dataset_rows, evidence = lifecycle_inputs
    dataset_row = dataset_rows[0]
    activation = migration._activation()

    await _insert_policy_source(scenario, source_record)
    await insert_subset_candidate(
        scenario,
        dataset_id="dataset-candidate",
        root_run_id="root-candidate",
    )
    await insert_valid_subset_resources(scenario, "dataset-candidate")
    await _terminalize_candidate(scenario, dataset_row)
    await flush_deferred_fixture_events(scenario)
    await _activate_policy_source(
        scenario,
        activation,
        source_record,
        dataset_row,
        evidence,
    )
    await scenario.connection.execute(
        f"""
        UPDATE {scenario.quoted_schema}.provider_directory_endpoint_dataset
           SET status = 'published', is_current = true,
               published_at = pg_catalog.transaction_timestamp()
         WHERE dataset_id = 'dataset-candidate'
        """
    )
    await flush_deferred_fixture_events(scenario)
    assert await scenario.connection.fetchval(
        f"SELECT {scenario.quoted_schema}."
        f'"{activation._ACTIVATION_VALID_FUNCTION}"($1)',
        source_record["source_id"],
    ) is True


@pytest.mark.asyncio
async def test_terminal_window_profile_is_oid_stable_closed_and_guarded(
    monkeypatch,
):
    scenario = await TransactionalSchema.create(monkeypatch)
    bounded = _load_migration(
        BOUNDED_MIGRATION_PATH,
        "provider_directory_bounded_profile_predecessor",
    )
    migration = _load_migration(
        MIGRATION_PATH,
        "provider_directory_terminal_window_profile_migration",
    )
    try:
        await _install_policy_predecessors(scenario)
        await _run_migration(scenario, bounded, "upgrade")
        direct = await _install_direct_predecessor(scenario)
        direct_identity = await _direct_object_identity(scenario, direct)
        before_identity_by_name = await _function_identity_by_name(
            scenario,
            migration,
        )
        _assert_function_security(before_identity_by_name)
        async with scenario.connection.transaction():
            await _run_migration(scenario, migration, "upgrade")
        assert await _function_identity_by_name(
            scenario,
            migration,
        ) == before_identity_by_name
        assert await _direct_object_identity(scenario, direct) == direct_identity
        await _prove_profile_boundaries(scenario, migration)
        await _prove_profile_guarded_lifecycle(scenario, migration)

        try:
            async with scenario.connection.transaction():
                await _run_migration(scenario, migration, "downgrade")
        except AssertionError as error:
            cause = error.__cause__
            assert isinstance(cause, asyncpg.PostgresError)
            assert "terminal_window_downgrade_blocked" in str(cause)
        else:
            raise AssertionError("terminal-window evidence permitted downgrade")
        assert await _function_identity_by_name(
            scenario,
            migration,
        ) == before_identity_by_name
    finally:
        await scenario.close()


@pytest.mark.asyncio
async def test_clean_terminal_window_downgrade_restores_v4(monkeypatch):
    scenario = await TransactionalSchema.create(monkeypatch)
    bounded = _load_migration(
        BOUNDED_MIGRATION_PATH,
        "provider_directory_bounded_profile_clean_predecessor",
    )
    migration = _load_migration(
        MIGRATION_PATH,
        "provider_directory_terminal_window_profile_clean_migration",
    )
    try:
        await _install_policy_predecessors(scenario)
        await _run_migration(scenario, bounded, "upgrade")
        direct = await _install_direct_predecessor(scenario)
        direct_identity = await _direct_object_identity(scenario, direct)
        before_identity_by_name = await _function_identity_by_name(
            scenario,
            migration,
        )
        predecessor_definitions = await _function_definitions(scenario, migration)
        _assert_function_security(before_identity_by_name)
        await _run_migration(scenario, migration, "upgrade")
        async with scenario.connection.transaction():
            await _run_migration(scenario, migration, "downgrade")
        assert await _function_identity_by_name(
            scenario,
            migration,
        ) == before_identity_by_name
        assert await _direct_object_identity(scenario, direct) == direct_identity
        definitions = await _function_definitions(scenario, migration)
        assert definitions == predecessor_definitions
        assert "traversal-subset-v4" in definitions
        assert "traversal-subset-v5" not in definitions
    finally:
        await scenario.close()


@pytest.mark.asyncio
async def test_direct_v4_replay_survives_terminal_window_round_trip(
    monkeypatch,
):
    """Keep one sealed direct-v4 root valid across v5 upgrade and downgrade."""

    scenario = await create_committed_subset_schema(monkeypatch)
    direct = _load_migration(
        DIRECT_DISPOSITION_MIGRATION_PATH,
        "provider_directory_terminal_window_direct_predecessor",
    )
    migration = _load_migration(
        MIGRATION_PATH,
        "provider_directory_terminal_window_direct_round_trip",
    )
    direct._MARKER_SHA256 = SYNTHETIC_MARKER_SHA256
    migration._direct_disposition()._MARKER_SHA256 = SYNTHETIC_MARKER_SHA256
    monkeypatch.setattr(
        v4_selection,
        "DIRECT_V4_TERMINAL_MARKER_SHA256",
        SYNTHETIC_MARKER_SHA256,
    )
    database = runtime_database()
    try:
        await _install_terminal_stack(scenario)
        await seed_direct_v4_terminal_root(scenario)
        async with scenario.connection.transaction():
            await _run_migration(scenario, direct, "upgrade")
        first = await sync_v4_terminal_disposition(database, "source-a")
        assert first.disposed is True
        direct_identity = await _direct_object_identity(scenario, direct)

        async with scenario.connection.transaction():
            await _run_migration(scenario, migration, "upgrade")
        assert await _direct_object_identity(scenario, direct) == direct_identity
        assert (
            await sync_v4_terminal_disposition(database, "source-a")
        ).disposed is False
        assert await scenario.connection.fetchval(
            f'SELECT {scenario.quoted_schema}."{direct._DIRECT_VALID}"($1)',
            "dataset-a",
        ) is True

        async with scenario.connection.transaction():
            await _run_migration(scenario, migration, "downgrade")
        assert await _direct_object_identity(scenario, direct) == direct_identity
        assert (
            await sync_v4_terminal_disposition(database, "source-a")
        ).disposed is False
    finally:
        await database.engine.dispose()
        await close_abandonment_scenario(scenario)
