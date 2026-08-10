# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Disposable PostgreSQL proof for the bounded reviewed-subset profile."""

from __future__ import annotations

from copy import deepcopy
import importlib.util
import json
from pathlib import Path

import asyncpg
import pytest

from process.provider_directory_fhir_subset_identity import (
    SERVER_ISSUED_SUBSET_EXACT_COMPLETION_SCOPES,
    SERVER_ISSUED_SUBSET_EXACT_STRATEGY_VERSION,
)
from tests.provider_directory_fhir_subset_activation_support import (
    single_root_activation_inputs,
)
from tests.provider_directory_fhir_subset_completion_support import (
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
from tests.provider_directory_subset_completion_pg_setup import (
    MigrationSqlCapture,
    insert_subset_candidate,
    insert_valid_subset_resources,
)
from tests.provider_directory_subset_completion_pg_support import (
    valid_evidence_pairs,
    valid_source_record,
)
from tests.tin_npi_connector_postgres_support import TransactionalSchema


MIGRATION_PATH = (
    Path(__file__).resolve().parents[1]
    / "alembic/versions"
    / "20260810000000_provider_directory_reviewed_subset_bounded_drift.py"
)


def _load_migration():
    module_spec = importlib.util.spec_from_file_location(
        "provider_directory_reviewed_subset_bounded_drift_postgres_migration",
        MIGRATION_PATH,
    )
    assert module_spec is not None and module_spec.loader is not None
    migration = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(migration)
    return migration


async def _run_migration(scenario, migration, action: str) -> None:
    capture = MigrationSqlCapture()
    migration.op = capture
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


def _function_names(migration) -> tuple[str, ...]:
    subset = migration._subset()
    activation = migration._activation()
    return (
        subset._PROOF_SHAPE_VALID_FUNCTION,
        subset._ENDPOINT_DATASET_GUARD,
        subset._SOURCE_GUARD,
        activation._ACTIVATION_VALID_FUNCTION,
    )


async def _function_oids(scenario, migration) -> dict[str, int]:
    rows = await scenario.connection.fetch(
        """
        SELECT function_row.proname, function_row.oid
          FROM pg_catalog.pg_proc AS function_row
          JOIN pg_catalog.pg_namespace AS namespace_row
            ON namespace_row.oid = function_row.pronamespace
         WHERE namespace_row.nspname = $1
           AND function_row.proname = ANY($2::text[])
        """,
        scenario.schema,
        _function_names(migration),
    )
    return {row["proname"]: row["oid"] for row in rows}


def _with_counts(
    proof,
    *,
    advertised_pre: int,
    advertised_post: int,
    returned_unique: int = 1,
    deficit: int | None = None,
):
    candidate = deepcopy(proof)
    resource = candidate["resources"]["Location"]
    resource.update(
        advertised_pre=advertised_pre,
        advertised_post=advertised_post,
        returned_unique=returned_unique,
        deficit=(
            advertised_pre - returned_unique
            if deficit is None
            else deficit
        ),
    )
    return candidate


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


async def _prove_profile_boundaries(scenario, migration) -> None:
    bounded_contract = build_subset_contract()
    legacy_contract = build_subset_contract(
        strategy_version=SERVER_ISSUED_SUBSET_EXACT_STRATEGY_VERSION,
        completion_scopes=SERVER_ISSUED_SUBSET_EXACT_COMPLETION_SCOPES,
    )
    bounded, *_ = valid_evidence_pairs(contract=bounded_contract)
    legacy, *_ = valid_evidence_pairs(contract=legacy_contract)

    assert await _is_proof_valid(scenario, migration, bounded)
    assert await _is_proof_valid(
        scenario,
        migration,
        _with_counts(bounded, advertised_pre=2, advertised_post=1),
    )
    assert not await _is_proof_valid(
        scenario,
        migration,
        _with_counts(bounded, advertised_pre=2, advertised_post=3),
    )
    assert not await _is_proof_valid(
        scenario,
        migration,
        _with_counts(bounded, advertised_pre=3, advertised_post=1),
    )
    assert not await _is_proof_valid(
        scenario,
        migration,
        _with_counts(bounded, advertised_pre=1, advertised_post=0),
    )
    assert not await _is_proof_valid(
        scenario,
        migration,
        _with_counts(
            bounded,
            advertised_pre=2,
            advertised_post=1,
            deficit=0,
        ),
    )
    mixed = deepcopy(bounded)
    mixed["completion_scopes"] = list(
        SERVER_ISSUED_SUBSET_EXACT_COMPLETION_SCOPES
    )
    assert not await _is_proof_valid(scenario, migration, mixed)

    assert await _is_proof_valid(scenario, migration, legacy)
    assert not await _is_proof_valid(
        scenario,
        migration,
        _with_counts(legacy, advertised_pre=2, advertised_post=1),
    )


async def _prove_profile_guarded_lifecycle(
    scenario,
    migration,
    lifecycle_inputs,
) -> None:
    source_record, dataset_rows, evidence = lifecycle_inputs
    dataset_row = dataset_rows[0]
    activation = migration._activation()

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
    await scenario.connection.execute(
        f"""
        UPDATE {scenario.quoted_schema}.provider_directory_source
           SET updated_at = pg_catalog.transaction_timestamp()
         WHERE source_id = 'synthetic-source'
        """
    )
    await flush_deferred_fixture_events(scenario)
    assert await scenario.connection.fetchval(
        f"SELECT {scenario.quoted_schema}."
        f'"{activation._ACTIVATION_VALID_FUNCTION}"($1)',
        source_record["source_id"],
    ) is True


@pytest.mark.asyncio
async def test_bounded_profile_migration_is_oid_stable_and_closed(monkeypatch):
    scenario = await TransactionalSchema.create(monkeypatch)
    migration = _load_migration()
    try:
        await _install_policy_predecessors(scenario)
        lifecycle_inputs = single_root_activation_inputs(
            contract=build_subset_contract()
        )
        await _insert_policy_source(scenario, lifecycle_inputs[0])
        before_oids = await _function_oids(scenario, migration)
        assert len(before_oids) == 4
        await _run_migration(scenario, migration, "upgrade")
        assert await _function_oids(scenario, migration) == before_oids
        await _prove_profile_boundaries(scenario, migration)
        await _prove_profile_guarded_lifecycle(
            scenario,
            migration,
            lifecycle_inputs,
        )

        try:
            async with scenario.connection.transaction():
                await _run_migration(scenario, migration, "downgrade")
        except AssertionError as error:
            cause = error.__cause__
            assert isinstance(cause, asyncpg.PostgresError)
            assert (
                "provider_directory_reviewed_subset_profile_downgrade_blocked"
                in str(cause)
            )
        else:
            raise AssertionError("bounded-profile evidence permitted downgrade")
        assert await _function_oids(scenario, migration) == before_oids
    finally:
        await scenario.close()


@pytest.mark.asyncio
async def test_bounded_profile_migration_rejects_mixed_source_adoption(
    monkeypatch,
):
    scenario = await TransactionalSchema.create(monkeypatch)
    migration = _load_migration()
    try:
        await _install_policy_predecessors(scenario)
        source_record = valid_source_record(
            "pending_reviewed_subset_acquisition",
            contract=build_subset_contract(),
        )
        source_record["metadata_json"][
            "provider_directory_current_version_census_completion_scopes"
        ] = list(SERVER_ISSUED_SUBSET_EXACT_COMPLETION_SCOPES)
        await _insert_policy_source(scenario, source_record)
        before_oids = await _function_oids(scenario, migration)

        try:
            async with scenario.connection.transaction():
                await _run_migration(scenario, migration, "upgrade")
        except AssertionError as error:
            cause = error.__cause__
            assert isinstance(cause, asyncpg.PostgresError)
            assert (
                "provider_directory_reviewed_subset_profile_adoption_blocked"
                in str(cause)
            )
        else:
            raise AssertionError("mixed reviewed-subset profile was adopted")
        assert await _function_oids(scenario, migration) == before_oids
    finally:
        await scenario.close()


@pytest.mark.asyncio
async def test_bounded_profile_adopts_existing_policy_one_legacy_root(
    monkeypatch,
):
    """Adopt a valid policy-one v3 root and its v2 source activation."""
    scenario = await TransactionalSchema.create(monkeypatch)
    migration = _load_migration()
    legacy_contract = build_subset_contract(
        strategy_version=SERVER_ISSUED_SUBSET_EXACT_STRATEGY_VERSION,
        completion_scopes=SERVER_ISSUED_SUBSET_EXACT_COMPLETION_SCOPES,
    )
    try:
        _subset_migration, activation_migration = (
            await _install_policy_predecessors(scenario)
        )
        source_record, dataset_rows, evidence = single_root_activation_inputs(
            contract=legacy_contract
        )
        dataset_row = dataset_rows[0]
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
            activation_migration,
            source_record,
            dataset_row,
            evidence,
        )

        await _run_migration(scenario, migration, "upgrade")
        assert await _is_proof_valid(
            scenario,
            migration,
            dataset_row["completion_proof_json"],
        )
        assert await scenario.connection.fetchval(
            f"SELECT {scenario.quoted_schema}."
            f'"{activation_migration._ACTIVATION_VALID_FUNCTION}"($1)',
            source_record["source_id"],
        ) is True
        await _run_migration(scenario, migration, "downgrade")
    finally:
        await scenario.close()


@pytest.mark.asyncio
async def test_clean_profile_downgrade_restores_legacy_bodies(monkeypatch):
    scenario = await TransactionalSchema.create(monkeypatch)
    migration = _load_migration()
    try:
        await _install_policy_predecessors(scenario)
        before_oids = await _function_oids(scenario, migration)
        await _run_migration(scenario, migration, "upgrade")
        await _run_migration(scenario, migration, "downgrade")
        assert await _function_oids(scenario, migration) == before_oids
        definitions = await scenario.connection.fetchval(
            """
            SELECT pg_catalog.string_agg(
                       pg_catalog.pg_get_functiondef(function_row.oid),
                       E'\n'
                   )
              FROM pg_catalog.pg_proc AS function_row
              JOIN pg_catalog.pg_namespace AS namespace_row
                ON namespace_row.oid = function_row.pronamespace
             WHERE namespace_row.nspname = $1
               AND function_row.proname = ANY($2::text[])
            """,
            scenario.schema,
            _function_names(migration),
        )
        assert migration._LEGACY_STRATEGY_VERSION in definitions
        assert migration._BOUNDED_STRATEGY_VERSION not in definitions
    finally:
        await scenario.close()
