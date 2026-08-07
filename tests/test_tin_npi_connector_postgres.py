# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Disposable PostgreSQL lifecycle proofs for the TIN-to-NPI connector."""

from __future__ import annotations

import pytest

from process import tin_npi_connector as connector
from tests.tin_npi_connector_pg_build_proof import (
    load_and_verify_complete_generation,
)
from tests.tin_npi_connector_pg_cleanup import prove_abandonment_and_gc
from tests.tin_npi_connector_pg_directory_guards import (
    prove_directory_guard_contract,
)
from tests.tin_npi_connector_pg_generation_store import (
    prove_store_atomic_rollback,
    prove_store_load_seal_reuse,
)
from tests.tin_npi_connector_pg_generation_store_resilience import (
    prove_store_cancel_rollback,
    prove_store_commit_ack_recovery,
    prove_store_concurrent_reuse,
)
from tests.tin_npi_connector_pg_immutability import (
    prove_generation_immutability,
)
from tests.tin_npi_connector_pg_lifecycle_model import (
    ConnectorLifecycleScenario,
)
from tests.tin_npi_connector_pg_parity import prove_two_policy_record_parity
from tests.tin_npi_connector_pg_publication import prove_publish_and_rollback
from tests.tin_npi_connector_pg_race import prove_dataset_validation_races
from tests.tin_npi_connector_postgres_support import (
    SqlCapture,
    TransactionalSchema,
    expect_postgres_error,
    run_migration,
)


@pytest.mark.asyncio
async def test_connector_migration_upgrades_and_downgrades_only_when_empty(
    monkeypatch,
):
    session = await TransactionalSchema.create(monkeypatch)
    try:
        await session.upgrade()
        downgrade_fence = _capture_downgrade_fence(session)
        await _prove_token_registry_downgrade_fence(session, downgrade_fence)
        await _prove_identifier_registry_downgrade_fence(session, downgrade_fence)
        guard_oid_before = await _endpoint_dataset_guard_oid(session)
        await run_migration(
            session.guard_migration,
            "downgrade",
            session.connection,
        )
        guard_after_downgrade = await session.connection.fetchrow(
            """
            SELECT function_row.oid,
                   pg_catalog.pg_get_functiondef(function_row.oid) AS definition
              FROM pg_catalog.pg_proc AS function_row
              JOIN pg_catalog.pg_namespace AS function_namespace
                ON function_namespace.oid = function_row.pronamespace
             WHERE function_namespace.nspname = $1
               AND function_row.proname =
                       'guard_tin_npi_connector_endpoint_dataset'
               AND function_row.pronargs = 0
            """,
            session.schema,
        )
        assert guard_after_downgrade["oid"] == guard_oid_before
        assert "to_jsonb(new)" in guard_after_downgrade["definition"].lower()
        await run_migration(session.migration, "downgrade", session.connection)
        table_count = await session.connection.fetchval(
            """
            SELECT COUNT(*)
              FROM information_schema.tables
             WHERE table_schema = $1
               AND table_name LIKE 'tin_npi_connector_%'
            """,
            session.schema,
        )
        assert table_count == 0
    finally:
        await session.close()


async def _endpoint_dataset_guard_oid(session):
    return await session.connection.fetchval(
        """
        SELECT function_row.oid
          FROM pg_catalog.pg_proc AS function_row
          JOIN pg_catalog.pg_namespace AS function_namespace
            ON function_namespace.oid = function_row.pronamespace
         WHERE function_namespace.nspname = $1
           AND function_row.proname =
                   'guard_tin_npi_connector_endpoint_dataset'
           AND function_row.pronargs = 0
        """,
        session.schema,
    )


@pytest.mark.asyncio
async def test_endpoint_dataset_guard_migration_rejects_schema_drift(monkeypatch):
    session = await TransactionalSchema.create(monkeypatch)
    try:
        await run_migration(session.migration, "upgrade", session.connection)
        await session.connection.execute(
            f"""
            ALTER TABLE {
                session.quoted_schema
            }.provider_directory_endpoint_dataset
                ADD COLUMN guard_drift_probe text
            """
        )
        sql_capture = SqlCapture()
        session.guard_migration.op = sql_capture
        session.guard_migration.upgrade()
        await expect_postgres_error(
            session.connection,
            "provider_directory_endpoint_dataset_guard_schema_changed",
            sql_capture.statements[0],
        )
    finally:
        await session.close()


def _capture_downgrade_fence(session):
    sql_capture = SqlCapture()
    session.migration.op = sql_capture
    session.migration.downgrade()
    return sql_capture.statements[0]


async def _prove_token_registry_downgrade_fence(session, downgrade_fence):
    token_policy = connector.TinTokenPolicyDescriptor.release_1(
        "ptg-tin-hmac-sha256-v1:release-1"
    )
    registry_transaction = session.connection.transaction()
    await registry_transaction.start()
    await session.connection.execute(
        f"""
        INSERT INTO {session.quoted_schema}.tin_npi_connector_token_policy (
            token_policy_id,
            token_policy_descriptor_sha256
        ) VALUES ($1, $2)
        """,
        token_policy.token_policy_id,
        bytes.fromhex(token_policy.token_policy_descriptor_sha256),
    )
    await expect_postgres_error(
        session.connection,
        "tin_npi_connector_downgrade_requires_empty_inactive_foundation",
        downgrade_fence,
    )
    await registry_transaction.rollback()


async def _prove_identifier_registry_downgrade_fence(session, downgrade_fence):
    identifier_policy = connector.DEFAULT_FHIR_TIN_NPI_IDENTIFIER_POLICY
    registry_transaction = session.connection.transaction()
    await registry_transaction.start()
    await session.connection.execute(
        f"""
        INSERT INTO {session.quoted_schema}.tin_npi_connector_identifier_policy (
            identifier_policy_id,
            descriptor_canonical_json,
            identifier_policy_sha256
        ) VALUES ($1, $2, $3)
        """,
        identifier_policy.policy_id,
        identifier_policy.descriptor_canonical_json,
        bytes.fromhex(identifier_policy.descriptor_sha256),
    )
    await expect_postgres_error(
        session.connection,
        "tin_npi_connector_downgrade_requires_empty_inactive_foundation",
        downgrade_fence,
    )
    await registry_transaction.rollback()


@pytest.mark.asyncio
async def test_two_policy_record_parity_is_required_at_generation_seal(monkeypatch):
    await prove_two_policy_record_parity(monkeypatch)


@pytest.mark.asyncio
async def test_dataset_resource_guard_serializes_validation_race_orders(monkeypatch):
    await prove_dataset_validation_races(monkeypatch)


@pytest.mark.asyncio
async def test_store_loads_seals_and_reuses_without_publishing(monkeypatch):
    await prove_store_load_seal_reuse(monkeypatch)


@pytest.mark.asyncio
async def test_store_rolls_back_after_copy_failure(monkeypatch):
    await prove_store_atomic_rollback(monkeypatch)


@pytest.mark.asyncio
async def test_store_cancellation_rolls_back_and_retries(monkeypatch):
    await prove_store_cancel_rollback(monkeypatch)


@pytest.mark.asyncio
async def test_store_commit_acknowledgement_loss_reuses_on_restart(monkeypatch):
    await prove_store_commit_ack_recovery(monkeypatch)


@pytest.mark.asyncio
async def test_store_concurrent_load_reuses_one_generation(monkeypatch):
    await prove_store_concurrent_reuse(monkeypatch)


@pytest.mark.asyncio
async def test_connector_digest_build_cas_and_mutation_guards(monkeypatch):
    scenario = await ConnectorLifecycleScenario.create(monkeypatch)
    try:
        await prove_directory_guard_contract(scenario)
        await load_and_verify_complete_generation(scenario)
        retired_generation_key = await prove_publish_and_rollback(scenario)
        await prove_generation_immutability(scenario, retired_generation_key)
        await prove_abandonment_and_gc(scenario)
    finally:
        await scenario.session.close()
