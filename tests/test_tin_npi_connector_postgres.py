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
