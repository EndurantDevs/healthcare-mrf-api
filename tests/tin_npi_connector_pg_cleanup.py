# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Expired-build abandonment, evidence-first GC, and downgrade proofs."""

from __future__ import annotations

from tests.tin_npi_connector_pg_generation_support import (
    insert_evidence_rows,
    insert_generation,
    insert_generation_policies,
    insert_lookup_rows,
    set_build_token,
)
from tests.tin_npi_connector_pg_lifecycle_model import (
    TOKEN_POLICY_ID,
    abandoned_dataset,
)
from tests.tin_npi_connector_postgres_support import (
    SqlCapture,
    expect_postgres_error,
)


ABANDONED_BUILD_TOKEN = "lost-abandoned-build-token"


async def prove_abandonment_and_gc(scenario):
    abandoned_model = scenario.empty_model(
        "2026-07-27T00:00:00.000000Z",
        dataset=abandoned_dataset(scenario),
    )
    abandoned_key = await _insert_expiring_build(scenario, abandoned_model)
    await _load_partial_abandoned_build(scenario, abandoned_key)
    await _expire_and_abandon(scenario, abandoned_key)
    await _prove_evidence_first_gc(scenario, abandoned_key)
    rebuilt_key = await insert_generation(
        scenario.connection,
        scenario.quoted_schema,
        abandoned_model,
        ABANDONED_BUILD_TOKEN,
        lease_seconds=2.0,
    )
    assert rebuilt_key > abandoned_key
    await _prove_downgrade_fence(scenario)


async def _insert_expiring_build(scenario, abandoned_model):
    abandoned_key = await insert_generation(
        scenario.connection,
        scenario.quoted_schema,
        abandoned_model,
        ABANDONED_BUILD_TOKEN,
        lease_seconds=2.0,
    )
    await set_build_token(scenario.connection, ABANDONED_BUILD_TOKEN)
    await insert_generation_policies(
        scenario.connection,
        scenario.quoted_schema,
        abandoned_key,
        (TOKEN_POLICY_ID,),
    )
    return abandoned_key


async def _load_partial_abandoned_build(scenario, abandoned_key):
    primary_lookup = scenario.model.lookup_rows[0]
    await insert_lookup_rows(
        scenario.connection,
        scenario.quoted_schema,
        abandoned_key,
        (primary_lookup,),
    )
    await insert_evidence_rows(
        scenario.connection,
        scenario.quoted_schema,
        abandoned_key,
        scenario.model.evidence_rows[:2],
    )


async def _expire_and_abandon(scenario, abandoned_key):
    await scenario.connection.execute(
        "SELECT set_config('healthporta.tin_npi_build_token', '', TRUE)"
    )
    await scenario.connection.execute("SELECT pg_sleep(2.05)")
    await expect_postgres_error(
        scenario.connection,
        "tin_npi_connector_build_token_invalid",
        f"""
        UPDATE {scenario.quoted_schema}.tin_npi_connector_generation
           SET state = 'failed'
         WHERE generation_key = $1
        """,
        abandoned_key,
    )
    returned_key = await scenario.connection.fetchval(
        f"""
        SELECT {scenario.quoted_schema}.
               abandon_tin_npi_connector_generation($1)
        """,
        abandoned_key,
    )
    assert returned_key == abandoned_key
    abandoned_state = await scenario.connection.fetchrow(
        f"""
        SELECT state, failed_at
          FROM {scenario.quoted_schema}.tin_npi_connector_generation
         WHERE generation_key = $1
        """,
        abandoned_key,
    )
    assert abandoned_state["state"] == "failed"
    assert abandoned_state["failed_at"] is not None


async def _prove_evidence_first_gc(scenario, abandoned_key):
    await expect_postgres_error(
        scenario.connection,
        "tin_npi_connector_gc_batch_invalid",
        f"""
        SELECT *
          FROM {scenario.quoted_schema}.
               gc_tin_npi_connector_generation($1, NULL)
        """,
        abandoned_key,
    )
    first_gc = await scenario.connection.fetchrow(
        f"""
        SELECT *
          FROM {scenario.quoted_schema}.
               gc_tin_npi_connector_generation($1, 1)
        """,
        abandoned_key,
    )
    assert first_gc["deleted_evidence_rows"] == 1
    assert first_gc["deleted_lookup_rows"] == 0
    assert first_gc["generation_removed"] is False
    final_gc = await scenario.connection.fetchrow(
        f"""
        SELECT *
          FROM {scenario.quoted_schema}.
               gc_tin_npi_connector_generation($1, 100)
        """,
        abandoned_key,
    )
    assert final_gc["deleted_evidence_rows"] == 1
    assert final_gc["deleted_lookup_rows"] == 1
    assert final_gc["generation_removed"] is True
    remaining_count = await scenario.connection.fetchval(
        f"""
        SELECT COUNT(*)
          FROM {scenario.quoted_schema}.tin_npi_connector_generation
         WHERE generation_key = $1
        """,
        abandoned_key,
    )
    assert remaining_count == 0


async def _prove_downgrade_fence(scenario):
    sql_capture = SqlCapture()
    scenario.session.migration.op = sql_capture
    scenario.session.migration.downgrade()
    await expect_postgres_error(
        scenario.connection,
        "tin_npi_connector_downgrade_requires_empty_inactive_foundation",
        sql_capture.statements[0],
    )
