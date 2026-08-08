"""PostgreSQL proof for the clean V5-to-V4 downgrade path."""

from __future__ import annotations

import pytest

from tests.test_ptg_wave_recovery_storage_postgres import (
    MATERIALIZED_PRECLAIM_PATH,
    _dsn,
    _evidence,
    _install_migration,
    _insert_successor,
    _insert_supersession,
    _load_migration,
    _quote,
    asyncpg,
)


async def _apply_clean_v5_downgrade(connection, monkeypatch) -> None:
    migration = _load_migration(MATERIALIZED_PRECLAIM_PATH)
    downgrade_statements: list[str] = []
    monkeypatch.setattr(migration.op, "execute", downgrade_statements.append)
    migration.downgrade()
    assert downgrade_statements
    async with connection.transaction():
        for downgrade_statement in downgrade_statements:
            await connection.execute(downgrade_statement)


async def _assert_v5_database_objects_removed(connection, schema: str) -> None:
    v5_function_names = (
        "ptg_import_wave_materialized_preclaim_guard",
        "ptg_import_wave_materialized_preclaim_binding_guard",
        "ptg_import_wave_materialized_child_guard",
        "ptg_import_wave_materialized_run_guard",
        "ptg_import_wave_materialized_event_guard",
        "ptg_import_wave_materialized_write_isolation_guard",
    )
    v5_trigger_names = (
        "ptg_import_wave_materialized_preclaim_guard",
        "ptg_import_wave_materialized_preclaim_binding_guard",
        "ptg_import_wave_intent_materialized_retirement_guard",
        "ptg_import_wave_claim_materialized_retirement_guard",
        "ptg_import_wave_outcome_materialized_retirement_guard",
        "ptg_import_wave_materialized_retired_run_guard",
        "ptg_import_wave_materialized_retired_event_guard",
    )
    assert await connection.fetchval(
        """
        SELECT count(*)
          FROM pg_catalog.pg_proc AS routine
          JOIN pg_catalog.pg_namespace AS namespace
            ON namespace.oid = routine.pronamespace
         WHERE namespace.nspname = $1
           AND routine.proname = ANY($2::text[])
        """,
        schema,
        list(v5_function_names),
    ) == 0
    assert await connection.fetchval(
        """
        SELECT count(*)
          FROM pg_catalog.pg_trigger AS trigger
          JOIN pg_catalog.pg_class AS relation
            ON relation.oid = trigger.tgrelid
          JOIN pg_catalog.pg_namespace AS namespace
            ON namespace.oid = relation.relnamespace
         WHERE namespace.nspname = $1
           AND trigger.tgname = ANY($2::text[])
        """,
        schema,
        list(v5_trigger_names),
    ) == 0


async def _prove_legacy_handoff_after_downgrade(
    connection,
    schema: str,
) -> None:
    successor_wave_id = "post-downgrade-successor"
    evidence, canonical = _evidence(successor_wave_id)
    cohort_map = {
        "schema_version": "healthporta.ptg-import-wave-attestation.v3",
        "wave_id": successor_wave_id,
        "supersession": evidence,
    }
    async with connection.transaction():
        await _insert_successor(
            connection,
            schema,
            successor_wave_id,
            "admitted",
            cohort_map,
        )
        await _insert_supersession(
            connection,
            schema,
            successor_wave_id,
            evidence,
            canonical,
        )
    assert await connection.fetchval(
        f"SELECT count(*) FROM {_quote(schema)}.ptg_import_wave_supersession "
        "WHERE predecessor_wave_id = 'predecessor-wave' "
        "AND successor_wave_id = $1 "
        "AND recovery_basis = 'logical_preclaim_failure'",
        successor_wave_id,
    ) == 1


@pytest.mark.asyncio
async def test_v5_clean_downgrade_restores_legacy_handoff(monkeypatch):
    """Execute the clean downgrade and prove the V3/V4 path still works."""

    schema = "wave_recovery_materialized_clean_downgrade"
    connection = await asyncpg.connect(_dsn())
    try:
        await _install_migration(connection, monkeypatch, schema)
        await _apply_clean_v5_downgrade(connection, monkeypatch)
        await _assert_v5_database_objects_removed(connection, schema)
        await _prove_legacy_handoff_after_downgrade(connection, schema)
    finally:
        await connection.execute(
            f"DROP SCHEMA IF EXISTS {_quote(schema)} CASCADE"
        )
        await connection.close()
