# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""PostgreSQL truth tests for capacity-ledger and delta migrations."""

from __future__ import annotations

import os
import uuid

import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy.exc import DBAPIError, OperationalError
from sqlalchemy.schema import MetaData

from db.connection import Database
from db.models.system import ProviderDirectoryProfileCapacityLeaseConsumption
from process import provider_directory_profile_capacity_attestation as lease
from tests.test_provider_directory_profile_capacity_attestation import (
    VALIDATION_TIME,
    _verify,
)
from tests.test_provider_directory_profile_capacity_attestation_schema import (
    _OperationsRecorder,
    _load_migration,
)

async def _require_disposable_database(database: Database) -> None:
    try:
        database_name = str(
            await database.scalar("SELECT current_database();") or ""
        )
    except (OSError, OperationalError):
        pytest.skip("capacity ledger tests need disposable PostgreSQL")
    is_schema_test_enabled = os.getenv(
        "HLTHPRT_PROVIDER_DIRECTORY_PROFILE_ALLOW_SCHEMA_TESTS",
        "",
    ).strip().lower() in {"1", "true", "yes", "on"}
    if "test" not in database_name.lower() and not is_schema_test_enabled:
        pytest.skip("capacity ledger tests need a test database")


def _consumption_values() -> dict[str, object]:
    binding = lease.CapacityLeaseConsumptionBinding(
        run_id="run_" + "6" * 32,
        build_id="pdpb_" + "7" * 32,
        executable_plan_hash="88" * 32,
        selection_proof_id="99" * 32,
        source_vector_hash="aa" * 32,
        source_context_vector_hash="bb" * 32,
        profile_as_of="2026-07-30",
    )
    return lease.capacity_lease_consumption_values(
        _verify(),
        binding,
        accepted_at=VALIDATION_TIME,
    )


async def _install_immutable_guards(database, schema):
    migration = _load_migration()
    recorder = _OperationsRecorder()
    migration.op = recorder
    migration._create_capacity_lease_consumption_table(schema)
    for guard_statement in recorder.statements:
        await database.status(guard_statement)


async def _assert_ledger_refuses_reuse_and_mutation(
    database,
    schema,
    ledger_table,
    consumption_by_field,
):
    duplicate_reservation_by_field = dict(consumption_by_field)
    duplicate_reservation_by_field["attestation_id"] = "cd" * 32
    duplicate_reservation_by_field["lease_digest"] = "dc" * 32
    with pytest.raises(DBAPIError):
        await database.insert(ledger_table).values(
            **duplicate_reservation_by_field
        ).status()
    table_ref = (
        f'"{schema}".'
        '"provider_directory_profile_capacity_lease_consumption"'
    )
    for mutation_statement in (
        f"UPDATE {table_ref} SET database_oid = database_oid;",
        f"DELETE FROM {table_ref};",
        f"TRUNCATE {table_ref};",
    ):
        with pytest.raises(DBAPIError):
            await database.status(mutation_statement)
    assert await database.scalar(
        f"SELECT count(*) FROM {table_ref};"
    ) == 1


@pytest.mark.asyncio
async def test_capacity_consumption_is_one_time_and_immutable_in_postgres():
    database = Database()
    schema = f"pd_capacity_lease_{uuid.uuid4().hex[:12]}"
    is_schema_created = False
    try:
        await database.connect()
        await _require_disposable_database(database)
        await database.status(f'CREATE SCHEMA "{schema}";')
        is_schema_created = True
        ledger_table = (
            ProviderDirectoryProfileCapacityLeaseConsumption.__table__
            .to_metadata(MetaData(), schema=schema)
        )
        await database.create_table(ledger_table)
        await _install_immutable_guards(database, schema)

        consumption_by_field = _consumption_values()
        assert (
            await database.insert(ledger_table)
            .values(**consumption_by_field)
            .status()
        ) == 1
        await _assert_ledger_refuses_reuse_and_mutation(
            database,
            schema,
            ledger_table,
            consumption_by_field,
        )
    finally:
        if is_schema_created:
            await database.status(
                f'DROP SCHEMA IF EXISTS "{schema}" CASCADE;'
            )
        await database.disconnect()


async def _apply_profile_delta_migration(
    database: Database,
    migration,
    action: str,
) -> None:
    """Run one Alembic migration action on the async database engine."""
    def run_action(sync_connection) -> None:
        migration.op = Operations(
            MigrationContext.configure(sync_connection)
        )
        getattr(migration, action)()

    assert database.engine is not None
    async with database.engine.begin() as connection:
        await connection.run_sync(run_action)


async def _assert_profile_delta_upgrade_schema(
    database: Database,
    schema: str,
) -> None:
    """Prove forecast columns, always-enabled guards, and receipt identity."""
    assert await database.scalar(
        """
        SELECT count(*)
          FROM information_schema.columns
         WHERE table_schema = :schema
           AND table_name =
               'provider_directory_profile_build_checkpoint'
           AND column_name LIKE 'cutover_forecast_%';
        """,
        schema=schema,
    ) == 3
    assert await database.scalar(
        """
        SELECT count(*) = 4 AND bool_and(trigger_row.tgenabled = 'A')
          FROM pg_trigger AS trigger_row
          JOIN pg_class AS relation ON relation.oid = trigger_row.tgrelid
          JOIN pg_namespace AS namespace
            ON namespace.oid = relation.relnamespace
         WHERE namespace.nspname = :schema
           AND NOT trigger_row.tgisinternal;
        """,
        schema=schema,
    ) is True
    assert await database.scalar(
        """
        SELECT count(*) = 1
          FROM pg_constraint AS constraint_row
          JOIN pg_class AS relation ON relation.oid = constraint_row.conrelid
          JOIN pg_namespace AS namespace
            ON namespace.oid = relation.relnamespace
         WHERE namespace.nspname = :schema
           AND relation.relname = 'provider_directory_profile_delta_receipt'
           AND constraint_row.contype = 'u'
           AND constraint_row.conname =
               'pd_profile_delta_receipt_control_proof_key';
        """,
        schema=schema,
    ) is True


async def _assert_checkpoint_geometry_guards(
    database: Database,
    checkpoint_ref: str,
) -> None:
    """Reject incomplete geometry and forecast triples on checkpoints."""
    await database.status(
        f"INSERT INTO {checkpoint_ref} (build_id) VALUES ('checkpoint');"
    )
    with pytest.raises(DBAPIError):
        await database.status(
            f"""
            UPDATE {checkpoint_ref}
               SET materialization_mode = 'source_delta',
                   current_source_vector_hash = :hash_value,
                   desired_source_vector_hash = :hash_value,
                   current_source_context_vector_hash = :hash_value,
                   desired_source_context_vector_hash = :hash_value,
                   affected_npi_stage = 'affected_stage',
                   affected_npi_stage_oid = 1,
                   capacity_geometry_status = 'verified',
                   capacity_geometry_hash = NULL,
                   capacity_geometry_json = '{{}}'::jsonb
             WHERE build_id = 'checkpoint';
            """,
            hash_value="a" * 64,
        )
    with pytest.raises(DBAPIError):
        await database.status(
            f"""
            UPDATE {checkpoint_ref}
               SET cutover_forecast_status = 'verified',
                   cutover_forecast_hash = NULL,
                   cutover_forecast_json = '{{}}'::jsonb
             WHERE build_id = 'checkpoint';
            """
        )


async def _assert_serving_geometry_guard(
    database: Database,
    serving_ref: str,
) -> None:
    """Reject a verified serving generation without its geometry hash."""
    with pytest.raises(DBAPIError):
        await database.status(
            f"""
            INSERT INTO {serving_ref} (
                singleton_key, status, operation, control_generation,
                generation_id, selection_proof_id, authority_revision,
                profile_schema_version, profile_strategy_version,
                source_vector_hash, source_vector_json,
                source_context_vector_hash, source_context_vector_json,
                executable_plan_hash, capacity_geometry_status,
                capacity_geometry_hash, capacity_geometry_json,
                evidence_target_oid, profile_target_oid,
                evidence_rows, profile_rows, profile_as_of, published_at
            ) VALUES (
                'global', 'published', 'publish', 1,
                :generation_id, :hash_value, 1, 1, 'strategy',
                :hash_value, '{{}}'::jsonb, :hash_value, '{{}}'::jsonb,
                :hash_value, 'verified', NULL, '{{}}'::jsonb,
                1, 2, 0, 0, '2026-07-30', now()
            );
            """,
            generation_id="pdprofile_" + "b" * 32,
            hash_value="b" * 64,
        )


async def _assert_receipt_geometry_guard(
    database: Database,
    receipt_ref: str,
) -> None:
    """Reject a receipt whose source geometry omits its verified hash."""
    with pytest.raises(DBAPIError):
        await database.status(
            f"""
            INSERT INTO {receipt_ref} (
                build_id, executable_plan_hash,
                from_capacity_geometry_status,
                from_capacity_geometry_hash,
                from_capacity_geometry_json,
                capacity_geometry_status, capacity_geometry_hash,
                capacity_geometry_json,
                from_source_vector_hash, to_source_vector_hash,
                from_source_context_vector_hash,
                to_source_context_vector_hash,
                from_generation_id, generation_id, operation,
                profile_as_of, selection_proof_id, control_generation,
                authority_revision, evidence_target_oid,
                profile_target_oid, evidence_rows, profile_rows,
                evidence_inserted, evidence_deleted,
                profile_inserted, profile_deleted,
                cutover_forecast_hash, cutover_forecast_json,
                cutover_actual_hash, cutover_actual_json,
                cutover_wal_start_lsn, cutover_wal_observed_lsn,
                cutover_wal_bytes, evidence_target_bytes_before,
                evidence_target_bytes_after, evidence_target_growth_bytes,
                profile_target_bytes_before, profile_target_bytes_after,
                profile_target_growth_bytes
            ) VALUES (
                :build_id, :hash_value, 'verified', NULL, '{{}}'::jsonb,
                'verified', :hash_value, '{{}}'::jsonb,
                :hash_value, :hash_value, :hash_value, :hash_value,
                :from_generation_id, :generation_id, 'publish',
                '2026-07-30', :hash_value, 1, 1, 1, 2, 0, 0,
                0, 0, 0, 0, :hash_value, '{{}}'::jsonb,
                :hash_value, '{{}}'::jsonb, '0/1', '0/2',
                0, 0, 0, 0, 0, 0, 0
            );
            """,
            build_id="pdpb_" + "c" * 32,
            from_generation_id="pdprofile_" + "d" * 32,
            generation_id="pdprofile_" + "e" * 32,
            hash_value="f" * 64,
        )


async def _assert_profile_delta_downgrade(
    database: Database,
    schema: str,
) -> None:
    """Prove downgrade restores the checkpoint stub and drops receipts."""
    assert await database.scalar(
        """
        SELECT array_agg(column_name ORDER BY ordinal_position)
          FROM information_schema.columns
         WHERE table_schema = :schema
           AND table_name =
               'provider_directory_profile_build_checkpoint';
        """,
        schema=schema,
    ) == ["build_id"]
    assert await database.scalar(
        "SELECT to_regclass(:relation) IS NULL;",
        relation=(
            f'"{schema}".'
            '"provider_directory_profile_delta_receipt"'
        ),
    ) is True


@pytest.mark.asyncio
async def test_profile_delta_migration_upgrades_and_downgrades_postgres(
    monkeypatch,
):
    """Upgrade, reject incomplete geometry, and restore the stub on downgrade."""
    database = Database()
    schema = f"pd_profile_delta_migration_{uuid.uuid4().hex[:12]}"
    is_schema_created = False
    try:
        await database.connect()
        await _require_disposable_database(database)
        await database.status(f'CREATE SCHEMA "{schema}";')
        is_schema_created = True
        await database.status(
            f'CREATE TABLE "{schema}".'
            'provider_directory_profile_build_checkpoint ('
            'build_id varchar(64) PRIMARY KEY);'
        )
        monkeypatch.setenv("DB_SCHEMA", schema)
        monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema)
        migration = _load_migration()
        await _apply_profile_delta_migration(database, migration, "upgrade")
        await _assert_profile_delta_upgrade_schema(database, schema)
        checkpoint_ref = (
            f'"{schema}".'
            '"provider_directory_profile_build_checkpoint"'
        )
        await _assert_checkpoint_geometry_guards(database, checkpoint_ref)
        await _assert_serving_geometry_guard(
            database,
            f'"{schema}"."provider_directory_profile_serving_generation"',
        )
        await _assert_receipt_geometry_guard(
            database,
            f'"{schema}"."provider_directory_profile_delta_receipt"',
        )
        await _apply_profile_delta_migration(database, migration, "downgrade")
        await _assert_profile_delta_downgrade(database, schema)
    finally:
        if is_schema_created:
            await database.status(
                f'DROP SCHEMA IF EXISTS "{schema}" CASCADE;'
            )
        await database.disconnect()
