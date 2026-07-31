# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""PostgreSQL storage and WAL proofs for profile-delta execution."""

from __future__ import annotations

import datetime
import importlib
import json
import os
import uuid
from contextlib import asynccontextmanager
from dataclasses import replace
from types import SimpleNamespace

import pytest
from sqlalchemy.exc import OperationalError

from db.connection import Database
from process import provider_directory_profile as profile
from process import provider_directory_profile_capacity as capacity
from tests.test_provider_directory_profile_capacity import _geometry_payload
from tests.test_provider_directory_profile_control_capacity import (
    _control_wal_plan_input,
)


importer = importlib.import_module("process.provider_directory_fhir")
from tests.provider_directory_profile_delta_schema_fixtures import (
    _create_delta_contract_tables,
)
from tests.provider_directory_profile_delta_scenario import _relation_oid
from tests.provider_directory_profile_delta_test_support import _delta_database


async def _profile_layout_pair(
    database: Database,
    schema: str,
):
    """Create structurally identical target and stage tables and layouts."""
    target_table = "profile_layout_target"
    stage_table = "profile_layout_stage"
    for table_name in (target_table, stage_table):
        await database.status(
            profile.profile_table_sql(schema, table_name, logged=True)
        )
        for statement in profile.profile_index_statements(
            schema, table_name, evidence=False
        ):
            await database.status(statement)
    target_oid = await _relation_oid(
        database, profile.qualified_table(schema, target_table)
    )
    stage_ref = profile.qualified_table(schema, stage_table)
    stage_oid = await _relation_oid(database, stage_ref)
    target_layout = (
        await importer._provider_directory_profile_relation_storage_fingerprint(
            target_oid, expected_persistence="p"
        )
    )
    stage_layout = (
        await importer._provider_directory_profile_relation_storage_fingerprint(
            stage_oid, expected_persistence="p"
        )
    )
    return target_layout, stage_layout, stage_ref


def _assert_profile_layout_parity(target_layout, stage_layout) -> None:
    """Assert structural parity while retaining exact OID identity."""
    assert target_layout.exact_fingerprint != stage_layout.exact_fingerprint
    assert (
        target_layout.structural_fingerprint
        == stage_layout.structural_fingerprint
    )
    assert target_layout.toastable_columns == stage_layout.toastable_columns
    assert target_layout.main_index_pages
    assert stage_layout.toast_oid is not None
    assert target_layout.effective_tablespace_oids
    assert (
        stage_layout.effective_tablespace_oids
        == target_layout.effective_tablespace_oids
    )


async def _insert_toasted_profile(database: Database, stage_ref: str) -> None:
    """Insert one high-entropy profile row that must use TOAST."""
    await database.status(
        f"""
        WITH payload AS (
            SELECT string_agg(md5(value::text), '') AS value
              FROM generate_series(1, 2_000) AS value
        )
        INSERT INTO {stage_ref} (
            npi, profile_json, evidence_json, source_ids,
            endpoint_ids, dataset_ids, source_count,
            independent_source_count, fact_count, generation_id,
            published_at
        )
        SELECT 1000000001,
               jsonb_build_object('payload', payload.value),
               jsonb_build_object('evidence', payload.value),
               ARRAY['source-a']::varchar[],
               ARRAY['endpoint-a']::varchar[],
               ARRAY['dataset-a']::varchar[],
               1, 1, 1, 'generation-a', now()
          FROM payload;
        """
    )


@pytest.mark.asyncio
async def test_pg18_storage_layout_and_exact_toast_chunk_proof(
    monkeypatch,
) -> None:
    """Prove structural layout parity and exact TOAST chunk accounting."""
    async with _delta_database(monkeypatch) as (database, schema):
        monkeypatch.setattr(importer, "db", database)
        target_layout, stage_layout, stage_ref = await _profile_layout_pair(
            database, schema
        )
        _assert_profile_layout_parity(target_layout, stage_layout)
        await _insert_toasted_profile(database, stage_ref)
        chunk_count = (
            await importer._provider_directory_profile_toast_chunk_count(
                source_sql=f"SELECT * FROM {stage_ref}",
                relation_oid=stage_layout.relation_oid,
                toast_oid=stage_layout.toast_oid,
                toastable_columns=stage_layout.toastable_columns,
                expected_compression="pglz",
            )
        )

        assert chunk_count > 0


@pytest.mark.asyncio
async def test_pg18_capacity_identity_refuses_disabled_immutable_trigger(
    monkeypatch,
) -> None:
    async with _delta_database(monkeypatch) as (database, schema):
        monkeypatch.setattr(importer, "db", database)
        await _create_delta_contract_tables(
            database,
            schema,
            evidence_stage="evidence_stage_trigger",
            profile_stage="profile_stage_trigger",
            affected_stage="affected_stage_trigger",
        )
        consumption_ref = profile.qualified_table(
            schema,
            "provider_directory_profile_capacity_lease_consumption",
        )
        consumption_oid = int(
            await database.scalar(
                "SELECT CAST(:relation_ref AS regclass)::oid::bigint;",
                relation_ref=consumption_ref,
            )
        )
        await (
            importer
            ._provider_directory_profile_relation_storage_fingerprint(
                consumption_oid,
                expected_persistence="p",
                expected_user_trigger_count=2,
                expected_immutable_trigger_error=(
                    "provider_directory_profile_capacity_consumption_"
                    "immutable"
                ),
            )
        )

        await database.status(
            f"ALTER TABLE {consumption_ref} DISABLE TRIGGER "
            "provider_directory_profile_capacity_write_guard;"
        )
        with pytest.raises(
            importer.ProviderDirectoryArtifactBuildStale,
            match="immutable_trigger_shape_changed",
        ):
            await (
                importer
                ._provider_directory_profile_relation_storage_fingerprint(
                    consumption_oid,
                    expected_persistence="p",
                    expected_user_trigger_count=2,
                    expected_immutable_trigger_error=(
                        "provider_directory_profile_capacity_consumption_"
                        "immutable"
                    ),
                )
            )


async def _control_update_projection(
    database: Database,
    schema: str,
):
    """Create the import run and project eight high-entropy updates."""
    await _create_delta_contract_tables(
        database,
        schema,
        evidence_stage="evidence_stage_control",
        profile_stage="profile_stage_control",
        affected_stage="affected_stage_control",
    )
    import_run_ref = profile.qualified_table(schema, "import_run")
    run_id = "run_" + "a" * 32
    await database.status(
        f"INSERT INTO {import_run_ref} (run_id, progress, metrics) "
        "VALUES (:run_id, '{}'::jsonb, '{}'::jsonb);",
        run_id=run_id,
    )
    import_run_oid = await _relation_oid(database, import_run_ref)
    layout = (
        await importer._provider_directory_profile_relation_storage_fingerprint(
            import_run_oid,
            expected_persistence="p",
        )
    )
    mutation = importer._provider_directory_profile_control_metadata_input(
        layout,
        relation_name="import_run",
        operation="update",
    )
    operation_count = 8
    geometry = capacity.validated_capacity_geometry(_geometry_payload())
    data_bytes_per_operation, wal_bytes_per_operation = (
        capacity._control_metadata_projection_per_operation(
            geometry,
            mutation,
            sequence_operation_count=operation_count,
        )
    )
    return SimpleNamespace(
        import_run_ref=import_run_ref,
        import_run_oid=import_run_oid,
        run_id=run_id,
        operation_count=operation_count,
        geometry=geometry,
        data_bytes_per_operation=data_bytes_per_operation,
        wal_bytes_per_operation=wal_bytes_per_operation,
    )


async def _write_toast_heavy_control_updates(
    database: Database,
    projection_context,
) -> None:
    """Apply the projected sequence of high-entropy progress updates."""
    for iteration in range(projection_context.operation_count):
        await database.status(
            f"""
            WITH payload AS (
                SELECT string_agg(
                           md5(CAST(:iteration AS text) || ':' || value::text),
                           ''
                       ) AS value
                  FROM generate_series(1, 1800) AS value
            )
            UPDATE {projection_context.import_run_ref}
               SET progress = jsonb_build_object(
                       'iteration', :iteration, 'payload', payload.value
                   ),
                   metrics = jsonb_build_object('iteration', :iteration)
              FROM payload
             WHERE run_id = :run_id;
            """,
            iteration=str(iteration),
            run_id=projection_context.run_id,
        )


async def _control_update_observations(
    database: Database,
    projection_context,
    *,
    bytes_before: int,
    wal_start_lsn: str,
):
    """Return post-update relation, WAL, and payload byte observations."""
    bytes_after = int(
        await database.scalar(
            "SELECT pg_total_relation_size(CAST(:relation_oid AS oid));",
            relation_oid=projection_context.import_run_oid,
        )
    )
    wal_bytes = int(
        await database.scalar(
            """
            SELECT pg_wal_lsn_diff(
                       pg_current_wal_insert_lsn(),
                       CAST(CAST(:wal_start_lsn AS text) AS pg_lsn)
                   )::bigint;
            """,
            wal_start_lsn=wal_start_lsn,
        )
    )
    payload_bytes = int(
        await database.scalar(
            f"SELECT octet_length(progress::text) "
            f"FROM {projection_context.import_run_ref} "
            "WHERE run_id = :run_id;",
            run_id=projection_context.run_id,
        )
    )
    return bytes_after, wal_bytes, payload_bytes


@pytest.mark.asyncio
async def test_pg18_toast_heavy_control_updates_stay_within_projection(
    monkeypatch,
) -> None:
    """Keep repeated TOAST-heavy metadata updates inside signed bounds."""
    async with _delta_database(monkeypatch) as (database, schema):
        monkeypatch.setattr(importer, "db", database)
        projection_context = await _control_update_projection(database, schema)
        bytes_before = int(
            await database.scalar(
                "SELECT pg_total_relation_size(CAST(:relation_oid AS oid));",
                relation_oid=projection_context.import_run_oid,
            )
        )
        wal_start_lsn = str(
            await database.scalar(
                "SELECT pg_current_wal_insert_lsn()::text;"
            )
        )

        await _write_toast_heavy_control_updates(
            database, projection_context
        )
        bytes_after, wal_bytes, serialized_payload_bytes = (
            await _control_update_observations(
                database,
                projection_context,
                bytes_before=bytes_before,
                wal_start_lsn=wal_start_lsn,
            )
        )

        assert serialized_payload_bytes <= (
            capacity.METADATA_PAYLOAD_UPPER_BOUND_BYTES
        )
        assert max(0, bytes_after - bytes_before) <= (
            projection_context.data_bytes_per_operation
            * projection_context.operation_count
        )
        assert wal_bytes <= projection_context.operation_count * (
            projection_context.wal_bytes_per_operation
            + projection_context.geometry.postgres_block_size_bytes
        )


@pytest.mark.asyncio
async def test_pg18_capacity_wal_observes_in_transaction_insert_lsn(
    monkeypatch,
):
    """Count this backend's generated WAL before the writer catches up."""
    async with _delta_database(monkeypatch) as (database, schema):
        probe_ref = profile.qualified_table(schema, "wal_insert_probe")
        await database.status(
            f"CREATE TABLE {probe_ref} ("
            "id bigint PRIMARY KEY, payload text NOT NULL);"
        )
        await database.status("CHECKPOINT;")
        wal_start = str(
            await database.scalar(
                "SELECT pg_current_wal_insert_lsn()::text;"
            )
        )
        monkeypatch.setattr(importer, "db", database)

        async with database.transaction():
            await database.status(
                f"INSERT INTO {probe_ref} "
                "SELECT value, repeat(value::text, 256) "
                "FROM generate_series(1, 64) AS value;"
            )
            expected_insert_bytes = int(
                await database.scalar(
                    "SELECT pg_wal_lsn_diff("
                    "pg_current_wal_insert_lsn(), "
                    "CAST(CAST(:wal_start AS text) AS pg_lsn)"
                    ")::bigint;",
                    wal_start=wal_start,
                )
            )
            observed_bytes = await (
                importer._provider_directory_profile_current_wal_bytes(
                    SimpleNamespace(initial_wal_lsn=wal_start)
                )
            )

        assert expected_insert_bytes > 0
        assert observed_bytes == expected_insert_bytes


@pytest.mark.asyncio
async def test_pg18_tuple_lock_wal_fits_signed_per_row_envelope(
    monkeypatch,
):
    async with _delta_database(monkeypatch) as (database, schema):
        lock_probe_ref = profile.qualified_table(schema, "lock_probe")
        await database.status(
            f"CREATE TABLE {lock_probe_ref} ("
            "id bigint PRIMARY KEY, payload text NOT NULL);"
        )
        await database.status(
            f"INSERT INTO {lock_probe_ref} "
            "SELECT value, repeat(value::text, 64) "
            "FROM generate_series(1, 32) AS value;"
        )
        await database.status("CHECKPOINT;")
        wal_start = str(
            await database.scalar(
                "SELECT pg_current_wal_insert_lsn()::text;"
            )
        )

        async with database.transaction():
            locked_rows = await database.all(
                f"SELECT id FROM {lock_probe_ref} ORDER BY id FOR UPDATE;"
            )
            observed_wal_bytes = int(
                await database.scalar(
                    "SELECT pg_wal_lsn_diff("
                    "pg_current_wal_insert_lsn(), "
                    "CAST(CAST(:wal_start AS text) AS pg_lsn)"
                    ")::bigint;",
                    wal_start=wal_start,
                )
                or 0
            )

        assert len(locked_rows) == 32
        assert 0 < observed_wal_bytes
        assert observed_wal_bytes <= (
            len(locked_rows)
            * capacity.CONTROL_WAL_ROW_LOCK_UPPER_BOUND_BYTES_PER_TUPLE
        )
