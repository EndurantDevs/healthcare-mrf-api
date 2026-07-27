# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""PostgreSQL proof for atomic audited predecessor retirement."""

from __future__ import annotations

import uuid

import pytest

from db.connection import Database
from process.ptg_parts import source_snapshot_control
from process.ptg_parts import source_snapshot_predecessor_retirement as retirement
from process.ptg_parts.source_snapshot_predecessor_retirement_types import (
    PTG2PredecessorRetirementConflict,
)
from tests.ptg_source_snapshot_predecessor_retirement_postgres_support import (
    CURRENT_SNAPSHOT_ID,
    IDEMPOTENCY_KEY,
    POINTER_TABLES,
    PREDECESSOR_SNAPSHOT_ID,
    SOURCE_KEY,
    assert_seeded_state_unchanged,
    configure_operation,
    count_rows,
    create_schema,
    drop_schema,
    manifest,
    pointer_pairs,
    quote_identifier,
    request_params,
    require_control_pin_blocks_without_mutation,
    require_release_bindings_block_without_mutation,
    require_postgres_opt_in,
    seed_pair,
)


async def _assert_retired_state(
    database: Database,
    schema_name: str,
    report: dict[str, object],
    snapshot_lineage_before: list[object],
) -> None:
    assert report["status"] == "retired"
    assert report["cleared_source_pointer_count"] == 1
    assert report["cleared_plan_pointer_count"] == 2
    assert report["cleared_global_pointer_count"] == 1
    assert report["deleted_rollback_pin_count"] == 1
    for table in POINTER_TABLES:
        assert all(
            pair == (CURRENT_SNAPSHOT_ID, None)
            for pair in await pointer_pairs(database, schema_name, table)
        )
    assert await count_rows(database, schema_name, "ptg2_snapshot_pin") == 0
    assert await database.all(
        f"""
        SELECT snapshot_id, previous_snapshot_id
          FROM {quote_identifier(schema_name)}.ptg2_snapshot
         ORDER BY snapshot_id
        """
    ) == snapshot_lineage_before


async def _assert_predecessor_is_removal_ready() -> None:
    removal_plan = await source_snapshot_control.build_source_snapshot_remove_plan(
        snapshot_id=PREDECESSOR_SNAPSHOT_ID,
        source_key=SOURCE_KEY,
    )
    assert removal_plan["removable"] is True
    assert not any(removal_plan["current_references"].values())


async def _delete_predecessor_and_assert_immutable_audit(
    database: Database,
    schema_name: str,
) -> None:
    schema = quote_identifier(schema_name)
    await database.status(
        f"""
        DELETE FROM {schema}.ptg2_snapshot
         WHERE snapshot_id = :snapshot_id
        """,
        snapshot_id=PREDECESSOR_SNAPSHOT_ID,
    )
    assert (
        await count_rows(
            database,
            schema_name,
            "ptg2_predecessor_retirement_audit",
        )
        == 1
    )
    with pytest.raises(
        Exception,
        match="PTG2_PREDECESSOR_RETIREMENT_AUDIT_IMMUTABLE",
    ):
        await database.status(
            f"""
            DELETE FROM {schema}.ptg2_predecessor_retirement_audit
             WHERE idempotency_key = :idempotency_key
            """,
            idempotency_key=IDEMPOTENCY_KEY,
        )
    with pytest.raises(
        Exception,
        match="PTG2_PREDECESSOR_RETIREMENT_AUDIT_IMMUTABLE",
    ):
        await database.status(
            f"TRUNCATE TABLE {schema}."
            "ptg2_predecessor_retirement_audit"
        )


@pytest.mark.asyncio
async def test_postgres_retirement_is_removal_ready_audited_and_replayable(
    monkeypatch,
) -> None:
    """Prove exact writes, removal eligibility, immutable audit, and replay."""

    require_postgres_opt_in()
    database = Database()
    schema_name = f"ptg2_predecessor_retirement_{uuid.uuid4().hex}"
    control_schema_name = f"ptg2_predecessor_control_{uuid.uuid4().hex}"
    await database.connect()
    configure_operation(monkeypatch, database, schema_name, control_schema_name)
    try:
        await create_schema(database, schema_name, control_schema_name)
        await seed_pair(database, schema_name)
        await require_control_pin_blocks_without_mutation(
            database,
            schema_name,
            control_schema_name,
        )
        await require_release_bindings_block_without_mutation(
            database,
            schema_name,
            control_schema_name,
        )
        snapshot_lineage_before = await database.all(
            f"""
            SELECT snapshot_id, previous_snapshot_id
              FROM {quote_identifier(schema_name)}.ptg2_snapshot
             ORDER BY snapshot_id
            """
        )
        report = await retirement.retire_ptg2_source_predecessor(
            **request_params()
        )
        await _assert_retired_state(
            database,
            schema_name,
            report,
            snapshot_lineage_before,
        )
        await _assert_predecessor_is_removal_ready()
        await _delete_predecessor_and_assert_immutable_audit(
            database,
            schema_name,
        )
        replay = await retirement.retire_ptg2_source_predecessor(
            **request_params()
        )
        assert replay["status"] == "already_retired"
        assert replay["idempotent"] is True
    finally:
        try:
            await drop_schema(database, schema_name)
            await drop_schema(database, control_schema_name)
        finally:
            await database.disconnect()


@pytest.mark.asyncio
async def test_postgres_incomplete_v4_root_blocks_retirement_without_writes(
    monkeypatch,
) -> None:
    """The shared removal validator rejects an incomplete V4 packed-map root."""

    require_postgres_opt_in()
    database = Database()
    schema_name = f"ptg2_predecessor_v4_root_{uuid.uuid4().hex}"
    control_schema_name = f"ptg2_predecessor_control_{uuid.uuid4().hex}"
    await database.connect()
    configure_operation(monkeypatch, database, schema_name, control_schema_name)
    schema = quote_identifier(schema_name)
    try:
        await create_schema(database, schema_name, control_schema_name)
        await seed_pair(database, schema_name)
        await database.status(
            f"""
            UPDATE {schema}.ptg2_snapshot
               SET manifest = CAST(:manifest AS jsonb)
             WHERE snapshot_id = :snapshot_id
            """,
            manifest=manifest(storage_generation="shared_blocks_v4"),
            snapshot_id=PREDECESSOR_SNAPSHOT_ID,
        )
        await database.status(
            f"""
            UPDATE {schema}.ptg2_v3_snapshot_layout
               SET generation = 'shared_blocks_v4'
             WHERE snapshot_key = 17
            """
        )
        with pytest.raises(
            PTG2PredecessorRetirementConflict,
            match="complete packed map root",
        ):
            await retirement.retire_ptg2_source_predecessor(
                **request_params()
            )
        await assert_seeded_state_unchanged(database, schema_name)
    finally:
        try:
            await drop_schema(database, schema_name)
            await drop_schema(database, control_schema_name)
        finally:
            await database.disconnect()
