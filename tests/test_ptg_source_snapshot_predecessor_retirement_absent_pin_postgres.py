# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""PostgreSQL proof for explicitly unpinned predecessor retirement."""

from __future__ import annotations

import asyncio
import uuid

import pytest

from db.connection import Database
from process.ptg_parts import source_snapshot_predecessor_retirement as retirement
from process.ptg_parts.source_snapshot_predecessor_retirement_types import (
    PTG2PredecessorRetirementConflict,
)
from tests.ptg_source_snapshot_predecessor_retirement_postgres_support import (
    CURRENT_SNAPSHOT_ID,
    POINTER_TABLES,
    PREDECESSOR_SNAPSHOT_ID,
    assert_seeded_state_unchanged,
    configure_operation,
    count_rows,
    create_schema,
    drop_schema,
    pointer_pairs,
    quote_identifier,
    request_params,
    require_postgres_opt_in,
    seed_pair,
)


async def _insert_committed_release_pin(
    database: Database,
    schema_name: str,
) -> None:
    await database.status(
        f"""
        INSERT INTO {quote_identifier(schema_name)}.ptg2_snapshot_pin
            (owner_type, owner_id, snapshot_id, reason)
        VALUES ('plan_release', 'release-drift', :snapshot_id,
                'unexpected release retention')
        """,
        snapshot_id=PREDECESSOR_SNAPSHOT_ID,
    )


async def _held_release_pin_writer(
    database: Database,
    schema_name: str,
    pin_inserted: asyncio.Event,
    allow_commit: asyncio.Event,
) -> None:
    async with database.transaction() as session:
        await session.execute(
            database.text(
                f"""
                INSERT INTO {quote_identifier(schema_name)}.ptg2_snapshot_pin
                    (owner_type, owner_id, snapshot_id, reason)
                VALUES ('plan_release', 'release-race', :snapshot_id,
                        'concurrent release retention')
                """
            ),
            {"snapshot_id": PREDECESSOR_SNAPSHOT_ID},
        )
        pin_inserted.set()
        await allow_commit.wait()


async def _assert_absent_retirement_result(
    database: Database,
    schema_name: str,
    report: dict[str, object],
) -> None:
    assert report["status"] == "retired"
    assert report["rollback_pin_mode"] == "absent"
    assert report["rollback_owner_id"] is None
    assert report["deleted_rollback_pin_count"] == 0
    for table in POINTER_TABLES:
        assert all(
            pair == (CURRENT_SNAPSHOT_ID, None)
            for pair in await pointer_pairs(database, schema_name, table)
        )
    audit_rows = await database.all(
        f"""
        SELECT rollback_pin_mode, rollback_owner_id,
               deleted_rollback_pin_count
          FROM {quote_identifier(schema_name)}.
               ptg2_predecessor_retirement_audit
        """
    )
    assert [tuple(row) for row in audit_rows] == [("absent", None, 0)]


@pytest.mark.asyncio
async def test_postgres_absent_pin_mode_rejects_drift_succeeds_and_replays(
    monkeypatch,
) -> None:
    """An explicitly unpinned predecessor retires without synthetic pin writes."""

    require_postgres_opt_in()
    database = Database()
    schema_name = f"ptg2_predecessor_absent_{uuid.uuid4().hex}"
    control_schema_name = f"ptg2_predecessor_control_{uuid.uuid4().hex}"
    await database.connect()
    configure_operation(monkeypatch, database, schema_name, control_schema_name)
    try:
        await create_schema(database, schema_name, control_schema_name)
        await seed_pair(database, schema_name, include_rollback_pin=False)
        await _insert_committed_release_pin(database, schema_name)
        with pytest.raises(
            PTG2PredecessorRetirementConflict,
            match="non-target retention pin",
        ):
            await retirement.retire_ptg2_source_predecessor(
                **request_params(rollback_pin_mode="absent")
            )
        await assert_seeded_state_unchanged(
            database,
            schema_name,
            expected_pin_count=1,
        )
        await database.status(
            f"DELETE FROM {quote_identifier(schema_name)}.ptg2_snapshot_pin"
        )
        report = await retirement.retire_ptg2_source_predecessor(
            **request_params(rollback_pin_mode="absent")
        )
        await _assert_absent_retirement_result(database, schema_name, report)
        replay = await retirement.retire_ptg2_source_predecessor(
            **request_params(rollback_pin_mode="absent")
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
async def test_postgres_absent_pin_race_is_retryable_and_write_free(
    monkeypatch,
) -> None:
    """A concurrent pin insertion cannot slip past the absent expectation."""

    require_postgres_opt_in()
    database = Database()
    schema_name = f"ptg2_predecessor_absent_race_{uuid.uuid4().hex}"
    control_schema_name = f"ptg2_predecessor_control_{uuid.uuid4().hex}"
    await database.connect()
    configure_operation(monkeypatch, database, schema_name, control_schema_name)
    pin_inserted = asyncio.Event()
    allow_commit = asyncio.Event()
    try:
        await create_schema(database, schema_name, control_schema_name)
        await seed_pair(database, schema_name, include_rollback_pin=False)
        writer = asyncio.create_task(
            _held_release_pin_writer(
                database,
                schema_name,
                pin_inserted,
                allow_commit,
            )
        )
        await pin_inserted.wait()
        with pytest.raises(
            PTG2PredecessorRetirementConflict,
            match="retry",
        ):
            await retirement.retire_ptg2_source_predecessor(
                **request_params(rollback_pin_mode="absent")
            )
        await assert_seeded_state_unchanged(
            database,
            schema_name,
            expected_pin_count=0,
        )
        allow_commit.set()
        await asyncio.wait_for(writer, timeout=3)
        assert await count_rows(
            database,
            schema_name,
            "ptg2_snapshot_pin",
        ) == 1
    finally:
        allow_commit.set()
        try:
            await drop_schema(database, schema_name)
            await drop_schema(database, control_schema_name)
        finally:
            await database.disconnect()
