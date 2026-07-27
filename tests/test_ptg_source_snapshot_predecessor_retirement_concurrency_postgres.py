# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""PostgreSQL concurrency proof for predecessor retirement."""

from __future__ import annotations

import asyncio
import uuid

import pytest

from db.connection import Database
from process.ptg_parts import source_snapshot_predecessor_retirement as retirement
from process.ptg_parts.ptg2_lifecycle_lock import acquire_ptg2_lifecycle_lock
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


async def _concurrent_pointer_mutation(
    database: Database,
    schema_name: str,
    *,
    operation: str,
    mutation_applied: asyncio.Event,
    allow_commit: asyncio.Event,
) -> None:
    pair_by_operation = {
        "promote": ("snapshot-next", CURRENT_SNAPSHOT_ID),
        "rollback": (PREDECESSOR_SNAPSHOT_ID, CURRENT_SNAPSHOT_ID),
    }
    snapshot_id, previous_snapshot_id = pair_by_operation[operation]
    async with database.transaction() as session:
        await acquire_ptg2_lifecycle_lock(session)
        for table in POINTER_TABLES:
            await session.execute(
                database.text(
                    f"""
                    UPDATE {quote_identifier(schema_name)}.
                        {quote_identifier(table)}
                       SET snapshot_id = :snapshot_id,
                           previous_snapshot_id = :previous_snapshot_id
                    """
                ),
                {
                    "snapshot_id": snapshot_id,
                    "previous_snapshot_id": previous_snapshot_id,
                },
            )
        mutation_applied.set()
        await allow_commit.wait()


async def _assert_race_result(
    database: Database,
    schema_name: str,
    operation: str,
) -> None:
    assert (
        await count_rows(
            database,
            schema_name,
            "ptg2_predecessor_retirement_audit",
        )
        == 0
    )
    assert await count_rows(database, schema_name, "ptg2_snapshot_pin") == 1
    expected_pair = {
        "promote": ("snapshot-next", CURRENT_SNAPSHOT_ID),
        "rollback": (PREDECESSOR_SNAPSHOT_ID, CURRENT_SNAPSHOT_ID),
    }[operation]
    for table in POINTER_TABLES:
        assert all(
            pair == expected_pair
            for pair in await pointer_pairs(database, schema_name, table)
        )


async def _run_race(
    database: Database,
    schema_name: str,
    operation: str,
    mutation_applied: asyncio.Event,
    allow_commit: asyncio.Event,
) -> None:
    mutation = asyncio.create_task(
        _concurrent_pointer_mutation(
            database,
            schema_name,
            operation=operation,
            mutation_applied=mutation_applied,
            allow_commit=allow_commit,
        )
    )
    await mutation_applied.wait()
    retire = asyncio.create_task(
        retirement.retire_ptg2_source_predecessor(**request_params())
    )
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(asyncio.shield(retire), timeout=0.2)
    allow_commit.set()
    await mutation
    with pytest.raises(PTG2PredecessorRetirementConflict):
        await retire


async def _publication_style_binding_writer(
    database: Database,
    schema_name: str,
    control_schema_name: str,
    control_written: asyncio.Event,
    allow_projection: asyncio.Event,
) -> None:
    schema = quote_identifier(schema_name)
    control_schema = quote_identifier(control_schema_name)
    async with database.transaction() as session:
        await session.execute(
            database.text(
                f"""
                INSERT INTO {control_schema}.hp_plan_release_binding
                    (release_binding_id, serving_revision_id, role, ordinal,
                     snapshot_id)
                VALUES ('binding-race', 'release-race', 'in_network', 0,
                        :snapshot_id)
                """
            ),
            {"snapshot_id": PREDECESSOR_SNAPSHOT_ID},
        )
        control_written.set()
        await allow_projection.wait()
        await session.execute(
            database.text(
                f"""
                INSERT INTO {schema}.plan_release_snapshot_binding
                    (serving_revision_id, role, binding_ordinal, snapshot_id)
                VALUES ('release-race', 'in_network', 0, :snapshot_id)
                """
            ),
            {"snapshot_id": PREDECESSOR_SNAPSHOT_ID},
        )


async def _seed_merge_style_release_pins(
    database: Database,
    schema_name: str,
    control_schema_name: str,
) -> None:
    await database.status(
        f"""
        INSERT INTO {quote_identifier(schema_name)}.ptg2_snapshot_pin
            (owner_type, owner_id, snapshot_id, reason)
        VALUES ('plan_release', 'release-race', :snapshot_id,
                'projected release retention')
        """,
        snapshot_id=PREDECESSOR_SNAPSHOT_ID,
    )
    await database.status(
        f"""
        INSERT INTO {quote_identifier(control_schema_name)}.hp_snapshot_pin
            (owner_type, owner_id, snapshot_id, source_key, node_id)
        VALUES ('plan_release', 'release-race', :snapshot_id,
                'synthetic-source', 'node-race')
        """,
        snapshot_id=PREDECESSOR_SNAPSHOT_ID,
    )


async def _merge_style_pin_writer(
    database: Database,
    schema_name: str,
    control_schema_name: str,
    mrf_pin_deleted: asyncio.Event,
    allow_control_delete: asyncio.Event,
) -> None:
    schema = quote_identifier(schema_name)
    control_schema = quote_identifier(control_schema_name)
    async with database.transaction() as session:
        await session.execute(
            database.text(
                f"""
                DELETE FROM {schema}.ptg2_snapshot_pin
                 WHERE owner_type = 'plan_release'
                   AND owner_id = 'release-race'
                """
            )
        )
        mrf_pin_deleted.set()
        await allow_control_delete.wait()
        await session.execute(
            database.text(
                f"""
                DELETE FROM {control_schema}.hp_snapshot_pin
                 WHERE owner_type = 'plan_release'
                   AND owner_id = 'release-race'
                """
            )
        )


@pytest.mark.parametrize("operation", ["promote", "rollback"])
@pytest.mark.asyncio
async def test_postgres_retirement_serializes_with_promote_and_rollback(
    monkeypatch,
    operation: str,
) -> None:
    """A concurrent lifecycle mutation wins wholly; retirement then conflicts."""

    require_postgres_opt_in()
    database = Database()
    schema_name = f"ptg2_predecessor_race_{uuid.uuid4().hex}"
    control_schema_name = f"ptg2_predecessor_control_{uuid.uuid4().hex}"
    await database.connect()
    configure_operation(monkeypatch, database, schema_name, control_schema_name)
    mutation_applied = asyncio.Event()
    allow_commit = asyncio.Event()
    try:
        await create_schema(database, schema_name, control_schema_name)
        await seed_pair(database, schema_name)
        await _run_race(
            database,
            schema_name,
            operation,
            mutation_applied,
            allow_commit,
        )
        await _assert_race_result(database, schema_name, operation)
    finally:
        allow_commit.set()
        try:
            await drop_schema(database, schema_name)
            await drop_schema(database, control_schema_name)
        finally:
            await database.disconnect()


@pytest.mark.asyncio
async def test_postgres_retirement_follows_control_to_mrf_publication_lock_order(
    monkeypatch,
) -> None:
    """Control-first publication and retirement serialize without deadlock."""

    require_postgres_opt_in()
    database = Database()
    schema_name = f"ptg2_predecessor_binding_race_{uuid.uuid4().hex}"
    control_schema_name = f"ptg2_predecessor_control_{uuid.uuid4().hex}"
    await database.connect()
    configure_operation(monkeypatch, database, schema_name, control_schema_name)
    control_written = asyncio.Event()
    allow_projection = asyncio.Event()
    try:
        await create_schema(database, schema_name, control_schema_name)
        await seed_pair(database, schema_name)
        writer = asyncio.create_task(
            _publication_style_binding_writer(
                database,
                schema_name,
                control_schema_name,
                control_written,
                allow_projection,
            )
        )
        await control_written.wait()
        retire = asyncio.create_task(
            retirement.retire_ptg2_source_predecessor(**request_params())
        )
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(asyncio.shield(retire), timeout=0.2)
        allow_projection.set()
        await asyncio.wait_for(writer, timeout=3)
        with pytest.raises(PTG2PredecessorRetirementConflict):
            await asyncio.wait_for(retire, timeout=3)
        await assert_seeded_state_unchanged(database, schema_name)
        assert await count_rows(
            database,
            control_schema_name,
            "hp_plan_release_binding",
        ) == 1
        assert await count_rows(
            database,
            schema_name,
            "plan_release_snapshot_binding",
        ) == 1
    finally:
        allow_projection.set()
        try:
            await drop_schema(database, schema_name)
            await drop_schema(database, control_schema_name)
        finally:
            await database.disconnect()


@pytest.mark.asyncio
async def test_postgres_mrf_first_merge_contention_is_retryable_and_atomic(
    monkeypatch,
) -> None:
    """MRF-first merge contention returns a conflict and releases control."""

    require_postgres_opt_in()
    database = Database()
    schema_name = f"ptg2_predecessor_merge_race_{uuid.uuid4().hex}"
    control_schema_name = f"ptg2_predecessor_control_{uuid.uuid4().hex}"
    await database.connect()
    configure_operation(monkeypatch, database, schema_name, control_schema_name)
    mrf_pin_deleted = asyncio.Event()
    allow_control_delete = asyncio.Event()
    try:
        await create_schema(database, schema_name, control_schema_name)
        await seed_pair(database, schema_name)
        await _seed_merge_style_release_pins(
            database,
            schema_name,
            control_schema_name,
        )
        writer = asyncio.create_task(
            _merge_style_pin_writer(
                database,
                schema_name,
                control_schema_name,
                mrf_pin_deleted,
                allow_control_delete,
            )
        )
        await mrf_pin_deleted.wait()
        with pytest.raises(
            PTG2PredecessorRetirementConflict,
            match="retry",
        ):
            await asyncio.wait_for(
                retirement.retire_ptg2_source_predecessor(**request_params()),
                timeout=1,
            )
        allow_control_delete.set()
        await asyncio.wait_for(writer, timeout=3)
        await assert_seeded_state_unchanged(database, schema_name)
        assert await count_rows(
            database,
            control_schema_name,
            "hp_snapshot_pin",
        ) == 0
    finally:
        allow_control_delete.set()
        try:
            await drop_schema(database, schema_name)
            await drop_schema(database, control_schema_name)
        finally:
            await database.disconnect()
