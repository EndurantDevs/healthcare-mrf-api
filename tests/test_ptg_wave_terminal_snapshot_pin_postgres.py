# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""PostgreSQL crash/replay proof for ordinary terminal manifest pins."""

from __future__ import annotations

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from process.ptg_parts.ptg_wave_terminal_snapshot_pin import (
    ORDINARY_TERMINAL_PIN_OWNER_TYPE,
    delete_ordinary_terminal_snapshot_pin,
    insert_ordinary_terminal_snapshot_pin,
)
from process.ptg_wave_receipt_contract import ordinary_cutover_id
from tests.test_ptg_wave_recovery_storage_postgres import _dsn, _quote


OPERATION_ID = "8" * 64
SNAPSHOT_ID = "ordinary-terminal-snapshot"
INTERNAL_RUN_ID = "ordinary-terminal-engine-run"


def _options() -> dict[str, object]:
    return {
        "ordinary_cutover_operation_id": OPERATION_ID,
        "ordinary_cutover_id": ordinary_cutover_id(OPERATION_ID),
        "ordinary_cutover_member_ordinal": 7,
    }


async def _create_terminal_pin_schema(engine, schema: str) -> None:
    statements = (
        f"CREATE TABLE {schema}.ptg2_snapshot ("
        "snapshot_id varchar(128) PRIMARY KEY, "
        "import_run_id varchar(96) NOT NULL)",
        f"CREATE TABLE {schema}.ptg2_snapshot_pin ("
        "owner_type varchar(48) NOT NULL, owner_id varchar(96) NOT NULL, "
        "snapshot_id varchar(128) NOT NULL REFERENCES "
        f"{schema}.ptg2_snapshot(snapshot_id) ON DELETE RESTRICT, "
        "reason varchar(256), created_at timestamptz NOT NULL, "
        "PRIMARY KEY (owner_type, owner_id, snapshot_id))",
        f"CREATE TABLE {schema}.terminal_receipt ("
        "member_ordinal integer PRIMARY KEY)",
    )
    async with engine.begin() as connection:
        await connection.execute(text(f"DROP SCHEMA IF EXISTS {schema} CASCADE"))
        await connection.execute(text(f"CREATE SCHEMA {schema}"))
        for statement in statements:
            await connection.execute(text(statement))
        await connection.execute(
            text(
                f"INSERT INTO {schema}.ptg2_snapshot "
                "(snapshot_id, import_run_id) VALUES (:snapshot_id, :run_id)"
            ),
            {"snapshot_id": SNAPSHOT_ID, "run_id": INTERNAL_RUN_ID},
        )


async def _insert_and_assert_terminal_pin(sessions, schema_name, schema) -> None:
    async with sessions.begin() as session:
        pin = await insert_ordinary_terminal_snapshot_pin(
            session,
            schema_name=schema_name,
            snapshot_id=SNAPSHOT_ID,
            internal_run_id=INTERNAL_RUN_ID,
            options=_options(),
        )
        assert pin is not None
        assert pin.owner_id == f"{OPERATION_ID}:7"
    async with sessions() as session:
        assert await session.scalar(
            text(
                f"SELECT COUNT(*) FROM {schema}.ptg2_snapshot_pin "
                "WHERE owner_type = :owner_type"
            ),
            {"owner_type": ORDINARY_TERMINAL_PIN_OWNER_TYPE},
        ) == 1


async def _assert_terminal_pin_release_rolls_back(
    sessions,
    schema_name: str,
    schema: str,
) -> None:
    with pytest.raises(RuntimeError, match="crash before receipt commit"):
        async with sessions.begin() as session:
            await session.execute(
                text(
                    f"INSERT INTO {schema}.terminal_receipt "
                    "(member_ordinal) VALUES (7)"
                )
            )
            assert await delete_ordinary_terminal_snapshot_pin(
                session,
                schema_name=schema_name,
                operation_id=OPERATION_ID,
                member_ordinal=7,
                snapshot_id=SNAPSHOT_ID,
            ) == 1
            raise RuntimeError("crash before receipt commit")
    async with sessions() as session:
        assert await session.scalar(
            text(f"SELECT COUNT(*) FROM {schema}.ptg2_snapshot_pin")
        ) == 1
        assert await session.scalar(
            text(f"SELECT COUNT(*) FROM {schema}.terminal_receipt")
        ) == 0


async def _commit_terminal_receipt_and_release_pin(
    sessions,
    schema_name: str,
    schema: str,
) -> None:
    async with sessions.begin() as session:
        await session.execute(
            text(
                f"INSERT INTO {schema}.terminal_receipt "
                "(member_ordinal) VALUES (7)"
            )
        )
        assert await delete_ordinary_terminal_snapshot_pin(
            session,
            schema_name=schema_name,
            operation_id=OPERATION_ID,
            member_ordinal=7,
            snapshot_id=SNAPSHOT_ID,
        ) == 1
    async with sessions.begin() as session:
        assert await delete_ordinary_terminal_snapshot_pin(
            session,
            schema_name=schema_name,
            operation_id=OPERATION_ID,
            member_ordinal=7,
            snapshot_id=SNAPSHOT_ID,
        ) == 0


@pytest.mark.asyncio
async def test_pin_spans_completion_to_receipt_and_crash_rolls_back_release():
    """Prove the terminal pin survives completion and receipt rollback."""
    schema_name = "ptg_terminal_manifest_pin"
    schema = _quote(schema_name)
    engine = create_async_engine(
        _dsn().replace("postgresql://", "postgresql+asyncpg://", 1)
    )
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    await _create_terminal_pin_schema(engine, schema)

    try:
        await _insert_and_assert_terminal_pin(sessions, schema_name, schema)
        await _assert_terminal_pin_release_rolls_back(
            sessions,
            schema_name,
            schema,
        )
        await _commit_terminal_receipt_and_release_pin(
            sessions,
            schema_name,
            schema,
        )
    finally:
        async with engine.begin() as connection:
            await connection.execute(text(f"DROP SCHEMA {schema} CASCADE"))
        await engine.dispose()
