# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Shared isolated-PostgreSQL support for predecessor retirement tests."""

from __future__ import annotations

import json
import os
from typing import Any

import pytest

from db.connection import Database
from db.migration_ptg2_predecessor_retirement_audit import (
    install_predecessor_retirement_audit,
)
from process.ptg_parts import ptg2_legacy_global_projection_queue
from process.ptg_parts import source_snapshot_control
from process.ptg_parts import source_snapshot_predecessor_retirement as retirement
from process.ptg_parts.source_snapshot_predecessor_retirement_types import (
    PTG2PredecessorRetirementConflict,
)


SOURCE_KEY = "synthetic-source"
CURRENT_SNAPSHOT_ID = "snapshot-current"
PREDECESSOR_SNAPSHOT_ID = "snapshot-previous"
ROLLBACK_OWNER_ID = "rollback-owner"
IDEMPOTENCY_KEY = "retire-synthetic-001"
POSTGRES_OPT_IN = "HLTHPRT_PTG2_SHARED_GC_POSTGRES_TEST"
POINTER_TABLES = (
    "ptg2_current_source_snapshot",
    "ptg2_current_plan_source",
    "ptg2_current_snapshot",
)

MRF_TABLE_STATEMENTS = (
    """
    CREATE TABLE {schema}.ptg2_snapshot (
        snapshot_id varchar(96) PRIMARY KEY,
        import_month date NOT NULL,
        previous_snapshot_id varchar(96),
        status varchar(32) NOT NULL,
        manifest jsonb NOT NULL,
        published_at timestamptz NOT NULL DEFAULT transaction_timestamp()
    )
    """,
    """
    CREATE TABLE {schema}.ptg2_current_snapshot (
        slot varchar(32) PRIMARY KEY,
        snapshot_id varchar(96),
        previous_snapshot_id varchar(96),
        updated_at timestamptz NOT NULL DEFAULT transaction_timestamp()
    )
    """,
    """
    CREATE TABLE {schema}.ptg2_v3_snapshot_layout (
        snapshot_key bigint PRIMARY KEY,
        generation varchar(32) NOT NULL,
        state varchar(32) NOT NULL
    )
    """,
    """
    CREATE TABLE {schema}.ptg2_v3_snapshot_binding (
        snapshot_id varchar(96) PRIMARY KEY,
        snapshot_key bigint NOT NULL
    )
    """,
    """
    CREATE TABLE {schema}.ptg2_v4_snapshot_map_root (
        snapshot_key bigint PRIMARY KEY,
        state varchar(32) NOT NULL
    )
    """,
    """
    CREATE TABLE {schema}.ptg2_current_source_snapshot (
        source_key varchar(96) PRIMARY KEY,
        snapshot_id varchar(96),
        previous_snapshot_id varchar(96),
        updated_at timestamptz NOT NULL DEFAULT transaction_timestamp()
    )
    """,
    """
    CREATE TABLE {schema}.ptg2_current_plan_source (
        plan_source_key varchar(96) PRIMARY KEY,
        source_key varchar(96),
        snapshot_id varchar(96),
        previous_snapshot_id varchar(96)
    )
    """,
    """
    CREATE TABLE {schema}.ptg2_snapshot_pin (
        owner_type varchar(48) NOT NULL,
        owner_id varchar(96) NOT NULL,
        snapshot_id varchar(96) NOT NULL,
        reason varchar(256),
        PRIMARY KEY (owner_type, owner_id, snapshot_id)
    )
    """,
    """
    CREATE TABLE {schema}.plan_release_snapshot_binding (
        serving_revision_id varchar(64) NOT NULL,
        role varchar(32) NOT NULL,
        binding_ordinal integer NOT NULL,
        snapshot_id varchar(96) NOT NULL,
        PRIMARY KEY (serving_revision_id, role, binding_ordinal)
    )
    """,
    """
    CREATE TABLE {schema}.ptg2_artifact_manifest (
        artifact_id varchar(96) PRIMARY KEY,
        snapshot_id varchar(96)
    )
    """,
    """
    CREATE TABLE {schema}.ptg2_legacy_global_pointer_projection_queue (
        source_key varchar(96) PRIMARY KEY,
        requested_generation bigint NOT NULL DEFAULT 1,
        applied_generation bigint NOT NULL DEFAULT 0,
        available_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
        lease_token varchar(64),
        lease_until timestamptz,
        created_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
        updated_at timestamptz NOT NULL DEFAULT transaction_timestamp()
    )
    """,
)

CONTROL_TABLE_STATEMENTS = (
    """
    CREATE TABLE {control_schema}.hp_plan_release_binding (
        release_binding_id varchar(64) PRIMARY KEY,
        serving_revision_id varchar(64) NOT NULL,
        role varchar(64) NOT NULL,
        ordinal integer NOT NULL,
        snapshot_id varchar(96) NOT NULL
    )
    """,
    """
    CREATE TABLE {control_schema}.hp_snapshot_pin (
        owner_type varchar(64) NOT NULL,
        owner_id varchar(64) NOT NULL,
        snapshot_id varchar(96) NOT NULL,
        source_key varchar(128) NOT NULL,
        node_id varchar(64) NOT NULL,
        PRIMARY KEY (owner_type, owner_id, snapshot_id)
    )
    """,
)


class CollectingOperations:
    """Collect SQL emitted by migration helpers."""

    def __init__(self):
        self.statements: list[str] = []

    def execute(self, statement: str) -> None:
        self.statements.append(statement)


def quote_identifier(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def require_postgres_opt_in() -> None:
    if os.getenv(POSTGRES_OPT_IN) != "1":
        pytest.skip(f"set {POSTGRES_OPT_IN}=1 for the isolated PostgreSQL test")


def manifest(
    *,
    storage_generation: str = "shared_blocks_v3",
) -> str:
    return json.dumps(
        {
            "serving_index": {
                "source_key": SOURCE_KEY,
                "arch_version": "postgres_binary_v3",
                "storage_generation": storage_generation,
                "shared_snapshot_key": 17,
            }
        },
        sort_keys=True,
    )


async def create_schema(
    database: Database,
    schema_name: str,
    control_schema_name: str,
) -> None:
    schema = quote_identifier(schema_name)
    control_schema = quote_identifier(control_schema_name)
    audit_operations = CollectingOperations()
    install_predecessor_retirement_audit(audit_operations, schema_name)
    statements = [
        statement.format(schema=schema)
        for statement in MRF_TABLE_STATEMENTS
    ]
    statements.extend(
        statement.format(control_schema=control_schema)
        for statement in CONTROL_TABLE_STATEMENTS
    )
    async with database.acquire() as connection:
        await connection.status(f"CREATE SCHEMA {schema}")
        await connection.status(f"CREATE SCHEMA {control_schema}")
        for statement in (*statements, *audit_operations.statements):
            await connection.status(statement)


async def _seed_snapshots(connection: Any, schema: str) -> None:
    for snapshot_id, previous_snapshot_id in (
        (PREDECESSOR_SNAPSHOT_ID, None),
        (CURRENT_SNAPSHOT_ID, PREDECESSOR_SNAPSHOT_ID),
    ):
        await connection.status(
            f"""
            INSERT INTO {schema}.ptg2_snapshot
                (snapshot_id, import_month, previous_snapshot_id,
                 status, manifest)
            VALUES
                (:snapshot_id, DATE '2026-07-01',
                 :previous_snapshot_id, 'published',
                 CAST(:manifest AS jsonb))
            """,
            snapshot_id=snapshot_id,
            previous_snapshot_id=previous_snapshot_id,
            manifest=manifest(),
        )


async def _seed_source_and_plan_pointers(connection: Any, schema: str) -> None:
    await connection.status(
        f"""
        INSERT INTO {schema}.ptg2_current_source_snapshot
            (source_key, snapshot_id, previous_snapshot_id)
        VALUES (:source_key, :current_snapshot_id,
                :predecessor_snapshot_id)
        """,
        **pair_params(),
    )
    for ordinal in range(2):
        await connection.status(
            f"""
            INSERT INTO {schema}.ptg2_current_plan_source
                (plan_source_key, source_key, snapshot_id,
                 previous_snapshot_id)
            VALUES (:plan_source_key, :source_key,
                    :current_snapshot_id, :predecessor_snapshot_id)
            """,
            plan_source_key=f"synthetic-plan-{ordinal}",
            **pair_params(),
        )


async def _seed_predecessor_shared_layout(
    connection: Any,
    schema: str,
) -> None:
    await connection.status(
        f"""
        INSERT INTO {schema}.ptg2_v3_snapshot_layout
            (snapshot_key, generation, state)
        VALUES (17, 'shared_blocks_v3', 'sealed')
        """
    )
    await connection.status(
        f"""
        INSERT INTO {schema}.ptg2_v3_snapshot_binding
            (snapshot_id, snapshot_key)
        VALUES (:snapshot_id, 17)
        """,
        snapshot_id=PREDECESSOR_SNAPSHOT_ID,
    )


async def _seed_global_pointer_and_pin(
    connection: Any,
    schema: str,
    *,
    include_rollback_pin: bool,
) -> None:
    await connection.status(
        f"""
        INSERT INTO {schema}.ptg2_current_snapshot
            (slot, snapshot_id, previous_snapshot_id)
        VALUES ('current', :current_snapshot_id,
                :predecessor_snapshot_id)
        """,
        **pair_params(),
    )
    if not include_rollback_pin:
        return
    await connection.status(
        f"""
        INSERT INTO {schema}.ptg2_snapshot_pin
            (owner_type, owner_id, snapshot_id, reason)
        VALUES ('ptg_v4_rollback', :rollback_owner_id,
                :predecessor_snapshot_id, 'exact rollback retention')
        """,
        rollback_owner_id=ROLLBACK_OWNER_ID,
        **pair_params(),
    )


async def seed_pair(
    database: Database,
    schema_name: str,
    *,
    include_rollback_pin: bool = True,
) -> None:
    schema = quote_identifier(schema_name)
    async with database.acquire() as connection:
        await _seed_snapshots(connection, schema)
        await _seed_predecessor_shared_layout(connection, schema)
        await _seed_source_and_plan_pointers(connection, schema)
        await _seed_global_pointer_and_pin(
            connection,
            schema,
            include_rollback_pin=include_rollback_pin,
        )


def pair_params() -> dict[str, str]:
    return {
        "source_key": SOURCE_KEY,
        "current_snapshot_id": CURRENT_SNAPSHOT_ID,
        "predecessor_snapshot_id": PREDECESSOR_SNAPSHOT_ID,
    }


def request_params(
    *,
    rollback_pin_mode: str = "owned",
) -> dict[str, Any]:
    return {
        **pair_params(),
        "rollback_pin_mode": rollback_pin_mode,
        "rollback_owner_id": (
            ROLLBACK_OWNER_ID if rollback_pin_mode == "owned" else None
        ),
        "actor": "operator@example.invalid",
        "reason": "retention window complete",
        "idempotency_key": IDEMPOTENCY_KEY,
    }


async def count_rows(
    database: Database,
    schema_name: str,
    table: str,
) -> int:
    return int(
        await database.scalar(
            f"SELECT COUNT(*) FROM "
            f"{quote_identifier(schema_name)}.{quote_identifier(table)}"
        )
        or 0
    )


async def pointer_pairs(
    database: Database,
    schema_name: str,
    table: str,
) -> list[tuple[str, str | None]]:
    rows = await database.all(
        f"""
        SELECT snapshot_id, previous_snapshot_id
          FROM {quote_identifier(schema_name)}.{quote_identifier(table)}
         ORDER BY 1, 2 NULLS FIRST
        """
    )
    return [
        (str(row[0]), str(row[1]) if row[1] is not None else None)
        for row in rows
    ]


def configure_operation(
    monkeypatch: pytest.MonkeyPatch,
    database: Database,
    schema_name: str,
    control_schema_name: str,
) -> None:
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema_name)
    monkeypatch.setenv("DB_SCHEMA", schema_name)
    monkeypatch.setenv(
        "HLTHPRT_" + "IMP" + "ORT_CONTROL_SCHEMA",
        control_schema_name,
    )
    monkeypatch.setattr(retirement, "db", database)
    monkeypatch.setattr(source_snapshot_control, "db", database)
    monkeypatch.setattr(ptg2_legacy_global_projection_queue, "db", database)


async def drop_schema(database: Database, schema_name: str) -> None:
    await database.status(
        f"DROP SCHEMA IF EXISTS {quote_identifier(schema_name)} CASCADE"
    )


async def assert_seeded_state_unchanged(
    database: Database,
    schema_name: str,
    *,
    expected_pin_count: int = 1,
) -> None:
    for table in POINTER_TABLES:
        assert all(
            pair == (CURRENT_SNAPSHOT_ID, PREDECESSOR_SNAPSHOT_ID)
            for pair in await pointer_pairs(database, schema_name, table)
        )
    assert (
        await count_rows(database, schema_name, "ptg2_snapshot_pin")
        == expected_pin_count
    )
    assert (
        await count_rows(
            database,
            schema_name,
            "ptg2_predecessor_retirement_audit",
        )
        == 0
    )


async def _require_retirement_conflict_without_mutation(
    database: Database,
    schema_name: str,
    message: str,
) -> None:
    with pytest.raises(PTG2PredecessorRetirementConflict, match=message):
        await retirement.retire_ptg2_source_predecessor(**request_params())
    await assert_seeded_state_unchanged(database, schema_name)


async def require_control_pin_blocks_without_mutation(
    database: Database,
    schema_name: str,
    control_schema_name: str,
) -> None:
    await database.status(
        f"""
        INSERT INTO {quote_identifier(control_schema_name)}.hp_snapshot_pin
            (owner_type, owner_id, snapshot_id, source_key, node_id)
        VALUES
            ('plan_release', 'release-1', :snapshot_id, :source_key, 'node-1')
        """,
        snapshot_id=PREDECESSOR_SNAPSHOT_ID,
        source_key=SOURCE_KEY,
    )
    await _require_retirement_conflict_without_mutation(
        database,
        schema_name,
        "non-target retention pin",
    )
    await database.status(
        f"DELETE FROM {quote_identifier(control_schema_name)}.hp_snapshot_pin"
    )


async def require_release_bindings_block_without_mutation(
    database: Database,
    schema_name: str,
    control_schema_name: str,
) -> None:
    schema = quote_identifier(schema_name)
    control_schema = quote_identifier(control_schema_name)
    await database.status(
        f"""
        INSERT INTO {schema}.plan_release_snapshot_binding
            (serving_revision_id, role, binding_ordinal, snapshot_id)
        VALUES ('release-mrf', 'in_network', 0, :snapshot_id)
        """,
        snapshot_id=PREDECESSOR_SNAPSHOT_ID,
    )
    await _require_retirement_conflict_without_mutation(
        database,
        schema_name,
        "release binding",
    )
    await database.status(f"DELETE FROM {schema}.plan_release_snapshot_binding")
    await database.status(
        f"""
        INSERT INTO {control_schema}.hp_plan_release_binding
            (release_binding_id, serving_revision_id, role, ordinal, snapshot_id)
        VALUES ('binding-control', 'release-control', 'in_network', 0,
                :snapshot_id)
        """,
        snapshot_id=PREDECESSOR_SNAPSHOT_ID,
    )
    await _require_retirement_conflict_without_mutation(
        database,
        schema_name,
        "release binding",
    )
    await database.status(
        f"DELETE FROM {control_schema}.hp_plan_release_binding"
    )
