"""Migration and common-writer PostgreSQL proof for legacy V3 repair."""

from __future__ import annotations

import asyncio
import json
import uuid

import asyncpg
import pytest

from process.ptg_parts.ptg_source_attempt_guard import source_attempt_lock_key
from tests.ptg2_legacy_v3_reconcile_postgres_support import (
    INTERNAL_RUN_ID,
    OUTER_RUN_ID,
    SNAPSHOT_ID,
    SOURCE_IMPORT_ID,
    LegacyV3PostgresContext,
    apply_reconcile_migration,
    apply_reconcile_downgrade,
    legacy_v3_postgres_context,
    seed_attempt_authority_capability,
    seed_ready_v3_target,
    seed_source_event,
)
from tests.ptg2_v4_stale_metadata_postgres_support import (
    create_stale_schema,
    postgres_dsn,
    quoted,
)


def _synthetic_audit_marker() -> dict[str, object]:
    return {
        "contract": "ptg2_legacy_v3_metadata_reconcile_v1",
        "source_file_import_id": SOURCE_IMPORT_ID,
        "snapshot_id": SNAPSHOT_ID,
        "internal_run_id": INTERNAL_RUN_ID,
        "outer_run_id": OUTER_RUN_ID,
        "target_digest": "b" * 64,
        "plan_digest": "c" * 64,
        "attachment_digest": "d" * 64,
        "catalog_digest": "e" * 64,
        "event_high_water_mark": 0,
        "retained_state_digest": "f" * 64,
        "preserved_row_digest": "1" * 64,
    }


async def _seed_audit_only(context: LegacyV3PostgresContext) -> None:
    marker = _synthetic_audit_marker()
    await context.connection.execute(
        f"""
        INSERT INTO {context.schema}.ptg2_legacy_v3_metadata_reconcile_audit (
            reconciliation_id, contract, source_file_import_id,
            snapshot_id, internal_run_id, outer_run_id, target_digest,
            plan_digest, attachment_digest, catalog_digest,
            event_high_water_mark, marker
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, 0, $11::jsonb
        )
        """,
        "a" * 64,
        marker["contract"],
        SOURCE_IMPORT_ID,
        SNAPSHOT_ID,
        INTERNAL_RUN_ID,
        OUTER_RUN_ID,
        marker["target_digest"],
        marker["plan_digest"],
        marker["attachment_digest"],
        marker["catalog_digest"],
        json.dumps(marker),
    )


async def _create_weaker_capability_table(
    connection: asyncpg.Connection,
    schema_name: str,
) -> None:
    schema = quoted(schema_name)
    await connection.execute(
        f"""
        CREATE TABLE {schema}.ptg_source_attempt_guard_capability (
            service_name varchar(32) NOT NULL,
            protocol_version varchar(64) NOT NULL,
            lock_namespace varchar(96) NOT NULL,
            hash_seed integer NOT NULL,
            database_name text NOT NULL,
            installed_at timestamptz NOT NULL DEFAULT statement_timestamp(),
            CONSTRAINT ptg_source_attempt_guard_capability_pkey
                PRIMARY KEY (service_name),
            CONSTRAINT ptg_source_attempt_guard_capability_service_check
                CHECK (service_name <> ''),
            CONSTRAINT ptg_source_attempt_guard_capability_protocol_check
                CHECK (protocol_version <> ''),
            CONSTRAINT ptg_source_attempt_guard_capability_namespace_check
                CHECK (lock_namespace <> ''),
            CONSTRAINT ptg_source_attempt_guard_capability_seed_check
                CHECK (hash_seed >= 0)
        )
        """
    )


@pytest.mark.asyncio
async def test_capability_adoption_rejects_weaker_named_constraints() -> None:
    """Refuse a same-named catalog whose checks permit noncontract rows."""

    connection = await asyncpg.connect(postgres_dsn())
    schema_name = "ptg_capability_weak_" + uuid.uuid4().hex[:12]
    try:
        await create_stale_schema(connection, schema_name)
        await _create_weaker_capability_table(connection, schema_name)
        with pytest.raises(
            asyncpg.PostgresError,
            match="PTG_SOURCE_ATTEMPT_CAPABILITY_SHAPE_CONFLICT",
        ):
            await apply_reconcile_migration(connection, schema_name)
    finally:
        await connection.execute(
            f"DROP SCHEMA IF EXISTS {quoted(schema_name)} CASCADE"
        )
        await connection.close()


async def _wait_for_advisory_waiter(
    context: LegacyV3PostgresContext,
) -> None:
    for _attempt in range(200):
        waiter_count = await context.connection.fetchval(
            "SELECT COUNT(*) FROM pg_locks "
            "WHERE locktype = 'advisory' AND NOT granted"
        )
        if waiter_count:
            return
        await asyncio.sleep(0.01)
    raise AssertionError("lifecycle advisory-lock waiter did not appear")


async def _insert_guarded_attachment(
    context: LegacyV3PostgresContext,
    external_callbacks: list[str],
) -> None:
    writer_connection = await asyncpg.connect(context.dsn)
    try:
        async with writer_connection.transaction():
            await writer_connection.execute(
                f"INSERT INTO {context.schema}.ptg2_v3_snapshot_scope "
                "(snapshot_id) VALUES ($1)",
                SNAPSHOT_ID,
            )
        external_callbacks.append("writer-committed")
    finally:
        await writer_connection.close()


@pytest.mark.asyncio
async def test_attachment_writer_rechecks_audit_after_lifecycle_lock(
    monkeypatch,
) -> None:
    """Stop a writer that passed precheck before repair won authority."""

    async with legacy_v3_postgres_context(monkeypatch) as context:
        await seed_ready_v3_target(context)
        callbacks: list[str] = []
        repair_authority = context.connection.transaction()
        await repair_authority.start()
        await context.connection.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))",
            source_attempt_lock_key(SOURCE_IMPORT_ID),
        )
        await context.connection.execute(
            "SELECT pg_advisory_xact_lock(hashtext($1))",
            "ptg2_source_pointer_gc_v1",
        )
        writer_task = asyncio.create_task(
            _insert_guarded_attachment(context, callbacks)
        )
        await _wait_for_advisory_waiter(context)
        await _seed_audit_only(context)
        await repair_authority.commit()
        writer_outcome = await asyncio.gather(
            writer_task,
            return_exceptions=True,
        )
        assert isinstance(writer_outcome[0], asyncpg.RaiseError)
        assert "PTG2_LEGACY_V3_ATTEMPT_RECONCILED" in str(
            writer_outcome[0]
        )
        assert callbacks == []
        assert await context.connection.fetchval(
            f"SELECT COUNT(*) FROM {context.schema}.ptg2_v3_snapshot_scope"
        ) == 1


async def _seed_downgrade_blocker(
    context: LegacyV3PostgresContext,
    blocker_kind: str,
) -> None:
    if blocker_kind == "event":
        await seed_source_event(context.connection, context.schema_name)
    elif blocker_kind == "audit":
        await _seed_audit_only(context)
    else:
        await seed_attempt_authority_capability(
            context.connection,
            context.schema_name,
        )


@pytest.mark.asyncio
async def test_empty_downgrade_restores_exact_v4_guard(monkeypatch) -> None:
    """Allow only an evidence-free downgrade and restore the V4 guard."""

    async with legacy_v3_postgres_context(monkeypatch) as context:
        await apply_reconcile_downgrade(
            context.connection,
            context.schema_name,
        )
        assert await context.connection.fetchval(
            "SELECT to_regprocedure($1) IS NOT NULL",
            f"{context.schema}.guard_ptg2_v4_attempt(text,text,boolean)",
        )
        assert await context.connection.fetchval(
            "SELECT to_regclass($1) IS NULL",
            f"{context.schema}.ptg_source_attempt_event",
        )
        await context.connection.execute(
            f"SELECT {context.schema}.guard_ptg2_v4_attempt(NULL, NULL, false)"
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("blocker_kind", ["event", "audit", "capability"])
async def test_downgrade_refuses_durable_evidence_or_peer(
    monkeypatch,
    blocker_kind,
) -> None:
    """Refuse evidence loss and withdrawal while the peer is active."""

    async with legacy_v3_postgres_context(monkeypatch) as context:
        await _seed_downgrade_blocker(context, blocker_kind)
        with pytest.raises(
            asyncpg.PostgresError,
            match="PTG_SOURCE_ATTEMPT_DOWNGRADE_REFUSED",
        ):
            await apply_reconcile_downgrade(
                context.connection,
                context.schema_name,
            )
