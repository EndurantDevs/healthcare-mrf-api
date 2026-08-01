# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""PostgreSQL compatibility proof for the legacy V3 reconciliation guard."""

from __future__ import annotations

import importlib
import json
import uuid
from unittest.mock import AsyncMock

import asyncpg
import pytest

from process.ptg_parts.ptg2_v4_stale_metadata_fence import (
    StaleMetadataFenceError,
    lock_writable_snapshot,
)
from tests.ptg2_legacy_v3_reconcile_postgres_support import (
    apply_reconcile_downgrade,
    apply_reconcile_migration,
    legacy_v3_postgres_context,
    quoted,
)
from tests.ptg2_v4_stale_metadata_postgres_support import (
    create_stale_schema,
    drop_stale_schema,
    postgres_dsn,
)


process_ptg = importlib.import_module("process.ptg")

_V4_SNAPSHOT_ID = "ptg2:202607:synthetic-v4-compatible"
_V4_RUN_ID = "ptg2:synthetic-v4-compatible-run"
_V3_SNAPSHOT_ID = "ptg2:202607:synthetic-v3-fenced"
_V3_RUN_ID = "ptg2:synthetic-v3-fenced-run"


async def _seed_pair(
    connection: asyncpg.Connection,
    schema_name: str,
    *,
    snapshot_id: str,
    internal_run_id: str,
    generation: str,
    snapshot_status: str = "validated",
) -> None:
    schema = quoted(schema_name)
    options = json.dumps({"storage_generation": generation})
    await connection.execute(
        f"""
        INSERT INTO {schema}.ptg2_import_run (
            import_run_id, status, started_at, heartbeat_at,
            options, report, error
        ) VALUES (
            $1, 'failed', timezone('UTC', now()), timezone('UTC', now()),
            $2::json, '{{"before":true}}'::json, 'synthetic prior error'
        )
        """,
        internal_run_id,
        options,
    )
    await connection.execute(
        f"""
        INSERT INTO {schema}.ptg2_snapshot (
            snapshot_id, import_run_id, status, created_at, manifest
        ) VALUES (
            $1, $2, $3, timezone('UTC', now()),
            '{{"terminal":"synthetic"}}'::json
        )
        """,
        snapshot_id,
        internal_run_id,
        snapshot_status,
    )


async def _guard_pair(
    context,
    *,
    snapshot_id: str,
    internal_run_id: str,
    allow_reconciled: bool = False,
) -> None:
    async with context.test_database.transaction() as session:
        await lock_writable_snapshot(
            session,
            context.test_database,
            schema_name=context.schema_name,
            snapshot_id=snapshot_id,
            internal_run_id=internal_run_id,
            allow_reconciled=allow_reconciled,
        )


async def _mark_v4_reconciled(
    connection: asyncpg.Connection,
    schema_name: str,
    snapshot_id: str,
) -> None:
    await connection.execute(
        f"""
        UPDATE {quoted(schema_name)}.ptg2_v4_attempt_fence
           SET state = 'reconciled',
               target_digest = repeat('1', 64),
               plan_digest = repeat('2', 64),
               marker_digest = repeat('3', 64),
               marker = '{{"synthetic":true}}'::jsonb,
               reconciled_at = statement_timestamp()
         WHERE snapshot_id = $1
        """,
        snapshot_id,
    )


async def _insert_legacy_audit(
    connection: asyncpg.Connection,
    schema_name: str,
) -> None:
    marker_by_name = {
        "contract": "ptg2_legacy_v3_metadata_reconcile_v1",
        "source_file_import_id": "synthetic-source-v3-fenced",
        "snapshot_id": _V3_SNAPSHOT_ID,
        "internal_run_id": _V3_RUN_ID,
        "outer_run_id": "run_synthetic_v3_fenced",
        "target_digest": "1" * 64,
        "plan_digest": "2" * 64,
        "attachment_digest": "3" * 64,
        "catalog_digest": "4" * 64,
        "event_high_water_mark": 0,
        "retained_state_digest": "5" * 64,
        "preserved_row_digest": "6" * 64,
    }
    await connection.execute(
        f"""
        INSERT INTO {quoted(schema_name)}.
            ptg2_legacy_v3_metadata_reconcile_audit (
                reconciliation_id, contract, source_file_import_id,
                snapshot_id, internal_run_id, outer_run_id,
                target_digest, plan_digest, attachment_digest,
                catalog_digest, event_high_water_mark, marker
            ) VALUES (
                repeat('7', 64), $1, $2, $3, $4, $5,
                $6, $7, $8, $9, 0, $10::jsonb
            )
        """,
        marker_by_name["contract"],
        marker_by_name["source_file_import_id"],
        _V3_SNAPSHOT_ID,
        _V3_RUN_ID,
        marker_by_name["outer_run_id"],
        marker_by_name["target_digest"],
        marker_by_name["plan_digest"],
        marker_by_name["attachment_digest"],
        marker_by_name["catalog_digest"],
        json.dumps(marker_by_name, sort_keys=True),
    )


async def _pair_versions(
    connection: asyncpg.Connection,
    schema_name: str,
    snapshot_id: str,
    internal_run_id: str,
) -> tuple[str, str]:
    row = await connection.fetchrow(
        f"""
        SELECT
          (SELECT xmin::text FROM {quoted(schema_name)}.ptg2_snapshot
            WHERE snapshot_id = $1) AS snapshot_xmin,
          (SELECT xmin::text FROM {quoted(schema_name)}.ptg2_import_run
            WHERE import_run_id = $2) AS run_xmin
        """,
        snapshot_id,
        internal_run_id,
    )
    return str(row["snapshot_xmin"]), str(row["run_xmin"])


@pytest.mark.asyncio
async def test_new_guard_preserves_active_v4_finalizer(monkeypatch):
    async with legacy_v3_postgres_context(monkeypatch) as context:
        await _seed_pair(
            context.connection,
            context.schema_name,
            snapshot_id=_V4_SNAPSHOT_ID,
            internal_run_id=_V4_RUN_ID,
            generation="shared_blocks_v4",
        )
        await _guard_pair(
            context,
            snapshot_id=_V4_SNAPSHOT_ID,
            internal_run_id=_V4_RUN_ID,
        )
        fence_nonce = await context.connection.fetchval(
            f"SELECT fence_nonce FROM {context.schema}.ptg2_v4_attempt_fence "
            "WHERE snapshot_id = $1",
            _V4_SNAPSHOT_ID,
        )
        monkeypatch.setattr(process_ptg, "db", context.test_database)

        await process_ptg._finalize_resumed_terminal_attempt(
            {"snapshot_id": _V4_SNAPSHOT_ID, "import_run_id": _V4_RUN_ID},
            internal_run_id=_V4_RUN_ID,
        )

        final_state = await context.connection.fetchrow(
            f"""
            SELECT run.status, run.error, run.report,
                   fence.state, fence.fence_nonce
              FROM {context.schema}.ptg2_import_run AS run
              JOIN {context.schema}.ptg2_v4_attempt_fence AS fence
                ON fence.internal_run_id = run.import_run_id
             WHERE run.import_run_id = $1
            """,
            _V4_RUN_ID,
        )
        assert final_state["status"] == "validated"
        assert final_state["error"] is None
        assert json.loads(final_state["report"]) == {"terminal": "synthetic"}
        assert final_state["state"] == "active"
        assert final_state["fence_nonce"] == fence_nonce


@pytest.mark.asyncio
async def test_new_guard_keeps_v4_reconciled_override(monkeypatch):
    async with legacy_v3_postgres_context(monkeypatch) as context:
        await _seed_pair(
            context.connection,
            context.schema_name,
            snapshot_id=_V4_SNAPSHOT_ID,
            internal_run_id=_V4_RUN_ID,
            generation="shared_blocks_v4",
        )
        await _guard_pair(
            context,
            snapshot_id=_V4_SNAPSHOT_ID,
            internal_run_id=_V4_RUN_ID,
        )
        await _mark_v4_reconciled(
            context.connection,
            context.schema_name,
            _V4_SNAPSHOT_ID,
        )

        monkeypatch.setattr(process_ptg, "db", context.test_database)
        with pytest.raises(StaleMetadataFenceError, match="durably fenced"):
            await process_ptg._finalize_resumed_terminal_attempt(
                {
                    "snapshot_id": _V4_SNAPSHOT_ID,
                    "import_run_id": _V4_RUN_ID,
                },
                internal_run_id=_V4_RUN_ID,
            )
        await _guard_pair(
            context,
            snapshot_id=_V4_SNAPSHOT_ID,
            internal_run_id=_V4_RUN_ID,
            allow_reconciled=True,
        )


@pytest.mark.asyncio
async def test_new_guard_preserves_non_v4_early_return(monkeypatch):
    async with legacy_v3_postgres_context(monkeypatch) as context:
        await _seed_pair(
            context.connection,
            context.schema_name,
            snapshot_id=_V3_SNAPSHOT_ID,
            internal_run_id=_V3_RUN_ID,
            generation="shared_blocks_v3",
        )
        before_versions = await _pair_versions(
            context.connection,
            context.schema_name,
            _V3_SNAPSHOT_ID,
            _V3_RUN_ID,
        )

        await _guard_pair(
            context,
            snapshot_id=_V3_SNAPSHOT_ID,
            internal_run_id=_V3_RUN_ID,
        )
        await _guard_pair(
            context,
            snapshot_id="",
            internal_run_id=_V3_RUN_ID,
        )

        assert await _pair_versions(
            context.connection,
            context.schema_name,
            _V3_SNAPSHOT_ID,
            _V3_RUN_ID,
        ) == before_versions
        assert await context.connection.fetchval(
            f"SELECT COUNT(*) FROM {context.schema}.ptg2_v4_attempt_fence"
        ) == 0


@pytest.mark.asyncio
async def test_legacy_audit_blocks_real_finalizer_before_writes(monkeypatch):
    async with legacy_v3_postgres_context(monkeypatch) as context:
        await _seed_pair(
            context.connection,
            context.schema_name,
            snapshot_id=_V3_SNAPSHOT_ID,
            internal_run_id=_V3_RUN_ID,
            generation="shared_blocks_v3",
        )
        await _insert_legacy_audit(context.connection, context.schema_name)
        before_versions = await _pair_versions(
            context.connection,
            context.schema_name,
            _V3_SNAPSHOT_ID,
            _V3_RUN_ID,
        )
        stage_drop_mock = AsyncMock()
        monkeypatch.setattr(process_ptg, "db", context.test_database)
        monkeypatch.setattr(
            process_ptg,
            "_drop_ptg2_snapshot_table_names",
            stage_drop_mock,
        )
        with pytest.raises(StaleMetadataFenceError, match="durably fenced"):
            await process_ptg._finalize_resumed_terminal_attempt(
                {
                    "snapshot_id": _V3_SNAPSHOT_ID,
                    "import_run_id": _V3_RUN_ID,
                },
                internal_run_id=_V3_RUN_ID,
            )

        stage_drop_mock.assert_not_awaited()
        assert await _pair_versions(
            context.connection,
            context.schema_name,
            _V3_SNAPSHOT_ID,
            _V3_RUN_ID,
        ) == before_versions
        assert await context.connection.fetchval(
            f"SELECT COUNT(*) FROM {context.schema}.ptg2_v4_attempt_stage"
        ) == 0


async def _guard_definition(
    connection: asyncpg.Connection,
    schema_name: str,
) -> str:
    signature = f"{schema_name}.guard_ptg2_v4_attempt(text,text,boolean)"
    return str(
        await connection.fetchval(
            "SELECT pg_get_functiondef($1::regprocedure)",
            signature,
        )
    )


@pytest.mark.asyncio
async def test_empty_downgrade_restores_exact_v4_guard_behavior():
    dsn = postgres_dsn()
    schema_name = "ptg_v3_v4_compat_" + uuid.uuid4().hex[:12]
    connection = await asyncpg.connect(dsn)
    schema = quoted(schema_name)
    try:
        await create_stale_schema(connection, schema_name)
        predecessor_definition = await _guard_definition(
            connection,
            schema_name,
        )
        await _seed_pair(
            connection,
            schema_name,
            snapshot_id=_V4_SNAPSHOT_ID,
            internal_run_id=_V4_RUN_ID,
            generation="shared_blocks_v4",
        )
        await connection.execute(
            f"SELECT {schema}.guard_ptg2_v4_attempt($1, $2, false)",
            _V4_SNAPSHOT_ID,
            _V4_RUN_ID,
        )
        await apply_reconcile_migration(connection, schema_name)
        assert await _guard_definition(connection, schema_name) != (
            predecessor_definition
        )

        await apply_reconcile_downgrade(connection, schema_name)

        assert await _guard_definition(
            connection,
            schema_name,
        ) == predecessor_definition
        await _mark_v4_reconciled(
            connection,
            schema_name,
            _V4_SNAPSHOT_ID,
        )
        with pytest.raises(asyncpg.PostgresError, match="STALE_METADATA_FENCE"):
            await connection.execute(
                f"SELECT {schema}.guard_ptg2_v4_attempt($1, $2, false)",
                _V4_SNAPSHOT_ID,
                _V4_RUN_ID,
            )
        await connection.execute(
            f"SELECT {schema}.guard_ptg2_v4_attempt($1, $2, true)",
            _V4_SNAPSHOT_ID,
            _V4_RUN_ID,
        )
    finally:
        await connection.close()
        await drop_stale_schema(dsn, schema_name)
