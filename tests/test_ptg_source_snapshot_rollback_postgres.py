# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""PostgreSQL proof for pinned published-snapshot rollback."""

from __future__ import annotations

import os
import re

import pytest
from sqlalchemy.engine import make_url

from api.ptg2_snapshot import resolve_current_ptg2_snapshot_id
from db.connection import db
from process.ptg_parts.ptg2_shared_gc import sweep_ptg2_shared_blocks
from process.ptg_parts.source_snapshot_rollback import (
    rollback_pinned_ptg2_source_snapshot,
)
from tests.ptg_source_snapshot_rollback_postgres_support import (
    CURRENT_OWNER_ID,
    CURRENT_SNAPSHOT_ID,
    SCHEMA_NAME,
    SOURCE_KEY,
    TARGET_OWNER_ID,
    TARGET_SNAPSHOT_ID,
    insert_snapshot_scope,
    seed_rollback_pair,
)


OPT_IN_DSN_ENV = "HLTHPRT_PTG2_ROLLBACK_POSTGRES_DSN"
DISPOSABLE_DATABASE_PATTERN = re.compile(
    r"^(ptg2_v3_lifecycle_test_[a-z0-9_]+|ptg_v4_acceptance_test)$"
)
def _configure_database(monkeypatch: pytest.MonkeyPatch, dsn: str) -> str:
    """Select only the known disposable local or CI database."""

    url = make_url(dsn)
    database_name = str(url.database or "")
    if (
        not url.drivername.startswith("postgresql")
        or not url.host
        or not url.username
        or not DISPOSABLE_DATABASE_PATTERN.fullmatch(database_name)
    ):
        pytest.fail(f"{OPT_IN_DSN_ENV} must identify a disposable PostgreSQL DB")
    monkeypatch.setenv("HLTHPRT_DB_DRIVER", "asyncpg")
    monkeypatch.setenv("HLTHPRT_DB_HOST", str(url.host))
    monkeypatch.setenv("HLTHPRT_DB_PORT", str(url.port or 5432))
    monkeypatch.setenv("HLTHPRT_DB_USER", str(url.username))
    monkeypatch.setenv("HLTHPRT_DB_PASSWORD", str(url.password or ""))
    monkeypatch.setenv("HLTHPRT_DB_DATABASE", database_name)
    monkeypatch.delenv("HLTHPRT_DB_DATABASE_OVERRIDE", raising=False)
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", SCHEMA_NAME)
    monkeypatch.setenv("HLTHPRT_PTG2_V3_BLOCK_GC_GRACE_SECONDS", "0")
    return database_name


async def _require_empty_database(database_name: str) -> None:
    assert await db.scalar("SELECT current_database()") == database_name
    required_tables = (
        "ptg2_current_plan_source",
        "ptg2_current_snapshot",
        "ptg2_current_source_snapshot",
        "ptg2_snapshot",
        "ptg2_v3_snapshot_binding",
        "ptg2_v3_snapshot_layout",
    )
    for table_name in required_tables:
        row_count = await db.scalar(
            f'SELECT COUNT(*) FROM "{SCHEMA_NAME}"."{table_name}"'
        )
        assert int(row_count or 0) == 0


async def _snapshot_metadata() -> list[tuple]:
    records = await db.all(
        f"""
        SELECT snapshot_id, import_run_id, import_month, status, created_at,
               validated_at, published_at, previous_snapshot_id,
               manifest::text
          FROM "{SCHEMA_NAME}".ptg2_snapshot
         ORDER BY snapshot_id
        """
    )
    return [tuple(record) for record in records]


async def _pointer_pair(table_name: str) -> tuple[str, str | None]:
    records = await db.all(
        f"""
        SELECT snapshot_id, previous_snapshot_id
          FROM "{SCHEMA_NAME}"."{table_name}"
         LIMIT 1
        """
    )
    assert len(records) == 1
    record = records[0]
    return str(record[0]), str(record[1]) if record[1] else None


async def _require_forward_pointer_pair() -> None:
    """Prove a rejected rollback left every serving pointer unchanged."""

    for table_name in (
        "ptg2_current_source_snapshot",
        "ptg2_current_plan_source",
        "ptg2_current_snapshot",
    ):
        assert await _pointer_pair(table_name) == (
            CURRENT_SNAPSHOT_ID,
            TARGET_SNAPSHOT_ID,
        )


async def _resolve_snapshot(arguments_by_name: dict[str, object]) -> str | None:
    async with db.transaction() as session:
        return await resolve_current_ptg2_snapshot_id(
            session,
            arguments_by_name,
        )


async def _cleanup() -> None:
    for table_name in (
        "ptg2_current_plan_source",
        "ptg2_current_snapshot",
        "ptg2_current_source_snapshot",
        "ptg2_snapshot_pin",
        "ptg2_v3_candidate_audit_attestation",
        "ptg2_v3_snapshot_plan_scope",
        "ptg2_v3_snapshot_scope",
        "ptg2_v3_snapshot_binding",
        "ptg2_snapshot",
        "ptg2_v3_snapshot_layout",
    ):
        await db.status(f'DELETE FROM "{SCHEMA_NAME}"."{table_name}"')


async def _require_incomplete_target_rejected(
    metadata_before: list[tuple],
    *,
    message: str,
) -> None:
    """Require relational validation to fail before any pointer mutation."""

    with pytest.raises(ValueError, match=message):
        await rollback_pinned_ptg2_source_snapshot(
            source_key=SOURCE_KEY,
            snapshot_id=TARGET_SNAPSHOT_ID,
            expected_current_snapshot_id=CURRENT_SNAPSHOT_ID,
            rollback_owner_id=TARGET_OWNER_ID,
        )
    await _require_forward_pointer_pair()
    assert await _snapshot_metadata() == metadata_before


async def _verify_incomplete_target_relations_rejected(
    metadata_before: list[tuple],
) -> None:
    """Prove missing scope and inactive audit rows cannot be published."""

    await db.status(
        f"""
        DELETE FROM "{SCHEMA_NAME}".ptg2_v3_snapshot_scope
         WHERE snapshot_id = :snapshot_id
        """,
        snapshot_id=TARGET_SNAPSHOT_ID,
    )
    await _require_incomplete_target_rejected(
        metadata_before,
        message="no immutable snapshot scope",
    )
    await insert_snapshot_scope(TARGET_SNAPSHOT_ID)
    await db.status(
        f"""
        UPDATE "{SCHEMA_NAME}".ptg2_v3_candidate_audit_attestation
           SET activated_at = NULL
         WHERE snapshot_id = :snapshot_id
        """,
        snapshot_id=TARGET_SNAPSHOT_ID,
    )
    await _require_incomplete_target_rejected(
        metadata_before,
        message="no activated source-matched audit attestation",
    )
    await db.status(
        f"""
        UPDATE "{SCHEMA_NAME}".ptg2_v3_candidate_audit_attestation
           SET activated_at = attested_at
         WHERE snapshot_id = :snapshot_id
        """,
        snapshot_id=TARGET_SNAPSHOT_ID,
    )


async def _rollback_and_verify_resolution() -> None:
    """Roll B to A and verify every serving pointer and explicit B access."""

    first_report = await rollback_pinned_ptg2_source_snapshot(
        source_key=SOURCE_KEY,
        snapshot_id=TARGET_SNAPSHOT_ID,
        expected_current_snapshot_id=CURRENT_SNAPSHOT_ID,
        rollback_owner_id=TARGET_OWNER_ID,
    )
    assert first_report["status"] == "rolled_back"
    for table_name in (
        "ptg2_current_source_snapshot",
        "ptg2_current_plan_source",
        "ptg2_current_snapshot",
    ):
        assert await _pointer_pair(table_name) == (
            TARGET_SNAPSHOT_ID,
            CURRENT_SNAPSHOT_ID,
        )
    assert await _resolve_snapshot({"source_key": SOURCE_KEY}) == (
        TARGET_SNAPSHOT_ID
    )
    assert await _resolve_snapshot(
        {
            "snapshot_id": CURRENT_SNAPSHOT_ID,
            "source_key": SOURCE_KEY,
        }
    ) == CURRENT_SNAPSHOT_ID


async def _verify_write_free_retry() -> None:
    """Require an exact retry to preserve the pointer update timestamp."""

    source_updated_at = await db.scalar(
        f"""
        SELECT updated_at
          FROM "{SCHEMA_NAME}".ptg2_current_source_snapshot
         WHERE source_key = :source_key
        """,
        source_key=SOURCE_KEY,
    )
    retry_report = await rollback_pinned_ptg2_source_snapshot(
        source_key=SOURCE_KEY,
        snapshot_id=TARGET_SNAPSHOT_ID,
        expected_current_snapshot_id=CURRENT_SNAPSHOT_ID,
        rollback_owner_id=TARGET_OWNER_ID,
    )
    assert retry_report["status"] == "already_rolled_back"
    assert await db.scalar(
        f"""
        SELECT updated_at
          FROM "{SCHEMA_NAME}".ptg2_current_source_snapshot
         WHERE source_key = :source_key
        """,
        source_key=SOURCE_KEY,
    ) == source_updated_at


async def _roll_forward_and_verify_metadata(
    metadata_before: list[tuple],
) -> None:
    """Reverse A to B and prove immutable snapshot rows did not change."""

    await rollback_pinned_ptg2_source_snapshot(
        source_key=SOURCE_KEY,
        snapshot_id=CURRENT_SNAPSHOT_ID,
        expected_current_snapshot_id=TARGET_SNAPSHOT_ID,
        rollback_owner_id=CURRENT_OWNER_ID,
    )
    assert await _pointer_pair("ptg2_current_source_snapshot") == (
        CURRENT_SNAPSHOT_ID,
        TARGET_SNAPSHOT_ID,
    )
    assert await _snapshot_metadata() == metadata_before


async def _verify_gc_retains_both_bindings() -> None:
    """Run the collector and require both pinned snapshot bindings retained."""

    await sweep_ptg2_shared_blocks(
        schema_name=SCHEMA_NAME,
        max_bytes=1,
        max_rows=1,
    )
    binding_count = await db.scalar(
        f"""
        SELECT COUNT(*)
          FROM "{SCHEMA_NAME}".ptg2_v3_snapshot_binding
         WHERE snapshot_id IN (:snapshot_a, :snapshot_b)
        """,
        snapshot_a=TARGET_SNAPSHOT_ID,
        snapshot_b=CURRENT_SNAPSHOT_ID,
    )
    assert int(binding_count or 0) == 2


@pytest.mark.asyncio
async def test_pinned_rollback_is_atomic_idempotent_and_reversible(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Prove rollback, retry, reversal, metadata, serving, and GC in PostgreSQL."""

    dsn = os.getenv(OPT_IN_DSN_ENV)
    if not dsn:
        pytest.skip(f"set {OPT_IN_DSN_ENV} to a disposable PostgreSQL DSN")
    database_name = _configure_database(monkeypatch, dsn)
    await db.disconnect()
    await db.connect()
    try:
        await _require_empty_database(database_name)
        await seed_rollback_pair()
        metadata_before = await _snapshot_metadata()
        await _verify_incomplete_target_relations_rejected(metadata_before)
        await _rollback_and_verify_resolution()
        await _verify_write_free_retry()
        await _roll_forward_and_verify_metadata(metadata_before)
        await _verify_gc_retains_both_bindings()
    finally:
        await _cleanup()
        await db.disconnect()
