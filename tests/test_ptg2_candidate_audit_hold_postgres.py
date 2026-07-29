# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""PostgreSQL proofs for durable PTG candidate audit-only holds."""

from __future__ import annotations

import asyncio
import datetime
import importlib.util
import os
from pathlib import Path
import uuid
from unittest.mock import AsyncMock

from alembic.migration import MigrationContext
from alembic.operations import Operations
import pytest

from db.connection import db
from process.ptg_parts import ptg2_candidate_attestation
from tests.ptg2_attestation_compat_test_support import (
    create_writer_attestation_table,
    quoted_identifier,
    writer_evidence_by_field,
    writer_identity_by_field,
    writer_report_by_field,
)


def _require_postgres() -> None:
    if os.getenv("HLTHPRT_PTG2_ATTESTATION_COMPAT_POSTGRES_TEST") != "1":
        pytest.skip(
            "set HLTHPRT_PTG2_ATTESTATION_COMPAT_POSTGRES_TEST=1 for the "
            "isolated PostgreSQL test"
        )


async def _record_held_attestation() -> dict[str, object]:
    return await ptg2_candidate_attestation.record_candidate_audit_attestation(
        snapshot_id="writer-snapshot",
        source_key="source-a",
        plan_id="12-3456789",
        plan_market_type="group",
        report=writer_report_by_field(4),
        activation_intent="audit_only",
    )


async def _assert_held_row(
    quoted_schema: str,
    attestation_digest: str,
) -> None:
    attestation_row = await db.first(
        f"""SELECT COUNT(*) OVER(), activation_intent,
                   activated_at, attestation_digest
              FROM {quoted_schema}.ptg2_v3_candidate_audit_attestation
             WHERE snapshot_id = 'writer-snapshot'
             LIMIT 1"""
    )
    assert attestation_row[0] == 1
    assert attestation_row[1] == "audit_only"
    assert attestation_row[2] is None
    assert bytes(attestation_row[3]).hex() == attestation_digest


async def _assert_held_activation_rejected(
    schema_name: str,
    report_digest: str,
) -> None:
    async with db.transaction() as session:
        with pytest.raises(ValueError, match="held for audit-only review"):
            await ptg2_candidate_attestation.verify_candidate_audit_attestation_in_transaction(
                session,
                schema_name=schema_name,
                snapshot_id="writer-snapshot",
                snapshot_key=17,
                source_key="source-a",
                plan_id="12-3456789",
                plan_market_type="group",
                coverage_scope_id=b"c" * 32,
            )
    async with db.transaction() as session:
        with pytest.raises(RuntimeError, match="changed during activation"):
            await ptg2_candidate_attestation.consume_candidate_audit_attestation_in_transaction(
                session,
                schema_name=schema_name,
                snapshot_id="writer-snapshot",
                report_digest=bytes.fromhex(report_digest),
                activated_at=datetime.datetime.now(datetime.timezone.utc),
            )


async def _activate_reviewed_hold(
    schema_name: str,
    report_digest: str,
    attestation_digest: str,
) -> None:
    approval_digest = bytes.fromhex(attestation_digest)
    async with db.transaction() as session:
        with pytest.raises(
            ptg2_candidate_attestation.CandidateAttestationApprovalConflict,
            match="does not match",
        ):
            await ptg2_candidate_attestation.verify_held_candidate_attestation_in_transaction(
                session,
                schema_name=schema_name,
                snapshot_id="writer-snapshot",
                expected_identity_by_field=writer_identity_by_field(),
                expected_attestation_digest=b"x" * 32,
            )
    async with db.transaction() as session:
        verified_report_digest = (
            await ptg2_candidate_attestation.verify_held_candidate_attestation_in_transaction(
                session,
                schema_name=schema_name,
                snapshot_id="writer-snapshot",
                expected_identity_by_field=writer_identity_by_field(),
                expected_attestation_digest=approval_digest,
            )
        )
        assert verified_report_digest == bytes.fromhex(report_digest)
        await ptg2_candidate_attestation.consume_candidate_audit_attestation_in_transaction(
            session,
            schema_name=schema_name,
            snapshot_id="writer-snapshot",
            report_digest=verified_report_digest,
            activated_at=datetime.datetime.now(datetime.timezone.utc),
            activation_intent="audit_only",
            expected_attestation_digest=approval_digest,
        )


async def _assert_reviewed_hold_consumed(
    schema_name: str,
    quoted_schema: str,
    attestation_digest: str,
) -> None:
    assert await db.scalar(
        f"""SELECT COUNT(*)
              FROM {quoted_schema}.ptg2_v3_candidate_audit_attestation
             WHERE snapshot_id = 'writer-snapshot'
               AND activation_intent = 'audit_only'
               AND activated_at IS NOT NULL"""
    ) == 1
    async with db.transaction() as session:
        with pytest.raises(
            ptg2_candidate_attestation.CandidateAttestationApprovalConflict,
            match="already consumed",
        ):
            await ptg2_candidate_attestation.verify_held_candidate_attestation_in_transaction(
                session,
                schema_name=schema_name,
                snapshot_id="writer-snapshot",
                expected_identity_by_field=writer_identity_by_field(),
                expected_attestation_digest=bytes.fromhex(
                    attestation_digest
                ),
            )


@pytest.mark.asyncio
async def test_real_postgres_held_attestation_requires_reviewed_activation(
    monkeypatch,
):
    """Replay stays held until the exact reviewed digest consumes it once."""

    _require_postgres()
    schema_name = f"ptg2_attestation_hold_{uuid.uuid4().hex[:16]}"
    quoted_schema = quoted_identifier(schema_name)
    completed_at = datetime.datetime.now(datetime.timezone.utc)
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema_name)
    monkeypatch.setattr(
        ptg2_candidate_attestation,
        "_locked_candidate_identity",
        AsyncMock(return_value=writer_identity_by_field()),
    )
    monkeypatch.setattr(
        ptg2_candidate_attestation,
        "validate_candidate_release_audit_report",
        lambda report, **_kwargs: writer_evidence_by_field(
            report,
            completed_at=completed_at,
        ),
    )
    await db.disconnect()
    await db.connect()
    try:
        await db.execute_ddl(f"CREATE SCHEMA {quoted_schema}")
        await create_writer_attestation_table(quoted_schema)
        first_result, repeated_result = await asyncio.gather(
            _record_held_attestation(),
            _record_held_attestation(),
        )
        assert first_result == repeated_result
        await _assert_held_row(
            quoted_schema,
            str(first_result["attestation_digest"]),
        )
        await _assert_held_activation_rejected(
            schema_name,
            str(first_result["report_digest"]),
        )
        await _activate_reviewed_hold(
            schema_name,
            str(first_result["report_digest"]),
            str(first_result["attestation_digest"]),
        )
        await _assert_reviewed_hold_consumed(
            schema_name,
            quoted_schema,
            str(first_result["attestation_digest"]),
        )
    finally:
        try:
            await db.execute_ddl(
                f"DROP SCHEMA IF EXISTS {quoted_schema} CASCADE"
            )
        finally:
            await db.disconnect()


def _load_candidate_hold_migration():
    migration_path = (
        Path(__file__).resolve().parents[1]
        / "alembic"
        / "versions"
        / "20260729100000_ptg2_candidate_audit_hold.py"
    )
    spec = importlib.util.spec_from_file_location(
        "candidate_audit_hold_migration",
        migration_path,
    )
    assert spec is not None and spec.loader is not None
    migration = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(migration)
    return migration


async def _run_hold_migration(migration, operation: str) -> None:
    assert db.engine is not None
    async with db.engine.begin() as connection:
        await connection.run_sync(
            lambda sync_connection: (
                setattr(
                    migration,
                    "op",
                    Operations(MigrationContext.configure(sync_connection)),
                ),
                getattr(migration, operation)(),
            )
        )


async def _create_legacy_attestation_table(
    quoted_schema: str,
    report_digest: bytes,
) -> None:
    await db.execute_ddl(
        f"""CREATE TABLE {quoted_schema}.ptg2_v3_candidate_audit_attestation (
            snapshot_id text PRIMARY KEY,
            report_digest bytea NOT NULL,
            activated_at timestamptz
        )"""
    )
    await db.status(
        f"""INSERT INTO {quoted_schema}.ptg2_v3_candidate_audit_attestation
                (snapshot_id, report_digest, activated_at)
            VALUES ('legacy-snapshot', :report_digest, NULL)""",
        report_digest=report_digest,
    )


async def _set_attestation_intent(
    quoted_schema: str,
    report_digest: bytes,
    activation_intent: str,
) -> None:
    await db.status(
        f"""UPDATE {quoted_schema}.ptg2_v3_candidate_audit_attestation
               SET activation_intent = :activation_intent,
                   attestation_digest = :attestation_digest
             WHERE snapshot_id = 'legacy-snapshot'""",
        activation_intent=activation_intent,
        attestation_digest=(
            ptg2_candidate_attestation.candidate_attestation_digest(
                report_digest,
                activation_intent,
            )
        ),
    )


async def _assert_migration_backfill(
    quoted_schema: str,
    report_digest: bytes,
) -> None:
    attestation_row = await db.first(
        f"""SELECT activation_intent, attestation_digest
              FROM {quoted_schema}.ptg2_v3_candidate_audit_attestation
             WHERE snapshot_id = 'legacy-snapshot'"""
    )
    assert attestation_row[0] == "audit_and_activate"
    assert bytes(attestation_row[1]) == (
        ptg2_candidate_attestation.candidate_attestation_digest(
            report_digest,
            "audit_and_activate",
        )
    )


@pytest.mark.asyncio
async def test_real_postgres_candidate_hold_migration_backfill_and_rollback(
    monkeypatch,
):
    """Backfill legacy evidence and refuse downgrade while a hold exists."""

    _require_postgres()
    schema_name = f"ptg2_attestation_migration_{uuid.uuid4().hex[:16]}"
    quoted_schema = quoted_identifier(schema_name)
    migration = _load_candidate_hold_migration()
    report_digest = b"r" * 32
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema_name)
    await db.disconnect()
    await db.connect()
    try:
        await db.execute_ddl(f"CREATE SCHEMA {quoted_schema}")
        await _create_legacy_attestation_table(
            quoted_schema,
            report_digest,
        )
        await _run_hold_migration(migration, "upgrade")
        await _assert_migration_backfill(quoted_schema, report_digest)
        await _set_attestation_intent(
            quoted_schema,
            report_digest,
            "audit_only",
        )
        with pytest.raises(RuntimeError, match="held attestations exist"):
            await _run_hold_migration(migration, "downgrade")
        await db.status(
            f"""UPDATE {quoted_schema}.ptg2_v3_candidate_audit_attestation
                   SET activated_at = :activated_at
                 WHERE snapshot_id = 'legacy-snapshot'""",
            activated_at=datetime.datetime.now(datetime.timezone.utc),
        )
        with pytest.raises(RuntimeError, match="held attestations exist"):
            await _run_hold_migration(migration, "downgrade")
        await _set_attestation_intent(
            quoted_schema,
            report_digest,
            "audit_and_activate",
        )
        await _run_hold_migration(migration, "downgrade")
        assert await db.scalar(
            f"""SELECT COUNT(*)
                  FROM {quoted_schema}.ptg2_v3_candidate_audit_attestation
                 WHERE snapshot_id = 'legacy-snapshot'"""
        ) == 1
    finally:
        try:
            await db.execute_ddl(
                f"DROP SCHEMA IF EXISTS {quoted_schema} CASCADE"
            )
        finally:
            await db.disconnect()
