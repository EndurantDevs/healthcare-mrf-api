# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Isolated PostgreSQL proof for rolling attestation-contract compatibility."""

from __future__ import annotations

import datetime
import hashlib
import json
import os
import uuid
from unittest.mock import AsyncMock, patch

import pytest

from api import ptg2_snapshot
from db.connection import db
from process.ptg_parts import ptg2_candidate_attestation
from process.ptg_parts.ptg2_provider_quarantine import (
    provider_identifier_quarantine_payload,
)
from tests.ptg2_attestation_compat_test_support import (
    quoted_identifier as _quoted,
    writer_evidence_by_field as _writer_evidence,
    writer_identity_by_field as _writer_identity,
    writer_report_by_field as _writer_report,
)


async def _create_writer_attestation_table(quoted_schema: str) -> None:
    await db.execute_ddl(
        f"""CREATE TABLE {quoted_schema}.ptg2_v3_candidate_audit_attestation (
            snapshot_id text PRIMARY KEY,
            snapshot_key bigint NOT NULL,
            source_key text NOT NULL,
            plan_id text NOT NULL,
            plan_market_type text NOT NULL,
            coverage_scope_id bytea NOT NULL,
            source_set_digest bytea NOT NULL,
            audit_sample_digest bytea NOT NULL,
            source_witness_digest bytea NOT NULL,
            contract text NOT NULL,
            tool_name text NOT NULL,
            tool_version text NOT NULL,
            report_digest bytea NOT NULL,
            report jsonb NOT NULL,
            attested_at timestamptz NOT NULL,
            expires_at timestamptz NOT NULL,
            activated_at timestamptz
        )"""
    )


async def _seed_writer_v3_attestation(quoted_schema: str) -> None:
    identity_by_field = _writer_identity()
    report_by_field = _writer_report(3)
    report_bytes = ptg2_candidate_attestation._canonical_report_bytes(
        report_by_field
    )
    await db.status(
        f"""INSERT INTO {quoted_schema}.ptg2_v3_candidate_audit_attestation
            (snapshot_id, snapshot_key, source_key, plan_id,
             plan_market_type, coverage_scope_id, source_set_digest,
             audit_sample_digest, source_witness_digest, contract,
             tool_name, tool_version, report_digest, report, attested_at,
             expires_at, activated_at)
        VALUES
            ('writer-snapshot', 17, 'source-a', '12-3456789', 'group',
             :coverage_scope_id, :source_set_digest, :audit_sample_digest,
             :source_witness_digest, :contract, :tool_name, '3.0.0',
             :report_digest, CAST(:report_json AS jsonb), clock_timestamp(),
             clock_timestamp() + interval '1 hour', NULL)""",
        coverage_scope_id=identity_by_field["coverage_scope_id"],
        source_set_digest=identity_by_field["source_set_digest"],
        audit_sample_digest=identity_by_field["audit_sample_digest"],
        source_witness_digest=identity_by_field["source_witness_digest"],
        contract=(
            ptg2_candidate_attestation.PTG2_CANDIDATE_ATTESTATION_CONTRACT_V3
        ),
        tool_name=ptg2_candidate_attestation.PTG2_FAST_AUDIT_TOOL,
        report_digest=hashlib.sha256(report_bytes).digest(),
        report_json=report_bytes.decode("utf-8"),
    )


async def _writer_attestation_row(quoted_schema: str):
    rows = await db.all(
        f"""SELECT contract, tool_name, tool_version, report, activated_at
              FROM {quoted_schema}.ptg2_v3_candidate_audit_attestation
             WHERE snapshot_id = 'writer-snapshot'"""
    )
    assert len(rows) == 1
    return rows[0]


async def _record_writer_report(schema_version: int) -> dict[str, object]:
    writer_contract = (
        ptg2_candidate_attestation.PTG2_CANDIDATE_ATTESTATION_CONTRACT_V4
        if schema_version == 4
        else ptg2_candidate_attestation.PTG2_CANDIDATE_ATTESTATION_CONTRACT_V3
    )
    with patch.object(
        ptg2_candidate_attestation,
        "PTG2_CANDIDATE_ATTESTATION_CURRENT_CONTRACT",
        writer_contract,
    ):
        return await (
            ptg2_candidate_attestation.record_candidate_audit_attestation(
                snapshot_id="writer-snapshot",
                source_key="source-a",
                plan_id="12-3456789",
                plan_market_type="group",
                report=_writer_report(schema_version),
            )
        )


async def _assert_writer_upgrade_and_guards(
    schema_name: str,
    quoted_schema: str,
) -> None:
    v4_result = await _record_writer_report(4)
    upgraded_row = await _writer_attestation_row(quoted_schema)
    assert v4_result["contract"] == (
        ptg2_candidate_attestation.PTG2_CANDIDATE_ATTESTATION_CONTRACT_V4
    )
    assert upgraded_row[0] == v4_result["contract"]
    assert upgraded_row[1] == ptg2_candidate_attestation.PTG2_BATCH_AUDIT_TOOL
    assert upgraded_row[2] == "4.0.0"
    assert upgraded_row[3]["schema_version"] == 4
    assert upgraded_row[4] is None

    with pytest.raises(ValueError, match="conflicts with existing evidence"):
        await _record_writer_report(3)
    assert await _writer_attestation_row(quoted_schema) == upgraded_row

    async with db.transaction() as session:
        report_digest = await (
            ptg2_candidate_attestation.verify_candidate_audit_attestation_in_transaction(
                session,
                schema_name=schema_name,
                snapshot_id="writer-snapshot",
                snapshot_key=17,
                source_key="source-a",
                plan_id="12-3456789",
                plan_market_type="group",
                coverage_scope_id=b"c" * 32,
            )
        )
        await ptg2_candidate_attestation.consume_candidate_audit_attestation_in_transaction(
            session,
            schema_name=schema_name,
            snapshot_id="writer-snapshot",
            report_digest=report_digest,
            activated_at=datetime.datetime.now(datetime.timezone.utc),
        )
    activated_row = await _writer_attestation_row(quoted_schema)
    assert activated_row[4] is not None
    for schema_version in (4, 3):
        with pytest.raises(ValueError, match="conflicts with existing evidence"):
            await _record_writer_report(schema_version)
        assert await _writer_attestation_row(quoted_schema) == activated_row


@pytest.mark.asyncio
async def test_real_postgres_writer_upgrade_is_monotonic_and_activation_safe(
    monkeypatch,
):
    """Prove the real PostgreSQL upsert permits only unactivated V3 to V4."""

    if os.getenv("HLTHPRT_PTG2_ATTESTATION_COMPAT_POSTGRES_TEST") != "1":
        pytest.skip(
            "set HLTHPRT_PTG2_ATTESTATION_COMPAT_POSTGRES_TEST=1 for the "
            "isolated PostgreSQL test"
        )
    schema_name = f"ptg2_attestation_writer_{uuid.uuid4().hex[:16]}"
    quoted_schema = _quoted(schema_name)
    completed_at = datetime.datetime.now(datetime.timezone.utc)
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema_name)
    monkeypatch.setattr(
        ptg2_candidate_attestation,
        "_locked_candidate_identity",
        AsyncMock(return_value=_writer_identity()),
    )
    monkeypatch.setattr(
        ptg2_candidate_attestation,
        "validate_candidate_release_audit_report",
        lambda report, **_kwargs: _writer_evidence(
            report,
            completed_at=completed_at,
        ),
    )

    await db.disconnect()
    await db.connect()
    try:
        await db.execute_ddl(f"CREATE SCHEMA {quoted_schema}")
        await _create_writer_attestation_table(quoted_schema)
        await _seed_writer_v3_attestation(quoted_schema)
        await _assert_writer_upgrade_and_guards(schema_name, quoted_schema)
    finally:
        try:
            await db.execute_ddl(f"DROP SCHEMA IF EXISTS {quoted_schema} CASCADE")
        finally:
            await db.disconnect()


@pytest.mark.asyncio
async def test_real_postgres_published_snapshot_accepts_v3_and_v4_attestations(
    monkeypatch,
):
    """Prove both rolling-deploy contracts resolve through real PostgreSQL readers."""
    if os.getenv("HLTHPRT_PTG2_ATTESTATION_COMPAT_POSTGRES_TEST") != "1":
        pytest.skip(
            "set HLTHPRT_PTG2_ATTESTATION_COMPAT_POSTGRES_TEST=1 for the "
            "isolated PostgreSQL test"
        )

    schema_name = f"ptg2_attestation_compat_{uuid.uuid4().hex[:16]}"
    quoted_schema = _quoted(schema_name)
    snapshot_id = "compat-snapshot"
    coverage_scope_id = b"\xcc" * 32
    source_set_digest = b"s" * 32
    audit_sample_digest = b"a" * 32
    source_witness_digest = b"w" * 32
    quarantine = provider_identifier_quarantine_payload({})
    report_by_field = {
        "source": {
            "provider_identifier_quarantine": quarantine,
            "witness": {"payload_sha256": source_witness_digest.hex()},
        }
    }
    report_digest = hashlib.sha256(
        ptg2_candidate_attestation._canonical_report_bytes(report_by_field)
    ).digest()
    monkeypatch.setattr(ptg2_snapshot, "PTG2_SCHEMA", schema_name)
    monkeypatch.setattr(
        ptg2_candidate_attestation,
        "_locked_candidate_identity",
        AsyncMock(
            return_value={
                **_writer_identity(),
                "coverage_scope_id": coverage_scope_id,
            }
        ),
    )

    await db.disconnect()
    await db.connect()
    try:
        await db.execute_ddl(f"CREATE SCHEMA {quoted_schema}")
        table_definitions = (
            f"""CREATE TABLE {quoted_schema}.ptg2_snapshot (
                snapshot_id text PRIMARY KEY,
                status text NOT NULL
            )""",
            f"""CREATE TABLE {quoted_schema}.ptg2_v3_snapshot_binding (
                snapshot_id text PRIMARY KEY,
                snapshot_key bigint NOT NULL
            )""",
            f"""CREATE TABLE {quoted_schema}.ptg2_v3_snapshot_layout (
                snapshot_key bigint PRIMARY KEY,
                state text NOT NULL,
                generation text NOT NULL
            )""",
            f"""CREATE TABLE {quoted_schema}.ptg2_v3_snapshot_scope (
                snapshot_id text PRIMARY KEY,
                coverage_scope_id bytea NOT NULL,
                plan_id text NOT NULL,
                plan_market_type text NOT NULL
            )""",
            f"""CREATE TABLE {quoted_schema}.ptg2_v3_candidate_audit_attestation (
                snapshot_id text PRIMARY KEY,
                snapshot_key bigint NOT NULL,
                coverage_scope_id bytea NOT NULL,
                source_set_digest bytea NOT NULL,
                audit_sample_digest bytea NOT NULL,
                source_witness_digest bytea NOT NULL,
                plan_id text NOT NULL,
                plan_market_type text NOT NULL,
                source_key text NOT NULL,
                contract text NOT NULL,
                report_digest bytea NOT NULL,
                report jsonb NOT NULL,
                expires_at timestamptz NOT NULL,
                activated_at timestamptz
            )""",
        )
        for table_definition in table_definitions:
            await db.execute_ddl(table_definition)

        await db.status(
            f"""
            INSERT INTO {quoted_schema}.ptg2_snapshot
                (snapshot_id, status)
            VALUES (:snapshot_id, 'published')
            """,
            snapshot_id=snapshot_id,
        )
        await db.status(
            f"""
            INSERT INTO {quoted_schema}.ptg2_v3_snapshot_binding
                (snapshot_id, snapshot_key)
            VALUES (:snapshot_id, 17)
            """,
            snapshot_id=snapshot_id,
        )
        await db.status(
            f"""
            INSERT INTO {quoted_schema}.ptg2_v3_snapshot_layout
                (snapshot_key, state, generation)
            VALUES (17, 'sealed', 'shared_blocks_v3')
            """
        )
        await db.status(
            f"""
            INSERT INTO {quoted_schema}.ptg2_v3_snapshot_scope
                (snapshot_id, coverage_scope_id, plan_id, plan_market_type)
            VALUES (:snapshot_id, decode(repeat('cc', 32), 'hex'),
                    '12-3456789', 'group')
            """,
            snapshot_id=snapshot_id,
        )
        await db.status(
            f"""
            INSERT INTO {quoted_schema}.ptg2_v3_candidate_audit_attestation
                (snapshot_id, snapshot_key, coverage_scope_id,
                 source_set_digest, audit_sample_digest,
                 source_witness_digest, plan_id, plan_market_type,
                 source_key, contract, report_digest, report,
                 expires_at, activated_at)
            VALUES (:snapshot_id, 17, :coverage_scope_id,
                    :source_set_digest, :audit_sample_digest,
                    :source_witness_digest, '12-3456789', 'group',
                    'source-a', :contract, :report_digest,
                    CAST(:report_json AS jsonb),
                    clock_timestamp() + interval '1 hour', NULL)
            """,
            snapshot_id=snapshot_id,
            coverage_scope_id=coverage_scope_id,
            source_set_digest=source_set_digest,
            audit_sample_digest=audit_sample_digest,
            source_witness_digest=source_witness_digest,
            contract=(
                ptg2_candidate_attestation.PTG2_CANDIDATE_ATTESTATION_CONTRACT_V3
            ),
            report_digest=report_digest,
            report_json=json.dumps(report_by_field),
        )

        async def assert_supported(contract: str) -> None:
            supported_report = (
                _writer_report(4)
                if contract
                == ptg2_candidate_attestation.PTG2_CANDIDATE_ATTESTATION_CONTRACT_V4
                else report_by_field
            )
            supported_digest = hashlib.sha256(
                ptg2_candidate_attestation._canonical_report_bytes(
                    supported_report
                )
            ).digest()
            await db.status(
                f"""
                UPDATE {quoted_schema}.ptg2_v3_candidate_audit_attestation
                   SET contract = :contract,
                       report_digest = :report_digest,
                       report = CAST(:report_json AS jsonb),
                       activated_at = NULL
                 WHERE snapshot_id = :snapshot_id
                """,
                snapshot_id=snapshot_id,
                contract=contract,
                report_digest=supported_digest,
                report_json=json.dumps(supported_report),
            )
            async with db.transaction() as session:
                observed_digest = await ptg2_candidate_attestation.verify_candidate_audit_attestation_in_transaction(
                    session,
                    schema_name=schema_name,
                    snapshot_id=snapshot_id,
                    snapshot_key=17,
                    source_key="source-a",
                    plan_id="12-3456789",
                    plan_market_type="group",
                    coverage_scope_id=coverage_scope_id,
                )
            assert observed_digest == supported_digest
            await db.status(
                f"""
                UPDATE {quoted_schema}.ptg2_v3_candidate_audit_attestation
                   SET activated_at = clock_timestamp()
                 WHERE snapshot_id = :snapshot_id
                """,
                snapshot_id=snapshot_id,
            )
            async with db.transaction() as session:
                assert await ptg2_snapshot.current_snapshot_id(
                    session,
                    requested_snapshot_id=snapshot_id,
                    requested_source_key="source-a",
                ) == snapshot_id

        await assert_supported(
            ptg2_candidate_attestation.PTG2_CANDIDATE_ATTESTATION_CONTRACT_V3
        )
        await assert_supported(
            ptg2_candidate_attestation.PTG2_CANDIDATE_ATTESTATION_CONTRACT_V4
        )

        await db.status(
            f"""
            UPDATE {quoted_schema}.ptg2_v3_candidate_audit_attestation
               SET contract = 'unsupported-contract', activated_at = NULL
             WHERE snapshot_id = :snapshot_id
            """,
            snapshot_id=snapshot_id,
        )
        async with db.transaction() as session:
            with pytest.raises(ValueError, match="no current passing"):
                await ptg2_candidate_attestation.verify_candidate_audit_attestation_in_transaction(
                    session,
                    schema_name=schema_name,
                    snapshot_id=snapshot_id,
                    snapshot_key=17,
                    source_key="source-a",
                    plan_id="12-3456789",
                    plan_market_type="group",
                    coverage_scope_id=coverage_scope_id,
                )
        await db.status(
            f"""
            UPDATE {quoted_schema}.ptg2_v3_candidate_audit_attestation
               SET activated_at = clock_timestamp()
             WHERE snapshot_id = :snapshot_id
            """,
            snapshot_id=snapshot_id,
        )
        async with db.transaction() as session:
            assert await ptg2_snapshot.current_snapshot_id(
                session,
                requested_snapshot_id=snapshot_id,
                requested_source_key="source-a",
            ) is None
    finally:
        try:
            await db.execute_ddl(f"DROP SCHEMA IF EXISTS {quoted_schema} CASCADE")
        finally:
            await db.disconnect()
