# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Isolated PostgreSQL proof for rolling attestation-contract compatibility."""

from __future__ import annotations

import hashlib
import json
import os
import uuid
from unittest.mock import AsyncMock

import pytest

from api import ptg2_snapshot
from db.connection import db
from process.ptg_parts import ptg2_candidate_attestation
from process.ptg_parts.ptg2_provider_quarantine import (
    provider_identifier_quarantine_payload,
)


def _quoted(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


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
    report = {
        "source": {
            "provider_identifier_quarantine": quarantine,
            "witness": {"payload_sha256": source_witness_digest.hex()},
        }
    }
    report_digest = hashlib.sha256(
        ptg2_candidate_attestation._canonical_report_bytes(report)
    ).digest()
    monkeypatch.setattr(ptg2_snapshot, "PTG2_SCHEMA", schema_name)
    monkeypatch.setattr(
        ptg2_candidate_attestation,
        "_locked_candidate_identity",
        AsyncMock(
            return_value={
                "snapshot_key": 17,
                "source_key": "source-a",
                "plan_id": "12-3456789",
                "plan_market_type": "group",
                "coverage_scope_id": coverage_scope_id,
                "source_set_digest": source_set_digest,
                "audit_sample_digest": audit_sample_digest,
                "source_witness_digest": source_witness_digest,
                "provider_identifier_quarantine": quarantine,
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
            report_json=json.dumps(report),
        )

        async def assert_supported(contract: str) -> None:
            await db.status(
                f"""
                UPDATE {quoted_schema}.ptg2_v3_candidate_audit_attestation
                   SET contract = :contract, activated_at = NULL
                 WHERE snapshot_id = :snapshot_id
                """,
                snapshot_id=snapshot_id,
                contract=contract,
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
            assert observed_digest == report_digest
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
