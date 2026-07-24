# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Real PostgreSQL proof for sealed V4 candidate attestation."""

from __future__ import annotations

import datetime
import json
import os
import uuid
from unittest.mock import Mock

import pytest

from db.connection import db
from process.ptg_parts import ptg2_candidate_attestation, source_pointers
from process.ptg_parts.ptg2_shared_source_set import (
    ordered_source_ordinal_digest,
    shared_source_set_metadata,
)
from tests.ptg2_attestation_compat_test_support import (
    create_writer_attestation_table,
    quoted_identifier,
    writer_audit_sample_by_field,
    writer_evidence_by_field,
    writer_identity_by_field,
    writer_report_by_field,
    writer_source_witness_by_field,
)


RAW_CONTAINER_DIGEST = b"\x11" * 32
MAP_DIGEST = b"\x22" * 32


def _v4_identity_and_manifests():
    source_set_by_field = shared_source_set_metadata(
        (RAW_CONTAINER_DIGEST.hex(),)
    )
    identity_by_field = writer_identity_by_field()
    identity_by_field.update(
        {
            "storage_generation": "shared_blocks_v4",
            "source_set_digest": bytes.fromhex(
                source_set_by_field["raw_container_sha256_digest"]
            ),
            "ordered_source_ordinal_digest": ordered_source_ordinal_digest(
                (RAW_CONTAINER_DIGEST.hex(),)
            ),
        }
    )
    identity_by_field["source_witness_manifest"] = (
        writer_source_witness_by_field(identity_by_field)
    )
    identity_by_field["audit_sample_public"] = (
        writer_audit_sample_by_field(identity_by_field)
    )
    common_serving_index = {
        "arch_version": "postgres_binary_v3",
        "type": "ptg2_shared_blocks_v4",
        "storage_generation": "shared_blocks_v4",
        "provider_scope_strategy": "postgres_packed_graph_v4",
        "shared_block_layout": "packed_snapshot_maps_v4",
        "shared_snapshot_key": 17,
        "snapshot_map": {"map_digest": MAP_DIGEST.hex()},
        "coverage_scope_id": identity_by_field["coverage_scope_id"].hex(),
        "source_witness": identity_by_field["source_witness_manifest"],
        "audit_sample": identity_by_field["audit_sample_public"],
        "provider_identifier_quarantine": identity_by_field[
            "provider_identifier_quarantine"
        ],
    }
    snapshot_manifest_by_field = {
        "activation": {
            "contract": "ptg2_candidate_activation_v1",
            "state": "validated",
            "source_key": "source-a",
        },
        "serving_index": {
            **common_serving_index,
            "source_set": source_set_by_field,
        },
    }
    layout_manifest_by_field = {
        "serving_index": {
            **common_serving_index,
            "source_count": 1,
        }
    }
    return (
        identity_by_field,
        snapshot_manifest_by_field,
        layout_manifest_by_field,
    )


async def _create_v4_candidate_tables(quoted_schema: str) -> None:
    table_definitions = (
        f"""CREATE TABLE {quoted_schema}.ptg2_snapshot (
            snapshot_id text PRIMARY KEY,
            import_run_id text NOT NULL DEFAULT 'run-1',
            import_month date NOT NULL DEFAULT DATE '2026-07-01',
            status text NOT NULL,
            created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
            validated_at timestamptz,
            published_at timestamptz,
            previous_snapshot_id text,
            manifest jsonb NOT NULL)""",
        f"""CREATE TABLE {quoted_schema}.ptg2_v3_snapshot_binding (
            snapshot_id text PRIMARY KEY, snapshot_key bigint NOT NULL)""",
        f"""CREATE TABLE {quoted_schema}.ptg2_v3_snapshot_scope (
            snapshot_id text PRIMARY KEY, plan_id text NOT NULL,
            plan_market_type text NOT NULL, coverage_scope_id bytea NOT NULL)""",
        f"""CREATE TABLE {quoted_schema}.ptg2_v3_snapshot_layout (
            snapshot_key bigint PRIMARY KEY, state text NOT NULL,
            generation text NOT NULL, mapping_digest bytea,
            layout_manifest jsonb NOT NULL)""",
        f"""CREATE TABLE {quoted_schema}.ptg2_v4_snapshot_map_root (
            snapshot_key bigint PRIMARY KEY, state text NOT NULL,
            map_digest bytea)""",
        f"""CREATE TABLE {quoted_schema}.ptg2_v3_snapshot_source (
            snapshot_id text NOT NULL, source_key text NOT NULL,
            raw_container_sha256 bytea NOT NULL)""",
    )
    for table_definition in table_definitions:
        await db.execute_ddl(table_definition)
    await create_writer_attestation_table(quoted_schema)


async def _seed_v4_candidate(
    quoted_schema: str,
    snapshot_manifest_by_field: dict[str, object],
    layout_manifest_by_field: dict[str, object],
) -> None:
    await db.status(
        f"""INSERT INTO {quoted_schema}.ptg2_snapshot
            (snapshot_id, status, manifest)
            VALUES ('candidate-v4', 'validated', CAST(:manifest AS jsonb))""",
        manifest=json.dumps(snapshot_manifest_by_field),
    )
    await db.status(
        f"""INSERT INTO {quoted_schema}.ptg2_v3_snapshot_binding
            VALUES ('candidate-v4', 17)"""
    )
    await db.status(
        f"""INSERT INTO {quoted_schema}.ptg2_v3_snapshot_scope
            VALUES ('candidate-v4', '12-3456789', 'group', :coverage_scope_id)""",
        coverage_scope_id=b"c" * 32,
    )
    await db.status(
        f"""INSERT INTO {quoted_schema}.ptg2_v3_snapshot_layout
            VALUES (17, 'sealed', 'shared_blocks_v4', :map_digest,
                    CAST(:layout_manifest AS jsonb))""",
        map_digest=MAP_DIGEST,
        layout_manifest=json.dumps(layout_manifest_by_field),
    )
    await db.status(
        f"""INSERT INTO {quoted_schema}.ptg2_v4_snapshot_map_root
            VALUES (17, 'complete', :map_digest)""",
        map_digest=MAP_DIGEST,
    )
    await db.status(
        f"""INSERT INTO {quoted_schema}.ptg2_v3_snapshot_source
            VALUES ('candidate-v4', 'source-a', :raw_digest)""",
        raw_digest=RAW_CONTAINER_DIGEST,
    )


async def _locked_v4_activation_generation(schema_name: str) -> str:
    async with db.transaction() as session:
        activation_row = await source_pointers._locked_candidate_activation_row(
            session,
            schema_name=schema_name,
            snapshot_id="candidate-v4",
        )
    return str(activation_row["storage_generation"])


def _v4_report_and_evidence(identity_by_field):
    report_by_field = writer_report_by_field(4)
    evidence_by_field = writer_evidence_by_field(
        report_by_field,
        completed_at=datetime.datetime.now(datetime.timezone.utc),
    )
    evidence_by_field.update(
        {
            field_name: identity_by_field[field_name]
            for field_name in (
                "storage_generation",
                "source_set_digest",
                "ordered_source_ordinal_digest",
                "source_witness_manifest",
                "audit_sample_public",
            )
        }
    )
    return report_by_field, evidence_by_field


async def _verify_changed_v4_root_rejection(
    quoted_schema: str,
    schema_name: str,
) -> None:
    await db.status(
        f"""UPDATE {quoted_schema}.ptg2_v4_snapshot_map_root
               SET map_digest = :changed_digest
             WHERE snapshot_key = 17""",
        changed_digest=b"x" * 32,
    )
    async with db.transaction() as session:
        with pytest.raises(ValueError, match="candidate is unavailable"):
            await ptg2_candidate_attestation._locked_candidate_identity(
                session,
                schema_name=schema_name,
                snapshot_id="candidate-v4",
            )


@pytest.mark.asyncio
async def test_real_postgres_v4_candidate_attestation_locks_complete_root(
    monkeypatch,
):
    """Attest one exact V4 candidate and reject a changed packed-map root."""

    if os.getenv("HLTHPRT_PTG2_ATTESTATION_COMPAT_POSTGRES_TEST") != "1":
        pytest.skip("enable the isolated PostgreSQL attestation test")
    schema_name = f"ptg2_v4_attestation_{uuid.uuid4().hex[:16]}"
    quoted_schema = quoted_identifier(schema_name)
    identity_by_field, snapshot_manifest_by_field, layout_manifest_by_field = (
        _v4_identity_and_manifests()
    )
    report_by_field, evidence_by_field = _v4_report_and_evidence(
        identity_by_field
    )
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema_name)
    monkeypatch.setattr(
        ptg2_candidate_attestation,
        "validate_candidate_release_audit_report",
        Mock(return_value=evidence_by_field),
    )

    await db.disconnect()
    await db.connect()
    try:
        await db.execute_ddl(f"CREATE SCHEMA {quoted_schema}")
        await _create_v4_candidate_tables(quoted_schema)
        await _seed_v4_candidate(
            quoted_schema,
            snapshot_manifest_by_field,
            layout_manifest_by_field,
        )
        assert await _locked_v4_activation_generation(schema_name) == (
            "shared_blocks_v4"
        )
        attestation_result = await (
            ptg2_candidate_attestation.record_candidate_audit_attestation(
                snapshot_id="candidate-v4",
                source_key="source-a",
                plan_id="12-3456789",
                plan_market_type="group",
                report=report_by_field,
                storage_generation="shared_blocks_v4",
            )
        )
        assert attestation_result["status"] == "attested"
        assert attestation_result["contract"] == (
            ptg2_candidate_attestation.PTG2_CANDIDATE_ATTESTATION_CONTRACT_V4
        )
        await _verify_changed_v4_root_rejection(
            quoted_schema,
            schema_name,
        )
    finally:
        try:
            await db.execute_ddl(
                f"DROP SCHEMA IF EXISTS {quoted_schema} CASCADE"
            )
        finally:
            await db.disconnect()
