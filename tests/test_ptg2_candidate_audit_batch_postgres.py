# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import os
import uuid
from dataclasses import dataclass
from typing import Any, Mapping
from unittest.mock import ANY, AsyncMock, Mock

import pytest

from api import (
    ptg2_candidate_audit_batch as candidate_batch,
    ptg2_candidate_audit_integrity as candidate_integrity,
    ptg2_serving,
)
from api.ptg2_candidate_audit import (
    PTG2CandidateAuditAccess,
)
from api.ptg2_candidate_audit_batch import CandidateAuditBatchResult
from api.ptg2_shared_blocks import shared_block_read_once_scope
from api.ptg2_types import PTG2ServingTables
from db.connection import db
from process.ptg_parts.ptg2_candidate_audit_batch_contract import (
    AuditBatchRequest,
    AuditBatchWitnessBinding,
    build_audit_batch_request,
)
from process.ptg_parts import ptg2_serving_binary_v3
from tests.ptg2_candidate_audit_batch_postgres_fixture import (
    PLAN_ID,
    SNAPSHOT_ID,
    SOURCE_DIGEST,
    SOURCE_KEY,
    audit_rows,
    seed_candidate,
    serving_tables,
    source_witness,
)
from tests.ptg2_candidate_audit_batch_postgres_schema import (
    create_candidate_schema,
    quote_identifier,
)


@dataclass(frozen=True)
class _CandidateBatchCase:
    schema_name: str
    witness_payload: bytes
    witness_metadata: dict[str, Any]
    persisted_audit_rows: list[dict[str, Any]]
    serving_tables: PTG2ServingTables
    audit_request: AuditBatchRequest
    access: PTG2CandidateAuditAccess


def _candidate_batch_case() -> _CandidateBatchCase:
    schema_name = f"ptg2_audit_batch_{uuid.uuid4().hex[:16]}"
    witness_payload, witness_metadata = source_witness()
    persisted_audit_rows = audit_rows()
    candidate_tables = serving_tables(witness_metadata, persisted_audit_rows)
    audit_request = build_audit_batch_request(
        snapshot_id=SNAPSHOT_ID,
        source_key=SOURCE_KEY,
        plan_id=PLAN_ID,
        plan_market_type="group",
        witness_binding=AuditBatchWitnessBinding(
            audit_sample_digest=candidate_tables.audit_sample["sample_digest"],
            source_witness_sample_digest=witness_metadata["sample_digest"],
            source_witness_payload_sha256=witness_metadata["payload_sha256"],
            raw_container_sha256=(SOURCE_DIGEST,),
            source_witness_occurrence_count=2,
        ),
    )
    access = PTG2CandidateAuditAccess(
        snapshot_id=SNAPSHOT_ID,
        source_key=SOURCE_KEY,
        plan_id=PLAN_ID,
        plan_market_type="group",
    )
    return _CandidateBatchCase(
        schema_name,
        witness_payload,
        witness_metadata,
        persisted_audit_rows,
        candidate_tables,
        audit_request,
        access,
    )


def _patch_candidate_modules(
    monkeypatch: pytest.MonkeyPatch,
    candidate_case: _CandidateBatchCase,
) -> AsyncMock:
    snapshot_descriptor = AsyncMock(return_value=candidate_case.serving_tables)
    monkeypatch.setattr(
        candidate_batch,
        "snapshot_serving_tables",
        snapshot_descriptor,
    )
    monkeypatch.setattr(candidate_batch, "PTG2_SCHEMA", candidate_case.schema_name)
    monkeypatch.setattr(candidate_integrity, "PTG2_SCHEMA", candidate_case.schema_name)
    monkeypatch.setattr(ptg2_serving, "PTG2_SCHEMA", candidate_case.schema_name)
    return snapshot_descriptor


async def _run_candidate_batch(
    candidate_case: _CandidateBatchCase,
) -> tuple[CandidateAuditBatchResult, Mapping[str, int]]:
    schema = quote_identifier(candidate_case.schema_name)
    await db.disconnect()
    await db.connect()
    try:
        await create_candidate_schema(candidate_case.schema_name)
        await seed_candidate(
            candidate_case.schema_name,
            candidate_case.witness_payload,
            candidate_case.witness_metadata,
            candidate_case.persisted_audit_rows,
        )
        async with db.session() as session:
            with shared_block_read_once_scope(
                max_retained_raw_bytes=1024 * 1024,
            ) as read_once_scope:
                audit_result = await candidate_batch.audit_candidate_source_witness_batch(
                    session,
                    candidate_case.audit_request,
                    candidate_case.access,
                )
                read_once_scope.assert_processed_once()
                block_io = read_once_scope.ledger
        return audit_result, block_io
    finally:
        try:
            await db.execute_ddl(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
        finally:
            await db.disconnect()


def _assert_witness_ledger(audit_result: CandidateAuditBatchResult) -> None:
    assert audit_result.matched_challenge_count == 2
    assert audit_result.unique_challenge_count == 2
    assert audit_result.persisted_audit_occurrence_count == 2
    assert audit_result.validated_persisted_audit_occurrence_count == 2
    assert audit_result.witness_io == {
        "payload_reads": 1,
        "payload_decodes": 1,
        "record_decodes": 2,
        "unique_evidence_entries": 2,
        "evidence_decompressions": 2,
        "evidence_sha256_hashes": 2,
        "evidence_json_parses": 2,
        "evidence_reuse_deliveries": 2,
        "repeated_evidence_decompressions": 0,
        "repeated_evidence_sha256_hashes": 0,
        "repeated_evidence_json_parses": 0,
    }
    assert audit_result.candidate_processing_io[
        "repeated_candidate_projection_builds"
    ] == 0


def _assert_block_ledger(block_io: Mapping[str, int]) -> None:
    assert block_io == {
        "logical_block_deliveries": 7,
        "physical_mapping_references": 7,
        "physical_mapping_aliases": 2,
        "unique_physical_blocks": 5,
        "physical_block_reads": 5,
        "physical_block_decodes": 5,
        "physical_payload_preparations": 5,
        "expected_logical_payload_processes": 5,
        "logical_payload_processes": 5,
        "logical_payload_fragment_references": 5,
        "logical_payload_fragment_aliases": 0,
        "repeated_physical_reads": 0,
        "repeated_physical_decodes": 0,
        "repeated_physical_preparations": 0,
        "repeated_logical_payload_processes": 0,
        "peak_raw_bytes": block_io["peak_raw_bytes"],
    }
    assert block_io["peak_raw_bytes"] > 0


@pytest.mark.asyncio
async def test_real_postgres_candidate_batch_reads_and_processes_each_block_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Prove matching and physical block accounting against PostgreSQL 5440."""

    if os.getenv("HLTHPRT_PTG2_AUDIT_BATCH_POSTGRES_TEST") != "1":
        pytest.skip("set HLTHPRT_PTG2_AUDIT_BATCH_POSTGRES_TEST=1")
    candidate_case = _candidate_batch_case()
    snapshot_descriptor = _patch_candidate_modules(monkeypatch, candidate_case)
    atom_header_spy = Mock(wraps=ptg2_serving_binary_v3._price_atom_header)
    monkeypatch.setattr(
        ptg2_serving_binary_v3,
        "_price_atom_header",
        atom_header_spy,
    )

    audit_result, block_io = await _run_candidate_batch(candidate_case)

    _assert_witness_ledger(audit_result)
    _assert_block_ledger(block_io)
    assert atom_header_spy.call_count == 1
    snapshot_descriptor.assert_awaited_once_with(
        ANY,
        SNAPSHOT_ID,
        candidate_audit_access=candidate_case.access,
    )
