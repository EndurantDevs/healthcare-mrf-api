# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import hashlib
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
from api import ptg2_candidate_audit_partition as candidate_partition
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
from process.ptg_parts.ptg2_batch_candidate_audit_report import (
    BatchAuditReportTarget,
)
from process.ptg_parts.ptg2_candidate_audit_plan_store import (
    load_persisted_audit_sample,
)
from process.ptg_parts.ptg2_partitioned_candidate_audit import (
    build_candidate_audit_partition_plan,
)
from process.ptg_parts.ptg2_partitioned_candidate_audit_contract import (
    PTG2_PARTITIONED_CANDIDATE_AUDIT_MAX_ITEMS,
    build_partitioned_candidate_audit_result,
    validate_partitioned_candidate_audit_results,
)
from process.ptg_parts.ptg2_source_witness_store import (
    load_shared_source_witness,
)
from process.ptg_parts import ptg2_source_witness_store as witness_store
from process.ptg_parts import ptg2_serving_binary_v3
from tests.ptg2_candidate_audit_batch_postgres_fixture import (
    PLAN_ID,
    SNAPSHOT_ID,
    SNAPSHOT_KEY,
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


def _candidate_batch_case(*, persisted_count: int = 2) -> _CandidateBatchCase:
    schema_name = f"ptg2_audit_batch_{uuid.uuid4().hex[:16]}"
    witness_payload, witness_metadata = source_witness()
    persisted_audit_rows = audit_rows(persisted_count)
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
                audit_result = (
                    await candidate_batch.audit_candidate_source_witness_batch(
                        session,
                        candidate_case.audit_request,
                        candidate_case.access,
                    )
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
    assert (
        audit_result.candidate_processing_io["repeated_candidate_projection_builds"]
        == 0
    )


def _assert_block_ledger(block_io: Mapping[str, int]) -> None:
    assert block_io == {
        "logical_block_deliveries": 9,
        "physical_mapping_references": 9,
        "physical_mapping_aliases": 2,
        "unique_physical_blocks": 7,
        "physical_block_reads": 7,
        "physical_block_decodes": 7,
        "physical_payload_preparations": 7,
        "expected_logical_payload_processes": 7,
        "logical_payload_processes": 7,
        "logical_payload_fragment_references": 7,
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
    provider_scope_spy = AsyncMock(
        wraps=candidate_batch._provider_network_digests_by_key
    )
    monkeypatch.setattr(
        ptg2_serving_binary_v3,
        "_price_atom_header",
        atom_header_spy,
    )
    monkeypatch.setattr(
        candidate_batch,
        "_provider_network_digests_by_key",
        provider_scope_spy,
    )

    audit_result, block_io = await _run_candidate_batch(candidate_case)

    _assert_witness_ledger(audit_result)
    _assert_block_ledger(block_io)
    assert atom_header_spy.call_count == 1
    assert provider_scope_spy.await_args.args[2] == {7: (5,), 8: (5,)}
    snapshot_descriptor.assert_awaited_once_with(
        ANY,
        SNAPSHOT_ID,
        candidate_audit_access=candidate_case.access,
    )


async def _load_postgres_partition_plan(
    monkeypatch: pytest.MonkeyPatch,
    candidate_case: _CandidateBatchCase,
):
    original_first = db.first
    original_all = db.all
    witness_query = AsyncMock(wraps=original_first)
    collection_query = AsyncMock(wraps=original_all)
    monkeypatch.setattr(db, "first", witness_query)
    monkeypatch.setattr(db, "all", collection_query)
    try:
        witness = await load_shared_source_witness(
            schema_name=candidate_case.schema_name,
            snapshot_key=SNAPSHOT_KEY,
            expected_raw_source_sha256=(SOURCE_DIGEST,),
            expected_metadata=candidate_case.witness_metadata,
        )
        persisted_sample = await load_persisted_audit_sample(
            schema_name=candidate_case.schema_name,
            snapshot_key=SNAPSHOT_KEY,
            expected_metadata=candidate_case.serving_tables.audit_sample,
        )
    finally:
        monkeypatch.setattr(db, "first", original_first)
        monkeypatch.setattr(db, "all", original_all)
    witness_query.assert_awaited_once()
    assert collection_query.await_count == 2
    executed_sql_statements = [
        str(query_call.args[0])
        for query_call in collection_query.await_args_list
    ]
    assert "ptg2_v3_source_audit_witness_part" in executed_sql_statements[0]
    assert "ptg2_v3_audit_occurrence" in executed_sql_statements[1]
    return build_candidate_audit_partition_plan(
        audit_target=BatchAuditReportTarget(
            snapshot_id=SNAPSHOT_ID,
            source_key=SOURCE_KEY,
            plan_id=PLAN_ID,
            plan_market_type="group",
            raw_container_sha256=(SOURCE_DIGEST,),
            source_witness=candidate_case.witness_metadata,
            audit_sample=candidate_case.serving_tables.audit_sample,
            provider_identifier_quarantine={},
        ),
        witness=witness,
        persisted_sample=persisted_sample,
    )


def _assert_exact_partition_dispatch(plan) -> None:
    assert plan.request_count == 2
    assert all(
        request.item_count <= PTG2_PARTITIONED_CANDIDATE_AUDIT_MAX_ITEMS
        for request in plan.requests
    )
    dispatched_ordinals = tuple(
        audit_item.ordinal
        for request in plan.requests
        for audit_item in (
            *request.source_challenges,
            *request.persisted_occurrences,
        )
    )
    assert sorted(dispatched_ordinals) == list(range(103))
    assert len(dispatched_ordinals) == len(set(dispatched_ordinals))


async def _execute_postgres_partition_plan(
    candidate_case: _CandidateBatchCase,
    plan,
):
    partition_results = []
    for request in plan.requests:
        async with db.session() as session:
            with shared_block_read_once_scope(
                max_retained_raw_bytes=1024 * 1024,
            ) as read_once_scope:
                audit_result = await candidate_partition.audit_candidate_partition(
                    session,
                    request,
                    candidate_case.access,
                )
                read_once_scope.assert_processed_once()
                block_io = read_once_scope.ledger
        partition_results.append(
            build_partitioned_candidate_audit_result(
                request=request,
                matched_source_occurrence_count=(
                    audit_result.matched_challenge_count
                ),
                validated_persisted_occurrence_count=(
                    audit_result.validated_persisted_audit_occurrence_count
                ),
                duration_ms=1,
                block_io=block_io,
                candidate_processing_io=audit_result.candidate_processing_io,
            )
        )
    return validate_partitioned_candidate_audit_results(
        plan,
        partition_results,
    )


def _assert_partition_aggregate(aggregate) -> None:
    assert aggregate.request_count == 2
    assert aggregate.source_occurrence_count == 2
    assert aggregate.persisted_occurrence_count == 101
    assert aggregate.block_io["repeated_physical_reads"] == 0
    assert aggregate.block_io["repeated_physical_decodes"] == 0
    assert (
        aggregate.candidate_processing_io[
            "repeated_candidate_projection_builds"
        ]
        == 0
    )


async def _persist_multipart_witness(
    monkeypatch: pytest.MonkeyPatch,
    candidate_case: _CandidateBatchCase,
    schema: str,
) -> None:
    segment_bytes = (len(candidate_case.witness_payload) + 2) // 3
    monkeypatch.setattr(
        witness_store,
        "PTG2_V3_SOURCE_WITNESS_MAX_PART_BYTES",
        segment_bytes,
    )
    payload_parts = witness_store.split_source_witness_payload(
        candidate_case.witness_payload
    )
    assert len(payload_parts) == 3
    await db.status(
        f"""
        UPDATE {schema}.ptg2_v3_source_audit_witness
           SET payload = :payload
         WHERE snapshot_key = :snapshot_key
        """,
        payload=payload_parts[0],
        snapshot_key=SNAPSHOT_KEY,
    )
    for part_number, payload_part in enumerate(payload_parts[1:], start=1):
        await db.status(
            f"""
            INSERT INTO {schema}.ptg2_v3_source_audit_witness_part
                (snapshot_key, part_number, part_sha256, payload)
            VALUES
                (:snapshot_key, :part_number, :part_sha256, :payload)
            """,
            snapshot_key=SNAPSHOT_KEY,
            part_number=part_number,
            part_sha256=hashlib.sha256(payload_part).digest(),
            payload=payload_part,
        )


@pytest.mark.asyncio
async def test_real_postgres_partition_plan_dispatches_every_item_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Prove two bounded V2 requests over one worker-side PostgreSQL load."""

    if os.getenv("HLTHPRT_PTG2_AUDIT_BATCH_POSTGRES_TEST") != "1":
        pytest.skip("set HLTHPRT_PTG2_AUDIT_BATCH_POSTGRES_TEST=1")
    candidate_case = _candidate_batch_case(persisted_count=101)
    snapshot_descriptor = _patch_candidate_modules(monkeypatch, candidate_case)
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
        await _persist_multipart_witness(monkeypatch, candidate_case, schema)
        plan = await _load_postgres_partition_plan(monkeypatch, candidate_case)
        _assert_exact_partition_dispatch(plan)
        aggregate = await _execute_postgres_partition_plan(
            candidate_case,
            plan,
        )
        _assert_partition_aggregate(aggregate)
        assert snapshot_descriptor.await_count == 2
    finally:
        try:
            await db.execute_ddl(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
        finally:
            await db.disconnect()
