from __future__ import annotations

import asyncio
import time
import types
from dataclasses import replace
from unittest.mock import AsyncMock

import pytest
from aiohttp import web

from process.ptg_parts import ptg2_partitioned_candidate_audit as audit
from process.ptg_parts import ptg2_partitioned_candidate_audit_contract as contract
from process.ptg_parts.ptg2_batch_candidate_audit import (
    BatchCandidateAuditContractError,
    BatchCandidateAuditTransportError,
)
from process.ptg_parts.ptg2_candidate_audit_contract import FastAuditHttpConfig
from process.ptg_parts.ptg2_partitioned_candidate_audit_report import (
    PartitionedAuditHttpMetrics,
)


def _plan():
    source_items = tuple(
        contract.PartitionedSourceChallenge(
            ordinal=0,
            code_system="CPT",
            code="99213",
            npi=1_000_000_000 + index,
            source_artifact_key=0,
            tuple_digest=f"{index:064x}",
            network_name_digests=(),
            multiplicity=1,
        )
        for index in range(201)
    )
    persisted_items = tuple(
        contract.PartitionedPersistedOccurrence(
            ordinal=0,
            occurrence_id=index.to_bytes(32, "big"),
            code_system="CPT",
            code="99213",
            code_key=7,
            provider_set_key=index,
            price_key=index,
            source_artifact_key=0,
            npi=2_000_000_000 + index,
            atom_ordinal=0,
            atom_key=index,
        )
        for index in (1, 2)
    )
    return contract.build_partitioned_candidate_audit_plan(
        binding=contract.PartitionedCandidateAuditBinding(
            snapshot_id="candidate-snapshot",
            source_key="test-source",
            plan_id="12-3456789",
            plan_market_type="group",
            audit_sample_digest="a" * 64,
            source_witness_sample_digest="b" * 64,
            source_witness_payload_sha256="c" * 64,
            ordered_source_ordinal_digest="d" * 64,
            source_occurrence_count=201,
            persisted_occurrence_count=2,
        ),
        source_challenges=source_items,
        persisted_occurrences=persisted_items,
    )


def _one_request_plan():
    return contract.build_partitioned_candidate_audit_plan(
        binding=contract.PartitionedCandidateAuditBinding(
            snapshot_id="candidate-snapshot",
            source_key="test-source",
            plan_id="12-3456789",
            plan_market_type="group",
            audit_sample_digest="a" * 64,
            source_witness_sample_digest="b" * 64,
            source_witness_payload_sha256="c" * 64,
            ordered_source_ordinal_digest="d" * 64,
            source_occurrence_count=1,
            persisted_occurrence_count=1,
        ),
        source_challenges=(
            contract.PartitionedSourceChallenge(
                ordinal=0,
                code_system="CPT",
                code="99213",
                npi=1_234_567_890,
                source_artifact_key=0,
                tuple_digest="e" * 64,
                network_name_digests=(),
                multiplicity=1,
            ),
        ),
        persisted_occurrences=(
            contract.PartitionedPersistedOccurrence(
                ordinal=0,
                occurrence_id=b"p" * 32,
                code_system="CPT",
                code="99213",
                code_key=7,
                provider_set_key=8,
                price_key=9,
                source_artifact_key=0,
                npi=1_234_567_890,
                atom_ordinal=0,
                atom_key=10,
            ),
        ),
    )


def _block_io():
    return {
        "logical_block_deliveries": 1,
        "physical_mapping_references": 1,
        "physical_mapping_aliases": 0,
        "unique_physical_blocks": 1,
        "physical_block_reads": 1,
        "physical_block_decodes": 1,
        "physical_payload_preparations": 1,
        "expected_logical_payload_processes": 1,
        "logical_payload_processes": 1,
        "logical_payload_fragment_references": 1,
        "logical_payload_fragment_aliases": 0,
        "repeated_physical_reads": 0,
        "repeated_physical_decodes": 0,
        "repeated_physical_preparations": 0,
        "repeated_logical_payload_processes": 0,
        "peak_raw_bytes": 1024,
    }


def _candidate_io(request):
    count = len(request.source_challenges)
    return {
        "candidate_occurrence_deliveries": count,
        "unique_candidate_projections": count,
        "candidate_projection_builds": count,
        "candidate_projection_reuse_deliveries": 0,
        "repeated_candidate_projection_builds": 0,
        "availability_condition_count": count,
        "duplicate_availability_deliveries": 0,
    }


def _http(base_url: str):
    return FastAuditHttpConfig(
        api_base_url=base_url,
        headers={"Authorization": "Bearer test-token"},
        verify_tls=False,
        transport_contract="authenticated_cluster_service_v1",
        concurrency=8,
        require_uvloop=False,
    )


@pytest.mark.asyncio
async def test_executes_three_requests_at_two_starts_per_second_with_overlap(
    unused_tcp_port,
):
    starts: list[float] = []
    counters_by_name = {"active": 0, "peak_active": 0}

    async def handler(web_request):
        starts.append(time.monotonic())
        counters_by_name["active"] += 1
        counters_by_name["peak_active"] = max(
            counters_by_name["peak_active"],
            counters_by_name["active"],
        )
        request = contract.parse_partitioned_candidate_audit_request(
            await web_request.json()
        )
        await asyncio.sleep(0.75)
        counters_by_name["active"] -= 1
        result = contract.build_partitioned_candidate_audit_result(
            request=request,
            matched_source_occurrence_count=request.source_occurrence_count,
            validated_persisted_occurrence_count=len(
                request.persisted_occurrences
            ),
            duration_ms=750,
            block_io=_block_io(),
            candidate_processing_io=_candidate_io(request),
        )
        return web.json_response(result.payload)

    application = web.Application()
    application.router.add_post(
        "/api/v1/pricing/providers/audit-source-witness-batch",
        handler,
    )
    runner = web.AppRunner(application)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", unused_tcp_port)
    await site.start()
    try:
        aggregate, metrics = await audit._execute_partition_plan(
            plan=_plan(),
            http_config=_http(f"http://127.0.0.1:{unused_tcp_port}"),
        )
    finally:
        await runner.cleanup()

    assert aggregate.request_count == 3
    assert metrics.started_request_count == 3
    assert metrics.completed_request_count == 3
    assert metrics.failed_request_count == 0
    assert metrics.peak_in_flight >= 2
    assert counters_by_name["peak_active"] >= 2
    assert len(starts) == 3
    assert all(
        later - earlier >= 0.45
        for earlier, later in zip(starts, starts[1:])
    )


@pytest.mark.asyncio
async def test_transient_response_is_not_retried(unused_tcp_port):
    counters_by_name = {"request_count": 0}

    async def handler(_request):
        counters_by_name["request_count"] += 1
        return web.Response(status=503)

    application = web.Application()
    application.router.add_post(
        "/api/v1/pricing/providers/audit-source-witness-batch",
        handler,
    )
    runner = web.AppRunner(application)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", unused_tcp_port)
    await site.start()
    try:
        with pytest.raises(
            BatchCandidateAuditTransportError,
            match="temporarily_unavailable",
        ):
            await audit._execute_partition_plan(
                plan=_plan(),
                http_config=_http(
                    f"http://127.0.0.1:{unused_tcp_port}"
                ),
            )
    finally:
        await runner.cleanup()

    assert counters_by_name["request_count"] == 1


@pytest.mark.parametrize("requests_per_second", [0, -1, float("inf"), "bad"])
def test_request_start_gate_rejects_nonpositive_or_nonfinite_rate(
    requests_per_second,
):
    with pytest.raises(BatchCandidateAuditContractError, match="start_rate"):
        audit._RequestStartGate(requests_per_second)


@pytest.mark.parametrize("deadline_seconds", [0, 56])
@pytest.mark.asyncio
async def test_execute_rejects_deadline_outside_release_bound(deadline_seconds):
    with pytest.raises(BatchCandidateAuditContractError, match="deadline"):
        await audit._execute_partition_plan(
            plan=_one_request_plan(),
            http_config=replace(
                _http("http://127.0.0.1:1"),
                deadline_seconds=deadline_seconds,
            ),
        )


@pytest.mark.parametrize(
    ("status", "body", "message"),
    [(400, b"{}", "rejected_400"), (200, b"not-json", "response_invalid")],
)
@pytest.mark.asyncio
async def test_partition_response_rejects_status_or_invalid_json(
    unused_tcp_port,
    status,
    body,
    message,
):
    async def handler(_request):
        return web.Response(status=status, body=body)

    application = web.Application()
    application.router.add_post(
        "/api/v1/pricing/providers/audit-source-witness-batch",
        handler,
    )
    runner = web.AppRunner(application)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", unused_tcp_port)
    await site.start()
    try:
        with pytest.raises(BatchCandidateAuditContractError, match=message):
            await audit._execute_partition_plan(
                plan=_one_request_plan(),
                http_config=_http(f"http://127.0.0.1:{unused_tcp_port}"),
            )
    finally:
        await runner.cleanup()


@pytest.mark.asyncio
async def test_partition_timeout_is_terminal_transport_failure(unused_tcp_port):
    async def handler(_request):
        await asyncio.sleep(0.1)
        return web.json_response({})

    application = web.Application()
    application.router.add_post(
        "/api/v1/pricing/providers/audit-source-witness-batch",
        handler,
    )
    runner = web.AppRunner(application)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", unused_tcp_port)
    await site.start()
    try:
        with pytest.raises(BatchCandidateAuditTransportError, match="deadline"):
            await audit._execute_partition_plan(
                plan=_one_request_plan(),
                http_config=replace(
                    _http(f"http://127.0.0.1:{unused_tcp_port}"),
                    deadline_seconds=0.01,
                ),
            )
    finally:
        await runner.cleanup()


@pytest.mark.asyncio
async def test_response_body_limit_fails_before_json_parse(monkeypatch):
    class _Content:
        async def iter_chunked(self, _chunk_size):
            yield b"12345"

    monkeypatch.setattr(audit, "PTG2_BATCH_AUDIT_MAX_RESPONSE_BYTES", 4)

    with pytest.raises(BatchCandidateAuditContractError, match="too_large"):
        await audit._bounded_response_body(types.SimpleNamespace(content=_Content()))


def test_worker_ledgers_count_linked_evidence_reuse_and_persisted_coordinates():
    witness = types.SimpleNamespace(
        records=(
            types.SimpleNamespace(linked_provider_sha256=None),
            types.SimpleNamespace(linked_provider_sha256="linked"),
        ),
        evidence_by_sha256={"rate": {}, "linked": {}},
    )
    sample = types.SimpleNamespace(
        records=(
            types.SimpleNamespace(
                occurrence_id=b"p" * 32,
                code_system="CPT",
                code="99213",
                code_key=7,
                provider_set_key=8,
                price_key=9,
                source_artifact_key=0,
                npi=1_234_567_890,
                atom_ordinal=0,
                atom_key=10,
            ),
        )
    )

    assert audit._witness_io(witness)["evidence_reuse_deliveries"] == 1
    persisted = audit._persisted_occurrences(sample)
    assert len(persisted) == 1
    assert persisted[0].occurrence_id == b"p" * 32


def _successful_aggregate(plan):
    results = tuple(
        contract.build_partitioned_candidate_audit_result(
            request=request,
            matched_source_occurrence_count=request.source_occurrence_count,
            validated_persisted_occurrence_count=len(request.persisted_occurrences),
            duration_ms=1,
            block_io=_block_io(),
            candidate_processing_io=_candidate_io(request),
        )
        for request in plan.requests
    )
    return contract.validate_partitioned_candidate_audit_results(plan, results)


@pytest.mark.asyncio
async def test_run_builds_report_from_complete_request_accounting(monkeypatch):
    plan = _one_request_plan()
    aggregate = _successful_aggregate(plan)
    metrics = PartitionedAuditHttpMetrics(
        planned_request_count=1,
        started_request_count=1,
        completed_request_count=1,
        peak_in_flight=1,
        start_times=[1.0],
    )
    witness = types.SimpleNamespace(
        metadata={},
        records=(types.SimpleNamespace(linked_provider_sha256=None),),
        evidence_by_sha256={"rate": {}},
    )
    monkeypatch.setattr(audit, "build_candidate_audit_partition_plan", lambda **_kwargs: plan)
    monkeypatch.setattr(audit, "_event_loop_contract", lambda **_kwargs: "uvloop")
    monkeypatch.setattr(
        audit,
        "_execute_partition_plan",
        AsyncMock(return_value=(aggregate, metrics)),
    )
    monkeypatch.setattr(
        audit,
        "build_partitioned_audit_report",
        lambda report_input: {"requests": report_input.metrics.completed_request_count},
    )

    report = await audit.run_partitioned_candidate_audit(
        audit_target=object(),
        witness=witness,
        persisted_sample=object(),
        http_config=_http("https://audit.internal.example"),
    )

    assert report == {"requests": 1}


@pytest.mark.asyncio
async def test_run_rejects_incomplete_http_accounting(monkeypatch):
    plan = _one_request_plan()
    aggregate = _successful_aggregate(plan)
    metrics = PartitionedAuditHttpMetrics(
        planned_request_count=1,
        started_request_count=1,
        completed_request_count=0,
        peak_in_flight=1,
        start_times=[1.0],
    )
    monkeypatch.setattr(audit, "build_candidate_audit_partition_plan", lambda **_kwargs: plan)
    monkeypatch.setattr(audit, "_event_loop_contract", lambda **_kwargs: "uvloop")
    monkeypatch.setattr(
        audit,
        "_execute_partition_plan",
        AsyncMock(return_value=(aggregate, metrics)),
    )

    with pytest.raises(BatchCandidateAuditContractError, match="accounting"):
        await audit.run_partitioned_candidate_audit(
            audit_target=object(),
            witness=types.SimpleNamespace(records=(), evidence_by_sha256={}),
            persisted_sample=object(),
            http_config=_http("https://audit.internal.example"),
        )
