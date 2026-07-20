# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Rate-limited multi-pod execution of bounded candidate audit partitions."""

from __future__ import annotations

import asyncio
import datetime
import json
import time
from typing import Any, Mapping, Sequence

import aiohttp

from process.ptg_parts.ptg2_batch_candidate_audit import (
    BatchCandidateAuditContractError,
    BatchCandidateAuditTransportError,
    PTG2_BATCH_AUDIT_MAX_RESPONSE_BYTES,
)
from process.ptg_parts.ptg2_batch_candidate_audit_report import (
    BatchAuditReportTarget,
)
from process.ptg_parts.ptg2_candidate_audit_batch_contract import (
    PTG2_AUDIT_BATCH_API_PATH,
    group_audit_batch_challenges,
)
from process.ptg_parts.ptg2_candidate_audit_contract import (
    FastAuditHttpConfig,
    PTG2_FAST_AUDIT_DEADLINE_SECONDS,
)
from process.ptg_parts.ptg2_candidate_audit_evidence import (
    source_audit_condition,
    validate_provider_witness,
)
from process.ptg_parts.ptg2_candidate_audit_plan_store import (
    PersistedAuditSample,
)
from process.ptg_parts.ptg2_partitioned_candidate_audit_contract import (
    PTG2_PARTITIONED_CANDIDATE_AUDIT_MAX_IN_FLIGHT,
    PTG2_PARTITIONED_CANDIDATE_AUDIT_REQUESTS_PER_SECOND,
    PartitionedCandidateAuditBinding,
    PartitionedCandidateAuditPlan,
    PartitionedCandidateAuditRequest,
    PartitionedCandidateAuditResult,
    PartitionedPersistedOccurrence,
    PartitionedSourceChallenge,
    build_partitioned_candidate_audit_plan,
    parse_partitioned_candidate_audit_result,
    validate_partitioned_candidate_audit_results,
)
from process.ptg_parts.ptg2_partitioned_candidate_audit_report import (
    PartitionedAuditHttpMetrics,
    PartitionedAuditReportInput,
    build_partitioned_audit_report,
)
from process.ptg_parts.ptg2_shared_source_set import (
    ordered_source_ordinal_digest,
)
from process.ptg_parts.ptg2_source_witness_contract import (
    LoadedSourceWitness,
)


_RETRYABLE_STATUS_CODES = frozenset({408, 429, 500, 502, 503, 504})


class _RequestStartGate:
    """Space actual POST starts without catch-up bursts."""

    def __init__(self, requests_per_second: float) -> None:
        if not _is_positive_finite(requests_per_second):
            raise BatchCandidateAuditContractError(
                "batch_request_start_rate_invalid"
            )
        self._interval_seconds = 1.0 / requests_per_second
        self._next_start = 0.0
        self._lock = asyncio.Lock()

    async def wait(self) -> None:
        """Wait until the next non-burst request-start slot."""

        async with self._lock:
            now = time.monotonic()
            if self._next_start > now:
                await asyncio.sleep(self._next_start - now)
            actual_start = time.monotonic()
            self._next_start = actual_start + self._interval_seconds


def _is_positive_finite(value: float) -> bool:
    try:
        normalized = float(value)
    except (TypeError, ValueError):
        return False
    return normalized > 0 and normalized != float("inf")


def _event_loop_contract(*, require_uvloop: bool) -> str:
    from process.ptg_parts.ptg2_batch_candidate_audit import (
        _event_loop_contract as legacy_event_loop_contract,
    )

    return legacy_event_loop_contract(require_uvloop=require_uvloop)


def _witness_io(witness: LoadedSourceWitness) -> dict[str, int]:
    evidence_reference_count = sum(
        1 + int(record.linked_provider_sha256 is not None)
        for record in witness.records
    )
    unique_evidence_count = len(witness.evidence_by_sha256)
    return {
        "payload_reads": 1,
        "payload_decodes": 1,
        "record_decodes": len(witness.records),
        "unique_evidence_entries": unique_evidence_count,
        "evidence_decompressions": unique_evidence_count,
        "evidence_sha256_hashes": unique_evidence_count,
        "evidence_json_parses": unique_evidence_count,
        "evidence_reuse_deliveries": (
            evidence_reference_count - unique_evidence_count
        ),
        "repeated_evidence_decompressions": 0,
        "repeated_evidence_sha256_hashes": 0,
        "repeated_evidence_json_parses": 0,
    }


def _source_challenges(
    witness: LoadedSourceWitness,
    raw_container_sha256: Sequence[str],
) -> tuple[PartitionedSourceChallenge, ...]:
    for provider_witness in witness.provider_records:
        validate_provider_witness(
            provider_witness,
            parsed_evidence_by_sha256=witness.evidence_by_sha256,
        )
    grouped = group_audit_batch_challenges(
        raw_container_sha256,
        tuple(
            source_audit_condition(
                witness_record,
                parsed_evidence_by_sha256=witness.evidence_by_sha256,
            )
            for witness_record in witness.occurrence_records
        ),
    )
    return tuple(
        PartitionedSourceChallenge(
            ordinal=ordinal,
            code_system=grouped_challenge.code_system,
            code=grouped_challenge.code,
            npi=grouped_challenge.npi,
            source_artifact_key=grouped_challenge.source_artifact_key,
            tuple_digest=grouped_challenge.tuple_digest,
            network_name_digests=grouped_challenge.network_name_digests,
            multiplicity=grouped_challenge.multiplicity,
        )
        for ordinal, grouped_challenge in enumerate(grouped)
    )


def _persisted_occurrences(
    sample: PersistedAuditSample,
) -> tuple[PartitionedPersistedOccurrence, ...]:
    return tuple(
        PartitionedPersistedOccurrence(
            ordinal=ordinal,
            occurrence_id=item.occurrence_id,
            code_system=item.code_system,
            code=item.code,
            code_key=item.code_key,
            provider_set_key=item.provider_set_key,
            price_key=item.price_key,
            source_artifact_key=item.source_artifact_key,
            npi=item.npi,
            atom_ordinal=item.atom_ordinal,
            atom_key=item.atom_key,
        )
        for ordinal, item in enumerate(sample.records)
    )


def build_candidate_audit_partition_plan(
    *,
    audit_target: BatchAuditReportTarget,
    witness: LoadedSourceWitness,
    persisted_sample: PersistedAuditSample,
) -> PartitionedCandidateAuditPlan:
    """Validate local evidence once and build the exact request plan."""

    witness_metadata = witness.metadata
    return build_partitioned_candidate_audit_plan(
        binding=PartitionedCandidateAuditBinding(
            snapshot_id=audit_target.snapshot_id,
            source_key=audit_target.source_key,
            plan_id=audit_target.plan_id,
            plan_market_type=audit_target.plan_market_type,
            audit_sample_digest=str(
                audit_target.audit_sample.get("sample_digest") or ""
            ),
            source_witness_sample_digest=str(
                witness_metadata.get("sample_digest") or ""
            ),
            source_witness_payload_sha256=str(
                witness_metadata.get("payload_sha256") or ""
            ),
            ordered_source_ordinal_digest=ordered_source_ordinal_digest(
                audit_target.raw_container_sha256
            ),
            source_occurrence_count=int(
                witness_metadata.get("occurrence_witness_count") or 0
            ),
            persisted_occurrence_count=persisted_sample.sample_count,
        ),
        source_challenges=_source_challenges(
            witness,
            audit_target.raw_container_sha256,
        ),
        persisted_occurrences=_persisted_occurrences(persisted_sample),
    )


async def _bounded_response_body(response: aiohttp.ClientResponse) -> bytes:
    response_body = bytearray()
    async for chunk in response.content.iter_chunked(64 * 1024):
        if len(response_body) + len(chunk) > PTG2_BATCH_AUDIT_MAX_RESPONSE_BYTES:
            raise BatchCandidateAuditContractError("batch_response_too_large")
        response_body.extend(chunk)
    return bytes(response_body)


async def _post_partition(
    *,
    client: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
    start_gate: _RequestStartGate,
    metrics: PartitionedAuditHttpMetrics,
    request: PartitionedCandidateAuditRequest,
    verify_tls: bool,
) -> PartitionedCandidateAuditResult:
    async with semaphore:
        await start_gate.wait()
        metrics.started()
        try:
            async with client.post(
                PTG2_AUDIT_BATCH_API_PATH,
                json=request.payload,
                ssl=verify_tls,
                allow_redirects=False,
            ) as response:
                body = await _bounded_response_body(response)
                if response.status in _RETRYABLE_STATUS_CODES:
                    raise BatchCandidateAuditTransportError(
                        "batch_endpoint_temporarily_unavailable"
                    )
                if response.status != 200:
                    raise BatchCandidateAuditContractError(
                        f"batch_endpoint_rejected_{response.status}"
                    )
            try:
                raw_result = json.loads(body)
                partition_result = parse_partitioned_candidate_audit_result(
                    raw_result,
                    request=request,
                )
            except (UnicodeDecodeError, json.JSONDecodeError, TypeError, ValueError) as exc:
                raise BatchCandidateAuditContractError(
                    "batch_response_invalid"
                ) from exc
        except (BatchCandidateAuditContractError, BatchCandidateAuditTransportError):
            metrics.failed()
            raise
        except (TimeoutError, aiohttp.ClientError) as exc:
            metrics.failed()
            reason = (
                "batch_deadline_exceeded"
                if isinstance(exc, TimeoutError)
                else "batch_endpoint_transport_failed"
            )
            raise BatchCandidateAuditTransportError(reason) from exc
        metrics.completed()
        return partition_result


async def _execute_partition_plan(
    *,
    plan: PartitionedCandidateAuditPlan,
    http_config: FastAuditHttpConfig,
) -> tuple[Any, PartitionedAuditHttpMetrics]:
    """Execute every planned request once with bounded paced concurrency."""

    concurrency = min(
        max(int(http_config.concurrency), 1),
        PTG2_PARTITIONED_CANDIDATE_AUDIT_MAX_IN_FLIGHT,
    )
    deadline_seconds = float(http_config.deadline_seconds)
    if not 0 < deadline_seconds <= PTG2_FAST_AUDIT_DEADLINE_SECONDS:
        raise BatchCandidateAuditContractError("batch_deadline_invalid")
    timeout = aiohttp.ClientTimeout(total=deadline_seconds)
    connector = aiohttp.TCPConnector(
        limit=concurrency,
        limit_per_host=concurrency,
        force_close=True,
        ssl=None if http_config.verify_tls else False,
    )
    metrics = PartitionedAuditHttpMetrics(planned_request_count=plan.request_count)
    semaphore = asyncio.Semaphore(concurrency)
    start_gate = _RequestStartGate(PTG2_PARTITIONED_CANDIDATE_AUDIT_REQUESTS_PER_SECOND)
    tasks: list[asyncio.Task[PartitionedCandidateAuditResult]] = []
    try:
        async with aiohttp.ClientSession(
            base_url=http_config.api_base_url.rstrip("/"),
            headers=dict(http_config.headers),
            timeout=timeout,
            connector=connector,
            trust_env=False,
        ) as client:
            tasks = [
                asyncio.create_task(
                    _post_partition(
                        client=client,
                        semaphore=semaphore,
                        start_gate=start_gate,
                        metrics=metrics,
                        request=request,
                        verify_tls=http_config.verify_tls,
                    )
                )
                for request in plan.requests
            ]
            partition_results = await asyncio.gather(*tasks)
    finally:
        incomplete_tasks = [task for task in tasks if not task.done()]
        for task in incomplete_tasks:
            task.cancel()
        if incomplete_tasks:
            await asyncio.gather(*incomplete_tasks, return_exceptions=True)
    return validate_partitioned_candidate_audit_results(plan, partition_results), metrics


async def run_partitioned_candidate_audit(
    *,
    audit_target: BatchAuditReportTarget,
    witness: LoadedSourceWitness,
    persisted_sample: PersistedAuditSample,
    http_config: FastAuditHttpConfig,
) -> dict[str, Any]:
    """Execute every exact partition once at two request starts per second."""

    plan = build_candidate_audit_partition_plan(
        audit_target=audit_target,
        witness=witness,
        persisted_sample=persisted_sample,
    )
    event_loop_contract = _event_loop_contract(
        require_uvloop=http_config.require_uvloop
    )
    started_at = datetime.datetime.now(datetime.timezone.utc)
    aggregate, metrics = await _execute_partition_plan(
        plan=plan,
        http_config=http_config,
    )
    completed_at = datetime.datetime.now(datetime.timezone.utc)
    if (
        metrics.planned_request_count != plan.request_count
        or metrics.started_request_count != plan.request_count
        or metrics.completed_request_count != plan.request_count
        or metrics.failed_request_count != 0
        or metrics.in_flight != 0
    ):
        raise BatchCandidateAuditContractError(
            "batch_http_accounting_incomplete"
        )
    return build_partitioned_audit_report(
        PartitionedAuditReportInput(
            audit_target=audit_target,
            plan=plan,
            aggregate=aggregate,
            metrics=metrics,
            http_config=http_config,
            witness_io=_witness_io(witness),
            event_loop_contract=event_loop_contract,
            started_at=started_at,
            completed_at=completed_at,
        )
    )


__all__ = [
    "PTG2_PARTITIONED_CANDIDATE_AUDIT_MAX_IN_FLIGHT",
    "PTG2_PARTITIONED_CANDIDATE_AUDIT_REQUESTS_PER_SECOND",
    "PartitionedAuditHttpMetrics",
    "build_candidate_audit_partition_plan",
    "run_partitioned_candidate_audit",
]
