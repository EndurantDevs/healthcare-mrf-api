# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""One authenticated, bounded, non-retrying PTG candidate batch request."""

from __future__ import annotations

import asyncio
import datetime
import json
from typing import Any, Mapping

import aiohttp
import uvloop

from process.ptg_parts.ptg2_batch_candidate_audit_report import (
    BatchAuditReportInput,
    BatchAuditReportTarget,
    build_batch_audit_report,
)
from process.ptg_parts.ptg2_candidate_audit_batch_contract import (
    AuditBatchWitnessBinding,
    PTG2_AUDIT_BATCH_API_PATH,
    build_audit_batch_request,
    parse_audit_batch_response,
)
from process.ptg_parts.ptg2_candidate_audit_contract import (
    FastAuditHttpConfig,
    PTG2_FAST_AUDIT_DEADLINE_SECONDS,
)


PTG2_BATCH_AUDIT_MAX_RESPONSE_BYTES = 256 * 1024
_RETRYABLE_STATUS_CODES = frozenset({408, 429, 500, 502, 503, 504})


class BatchCandidateAuditContractError(RuntimeError):
    """A deterministic request, response, or release-contract mismatch."""

    def __init__(self, reason: str):
        super().__init__(reason)
        self.reason = reason


class BatchCandidateAuditTransportError(RuntimeError):
    """A transient transport failure eligible for orchestration-level retry."""

    def __init__(self, reason: str):
        super().__init__(reason)
        self.reason = reason


def _event_loop_contract(*, require_uvloop: bool) -> str:
    event_loop = asyncio.get_running_loop()
    if isinstance(event_loop, uvloop.Loop):
        return "uvloop"
    if require_uvloop:
        raise BatchCandidateAuditContractError("uvloop_required")
    return f"{type(event_loop).__module__}.{type(event_loop).__name__}"


async def _bounded_response_body(response: aiohttp.ClientResponse) -> bytes:
    response_body = bytearray()
    async for response_chunk in response.content.iter_chunked(64 * 1024):
        if (
            len(response_body) + len(response_chunk)
            > PTG2_BATCH_AUDIT_MAX_RESPONSE_BYTES
        ):
            raise BatchCandidateAuditContractError("batch_response_too_large")
        response_body.extend(response_chunk)
    return bytes(response_body)


def _response_payload(response_body: bytes) -> Any:
    try:
        return json.loads(response_body)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise BatchCandidateAuditContractError(
            "batch_response_json_invalid"
        ) from exc


def _witness_binding(target: BatchAuditReportTarget) -> AuditBatchWitnessBinding:
    witness = target.source_witness
    return AuditBatchWitnessBinding(
        audit_sample_digest=str(target.audit_sample.get("sample_digest") or ""),
        source_witness_sample_digest=str(witness.get("sample_digest") or ""),
        source_witness_payload_sha256=str(witness.get("payload_sha256") or ""),
        raw_container_sha256=target.raw_container_sha256,
        source_witness_occurrence_count=int(
            witness.get("occurrence_witness_count") or 0
        ),
    )


def _validated_http_deadline(http_config: FastAuditHttpConfig) -> float:
    deadline_seconds = float(http_config.deadline_seconds)
    if (
        deadline_seconds <= 0
        or deadline_seconds > PTG2_FAST_AUDIT_DEADLINE_SECONDS
    ):
        raise BatchCandidateAuditContractError("batch_deadline_invalid")
    return deadline_seconds


def _transport_failure_reason(exc: BaseException) -> str:
    """Return a stable, non-sensitive reason for an HTTP client failure."""

    if isinstance(exc, TimeoutError):
        return "batch_deadline_exceeded"
    if isinstance(exc, aiohttp.ClientConnectorError):
        return "batch_endpoint_connect_failed"
    if isinstance(exc, aiohttp.ServerDisconnectedError):
        return "batch_endpoint_server_disconnected"
    if isinstance(exc, aiohttp.ClientPayloadError):
        return "batch_response_incomplete"
    return "batch_endpoint_transport_failed"


async def _post_batch_request(
    *,
    request_payload_by_field: Mapping[str, Any],
    http_config: FastAuditHttpConfig,
    deadline_seconds: float,
) -> Any:
    """Execute the single bounded POST and return its decoded JSON payload."""

    try:
        timeout = aiohttp.ClientTimeout(total=deadline_seconds)
        async with aiohttp.ClientSession(
            headers=dict(http_config.headers),
            timeout=timeout,
        ) as session:
            async with session.post(
                http_config.api_base_url + PTG2_AUDIT_BATCH_API_PATH,
                json=dict(request_payload_by_field),
                ssl=http_config.verify_tls,
                allow_redirects=False,
            ) as response:
                response_body = await _bounded_response_body(response)
                if response.status in _RETRYABLE_STATUS_CODES:
                    raise BatchCandidateAuditTransportError(
                        "batch_endpoint_temporarily_unavailable"
                    )
                if response.status != 200:
                    raise BatchCandidateAuditContractError(
                        "batch_endpoint_rejected"
                    )
                return _response_payload(response_body)
    except BatchCandidateAuditContractError:
        raise
    except BatchCandidateAuditTransportError:
        raise
    except (TimeoutError, aiohttp.ClientError) as exc:
        raise BatchCandidateAuditTransportError(
            _transport_failure_reason(exc)
        ) from exc


async def run_batch_candidate_audit(
    *,
    audit_target: BatchAuditReportTarget,
    http_config: FastAuditHttpConfig,
) -> dict[str, Any]:
    """Execute exactly one POST and return a truthful V4 release report."""

    request = build_audit_batch_request(
        snapshot_id=audit_target.snapshot_id,
        source_key=audit_target.source_key,
        plan_id=audit_target.plan_id,
        plan_market_type=audit_target.plan_market_type,
        witness_binding=_witness_binding(audit_target),
    )
    event_loop_contract = _event_loop_contract(
        require_uvloop=http_config.require_uvloop
    )
    deadline_seconds = _validated_http_deadline(http_config)
    started_at = datetime.datetime.now(datetime.timezone.utc)
    response_payload = await _post_batch_request(
        request_payload_by_field=request.payload,
        http_config=http_config,
        deadline_seconds=deadline_seconds,
    )
    try:
        parsed_response = parse_audit_batch_response(
            response_payload,
            request=request,
            expected_source_witness=audit_target.source_witness,
            expected_audit_sample=audit_target.audit_sample,
        )
    except (TypeError, ValueError) as exc:
        raise BatchCandidateAuditContractError(
            f"batch_response_invalid:{exc}"
        ) from exc
    completed_at = datetime.datetime.now(datetime.timezone.utc)
    return build_batch_audit_report(
        BatchAuditReportInput(
            target=audit_target,
            request=request,
            response=parsed_response,
            http_config=http_config,
            event_loop_contract=event_loop_contract,
            started_at=started_at,
            completed_at=completed_at,
        )
    )


__all__ = [
    "BatchCandidateAuditContractError",
    "BatchCandidateAuditTransportError",
    "PTG2_BATCH_AUDIT_MAX_RESPONSE_BYTES",
    "run_batch_candidate_audit",
]
