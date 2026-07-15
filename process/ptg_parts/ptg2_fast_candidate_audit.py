# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Fast, bounded source-witness verification for strict PTG V3 candidates."""

from __future__ import annotations

import asyncio
import datetime
import json
import time
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Mapping

import aiohttp
import uvloop

from process.ptg_parts.ptg2_candidate_audit_contract import (
    PTG2_FAST_AUDIT_CONTRACT,
    PTG2_FAST_AUDIT_DEADLINE_SECONDS,
    PTG2_FAST_AUDIT_TOOL,
    PTG2_FAST_AUDIT_TOOL_VERSION,
    FastAuditHttpConfig,
    FastAuditTarget,
    FastCandidateAuditError,
)
from process.ptg_parts.ptg2_candidate_audit_evidence import (
    SourceChallenge,
    source_challenge,
    is_tuple_matching_challenge,
    validate_provider_witness,
)
from process.ptg_parts.ptg2_fast_candidate_audit_report import (
    FastAuditHttpMetrics,
    FastAuditReportInput,
    build_fast_audit_report,
)
from process.ptg_parts.ptg2_source_witness import LoadedSourceWitness
from scripts.validation import ptg2_v3_source_api_audit as source_audit


FAST_AUDIT_CONTRACT = PTG2_FAST_AUDIT_CONTRACT
FAST_AUDIT_TOOL = PTG2_FAST_AUDIT_TOOL
FAST_AUDIT_TOOL_VERSION = PTG2_FAST_AUDIT_TOOL_VERSION
FAST_AUDIT_DEADLINE_SECONDS = PTG2_FAST_AUDIT_DEADLINE_SECONDS
FAST_AUDIT_CONCURRENCY = 32
FAST_AUDIT_PAGE_SIZE = 100
FAST_AUDIT_MAX_PAGES = 8
FAST_AUDIT_MAX_RESPONSE_BYTES = 8 * 1024 * 1024
_RETRYABLE_STATUS = frozenset({429, 502, 503, 504})


@dataclass(frozen=True)
class _CandidatePage:
    response_items: list[Mapping[str, Any]]
    total: int


def _event_loop_contract(*, require_uvloop: bool) -> str:
    loop = asyncio.get_running_loop()
    if isinstance(loop, uvloop.Loop):
        return "uvloop"
    if require_uvloop:
        raise FastCandidateAuditError("uvloop_required")
    return f"{type(loop).__module__}.{type(loop).__name__}"


def _mapping(value: Any, *, field_name: str) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise FastCandidateAuditError(f"{field_name}_invalid")
    return dict(value)


def _strict_int(value: Any, *, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise FastCandidateAuditError(f"{field_name}_invalid")
    return value


async def _bounded_response_body(response: aiohttp.ClientResponse) -> bytes:
    response_body = bytearray()
    async for chunk in response.content.iter_chunked(64 * 1024):
        if len(response_body) + len(chunk) > FAST_AUDIT_MAX_RESPONSE_BYTES:
            raise FastCandidateAuditError("api_response_too_large")
        response_body.extend(chunk)
    return bytes(response_body)


def _contract_errors(
    response_fields: Mapping[str, Any],
    audit_target: FastAuditTarget,
    *,
    positive: bool,
) -> list[str]:
    try:
        contract = source_audit.HttpApiFetcher._contract(response_fields)
    except source_audit.AuditError as exc:
        return [str(exc)]
    expected_contract_by_field = {
        "pricing_scope": source_audit.EXPECTED_PRICING_SCOPE,
        "query_mode": source_audit.EXPECTED_QUERY_MODE,
        "query_snapshot_id": audit_target.snapshot_id,
        "query_plan_id": audit_target.plan_id,
        "resolved_snapshot_id": audit_target.snapshot_id,
        "provenance_arch_version": source_audit.EXPECTED_ARCHITECTURE,
        "provenance_storage_generation": source_audit.EXPECTED_STORAGE_GENERATION,
        "provenance_database_backend": source_audit.EXPECTED_DATABASE_BACKEND,
        "provenance_plan_id": audit_target.plan_id,
        "provenance_snapshot_id": audit_target.snapshot_id,
        "provenance_mode": source_audit.EXPECTED_QUERY_MODE,
        "provenance_pricing_scope": source_audit.EXPECTED_PRICING_SCOPE,
        "query_source_key": audit_target.source_key,
        "provenance_source_key": audit_target.source_key,
    }
    errors = [
        f"{field_name}_mismatch"
        for field_name, expected_value in expected_contract_by_field.items()
        if getattr(contract, field_name) != expected_value
    ]
    if (
        contract.provenance_database_evidence_contract
        != source_audit.DATABASE_EVIDENCE_CONTRACT
        or contract.provenance_postgres_server_version_num < 10_000
        or not contract.provenance_database_selected
        or not contract.provenance_backend_session_active
        or not contract.provenance_transaction_snapshot_observed
    ):
        errors.append("database_evidence_mismatch")
    if positive and contract.result_state != "matched":
        errors.append("positive_result_state_mismatch")
    return errors


async def _request_json(
    client: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
    metrics: FastAuditHttpMetrics,
    url: str,
    params: Mapping[str, Any],
) -> Mapping[str, Any]:
    for attempt in range(2):
        started = time.perf_counter()
        async with semaphore:
            try:
                async with client.get(
                    url,
                    params=params,
                    allow_redirects=False,
                ) as response:
                    body = await _bounded_response_body(response)
                    status_code = response.status
            except asyncio.TimeoutError as exc:
                metrics.request_count += 1
                metrics.latencies_ms.append((time.perf_counter() - started) * 1_000)
                if attempt == 0:
                    metrics.retry_count += 1
                    continue
                raise FastCandidateAuditError("api_request_timeout") from exc
            except aiohttp.ClientError as exc:
                metrics.request_count += 1
                metrics.latencies_ms.append((time.perf_counter() - started) * 1_000)
                if attempt == 0:
                    metrics.retry_count += 1
                    continue
                raise FastCandidateAuditError("api_transport_failure") from exc
        metrics.request_count += 1
        metrics.latencies_ms.append((time.perf_counter() - started) * 1_000)
        if status_code in _RETRYABLE_STATUS and attempt == 0:
            metrics.retry_count += 1
            await asyncio.sleep(0.05)
            continue
        if status_code < 200 or status_code >= 300:
            raise FastCandidateAuditError("api_non_success_status")
        try:
            response_fields = json.loads(body, parse_float=Decimal, parse_int=int)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise FastCandidateAuditError("api_response_json_invalid") from exc
        if not isinstance(response_fields, Mapping):
            raise FastCandidateAuditError("api_response_not_object")
        return response_fields
    raise FastCandidateAuditError("api_transport_failure")


def _challenge_params(
    audit_target: FastAuditTarget,
    challenge: SourceChallenge,
    *,
    offset: int,
) -> dict[str, Any]:
    query_by_parameter: dict[str, Any] = {
        "plan_id": audit_target.plan_id,
        "snapshot_id": audit_target.snapshot_id,
        "plan_market_type": audit_target.plan_market_type,
        "source_key": audit_target.source_key,
        "mode": source_audit.EXPECTED_QUERY_MODE,
        "code_system": challenge.query.code_system,
        "code": challenge.query.code,
        "npi": challenge.query.npi,
        "negotiated_rate": challenge.negotiated_rate,
        "negotiated_rate_tolerance": "0",
        "include_providers": "true",
        "include_details": "true",
        "include_sources": "true",
        "include_allowed_amounts": "false",
        "include_unverified_addresses": "true",
        "order": "asc",
        "order_by": "npi",
        "limit": FAST_AUDIT_PAGE_SIZE,
        "offset": offset,
    }
    if challenge.service_codes:
        query_by_parameter["service_code"] = challenge.service_codes[0]
    if challenge.modifiers:
        query_by_parameter["billing_code_modifier"] = ",".join(challenge.modifiers)
    return query_by_parameter


def _validated_candidate_page(
    response_fields: Mapping[str, Any],
    audit_target: FastAuditTarget,
    *,
    requested_offset: int,
    declared_total: int | None,
) -> _CandidatePage:
    if _contract_errors(response_fields, audit_target, positive=True):
        raise FastCandidateAuditError("api_contract_mismatch")
    raw_response_items = response_fields.get("items")
    pagination = _mapping(
        response_fields.get("pagination"),
        field_name="api_pagination",
    )
    if not isinstance(raw_response_items, list) or any(
        not isinstance(response_item, Mapping)
        for response_item in raw_response_items
    ):
        raise FastCandidateAuditError("api_items_invalid")
    response_items = [dict(response_item) for response_item in raw_response_items]
    returned_offset = _strict_int(
        pagination.get("offset"),
        field_name="api_pagination_offset",
    )
    returned_limit = _strict_int(
        pagination.get("limit"),
        field_name="api_pagination_limit",
    )
    total = _strict_int(
        pagination.get("total"),
        field_name="api_pagination_total",
    )
    if (
        returned_offset != requested_offset
        or returned_limit != FAST_AUDIT_PAGE_SIZE
        or total < requested_offset + len(response_items)
        or (declared_total is not None and total != declared_total)
    ):
        raise FastCandidateAuditError("api_pagination_mismatch")
    return _CandidatePage(response_items=response_items, total=total)


async def _run_challenge(
    client: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
    metrics: FastAuditHttpMetrics,
    audit_target: FastAuditTarget,
    challenge: SourceChallenge,
) -> None:
    """Resolve one sealed source occurrence through bounded API pagination."""

    registry = source_audit.SourceIdentityRegistry()
    offset = 0
    declared_total: int | None = None
    for _page_number in range(FAST_AUDIT_MAX_PAGES):
        response_fields = await _request_json(
            client,
            semaphore,
            metrics,
            source_audit.DEFAULT_CANDIDATE_API_PATH,
            _challenge_params(audit_target, challenge, offset=offset),
        )
        candidate_page = _validated_candidate_page(
            response_fields,
            audit_target,
            requested_offset=offset,
            declared_total=declared_total,
        )
        response_items = candidate_page.response_items
        declared_total = candidate_page.total
        extracted = source_audit.extract_api_tuples(
            response_items,
            registry=registry,
        )
        if extracted.schema_errors:
            raise FastCandidateAuditError("api_tuple_schema_mismatch")
        if any(
            is_tuple_matching_challenge(occurrence_key, candidate_tuple, challenge)
            for occurrence_key, candidate_tuple in extracted.tuples.items()
        ):
            return
        offset += len(response_items)
        if offset >= candidate_page.total:
            raise FastCandidateAuditError("source_witness_missing_from_api")
        if not response_items:
            raise FastCandidateAuditError("api_pagination_stalled")
    raise FastCandidateAuditError("api_challenge_page_limit")


async def _validate_audit_sample_preflight(
    client: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
    metrics: FastAuditHttpMetrics,
    audit_target: FastAuditTarget,
) -> dict[str, Any]:
    query_by_parameter = {
        "plan_id": audit_target.plan_id,
        "snapshot_id": audit_target.snapshot_id,
        "plan_market_type": audit_target.plan_market_type,
        "source_key": audit_target.source_key,
        "mode": source_audit.EXPECTED_QUERY_MODE,
        "order_by": "occurrence_id",
        "order": "asc",
        "limit": 1,
        "offset": 0,
    }
    response_fields = await _request_json(
        client,
        semaphore,
        metrics,
        source_audit.DEFAULT_API_AUDIT_PATH,
        query_by_parameter,
    )
    if _contract_errors(response_fields, audit_target, positive=True):
        raise FastCandidateAuditError("api_audit_contract_mismatch")
    source_set = _mapping(
        response_fields.get("source_set"),
        field_name="api_source_set",
    )
    if (
        source_set.get("contract") != source_audit.SOURCE_SET_CONTRACT
        or source_set.get("source_count") != audit_target.source_count
        or source_set.get("raw_container_sha256_digest")
        != audit_target.source_set_digest
    ):
        raise FastCandidateAuditError("api_source_set_mismatch")
    audit_sample = _mapping(
        response_fields.get("audit_sample"),
        field_name="api_audit_sample",
    )
    for field_name, expected_value in dict(audit_target.audit_sample).items():
        if audit_sample.get(field_name) != expected_value:
            raise FastCandidateAuditError("api_audit_sample_mismatch")
    response_items = response_fields.get("items")
    if not isinstance(response_items, list) or len(response_items) != 1:
        raise FastCandidateAuditError("api_audit_preflight_item_missing")
    source_audit.extract_api_occurrence(response_items[0])
    return audit_sample


async def _cancel_incomplete_tasks(audit_tasks: Sequence[asyncio.Task[None]]) -> None:
    incomplete_tasks = [task for task in audit_tasks if not task.done()]
    for task in incomplete_tasks:
        task.cancel()
    if incomplete_tasks:
        await asyncio.gather(*incomplete_tasks, return_exceptions=True)


async def _execute_audit_requests(
    challenges: Sequence[SourceChallenge],
    audit_target: FastAuditTarget,
    http_config: FastAuditHttpConfig,
) -> tuple[dict[str, Any], FastAuditHttpMetrics, int]:
    """Execute the preflight and all bounded source challenges."""

    concurrency = min(max(int(http_config.concurrency), 1), 64)
    deadline_seconds = min(max(float(http_config.deadline_seconds), 1.0), 55.0)
    request_timeout = min(4.0, deadline_seconds)
    timeout = aiohttp.ClientTimeout(
        total=request_timeout,
        connect=request_timeout,
        sock_connect=request_timeout,
        sock_read=request_timeout,
    )
    connector = aiohttp.TCPConnector(
        limit=concurrency,
        limit_per_host=concurrency,
        ttl_dns_cache=300,
        ssl=None if http_config.verify_tls else False,
    )
    metrics = FastAuditHttpMetrics()
    semaphore = asyncio.Semaphore(concurrency)
    audit_tasks: list[asyncio.Task[None]] = []
    try:
        async with asyncio.timeout(deadline_seconds):
            async with aiohttp.ClientSession(
                base_url=http_config.api_base_url.rstrip("/"),
                headers=dict(http_config.headers),
                timeout=timeout,
                connector=connector,
                trust_env=False,
            ) as client:
                audit_sample = await _validate_audit_sample_preflight(
                    client,
                    semaphore,
                    metrics,
                    audit_target,
                )
                audit_tasks = [
                    asyncio.create_task(
                        _run_challenge(
                            client,
                            semaphore,
                            metrics,
                            audit_target,
                            challenge,
                        )
                    )
                    for challenge in challenges
                ]
                await asyncio.gather(*audit_tasks)
    except TimeoutError as exc:
        raise FastCandidateAuditError("audit_deadline_exceeded") from exc
    finally:
        await _cancel_incomplete_tasks(audit_tasks)
    return audit_sample, metrics, concurrency


async def run_fast_candidate_audit(
    *,
    witness: LoadedSourceWitness,
    audit_target: FastAuditTarget,
    http: FastAuditHttpConfig,
) -> dict[str, Any]:
    """Verify all sealed source witnesses through bounded concurrent API calls."""

    event_loop_contract = _event_loop_contract(require_uvloop=http.require_uvloop)
    occurrence_records = witness.occurrence_records
    provider_records = witness.provider_records
    expected_challenge_count = int(witness.metadata["occurrence_witness_count"])
    if len(occurrence_records) != expected_challenge_count:
        raise FastCandidateAuditError("source_witness_challenge_count_mismatch")
    challenges = tuple(
        source_challenge(witness_record)
        for witness_record in occurrence_records
    )
    for provider_record in provider_records:
        validate_provider_witness(provider_record)
    started_at = datetime.datetime.now(datetime.timezone.utc)
    audit_sample, metrics, concurrency = await _execute_audit_requests(
        challenges,
        audit_target,
        http,
    )
    completed_at = datetime.datetime.now(datetime.timezone.utc)
    return build_fast_audit_report(
        FastAuditReportInput(
            witness=witness,
            audit_target=audit_target,
            http_config=http,
            audit_sample=audit_sample,
            http_metrics=metrics,
            concurrency=concurrency,
            challenge_count=len(challenges),
            provider_count=len(provider_records),
            event_loop_contract=event_loop_contract,
            started_at=started_at,
            completed_at=completed_at,
        )
    )


__all__ = [
    "FAST_AUDIT_CONCURRENCY",
    "FAST_AUDIT_CONTRACT",
    "FAST_AUDIT_DEADLINE_SECONDS",
    "FAST_AUDIT_TOOL",
    "FAST_AUDIT_TOOL_VERSION",
    "FastAuditHttpConfig",
    "FastAuditTarget",
    "FastCandidateAuditError",
    "run_fast_candidate_audit",
]
