"""Bounded public-API probes for the PTG V4 dev canary."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping
from urllib.parse import urlsplit

import httpx

from scripts.ptg_v4_dev_canary_io import (
    CanaryInfrastructureError,
    get_identity_json,
    get_metrics,
    load_json,
)
from scripts.ptg_v4_dev_canary_reference import (
    validate_reference_evidence,
)
from scripts.ptg_v4_dev_canary_support import (
    PROTECTED_API_PARAMETERS,
    BoundedApiSpec,
    CanaryConfigurationError,
    GraphReadLimits,
    evaluate_api_probe,
    evaluate_graph_metric_delta,
    headers_from_environment,
    parse_string_pairs,
    validate_base_url,
)


@dataclass(frozen=True)
class _WarmProbeEvidence:
    """Response, latency, and graph evidence collected from one API process."""

    response_documents: list[dict[str, Any]]
    warm_samples_ms: list[float]
    graph_read_evidence: dict[str, Any]


@dataclass(frozen=True)
class _ApiProbeContext:
    """Immutable endpoint, credentials, query, and payload bound."""

    api_url: str
    metrics_url: str
    api_headers: Mapping[str, str]
    metrics_headers: Mapping[str, str]
    parameters: Mapping[str, str]
    maximum_bytes: int


def service_inputs(
    args: Any,
) -> tuple[str, str, dict[str, str], dict[str, str]]:
    """Resolve a same-origin API and metrics endpoint without exposing secrets."""

    api_url = validate_base_url(
        args.api_base_url,
        allow_insecure_http=args.allow_insecure_http,
    )
    metrics_url = validate_base_url(
        args.metrics_url,
        allow_insecure_http=args.allow_insecure_http,
    )
    api_headers, _api_environment_names = headers_from_environment(
        args.api_header_env
    )
    metrics_headers, _metrics_environment_names = headers_from_environment(
        args.metrics_header_env
    )
    if _url_origin(api_url) != _url_origin(metrics_url):
        raise CanaryConfigurationError(
            "API and metrics evidence must use the same direct process origin"
        )
    return api_url, metrics_url, api_headers, metrics_headers


def _url_origin(url: str) -> tuple[str, str, int | None]:
    parsed = urlsplit(url)
    return parsed.scheme, str(parsed.hostname or ""), parsed.port


def require_same_runtime_identity(
    *runtime_identities: Mapping[str, Any],
) -> dict[str, str]:
    """Return the common process identity or reject mixed-pod evidence."""

    normalized_identities = [
        {
            field_name: str(identity.get(field_name) or "").strip()
            for field_name in (
                "process_identity",
                "process_started_at",
                "image_identity",
            )
        }
        for identity in runtime_identities
    ]
    if (
        not normalized_identities
        or any(
            not all(identity_by_field.values())
            for identity_by_field in normalized_identities
        )
        or any(
            identity_by_field != normalized_identities[0]
            for identity_by_field in normalized_identities[1:]
        )
    ):
        raise CanaryInfrastructureError(
            "API and metrics evidence came from different runtime processes"
        )
    return normalized_identities[0]


def query_fingerprint(parameters: Mapping[str, str]) -> str:
    """Digest the complete fixed public query used for persisted cold evidence."""

    digest = hashlib.sha256()
    digest.update(b"PTG-V4-CANARY-QUERY-V1\0")
    digest.update(
        json.dumps(
            dict(parameters),
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    )
    return digest.hexdigest()


def api_spec(args: Any) -> BoundedApiSpec:
    """Validate and build the bounded public V4 serving contract."""

    if args.page_limit < 1 or args.expected_item_count < 1:
        raise CanaryConfigurationError("bounded page sizes must be positive")
    if args.expected_item_count > args.page_limit:
        raise CanaryConfigurationError(
            "expected items cannot exceed the page limit"
        )
    if args.minimum_cold_process_samples < 20:
        raise CanaryConfigurationError(
            "cold p95 requires at least 20 unique fresh-process samples"
        )
    if (
        not 0 < args.cold_p95_limit_ms <= 50
        or not 0 < args.warm_p95_limit_ms <= 50
    ):
        raise CanaryConfigurationError(
            "public cold and warm latency gates must be within 50ms"
        )
    return BoundedApiSpec(
        snapshot_id=args.snapshot_id,
        npi=None,
        code_system=str(args.code_system).strip().upper(),
        code=str(args.code).strip(),
        limit=args.page_limit,
        expected_item_count=args.expected_item_count,
        expected_result_state="matched",
        cold_p95_limit_ms=args.cold_p95_limit_ms,
        warm_p95_limit_ms=args.warm_p95_limit_ms,
        minimum_cold_process_samples=args.minimum_cold_process_samples,
    )


def reference_spec(args: Any) -> BoundedApiSpec:
    """Build the fixed V3 oracle query without accepting latency settings."""

    if args.page_limit != 25 or args.expected_item_count != 25:
        raise CanaryConfigurationError(
            "frozen V3 reference must capture the exact 25-item public page"
        )
    if (
        str(args.code_system).strip().upper() != "CPT"
        or str(args.code).strip() != "70553"
    ):
        raise CanaryConfigurationError(
            "frozen V3 reference is fixed to CPT 70553"
        )
    return BoundedApiSpec(
        snapshot_id=str(args.snapshot_id).strip(),
        npi=None,
        code_system="CPT",
        code="70553",
        limit=25,
        expected_item_count=25,
        expected_result_state="matched",
        cold_p95_limit_ms=50,
        warm_p95_limit_ms=50,
        minimum_cold_process_samples=20,
    )


def graph_limits(_args: Any, *, fallback_mode: str) -> GraphReadLimits:
    """Build measurement-only graph checks for one public request scope."""

    return GraphReadLimits(
        database_bytes=None,
        database_blocks=None,
        logical_lookups=None,
        minimum_request_scopes=1,
        maximum_request_scopes=1,
        component_fallback_mode=fallback_mode,
    )


def api_parameters(
    args: Any,
    spec: BoundedApiSpec,
) -> dict[str, str]:
    """Return the fixed unfiltered CPT query and reject caller narrowing."""

    if args.api_param:
        raise CanaryConfigurationError(
            "fixed unfiltered CPT probe does not accept --api-param"
        )
    extras_by_name = parse_string_pairs(
        args.api_param,
        label="--api-param",
        protected_keys=PROTECTED_API_PARAMETERS,
    )
    parameters_by_name = spec.parameters(extras_by_name)
    if "npi" in parameters_by_name:
        raise CanaryConfigurationError(
            "public traversal probe must not send npi"
        )
    return parameters_by_name


async def _metric_wrapped_request(
    client: httpx.AsyncClient,
    *,
    context: _ApiProbeContext,
    graph_limits_by_field: GraphReadLimits,
) -> tuple[dict[str, Any], dict[str, Any]]:
    metrics_before = await get_metrics(
        client,
        context.metrics_url,
        headers=context.metrics_headers,
        maximum_bytes=context.maximum_bytes,
    )
    document_by_field, _elapsed_ms, api_identity_by_field = (
        await get_identity_json(
            client,
            f"{context.api_url}/api/v1/pricing/providers/by-procedure",
            headers=context.api_headers,
            parameters=context.parameters,
            maximum_bytes=context.maximum_bytes,
        )
    )
    metrics_after = await get_metrics(
        client,
        context.metrics_url,
        headers=context.metrics_headers,
        maximum_bytes=context.maximum_bytes,
    )
    graph_evidence_by_field = evaluate_graph_metric_delta(
        metrics_before,
        metrics_after,
        limits=graph_limits_by_field,
    )
    graph_evidence_by_field["runtime_identity"] = (
        require_same_runtime_identity(
            metrics_before.get("runtime_identity", {}),
            api_identity_by_field,
            metrics_after.get("runtime_identity", {}),
        )
    )
    return document_by_field, graph_evidence_by_field


async def _assert_warmup_process(
    client: httpx.AsyncClient,
    *,
    context: _ApiProbeContext,
    expected_identity_by_field: Mapping[str, Any],
    sample_count: int,
) -> None:
    for _sample_index in range(sample_count):
        _document, _elapsed_ms, identity_by_field = await get_identity_json(
            client,
            f"{context.api_url}/api/v1/pricing/providers/by-procedure",
            headers=context.api_headers,
            parameters=context.parameters,
            maximum_bytes=context.maximum_bytes,
        )
        require_same_runtime_identity(
            expected_identity_by_field,
            identity_by_field,
        )


async def _collect_warm_samples(
    client: httpx.AsyncClient,
    *,
    context: _ApiProbeContext,
    expected_identity_by_field: Mapping[str, Any],
    sample_count: int,
) -> tuple[list[dict[str, Any]], list[float]]:
    response_documents: list[dict[str, Any]] = []
    warm_samples_ms: list[float] = []
    for _sample_index in range(sample_count):
        document_by_field, elapsed_ms, identity_by_field = (
            await get_identity_json(
                client,
                f"{context.api_url}/api/v1/pricing/providers/by-procedure",
                headers=context.api_headers,
                parameters=context.parameters,
                maximum_bytes=context.maximum_bytes,
            )
        )
        require_same_runtime_identity(
            expected_identity_by_field,
            identity_by_field,
        )
        response_documents.append(document_by_field)
        warm_samples_ms.append(elapsed_ms)
    return response_documents, warm_samples_ms


async def _collect_warm_probe_evidence(
    args: Any,
    *,
    context: _ApiProbeContext,
    graph_limits_by_field: GraphReadLimits,
) -> _WarmProbeEvidence:
    timeout = httpx.Timeout(args.request_timeout_seconds)
    async with httpx.AsyncClient(timeout=timeout) as client:
        first_document, graph_evidence_by_field = (
            await _metric_wrapped_request(
                client,
                context=context,
                graph_limits_by_field=graph_limits_by_field,
            )
        )
        expected_identity_by_field = dict(
            graph_evidence_by_field["runtime_identity"]
        )
        await _assert_warmup_process(
            client,
            context=context,
            expected_identity_by_field=expected_identity_by_field,
            sample_count=args.warmup_samples,
        )
        warm_documents, warm_samples_ms = await _collect_warm_samples(
            client,
            context=context,
            expected_identity_by_field=expected_identity_by_field,
            sample_count=args.warm_samples,
        )
    return _WarmProbeEvidence(
        response_documents=[first_document, *warm_documents],
        warm_samples_ms=warm_samples_ms,
        graph_read_evidence=graph_evidence_by_field,
    )


def _load_valid_reference(reference_path: str) -> dict[str, Any]:
    reference_by_field = load_json(Path(reference_path))
    reference_failures = validate_reference_evidence(reference_by_field)
    if reference_failures:
        raise CanaryConfigurationError("; ".join(reference_failures))
    return reference_by_field


async def public_warm_probe(
    args: Any,
    cold_sample_evidence: list[Mapping[str, Any]],
    *,
    cold_not_before: str | None,
) -> dict[str, Any]:
    """Evaluate warm serving against fresh cold and frozen-reference evidence."""

    api_url, metrics_url, api_headers, metrics_headers = service_inputs(args)
    spec = api_spec(args)
    parameters_by_name = api_parameters(args, spec)
    reference_by_field = _load_valid_reference(args.reference_evidence)
    context = _ApiProbeContext(
        api_url=api_url,
        metrics_url=metrics_url,
        api_headers=api_headers,
        metrics_headers=metrics_headers,
        parameters=parameters_by_name,
        maximum_bytes=args.maximum_response_bytes,
    )
    warm_evidence = await _collect_warm_probe_evidence(
        args,
        context=context,
        graph_limits_by_field=graph_limits(
            args,
            fallback_mode="allowed",
        ),
    )
    return evaluate_api_probe(
        spec=spec,
        cold_sample_evidence=cold_sample_evidence,
        warm_samples_ms=warm_evidence.warm_samples_ms,
        response_documents=warm_evidence.response_documents,
        graph_read_evidence=warm_evidence.graph_read_evidence,
        semantic_reference=reference_by_field,
        expected_query_fingerprint=query_fingerprint(parameters_by_name),
        cold_not_before=cold_not_before,
    )
