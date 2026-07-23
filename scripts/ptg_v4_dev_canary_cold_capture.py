"""Fresh-process public sample capture for the PTG V4 dev canary."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

import httpx

from scripts.ptg_v4_dev_canary_api import (
    api_parameters,
    api_spec,
    graph_limits,
    query_fingerprint,
    require_same_runtime_identity,
    service_inputs,
)
from scripts.ptg_v4_dev_canary_io import (
    CanaryInfrastructureError,
    get_identity_json,
    get_metrics,
    load_json,
    load_optional_json,
    utc_now_text,
    write_json,
)
from scripts.ptg_v4_dev_canary_reference import (
    compare_v4_document_to_reference,
    validate_reference_evidence,
)
from scripts.ptg_v4_dev_canary_support import (
    CanaryConfigurationError,
    evaluate_graph_metric_delta,
    validate_api_document,
)


COLD_EVIDENCE_CONTRACT = "ptg_v4_cold_process_samples_v2"


@dataclass(frozen=True)
class _ColdHttpEvidence:
    """Metric snapshots, API document, latency, and process identity."""

    metrics_before: dict[str, Any]
    document_by_field: dict[str, Any]
    elapsed_ms: float
    api_identity_by_field: dict[str, Any]
    metrics_after: dict[str, Any]


def _load_valid_reference(reference_path: str) -> dict[str, Any]:
    reference_by_field = load_json(Path(reference_path))
    reference_failures = validate_reference_evidence(reference_by_field)
    if reference_failures:
        raise CanaryConfigurationError("; ".join(reference_failures))
    return reference_by_field


async def _collect_cold_http_evidence(
    args: Any,
    api_url: str,
    metrics_url: str,
    api_headers: Mapping[str, str],
    metrics_headers: Mapping[str, str],
    parameters_by_name: Mapping[str, str],
) -> _ColdHttpEvidence:
    """Collect one identity-bound first request and its metric snapshots."""

    timeout = httpx.Timeout(args.request_timeout_seconds)
    async with httpx.AsyncClient(timeout=timeout) as client:
        metrics_before = await get_metrics(
            client,
            metrics_url,
            headers=metrics_headers,
            maximum_bytes=args.maximum_response_bytes,
        )
        document_by_field, elapsed_ms, api_identity_by_field = (
            await get_identity_json(
                client,
                f"{api_url}/api/v1/pricing/providers/by-procedure",
                headers=api_headers,
                parameters=parameters_by_name,
                maximum_bytes=args.maximum_response_bytes,
            )
        )
        metrics_after = await get_metrics(
            client,
            metrics_url,
            headers=metrics_headers,
            maximum_bytes=args.maximum_response_bytes,
        )
    return _ColdHttpEvidence(
        metrics_before=metrics_before,
        document_by_field=document_by_field,
        elapsed_ms=elapsed_ms,
        api_identity_by_field=api_identity_by_field,
        metrics_after=metrics_after,
    )


def _cold_graph_evidence(
    args: Any,
    http_evidence: _ColdHttpEvidence,
) -> dict[str, Any]:
    """Bind graph deltas to the same API process that served the sample."""

    graph_evidence_by_field = evaluate_graph_metric_delta(
        http_evidence.metrics_before,
        http_evidence.metrics_after,
        limits=graph_limits(
            args,
            fallback_mode=args.component_fallback_mode,
        ),
    )
    graph_evidence_by_field["runtime_identity"] = (
        require_same_runtime_identity(
            http_evidence.metrics_before.get("runtime_identity", {}),
            http_evidence.api_identity_by_field,
            http_evidence.metrics_after.get("runtime_identity", {}),
        )
    )
    return graph_evidence_by_field


def _require_valid_cold_sample(
    sample_by_field: Mapping[str, Any],
) -> None:
    """Reject invalid first-request, semantic, or graph evidence."""

    if (
        not sample_by_field["first_v4_request"]
        or not sample_by_field["document_valid"]
    ):
        raise CanaryInfrastructureError(
            "cold sample was not a valid first V4 request"
        )
    graph_evidence_by_field = sample_by_field.get("graph_read_evidence")
    if (
        not isinstance(graph_evidence_by_field, Mapping)
        or graph_evidence_by_field.get("passed") is not True
    ):
        raise CanaryInfrastructureError(
            "cold sample exceeded graph read bounds"
        )


def _shape_cold_sample(
    args: Any,
    spec: Any,
    parameters_by_name: Mapping[str, str],
    reference_by_field: Mapping[str, Any],
    http_evidence: _ColdHttpEvidence,
) -> dict[str, Any]:
    """Validate one first request and shape its persisted evidence row."""

    graph_evidence_by_field = _cold_graph_evidence(args, http_evidence)
    semantic_failures, semantic_page_by_field = (
        compare_v4_document_to_reference(
            http_evidence.document_by_field,
            spec,
            reference_by_field,
        )
    )
    runtime_identity_by_field = graph_evidence_by_field["runtime_identity"]
    sample_by_field = {
        "process_identity": runtime_identity_by_field["process_identity"],
        "process_started_at": runtime_identity_by_field[
            "process_started_at"
        ],
        "image_identity": runtime_identity_by_field["image_identity"],
        "snapshot_id": spec.snapshot_id,
        "query_fingerprint": query_fingerprint(parameters_by_name),
        "captured_at": utc_now_text(),
        "latency_ms": round(http_evidence.elapsed_ms, 3),
        "first_v4_request": (
            http_evidence.metrics_before["request_count"] == 0
        ),
        "document_valid": (
            not validate_api_document(
                http_evidence.document_by_field,
                spec,
            )
            and not semantic_failures
        ),
        "reference_snapshot_id": reference_by_field.get(
            "reference_snapshot_id"
        ),
        "semantic_page_digest": semantic_page_by_field["page_digest"],
        "graph_read_evidence": graph_evidence_by_field,
    }
    _require_valid_cold_sample(sample_by_field)
    return sample_by_field


def _persist_cold_sample(
    output_path: str,
    case_name: str,
    sample_by_field: Mapping[str, Any],
) -> None:
    """Append one validated sample to the compatible evidence contract."""

    evidence_path = Path(output_path)
    evidence_by_field = load_optional_json(evidence_path) or {
        "contract": COLD_EVIDENCE_CONTRACT,
        "cases": {},
    }
    if evidence_by_field.get("contract") != COLD_EVIDENCE_CONTRACT:
        raise CanaryConfigurationError(
            "cold evidence file has an incompatible contract"
        )
    evidence_by_field.setdefault("cases", {}).setdefault(
        case_name,
        [],
    ).append(dict(sample_by_field))
    write_json(evidence_path, evidence_by_field)


async def record_cold_sample(args: Any) -> dict[str, Any]:
    """Collect, validate, and persist one fresh-process public sample."""

    api_url, metrics_url, api_headers, metrics_headers = service_inputs(args)
    spec = api_spec(args)
    parameters_by_name = api_parameters(args, spec)
    reference_by_field = _load_valid_reference(args.reference_evidence)
    http_evidence = await _collect_cold_http_evidence(
        args,
        api_url,
        metrics_url,
        api_headers,
        metrics_headers,
        parameters_by_name,
    )
    sample_by_field = _shape_cold_sample(
        args,
        spec,
        parameters_by_name,
        reference_by_field,
        http_evidence,
    )
    _persist_cold_sample(args.output, args.case_name, sample_by_field)
    return sample_by_field
