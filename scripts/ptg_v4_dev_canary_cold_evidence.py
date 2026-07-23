"""Persisted cold-process evidence validation for PTG V4 canaries."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Mapping, Sequence


@dataclass(frozen=True)
class ColdEvidenceExpectation:
    """Identity and freshness bounds for one persisted cold sample set."""

    minimum_sample_count: int
    semantic_page_digest: str | None = None
    reference_snapshot_id: str | None = None
    v4_snapshot_id: str | None = None
    query_fingerprint: str | None = None
    image_identity: str | None = None
    not_before: str | None = None


def validated_cold_latencies(
    sample_rows: Sequence[Mapping[str, Any]],
    failures: list[str],
    expectation: ColdEvidenceExpectation,
) -> list[float]:
    """Return eligible latencies while appending every evidence failure."""

    latency_samples_ms: list[float] = []
    process_identities: list[str] = []
    for sample_by_field in sample_rows:
        _append_identity_binding_failures(
            sample_by_field,
            expectation,
            failures,
        )
        process_identity = str(
            sample_by_field.get("process_identity") or ""
        ).strip()
        process_started_at = str(
            sample_by_field.get("process_started_at") or ""
        ).strip()
        latency_ms = _sample_latency_ms(sample_by_field)
        if not _has_valid_sample_contract(
            sample_by_field,
            process_identity,
            process_started_at,
            latency_ms,
        ):
            failures.append(
                "cold sample lacks valid fresh-process response and graph evidence"
            )
            continue
        process_identities.append(process_identity)
        latency_samples_ms.append(latency_ms)
    _append_process_count_failures(
        process_identities,
        expectation.minimum_sample_count,
        failures,
    )
    if not latency_samples_ms:
        raise ValueError("cold process evidence cannot be empty")
    return latency_samples_ms


def _append_identity_binding_failures(
    sample_by_field: Mapping[str, Any],
    expectation: ColdEvidenceExpectation,
    failures: list[str],
) -> None:
    expected_bindings = (
        (
            expectation.semantic_page_digest,
            sample_by_field.get("semantic_page_digest"),
            "persisted cold response differs from frozen V3 semantic page",
        ),
        (
            expectation.reference_snapshot_id,
            sample_by_field.get("reference_snapshot_id"),
            "persisted cold response uses a different V3 reference snapshot",
        ),
        (
            expectation.v4_snapshot_id,
            sample_by_field.get("snapshot_id"),
            "persisted cold response uses a different V4 snapshot",
        ),
        (
            expectation.query_fingerprint,
            sample_by_field.get("query_fingerprint"),
            "persisted cold response uses a different query",
        ),
        (
            expectation.image_identity,
            sample_by_field.get("image_identity"),
            "persisted cold response uses a different V4 image",
        ),
    )
    failures.extend(
        failure_message
        for expected_value, observed_value, failure_message in expected_bindings
        if expected_value is not None and observed_value != expected_value
    )
    if not _is_valid_cold_capture_time(
        sample_by_field,
        not_before=expectation.not_before,
    ):
        failures.append(
            "cold sample capture time is stale or precedes its API process"
        )
    if not _has_matching_graph_identity(sample_by_field):
        failures.append(
            "cold sample API and metrics runtime identities differ"
        )


def _has_matching_graph_identity(
    sample_by_field: Mapping[str, Any],
) -> bool:
    graph_evidence_by_field = _mapping(
        sample_by_field.get("graph_read_evidence")
    )
    runtime_identity_by_field = _mapping(
        graph_evidence_by_field.get("runtime_identity")
    )
    identity_field_names = (
        "process_identity",
        "process_started_at",
        "image_identity",
    )
    return bool(
        all(
            str(runtime_identity_by_field.get(field_name) or "").strip()
            for field_name in identity_field_names
        )
        and all(
            runtime_identity_by_field.get(field_name)
            == sample_by_field.get(field_name)
            for field_name in identity_field_names
        )
    )


def _has_valid_sample_contract(
    sample_by_field: Mapping[str, Any],
    process_identity: str,
    process_started_at: str,
    latency_ms: float,
) -> bool:
    graph_evidence_by_field = _mapping(
        sample_by_field.get("graph_read_evidence")
    )
    return bool(
        process_identity
        and process_started_at
        and sample_by_field.get("first_v4_request") is True
        and sample_by_field.get("document_valid") is True
        and graph_evidence_by_field.get("passed") is True
        and _has_matching_graph_identity(sample_by_field)
        and latency_ms >= 0
    )


def _sample_latency_ms(sample_by_field: Mapping[str, Any]) -> float:
    try:
        return float(sample_by_field.get("latency_ms"))
    except (TypeError, ValueError):
        return -1


def _append_process_count_failures(
    process_identities: Sequence[str],
    minimum_sample_count: int,
    failures: list[str],
) -> None:
    distinct_process_identities = set(process_identities)
    if len(process_identities) != len(distinct_process_identities):
        failures.append("cold samples reused an API process identity")
    if len(distinct_process_identities) < minimum_sample_count:
        failures.append(
            "cold p95 lacks the required unique fresh-process sample count"
        )


def _is_valid_cold_capture_time(
    sample_by_field: Mapping[str, Any],
    *,
    not_before: str | None,
) -> bool:
    try:
        captured_at = _aware_datetime(sample_by_field.get("captured_at"))
        process_started_at = _aware_datetime(
            sample_by_field.get("process_started_at")
        )
        minimum_time = _aware_datetime(not_before) if not_before else None
    except (TypeError, ValueError):
        return (
            not_before is None
            and sample_by_field.get("captured_at") is None
        )
    return bool(
        process_started_at <= captured_at
        and (minimum_time is None or captured_at >= minimum_time)
    )


def _aware_datetime(value: Any) -> datetime:
    if not isinstance(value, str) or not value.strip():
        raise ValueError("timestamp is missing")
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return (
        parsed.replace(tzinfo=timezone.utc)
        if parsed.tzinfo is None
        else parsed.astimezone(timezone.utc)
    )


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}
