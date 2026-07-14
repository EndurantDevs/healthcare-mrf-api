"""Shared value objects for Provider Directory API evidence checks."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class SourceSelection:
    """One exact manifest source and its API-readiness requirement."""

    entry_id: str
    source_id: str
    classification: str
    required: bool
    resources: tuple[str, ...] = ()
    resource_profile: str = "NONE"
    matrix_checks: tuple[str, ...] = ()
    profile_fact_resources: tuple[str, ...] = ()


@dataclass(frozen=True)
class OverlaySample:
    """One bounded current overlay sample used for API verification."""

    source_id: str
    npi: int
    phone: str | None
    address_key: str | None = None
    latitude: float | None = None
    longitude: float | None = None


def normalized_coordinate(value: object) -> float | None:
    """Return a finite coordinate value, or ``None`` for unusable input."""
    try:
        coordinate = float(value)
    except (TypeError, ValueError):
        return None
    return coordinate if coordinate == coordinate else None


@dataclass(frozen=True)
class SourceProvenance:
    """Current public FHIR provenance expected for one selected source."""

    source_id: str
    endpoint_id: str
    dataset_id: str
    api_base: str
    org_name: str | None = None
    plan_name: str | None = None


@dataclass(frozen=True)
class SourceEvaluationContext:
    """Shared limits and probe outcomes for one source evaluation."""

    candidate_limit: int
    api_latency_slo_ms: float
    api_skip_reason: str | None = None
    witness_probe_error: str | None = None
    completion_probe_error: str | None = None
