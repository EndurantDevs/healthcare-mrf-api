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


@dataclass(frozen=True)
class OverlaySample:
    """One bounded current overlay sample used for API verification."""

    source_id: str
    npi: int
    phone: str | None


@dataclass(frozen=True)
class SourceEvaluationContext:
    """Shared limits and probe outcomes for one source evaluation."""

    candidate_limit: int
    api_latency_slo_ms: float
    api_skip_reason: str | None = None
    witness_probe_error: str | None = None
    completion_probe_error: str | None = None
