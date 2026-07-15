# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Versioned contract constants for bounded PTG V3 candidate audits."""

from dataclasses import dataclass
from typing import Any, Mapping

PTG2_FAST_AUDIT_CONTRACT = "ptg2_v3_fast_source_witness_audit_v2"
PTG2_FAST_AUDIT_TOOL = "ptg2_v3_fast_source_witness_audit"
PTG2_FAST_AUDIT_TOOL_VERSION = "2.0.0"
PTG2_FAST_AUDIT_DEADLINE_SECONDS = 55.0
PTG2_PUBLIC_AUDIT_SAMPLE_FIELDS = (
    "contract",
    "format_version",
    "method",
    "sample_count",
    "maximum_rows",
    "sample_digest",
    "source_count",
    "occurrence_identity",
    "complete_population",
    "serving_multiplicity_semantics",
)


def public_audit_sample_projection(
    audit_sample: Mapping[str, Any],
) -> dict[str, Any]:
    """Return the stable audit fields exposed by the candidate API."""

    missing_fields = [
        field_name
        for field_name in PTG2_PUBLIC_AUDIT_SAMPLE_FIELDS
        if field_name not in audit_sample
    ]
    if missing_fields:
        raise ValueError(
            "audit sample is missing public field(s): " + ", ".join(missing_fields)
        )
    return {
        field_name: audit_sample[field_name]
        for field_name in PTG2_PUBLIC_AUDIT_SAMPLE_FIELDS
    }


@dataclass(frozen=True)
class FastAuditTarget:
    """Public identifiers and sealed metadata for one candidate audit."""

    snapshot_id: str
    source_key: str
    plan_id: str
    plan_market_type: str
    source_count: int
    source_set_digest: str
    audit_sample: Mapping[str, Any]
    provider_identifier_quarantine: Mapping[str, Any]


@dataclass(frozen=True)
class FastAuditHttpConfig:
    """Bounded HTTP runtime configuration for one candidate audit."""

    api_base_url: str
    headers: Mapping[str, str]
    verify_tls: bool
    transport_contract: str
    concurrency: int = 32
    deadline_seconds: float = PTG2_FAST_AUDIT_DEADLINE_SECONDS
    require_uvloop: bool = True


class FastCandidateAuditError(RuntimeError):
    """A fail-closed bounded-audit error with a report-safe reason."""

    def __init__(self, reason: str):
        super().__init__(reason)
        self.reason = reason


__all__ = [
    "FastAuditHttpConfig",
    "FastAuditTarget",
    "PTG2_FAST_AUDIT_CONTRACT",
    "PTG2_FAST_AUDIT_DEADLINE_SECONDS",
    "PTG2_FAST_AUDIT_TOOL",
    "PTG2_FAST_AUDIT_TOOL_VERSION",
    "PTG2_PUBLIC_AUDIT_SAMPLE_FIELDS",
    "FastCandidateAuditError",
    "public_audit_sample_projection",
]
