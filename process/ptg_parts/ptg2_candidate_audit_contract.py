# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Versioned contract constants for bounded PTG V3 candidate audits."""

from dataclasses import dataclass
from typing import Any, Mapping

PTG2_FAST_AUDIT_CONTRACT = "ptg2_v3_fast_source_witness_audit_v2"
PTG2_FAST_AUDIT_TOOL = "ptg2_v3_fast_source_witness_audit"
PTG2_FAST_AUDIT_TOOL_VERSION = "2.0.0"
PTG2_FAST_AUDIT_DEADLINE_SECONDS = 55.0


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
    "FastCandidateAuditError",
]
