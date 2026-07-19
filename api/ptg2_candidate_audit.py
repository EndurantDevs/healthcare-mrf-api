# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Request-scoped authorization for reading one validated PTG V3 candidate."""

from __future__ import annotations

import hmac
from dataclasses import dataclass
from typing import Any, Mapping

from sanic.exceptions import Forbidden

from api.control_auth import require_control_auth


PTG2_CANDIDATE_AUDIT_HEADER = "X-HealthPorta-PTG-Candidate-Audit"
PTG2_CANDIDATE_AUDIT_ACCESS_ARG = "_ptg2_candidate_audit_access"


def _normalized(value: Any, *, lower: bool = False) -> str:
    result = str(value or "").strip()
    return result.lower() if lower else result


@dataclass(frozen=True)
class PTG2CandidateAuditAccess:
    """Trusted capability created only after control-token authentication."""

    snapshot_id: str
    source_key: str
    plan_id: str
    plan_market_type: str

    def matches(
        self,
        *,
        snapshot_id: Any,
        source_key: Any,
        plan_id: Any,
        plan_market_type: Any,
    ) -> bool:
        """Return whether all normalized coordinates match this capability."""

        return (
            hmac.compare_digest(self.snapshot_id, _normalized(snapshot_id))
            and hmac.compare_digest(
                self.source_key,
                _normalized(source_key, lower=True),
            )
            and hmac.compare_digest(self.plan_id, _normalized(plan_id))
            and hmac.compare_digest(
                self.plan_market_type,
                _normalized(plan_market_type, lower=True),
            )
        )


def candidate_audit_access_from_request(
    request,
    args: Mapping[str, Any],
) -> PTG2CandidateAuditAccess | None:
    """Create an exact candidate capability only for an authenticated request."""

    headers = getattr(request, "headers", {}) or {}
    requested_candidate = _normalized(headers.get(PTG2_CANDIDATE_AUDIT_HEADER))
    if not requested_candidate:
        return None
    require_control_auth(request)
    return candidate_audit_access_from_verified_request(request, args)


def candidate_audit_access_from_verified_request(
    request,
    args: Mapping[str, Any],
) -> PTG2CandidateAuditAccess | None:
    """Create a capability after the caller verified control authentication."""

    headers = getattr(request, "headers", {}) or {}
    requested_candidate = _normalized(headers.get(PTG2_CANDIDATE_AUDIT_HEADER))
    if not requested_candidate:
        return None
    snapshot_id = _normalized(args.get("snapshot_id"))
    source_key = _normalized(args.get("source_key"), lower=True)
    plan_id = _normalized(args.get("plan_id") or args.get("plan_external_id"))
    plan_market_type = _normalized(
        args.get("plan_market_type") or args.get("market_type"),
        lower=True,
    )
    if not snapshot_id or not source_key or not plan_id or not plan_market_type:
        raise Forbidden("candidate audit access is invalid")
    if not hmac.compare_digest(requested_candidate, snapshot_id):
        raise Forbidden("candidate audit access is invalid")
    return PTG2CandidateAuditAccess(
        snapshot_id=snapshot_id,
        source_key=source_key,
        plan_id=plan_id,
        plan_market_type=plan_market_type,
    )


def candidate_audit_access_from_args(
    args: Mapping[str, Any],
) -> PTG2CandidateAuditAccess | None:
    """Return only a trusted in-process capability, never a user scalar."""

    candidate = args.get(PTG2_CANDIDATE_AUDIT_ACCESS_ARG)
    return candidate if isinstance(candidate, PTG2CandidateAuditAccess) else None


def attach_candidate_audit_access(
    request,
    args: dict[str, Any],
) -> dict[str, Any]:
    """Attach trusted candidate access to an internal argument mapping."""

    access = candidate_audit_access_from_request(request, args)
    if access is not None:
        args[PTG2_CANDIDATE_AUDIT_ACCESS_ARG] = access
    return args
