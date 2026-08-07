# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Value-free access journals for authenticated billing-search outcomes."""

from __future__ import annotations

import hashlib
import json
import math
import time
from typing import Literal

from api.billing_search_access_contract import (
    billing_search_access_journal_record,
    build_billing_search_access_journal_seed,
)
from api.billing_search_post_endpoint_access import (
    BillingSearchPostEndpointAccess,
    validate_billing_search_post_endpoint_access,
)

_NO_GENERATION_DOMAIN = b"HEALTHPORTA_BILLING_SEARCH_NO_GENERATION_V1\x00"
_FAILURE_DECISIONS = frozenset({"denied", "unavailable"})
_MAX_DURATION_US = 60_000_000
_INVALID = "billing_search_post_endpoint_journal_invalid"


class BillingSearchPostEndpointJournalError(RuntimeError):
    """Value-free failure to construct an endpoint access journal."""


def _fail() -> BillingSearchPostEndpointJournalError:
    return BillingSearchPostEndpointJournalError(_INVALID)


def _no_generation_digest(access: BillingSearchPostEndpointAccess) -> str:
    payload = json.dumps(
        {
            "plan_entitlement_sha256": (
                access.authorization_context.plan_entitlement_sha256
            ),
            "request_shape_sha256": access.request.request_shape_sha256,
        },
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("ascii")
    digest = hashlib.sha256()
    digest.update(_NO_GENERATION_DOMAIN)
    digest.update(len(payload).to_bytes(8, "big"))
    digest.update(payload)
    return digest.hexdigest()


def _bounded_duration_us(started_at: object) -> int:
    if type(started_at) is not float:
        raise _fail()
    elapsed_seconds = time.perf_counter() - started_at
    if not math.isfinite(elapsed_seconds):
        raise _fail()
    bounded_seconds = min(
        max(elapsed_seconds, 0.0),
        _MAX_DURATION_US / 1_000_000,
    )
    return int(bounded_seconds * 1_000_000)


def _journal_record(
    access: object,
    *,
    decision: str,
    generation_bundle_sha256: str | None,
    trusted_observed_at: str,
    started_at: float,
) -> dict[str, object]:
    validated_access = validate_billing_search_post_endpoint_access(access)
    generation_digest = (
        _no_generation_digest(validated_access)
        if generation_bundle_sha256 is None
        else generation_bundle_sha256
    )
    seed = build_billing_search_access_journal_seed(
        validated_access.authorization_context,
        generation_bundle_sha256=generation_digest,
        request_shape_sha256=validated_access.request.request_shape_sha256,
        selector_kind=validated_access.request.selector_kind,
        decision=decision,
        trusted_observed_at=trusted_observed_at,
        duration_us=_bounded_duration_us(started_at),
        detailed_provenance=validated_access.request.include_evidence,
    )
    return billing_search_access_journal_record(seed)


def billing_search_post_success_journal(
    access: object,
    *,
    generation_bundle_sha256: str | None,
    trusted_observed_at: str,
    started_at: float,
) -> dict[str, object]:
    """Project one closed authorized decision for endpoint logging."""

    try:
        return _journal_record(
            access,
            decision="authorized",
            generation_bundle_sha256=generation_bundle_sha256,
            trusted_observed_at=trusted_observed_at,
            started_at=started_at,
        )
    except BillingSearchPostEndpointJournalError:
        raise
    except Exception:
        raise _fail() from None


def billing_search_post_failure_journal(
    access: object,
    *,
    decision: Literal["denied", "unavailable"],
    trusted_observed_at: str,
    started_at: float,
) -> dict[str, object]:
    """Project one closed post-authentication failure decision for logging."""

    try:
        if type(decision) is not str or decision not in _FAILURE_DECISIONS:
            raise _fail()
        return _journal_record(
            access,
            decision=decision,
            generation_bundle_sha256=None,
            trusted_observed_at=trusted_observed_at,
            started_at=started_at,
        )
    except BillingSearchPostEndpointJournalError:
        raise
    except Exception:
        raise _fail() from None


__all__ = [
    "BillingSearchPostEndpointJournalError",
    "billing_search_post_failure_journal",
    "billing_search_post_success_journal",
]
