# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Pure post-authentication contracts for billing-identity pricing search.

This module does not authenticate callers, resolve plans, inspect billing
identities, persist audit events, or enforce quotas. A trusted upstream adapter
must authenticate and authorize the caller before supplying the pseudonymous
claims accepted here. Route and storage wiring are intentionally absent.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
import hashlib
import hmac
import json
import re
from typing import Literal, Mapping

BILLING_SEARCH_ACCESS_CONTRACT = "healthporta.billing-search-access.v1"
BILLING_SEARCH_CAPABILITY = "pricing:billing-search"
BILLING_SEARCH_PROVENANCE_CAPABILITY = "pricing:billing-search:provenance"
BILLING_SEARCH_CACHE_CONTROL = "private, no-store"

_CONTEXT_DIGEST_DOMAIN = b"HEALTHPORTA_BILLING_SEARCH_ACCESS_CONTEXT_V1\x00"
_JOURNAL_DIGEST_DOMAIN = b"HEALTHPORTA_BILLING_SEARCH_ACCESS_JOURNAL_V1\x00"
_METER_DIGEST_DOMAIN = b"HEALTHPORTA_BILLING_SEARCH_ACCESS_METER_V1\x00"
_INVALID = "billing_search_access_invalid"
_DENIED = "billing_search_access_denied"
_REDACTED = "<redacted-billing-search-access>"
_MAX_VALIDITY_SECONDS = 300
_MAX_JOURNAL_DURATION_US = 60_000_000
_SHA256_RE = re.compile(r"[0-9a-f]{64}", flags=re.ASCII)
_UTC_RE = re.compile(
    r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z",
    flags=re.ASCII,
)
_CAPABILITY_SETS = (
    (BILLING_SEARCH_CAPABILITY,),
    (BILLING_SEARCH_CAPABILITY, BILLING_SEARCH_PROVENANCE_CAPABILITY),
)
_CONTEXT_FIELDS = frozenset(
    {
        "principal_scope_sha256",
        "tenant_scope_sha256",
        "plan_entitlement_sha256",
        "audit_scope_sha256",
        "quota_scope_sha256",
        "capabilities",
        "issued_at",
        "expires_at",
    }
)
_SELECTOR_KINDS = frozenset({"billing_entity_ref"})
_ACCESS_DECISIONS = frozenset({"authorized", "denied", "rate_limited", "unavailable"})


class BillingSearchAccessError(RuntimeError):
    """Base value-free contract failure."""


class BillingSearchAccessDenied(BillingSearchAccessError):
    """Value-free denial of a scoped billing search."""


def _fail() -> BillingSearchAccessError:
    return BillingSearchAccessError(_INVALID)


def _deny() -> BillingSearchAccessDenied:
    return BillingSearchAccessDenied(_DENIED)


def _strict_sha256(value: object) -> str:
    if (
        type(value) is not str
        or _SHA256_RE.fullmatch(value) is None
        or value == "0" * 64
    ):
        raise _fail()
    return value


def _canonical_utc(value: object) -> tuple[str, datetime]:
    if type(value) is not str or _UTC_RE.fullmatch(value) is None:
        raise _fail()
    try:
        parsed = datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(
            tzinfo=timezone.utc
        )
    except ValueError:
        raise _fail() from None
    return value, parsed


def _is_strict_bool(value: object) -> bool:
    if type(value) is not bool:
        raise _fail()
    return value


def _strict_choice(value: object, allowed: frozenset[str]) -> str:
    if type(value) is not str or value not in allowed:
        raise _fail()
    return value


def _digest_payload(domain: bytes, payload: Mapping[str, object]) -> str:
    encoded = json.dumps(
        payload,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("ascii")
    digest = hashlib.sha256()
    digest.update(domain)
    digest.update(len(encoded).to_bytes(8, "big"))
    digest.update(encoded)
    return digest.hexdigest()


def _normalized_claims(raw: object) -> dict[str, object]:
    if type(raw) is not dict:
        raise _fail()
    raw_keys = tuple(raw.keys())
    if (
        any(type(raw_key) is not str for raw_key in raw_keys)
        or frozenset(raw_keys) != _CONTEXT_FIELDS
    ):
        raise _fail()
    capabilities = raw.get("capabilities")
    if (
        type(capabilities) is not tuple
        or any(type(capability) is not str for capability in capabilities)
        or capabilities not in _CAPABILITY_SETS
    ):
        raise _fail()
    canonical_capabilities = next(
        candidate for candidate in _CAPABILITY_SETS if capabilities == candidate
    )
    issued_at, issued = _canonical_utc(raw.get("issued_at"))
    expires_at, expires = _canonical_utc(raw.get("expires_at"))
    validity_seconds = (expires - issued).total_seconds()
    if not 0 < validity_seconds <= _MAX_VALIDITY_SECONDS:
        raise _fail()
    return {
        "principal_scope_sha256": _strict_sha256(raw.get("principal_scope_sha256")),
        "tenant_scope_sha256": _strict_sha256(raw.get("tenant_scope_sha256")),
        "plan_entitlement_sha256": _strict_sha256(raw.get("plan_entitlement_sha256")),
        "audit_scope_sha256": _strict_sha256(raw.get("audit_scope_sha256")),
        "quota_scope_sha256": _strict_sha256(raw.get("quota_scope_sha256")),
        "capabilities": canonical_capabilities,
        "issued_at": issued_at,
        "expires_at": expires_at,
    }


def _context_payload(values: Mapping[str, object]) -> dict[str, object]:
    return {
        "contract": BILLING_SEARCH_ACCESS_CONTRACT,
        "authentication_capability": "none",
        "self_authorizing": False,
        **{field_name: values[field_name] for field_name in sorted(_CONTEXT_FIELDS)},
    }


def _context_sha256(values: Mapping[str, object]) -> str:
    return _digest_payload(_CONTEXT_DIGEST_DOMAIN, _context_payload(values))


class _RedactedImmutable:
    __slots__ = ()

    def __repr__(self) -> str:
        return _REDACTED

    __str__ = __repr__

    def __copy__(self):
        return self

    def __deepcopy__(self, memo):
        del memo
        return self

    def __reduce_ex__(self, protocol):
        del protocol
        raise _fail()


@dataclass(frozen=True, slots=True, repr=False)
class BillingSearchAuthorizationContext(_RedactedImmutable):
    """Non-authenticating receipt of an authorization performed upstream."""

    principal_scope_sha256: str
    tenant_scope_sha256: str
    plan_entitlement_sha256: str
    audit_scope_sha256: str
    quota_scope_sha256: str
    capabilities: tuple[str, ...]
    issued_at: str
    expires_at: str
    context_sha256: str
    contract: str = field(default=BILLING_SEARCH_ACCESS_CONTRACT, init=False)
    authentication_capability: Literal["none"] = field(default="none", init=False)
    self_authorizing: Literal[False] = field(default=False, init=False)

    def __post_init__(self) -> None:
        values = _context_values(self)
        supplied_digest = _strict_sha256(self.context_sha256)
        if (
            type(self.contract) is not str
            or self.contract != BILLING_SEARCH_ACCESS_CONTRACT
            or type(self.authentication_capability) is not str
            or self.authentication_capability != "none"
            or type(self.self_authorizing) is not bool
            or self.self_authorizing is not False
            or not hmac.compare_digest(supplied_digest, _context_sha256(values))
        ):
            raise _fail()


def _context_values(context: BillingSearchAuthorizationContext) -> dict[str, object]:
    return _normalized_claims(
        {field_name: getattr(context, field_name) for field_name in _CONTEXT_FIELDS}
    )


def _validated_context_values(context: object) -> dict[str, object]:
    if type(context) is not BillingSearchAuthorizationContext:
        raise _fail()
    values = _context_values(context)
    if (
        type(context.contract) is not str
        or context.contract != BILLING_SEARCH_ACCESS_CONTRACT
        or type(context.authentication_capability) is not str
        or context.authentication_capability != "none"
        or type(context.self_authorizing) is not bool
        or context.self_authorizing is not False
        or not hmac.compare_digest(
            _strict_sha256(context.context_sha256),
            _context_sha256(values),
        )
    ):
        raise _fail()
    return values


def _require_current(values: Mapping[str, object], trusted_now: object) -> None:
    _, now = _canonical_utc(trusted_now)
    _, issued = _canonical_utc(values["issued_at"])
    _, expires = _canonical_utc(values["expires_at"])
    if now < issued or now >= expires:
        raise _deny()


def build_billing_search_authorization_context(
    verified_claims: Mapping[str, object],
    *,
    trusted_now: str,
) -> BillingSearchAuthorizationContext:
    """Freeze already-verified pseudonymous claims; perform no authentication."""

    try:
        values = _normalized_claims(verified_claims)
        _require_current(values, trusted_now)
        return BillingSearchAuthorizationContext(
            **values,
            context_sha256=_context_sha256(values),
        )
    except BillingSearchAccessError:
        raise
    except Exception:
        raise _fail() from None


def validate_billing_search_authorization_context(
    context: object,
    *,
    trusted_now: str,
) -> BillingSearchAuthorizationContext:
    """Revalidate integrity and short validity without granting new authority."""

    try:
        values = _validated_context_values(context)
        _require_current(values, trusted_now)
        return context
    except BillingSearchAccessError:
        raise
    except Exception:
        raise _fail() from None


def require_billing_search_access(
    context: object,
    *,
    requested_plan_entitlement_sha256: str,
    detailed_provenance: bool,
    trusted_now: str,
) -> BillingSearchAuthorizationContext:
    """Require exact pseudonymous plan scope and the requested capability."""

    validated = validate_billing_search_authorization_context(
        context,
        trusted_now=trusted_now,
    )
    requested_scope = _strict_sha256(requested_plan_entitlement_sha256)
    needs_provenance = _is_strict_bool(detailed_provenance)
    plan_matches = hmac.compare_digest(
        validated.plan_entitlement_sha256,
        requested_scope,
    )
    has_provenance_access = (
        BILLING_SEARCH_PROVENANCE_CAPABILITY in validated.capabilities
    )
    if not plan_matches or (needs_provenance and not has_provenance_access):
        raise _deny()
    return validated


def billing_search_metering_key(
    context: object,
    *,
    trusted_now: str,
) -> str:
    """Return a domain-separated digest key; perform no quota operation."""

    validated = validate_billing_search_authorization_context(
        context,
        trusted_now=trusted_now,
    )
    digest = hashlib.sha256()
    digest.update(_METER_DIGEST_DOMAIN)
    digest.update(bytes.fromhex(validated.quota_scope_sha256))
    return f"bsm1_{digest.hexdigest()}"


def _journal_payload(values: Mapping[str, object]) -> dict[str, object]:
    return {
        "contract": BILLING_SEARCH_ACCESS_CONTRACT,
        "event": "billing_search_access",
        **values,
    }


@dataclass(frozen=True, slots=True, repr=False)
class BillingSearchAccessJournalSeed(_RedactedImmutable):
    """Closed seed containing no raw selector, plan, or generation identifier."""

    audit_scope_sha256: str
    authorization_context_sha256: str
    plan_entitlement_sha256: str
    generation_bundle_sha256: str
    request_shape_sha256: str
    selector_kind: Literal["billing_entity_ref"]
    decision: Literal["authorized", "denied", "rate_limited", "unavailable"]
    observed_at: str
    duration_us: int
    detailed_provenance: bool
    event_sha256: str
    contract: str = field(default=BILLING_SEARCH_ACCESS_CONTRACT, init=False)

    def __post_init__(self) -> None:
        values = _journal_values(self)
        if (
            type(self.contract) is not str
            or self.contract != BILLING_SEARCH_ACCESS_CONTRACT
            or not hmac.compare_digest(
                _strict_sha256(self.event_sha256),
                _digest_payload(_JOURNAL_DIGEST_DOMAIN, _journal_payload(values)),
            )
        ):
            raise _fail()


def _journal_values(seed: BillingSearchAccessJournalSeed) -> dict[str, object]:
    duration_us = seed.duration_us
    if type(duration_us) is not int or not 0 <= duration_us <= _MAX_JOURNAL_DURATION_US:
        raise _fail()
    observed_at, _ = _canonical_utc(seed.observed_at)
    return {
        "audit_scope_sha256": _strict_sha256(seed.audit_scope_sha256),
        "authorization_context_sha256": _strict_sha256(
            seed.authorization_context_sha256
        ),
        "plan_entitlement_sha256": _strict_sha256(seed.plan_entitlement_sha256),
        "generation_bundle_sha256": _strict_sha256(seed.generation_bundle_sha256),
        "request_shape_sha256": _strict_sha256(seed.request_shape_sha256),
        "selector_kind": _strict_choice(seed.selector_kind, _SELECTOR_KINDS),
        "decision": _strict_choice(seed.decision, _ACCESS_DECISIONS),
        "observed_at": observed_at,
        "duration_us": duration_us,
        "detailed_provenance": _is_strict_bool(seed.detailed_provenance),
    }


def build_billing_search_access_journal_seed(
    context: object,
    *,
    generation_bundle_sha256: str,
    request_shape_sha256: str,
    selector_kind: str,
    decision: str,
    trusted_observed_at: str,
    duration_us: int,
    detailed_provenance: bool,
) -> BillingSearchAccessJournalSeed:
    """Build a value-free event seed using a server-clock observation time."""

    try:
        validated_context = validate_billing_search_authorization_context(
            context,
            trusted_now=trusted_observed_at,
        )
        normalized_decision = _strict_choice(decision, _ACCESS_DECISIONS)
        is_detailed_provenance_requested = _is_strict_bool(detailed_provenance)
        if normalized_decision == "authorized":
            require_billing_search_access(
                validated_context,
                requested_plan_entitlement_sha256=(
                    validated_context.plan_entitlement_sha256
                ),
                detailed_provenance=is_detailed_provenance_requested,
                trusted_now=trusted_observed_at,
            )
        journal_fields_by_name = {
            "audit_scope_sha256": validated_context.audit_scope_sha256,
            "authorization_context_sha256": validated_context.context_sha256,
            "plan_entitlement_sha256": (validated_context.plan_entitlement_sha256),
            "generation_bundle_sha256": generation_bundle_sha256,
            "request_shape_sha256": request_shape_sha256,
            "selector_kind": selector_kind,
            "decision": normalized_decision,
            "observed_at": trusted_observed_at,
            "duration_us": duration_us,
            "detailed_provenance": is_detailed_provenance_requested,
        }
        provisional = object.__new__(BillingSearchAccessJournalSeed)
        for field_name, field_value in journal_fields_by_name.items():
            object.__setattr__(provisional, field_name, field_value)
        normalized = _journal_values(provisional)
        event_sha256 = _digest_payload(
            _JOURNAL_DIGEST_DOMAIN,
            _journal_payload(normalized),
        )
        return BillingSearchAccessJournalSeed(**normalized, event_sha256=event_sha256)
    except BillingSearchAccessError:
        raise
    except Exception:
        raise _fail() from None


def validate_billing_search_access_journal_seed(
    seed: object,
) -> BillingSearchAccessJournalSeed:
    """Revalidate one exact event seed without writing it anywhere."""

    try:
        if type(seed) is not BillingSearchAccessJournalSeed:
            raise _fail()
        values = _journal_values(seed)
        if (
            type(seed.contract) is not str
            or seed.contract != BILLING_SEARCH_ACCESS_CONTRACT
            or not hmac.compare_digest(
                _strict_sha256(seed.event_sha256),
                _digest_payload(_JOURNAL_DIGEST_DOMAIN, _journal_payload(values)),
            )
        ):
            raise _fail()
        return seed
    except BillingSearchAccessError:
        raise
    except Exception:
        raise _fail() from None


def billing_search_access_journal_record(seed: object) -> dict[str, object]:
    """Project the closed pseudonymous record; perform no journal write."""

    validated = validate_billing_search_access_journal_seed(seed)
    values = _journal_values(validated)
    return {
        **_journal_payload(values),
        "event_sha256": validated.event_sha256,
    }
