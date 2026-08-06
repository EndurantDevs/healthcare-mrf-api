# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Authorization-binding tests for billing-search journal seeds."""

from __future__ import annotations

import hashlib
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
import sys

import pytest

MODULE_PATH = Path(__file__).parents[1] / "api" / "billing_search_access_contract.py"
MODULE_SPEC = spec_from_file_location(
    "billing_search_journal_authorization_under_test",
    MODULE_PATH,
)
assert MODULE_SPEC is not None and MODULE_SPEC.loader is not None
access = module_from_spec(MODULE_SPEC)
sys.modules[MODULE_SPEC.name] = access
MODULE_SPEC.loader.exec_module(access)

NOW = "2031-01-02T03:04:05Z"


def _sha256(label: str) -> str:
    return hashlib.sha256(label.encode("ascii")).hexdigest()


def _claims() -> dict[str, object]:
    return {
        "principal_scope_sha256": _sha256("synthetic-principal-scope"),
        "tenant_scope_sha256": _sha256("synthetic-tenant-scope"),
        "plan_entitlement_sha256": _sha256("synthetic-plan-entitlement"),
        "audit_scope_sha256": _sha256("synthetic-audit-scope"),
        "quota_scope_sha256": _sha256("synthetic-quota-scope"),
        "capabilities": (access.BILLING_SEARCH_CAPABILITY,),
        "issued_at": "2031-01-02T03:03:00Z",
        "expires_at": "2031-01-02T03:08:00Z",
    }


def _context():
    return access.build_billing_search_authorization_context(
        _claims(),
        trusted_now=NOW,
    )


def test_capability_subclasses_cannot_forge_provenance_authority():
    class AlwaysEqualCapability(str):
        def __eq__(self, other):
            del other
            return True

        __hash__ = str.__hash__

    claims_by_name = _claims()
    claims_by_name["capabilities"] = (AlwaysEqualCapability("unprivileged"),)
    with pytest.raises(
        access.BillingSearchAccessError,
        match="^billing_search_access_invalid$",
    ):
        access.build_billing_search_authorization_context(
            claims_by_name,
            trusted_now=NOW,
        )


def test_claim_key_subclasses_cannot_inject_error_text():
    sentinel = "SENSITIVE-MAGIC-METHOD-ECHO"

    class RaisingKey(str):
        def __eq__(self, other):
            del other
            raise access.BillingSearchAccessError(sentinel)

        __hash__ = str.__hash__

    claims_by_name = _claims()
    principal_scope = claims_by_name.pop("principal_scope_sha256")
    claims_by_name[RaisingKey("principal_scope_sha256")] = principal_scope

    with pytest.raises(access.BillingSearchAccessError) as error:
        access.build_billing_search_authorization_context(
            claims_by_name,
            trusted_now=NOW,
        )
    assert str(error.value) == "billing_search_access_invalid"
    assert sentinel not in str(error.value)

    with pytest.raises(
        access.BillingSearchAccessError,
        match="^billing_search_access_invalid$",
    ):
        access.build_billing_search_authorization_context(
            tuple(claims_by_name.items()),
            trusted_now=NOW,
        )


def _journal(context, *, decision: str, detailed_provenance: bool, observed_at=NOW):
    return access.build_billing_search_access_journal_seed(
        context,
        generation_bundle_sha256=_sha256("sealed-generation-bundle"),
        request_shape_sha256=_sha256("safe-request-shape"),
        selector_kind="tax_identity",
        decision=decision,
        trusted_observed_at=observed_at,
        duration_us=1234,
        detailed_provenance=detailed_provenance,
    )


def test_authorized_journal_requires_requested_provenance_capability():
    context = _context()

    with pytest.raises(
        access.BillingSearchAccessDenied,
        match="^billing_search_access_denied$",
    ):
        _journal(context, decision="authorized", detailed_provenance=True)

    denied = _journal(context, decision="denied", detailed_provenance=True)
    assert denied.decision == "denied"
    assert denied.detailed_provenance is True


def test_journal_builder_rejects_expired_context_at_observation_time():
    context = _context()

    with pytest.raises(
        access.BillingSearchAccessDenied,
        match="^billing_search_access_denied$",
    ):
        _journal(
            context,
            decision="authorized",
            detailed_provenance=False,
            observed_at=context.expires_at,
        )
