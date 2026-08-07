# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Security and closure tests for the pure billing-search access contract."""

from __future__ import annotations

import copy
import hashlib
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
import pickle
import sys

import pytest

MODULE_PATH = Path(__file__).parents[1] / "api" / "billing_search_access_contract.py"
MODULE_SPEC = spec_from_file_location(
    "billing_search_access_contract_under_test", MODULE_PATH
)
assert MODULE_SPEC is not None and MODULE_SPEC.loader is not None
access = module_from_spec(MODULE_SPEC)
sys.modules[MODULE_SPEC.name] = access
MODULE_SPEC.loader.exec_module(access)


NOW = "2031-01-02T03:04:05Z"
SENSITIVE_SENTINEL = "SENSITIVE-SELECTOR-VALUE-MUST-NOT-ESCAPE"


def _sha256(label: str) -> str:
    return hashlib.sha256(label.encode("ascii")).hexdigest()


def _claims(*, provenance: bool = False) -> dict[str, object]:
    capabilities = (access.BILLING_SEARCH_CAPABILITY,)
    if provenance:
        capabilities += (access.BILLING_SEARCH_PROVENANCE_CAPABILITY,)
    return {
        "principal_scope_sha256": _sha256("synthetic-principal-scope"),
        "tenant_scope_sha256": _sha256("synthetic-tenant-scope"),
        "plan_entitlement_sha256": _sha256("synthetic-plan-entitlement"),
        "audit_scope_sha256": _sha256("synthetic-audit-scope"),
        "quota_scope_sha256": _sha256("synthetic-quota-scope"),
        "capabilities": capabilities,
        "issued_at": "2031-01-02T03:03:00Z",
        "expires_at": "2031-01-02T03:08:00Z",
    }


def _context(*, provenance: bool = False):
    return access.build_billing_search_authorization_context(
        _claims(provenance=provenance),
        trusted_now=NOW,
    )


def _journal(context=None, **overrides):
    journal_fields_by_name = {
        "generation_bundle_sha256": _sha256("sealed-generation-bundle"),
        "request_shape_sha256": _sha256("safe-request-shape"),
        "selector_kind": "billing_entity_ref",
        "decision": "authorized",
        "trusted_observed_at": NOW,
        "duration_us": 1234,
        "detailed_provenance": False,
    }
    journal_fields_by_name.update(overrides)
    return access.build_billing_search_access_journal_seed(
        context or _context(),
        **journal_fields_by_name,
    )


def _assert_value_free_error(callable_):
    with pytest.raises(access.BillingSearchAccessError) as error:
        callable_()
    assert str(error.value) in {
        "billing_search_access_invalid",
        "billing_search_access_denied",
    }
    assert SENSITIVE_SENTINEL not in str(error.value)


def test_contract_constants_are_fixed_and_source_neutral():
    assert access.BILLING_SEARCH_ACCESS_CONTRACT == (
        "healthporta.billing-search-access.v1"
    )
    assert access.BILLING_SEARCH_CAPABILITY == "pricing:billing-search"
    assert access.BILLING_SEARCH_PROVENANCE_CAPABILITY == (
        "pricing:billing-search:provenance"
    )
    assert access.BILLING_SEARCH_CACHE_CONTROL == "private, no-store"


def test_context_is_pseudonymous_redacted_and_current():
    context = _context()

    assert context.authentication_capability == "none"
    assert context.self_authorizing is False
    assert (
        access.validate_billing_search_authorization_context(
            context,
            trusted_now=NOW,
        )
        is context
    )
    assert repr(context) == "<redacted-billing-search-access>"
    assert copy.copy(context) is context
    assert copy.deepcopy(context) is context
    with pytest.raises(access.BillingSearchAccessError, match="^billing_search"):
        pickle.dumps(context)


def test_context_requires_exact_closed_claims_and_canonical_capabilities():
    missing = _claims()
    missing.pop("audit_scope_sha256")
    extra_claims_by_name = {**_claims(), "selector_value": SENSITIVE_SENTINEL}
    wrong_capabilities = (
        (access.BILLING_SEARCH_PROVENANCE_CAPABILITY,),
        [access.BILLING_SEARCH_CAPABILITY],
        (access.BILLING_SEARCH_CAPABILITY, "pricing:anything"),
    )

    for claims in (missing, extra_claims_by_name):
        _assert_value_free_error(
            lambda claims=claims: access.build_billing_search_authorization_context(
                claims,
                trusted_now=NOW,
            )
        )
    for capabilities in wrong_capabilities:
        claims_by_name = {**_claims(), "capabilities": capabilities}
        _assert_value_free_error(
            lambda claims_by_name=claims_by_name: access.build_billing_search_authorization_context(
                claims_by_name,
                trusted_now=NOW,
            )
        )


@pytest.mark.parametrize(
    "field,value",
    (
        ("principal_scope_sha256", None),
        ("tenant_scope_sha256", True),
        ("plan_entitlement_sha256", "A" * 64),
        ("audit_scope_sha256", "f" * 63),
        ("quota_scope_sha256", "0" * 64),
    ),
)
def test_context_rejects_noncanonical_pseudonymous_digests(field, value):
    claims_by_name = {**_claims(), field: value}
    _assert_value_free_error(
        lambda: access.build_billing_search_authorization_context(
            claims_by_name,
            trusted_now=NOW,
        )
    )


@pytest.mark.parametrize(
    "issued_at,expires_at,trusted_now",
    (
        ("2031-01-02T03:04:05Z", "2031-01-02T03:04:05Z", NOW),
        ("2031-01-02T03:04:06Z", "2031-01-02T03:05:00Z", NOW),
        ("2031-01-02T03:00:00Z", "2031-01-02T03:08:01Z", NOW),
        ("2031-01-02T03:00:00+00:00", "2031-01-02T03:04:30Z", NOW),
        ("2031-02-30T03:00:00Z", "2031-02-30T03:04:00Z", NOW),
        ("2031-01-02T03:00:00Z", "2031-01-02T03:04:05Z", NOW),
        ("2031-01-02T03:00:00Z", "2031-01-02T03:04:00Z", "not-a-time"),
    ),
)
def test_context_rejects_invalid_noncurrent_or_long_validity(
    issued_at,
    expires_at,
    trusted_now,
):
    claims_by_name = {
        **_claims(),
        "issued_at": issued_at,
        "expires_at": expires_at,
    }
    _assert_value_free_error(
        lambda: access.build_billing_search_authorization_context(
            claims_by_name,
            trusted_now=trusted_now,
        )
    )


def test_context_expires_and_rejects_wrong_types_on_revalidation():
    context = _context()

    with pytest.raises(
        access.BillingSearchAccessDenied,
        match="^billing_search_access_denied$",
    ):
        access.validate_billing_search_authorization_context(
            context,
            trusted_now=context.expires_at,
        )
    _assert_value_free_error(
        lambda: access.validate_billing_search_authorization_context(
            object(),
            trusted_now=NOW,
        )
    )


def test_context_digest_detects_field_and_fixed_state_tampering():
    context = _context()
    object.__setattr__(context, "quota_scope_sha256", _sha256("tampered-scope"))
    _assert_value_free_error(
        lambda: access.validate_billing_search_authorization_context(
            context,
            trusted_now=NOW,
        )
    )

    for field, value in (
        ("contract", "different-contract"),
        ("authentication_capability", "self-issued"),
        ("self_authorizing", True),
    ):
        context = _context()
        object.__setattr__(context, field, value)
        _assert_value_free_error(
            lambda context=context: access.validate_billing_search_authorization_context(
                context,
                trusted_now=NOW,
            )
        )


def test_exact_plan_scope_and_provenance_capability_are_required(monkeypatch):
    context = _context()
    compared_pairs = []
    original = access.hmac.compare_digest

    def compare(left, right):
        compared_pairs.append((left, right))
        return original(left, right)

    monkeypatch.setattr(access.hmac, "compare_digest", compare)
    assert (
        access.require_billing_search_access(
            context,
            requested_plan_entitlement_sha256=context.plan_entitlement_sha256,
            detailed_provenance=False,
            trusted_now=NOW,
        )
        is context
    )
    assert (
        context.plan_entitlement_sha256,
        context.plan_entitlement_sha256,
    ) in compared_pairs

    for plan_scope, provenance in (
        (_sha256("different-plan-scope"), False),
        (context.plan_entitlement_sha256, True),
    ):
        with pytest.raises(
            access.BillingSearchAccessDenied,
            match="^billing_search_access_denied$",
        ):
            access.require_billing_search_access(
                context,
                requested_plan_entitlement_sha256=plan_scope,
                detailed_provenance=provenance,
                trusted_now=NOW,
            )

    privileged = _context(provenance=True)
    assert (
        access.require_billing_search_access(
            privileged,
            requested_plan_entitlement_sha256=privileged.plan_entitlement_sha256,
            detailed_provenance=True,
            trusted_now=NOW,
        )
        is privileged
    )


def test_access_requirement_rejects_type_confusion_without_echo():
    context = _context()
    for plan_scope, provenance in (
        (SENSITIVE_SENTINEL, False),
        (context.plan_entitlement_sha256, 1),
    ):
        _assert_value_free_error(
            lambda plan_scope=plan_scope, provenance=provenance: (
                access.require_billing_search_access(
                    context,
                    requested_plan_entitlement_sha256=plan_scope,
                    detailed_provenance=provenance,
                    trusted_now=NOW,
                )
            )
        )


def test_metering_key_is_fixed_digest_only_and_scope_bound():
    context = _context()
    meter_key = access.billing_search_metering_key(context, trusted_now=NOW)

    assert meter_key.startswith("bsm1_")
    assert len(meter_key) == len("bsm1_") + 64
    assert meter_key.removeprefix("bsm1_").isalnum()
    assert context.quota_scope_sha256 not in meter_key
    assert context.tenant_scope_sha256 not in meter_key
    assert context.plan_entitlement_sha256 not in meter_key
    assert SENSITIVE_SENTINEL not in meter_key
    assert access.billing_search_metering_key(context, trusted_now=NOW) == meter_key

    changed = _claims()
    changed["quota_scope_sha256"] = _sha256("another-quota-scope")
    changed_context = access.build_billing_search_authorization_context(
        changed,
        trusted_now=NOW,
    )
    assert access.billing_search_metering_key(changed_context, trusted_now=NOW) != (
        meter_key
    )


def test_journal_record_is_closed_pseudonymous_and_deterministic():
    context = _context()
    seed = _journal(context)
    journal_record_by_field = access.billing_search_access_journal_record(seed)

    assert journal_record_by_field == {
        "contract": access.BILLING_SEARCH_ACCESS_CONTRACT,
        "event": "billing_search_access",
        "audit_scope_sha256": context.audit_scope_sha256,
        "authorization_context_sha256": context.context_sha256,
        "plan_entitlement_sha256": context.plan_entitlement_sha256,
        "generation_bundle_sha256": _sha256("sealed-generation-bundle"),
        "request_shape_sha256": _sha256("safe-request-shape"),
        "selector_kind": "billing_entity_ref",
        "decision": "authorized",
        "observed_at": NOW,
        "duration_us": 1234,
        "detailed_provenance": False,
        "event_sha256": seed.event_sha256,
    }
    assert _journal(context).event_sha256 == seed.event_sha256
    assert repr(seed) == "<redacted-billing-search-access>"
    assert copy.copy(seed) is seed
    assert copy.deepcopy(seed) is seed
    with pytest.raises(access.BillingSearchAccessError, match="^billing_search"):
        pickle.dumps(seed)

    serialized = repr(journal_record_by_field)
    for forbidden in (
        context.principal_scope_sha256,
        context.tenant_scope_sha256,
        context.quota_scope_sha256,
        SENSITIVE_SENTINEL,
    ):
        assert forbidden not in serialized


@pytest.mark.parametrize(
    "decision",
    ("authorized", "denied", "rate_limited", "unavailable"),
)
@pytest.mark.parametrize("detailed_provenance", (False, True))
def test_journal_accepts_only_fixed_low_cardinality_values(
    decision,
    detailed_provenance,
):
    context = _context(
        provenance=decision == "authorized" and detailed_provenance,
    )
    seed = _journal(
        context,
        selector_kind="billing_entity_ref",
        decision=decision,
        detailed_provenance=detailed_provenance,
    )
    assert access.validate_billing_search_access_journal_seed(seed) is seed


@pytest.mark.parametrize(
    "overrides",
    (
        {"request_shape_sha256": SENSITIVE_SENTINEL},
        {"generation_bundle_sha256": SENSITIVE_SENTINEL},
        {"selector_kind": "tax_identity"},
        {"selector_kind": SENSITIVE_SENTINEL},
        {"decision": SENSITIVE_SENTINEL},
        {"trusted_observed_at": SENSITIVE_SENTINEL},
        {"duration_us": True},
        {"duration_us": -1},
        {"duration_us": 60_000_001},
        {"detailed_provenance": 1},
    ),
)
def test_journal_rejects_sensitive_or_noncanonical_input_without_echo(overrides):
    _assert_value_free_error(lambda: _journal(**overrides))


def test_journal_revalidates_exact_type_digest_fields_and_contract():
    _assert_value_free_error(
        lambda: access.validate_billing_search_access_journal_seed(object())
    )

    for field, value in (
        ("authorization_context_sha256", _sha256("different-context")),
        ("decision", "denied"),
        ("event_sha256", _sha256("tampered-event")),
        ("contract", "different-contract"),
    ):
        seed = _journal()
        object.__setattr__(seed, field, value)
        _assert_value_free_error(
            lambda seed=seed: access.billing_search_access_journal_record(seed)
        )
    seed = _journal()
    object.__delattr__(seed, "decision")
    _assert_value_free_error(
        lambda: access.validate_billing_search_access_journal_seed(seed)
    )


def test_journal_builder_rejects_tampered_context_before_projection():
    context = _context()
    object.__setattr__(context, "audit_scope_sha256", _sha256("tampered-audit"))
    _assert_value_free_error(lambda: _journal(context))


def test_public_surfaces_never_contain_forbidden_selector_or_scope_values():
    context = _context(provenance=True)
    seed = _journal(context, selector_kind="billing_entity_ref")
    meter_key = access.billing_search_metering_key(context, trusted_now=NOW)
    public_text = " ".join(
        (
            repr(context),
            repr(seed),
            repr(access.billing_search_access_journal_record(seed)),
            meter_key,
        )
    )

    forbidden_values = {
        context.principal_scope_sha256,
        context.tenant_scope_sha256,
        context.quota_scope_sha256,
        SENSITIVE_SENTINEL,
    }
    for forbidden in forbidden_values:
        assert str(forbidden) not in public_text


def test_direct_construction_rejects_forged_integrity_digests():
    _assert_value_free_error(
        lambda: access.BillingSearchAuthorizationContext(
            **_claims(),
            context_sha256=_sha256("forged-context"),
        )
    )
    journal_values = access.billing_search_access_journal_record(_journal())
    for field_name in ("contract", "event", "event_sha256"):
        journal_values.pop(field_name)
    _assert_value_free_error(
        lambda: access.BillingSearchAccessJournalSeed(
            **journal_values,
            event_sha256=_sha256("forged-event"),
        )
    )


@pytest.mark.parametrize(
    "target,invoke",
    (
        (
            "_normalized_claims",
            lambda: access.build_billing_search_authorization_context(
                _claims(), trusted_now=NOW
            ),
        ),
        (
            "_validated_context_values",
            lambda: access.validate_billing_search_authorization_context(
                _context(), trusted_now=NOW
            ),
        ),
        ("validate_billing_search_authorization_context", lambda: _journal()),
    ),
)
def test_unexpected_internal_failures_are_value_free(monkeypatch, target, invoke):
    def raise_unexpected(*args, **kwargs):
        del args, kwargs
        raise ValueError(SENSITIVE_SENTINEL)

    monkeypatch.setattr(access, target, raise_unexpected)
    _assert_value_free_error(invoke)
