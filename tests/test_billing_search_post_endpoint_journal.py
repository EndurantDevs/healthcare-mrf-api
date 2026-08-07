# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Closure tests for authenticated billing-search endpoint journals."""

from __future__ import annotations

import hashlib
from types import SimpleNamespace

import pytest

from api.billing_search_access_contract import (
    BILLING_SEARCH_ACCESS_CONTRACT,
    BILLING_SEARCH_CAPABILITY,
    build_billing_search_authorization_context,
)
from api import billing_search_post_endpoint_journal as endpoint_journal

NOW = "2031-01-02T03:04:05Z"
SENSITIVE_SELECTOR = "SENSITIVE-BILLING-SELECTOR-MUST-NOT-ESCAPE"


def _sha256(label: str) -> str:
    return hashlib.sha256(label.encode("ascii")).hexdigest()


def _access(*, selector_kind: str = "tax_identity") -> SimpleNamespace:
    authorization_context = build_billing_search_authorization_context(
        {
            "principal_scope_sha256": _sha256("synthetic-principal"),
            "tenant_scope_sha256": _sha256("synthetic-tenant"),
            "plan_entitlement_sha256": _sha256("synthetic-entitlement"),
            "audit_scope_sha256": _sha256("synthetic-audit"),
            "quota_scope_sha256": _sha256("synthetic-quota"),
            "capabilities": (BILLING_SEARCH_CAPABILITY,),
            "issued_at": "2031-01-02T03:03:00Z",
            "expires_at": "2031-01-02T03:08:00Z",
        },
        trusted_now=NOW,
    )
    request = SimpleNamespace(
        request_shape_sha256=_sha256("synthetic-request-shape"),
        selector_kind=selector_kind,
        include_evidence=False,
        selector_value=SENSITIVE_SELECTOR,
    )
    return SimpleNamespace(
        authorization_context=authorization_context,
        request=request,
        internal_release_id="hprelease_sensitive_internal_value",
    )


@pytest.mark.parametrize("decision", ("denied", "unavailable"))
@pytest.mark.parametrize("selector_kind", ("tax_identity", "billing_entity_ref"))
def test_failure_journal_is_closed_value_free_and_bounded(
    monkeypatch,
    decision: str,
    selector_kind: str,
) -> None:
    access = _access(selector_kind=selector_kind)
    monkeypatch.setattr(
        endpoint_journal,
        "validate_billing_search_post_endpoint_access",
        lambda candidate: candidate,
    )
    monkeypatch.setattr(endpoint_journal.time, "perf_counter", lambda: 101.25)

    journal_record = endpoint_journal.billing_search_post_failure_journal(
        access,
        decision=decision,
        trusted_observed_at=NOW,
        started_at=100.0,
    )

    assert journal_record["contract"] == BILLING_SEARCH_ACCESS_CONTRACT
    assert journal_record["event"] == "billing_search_access"
    assert journal_record["decision"] == decision
    assert journal_record["selector_kind"] == selector_kind
    assert journal_record["duration_us"] == 1_250_000
    assert set(journal_record) == {
        "audit_scope_sha256",
        "authorization_context_sha256",
        "contract",
        "decision",
        "detailed_provenance",
        "duration_us",
        "event",
        "event_sha256",
        "generation_bundle_sha256",
        "observed_at",
        "plan_entitlement_sha256",
        "request_shape_sha256",
        "selector_kind",
    }
    serialized = repr(journal_record)
    assert SENSITIVE_SELECTOR not in serialized
    assert access.internal_release_id not in serialized


@pytest.mark.parametrize(
    ("finished_at", "expected_duration_us"),
    ((50.0, 0), (100_000.0, 60_000_000)),
)
def test_failure_journal_clamps_clock_anomalies_and_long_requests(
    monkeypatch,
    finished_at: float,
    expected_duration_us: int,
) -> None:
    access = _access()
    monkeypatch.setattr(
        endpoint_journal,
        "validate_billing_search_post_endpoint_access",
        lambda candidate: candidate,
    )
    monkeypatch.setattr(endpoint_journal.time, "perf_counter", lambda: finished_at)

    journal_record = endpoint_journal.billing_search_post_failure_journal(
        access,
        decision="unavailable",
        trusted_observed_at=NOW,
        started_at=100.0,
    )

    assert journal_record["duration_us"] == expected_duration_us


@pytest.mark.parametrize("generation_digest", (None, "6" * 64))
def test_success_journal_preserves_generation_or_safe_fallback(
    monkeypatch,
    generation_digest: str | None,
) -> None:
    access = _access()
    monkeypatch.setattr(
        endpoint_journal,
        "validate_billing_search_post_endpoint_access",
        lambda candidate: candidate,
    )
    monkeypatch.setattr(endpoint_journal.time, "perf_counter", lambda: 101.0)

    journal_record = endpoint_journal.billing_search_post_success_journal(
        access,
        generation_bundle_sha256=generation_digest,
        trusted_observed_at=NOW,
        started_at=100.0,
    )

    assert journal_record["decision"] == "authorized"
    assert journal_record["duration_us"] == 1_000_000
    if generation_digest is None:
        assert journal_record["generation_bundle_sha256"] not in {None, "0" * 64}
    else:
        assert journal_record["generation_bundle_sha256"] == generation_digest


@pytest.mark.parametrize("decision", ("authorized", "rate_limited", "other"))
def test_failure_journal_rejects_nonfailure_decisions_without_echo(
    monkeypatch,
    decision: str,
) -> None:
    monkeypatch.setattr(
        endpoint_journal,
        "validate_billing_search_post_endpoint_access",
        lambda candidate: candidate,
    )

    with pytest.raises(
        endpoint_journal.BillingSearchPostEndpointJournalError,
        match="^billing_search_post_endpoint_journal_invalid$",
    ) as captured:
        endpoint_journal.billing_search_post_failure_journal(
            _access(),
            decision=decision,
            trusted_observed_at=NOW,
            started_at=100.0,
        )

    assert decision not in str(captured.value)
    assert SENSITIVE_SELECTOR not in str(captured.value)


def test_failure_journal_rejects_unvalidated_access_value_free() -> None:
    with pytest.raises(
        endpoint_journal.BillingSearchPostEndpointJournalError,
        match="^billing_search_post_endpoint_journal_invalid$",
    ) as captured:
        endpoint_journal.billing_search_post_failure_journal(
            SimpleNamespace(selector_value=SENSITIVE_SELECTOR),
            decision="denied",
            trusted_observed_at=NOW,
            started_at=100.0,
        )

    assert SENSITIVE_SELECTOR not in str(captured.value)
