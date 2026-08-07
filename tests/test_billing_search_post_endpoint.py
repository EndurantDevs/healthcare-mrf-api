# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import hashlib
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from api import billing_search_post_endpoint_journal as endpoint_journal
from api.billing_search_access_contract import (
    BILLING_SEARCH_CAPABILITY,
    build_billing_search_authorization_context,
)
from api.billing_search_post_endpoint_access import (
    BillingSearchPostEndpointAccessError,
)
from api.billing_search_post_operation import (
    BillingSearchCursorGenerationExpiredError,
    BillingSearchPostCursorInvalidError,
    BillingSearchPostExecution,
    BillingSearchResourceNotFoundError,
    BillingSearchPostServingUnavailableError,
)
from api.billing_search_transport_keys import BillingSearchTransportKeyringError
from api.endpoint import pricing

PATH = "/api/v1/pricing/providers/search-by-procedure"
NOW = "2026-08-07T10:00:10Z"
SENSITIVE_SELECTOR = "SENSITIVE-BILLING-SELECTOR-MUST-NOT-ESCAPE"
SENSITIVE_FAILURE = "SENSITIVE-FAILURE-DETAIL-MUST-NOT-ESCAPE"


def _sha256(label: str) -> str:
    return hashlib.sha256(label.encode("ascii")).hexdigest()


def _authenticated_access() -> SimpleNamespace:
    context = build_billing_search_authorization_context(
        {
            "principal_scope_sha256": _sha256("synthetic-principal"),
            "tenant_scope_sha256": _sha256("synthetic-tenant"),
            "plan_entitlement_sha256": _sha256("synthetic-entitlement"),
            "audit_scope_sha256": _sha256("synthetic-audit"),
            "quota_scope_sha256": _sha256("synthetic-quota"),
            "capabilities": (BILLING_SEARCH_CAPABILITY,),
            "issued_at": "2026-08-07T10:00:00Z",
            "expires_at": "2026-08-07T10:01:00Z",
        },
        trusted_now=NOW,
    )
    return SimpleNamespace(
        authorization_context=context,
        request=SimpleNamespace(
            request_shape_sha256=_sha256("synthetic-request-shape"),
            selector_kind="billing_entity_ref",
            include_evidence=False,
            selector_value=SENSITIVE_SELECTOR,
        ),
        internal_release_id="hprelease_sensitive_internal_value",
    )


def _request():
    return SimpleNamespace(
        body=b"{}",
        headers={"synthetic": "header"},
        method="POST",
        path=PATH,
        content_type="application/json; charset=utf-8",
        ctx=SimpleNamespace(sa_session=object()),
    )


def _decoded(response) -> dict[str, object]:
    return json.loads(bytes(response.body))


def _install_post_failure_case(
    monkeypatch,
    caplog,
    access: object,
    operation_failure: Exception,
    journal_calls: list[tuple[object, dict[str, object]]],
) -> None:
    original_journal_builder = pricing.billing_search_post_failure_journal

    def _captured_journal(candidate_access, **kwargs):
        journal_calls.append((candidate_access, kwargs))
        return original_journal_builder(candidate_access, **kwargs)

    caplog.set_level("INFO", logger=pricing.logger.name)
    monkeypatch.setattr(pricing, "_billing_search_trusted_now", lambda: NOW)
    monkeypatch.setattr(
        endpoint_journal,
        "validate_billing_search_post_endpoint_access",
        lambda candidate: candidate,
    )
    monkeypatch.setattr(
        pricing,
        "load_billing_search_transport_keyring",
        lambda _environment: object(),
    )
    monkeypatch.setattr(
        pricing,
        "authorize_billing_search_post_endpoint",
        lambda *_args, **_kwargs: access,
    )
    monkeypatch.setattr(
        pricing,
        "execute_billing_search_post",
        AsyncMock(side_effect=operation_failure),
    )
    monkeypatch.setattr(
        pricing,
        "billing_search_post_failure_journal",
        _captured_journal,
    )


@pytest.mark.asyncio
async def test_post_route_authorizes_before_database_and_never_reads_query_args(
    monkeypatch,
) -> None:
    request = _request()
    events: list[str] = []
    access = object()

    def _authorize(body, headers, **kwargs):
        events.append("authorize")
        assert body == b"{}"
        assert headers == {"synthetic": "header"}
        assert kwargs["method"] == "POST"
        assert kwargs["path"] == PATH
        assert kwargs["media_type"] == "application/json"
        return access

    async def _execute(session, candidate_access, **kwargs):
        events.append("database")
        assert session is request.ctx.sa_session
        assert candidate_access is access
        assert kwargs["trusted_now"] == NOW
        return BillingSearchPostExecution(
            payload={"result_state": "no_matching_rates", "items": []},
            audit_record={"event": "billing_search_access"},
            stage_timings_ms=(("exact_reader", 1.0),),
        )

    monkeypatch.setattr(pricing, "_billing_search_trusted_now", lambda: NOW)
    monkeypatch.setattr(
        pricing,
        "load_billing_search_transport_keyring",
        lambda _environment: object(),
    )
    monkeypatch.setattr(
        pricing,
        "authorize_billing_search_post_endpoint",
        _authorize,
    )
    monkeypatch.setattr(pricing, "execute_billing_search_post", _execute)

    http_response = await pricing.search_providers_by_procedure_billing_identity(
        request
    )

    assert events == ["authorize", "database"]
    assert http_response.status == 200
    assert http_response.headers["cache-control"] == "private, no-store"
    assert _decoded(http_response)["result_state"] == "no_matching_rates"


@pytest.mark.asyncio
async def test_post_route_maps_session_acquisition_failure_after_auth(
    monkeypatch,
    caplog,
) -> None:
    """A missing database session must fail closed and remain journaled."""

    access = _authenticated_access()
    journal_calls: list[tuple[object, dict[str, object]]] = []
    _install_post_failure_case(
        monkeypatch,
        caplog,
        access,
        BillingSearchPostServingUnavailableError(SENSITIVE_FAILURE),
        journal_calls,
    )
    request = _request()
    request.ctx = SimpleNamespace()

    http_response = await pricing.search_providers_by_procedure_billing_identity(
        request
    )

    assert http_response.status == 503
    assert http_response.headers["cache-control"] == "private, no-store"
    assert _decoded(http_response)["error"]["code"] == ("billing_search_unavailable")
    pricing.execute_billing_search_post.assert_not_awaited()
    assert len(journal_calls) == 1
    assert journal_calls[0][0] is access
    assert journal_calls[0][1]["decision"] == "unavailable"
    access_logs = [
        log_entry
        for log_entry in caplog.records
        if log_entry.getMessage() == "Billing search POST access decision"
    ]
    assert len(access_logs) == 1
    assert access_logs[0].billing_search_audit["decision"] == "unavailable"
    logged_payload = repr(access_logs[0].billing_search_audit)
    assert SENSITIVE_SELECTOR not in logged_payload
    assert SENSITIVE_FAILURE not in logged_payload


@pytest.mark.asyncio
@pytest.mark.parametrize(
    (
        "operation_failure",
        "expected_status",
        "expected_code",
        "expected_decision",
    ),
    (
        (
            BillingSearchResourceNotFoundError(SENSITIVE_FAILURE),
            404,
            "resource_not_found",
            "denied",
        ),
        (
            BillingSearchCursorGenerationExpiredError(SENSITIVE_FAILURE),
            409,
            "cursor_generation_expired",
            "unavailable",
        ),
        (
            BillingSearchPostCursorInvalidError(SENSITIVE_FAILURE),
            400,
            "invalid_request",
            "denied",
        ),
        (
            BillingSearchPostServingUnavailableError(SENSITIVE_FAILURE),
            503,
            "billing_search_unavailable",
            "unavailable",
        ),
    ),
)
async def test_post_route_maps_value_free_operation_failures(
    monkeypatch,
    caplog,
    operation_failure,
    expected_status: int,
    expected_code: str,
    expected_decision: str,
) -> None:
    """Post-auth failures must map and journal without sensitive values."""

    access = _authenticated_access()
    journal_calls: list[tuple[object, dict[str, object]]] = []
    _install_post_failure_case(
        monkeypatch,
        caplog,
        access,
        operation_failure,
        journal_calls,
    )

    http_response = await pricing.search_providers_by_procedure_billing_identity(
        _request()
    )

    assert http_response.status == expected_status
    assert http_response.headers["cache-control"] == "private, no-store"
    assert _decoded(http_response)["error"]["code"] == expected_code
    assert len(journal_calls) == 1
    journal_access, journal_kwargs = journal_calls[0]
    assert journal_access is access
    assert journal_kwargs["decision"] == expected_decision
    assert journal_kwargs["trusted_observed_at"] == NOW
    assert isinstance(journal_kwargs["started_at"], float)
    access_logs = [
        log_entry
        for log_entry in caplog.records
        if log_entry.getMessage() == "Billing search POST access decision"
    ]
    assert len(access_logs) == 1
    audit_record = access_logs[0].billing_search_audit
    assert audit_record["event"] == "billing_search_access"
    assert audit_record["decision"] == expected_decision
    assert 0 <= audit_record["duration_us"] <= 60_000_000
    logged_payload = repr(audit_record)
    assert SENSITIVE_SELECTOR not in logged_payload
    assert SENSITIVE_FAILURE not in logged_payload
    assert access.internal_release_id not in logged_payload


@pytest.mark.asyncio
async def test_post_route_hides_invalid_gateway_authority(monkeypatch) -> None:
    monkeypatch.setattr(pricing, "_billing_search_trusted_now", lambda: NOW)
    monkeypatch.setattr(
        pricing,
        "load_billing_search_transport_keyring",
        lambda _environment: object(),
    )
    monkeypatch.setattr(
        pricing,
        "authorize_billing_search_post_endpoint",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            BillingSearchPostEndpointAccessError(
                "billing_search_post_endpoint_access_invalid"
            )
        ),
    )

    http_response = await pricing.search_providers_by_procedure_billing_identity(
        _request()
    )

    assert http_response.status == 404
    assert _decoded(http_response)["error"] == {
        "code": "resource_not_found",
        "message": "Resource not found.",
    }


@pytest.mark.asyncio
async def test_post_route_reports_missing_transport_keys_as_unavailable(
    monkeypatch,
) -> None:
    monkeypatch.setattr(pricing, "_billing_search_trusted_now", lambda: NOW)
    monkeypatch.setattr(
        pricing,
        "load_billing_search_transport_keyring",
        lambda _environment: (_ for _ in ()).throw(
            BillingSearchTransportKeyringError(
                "billing_search_transport_keyring_invalid"
            )
        ),
    )

    http_response = await pricing.search_providers_by_procedure_billing_identity(
        _request()
    )

    assert http_response.status == 503
    assert _decoded(http_response)["error"]["code"] == ("billing_search_unavailable")
