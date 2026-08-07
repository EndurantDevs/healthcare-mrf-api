# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""HTTP and transaction boundaries for exact billing-identity search."""

from __future__ import annotations

import logging
import re

import pytest

from api import billing_search_http as billing_http
from api.billing_search_cursor import (
    BillingSearchCursorError,
    BillingSearchCursorGenerationExpired,
)
from api.billing_search_cursor_keys import BillingSearchCursorKeyringError
from api.billing_search_endpoint_access import BillingSearchEndpointAccessError
from api.billing_search_transport_contract import BILLING_SEARCH_TRANSPORT_PATH
from api.ptg2_billing_search_contract import resource_not_found, serving_unavailable
from tests.billing_search_http_support import (
    SUCCESS_TRANSACTION_EVENTS,
    TRUSTED_NOW,
    RecordingSession,
    assert_private_response,
    install_authorized_boundary,
    install_success_pipeline,
    make_request,
    response_payload,
)


def test_keyrings_cache_only_the_current_raw_environment_document(monkeypatch) -> None:
    transport_documents = []
    cursor_documents = []

    def load_transport(environment_map):
        transport_documents.append(dict(environment_map))
        return ("transport", len(transport_documents))

    def load_cursor(environment_map):
        cursor_documents.append(dict(environment_map))
        return ("cursor", len(cursor_documents))

    billing_http._transport_keyring_for_document.cache_clear()
    billing_http._cursor_keyring_for_document.cache_clear()
    monkeypatch.setattr(
        billing_http,
        "load_billing_search_transport_keyring",
        load_transport,
    )
    monkeypatch.setattr(
        billing_http,
        "load_billing_search_cursor_keyring",
        load_cursor,
    )
    try:
        first_transport = billing_http._transport_keyring_for_document("document-a")
        assert (
            billing_http._transport_keyring_for_document("document-a")
            is first_transport
        )
        second_transport = billing_http._transport_keyring_for_document("document-b")
        assert second_transport is not first_transport

        first_cursor = billing_http._cursor_keyring_for_document("document-a")
        assert billing_http._cursor_keyring_for_document("document-a") is first_cursor
        second_cursor = billing_http._cursor_keyring_for_document("document-b")
        assert second_cursor is not first_cursor

        assert transport_documents == [
            {billing_http.BILLING_SEARCH_TRANSPORT_KEYRING_ENV: "document-a"},
            {billing_http.BILLING_SEARCH_TRANSPORT_KEYRING_ENV: "document-b"},
        ]
        assert cursor_documents == [
            {billing_http.BILLING_SEARCH_CURSOR_KEYRING_ENV: "document-a"},
            {billing_http.BILLING_SEARCH_CURSOR_KEYRING_ENV: "document-b"},
        ]
    finally:
        billing_http._transport_keyring_for_document.cache_clear()
        billing_http._cursor_keyring_for_document.cache_clear()


def test_runtime_keyring_lookup_is_keyed_by_the_exact_environment_document(
    monkeypatch,
) -> None:
    seen_documents = []
    monkeypatch.setenv(
        billing_http.BILLING_SEARCH_TRANSPORT_KEYRING_ENV,
        "transport-document",
    )
    monkeypatch.setenv(
        billing_http.BILLING_SEARCH_CURSOR_KEYRING_ENV,
        "cursor-document",
    )
    monkeypatch.setattr(
        billing_http,
        "_transport_keyring_for_document",
        lambda document: seen_documents.append(("transport", document)) or object(),
    )
    monkeypatch.setattr(
        billing_http,
        "_cursor_keyring_for_document",
        lambda document: seen_documents.append(("cursor", document)) or object(),
    )

    billing_http._transport_keyring()
    billing_http._cursor_keyring()

    assert seen_documents == [
        ("transport", "transport-document"),
        ("cursor", "cursor-document"),
    ]


def test_server_clock_uses_the_canonical_utc_second_contract() -> None:
    assert re.fullmatch(
        r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z",
        billing_http._trusted_now(),
    )


@pytest.mark.asyncio
async def test_success_uses_one_read_only_snapshot_and_encodes_inside_it(
    monkeypatch,
) -> None:
    """Keep authorization, service, shaping, and encoding on one trusted snapshot."""

    session = RecordingSession()
    endpoint_access, cursor_keyring = install_authorized_boundary(
        monkeypatch,
        session.events,
    )
    service_result = object()
    install_success_pipeline(
        monkeypatch,
        session,
        endpoint_access,
        cursor_keyring,
        service_result,
    )

    http_response = await billing_http.serve_billing_search_get(
        make_request(),
        session,
    )

    assert http_response.status == 200
    assert http_response.headers.get("Cache-Control") == "private, no-store"
    assert response_payload(http_response) == {
        "result_state": "matched",
        "items": [],
    }
    assert session.begin_count == 1
    assert session.events == SUCCESS_TRANSACTION_EVENTS


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("method", "path"),
    (
        ("POST", BILLING_SEARCH_TRANSPORT_PATH),
        ("GET", "/api/v1/pricing/providers/audit-search-by-procedure"),
        ("GET", "/api/v1/pricing/providers/by-procedure"),
        ("GET", "/api/v1/pricing/providers/by-service"),
        ("GET", "/api/v1/pricing/physicians/by-service"),
    ),
)
async def test_noncanonical_selector_requests_stop_before_keys_auth_and_sql(
    monkeypatch,
    method: str,
    path: str,
) -> None:
    session = RecordingSession()

    def forbidden(*_args, **_kwargs):
        raise AssertionError("noncanonical request crossed the path gate")

    monkeypatch.setattr(
        billing_http,
        "_transport_keyring",
        forbidden,
    )
    monkeypatch.setattr(
        billing_http,
        "_cursor_keyring",
        forbidden,
    )
    monkeypatch.setattr(billing_http, "authorize_billing_search_endpoint", forbidden)

    http_response = await billing_http.serve_billing_search_get(
        make_request(method=method, path=path),
        session,
    )

    assert_private_response(
        http_response,
        status=404,
        code="resource_not_found",
    )
    assert session.begin_count == 0
    assert session.events == []


@pytest.mark.asyncio
async def test_key_configuration_fails_closed_before_sql(
    monkeypatch,
    caplog,
) -> None:
    session = RecordingSession()
    monkeypatch.setattr(billing_http, "_trusted_now", lambda: TRUSTED_NOW)

    def unavailable_keyring():
        raise BillingSearchCursorKeyringError("private-key-material")

    monkeypatch.setattr(
        billing_http,
        "_transport_keyring",
        lambda: object(),
    )
    monkeypatch.setattr(
        billing_http,
        "_cursor_keyring",
        unavailable_keyring,
    )
    with caplog.at_level(logging.WARNING, logger=billing_http.__name__):
        unavailable_response = await billing_http.serve_billing_search_get(
            make_request(),
            session,
        )

    assert_private_response(
        unavailable_response,
        status=503,
        code="billing_search_serving_unavailable",
    )
    assert "private-key-material" not in caplog.text
    assert caplog.records[-1].billing_search_failure_class == (
        "BillingSearchCursorKeyringError"
    )
    assert session.begin_count == 0


@pytest.mark.asyncio
async def test_access_denial_fails_closed_before_sql(monkeypatch, caplog) -> None:
    session = RecordingSession()
    monkeypatch.setattr(billing_http, "_trusted_now", lambda: TRUSTED_NOW)
    monkeypatch.setattr(billing_http, "_transport_keyring", lambda: object())
    monkeypatch.setattr(billing_http, "_cursor_keyring", lambda: object())

    def denied(*_args, **_kwargs):
        raise BillingSearchEndpointAccessError("private-authorization-context")

    monkeypatch.setattr(billing_http, "authorize_billing_search_endpoint", denied)
    with caplog.at_level(logging.WARNING, logger=billing_http.__name__):
        denied_response = await billing_http.serve_billing_search_get(
            make_request(),
            session,
        )

    assert_private_response(
        denied_response,
        status=404,
        code="resource_not_found",
    )
    assert "private-authorization-context" not in caplog.text
    assert caplog.records[-1].billing_search_failure_class == (
        "BillingSearchEndpointAccessError"
    )
    assert session.begin_count == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("failure_stage", ("key_configuration", "authorization"))
async def test_unexpected_boundary_failures_are_sanitized_503s(
    monkeypatch,
    caplog,
    failure_stage: str,
) -> None:
    session = RecordingSession()
    monkeypatch.setattr(billing_http, "_trusted_now", lambda: TRUSTED_NOW)
    monkeypatch.setattr(billing_http, "_transport_keyring", lambda: object())
    monkeypatch.setattr(billing_http, "_cursor_keyring", lambda: object())
    monkeypatch.setattr(
        billing_http,
        "authorize_billing_search_endpoint",
        lambda *_args, **_kwargs: object(),
    )
    if failure_stage == "key_configuration":

        def failed_keyring():
            raise RuntimeError("private-unexpected-key-context")

        monkeypatch.setattr(billing_http, "_transport_keyring", failed_keyring)
        private_value = "private-unexpected-key-context"
    else:

        def failed_authorization(*_args, **_kwargs):
            raise RuntimeError("private-unexpected-authorization-context")

        monkeypatch.setattr(
            billing_http,
            "authorize_billing_search_endpoint",
            failed_authorization,
        )
        private_value = "private-unexpected-authorization-context"

    with caplog.at_level(logging.WARNING, logger=billing_http.__name__):
        http_response = await billing_http.serve_billing_search_get(
            make_request(),
            session,
        )

    assert_private_response(
        http_response,
        status=503,
        code="billing_search_serving_unavailable",
    )
    assert private_value not in caplog.text
    assert caplog.records[-1].billing_search_failure_class == "RuntimeError"
    assert session.begin_count == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("failure", "status", "code"),
    (
        (
            BillingSearchCursorGenerationExpired("private-expired-cursor"),
            409,
            "billing_search_cursor_generation_expired",
        ),
        (
            BillingSearchCursorError("private-invalid-cursor"),
            400,
            "billing_search_cursor_invalid",
        ),
        (
            resource_not_found(),
            404,
            "resource_not_found",
        ),
        (
            serving_unavailable(),
            503,
            "billing_search_serving_unavailable",
        ),
        (
            RuntimeError("private-database-context"),
            503,
            "billing_search_serving_unavailable",
        ),
    ),
)
async def test_service_failures_have_closed_status_and_rollback(
    monkeypatch,
    caplog,
    failure: Exception,
    status: int,
    code: str,
) -> None:
    session = RecordingSession()
    install_authorized_boundary(monkeypatch, session.events)

    async def failed_service(_session, **_kwargs):
        raise failure

    monkeypatch.setattr(
        billing_http,
        "search_exact_billing_provider_page",
        failed_service,
    )
    with caplog.at_level(logging.WARNING, logger=billing_http.__name__):
        http_response = await billing_http.serve_billing_search_get(
            make_request(),
            session,
        )

    assert_private_response(http_response, status=status, code=code)
    assert str(failure) not in http_response.body.decode("utf-8")
    assert str(failure) not in caplog.text
    assert caplog.records[-1].billing_search_failure_class == type(failure).__name__
    assert session.begin_count == 1
    assert session.active is False
    assert session.events[-1] == ("transaction_exit", type(failure).__name__)


@pytest.mark.asyncio
async def test_outgoing_cursor_or_shape_failure_is_503_not_client_cursor_error(
    monkeypatch,
    caplog,
) -> None:
    session = RecordingSession()
    install_authorized_boundary(monkeypatch, session.events)

    async def search(_session, **_kwargs):
        return object()

    def failed_shape(*_args, **_kwargs):
        raise BillingSearchCursorGenerationExpired("private-outgoing-cursor")

    monkeypatch.setattr(billing_http, "search_exact_billing_provider_page", search)
    monkeypatch.setattr(billing_http, "shape_billing_search_response", failed_shape)
    with caplog.at_level(logging.WARNING, logger=billing_http.__name__):
        http_response = await billing_http.serve_billing_search_get(
            make_request(),
            session,
        )

    assert_private_response(
        http_response,
        status=503,
        code="billing_search_serving_unavailable",
    )
    assert "private-outgoing-cursor" not in caplog.text
    assert caplog.records[-1].billing_search_failure_class == (
        "BillingSearchCursorGenerationExpired"
    )
    assert session.events[-1] == (
        "transaction_exit",
        "_BillingSearchResponseFailure",
    )


@pytest.mark.asyncio
async def test_oversized_success_body_is_rejected_inside_the_transaction(
    monkeypatch,
    caplog,
) -> None:
    session = RecordingSession()
    install_authorized_boundary(monkeypatch, session.events)

    async def search(_session, **_kwargs):
        return object()

    def shape(*_args, **_kwargs):
        assert session.active
        return {"payload": "x" * billing_http._MAX_SUCCESS_BODY_BYTES}

    monkeypatch.setattr(billing_http, "search_exact_billing_provider_page", search)
    monkeypatch.setattr(billing_http, "shape_billing_search_response", shape)
    with caplog.at_level(logging.WARNING, logger=billing_http.__name__):
        http_response = await billing_http.serve_billing_search_get(
            make_request(),
            session,
        )

    assert_private_response(
        http_response,
        status=503,
        code="billing_search_serving_unavailable",
    )
    assert caplog.records[-1].billing_search_failure_class == (
        "BillingSearchServingUnavailableError"
    )
    assert session.events[-1] == (
        "transaction_exit",
        "_BillingSearchResponseFailure",
    )
