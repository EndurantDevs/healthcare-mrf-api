# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

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
@pytest.mark.parametrize(
    ("error", "status", "code"),
    (
        (
            BillingSearchResourceNotFoundError("billing_search_resource_not_found"),
            404,
            "resource_not_found",
        ),
        (
            BillingSearchCursorGenerationExpiredError(
                "billing_search_cursor_generation_expired"
            ),
            409,
            "cursor_generation_expired",
        ),
        (
            BillingSearchPostCursorInvalidError("billing_search_cursor_invalid"),
            400,
            "invalid_request",
        ),
        (
            BillingSearchPostServingUnavailableError(
                "billing_search_serving_unavailable"
            ),
            503,
            "billing_search_unavailable",
        ),
    ),
)
async def test_post_route_maps_value_free_operation_failures(
    monkeypatch,
    error,
    status: int,
    code: str,
) -> None:
    monkeypatch.setattr(pricing, "_billing_search_trusted_now", lambda: NOW)
    monkeypatch.setattr(
        pricing,
        "load_billing_search_transport_keyring",
        lambda _environment: object(),
    )
    monkeypatch.setattr(
        pricing,
        "authorize_billing_search_post_endpoint",
        lambda *_args, **_kwargs: object(),
    )
    monkeypatch.setattr(
        pricing,
        "execute_billing_search_post",
        AsyncMock(side_effect=error),
    )

    http_response = await pricing.search_providers_by_procedure_billing_identity(
        _request()
    )

    assert http_response.status == status
    assert http_response.headers["cache-control"] == "private, no-store"
    assert _decoded(http_response)["error"]["code"] == code


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
