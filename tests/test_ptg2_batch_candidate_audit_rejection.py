from __future__ import annotations

import json
import uuid

import pytest
from sanic import Sanic
from sanic.exceptions import InvalidUsage

from process.ptg_parts import ptg2_batch_candidate_audit as batch_audit
from tests.test_ptg2_batch_candidate_audit import _http_config


ALLOWLISTED_REJECTION_REASONS = (
    (
        "PTG2 candidate exact provider-code matches exceed their bounded limit",
        "provider_code_matches_limit_exceeded",
    ),
    (
        "PTG2 candidate provider-code scope exceeds its bounded limit",
        "provider_code_scope_limit_exceeded",
    ),
    (
        "PTG2 candidate requested code scope exceeds its bounded limit",
        "requested_code_scope_limit_exceeded",
    ),
    (
        "PTG2 candidate provider-code scope has no requested codes",
        "provider_code_scope_empty",
    ),
    (
        "PTG2 candidate provider-code artifact is missing a referenced provider set",
        "provider_code_artifact_missing_provider_set",
    ),
    (
        "PTG2 provider-code intersections exceed their retention limit",
        "provider_code_intersections_limit_exceeded",
    ),
)


class _ResponseContent:
    def __init__(self, response_body: bytes):
        self.response_body = response_body

    async def iter_chunked(self, _chunk_size):
        yield self.response_body


def _sanic_error_body(response_status: int, response_message: str) -> bytes:
    return json.dumps(
        {
            "description": "Bad Request",
            "status": response_status,
            "message": response_message,
        }
    ).encode()


def _install_http_response(monkeypatch, status: int, response_body: bytes):
    observed_posts = []

    class StubResponse:
        content = _ResponseContent(response_body)

        def __init__(self):
            self.status = status

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_exc_info):
            return False

    class StubClientSession:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_exc_info):
            return False

        def post(self, request_url, **request_options):
            observed_posts.append((request_url, request_options))
            return StubResponse()

    monkeypatch.setattr(
        batch_audit.aiohttp,
        "ClientSession",
        lambda **_session_options: StubClientSession(),
    )
    return observed_posts


@pytest.mark.asyncio
async def test_sanic_invalid_usage_envelope_keeps_guard_in_message():
    endpoint_message, safe_detail = ALLOWLISTED_REJECTION_REASONS[0]

    async def reject_request(_request):
        raise InvalidUsage(endpoint_message)

    app = Sanic(f"audit-rejection-envelope-{uuid.uuid4().hex}")
    app.add_route(reject_request, "/audit", methods={"POST"})
    _request, response = await app.asgi_client.post("/audit", json={"audit": True})

    assert response.status == 400
    assert response.json["description"] == "Bad Request"
    assert response.json["message"] == endpoint_message
    assert batch_audit._batch_rejection_reason(400, response.body) == (
        f"batch_endpoint_rejected_400_{safe_detail}"
    )


@pytest.mark.parametrize("response_status", (400, 401, 403, 404))
@pytest.mark.asyncio
async def test_nonretryable_rejection_preserves_status_and_redacts_body(
    monkeypatch,
    response_status,
):
    sensitive_detail = "private endpoint detail"
    response_body = _sanic_error_body(response_status, sensitive_detail)
    observed_posts = _install_http_response(
        monkeypatch,
        response_status,
        response_body,
    )

    expected_reason = f"batch_endpoint_rejected_{response_status}"
    with pytest.raises(
        batch_audit.BatchCandidateAuditContractError,
        match=expected_reason,
    ) as exc_info:
        await batch_audit._post_batch_request(
            request_payload_by_field={},
            http_config=_http_config("https://private.example"),
            deadline_seconds=55.0,
        )

    assert len(observed_posts) == 1
    assert exc_info.value.reason == expected_reason
    assert sensitive_detail not in str(exc_info.value)


@pytest.mark.parametrize("response_status", (408, 429, 500, 502, 503, 504))
@pytest.mark.asyncio
async def test_retryable_rejection_classification_is_unchanged(
    monkeypatch,
    response_status,
):
    observed_posts = _install_http_response(
        monkeypatch,
        response_status,
        _sanic_error_body(response_status, "private endpoint detail"),
    )

    with pytest.raises(
        batch_audit.BatchCandidateAuditTransportError,
        match="batch_endpoint_temporarily_unavailable",
    ) as exc_info:
        await batch_audit._post_batch_request(
            request_payload_by_field={},
            http_config=_http_config("https://private.example"),
            deadline_seconds=55.0,
        )

    assert len(observed_posts) == 1
    assert exc_info.value.reason == "batch_endpoint_temporarily_unavailable"


@pytest.mark.parametrize(
    ("endpoint_message", "safe_detail"),
    ALLOWLISTED_REJECTION_REASONS,
)
def test_allowlisted_endpoint_message_adds_stable_rejection_detail(
    endpoint_message,
    safe_detail,
):
    response_body = _sanic_error_body(400, endpoint_message)

    assert batch_audit._batch_rejection_reason(400, response_body) == (
        f"batch_endpoint_rejected_400_{safe_detail}"
    )


@pytest.mark.parametrize(
    "response_body",
    (
        b"not-json",
        b'[{"message":"private endpoint detail"}]',
        b'{"error":{"message":"private endpoint detail"}}',
        b'{"message":42}',
        b'{"description":"PTG2 candidate provider-code scope exceeds its bounded limit",'
        b'"status":400,"message":"private endpoint detail"}',
    ),
)
def test_unknown_or_malformed_rejection_body_is_not_classified(response_body):
    assert (
        batch_audit._batch_rejection_reason(400, response_body)
        == "batch_endpoint_rejected_400"
    )
