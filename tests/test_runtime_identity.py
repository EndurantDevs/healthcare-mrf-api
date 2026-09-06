# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

from types import SimpleNamespace

from api.runtime_identity import (
    RUNTIME_IDENTITY_HEADERS_ENV,
    add_runtime_identity_headers,
)


def test_runtime_identity_middleware_ignores_missing_response() -> None:
    add_runtime_identity_headers(None, None)
    add_runtime_identity_headers(None, object())


def test_runtime_identity_headers_are_candidate_opt_in(
    monkeypatch,
) -> None:
    response = SimpleNamespace(headers={})

    add_runtime_identity_headers(None, response)
    assert response.headers == {}

    monkeypatch.setenv(RUNTIME_IDENTITY_HEADERS_ENV, "false")
    add_runtime_identity_headers(None, response)
    assert response.headers == {}
