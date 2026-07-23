# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

from types import SimpleNamespace

from api.runtime_identity import (
    IMAGE_IDENTITY_HEADER,
    PROCESS_IDENTITY_HEADER,
    PROCESS_STARTED_AT_HEADER,
    RUNTIME_IMAGE_ENV,
    add_runtime_identity_headers,
    runtime_identity,
)
from scripts.ptg_v4_dev_canary_metrics import parse_runtime_identity_metric


def test_runtime_identity_headers_match_prometheus_identity(
    monkeypatch,
) -> None:
    monkeypatch.setenv(RUNTIME_IMAGE_ENV, "dev-main-example")
    response = SimpleNamespace(headers={})

    add_runtime_identity_headers(None, response)
    identity = runtime_identity()
    metric_identity = parse_runtime_identity_metric(
        "hp_mrf_api_process_identity_info{"
        f'identity="{identity["process_identity"]}",'
        f'image="{identity["image_identity"]}",'
        f'started_at="{identity["process_started_at"]}"'
        "} 1.000000"
    )

    assert response.headers == {
        PROCESS_IDENTITY_HEADER: identity["process_identity"],
        PROCESS_STARTED_AT_HEADER: identity["process_started_at"],
        IMAGE_IDENTITY_HEADER: "dev-main-example",
    }
    assert metric_identity == identity


def test_runtime_identity_middleware_ignores_missing_response() -> None:
    add_runtime_identity_headers(None, None)
    add_runtime_identity_headers(None, object())
