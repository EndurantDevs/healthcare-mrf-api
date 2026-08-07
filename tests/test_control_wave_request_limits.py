# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Raw request limits for signed exact-wave admission."""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest
from sanic.exceptions import BadRequest, SanicException

from api import control_import_wave_direct
from api import control_wave_routes
from api.control_import_waves import MAX_ATTESTATION_CANONICAL_BYTES


class _OversizedRequest:
    def __init__(self, *, headers: dict[str, str], body: bytes):
        self.headers = headers
        self.body = body

    @property
    def json(self):
        raise AssertionError("oversized request JSON must not be accessed")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("headers", "body"),
    (
        (
            {
                "content-length": str(
                    MAX_ATTESTATION_CANONICAL_BYTES + 1
                )
            },
            b"",
        ),
        ({}, b"x" * (MAX_ATTESTATION_CANONICAL_BYTES + 1)),
    ),
)
async def test_oversized_raw_wave_is_rejected_before_admission(
    monkeypatch,
    headers,
    body,
):
    admit = AsyncMock()
    monkeypatch.setattr(
        control_wave_routes,
        "require_control_auth",
        lambda _request: None,
    )
    monkeypatch.setattr(control_wave_routes, "admit_import_wave", admit)

    with pytest.raises(SanicException) as raised:
        await control_wave_routes.control_admit_import_wave(
            _OversizedRequest(headers=headers, body=body)
        )

    assert raised.value.status_code == 413
    admit.assert_not_awaited()


def test_canonical_wave_and_param_shape_limits_fail_closed(monkeypatch):
    monkeypatch.setattr(
        control_import_wave_direct,
        "MAX_ATTESTATION_CANONICAL_BYTES",
        8,
    )

    with pytest.raises(ValueError, match="canonical byte limit"):
        control_import_wave_direct.require_bounded_wave_request(
            {"padding": "long"}
        )
    with pytest.raises(ValueError, match="params must be an object"):
        control_import_wave_direct.normalized_wave_params([])


@pytest.mark.asyncio
async def test_invalid_content_length_is_rejected_before_admission(monkeypatch):
    admit = AsyncMock()
    monkeypatch.setattr(
        control_wave_routes,
        "require_control_auth",
        lambda _request: None,
    )
    monkeypatch.setattr(control_wave_routes, "admit_import_wave", admit)

    with pytest.raises(BadRequest) as raised:
        await control_wave_routes.control_admit_import_wave(
            _OversizedRequest(
                headers={"content-length": "invalid"},
                body=b"",
            )
        )

    assert raised.value.status_code == 400
    admit.assert_not_awaited()
