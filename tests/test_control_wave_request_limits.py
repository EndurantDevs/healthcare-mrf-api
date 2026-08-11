# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Raw request limits for signed exact-wave admission."""

from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from sanic.exceptions import BadRequest, SanicException

from api import control_import_wave_direct
from api import control_wave_routes
from api.control_import_waves import MAX_ATTESTATION_CANONICAL_BYTES
from process.ptg_wave_receipt_authority import PTGWaveReceiptAuthorityError
from tests.ptg_wave_receipt_test_keys import (
    write_ephemeral_receipt_private_key,
)


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


@pytest.mark.asyncio
async def test_control_start_checks_open_receipt_epochs_before_controller(
    monkeypatch,
):
    coverage = AsyncMock()
    start = AsyncMock()
    monkeypatch.setattr(
        control_wave_routes,
        "assert_nonterminal_receipt_key_coverage",
        coverage,
    )
    monkeypatch.setattr(
        control_wave_routes,
        "start_ptg_wave_controller",
        start,
    )
    keyring = object()
    monkeypatch.setattr(
        control_wave_routes,
        "load_process_receipt_keyring",
        lambda: keyring,
    )

    app = SimpleNamespace(ctx=SimpleNamespace())
    await control_wave_routes.control_initialize_ptg_wave_receipt_authority(
        app,
        None,
    )
    await control_wave_routes.control_start_ptg_wave_controller(app, None)

    coverage.assert_awaited_once_with(keyring=keyring)
    assert app.ctx.ptg_wave_receipt_keyring is keyring
    start.assert_awaited_once_with(app)


@pytest.mark.asyncio
async def test_reader_process_initializes_without_signing_authority(
    monkeypatch,
):
    load_keyring = Mock(return_value=None)
    coverage = AsyncMock()
    start = AsyncMock()
    monkeypatch.setattr(
        control_wave_routes,
        "load_process_receipt_keyring",
        load_keyring,
    )
    monkeypatch.setattr(
        control_wave_routes,
        "assert_nonterminal_receipt_key_coverage",
        coverage,
    )
    monkeypatch.setattr(
        control_wave_routes,
        "start_ptg_wave_controller",
        start,
    )
    app = SimpleNamespace(ctx=SimpleNamespace(ptg_wave_receipt_keyring=object()))

    await control_wave_routes.control_initialize_ptg_wave_receipt_authority(
        app,
        None,
    )

    assert app.ctx.ptg_wave_receipt_keyring is None
    load_keyring.assert_called_once_with()
    coverage.assert_not_awaited()
    start.assert_not_awaited()


@pytest.mark.asyncio
async def test_public_epoch_route_uses_process_pinned_keyring(monkeypatch):
    payload = {
        "schema_version": "healthporta.ptg-wave-receipt-key-epochs.v1",
        "active_key_id": "epoch-pinned",
        "epochs": [],
    }
    keyring = SimpleNamespace(public_epochs_mapping=lambda: payload)
    request = SimpleNamespace(
        app=SimpleNamespace(
            ctx=SimpleNamespace(ptg_wave_receipt_keyring=keyring)
        )
    )
    monkeypatch.setattr(
        control_wave_routes,
        "require_control_auth",
        lambda _request: None,
    )

    result = await (
        control_wave_routes.control_get_receipt_key_epochs(
            request
        )
    )

    assert json.loads(result.body) == payload


@pytest.mark.asyncio
async def test_reader_rejects_v6_routes_without_env_reload(
    monkeypatch,
):
    """A reader never discovers private receipt material during a request."""

    load_from_environment = Mock(
        side_effect=AssertionError("request path must not reload receipt keys")
    )
    monkeypatch.setattr(
        control_wave_routes.PTGWaveReceiptKeyring,
        "from_environment",
        load_from_environment,
    )
    monkeypatch.setattr(
        control_wave_routes,
        "require_control_auth",
        lambda _request: None,
    )
    request = SimpleNamespace(
        app=SimpleNamespace(
            ctx=SimpleNamespace(ptg_wave_receipt_keyring=None)
        )
    )

    with pytest.raises(SanicException) as raised:
        await control_wave_routes.control_get_receipt_key_epochs(request)

    assert raised.value.status_code == 503
    load_from_environment.assert_not_called()


@pytest.mark.asyncio
async def test_authority_process_pins_one_keyring_across_same_id_secret_rotation(
    monkeypatch,
    tmp_path,
):
    """One signer process keeps its startup key object after file rotation."""

    private_path = write_ephemeral_receipt_private_key(
        tmp_path / "active.pem"
    )
    monkeypatch.setenv(
        "HLTHPRT_PTG_WAVE_RECEIPT_AUTHORITY_ROLE",
        "signer",
    )
    monkeypatch.setenv("HLTHPRT_API_WORKERS", "1")
    monkeypatch.setenv(
        "HLTHPRT_PTG_WAVE_RECEIPT_ACTIVE_KEY_ID",
        "epoch-stable",
    )
    monkeypatch.setenv(
        "HLTHPRT_PTG_WAVE_RECEIPT_ACTIVE_PRIVATE_KEY_FILE",
        str(private_path),
    )
    coverage = AsyncMock()
    monkeypatch.setattr(
        control_wave_routes,
        "assert_nonterminal_receipt_key_coverage",
        coverage,
    )
    app = SimpleNamespace(ctx=SimpleNamespace())

    await control_wave_routes.control_initialize_ptg_wave_receipt_authority(
        app,
        None,
    )
    pinned = app.ctx.ptg_wave_receipt_keyring
    pinned_modulus = pinned.public_by_key_id["epoch-stable"].rsa_modulus
    write_ephemeral_receipt_private_key(private_path)

    assert app.ctx.ptg_wave_receipt_keyring is pinned
    assert pinned.public_by_key_id["epoch-stable"].rsa_modulus == pinned_modulus
    coverage.assert_awaited_once_with(keyring=pinned)


def test_receipt_authority_rejects_multi_worker_and_partial_configuration(
    monkeypatch,
):
    """Signer topology and configuration fail before traffic."""

    monkeypatch.setenv(
        "HLTHPRT_PTG_WAVE_RECEIPT_AUTHORITY_ROLE",
        "signer",
    )
    monkeypatch.setenv("HLTHPRT_API_WORKERS", "2")
    with pytest.raises(PTGWaveReceiptAuthorityError, match="one API worker"):
        control_wave_routes.load_process_receipt_keyring()

    monkeypatch.setenv("HLTHPRT_API_WORKERS", "1")
    monkeypatch.setenv(
        "HLTHPRT_PTG_WAVE_RECEIPT_ACTIVE_KEY_ID",
        "partial-epoch",
    )
    with pytest.raises(PTGWaveReceiptAuthorityError):
        control_wave_routes.load_process_receipt_keyring()


def test_legacy_only_reader_accepts_absent_receipt_configuration(monkeypatch):
    """A configuration-free reader preserves legacy V1-V5 service."""

    for name in (
        "HLTHPRT_PTG_WAVE_RECEIPT_AUTHORITY_ROLE",
        "HLTHPRT_PTG_WAVE_RECEIPT_ACTIVE_KEY_ID",
        "HLTHPRT_PTG_WAVE_RECEIPT_ACTIVE_PRIVATE_KEY_FILE",
        "HLTHPRT_PTG_WAVE_RECEIPT_RETAINED_PRIVATE_KEY_FILES_JSON",
        "HLTHPRT_PTG_WAVE_RECEIPT_RETIRED_PUBLIC_EPOCHS_FILE",
    ):
        monkeypatch.delenv(name, raising=False)

    assert control_wave_routes.load_process_receipt_keyring() is None
