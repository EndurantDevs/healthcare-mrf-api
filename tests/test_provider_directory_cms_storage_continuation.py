# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Fresh authority observations retain the original consumed lease identity."""

import base64
import copy
import datetime
import json
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from process import provider_directory_cms_storage_continuation as continuation
from tests.test_provider_directory_profile_capacity_attestation import (
    PRIVATE_KEY,
    VALIDATION_TIME,
    _signed_envelope,
    _trust,
    _verify,
)


def _fixture():
    """Build a real synthetic signature and a fresh engine-generated phase nonce."""
    lease = _verify(_signed_envelope())
    request = continuation.request_storage_continuation(
        lease, run_id="run_" + "a" * 32, phase="pre_logging", requested_at=VALIDATION_TIME
    )
    storage = copy.deepcopy(lease.signing_preflight_guard["control_plane_request"]["storage_observation"])
    storage.update(observed_at=continuation._utc(VALIDATION_TIME), issued_at=continuation._utc(VALIDATION_TIME))
    witness_by_field = {
        **request.binding_by_field,
        "issued_at": continuation._utc(VALIDATION_TIME),
        "expires_at": continuation._utc(VALIDATION_TIME + datetime.timedelta(seconds=30)),
        "storage_observation": storage,
    }
    return lease, request, witness_by_field


def _envelope(witness):
    """Sign only public synthetic capacity material with the test key."""
    canonical = json.dumps(witness, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("ascii")
    signature = PRIVATE_KEY.sign(continuation.STORAGE_CONTINUATION_DOMAIN + canonical)
    return {"observation": witness, "signature": base64.urlsafe_b64encode(signature).rstrip(b"=").decode("ascii")}


def _verify_continuation(lease, request, witness, *, now=VALIDATION_TIME):
    """Verify the untrusted envelope with the same current capacity trust set."""
    return continuation.verify_storage_continuation(
        _envelope(witness), request=request, lease=lease, trust=_trust(), now=now
    )


def test_fresh_signature_does_not_replace_original_lease_or_consumption():
    lease, request, witness = _fixture()
    canonical, signature, nonce = lease.canonical_lease_json, lease.signature, lease.nonce
    verified = _verify_continuation(lease, request, witness)
    assert verified.request_nonce == request.binding_by_field["request_nonce"]
    assert verified.observed_at == VALIDATION_TIME
    assert (lease.canonical_lease_json, lease.signature, lease.nonce) == (canonical, signature, nonce)
    second = continuation.request_storage_continuation(
        lease, run_id="run_" + "a" * 32, phase="pre_logging", requested_at=VALIDATION_TIME
    )
    assert second.binding_by_field["request_nonce"] != verified.request_nonce
    with pytest.raises(RuntimeError, match="request_binding_changed"):
        _verify_continuation(lease, second, witness)


@pytest.mark.parametrize(
    "field",
    [
        "run_id",
        "phase",
        "request_nonce",
        "original_lease_digest",
        "capacity_geometry_hash",
        "reservation_id",
        "attestation_id",
        "database_oid",
        "key_id",
    ],
)
def test_signed_substitution_cannot_change_request_binding(field):
    lease, request, witness = _fixture()
    witness[field] = 234 if field == "database_oid" else "changed"
    with pytest.raises(RuntimeError, match="request_binding_changed"):
        _verify_continuation(lease, request, witness)


@pytest.mark.parametrize(
    "change", ["stale", "future", "expiry", "deadline", "volume", "temp", "extra", "bad_signature"]
)
def test_observation_rejects_stale_or_changed_authority_material(change):
    lease, request, witness = _fixture()
    _mutate(witness, change)
    envelope = _envelope(witness)
    if change == "bad_signature":
        envelope["signature"] = "A" * 86
    with pytest.raises((RuntimeError, ValueError)):
        continuation.verify_storage_continuation(
            envelope, request=request, lease=lease, trust=_trust(), now=VALIDATION_TIME
        )


def _mutate(witness, change):
    """Alter a single signed synthetic field without introducing a new contract."""
    storage = witness["storage_observation"]
    if change == "stale":
        storage["observed_at"] = continuation._utc(VALIDATION_TIME - datetime.timedelta(seconds=6))
        return
    if change == "future":
        witness["issued_at"] = storage["issued_at"] = continuation._utc(VALIDATION_TIME + datetime.timedelta(seconds=6))
        return
    if change == "expiry":
        witness["expires_at"] = continuation._utc(VALIDATION_TIME + datetime.timedelta(seconds=31))
        return
    if change == "deadline":
        storage["max_build_deadline"] = continuation._utc(VALIDATION_TIME + datetime.timedelta(minutes=10))
        return
    if change == "volume":
        storage["volumes"][0]["volume_digest"] = "ef" * 32
        return
    if change == "temp":
        storage["temp_tablespace"]["tablespace_oid"] += 1
        return
    if change == "extra":
        witness["extra"] = True


def test_same_lease_signature_cannot_authorize_storage_continuation():
    lease, request, witness = _fixture()
    envelope = _envelope(witness)
    envelope["signature"] = lease.signature
    with pytest.raises(RuntimeError, match="signature_invalid"):
        continuation.verify_storage_continuation(
            envelope, request=request, lease=lease, trust=_trust(), now=VALIDATION_TIME
        )


def test_expired_phase_and_replaced_trust_pin_fail_closed():
    from dataclasses import replace

    lease, request, witness = _fixture()
    with pytest.raises(RuntimeError, match="fresh_observation_required"):
        _verify_continuation(lease, request, witness, now=VALIDATION_TIME + datetime.timedelta(seconds=30))
    with pytest.raises(ValueError, match="pin_mismatch"):
        continuation.verify_storage_continuation(
            _envelope(witness),
            request=request,
            lease=lease,
            trust=replace(_trust(), database_oid=16402),
            now=VALIDATION_TIME,
        )


@pytest.mark.parametrize(
    "origin,token",
    [
        ("", "secret"),
        ("http://capacity.example", "secret"),
        ("https://user:secret@capacity.example", "secret"),
        ("https://capacity.example/path", "secret"),
        ("https://capacity.example?query=yes", "secret"),
        ("https://capacity.example#fragment", "secret"),
        ("https://capacity.example:999999", "secret"),
        ("https://capacity.example", ""),
        ("https://capacity.example", "token\ninvalid"),
    ],
)
def test_transport_requires_operator_owned_authenticated_origin(monkeypatch, origin, token):
    monkeypatch.setenv("HLTHPRT_PROVIDER_DIRECTORY_CAPACITY_AUTHORITY_URL", origin)
    monkeypatch.setenv("HLTHPRT_PROVIDER_DIRECTORY_CAPACITY_AUTHORITY_TOKEN", token)
    with pytest.raises(RuntimeError, match="authority_configuration_invalid"):
        continuation.configured_storage_continuation()


class _HTTPContext:
    def __init__(self, value):
        self.value = value

    async def __aenter__(self):
        return self.value

    async def __aexit__(self, *args):
        return False


@pytest.mark.asyncio
@pytest.mark.parametrize("case", ["valid", "redirect", "denied", "malformed", "oversized", "wrong-fields"])
async def test_transport_never_redirects_or_accepts_unbounded_responses(monkeypatch, case):
    lease, request, witness = _fixture()
    response_payload_bytes = json.dumps(_envelope(witness)).encode()
    if case == "malformed":
        response_payload_bytes = b"not-json"
    elif case == "oversized":
        response_payload_bytes = b"x" * (continuation._MAX_RESPONSE_BYTES + 1)
    elif case == "wrong-fields":
        response_payload_bytes = b'{"signature":"x"}'

    async def chunks(size):
        assert size == 8192
        for offset in range(0, len(response_payload_bytes), size):
            yield response_payload_bytes[offset : offset + size]

    response = SimpleNamespace(
        status=302 if case == "redirect" else 401 if case == "denied" else 200,
        content=SimpleNamespace(iter_chunked=chunks),
    )
    post = Mock(return_value=_HTTPContext(response))
    client = Mock(return_value=_HTTPContext(SimpleNamespace(post=post)))
    monkeypatch.setattr(continuation.aiohttp, "ClientSession", client)
    monkeypatch.setenv("HLTHPRT_PROVIDER_DIRECTORY_CAPACITY_AUTHORITY_URL", "https://capacity.example/")
    monkeypatch.setenv("HLTHPRT_PROVIDER_DIRECTORY_CAPACITY_AUTHORITY_TOKEN", "synthetic-token")
    callback = continuation.configured_storage_continuation()
    monkeypatch.setenv("HLTHPRT_PROVIDER_DIRECTORY_CAPACITY_AUTHORITY_URL", "https://changed.example")
    if case == "valid":
        envelope = await callback(request)
        assert (
            continuation.verify_storage_continuation(
                envelope, request=request, lease=lease, trust=_trust(), now=VALIDATION_TIME
            ).request_nonce
            == request.binding_by_field["request_nonce"]
        )
    else:
        with pytest.raises(RuntimeError):
            await callback(request)
    assert client.call_args.kwargs["trust_env"] is False
    assert client.call_args.kwargs["timeout"].total == 10
    post.assert_called_once_with(
        "https://capacity.example/v1/provider-directory/cms-storage-continuation",
        json=request.payload,
        headers={"Authorization": "Bearer synthetic-token"},
        allow_redirects=False,
    )
