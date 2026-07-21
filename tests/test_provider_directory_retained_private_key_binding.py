# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import copy
import pickle
from dataclasses import asdict, dataclass

import pytest

from process.provider_directory_retained_artifact_base import RetainedArtifactError
from process.provider_directory_retained_artifact_keys import (
    LOCATOR_PRIVATE_VALUE,
    VALIDATOR_PRIVATE_VALUE,
    _consume_private_value,
    _is_private_identity_valid,
    _open_private_value,
    _validated_key_context,
    private_identity_hmac,
    private_item_binding,
    seal_private_value,
)


KEY_ID_ENV = "HLTHPRT_PROVIDER_DIRECTORY_RETAINED_ARTIFACT_KEY_ID"
KEY_ENV = "HLTHPRT_PROVIDER_DIRECTORY_RETAINED_ARTIFACT_KEY"
PREVIOUS_ENV = "HLTHPRT_PROVIDER_DIRECTORY_RETAINED_ARTIFACT_PREVIOUS_KEYRING"
PRIVATE_BINDING = private_item_binding(
    campaign_id="a" * 64,
    source_item_id="b" * 64,
    endpoint_id="c" * 64,
)
OTHER_PRIVATE_BINDING = private_item_binding(
    campaign_id="a" * 64,
    source_item_id="d" * 64,
    endpoint_id="c" * 64,
)


@pytest.fixture
def retained_keyring(monkeypatch):
    monkeypatch.setenv(KEY_ID_ENV, "current-v3")
    monkeypatch.setenv(KEY_ENV, "c" * 32)
    monkeypatch.delenv(PREVIOUS_ENV, raising=False)


def test_private_ciphertext_and_identity_are_item_bound(retained_keyring) -> None:
    private_value = "https://private.example.test/item"
    ciphertext = seal_private_value(
        private_value,
        purpose=LOCATOR_PRIVATE_VALUE,
        binding_sha256=PRIVATE_BINDING,
    )
    opened = _open_private_value(
        ciphertext,
        purpose=LOCATOR_PRIVATE_VALUE,
        binding_sha256=PRIVATE_BINDING,
    )
    identity = private_identity_hmac(
        private_value,
        purpose=LOCATOR_PRIVATE_VALUE,
        binding_sha256=PRIVATE_BINDING,
    )
    with pytest.raises(
        RetainedArtifactError,
        match="retained_private_decryption_failed",
    ):
        _open_private_value(
            ciphertext,
            purpose=LOCATOR_PRIVATE_VALUE,
            binding_sha256=OTHER_PRIVATE_BINDING,
        )
    assert not _is_private_identity_valid(
        opened,
        identity,
        purpose=LOCATOR_PRIVATE_VALUE,
        binding_sha256=OTHER_PRIVATE_BINDING,
    )
    assert identity != private_identity_hmac(
        private_value,
        purpose=LOCATOR_PRIVATE_VALUE,
        binding_sha256=OTHER_PRIVATE_BINDING,
    )


@dataclass(frozen=True)
class _CapabilityEnvelope:
    capability: object


def _render_exception_chain(error: BaseException) -> bytes:
    chain_parts: list[str] = []
    current_error: BaseException | None = error
    while current_error is not None:
        chain_parts.extend(
            (repr(current_error), str(current_error), repr(vars(current_error)))
        )
        current_error = current_error.__cause__ or current_error.__context__
    return "".join(chain_parts).encode("utf-8", errors="backslashreplace")


def test_secret_capabilities_are_redacted_and_serialize_only_inert_state(
    retained_keyring,
) -> None:
    sentinel = "private-capability-sentinel-6c901"
    context = _validated_key_context("safe-v1", sentinel + "x" * 32)
    ciphertext = seal_private_value(
        sentinel,
        purpose=LOCATOR_PRIVATE_VALUE,
        binding_sha256=PRIVATE_BINDING,
    )
    opened = _open_private_value(
        ciphertext,
        purpose=LOCATOR_PRIVATE_VALUE,
        binding_sha256=PRIVATE_BINDING,
    )
    serialized_surfaces: list[bytes] = []
    for capability in (context, opened):
        serialized_surfaces.extend(
            (
                repr(capability).encode(),
                str(capability).encode(),
                repr(asdict(_CapabilityEnvelope(capability))).encode(),
                repr(copy.copy(capability)).encode(),
                repr(copy.deepcopy(capability)).encode(),
                pickle.dumps(capability),
            )
        )
    with pytest.raises(
        RetainedArtifactError,
        match="retained_private_capability_mismatch",
    ) as capability_error:
        _consume_private_value(opened, purpose=VALIDATOR_PRIVATE_VALUE)
    serialized_surfaces.append(_render_exception_chain(capability_error.value))
    assert all(sentinel.encode() not in surface for surface in serialized_surfaces)


def test_stable_facade_does_not_export_raw_secret_capabilities() -> None:
    from process import provider_directory_retained_artifact_contract as facade

    assert not hasattr(facade, "PrivateKeyContext")
    assert not hasattr(facade, "OpenedPrivateValue")
    assert not hasattr(facade, "open_private_value")
