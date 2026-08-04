# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import base64
import copy
import json
import pickle
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest

from process.ptg_parts import ptg2_raw_tin_vault_crypto as vault
from process.ptg_parts import ptg2_raw_tin_vault_keyring as vault_keyring
from process.tin_npi_connector_security import load_tin_token_policy


TOKEN_POLICY_ID = "ptg-tin-hmac-sha256-v1:test-v1"
SYNTHETIC_EIN = "12-3456789"
NORMALIZED_SYNTHETIC_EIN = "123456789"


def _owner_only_file(path: Path, payload: bytes) -> Path:
    if path.exists():
        path.chmod(0o600)
    path.write_bytes(payload)
    path.chmod(0o400)
    return path


def _keyring_payload(
    *,
    active_key_id: str = "vault-v1",
    key_bytes_by_id: dict[str, bytes] | None = None,
) -> bytes:
    keys = key_bytes_by_id or {active_key_id: bytes(range(32))}
    return json.dumps(
        {
            "contract": vault.RAW_TIN_VAULT_KEYRING_CONTRACT,
            "active_key_id": active_key_id,
            "keys": {
                key_id: base64.urlsafe_b64encode(key_bytes).decode("ascii")
                for key_id, key_bytes in keys.items()
            },
        },
        separators=(",", ":"),
    ).encode("utf-8")


def _keyring(tmp_path: Path, **payload_kwargs):
    keyring_path = _owner_only_file(
        tmp_path / "vault-keyring.json",
        _keyring_payload(**payload_kwargs),
    )
    return vault.load_raw_tin_vault_keyring(keyring_path)


def _token_projector(tmp_path: Path, *, policy_id: str = TOKEN_POLICY_ID):
    secret_path = _owner_only_file(tmp_path / f"{policy_id[-7:]}.key", b"p" * 32)
    return load_tin_token_policy(
        token_policy_id=policy_id,
        secret_file=secret_path,
    )


@pytest.mark.parametrize("candidate", [SYNTHETIC_EIN, NORMALIZED_SYNTHETIC_EIN])
def test_vault_round_trip_is_redacted_and_contract_bound(tmp_path, candidate):
    keyring = _keyring(tmp_path)
    token_projector = _token_projector(tmp_path)

    sealed = vault.seal_ein(keyring, token_projector, candidate)
    opened = vault.open_ein(keyring, token_projector, sealed)

    assert vault._consume_opened_ein(opened, token_projector) == NORMALIZED_SYNTHETIC_EIN
    assert sealed.encryption_contract == vault.RAW_TIN_VAULT_ENCRYPTION_CONTRACT
    assert sealed.binding_contract == vault.RAW_TIN_VAULT_BINDING_CONTRACT
    assert sealed.tin_type == "ein"
    assert sealed.encryption_key_id == "vault-v1"
    assert NORMALIZED_SYNTHETIC_EIN not in sealed.ciphertext
    assert NORMALIZED_SYNTHETIC_EIN not in repr(sealed)
    assert NORMALIZED_SYNTHETIC_EIN not in repr(opened)
    assert NORMALIZED_SYNTHETIC_EIN not in str(opened)
    assert not hasattr(opened, "value")
    assert "RawTinVaultKeyring" not in vault.__all__
    assert "raw-tin-vault-keyring" in repr(keyring)
    assert "raw-tin-vault-key-context" in repr(keyring._active_context())

    for capability in (sealed, opened, keyring):
        snapshot = copy.copy(capability)
        assert "redacted" in repr(snapshot)
        assert "redacted" in str(snapshot)
        assert copy.copy(snapshot) is snapshot
        assert copy.deepcopy(snapshot) is snapshot
        assert "redacted" in repr(pickle.loads(pickle.dumps(snapshot)))
        assert "redacted" in repr(copy.deepcopy(capability))
        assert "redacted" in repr(pickle.loads(pickle.dumps(capability)))


def test_vault_binding_uses_authoritative_full_hmac(tmp_path):
    keyring = _keyring(tmp_path)
    token_projector = _token_projector(tmp_path)
    sealed = vault.seal_ein(keyring, token_projector, SYNTHETIC_EIN)
    colliding_prefix_hmac = sealed.tin_hmac_sha256[:16] + b"x" * 16
    transplanted = replace(sealed, tin_hmac_sha256=colliding_prefix_hmac)

    with pytest.raises(vault.RawTinVaultError, match="decryption_failed"):
        vault.open_ein(keyring, token_projector, transplanted)


def test_vault_rejects_tamper_wrong_policy_and_unknown_tagged_key(tmp_path):
    keyring = _keyring(tmp_path)
    token_projector = _token_projector(tmp_path)
    sealed = vault.seal_ein(keyring, token_projector, SYNTHETIC_EIN)
    prefix, token_text = sealed.ciphertext.rsplit(":", 1)
    tamper_index = len(token_text) // 2
    replacement = "A" if token_text[tamper_index] != "A" else "B"
    tampered = replace(
        sealed,
        ciphertext=(
            f"{prefix}:{token_text[:tamper_index]}{replacement}"
            f"{token_text[tamper_index + 1:]}"
        ),
    )
    with pytest.raises(vault.RawTinVaultError, match="decryption_failed"):
        vault.open_ein(keyring, token_projector, tampered)

    other_projector = _token_projector(
        tmp_path,
        policy_id="ptg-tin-hmac-sha256-v1:other-v1",
    )
    with pytest.raises(vault.RawTinVaultError, match="identity_mismatch"):
        vault.open_ein(keyring, other_projector, sealed)

    _, tagged_token = sealed.ciphertext.split(":", 2)[1:]
    unknown_key = replace(
        sealed,
        encryption_key_id="missing-v1",
        ciphertext=f"{vault.RAW_TIN_VAULT_CIPHERTEXT_PREFIX}missing-v1:{tagged_token}",
    )
    with pytest.raises(vault.RawTinVaultError, match="key_unavailable"):
        vault.open_ein(keyring, token_projector, unknown_key)


def test_vault_cryptographically_binds_the_tagged_key_id(tmp_path):
    shared_key = b"s" * 32
    keyring = _keyring(
        tmp_path,
        active_key_id="old-v1",
        key_bytes_by_id={"old-v1": shared_key, "current-v2": shared_key},
    )
    token_projector = _token_projector(tmp_path)
    sealed = vault.seal_ein(keyring, token_projector, SYNTHETIC_EIN)
    token_text = sealed.ciphertext.split(":", 2)[2]
    relabeled = replace(
        sealed,
        encryption_key_id="current-v2",
        ciphertext=f"{vault.RAW_TIN_VAULT_CIPHERTEXT_PREFIX}current-v2:{token_text}",
    )

    with pytest.raises(vault.RawTinVaultError, match="decryption_failed"):
        vault.open_ein(keyring, token_projector, relabeled)


def test_vault_rotation_opens_previous_key_and_rewraps_with_active(tmp_path):
    old_keyring = _keyring(tmp_path, active_key_id="old-v1")
    token_projector = _token_projector(tmp_path)
    old_entry = vault.seal_ein(old_keyring, token_projector, SYNTHETIC_EIN)

    keyring_path = tmp_path / "vault-keyring.json"
    keyring_path.chmod(0o600)
    keyring_path.write_bytes(
        _keyring_payload(
            active_key_id="current-v2",
            key_bytes_by_id={
                "old-v1": bytes(range(32)),
                "current-v2": bytes(range(32, 64)),
            },
        )
    )
    keyring_path.chmod(0o400)
    rotated_keyring = vault.load_raw_tin_vault_keyring(keyring_path)

    assert rotated_keyring.active_key_id == "current-v2"
    assert rotated_keyring.configured_key_ids == ("current-v2", "old-v1")
    assert vault._consume_opened_ein(
        vault.open_ein(rotated_keyring, token_projector, old_entry),
        token_projector,
    ) == NORMALIZED_SYNTHETIC_EIN

    rewrapped = vault.rewrap_ein(rotated_keyring, token_projector, old_entry)
    assert rewrapped.encryption_key_id == "current-v2"
    assert rewrapped.tin_hmac_sha256 == old_entry.tin_hmac_sha256
    assert rewrapped.ciphertext != old_entry.ciphertext

    current_only = _keyring(
        tmp_path,
        active_key_id="current-v2",
        key_bytes_by_id={"current-v2": bytes(range(32, 64))},
    )
    with pytest.raises(vault.RawTinVaultError, match="key_unavailable"):
        vault.open_ein(current_only, token_projector, old_entry)
    assert vault._consume_opened_ein(
        vault.open_ein(current_only, token_projector, rewrapped),
        token_projector,
    ) == NORMALIZED_SYNTHETIC_EIN


@pytest.mark.parametrize(
    ("field_changes", "error_code"),
    [
        ({"token_policy_descriptor_sha256": b"x" * 32}, "identity_invalid"),
        ({"tin_hmac_sha256": b"short"}, "identity_invalid"),
        ({"encryption_key_id": "bad:key"}, "key_id_invalid"),
        ({"ciphertext": "not-a-vault-token"}, "ciphertext_invalid"),
    ],
)
def test_sealed_entry_rejects_invalid_storage_contracts(
    tmp_path,
    field_changes,
    error_code,
):
    sealed = vault.seal_ein(
        _keyring(tmp_path),
        _token_projector(tmp_path),
        SYNTHETIC_EIN,
    )
    with pytest.raises(vault.RawTinVaultError, match=error_code):
        replace(sealed, **field_changes)


def test_vault_rejects_wrong_capability_types_without_private_echo(tmp_path):
    keyring = _keyring(tmp_path)
    token_projector = _token_projector(tmp_path)
    sealed = vault.seal_ein(keyring, token_projector, SYNTHETIC_EIN)

    with pytest.raises(vault.RawTinVaultError, match="entry_invalid") as error:
        vault.open_ein(keyring, token_projector, object())
    assert NORMALIZED_SYNTHETIC_EIN not in str(error.value)
    with pytest.raises(vault.RawTinVaultError, match="capability_mismatch"):
        vault._consume_opened_ein(object(), token_projector)
    with pytest.raises(vault.RawTinVaultError, match="token_policy_invalid"):
        vault.seal_ein(keyring, object(), SYNTHETIC_EIN)
    invalid_projector = SimpleNamespace(
        token_policy_id=TOKEN_POLICY_ID,
        tokenize_ein=lambda _candidate: object(),
    )
    with pytest.raises(vault.RawTinVaultError, match="token_policy_invalid"):
        vault.seal_ein(keyring, invalid_projector, SYNTHETIC_EIN)
    invalid_policy_projector = SimpleNamespace(
        token_policy_id="invalid-policy",
        tokenize_ein=lambda _candidate: object(),
    )
    with pytest.raises(vault.RawTinVaultError, match="token_policy_invalid"):
        vault.seal_ein(keyring, invalid_policy_projector, SYNTHETIC_EIN)
    with pytest.raises(ValueError, match="malformed"):
        vault.seal_ein(keyring, token_projector, "123")
    assert sealed.ciphertext.startswith(vault.RAW_TIN_VAULT_CIPHERTEXT_PREFIX)


def test_sealed_entry_rejects_malformed_policy_and_tag_mismatch(tmp_path):
    sealed = vault.seal_ein(
        _keyring(tmp_path),
        _token_projector(tmp_path),
        SYNTHETIC_EIN,
    )
    with pytest.raises(vault.RawTinVaultError, match="identity_invalid"):
        replace(sealed, token_policy_id="invalid-policy")
    with pytest.raises(vault.RawTinVaultError, match="ciphertext_invalid"):
        replace(sealed, ciphertext="hptinv1:vault-v1:")
    with pytest.raises(vault.RawTinVaultError, match="ciphertext_invalid"):
        replace(sealed, encryption_key_id="other-v1")


def test_private_capabilities_are_immutable_and_revalidate_state(tmp_path):
    keyring = _keyring(tmp_path)
    token_projector = _token_projector(tmp_path)
    opened = vault.open_ein(
        keyring,
        token_projector,
        vault.seal_ein(keyring, token_projector, SYNTHETIC_EIN),
    )
    context = keyring._active_context()

    for capability in (keyring, context, opened):
        with pytest.raises(TypeError, match="immutable"):
            capability.arbitrary_state = "changed"

    object.__setattr__(
        opened,
        "_OpenedRawTinVaultEntry__normalized_ein",
        "987654321",
    )
    with pytest.raises(vault.RawTinVaultError, match="capability_state_invalid"):
        vault._consume_opened_ein(opened, token_projector)

    malformed_policy_opened = vault.open_ein(
        _keyring(tmp_path),
        token_projector,
        vault.seal_ein(_keyring(tmp_path), token_projector, SYNTHETIC_EIN),
    )
    object.__setattr__(
        malformed_policy_opened,
        "_OpenedRawTinVaultEntry__token_policy_id",
        "bad-policy",
    )
    with pytest.raises(vault.RawTinVaultError, match="capability_state_invalid"):
        vault._consume_opened_ein(malformed_policy_opened, token_projector)

    object.__setattr__(keyring, "_RawTinVaultKeyring__active_key_id", "missing-v9")
    with pytest.raises(vault.RawTinVaultError, match="keyring_state_invalid"):
        _ = keyring.active_key_id

    direct_context = vault_keyring._VaultKeyContext("vault-v1", b"k" * 32)
    object.__setattr__(direct_context, "_VaultKeyContext__secret", b"short")
    with pytest.raises(vault.RawTinVaultError, match="key_context_invalid"):
        _ = direct_context.key_id


def test_private_capability_constructors_reject_malformed_state():
    with pytest.raises(vault.RawTinVaultError, match="key_id_invalid"):
        vault_keyring._VaultKeyContext("bad:key", b"k" * 32)
    with pytest.raises(vault.RawTinVaultError, match="master_key_invalid"):
        vault_keyring._VaultKeyContext("vault-v1", b"short")

    context = vault_keyring._VaultKeyContext("vault-v1", b"k" * 32)
    with pytest.raises(vault.RawTinVaultError, match="keyring_invalid"):
        vault_keyring._RawTinVaultKeyring(
            active_key_id="vault-v1",
            context_by_key_id=(("vault-v1", context),),
        )
    with pytest.raises(vault.RawTinVaultError, match="keyring_invalid"):
        vault_keyring._RawTinVaultKeyring(
            active_key_id="vault-v1",
            context_by_key_id={"wrong-v1": context},
        )
    with pytest.raises(vault.RawTinVaultError, match="keyring_state_invalid"):
        vault_keyring._RawTinVaultKeyring(
            active_key_id="missing-v1",
            context_by_key_id={"vault-v1": context},
        )
    with pytest.raises(vault.RawTinVaultError, match="ein_invalid"):
        vault._OpenedRawTinVaultEntry(
            SYNTHETIC_EIN,
            TOKEN_POLICY_ID,
            b"h" * 32,
        )
    with pytest.raises(vault.RawTinVaultError, match="capability_state_invalid"):
        vault._OpenedRawTinVaultEntry(
            NORMALIZED_SYNTHETIC_EIN,
            TOKEN_POLICY_ID,
            b"short",
        )
    with pytest.raises(vault.RawTinVaultError, match="ein_invalid"):
        vault._OpenedRawTinVaultEntry("123", TOKEN_POLICY_ID, b"h" * 32)


@pytest.mark.parametrize(
    ("state_name", "state_value"),
    [
        ("_RawTinVaultKeyring__active_key_id", "bad:key"),
        ("_RawTinVaultKeyring__contexts", []),
        ("_RawTinVaultKeyring__contexts", ()),
        ("_RawTinVaultKeyring__contexts", (object(),)),
    ],
)
def test_keyring_rejects_corrupted_private_state(state_name, state_value):
    context = vault_keyring._VaultKeyContext("vault-v1", b"k" * 32)
    keyring = vault_keyring._RawTinVaultKeyring(
        active_key_id="vault-v1",
        context_by_key_id={"vault-v1": context},
    )
    object.__setattr__(keyring, state_name, state_value)

    with pytest.raises(vault.RawTinVaultError, match="keyring_state_invalid"):
        _ = keyring.active_key_id


def test_keyring_rejects_duplicate_and_excessive_corrupted_contexts():
    context = vault_keyring._VaultKeyContext("vault-v1", b"k" * 32)
    keyring = vault_keyring._RawTinVaultKeyring(
        active_key_id="vault-v1",
        context_by_key_id={"vault-v1": context},
    )
    object.__setattr__(keyring, "_RawTinVaultKeyring__contexts", (context, context))
    with pytest.raises(vault.RawTinVaultError, match="keyring_state_invalid"):
        _ = keyring.active_key_id

    object.__setattr__(
        keyring,
        "_RawTinVaultKeyring__contexts",
        (context,) * (vault_keyring.RAW_TIN_VAULT_KEYRING_MAX_ENTRIES + 1),
    )
    with pytest.raises(vault.RawTinVaultError, match="keyring_state_invalid"):
        _ = keyring.active_key_id
