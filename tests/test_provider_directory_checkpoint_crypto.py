# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Retained checkpoint decryption needs cipher keys, not prior master credentials."""

import base64
import hashlib
import hmac
import importlib
import json

import pytest
from cryptography.fernet import Fernet, InvalidToken

from process import provider_directory_checkpoint_crypto as crypto

importer = importlib.import_module("process.provider_directory_fhir")

PRIMARY_ENV = "HLTHPRT_PROVIDER_DIRECTORY_CHECKPOINT_KEY"
RAW_PREVIOUS_ENV = "HLTHPRT_PROVIDER_DIRECTORY_CHECKPOINT_PREVIOUS_KEYS"
CONTROL_ENV = "HLTHPRT_CONTROL_API_TOKEN"
CAPABILITY = "https://example.test/export/status?capability=synthetic"


@pytest.fixture(autouse=True)
def isolated_crypto_environment(monkeypatch):
    for name in (PRIMARY_ENV, RAW_PREVIOUS_ENV, CONTROL_ENV, crypto.PREVIOUS_FERNET_KEYS_ENV):
        monkeypatch.delenv(name, raising=False)


def derived_key(master: str) -> str:
    """Use the existing checkpoint domain to reproduce historical ciphertext keys."""
    return base64.urlsafe_b64encode(
        hmac.new(master.encode(), b"healthporta:provider-directory:bulk-checkpoint:v1", hashlib.sha256).digest()
    ).decode("ascii")


def encrypted_with(master: str, plaintext: str = CAPABILITY) -> str:
    return importer.BULK_EXPORT_CAPABILITY_CIPHER_PREFIX + importer._bulk_checkpoint_fernet(master).encrypt(
        plaintext.encode()
    ).decode("ascii")


def test_derived_ring_reads_both_prior_keys_without_their_master_credentials(monkeypatch):
    previous_masters = ("synthetic-old-primary", "synthetic-old-control")
    ciphertexts = tuple(encrypted_with(master) for master in previous_masters)
    monkeypatch.setenv(PRIMARY_ENV, "synthetic-current-primary")
    monkeypatch.setenv(
        crypto.PREVIOUS_FERNET_KEYS_ENV, json.dumps([derived_key(master) for master in previous_masters])
    )

    assert importer.os.getenv(CONTROL_ENV) is None
    assert importer.os.getenv(RAW_PREVIOUS_ENV) is None
    assert importer._bulk_checkpoint_decryption_secrets() == ("synthetic-current-primary",)
    assert [importer._decrypt_bulk_capability(value) for value in ciphertexts] == [CAPABILITY, CAPABILITY]
    assert all(CAPABILITY not in value and "capability=synthetic" not in value for value in ciphertexts)


def test_new_ciphertext_uses_only_the_current_primary(monkeypatch):
    previous_key = derived_key("synthetic-previous-primary")
    monkeypatch.setenv(PRIMARY_ENV, "synthetic-current-primary")
    monkeypatch.setenv(crypto.PREVIOUS_FERNET_KEYS_ENV, json.dumps([previous_key]))
    ciphertext = importer._encrypt_bulk_capability(CAPABILITY)
    token = ciphertext.removeprefix(importer.BULK_EXPORT_CAPABILITY_CIPHER_PREFIX).encode("ascii")

    assert importer._bulk_checkpoint_fernet("synthetic-current-primary").decrypt(token).decode() == CAPABILITY
    with pytest.raises(InvalidToken):
        Fernet(previous_key.encode()).decrypt(token)


def test_previous_derived_keys_can_decrypt_but_are_never_the_encryption_default(monkeypatch):
    previous_master = "synthetic-retained-primary"
    ciphertext = encrypted_with(previous_master)
    monkeypatch.setenv(crypto.PREVIOUS_FERNET_KEYS_ENV, json.dumps([derived_key(previous_master)]))
    assert importer._decrypt_bulk_capability(ciphertext) == CAPABILITY
    with pytest.raises(RuntimeError, match="^bulk_export_checkpoint_encryption_key_missing$"):
        importer._encrypt_bulk_capability(CAPABILITY)


@pytest.mark.parametrize("raw", [None, "", "  ", "[]"])
def test_absent_or_empty_derived_ring_is_optional(raw):
    assert crypto.previous_fernet_ciphers(raw) == ()


@pytest.mark.parametrize("raw", ["null", "{}", '"key"', "[", "[null]", "[1]", "[{}]", '[""]', '["not-a-key"]', '["☃"]'])
def test_derived_ring_requires_a_json_list_of_complete_keys(raw):
    with pytest.raises(RuntimeError, match="^bulk_export_checkpoint_keyring_invalid$"):
        crypto.previous_fernet_ciphers(raw)


@pytest.mark.parametrize("changed", ["whitespace", "missing_padding", "wrong_size", "noncanonical", "standard_base64"])
def test_derived_key_encoding_is_strict(changed):
    key = base64.urlsafe_b64encode(bytes([255]) * 32).decode("ascii")
    invalid_by_change = {
        "whitespace": " " + key,
        "missing_padding": key[:-1],
        "wrong_size": base64.urlsafe_b64encode(bytes(31)).decode("ascii"),
        "noncanonical": key[:-2] + "9=",
        "standard_base64": base64.b64encode(bytes([255]) * 32).decode("ascii"),
    }
    with pytest.raises(RuntimeError, match="^bulk_export_checkpoint_keyring_invalid$"):
        crypto.previous_fernet_ciphers(json.dumps([invalid_by_change[changed]]))


def test_entire_ring_is_validated_without_leaking_contents(monkeypatch, caplog):
    monkeypatch.setenv(PRIMARY_ENV, "synthetic-current-primary")
    ciphertext = importer._encrypt_bulk_capability(CAPABILITY)
    previous_key = derived_key("synthetic-previous-primary")
    invalid_entry = "synthetic-invalid-private-material"
    monkeypatch.setenv(crypto.PREVIOUS_FERNET_KEYS_ENV, json.dumps([previous_key, invalid_entry]))
    with pytest.raises(RuntimeError, match="^bulk_export_checkpoint_keyring_invalid$") as raised:
        importer._decrypt_bulk_capability(ciphertext)
    assert previous_key not in str(raised.value) + caplog.text
    assert invalid_entry not in str(raised.value) + caplog.text
    assert CAPABILITY not in str(raised.value) + caplog.text
    assert not importer._is_bulk_export_error_terminal(str(raised.value))


def test_duplicate_previous_keys_are_accepted_once():
    key = derived_key("synthetic-previous-primary")
    assert len(crypto.previous_fernet_ciphers(json.dumps([key, key]))) == 1


def test_raw_previous_keys_and_control_fallback_remain_compatible(monkeypatch):
    monkeypatch.setenv(CONTROL_ENV, "synthetic-control-primary")
    ciphertext = importer._encrypt_bulk_capability(CAPABILITY)
    monkeypatch.setenv(PRIMARY_ENV, "synthetic-current-primary")
    assert importer._decrypt_bulk_capability(ciphertext) == CAPABILITY
    monkeypatch.delenv(CONTROL_ENV)
    monkeypatch.setenv(RAW_PREVIOUS_ENV, json.dumps(["synthetic-control-primary"]))
    assert importer._decrypt_bulk_capability(ciphertext) == CAPABILITY


def test_importer_crypto_hooks_and_failed_key_fallback_are_preserved(monkeypatch):
    cipher = Fernet(Fernet.generate_key())
    ciphertext = importer.BULK_EXPORT_CAPABILITY_CIPHER_PREFIX + cipher.encrypt(CAPABILITY.encode()).decode()
    attempts = []

    def chosen_cipher(master=None):
        attempts.append(master)
        if master == "first":
            raise ValueError("synthetic invalid key")
        return cipher

    monkeypatch.setattr(importer, "_bulk_checkpoint_decryption_secrets", lambda: ("first", "second"))
    monkeypatch.setattr(importer, "_bulk_checkpoint_fernet", chosen_cipher)
    assert importer._decrypt_bulk_capability(ciphertext) == CAPABILITY
    assert attempts == ["first", "second"]


@pytest.mark.parametrize("value", [None, "", CAPABILITY, "plaintext"])
def test_plaintext_is_never_accepted_as_a_checkpoint(value):
    with pytest.raises(RuntimeError, match="^bulk_export_checkpoint_ciphertext_invalid$"):
        importer._decrypt_bulk_capability(value)


def test_wrong_previous_key_fails_closed_without_changing_ciphertext(monkeypatch):
    ciphertext = encrypted_with("synthetic-original-primary")
    monkeypatch.setenv(crypto.PREVIOUS_FERNET_KEYS_ENV, json.dumps([derived_key("synthetic-other-primary")]))
    with pytest.raises(RuntimeError, match="^bulk_export_checkpoint_decryption_failed$"):
        importer._decrypt_bulk_capability(ciphertext)
    assert ciphertext.startswith(importer.BULK_EXPORT_CAPABILITY_CIPHER_PREFIX)
