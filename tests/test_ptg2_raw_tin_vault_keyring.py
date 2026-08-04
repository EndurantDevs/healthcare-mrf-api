# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""File-safety and document-contract tests for the raw-TIN vault keyring."""

from __future__ import annotations

import base64
import json
import os
from types import SimpleNamespace

import pytest

from process.ptg_parts import ptg2_raw_tin_vault_crypto as vault
from process.ptg_parts import ptg2_raw_tin_vault_keyring as vault_keyring
from tests.test_ptg2_raw_tin_vault_crypto import _keyring_payload, _owner_only_file


@pytest.mark.parametrize(
    "document",
    [
        {},
        {"contract": "wrong", "active_key_id": "vault-v1", "keys": {}},
        {
            "contract": vault.RAW_TIN_VAULT_KEYRING_CONTRACT,
            "active_key_id": "missing-v1",
            "keys": {"vault-v1": base64.urlsafe_b64encode(b"k" * 32).decode()},
        },
        {
            "contract": vault.RAW_TIN_VAULT_KEYRING_CONTRACT,
            "active_key_id": "bad:key",
            "keys": {"bad:key": base64.urlsafe_b64encode(b"k" * 32).decode()},
        },
        {
            "contract": vault.RAW_TIN_VAULT_KEYRING_CONTRACT,
            "active_key_id": "vault-v1",
            "keys": {"vault-v1": base64.urlsafe_b64encode(b"k" * 31).decode()},
        },
        {
            "contract": vault.RAW_TIN_VAULT_KEYRING_CONTRACT,
            "active_key_id": "vault-v1",
            "keys": {"vault-v1": base64.urlsafe_b64encode(b"k" * 33).decode()},
        },
    ],
)
def test_keyring_rejects_malformed_contracts(tmp_path, document):
    keyring_path = _owner_only_file(
        tmp_path / "invalid-keyring.json",
        json.dumps(document).encode(),
    )
    with pytest.raises(
        vault.RawTinVaultError,
        match="keyring_invalid|key_id_invalid",
    ):
        vault.load_raw_tin_vault_keyring(keyring_path)


@pytest.mark.parametrize(
    "encoded_secret",
    [
        None,
        "x" * 129,
        "not-base64*",
        "non-ascii-\u00e9",
        base64.b64encode(b"\xfb" * 32).decode("ascii"),
    ],
)
def test_keyring_rejects_noncanonical_encoded_keys(tmp_path, encoded_secret):
    document_by_field = {
        "contract": vault.RAW_TIN_VAULT_KEYRING_CONTRACT,
        "active_key_id": "vault-v1",
        "keys": {"vault-v1": encoded_secret},
    }
    keyring_path = _owner_only_file(
        tmp_path / "invalid-encoded-keyring.json",
        json.dumps(document_by_field).encode(),
    )
    with pytest.raises(vault.RawTinVaultError, match="keyring_invalid"):
        vault.load_raw_tin_vault_keyring(keyring_path)


@pytest.mark.parametrize(
    "raw_document",
    [
        b"",
        b"{",
        b"\xff",
        b'{"contract":"x","contract":"y"}',
        b"x" * (vault_keyring.RAW_TIN_VAULT_KEYRING_MAX_BYTES + 1),
    ],
)
def test_keyring_rejects_malformed_bounded_documents(tmp_path, raw_document):
    keyring_path = _owner_only_file(tmp_path / "invalid.json", raw_document)
    with pytest.raises(vault.RawTinVaultError, match="keyring_invalid"):
        vault.load_raw_tin_vault_keyring(keyring_path)


def test_keyring_rejects_unsafe_files_and_accepts_valid_symlink(tmp_path, monkeypatch):
    keyring_path = tmp_path / "vault-keyring.json"
    keyring_path.write_bytes(_keyring_payload())
    with pytest.raises(vault.RawTinVaultError, match="file_invalid"):
        vault.load_raw_tin_vault_keyring(keyring_path)

    keyring_path.chmod(0o400)
    keyring_link = tmp_path / "vault-keyring-link.json"
    keyring_link.symlink_to(keyring_path)
    assert vault.load_raw_tin_vault_keyring(keyring_link).active_key_id == "vault-v1"

    actual_fstat = vault_keyring.os.fstat

    def _wrong_owner_fstat(descriptor):
        metadata = actual_fstat(descriptor)
        return SimpleNamespace(
            st_mode=metadata.st_mode,
            st_uid=os.geteuid() + 1,
        )

    monkeypatch.setattr(vault_keyring.os, "fstat", _wrong_owner_fstat)
    with pytest.raises(vault.RawTinVaultError, match="file_invalid"):
        vault.load_raw_tin_vault_keyring(keyring_path)


def test_keyring_rejects_missing_nonregular_and_excess_entries(tmp_path):
    with pytest.raises(vault.RawTinVaultError, match="file_unavailable"):
        vault.load_raw_tin_vault_keyring(tmp_path / "missing.json")
    with pytest.raises(vault.RawTinVaultError, match="file_unavailable"):
        vault.load_raw_tin_vault_keyring(tmp_path)

    fifo_path = tmp_path / "vault-keyring.fifo"
    os.mkfifo(fifo_path)
    fifo_path.chmod(0o400)
    with pytest.raises(vault.RawTinVaultError, match="file_invalid"):
        vault.load_raw_tin_vault_keyring(fifo_path)

    excessive_key_bytes_by_id = {
        f"key-{key_index}": bytes([key_index]) * 32
        for key_index in range(vault_keyring.RAW_TIN_VAULT_KEYRING_MAX_ENTRIES + 1)
    }
    excessive_path = _owner_only_file(
        tmp_path / "excessive.json",
        _keyring_payload(
            active_key_id="key-0",
            key_bytes_by_id=excessive_key_bytes_by_id,
        ),
    )
    with pytest.raises(vault.RawTinVaultError, match="keyring_invalid"):
        vault.load_raw_tin_vault_keyring(excessive_path)
