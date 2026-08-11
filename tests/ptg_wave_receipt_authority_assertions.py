"""Focused setup and assertions for public receipt-authority tests."""

from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest

from process.ptg_wave_ordinary_terminal_receipt import (
    ORDINARY_TERMINAL_PAYLOAD_FIELDS,
    validate_ordinary_terminal_request,
)
from process.ptg_wave_receipt_authority import (
    ABANDONMENT_RECEIPT_SCHEMA,
    ACTIVE_KEY_ID_ENV,
    ACTIVE_PRIVATE_KEY_FILE_ENV,
    LINKAGE_RECEIPT_SCHEMA,
    ORDINARY_TERMINAL_RECEIPT_SCHEMA,
    PTGWaveReceiptAuthorityError,
    PTGWaveReceiptKeyring,
    PTGWaveReceiptPublicEpoch,
    RETAINED_PRIVATE_KEY_FILES_ENV,
    RETIRED_PUBLIC_EPOCHS_FILE_ENV,
    signed_receipt_message,
)
from process.ptg_wave_state import canonical_json, sha256_digest
from process.ptg_wave_v12_pristine_abandonment import (
    proof_signing_bytes,
    validate_v12_pristine_abandonment_proof,
)
from tests.ptg_wave_receipt_test_keys import write_ephemeral_receipt_private_key


def build_rotating_and_historical_keyrings(monkeypatch, tmp_path, fixed_key):
    """Build retained-private and public-only views of one retired epoch."""
    new_key = write_ephemeral_receipt_private_key(tmp_path / "new-active.pem")
    _configure_active(monkeypatch, "epoch-new", new_key)
    monkeypatch.setenv(
        RETAINED_PRIVATE_KEY_FILES_ENV,
        json.dumps({"epoch-old": str(fixed_key.resolve())}),
    )
    rotating = PTGWaveReceiptKeyring.from_environment()
    old_public = rotating.public_by_key_id["epoch-old"]
    public_file = tmp_path / "retired-public.json"
    public_file.write_text(
        json.dumps([old_public.as_mapping()]),
        encoding="utf-8",
    )
    _configure_active(monkeypatch, "epoch-new", new_key)
    monkeypatch.setenv(RETIRED_PUBLIC_EPOCHS_FILE_ENV, str(public_file.resolve()))
    historical = PTGWaveReceiptKeyring.from_environment()
    return new_key, rotating, historical, old_public


def pinned_receipt_epoch_rows(public_epoch):
    """Project one public epoch into the persisted coverage-row shape."""
    return [
        (
            public_epoch.key_id,
            public_epoch.rsa_modulus,
            public_epoch.rsa_exponent,
        )
    ]


def _configure_active(monkeypatch, key_id: str, path: Path) -> None:
    monkeypatch.setenv(ACTIVE_KEY_ID_ENV, key_id)
    monkeypatch.setenv(ACTIVE_PRIVATE_KEY_FILE_ENV, str(path.resolve()))
    monkeypatch.delenv(RETAINED_PRIVATE_KEY_FILES_ENV, raising=False)
    monkeypatch.delenv(RETIRED_PUBLIC_EPOCHS_FILE_ENV, raising=False)


def assert_shared_receipt_fixture(root: Path, fixture_path: Path) -> None:
    """Validate the public-only shared fixture and every signed family."""
    fixture, keyring = _loaded_fixture_keyring(root, fixture_path)
    _assert_fixture_receipt_sections(fixture, keyring)
    _assert_ordinary_terminal_fixture(fixture, keyring)
    proof = validate_v12_pristine_abandonment_proof(
        fixture["abandonment"]["proof"]
    )
    assert proof_signing_bytes(proof).hex() == fixture["abandonment"][
        "proof_signing_bytes_hex"
    ]


def _loaded_fixture_keyring(root: Path, fixture_path: Path):
    raw_fixture = fixture_path.read_bytes()
    fixture = json.loads(raw_fixture)
    assert raw_fixture == canonical_json(fixture) + b"\n"
    assert sha256_digest(raw_fixture) == (
        "701b913369f4896b5ea943844d12519a73ffd8da276dd1ad1bd71fd68692a5da"
    )
    public_epoch = PTGWaveReceiptPublicEpoch(**fixture["key_epoch"])
    keyring = PTGWaveReceiptKeyring(
        active_key_id=public_epoch.key_id,
        signing_by_key_id={},
        public_by_key_id={public_epoch.key_id: public_epoch},
    )
    assert keyring.public_epochs_mapping()["epochs"] == [fixture["key_epoch"]]
    assert not (
        root / "tests" / "fixtures" / "ptg_wave_receipt_test_private_key.pem"
    ).exists()
    return fixture, keyring


def _assert_fixture_receipt_sections(fixture, keyring) -> None:
    for section_name, schema in (
        ("linkage", LINKAGE_RECEIPT_SCHEMA),
        ("abandonment", ABANDONMENT_RECEIPT_SCHEMA),
        ("ordinary_terminal", ORDINARY_TERMINAL_RECEIPT_SCHEMA),
    ):
        section = fixture[section_name]
        receipt = section["receipt"]
        assert signed_receipt_message(
            schema=schema,
            **section["signed_material"],
        ).hex() == section["signing_bytes_hex"]
        assert keyring.validate_stored_receipt(
            receipt, schema=schema, key_id=receipt["key_id"],
            expected_payload=receipt["payload"],
        ) == receipt
        forged = copy.deepcopy(receipt)
        forged["signature"] = (
            f"{int(forged['signature'][0], 16) ^ 1:x}" + forged["signature"][1:]
        )
        with pytest.raises(PTGWaveReceiptAuthorityError, match="signature is invalid"):
            keyring.validate_stored_receipt(
                forged, schema=schema, key_id=receipt["key_id"],
                expected_payload=receipt["payload"],
            )


def _assert_ordinary_terminal_fixture(fixture, keyring) -> None:
    terminal = fixture["ordinary_terminal"]
    assert validate_ordinary_terminal_request(terminal["request"]) == terminal["request"]
    assert terminal["payload"] == terminal["receipt"]["payload"]
    assert set(terminal["payload"]) == ORDINARY_TERMINAL_PAYLOAD_FIELDS
    with pytest.raises(PTGWaveReceiptAuthorityError, match="unavailable for signing"):
        keyring.sign_receipt(
            schema=ORDINARY_TERMINAL_RECEIPT_SCHEMA,
            key_id=terminal["receipt"]["key_id"],
            issued_at=terminal["receipt"]["issued_at"],
            receipt_payload=terminal["payload"],
        )
