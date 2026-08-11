"""Fail-closed boundary coverage for receipt-key authority configuration."""

from __future__ import annotations

import copy
import datetime as dt
import json
from types import SimpleNamespace

import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ec, rsa

from process import ptg_wave_receipt_authority as authority
from process import ptg_wave_receipt_key_configuration as key_configuration
from process.ptg_wave_receipt_authority import (
    LINKAGE_RECEIPT_SCHEMA,
    PTGWaveReceiptAuthorityError,
    PTGWaveReceiptKeyring,
    PTGWaveReceiptPublicEpoch,
)
from tests.ptg_wave_receipt_test_keys import EPHEMERAL_RECEIPT_PRIVATE_KEY
from tests.ptg_wave_v12_pristine_abandonment_support import keyring


def _signed_receipt(monkeypatch):
    receipt_keyring = keyring(monkeypatch)
    receipt_by_field = receipt_keyring.sign_receipt(
        schema=LINKAGE_RECEIPT_SCHEMA,
        key_id=receipt_keyring.active_key_id,
        issued_at="2026-08-10T12:34:56.123456Z",
        receipt_payload={"operation_id": "a" * 64},
    )
    return receipt_keyring, receipt_by_field


def test_receipt_signing_rejects_schema_payload_and_signature_shape(monkeypatch):
    receipt_keyring = keyring(monkeypatch)
    common_keywords_by_field = {
        "key_id": receipt_keyring.active_key_id,
        "issued_at": "2026-08-10T12:34:56.123456Z",
    }
    with pytest.raises(PTGWaveReceiptAuthorityError, match="schema is unsupported"):
        receipt_keyring.sign_receipt(
            schema="unsupported",
            receipt_payload={},
            **common_keywords_by_field,
        )
    with pytest.raises(PTGWaveReceiptAuthorityError, match="payload must be an object"):
        receipt_keyring.sign_receipt(
            schema=LINKAGE_RECEIPT_SCHEMA,
            receipt_payload=[],
            **common_keywords_by_field,
        )

    monkeypatch.setattr(
        authority,
        "_HEX_512",
        SimpleNamespace(fullmatch=lambda _signature: None),
    )
    with pytest.raises(PTGWaveReceiptAuthorityError, match="signature shape"):
        receipt_keyring.sign_receipt(
            schema=LINKAGE_RECEIPT_SCHEMA,
            receipt_payload={},
            **common_keywords_by_field,
        )


def test_receipt_timestamps_and_messages_reject_closed_invalid_inputs():
    invalid_timestamps = (
        object(),
        "2026-02-30T00:00:00.000000Z",
    )
    for invalid_timestamp in invalid_timestamps:
        with pytest.raises(PTGWaveReceiptAuthorityError, match="issued_at is invalid"):
            authority.canonical_receipt_timestamp(invalid_timestamp)

    with pytest.raises(PTGWaveReceiptAuthorityError, match="schema is unsupported"):
        authority.signed_receipt_message(
            schema="unsupported",
            key_id="receipt-active",
            issued_at="2026-08-10T12:34:56.123456Z",
            payload={},
        )

    aware_timestamp = dt.datetime(2026, 8, 10, 14, 34, 56, tzinfo=dt.timezone.utc)
    assert authority.canonical_receipt_timestamp(aware_timestamp).endswith(".000000Z")


def test_receipt_envelope_rejects_shape_schema_payload_and_digest(monkeypatch):
    _receipt_keyring, receipt_by_field = _signed_receipt(monkeypatch)
    invalid_receipts = (
        None,
        {**receipt_by_field, "schema": "unsupported"},
        {**receipt_by_field, "payload": []},
        {**receipt_by_field, "payload_digest": None},
        {**receipt_by_field, "signature": "0"},
    )
    for invalid_receipt in invalid_receipts:
        with pytest.raises(PTGWaveReceiptAuthorityError):
            authority.validate_receipt_envelope(
                invalid_receipt,
                schema=LINKAGE_RECEIPT_SCHEMA,
            )


def test_stored_receipt_rejects_binding_and_missing_public_epoch(monkeypatch):
    receipt_keyring, receipt_by_field = _signed_receipt(monkeypatch)
    with pytest.raises(PTGWaveReceiptAuthorityError, match="immutable binding"):
        receipt_keyring.validate_stored_receipt(
            receipt_by_field,
            schema=LINKAGE_RECEIPT_SCHEMA,
            key_id=receipt_keyring.active_key_id,
            expected_payload={"operation_id": "b" * 64},
        )

    keyring_without_public_epoch = PTGWaveReceiptKeyring(
        active_key_id=receipt_keyring.active_key_id,
        signing_by_key_id=receipt_keyring.signing_by_key_id,
        public_by_key_id={},
    )
    with pytest.raises(PTGWaveReceiptAuthorityError, match="public key epoch"):
        keyring_without_public_epoch.validate_stored_receipt(
            receipt_by_field,
            schema=LINKAGE_RECEIPT_SCHEMA,
            key_id=receipt_keyring.active_key_id,
            expected_payload=receipt_by_field["payload"],
        )


def test_public_projection_requires_one_active_epoch(monkeypatch):
    receipt_keyring = keyring(monkeypatch)
    active_epoch = receipt_keyring.public_by_key_id[receipt_keyring.active_key_id]
    retired_epoch = PTGWaveReceiptPublicEpoch(
        **{**active_epoch.as_mapping(), "state": "retired"}
    )
    invalid_keyring = PTGWaveReceiptKeyring(
        active_key_id=receipt_keyring.active_key_id,
        signing_by_key_id=receipt_keyring.signing_by_key_id,
        public_by_key_id={receipt_keyring.active_key_id: retired_epoch},
    )

    with pytest.raises(PTGWaveReceiptAuthorityError, match="exactly one active"):
        invalid_keyring.public_epochs_mapping()


def test_receipt_key_coverage_rejects_collection_and_row_shapes(monkeypatch):
    receipt_keyring = keyring(monkeypatch)
    invalid_calls = (
        lambda: authority.require_nonterminal_signing_key_coverage(
            "receipt-active",
            keyring=receipt_keyring,
        ),
        lambda: authority.require_persisted_receipt_key_coverage(
            "rows",
            (),
            keyring=receipt_keyring,
        ),
        lambda: authority.require_persisted_receipt_key_coverage(
            [(receipt_keyring.active_key_id,)],
            (),
            keyring=receipt_keyring,
        ),
        lambda: authority.require_receipt_key_id(None),
    )
    for invalid_call in invalid_calls:
        with pytest.raises(PTGWaveReceiptAuthorityError):
            invalid_call()


def test_signing_configuration_rejects_duplicate_active_epoch(monkeypatch):
    monkeypatch.setattr(
        key_configuration,
        "_load_signing_epoch",
        lambda key_id, _path: key_id,
    )
    retained_configuration = json.dumps(
        {"receipt-active": str(EPHEMERAL_RECEIPT_PRIVATE_KEY.resolve())}
    )
    with pytest.raises(PTGWaveReceiptAuthorityError, match="cannot also be retained"):
        key_configuration._configured_signing_epochs(
            "receipt-active",
            str(EPHEMERAL_RECEIPT_PRIVATE_KEY.resolve()),
            retained_configuration,
        )


def test_bounded_key_file_reads_reject_empty_and_missing_files(tmp_path):
    empty_path = tmp_path / "empty.pem"
    empty_path.write_bytes(b"")
    with pytest.raises(PTGWaveReceiptAuthorityError, match="is invalid"):
        key_configuration._read_bounded(empty_path, 100, "test key")
    with pytest.raises(PTGWaveReceiptAuthorityError, match="is unavailable"):
        key_configuration._read_bounded(tmp_path / "missing.pem", 100, "test key")


@pytest.mark.parametrize(
    "raw_configuration",
    (
        [],
        "not-json",
        "[]",
        json.dumps({f"retained-{index}": "/tmp/key" for index in range(8)}),
    ),
)
def test_retained_key_configuration_rejects_invalid_shapes(raw_configuration):
    with pytest.raises(PTGWaveReceiptAuthorityError, match="configuration is invalid"):
        key_configuration._retained_private_key_paths(raw_configuration)


def _retired_epoch_mapping(receipt_keyring) -> dict[str, object]:
    public_epoch = receipt_keyring.public_by_key_id[receipt_keyring.active_key_id]
    return {
        "key_id": "receipt-retired",
        "rsa_modulus": public_epoch.rsa_modulus,
        "rsa_exponent": public_epoch.rsa_exponent,
        "state": "retired",
    }


def test_retired_epoch_files_reject_shape_count_and_duplicates(monkeypatch, tmp_path):
    receipt_keyring = keyring(monkeypatch)
    retired_epoch_by_field = _retired_epoch_mapping(receipt_keyring)
    invalid_documents = (
        {},
        [retired_epoch_by_field] * 8,
        [retired_epoch_by_field, copy.deepcopy(retired_epoch_by_field)],
    )
    for index, invalid_document in enumerate(invalid_documents):
        retired_path = tmp_path / f"retired-{index}.json"
        retired_path.write_text(json.dumps(invalid_document))
        with pytest.raises(PTGWaveReceiptAuthorityError):
            key_configuration._retired_public_epochs(str(retired_path.resolve()))


def test_retired_epoch_entry_rejects_fields_and_public_material(monkeypatch):
    receipt_keyring = keyring(monkeypatch)
    retired_epoch_by_field = _retired_epoch_mapping(receipt_keyring)
    invalid_entries = (
        {**retired_epoch_by_field, "extra": True},
        {**retired_epoch_by_field, "state": "active"},
        {**retired_epoch_by_field, "rsa_modulus": "0" * 512},
    )
    for invalid_entry in invalid_entries:
        with pytest.raises(PTGWaveReceiptAuthorityError):
            key_configuration._validated_retired_epoch(invalid_entry)


def test_public_epoch_configuration_rejects_conflict_count_and_alias(monkeypatch):
    receipt_keyring = keyring(monkeypatch)
    active_key_id = receipt_keyring.active_key_id
    active_signing_epochs = receipt_keyring.signing_by_key_id
    active_public_epoch = receipt_keyring.public_by_key_id[active_key_id]

    conflicting_epoch = PTGWaveReceiptPublicEpoch(
        **{**active_public_epoch.as_mapping(), "state": "retired"}
    )
    monkeypatch.setattr(
        key_configuration,
        "_retired_public_epochs",
        lambda _path: (conflicting_epoch,),
    )
    with pytest.raises(PTGWaveReceiptAuthorityError, match="definitions conflict"):
        key_configuration._configured_public_epochs(
            active_key_id,
            active_signing_epochs,
            "ignored",
        )

    excessive_epochs = tuple(
        PTGWaveReceiptPublicEpoch(
            key_id=f"retired-{index}",
            rsa_modulus=f"{(1 << 2047) + (index * 2) + 1:0512x}",
            rsa_exponent=65537,
            state="retired",
        )
        for index in range(8)
    )
    monkeypatch.setattr(
        key_configuration,
        "_retired_public_epochs",
        lambda _path: excessive_epochs,
    )
    with pytest.raises(PTGWaveReceiptAuthorityError, match="epoch count"):
        key_configuration._configured_public_epochs(
            active_key_id,
            active_signing_epochs,
            "ignored",
        )

    aliased_epoch = PTGWaveReceiptPublicEpoch(
        key_id="receipt-alias",
        rsa_modulus=active_public_epoch.rsa_modulus,
        rsa_exponent=active_public_epoch.rsa_exponent,
        state="retired",
    )
    monkeypatch.setattr(
        key_configuration,
        "_retired_public_epochs",
        lambda _path: (aliased_epoch,),
    )
    with pytest.raises(PTGWaveReceiptAuthorityError, match="identify one epoch"):
        key_configuration._configured_public_epochs(
            active_key_id,
            active_signing_epochs,
            "ignored",
        )


def test_private_key_loader_rejects_non_rsa_and_wrong_rsa_size(tmp_path):
    private_keys = (
        ec.generate_private_key(ec.SECP256R1()),
        rsa.generate_private_key(public_exponent=65537, key_size=1024),
    )
    expected_messages = ("must be RSA", "must be RSA-2048")
    for index, (private_key, expected_message) in enumerate(
        zip(private_keys, expected_messages)
    ):
        private_path = tmp_path / f"invalid-{index}.pem"
        private_path.write_bytes(
            private_key.private_bytes(
                serialization.Encoding.PEM,
                serialization.PrivateFormat.PKCS8,
                serialization.NoEncryption(),
            )
        )
        with pytest.raises(PTGWaveReceiptAuthorityError, match=expected_message):
            key_configuration._load_signing_epoch("receipt-test", private_path)
