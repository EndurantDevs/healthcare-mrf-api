# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Synthetic pure boundaries for registration authority inputs and retained receipts."""

from __future__ import annotations

import datetime as dt
import hashlib
import json
from dataclasses import replace

import pytest

import process.custom_import.registration_authority as authority
from process.custom_import.definition import canonical_json
from process.custom_import.snowflake_source_binding import SnowflakeSourceBindingReceipt
from tests.test_custom_import_snowflake_operator_cli import _loaded_binding


def _document_by_name() -> dict[str, object]:
    """Build existing synthetic connector metadata without a capability."""

    loaded = _loaded_binding()
    return {
        "dataset_key": "synthetic_authority",
        "definition": json.loads(loaded.definition.canonical),
        "source_binding": json.loads(loaded.binding.canonical),
    }


def _receipt() -> SnowflakeSourceBindingReceipt:
    """Provide one redacted result from the existing synthetic source contract."""

    return SnowflakeSourceBindingReceipt(
        dataset_id=1,
        definition_revision_id=2,
        schema_revision_id=3,
        source_binding_revision_id=4,
        revision_number=1,
        source_binding_sha256=bytes.fromhex(_loaded_binding().binding.digest),
        created=True,
    )


def test_input_digest_covers_the_full_reconstructed_envelope():
    document_by_name = _document_by_name()
    prepared = authority._registration(document_by_name)

    assert prepared.input_sha256 == hashlib.sha256(canonical_json(document_by_name).encode("utf-8")).digest()
    document_by_name["dataset_key"] = "other_authority"
    assert authority._registration(document_by_name).input_sha256 != prepared.input_sha256


def test_semantic_digest_preserves_unicode_separately_from_ascii_wire_encoding():
    # This isolates encoding; the connector independently restricts selected column names.
    document_by_name = _document_by_name()
    document_by_name["definition"]["aliases"]["root_source"] = {"識別子": "npi"}
    semantic_bytes = canonical_json(document_by_name).encode("utf-8")
    wire_bytes = json.dumps(
        document_by_name, allow_nan=False, ensure_ascii=True, separators=(",", ":"), sort_keys=True
    ).encode("ascii")

    assert authority._semantic_input_digest(document_by_name) == hashlib.sha256(semantic_bytes).digest()
    assert authority._semantic_input_digest(document_by_name) != hashlib.sha256(wire_bytes).digest()


@pytest.mark.parametrize("extra_field", ["input_sha256", "token", "expires_at", "owner"])
def test_registration_rejects_caller_digest_and_authority_fields(extra_field):
    document_by_name = _document_by_name()
    document_by_name[extra_field] = None

    with pytest.raises(authority.RegistrationAuthorityError, match="registration input is invalid"):
        authority._registration(document_by_name)


@pytest.mark.parametrize(
    "bad_digest", [32, "0" * 64, b"", b"x" * 31, b"x" * 33, None, memoryview(b"x" * 128).cast("I")]
)
def test_digest_boundary_rejects_coercions_and_wrong_lengths(bad_digest):
    with pytest.raises(authority.RegistrationAuthorityError, match="digest is invalid"):
        authority._digest(bad_digest)


def test_digest_boundary_uses_bytes_for_a_nonbyte_memoryview():
    digest = hashlib.sha256(b"synthetic binary digest").digest()

    assert authority._digest(memoryview(digest).cast("I")) == digest


def test_expiry_requires_timezone_and_preserves_the_original_instant():
    with pytest.raises(authority.RegistrationAuthorityError, match="timestamp is invalid"):
        authority._utc(dt.datetime(2030, 1, 1))
    offset = dt.timezone(dt.timedelta(hours=3))
    original = dt.datetime(2030, 1, 1, 3, tzinfo=offset)

    assert authority._utc(original) == dt.datetime(2030, 1, 1, tzinfo=dt.UTC)


@pytest.mark.parametrize("change_by_name", [{"created": 1}, {"dataset_id": True}, {"schema_revision_id": -1}])
def test_graph_result_rejects_nonredacted_or_invalid_identity(change_by_name):
    with pytest.raises(authority.RegistrationAuthorityError, match="receipt is invalid"):
        authority._graph_receipt(replace(_receipt(), **change_by_name), authority._registration(_document_by_name()))


def test_graph_result_has_the_existing_closed_redacted_shape():
    prepared = authority._registration(_document_by_name())
    rendered = authority._graph_receipt(_receipt(), prepared)
    receipt_by_name = json.loads(rendered)

    assert set(receipt_by_name) == {
        "dataset_id",
        "definition_revision_id",
        "schema_revision_id",
        "source_binding_revision_id",
        "source_binding_revision",
        "definition_sha256",
        "schema_sha256",
        "source_binding_sha256",
        "status",
    }
    assert receipt_by_name["status"] == "registered"
    assert receipt_by_name["source_binding_sha256"] == prepared.binding.digest
    assert len(rendered.encode("utf-8")) <= authority.MAX_RESULT_RECEIPT_BYTES


def test_stored_result_rejects_extra_content_before_exposing_history():
    receipt_by_name = json.loads(authority._graph_receipt(_receipt(), authority._registration(_document_by_name())))
    receipt_by_name["registration"] = _document_by_name()
    now = dt.datetime.now(dt.UTC)
    stored_by_name = {
        "authority_id": "synthetic_authority",
        "input_sha256": hashlib.sha256(b"synthetic input").digest(),
        "token_sha256": hashlib.sha256(b"synthetic digest").digest(),
        "expires_at": now + dt.timedelta(minutes=5),
        "created_at": now,
        "revoked_at": None,
        "result_receipt": canonical_json(receipt_by_name),
    }

    with pytest.raises(authority.RegistrationAuthorityError, match="stored registration receipt is invalid"):
        authority._state(stored_by_name)
