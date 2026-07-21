# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import copy
import json
import pickle

import pytest

from process import provider_directory_retained_artifact_keys as artifact_keys
from process.provider_directory_retained_artifact_base import (
    RetainedArtifactError,
    canonical_json,
    require_digest,
    require_nonnegative_int,
    require_positive_int,
    require_safe_id,
    sha256_json,
)
from process.provider_directory_retained_artifact_keys import (
    LOCATOR_PRIVATE_VALUE,
    MAX_PRIVATE_CIPHERTEXT_CHARS,
    MAX_PRIVATE_KEYRING_BYTES,
    MAX_PRIVATE_KEYRING_ENTRIES,
    MAX_PRIVATE_VALUE_BYTES,
    VALIDATOR_PRIVATE_VALUE,
    _OpenedPrivateValue,
    _consume_private_value,
    _context_secret_bytes,
    _primary_key_context,
    _previous_keyring,
    _validated_key_context,
    active_private_key_id,
    configured_private_key_ids,
    private_item_binding,
)


KEY_ID_ENV = "HLTHPRT_PROVIDER_DIRECTORY_RETAINED_ARTIFACT_KEY_ID"
KEY_ENV = "HLTHPRT_PROVIDER_DIRECTORY_RETAINED_ARTIFACT_KEY"
PREVIOUS_ENV = "HLTHPRT_PROVIDER_DIRECTORY_RETAINED_ARTIFACT_PREVIOUS_KEYRING"
PRIVATE_BINDING = private_item_binding(
    campaign_id="a" * 64,
    source_item_id="b" * 64,
    endpoint_id="c" * 64,
)


def seal_private_value(value, *, purpose, binding_sha256=PRIVATE_BINDING):
    return artifact_keys.seal_private_value(
        value,
        purpose=purpose,
        binding_sha256=binding_sha256,
    )


def _open_private_value(ciphertext, *, purpose, binding_sha256=PRIVATE_BINDING):
    return artifact_keys._open_private_value(
        ciphertext,
        purpose=purpose,
        binding_sha256=binding_sha256,
    )


def private_identity_hmac(
    value,
    *,
    purpose,
    binding_sha256=PRIVATE_BINDING,
    key_id=None,
):
    return artifact_keys.private_identity_hmac(
        value,
        purpose=purpose,
        binding_sha256=binding_sha256,
        key_id=key_id,
    )


def _is_private_identity_valid(
    opened,
    identity,
    *,
    purpose,
    binding_sha256=PRIVATE_BINDING,
):
    return artifact_keys._is_private_identity_valid(
        opened,
        identity,
        purpose=purpose,
        binding_sha256=binding_sha256,
    )


@pytest.fixture
def retained_keyring(monkeypatch):
    monkeypatch.setenv(KEY_ID_ENV, "current-v2")
    monkeypatch.setenv(KEY_ENV, "c" * 32)
    monkeypatch.delenv(PREVIOUS_ENV, raising=False)


def test_base_contract_normalizes_errors_and_canonical_values():
    assert str(RetainedArtifactError("not safe!")) == "retained_artifact_failed"
    assert canonical_json({"b": 2, "a": 1}) == b'{"a":1,"b":2}'
    assert len(sha256_json({"a": 1})) == 64
    with pytest.raises(RetainedArtifactError, match="retained_contract_value_invalid"):
        canonical_json({"unsupported": object()})


@pytest.mark.parametrize(
    ("document", "limits"),
    (
        ({"too_large": "x" * 20}, {"maximum_bytes": 16}),
        ({"unicode": "é" * 9}, {"maximum_bytes": 16}),
        ({"escaped": "\x01" * 5}, {"maximum_bytes": 12}),
        ({"too_many": [None, None]}, {"maximum_nodes": 2}),
        ({"too_deep": [[True]]}, {"maximum_depth": 1}),
        ({1: "non-string-key"}, {}),
        ({"tuple": (1, 2)}, {}),
        ({"huge_int": 2**63}, {}),
        ({"nan": float("nan")}, {}),
        ({"surrogate": "\ud800"}, {}),
    ),
)
def test_canonical_json_rejects_unbounded_or_non_json_trees(document, limits):
    with pytest.raises(RetainedArtifactError, match="retained_contract_value_invalid"):
        canonical_json(document, **limits)


def test_canonical_json_rejects_repeated_containers_and_text_budget():
    shared_list: list[object] = []
    with pytest.raises(RetainedArtifactError, match="retained_contract_value_invalid"):
        canonical_json({"first": shared_list, "second": shared_list})
    with pytest.raises(RetainedArtifactError, match="retained_contract_value_invalid"):
        canonical_json(
            {"first": "1234", "second": "5678"},
            maximum_bytes=20,
        )
    assert canonical_json({"values": [None, True, 1, 1.5, "ok"]})


@pytest.mark.parametrize("candidate", [None, "A" * 64, "a" * 63, "g" * 64])
def test_digest_rejects_non_sha256_values(candidate):
    with pytest.raises(RetainedArtifactError, match="digest_invalid"):
        require_digest(candidate, "digest")
    assert require_digest("a" * 64, "digest") == "a" * 64


@pytest.mark.parametrize("candidate", [None, "bad value", "x" * 4])
def test_safe_id_rejects_wrong_type_pattern_or_length(candidate):
    with pytest.raises(RetainedArtifactError, match="name_invalid"):
        require_safe_id(candidate, "name", maximum=3)
    assert require_safe_id("ok-1", "name") == "ok-1"


@pytest.mark.parametrize("candidate", [True, "1", -1, 2**63])
def test_bounded_integer_contracts_reject_invalid_values(candidate):
    with pytest.raises(RetainedArtifactError, match="count_invalid"):
        require_nonnegative_int(candidate, "count")
    assert require_nonnegative_int(0, "count") == 0
    with pytest.raises(RetainedArtifactError, match="count_invalid"):
        require_positive_int(0, "count")
    assert require_positive_int(1, "count") == 1


@pytest.mark.parametrize(
    ("key_id", "secret", "code"),
    [
        (7, "s" * 32, "retained_artifact_key_id_invalid"),
        ("bad:key", "s" * 32, "retained_artifact_key_id_invalid"),
        ("valid", 7, "retained_artifact_key_missing"),
        ("valid", "short", "retained_artifact_key_missing"),
    ],
)
def test_key_context_rejects_invalid_identifiers_and_secrets(key_id, secret, code):
    with pytest.raises(RetainedArtifactError, match=code):
        _validated_key_context(key_id, secret)
    context = _validated_key_context("valid", f" {'s' * 32} ")
    assert _context_secret_bytes(context) == b"s" * 32


def test_private_capability_snapshot_is_permanently_inert(retained_keyring) -> None:
    context = _primary_key_context()
    snapshot = copy.copy(context)
    assert copy.copy(snapshot) is snapshot
    assert copy.deepcopy(snapshot) is snapshot
    assert repr(pickle.loads(pickle.dumps(snapshot))) == repr(snapshot)
    with pytest.raises(AttributeError, match="retained_private_purpose_immutable"):
        LOCATOR_PRIVATE_VALUE.name = "validator"


def test_key_context_rejects_non_unicode_secret() -> None:
    with pytest.raises(RetainedArtifactError, match="retained_artifact_key_missing"):
        _validated_key_context("valid", "\ud800" * 32)


@pytest.mark.parametrize(
    "document",
    [
        "{",
        '["not", "a", "mapping"]',
        '{"old":"sufficient-secret-material-123456"',
        '{"old":"sufficient-secret-material-123456",'
        '"old":"different-secret-material-123456"}',
        '{"bad:key":"sufficient-secret-material-123456"}',
        '{"old":"short"}',
    ],
)
def test_previous_keyring_rejects_malformed_documents(
    monkeypatch,
    retained_keyring,
    document,
):
    monkeypatch.setenv(PREVIOUS_ENV, document)
    with pytest.raises(
        RetainedArtifactError, match="retained_artifact_keyring_invalid"
    ):
        _previous_keyring()


def test_keyring_reports_ids_and_rejects_primary_duplication(
    monkeypatch,
    retained_keyring,
):
    assert active_private_key_id() == "current-v2"
    assert configured_private_key_ids() == ("current-v2",)
    monkeypatch.setenv(PREVIOUS_ENV, json.dumps({"old-v1": "o" * 32}))
    assert configured_private_key_ids() == ("current-v2", "old-v1")
    monkeypatch.setenv(PREVIOUS_ENV, json.dumps({"current-v2": "o" * 32}))
    with pytest.raises(
        RetainedArtifactError, match="retained_artifact_keyring_invalid"
    ):
        configured_private_key_ids()


@pytest.mark.parametrize("private_value", [None, "", "line\nbreak"])
def test_sealing_rejects_invalid_private_values(retained_keyring, private_value):
    with pytest.raises(RetainedArtifactError, match="retained_private_value_invalid"):
        seal_private_value(private_value, purpose=LOCATOR_PRIVATE_VALUE)


@pytest.mark.parametrize(
    "ciphertext",
    [
        None,
        "wrong-prefix",
        "pdart2:legacy-v2:token",
        "pdart3:no-separator",
        "pdart3:-bad:token",
        "pdart3:key:",
    ],
)
def test_open_rejects_malformed_ciphertext(retained_keyring, ciphertext):
    with pytest.raises(
        RetainedArtifactError, match="retained_private_ciphertext_invalid"
    ):
        _open_private_value(ciphertext, purpose=LOCATOR_PRIVATE_VALUE)


def test_open_rejects_nonascii_unknown_and_tampered_tokens(
    monkeypatch,
    retained_keyring,
):
    with pytest.raises(
        RetainedArtifactError, match="retained_private_ciphertext_invalid"
    ):
        _open_private_value(
            "pdart3:current-v2:é",
            purpose=LOCATOR_PRIVATE_VALUE,
        )
    with pytest.raises(RetainedArtifactError, match="retained_private_key_unavailable"):
        _open_private_value(
            "pdart3:missing-v1:token",
            purpose=LOCATOR_PRIVATE_VALUE,
        )
    with pytest.raises(
        RetainedArtifactError, match="retained_private_decryption_failed"
    ):
        _open_private_value(
            "pdart3:current-v2:tampered-token",
            purpose=LOCATOR_PRIVATE_VALUE,
        )
    monkeypatch.setenv(PREVIOUS_ENV, json.dumps({"old-v1": "o" * 32}))
    monkeypatch.setenv(KEY_ID_ENV, "old-v1")
    monkeypatch.setenv(KEY_ENV, "o" * 32)
    old_ciphertext = seal_private_value(
        "old capability",
        purpose=LOCATOR_PRIVATE_VALUE,
    )
    monkeypatch.setenv(KEY_ID_ENV, "current-v2")
    monkeypatch.setenv(KEY_ENV, "c" * 32)
    opened = _open_private_value(
        old_ciphertext,
        purpose=LOCATOR_PRIVATE_VALUE,
    )
    assert opened.key_id == "old-v1"
    assert (
        _consume_private_value(
            opened,
            purpose=LOCATOR_PRIVATE_VALUE,
        )
        == "old capability"
    )


def test_private_identity_is_key_scoped_and_fail_closed(
    monkeypatch,
    retained_keyring,
):
    ciphertext = seal_private_value(
        "private locator",
        purpose=LOCATOR_PRIVATE_VALUE,
    )
    opened = _open_private_value(ciphertext, purpose=LOCATOR_PRIVATE_VALUE)
    identity = private_identity_hmac(
        "private locator",
        purpose=LOCATOR_PRIVATE_VALUE,
    )
    assert _is_private_identity_valid(
        opened,
        identity,
        purpose=LOCATOR_PRIVATE_VALUE,
    )
    assert not _is_private_identity_valid(
        opened,
        7,
        purpose=LOCATOR_PRIVATE_VALUE,
    )
    wrong_suffix = "0" if identity[-1] != "0" else "1"
    assert not _is_private_identity_valid(
        opened,
        identity[:-1] + wrong_suffix,
        purpose=LOCATOR_PRIVATE_VALUE,
    )
    with pytest.raises(RetainedArtifactError, match="retained_private_value_invalid"):
        private_identity_hmac("", purpose=LOCATOR_PRIVATE_VALUE)
    with pytest.raises(RetainedArtifactError, match="retained_private_key_unavailable"):
        private_identity_hmac(
            "private locator",
            purpose=LOCATOR_PRIVATE_VALUE,
            key_id="missing-v1",
        )
    monkeypatch.setenv(PREVIOUS_ENV, json.dumps({"old-v1": "o" * 32}))
    assert private_identity_hmac(
        "private locator",
        purpose=LOCATOR_PRIVATE_VALUE,
        key_id="old-v1",
    ).startswith("pdhmac3:old-v1:")


@pytest.mark.parametrize(
    "private_value",
    (
        "x" * (MAX_PRIVATE_VALUE_BYTES + 1),
        "é" * (MAX_PRIVATE_VALUE_BYTES // 2 + 1),
    ),
)
def test_private_crypto_rejects_oversized_character_and_byte_inputs(
    retained_keyring,
    private_value,
) -> None:
    with pytest.raises(RetainedArtifactError, match="retained_private_value_invalid"):
        seal_private_value(private_value, purpose=LOCATOR_PRIVATE_VALUE)
    with pytest.raises(RetainedArtifactError, match="retained_private_value_invalid"):
        private_identity_hmac(private_value, purpose=LOCATOR_PRIVATE_VALUE)
    with pytest.raises(
        RetainedArtifactError,
        match="retained_private_ciphertext_invalid",
    ):
        _open_private_value(
            "x" * (MAX_PRIVATE_CIPHERTEXT_CHARS + 1),
            purpose=LOCATOR_PRIVATE_VALUE,
        )


def test_private_keyring_entry_count_is_bounded(monkeypatch, retained_keyring) -> None:
    key_material_by_id = {
        f"old-{key_index}": "o" * 32
        for key_index in range(MAX_PRIVATE_KEYRING_ENTRIES + 1)
    }
    monkeypatch.setenv(PREVIOUS_ENV, json.dumps(key_material_by_id))
    with pytest.raises(
        RetainedArtifactError,
        match="retained_artifact_keyring_invalid",
    ):
        configured_private_key_ids()


@pytest.mark.parametrize(
    "raw_keyring",
    (
        "x" * (MAX_PRIVATE_KEYRING_BYTES + 1),
        "é" * (MAX_PRIVATE_KEYRING_BYTES // 2 + 1),
    ),
)
def test_private_keyring_document_size_is_bounded(
    monkeypatch,
    retained_keyring,
    raw_keyring,
) -> None:
    monkeypatch.setenv(PREVIOUS_ENV, raw_keyring)
    with pytest.raises(
        RetainedArtifactError,
        match="retained_artifact_keyring_invalid",
    ):
        _previous_keyring()


def test_private_keyring_rejects_non_unicode_document(
    monkeypatch,
    retained_keyring,
) -> None:
    original_getenv = artifact_keys.os.getenv

    def _getenv(name: str, default=None):
        if name == PREVIOUS_ENV:
            return "\ud800"
        return original_getenv(name, default)

    monkeypatch.setattr(artifact_keys.os, "getenv", _getenv)
    with pytest.raises(
        RetainedArtifactError,
        match="retained_artifact_keyring_invalid",
    ):
        _previous_keyring()


def test_private_crypto_rejects_non_unicode_plaintext_and_decrypted_bytes(
    retained_keyring,
) -> None:
    with pytest.raises(RetainedArtifactError, match="retained_private_value_invalid"):
        seal_private_value("\ud800", purpose=LOCATOR_PRIVATE_VALUE)
    context = _primary_key_context()
    invalid_token = artifact_keys._fernet(
        context,
        LOCATOR_PRIVATE_VALUE,
        PRIVATE_BINDING,
    ).encrypt(b"\xff")
    invalid_ciphertext = f"pdart3:{context.key_id}:{invalid_token.decode('ascii')}"
    with pytest.raises(
        RetainedArtifactError,
        match="retained_private_decryption_failed",
    ):
        _open_private_value(
            invalid_ciphertext,
            purpose=LOCATOR_PRIVATE_VALUE,
        )


def test_private_ciphertext_and_identity_are_purpose_separated(
    retained_keyring,
) -> None:
    private_value = '"purpose-separated-etag"'
    locator_ciphertext = seal_private_value(
        private_value,
        purpose=LOCATOR_PRIVATE_VALUE,
    )
    with pytest.raises(
        RetainedArtifactError,
        match="retained_private_decryption_failed",
    ):
        _open_private_value(
            locator_ciphertext,
            purpose=VALIDATOR_PRIVATE_VALUE,
        )
    assert private_identity_hmac(
        private_value,
        purpose=LOCATOR_PRIVATE_VALUE,
    ) != private_identity_hmac(
        private_value,
        purpose=VALIDATOR_PRIVATE_VALUE,
    )
