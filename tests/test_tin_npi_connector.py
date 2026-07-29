# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Token policy and secret-boundary tests for the TIN-to-NPI connector."""

from __future__ import annotations

import copy
import json
import os
import signal

import pytest

from process.tin_npi_connector import (
    TIN_TOKEN_MESSAGE_FORMAT_ID,
    TIN_TOKEN_POLICY_PREFIX,
    TinNpiConnectorError,
    TinTaxIdentityToken,
    TinTokenPolicyDescriptor,
    canonical_token_policy_id,
    load_tin_token_policy,
    normalize_ein,
    token_policy_descriptor_sha256,
)
from tests.tin_npi_connector_unit_support import (
    RELEASE_1_POLICY_DESCRIPTOR_SHA256,
    RELEASE_1_TOKEN_POLICY_ID,
    TEST_EIN,
    TEST_EIN_NORMALIZED,
    TEST_HMAC_HEX,
    TEST_SECRET,
    TOKEN_POLICY_ID,
    token_policy,
)


def test_token_policy_id_enforces_frozen_ascii_grammar_and_55_byte_limit():
    maximum_policy_id = TIN_TOKEN_POLICY_PREFIX + "a" * 32

    assert len(maximum_policy_id.encode("ascii")) == 55
    assert canonical_token_policy_id(maximum_policy_id) == maximum_policy_id

    invalid_policy_ids = (
        TIN_TOKEN_POLICY_PREFIX,
        TIN_TOKEN_POLICY_PREFIX + "a" * 33,
        TIN_TOKEN_POLICY_PREFIX + "UPPER",
        TIN_TOKEN_POLICY_PREFIX + "é",
        "other:a",
    )
    for invalid_policy_id in invalid_policy_ids:
        with pytest.raises(TinNpiConnectorError, match="policy ID is invalid"):
            canonical_token_policy_id(invalid_policy_id)


def test_release_1_policy_descriptor_matches_cross_language_vector():
    descriptor = TinTokenPolicyDescriptor(
        token_policy_id=RELEASE_1_TOKEN_POLICY_ID,
        token_policy_descriptor_sha256=RELEASE_1_POLICY_DESCRIPTOR_SHA256,
    )

    assert (
        token_policy_descriptor_sha256(RELEASE_1_TOKEN_POLICY_ID)
        == RELEASE_1_POLICY_DESCRIPTOR_SHA256
    )
    assert descriptor.public_payload() == {
        "token_policy_descriptor_sha256": RELEASE_1_POLICY_DESCRIPTOR_SHA256,
        "token_policy_id": RELEASE_1_TOKEN_POLICY_ID,
    }
    with pytest.raises(TinNpiConnectorError, match="policy descriptor is invalid"):
        TinTokenPolicyDescriptor(
            token_policy_id=RELEASE_1_TOKEN_POLICY_ID,
            token_policy_descriptor_sha256="0" * 64,
        )


@pytest.mark.parametrize(
    "raw_ein",
    (
        "012345678",
        "01-2345678",
        " \t01-2345678\r\n",
    ),
)
def test_ein_normalization_matches_ptg_ascii_alphanumeric_contract(raw_ein):
    assert normalize_ein(raw_ein) == TEST_EIN_NORMALIZED


@pytest.mark.parametrize(
    "raw_ein",
    (
        None,
        "",
        "12345678",
        "1234567890",
        "12-34AB789",
        "01 2345678",
        "01.2345678",
        "01💥2345678",
        "０１２３４５６７８",
    ),
)
def test_ein_normalization_fails_closed_without_echoing_raw_value(raw_ein):
    with pytest.raises(TinNpiConnectorError) as error:
        normalize_ein(raw_ein)

    if str(raw_ein):
        assert str(raw_ein) not in str(error.value)


def test_token_wire_vector_uses_domain_nul_independent_u16be_lengths(tmp_path):
    projector = token_policy(tmp_path)

    token = projector.tokenize_ein(TEST_EIN)

    assert token.token_policy_id == TOKEN_POLICY_ID
    assert token.tin_hmac_sha256.hex() == TEST_HMAC_HEX
    assert token.tin_id_128.hex() == TEST_HMAC_HEX[:32]
    assert token.matches_full_hmac(bytes.fromhex(TEST_HMAC_HEX))
    assert projector.public_descriptor() == {
        "message_format_id": TIN_TOKEN_MESSAGE_FORMAT_ID,
        "token_policy_descriptor_sha256": (
            token_policy_descriptor_sha256(TOKEN_POLICY_ID)
        ),
        "token_policy_id": TOKEN_POLICY_ID,
    }


def test_full_hmac_is_authoritative_after_128_bit_candidate_lookup(tmp_path):
    token = token_policy(tmp_path).tokenize_ein(TEST_EIN)
    colliding_digest = token.tin_id_128 + b"\xff" * 16
    collision = TinTaxIdentityToken(
        token_policy_id=TOKEN_POLICY_ID,
        tin_id_128=token.tin_id_128,
        tin_hmac_sha256=colliding_digest,
    )

    assert collision.tin_id_128 == token.tin_id_128
    assert not collision.matches_full_hmac(token.tin_hmac_sha256)
    assert not token.matches_full_hmac(colliding_digest)


@pytest.mark.parametrize(
    "secret_bytes",
    (
        b"",
        b"x" * 31,
        b"x" * 33,
        b"x" * 32 + b"\n",
    ),
)
def test_secret_file_requires_exactly_32_raw_bytes(tmp_path, secret_bytes):
    secret_path = tmp_path / "tin-token.key"
    secret_path.write_bytes(secret_bytes)
    secret_path.chmod(0o400)

    with pytest.raises(TinNpiConnectorError, match="secret file is invalid"):
        load_tin_token_policy(
            token_policy_id=TOKEN_POLICY_ID,
            secret_file=secret_path,
        )


@pytest.mark.parametrize("file_mode", (0o600, 0o440, 0o444))
def test_secret_file_requires_exact_0400_mode(tmp_path, file_mode):
    secret_path = tmp_path / "tin-token.key"
    secret_path.write_bytes(TEST_SECRET)
    secret_path.chmod(file_mode)

    with pytest.raises(TinNpiConnectorError, match="secret file is invalid"):
        load_tin_token_policy(
            token_policy_id=TOKEN_POLICY_ID,
            secret_file=secret_path,
        )


def test_secret_file_rejects_nonregular_path(tmp_path):
    secret_directory = tmp_path / "tin-token.key"
    secret_directory.mkdir(mode=0o400)

    with pytest.raises(TinNpiConnectorError, match="secret file is"):
        load_tin_token_policy(
            token_policy_id=TOKEN_POLICY_ID,
            secret_file=secret_directory,
        )


def test_secret_file_accepts_projected_volume_style_symbolic_link(tmp_path):
    secret_path = tmp_path / "mounted-secret.key"
    secret_path.write_bytes(TEST_SECRET)
    secret_path.chmod(0o400)
    secret_link = tmp_path / "tin-token.key"
    secret_link.symlink_to(secret_path)

    projector = load_tin_token_policy(
        token_policy_id=TOKEN_POLICY_ID,
        secret_file=secret_link,
    )

    assert projector.token_policy_id == TOKEN_POLICY_ID


def test_secret_file_rejects_fifo_without_blocking(tmp_path):
    secret_fifo = tmp_path / "tin-token.key"
    os.mkfifo(secret_fifo, mode=0o400)

    def fail_blocked_open(_signal_number, _frame):
        raise TimeoutError("secret FIFO open blocked")

    previous_handler = signal.signal(signal.SIGALRM, fail_blocked_open)
    signal.setitimer(signal.ITIMER_REAL, 1.0)
    try:
        with pytest.raises(TinNpiConnectorError, match="secret file is invalid"):
            load_tin_token_policy(
                token_policy_id=TOKEN_POLICY_ID,
                secret_file=secret_fifo,
            )
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous_handler)


def test_secret_capability_never_exposes_or_copies_protected_material(tmp_path):
    secret_bytes = b"raw-secret-material-is-32-bytes!"
    assert len(secret_bytes) == 32
    projector = token_policy(tmp_path, secret=secret_bytes)
    token = projector.tokenize_ein(TEST_EIN)
    inert_copy = copy.copy(projector)

    public_text = json.dumps(projector.public_descriptor(), sort_keys=True)
    assert secret_bytes.decode("ascii") not in repr(projector)
    assert secret_bytes.decode("ascii") not in repr(token)
    assert secret_bytes.decode("ascii") not in public_text
    assert repr(inert_copy) == "<redacted-tin-token-policy>"
    assert not hasattr(inert_copy, "tokenize_ein")
