# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Synthetic, portable regression checks for the closed admission envelope."""

import base64
import hashlib
import hmac
import json
from dataclasses import FrozenInstanceError, fields
from datetime import datetime, timedelta, timezone

import pytest

import process.custom_import.admission_authorization as auth

NOW = datetime(2030, 1, 1, tzinfo=timezone.utc)
ORIGIN = "https://engine.example.invalid"
CURRENT_KEY = b"synthetic-current-key-material!!"
PREVIOUS_KEY = b"synthetic-previous-key-material!"
TOKEN = b"opaque-test-token-DO-NOT-LOG\x00\xff"


def canonical(document):
    return json.dumps(document, ensure_ascii=True, allow_nan=False, sort_keys=True, separators=(",", ":")).encode(
        "ascii"
    )


def b64(value):
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode("ascii")


def permit(**changes):
    return {
        "audience": "custom-import-engine",
        "contract": "custom-import-admission-permit/v1",
        "dataset_id": 11,
        "definition_revision_id": 12,
        "expires_at": "2030-01-01T00:15:00Z",
        "idempotency_key": "synthetic-run-001",
        "issued_at": "2030-01-01T00:00:00Z",
        "issuer": "custom-import-execution-controller",
        "method": "POST",
        "origin": ORIGIN,
        "path": "/control/v1/custom-import/admission-batch",
        "schema_revision_id": 13,
        "source_binding_revision_id": 14,
        "source_binding_sha256": "a" * 64,
        **changes,
    }


def body(**changes):
    return {"build_id": 51, "execution_id": 40, "expected_after_occurrence_id": 0, "fence": 7, **changes}


def keyring_document(**changes):
    return {
        "active_key_id": "current",
        "contract": "custom-import-admission-keyring/v1",
        "keys": [
            {"key_id": "current", "key_base64url": b64(CURRENT_KEY)},
            {"key_id": "previous", "key_base64url": b64(PREVIOUS_KEY)},
        ],
        **changes,
    }


def keyring(**changes):
    return auth.load_keyring(canonical(keyring_document(**changes)))


def signed_headers(context=None, *, key_id="current", key=CURRENT_KEY, token=TOKEN, domain=None):
    """Independent framing also permits signing intentionally invalid documents."""

    if context is None:
        context = canonical(permit())
    encoded_id = key_id.encode("ascii")
    framing = (
        (domain if domain is not None else b"CUSTOM_IMPORT_ADMISSION_PERMIT_V1\x00")
        + len(encoded_id).to_bytes(2, "big")
        + encoded_id
        + len(context).to_bytes(8, "big")
        + context
    )
    return [
        ("Content-Type", "application/json"),
        ("Authorization", "Bearer " + b64(token)),
        (auth.CONTEXT_HEADER, b64(context)),
        (auth.KEY_ID_HEADER, key_id),
        (auth.SIGNATURE_HEADER, b64(hmac.new(key, framing, hashlib.sha256).digest())),
    ]


def replacing(headers, name, value):
    return [
        (
            name if candidate.lower() == name.lower() else candidate,
            value if candidate.lower() == name.lower() else supplied,
        )
        for candidate, supplied in headers
    ]


def verify(**changes):
    argument_by_name = {
        "headers": signed_headers(),
        "body": canonical(body()),
        "method": "POST",
        "path": auth.ADMISSION_PATH,
        "query_string": "",
        "trusted_now": NOW,
        "expected_origin": ORIGIN,
        "keyring": keyring(),
    }
    return auth.verify_request(**{**argument_by_name, **changes})


def rejected(call, *args, **kwargs):
    with pytest.raises(auth.AdmissionAuthorizationError) as caught:
        call(*args, **kwargs)
    error = caught.value
    assert error.args == (auth.ERROR,)
    assert error.__context__ is None
    assert error.__cause__ is None
    assert "DO-NOT-LOG" not in str(error) + repr(error)


def test_closed_typed_result_and_credential_redaction(caplog):
    result = verify()
    assert {item.name for item in fields(result.permit)} == permit().keys()
    assert {item.name for item in fields(result.pins)} == body().keys()
    assert result.permit.expires_at == NOW + timedelta(minutes=15)
    assert result.permit.dataset_id == 11
    assert result.pins == auth.BatchPins(51, 40, 0, 7)
    assert result.token.value == TOKEN
    assert repr(result.token) == "LeaseToken()"
    assert "DO-NOT-LOG" not in repr(result)
    assert b64(TOKEN) not in repr(result)
    assert CURRENT_KEY.decode() not in repr(keyring())
    assert caplog.records == []
    with pytest.raises(FrozenInstanceError):
        result.pins.fence = 8


def test_issuer_signing_matches_independent_domain_and_length_framing():
    signed = auth.sign_permit(
        canonical(permit()),
        expected_origin=ORIGIN,
        trusted_now=NOW,
        launch_expires_at=NOW + timedelta(minutes=15),
        keyring=keyring(),
    )
    expected_by_header = dict(signed_headers())
    assert signed.context == expected_by_header[auth.CONTEXT_HEADER]
    assert signed.key_id == expected_by_header[auth.KEY_ID_HEADER]
    assert signed.signature == expected_by_header[auth.SIGNATURE_HEADER]
    assert signed.context not in repr(signed)
    assert signed.signature not in repr(signed)


def test_signer_uses_active_key_and_verifier_accepts_retained_previous_key():
    ring = keyring(active_key_id="previous")
    signed = auth.sign_permit(
        canonical(permit()),
        expected_origin=ORIGIN,
        trusted_now=NOW,
        launch_expires_at=NOW + timedelta(minutes=15),
        keyring=ring,
    )
    assert signed.key_id == "previous"
    assert verify(headers=signed_headers(key_id="previous", key=PREVIOUS_KEY)).token.value == TOKEN
    current_only = keyring(keys=[keyring_document()["keys"][0]])
    rejected(verify, headers=signed_headers(key_id="previous", key=PREVIOUS_KEY), keyring=current_only)


def test_issuer_cannot_exceed_launch_deadline_even_by_microsecond():
    rejected(
        auth.sign_permit,
        canonical(permit()),
        expected_origin=ORIGIN,
        trusted_now=NOW,
        launch_expires_at=NOW + timedelta(minutes=15) - timedelta(microseconds=1),
        keyring=keyring(),
    )


@pytest.mark.parametrize(
    "changes",
    [
        {"method": "GET"},
        {"method": "post"},
        {"path": auth.ADMISSION_PATH + "/"},
        {"path": "/control/v1/custom-import/execution-stop-request"},
        {"path": "/api/v1/extensions/custom-import/search"},
        {"query_string": "cursor=0"},
        {"query_string": b""},
        {"expected_origin": "https://other.example.invalid"},
    ],
)
def test_actual_destination_is_exact(changes):
    rejected(verify, **changes)


@pytest.mark.parametrize(
    "origin",
    [
        "http://engine.example.invalid",
        "HTTPS://engine.example.invalid",
        "https://ENGINE.example.invalid",
        "https://engine.example.invalid/",
        "https://engine.example.invalid/path",
        "https://user@engine.example.invalid",
        "https://user:password@engine.example.invalid",
        "https://engine.example.invalid?",
        "https://engine.example.invalid#",
        "https://engine.example.invalid:0",
        "https://engine.example.invalid:",
        "https://engine.example.invalid:00443",
        "https://engine.example.invalid:65536",
        "https://engine.example.invalid\\other",
        "https://engine%2eexample.invalid",
        "https://engine.example.invalid\n",
        "https://enginé.example.invalid",
        "https://-engine.example.invalid",
        "https://engine..example.invalid",
        "https://",
    ],
)
def test_invalid_origin_is_rejected_even_if_signed_and_configured(origin):
    rejected(verify, expected_origin=origin, headers=signed_headers(canonical(permit(origin=origin))))


@pytest.mark.parametrize("origin", ["https://engine.example.invalid:8443", "https://[::1]:8443"])
def test_explicit_https_origin_can_use_port_or_ipv6(origin):
    assert (
        verify(expected_origin=origin, headers=signed_headers(canonical(permit(origin=origin)))).permit.origin == origin
    )


@pytest.mark.parametrize(
    "field,value",
    [
        ("contract", "custom-import-admission-permit/v2"),
        ("audience", "another-engine"),
        ("issuer", "another-controller"),
        ("method", "GET"),
        ("path", auth.ADMISSION_PATH + "/"),
        ("source_binding_sha256", "A" * 64),
        ("source_binding_sha256", "0" * 64),
        ("source_binding_sha256", "a" * 63),
        ("source_binding_sha256", True),
        ("idempotency_key", ""),
        ("idempotency_key", "a" * 129),
        ("idempotency_key", "a b"),
        ("idempotency_key", "é"),
        ("idempotency_key", ":starts-wrong"),
        ("idempotency_key", 1),
    ],
)
def test_permit_semantics(field, value):
    rejected(verify, headers=signed_headers(canonical(permit(**{field: value}))))


@pytest.mark.parametrize(
    "field", ["dataset_id", "definition_revision_id", "schema_revision_id", "source_binding_revision_id"]
)
@pytest.mark.parametrize("value", [True, False, 0, -1, 1.0, "1", None, 1 << 63])
def test_permit_ids_are_positive_bigints(field, value):
    rejected(verify, headers=signed_headers(canonical(permit(**{field: value}))))


@pytest.mark.parametrize("field", tuple(permit()))
def test_every_permit_field_is_required(field):
    document = permit()
    del document[field]
    rejected(verify, headers=signed_headers(canonical(document)))


@pytest.mark.parametrize(
    "context",
    [
        canonical(permit(extra="not-allowed")),
        canonical(permit()) + b"\n",
        canonical(permit()).replace(b'"dataset_id":11', b'"dataset_id":11,"dataset_id":11'),
        canonical(permit()).replace(b'"dataset_id":11', b'"dataset_id":NaN'),
        canonical(permit()).replace(b'"dataset_id":11', b'"dataset_id":Infinity'),
        canonical(permit()).replace(b'"dataset_id":11', b'"dataset_id":1e1'),
        canonical(permit()).replace(b'"dataset_id":11', b'"dataset_id":' + b"9" * 100),
        canonical(permit()).replace(b'"dataset_id"', b'"\\u0064ataset_id"'),
        b"{" + b" " * 2047 + b"}",
        b"[]",
        b"null",
        b"{",
        b"\xff",
        b"",
    ],
)
def test_noncanonical_or_malformed_signed_context(context):
    rejected(verify, headers=signed_headers(context))


@pytest.mark.parametrize(
    "value",
    [
        "2030-01-01T00:00:00+00:00",
        "2030-01-01T00:00:00.000Z",
        "2030-1-01T00:00:00Z",
        "2030-01-01t00:00:00Z",
        "2030-01-01T00:00:00z",
        "2030-02-30T00:00:00Z",
        "2030-01-01T00:00:60Z",
        "0000-01-01T00:00:00Z",
        True,
        None,
    ],
)
def test_wire_times_require_real_canonical_utc(value):
    rejected(verify, headers=signed_headers(canonical(permit(issued_at=value))))


@pytest.mark.parametrize(
    "changes",
    [
        {"issued_at": "2030-01-01T00:00:01Z"},
        {"expires_at": "2030-01-01T00:00:00Z"},
        {"expires_at": "2029-12-31T23:59:59Z"},
        {"expires_at": "2030-01-02T01:00:01Z"},
    ],
)
def test_future_expired_reversed_and_excessive_ttl(changes):
    rejected(verify, headers=signed_headers(canonical(permit(**changes))))


def test_absolute_expiry_is_exclusive_without_retry_grace():
    deadline = NOW + timedelta(minutes=15)
    assert verify(trusted_now=deadline - timedelta(microseconds=1))
    rejected(verify, trusted_now=deadline)
    rejected(verify, trusted_now=deadline + timedelta(seconds=1))
    rejected(verify, trusted_now=NOW - timedelta(microseconds=1))
    assert verify(headers=signed_headers(canonical(permit(expires_at="2030-01-02T01:00:00Z"))))


def test_retry_reuses_identical_permit_without_minting_a_fresh_deadline():
    headers = signed_headers()
    first = verify(headers=headers)
    assert verify(headers=headers, trusted_now=NOW + timedelta(minutes=14)) == first
    rejected(verify, headers=headers, trusted_now=first.permit.expires_at)


@pytest.mark.parametrize(
    "now", [NOW.replace(tzinfo=None), "2030-01-01T00:00:00Z", True, NOW.astimezone(timezone(timedelta(hours=1)))]
)
def test_trusted_clock_must_be_utc_aware_datetime(now):
    rejected(verify, trusted_now=now)


@pytest.mark.parametrize("field", tuple(body()))
@pytest.mark.parametrize("value", [True, False, -1, 1.0, "1", None, [], 1 << 63])
def test_runtime_pins_are_integers_in_bigint_range(field, value):
    rejected(verify, body=canonical(body(**{field: value})))


@pytest.mark.parametrize("field", ["build_id", "execution_id", "fence"])
def test_runtime_identity_and_fence_cannot_be_zero(field):
    rejected(verify, body=canonical(body(**{field: 0})))


def test_positive_bigint_upper_bound_and_zero_cursor_are_accepted():
    pin_by_name = {name: auth.MAX_BIGINT for name in body()}
    assert verify(body=canonical(pin_by_name)).pins.fence == auth.MAX_BIGINT
    assert verify().pins.expected_after_occurrence_id == 0
    document = permit(
        **{
            name: auth.MAX_BIGINT
            for name in (
                "dataset_id",
                "definition_revision_id",
                "schema_revision_id",
                "source_binding_revision_id",
            )
        }
    )
    assert verify(headers=signed_headers(canonical(document))).permit.dataset_id == auth.MAX_BIGINT


@pytest.mark.parametrize("field", tuple(body()))
def test_every_body_field_is_required(field):
    document = body()
    del document[field]
    rejected(verify, body=canonical(document))


@pytest.mark.parametrize(
    "raw",
    [
        canonical(body(extra=1)),
        canonical(body()) + b" ",
        canonical(body()).replace(b'"fence":7', b'"fence":7,"fence":7'),
        canonical(body()).replace(b'"fence":7', b'"fence":NaN'),
        canonical(body()).replace(b'"fence":7', b'"fence":-Infinity'),
        canonical(body()).replace(b'"fence":7', b'"fence":7e0'),
        canonical(body()).replace(b'"expected_after_occurrence_id":0', b'"expected_after_occurrence_id":-0'),
        canonical(body()).replace(b'"build_id"', b'"\\u0062uild_id"'),
        b'{"fence":7,"build_id":51,"execution_id":40,"expected_after_occurrence_id":0}',
        b"{" + b" " * 511 + b"}",
        b"[]",
        b"null",
        b"{",
        b"\xff",
        b"",
        "{}",
    ],
)
def test_closed_bounded_canonical_body(raw):
    rejected(verify, body=raw)


def test_runtime_pins_are_not_lease_or_database_authorization():
    first = verify()
    second = verify(body=canonical(body(execution_id=99, build_id=100, fence=101, expected_after_occurrence_id=200)))
    assert first.permit == second.permit
    assert first.pins != second.pins
    assert first.token == second.token


@pytest.mark.parametrize(
    "name", ["Content-Type", "Authorization", auth.CONTEXT_HEADER, auth.KEY_ID_HEADER, auth.SIGNATURE_HEADER]
)
def test_missing_or_duplicate_headers_are_rejected(name):
    headers = signed_headers()
    value = dict(headers)[name]
    rejected(verify, headers=[pair for pair in headers if pair[0] != name])
    rejected(verify, headers=headers + [(name.lower(), value)])
    duplicate_replaces_another = (
        [(name.lower(), value), *headers[1:]] if name != "Content-Type" else [*headers[:-1], (name.lower(), value)]
    )
    rejected(verify, headers=duplicate_replaces_another)


@pytest.mark.parametrize(
    "name",
    [
        "X-Custom-Import-Admission-Extra",
        "X-Custom-Import-Admission-Nonce",
        "X-Extension-Read-Context",
        "X-Control-Token",
        "Unknown",
        "X-Custom-Import-Admission-Key-Id",
    ],
)
def test_unknown_or_nonascii_header_names_are_rejected(name):
    headers = signed_headers()
    headers[3] = (name, "current")
    rejected(verify, headers=headers)


@pytest.mark.parametrize(
    "name,value",
    [
        ("Content-Type", "application/json; charset=utf-8"),
        ("Content-Type", "APPLICATION/JSON"),
        (auth.KEY_ID_HEADER, "Current"),
        (auth.KEY_ID_HEADER, "unknown"),
        (auth.KEY_ID_HEADER, " current"),
        (auth.KEY_ID_HEADER, "current\r\n"),
        (auth.KEY_ID_HEADER, "currént"),
        (auth.KEY_ID_HEADER, "a" * 33),
        (auth.CONTEXT_HEADER, "e30="),
        (auth.CONTEXT_HEADER, "/w"),
        (auth.CONTEXT_HEADER, "_"),
        (auth.CONTEXT_HEADER, "A" * 2732),
        (auth.SIGNATURE_HEADER, "AA"),
        (auth.SIGNATURE_HEADER, "A" * 44),
        (auth.SIGNATURE_HEADER, "_"),
        ("Authorization", "bearer eA"),
        ("Authorization", "Bearer  eA"),
        ("Authorization", "Bearer eA="),
        ("Authorization", "Bearer _x"),
        ("Authorization", "Bearer "),
        ("Authorization", "Bearer _"),
        ("Authorization", "Bearer " + "A" * 5463),
        ("Authorization", "Bearer eA\n"),
        ("Authorization", "Bearer eyJzdG9wIjoxfQ, Bearer eA"),
    ],
)
def test_header_value_limits_and_canonical_encodings(name, value):
    rejected(verify, headers=replacing(signed_headers(), name, value))


def test_header_names_are_case_insensitive_but_must_preserve_duplicates():
    headers = [(name.lower(), value) for name, value in signed_headers()]
    assert verify(headers=tuple(headers))
    rejected(verify, headers=dict(headers))


@pytest.mark.parametrize("domain", [b"CUSTOM_IMPORT_ADMISSION_PERMIT_V2\x00", b"CUSTOM_IMPORT_READ_PERMIT_V1\x00", b""])
def test_signature_domain_prevents_cross_purpose_reuse(domain):
    rejected(verify, headers=signed_headers(domain=domain))


def test_signature_and_key_id_are_both_authenticated():
    rejected(verify, headers=signed_headers(key=PREVIOUS_KEY))
    rejected(verify, headers=replacing(signed_headers(), auth.KEY_ID_HEADER, "previous"))
    headers = replacing(signed_headers(), auth.CONTEXT_HEADER, b64(canonical(permit(dataset_id=99))))
    rejected(verify, headers=headers)


def test_signature_base64_alias_for_same_mac_is_rejected():
    alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"
    headers = signed_headers()
    signature = dict(headers)[auth.SIGNATURE_HEADER]
    alias = signature[:-1] + alphabet[alphabet.index(signature[-1]) + 1]
    assert base64.urlsafe_b64decode(alias + "=") == base64.urlsafe_b64decode(signature + "=")
    rejected(verify, headers=replacing(headers, auth.SIGNATURE_HEADER, alias))


@pytest.mark.parametrize(
    "token",
    [b"\x00", b"\xff", b"\x00\xff" * 2048, "opaque-é", bytearray(b"\xff"), memoryview(b"\x00")],
    ids=["nul", "non-utf8", "maximum-bytes", "utf8-text", "bytearray", "memoryview"],
)
def test_lease_bearer_preserves_engine_token_bytes(token):
    expected = token.encode("utf-8") if isinstance(token, str) else bytes(token)
    headers = replacing(signed_headers(), "Authorization", auth.encode_lease_bearer(token))
    result = verify(headers=headers)
    assert result.token.value == expected
    assert hashlib.sha256(result.token.value).digest() == hashlib.sha256(expected).digest()


@pytest.mark.parametrize(
    "token",
    [b"", "", b"x" * 4097, "é" * 2049, "\ud800", 1, True, None],
    ids=[
        "empty-bytes",
        "empty-text",
        "too-many-bytes",
        "too-many-utf8-bytes",
        "surrogate",
        "integer",
        "boolean",
        "none",
    ],
)
def test_lease_encoder_preserves_engine_limits(token):
    rejected(auth.encode_lease_bearer, token)


def test_bearer_decoded_length_over_limit_is_rejected():
    rejected(verify, headers=signed_headers(token=b"x" * 4097))


@pytest.mark.parametrize(
    "changes",
    [
        {"contract": "custom-import-read-keyring/v1"},
        {"contract": "custom-import-admission-keyring/v2"},
        {"extra": True},
        {"active_key_id": "missing"},
        {"active_key_id": True},
        {"active_key_id": "Current"},
        {"keys": []},
        {"keys": {}},
        {"keys": [{"key_id": "current", "key_base64url": b64(CURRENT_KEY), "extra": "no"}]},
        {"keys": [{"key_id": "current", "key_base64url": b64(b"x" * 31)}]},
        {"keys": [{"key_id": "current", "key_base64url": b64(b"x" * 33)}]},
        {"keys": [{"key_id": "current", "key_base64url": b64(CURRENT_KEY) + "="}]},
        {
            "keys": [
                {"key_id": "current", "key_base64url": b64(CURRENT_KEY)},
                {"key_id": "current", "key_base64url": b64(PREVIOUS_KEY)},
            ]
        },
        {
            "keys": [
                {"key_id": "current", "key_base64url": b64(CURRENT_KEY)},
                {"key_id": "other", "key_base64url": b64(CURRENT_KEY)},
            ]
        },
    ],
)
def test_keyring_is_closed_purpose_specific_and_unambiguous(changes):
    rejected(auth.load_keyring, canonical(keyring_document(**changes)))


def test_keyring_rotation_is_bounded_to_four_keys():
    keys = [{"key_id": f"key-{index}", "key_base64url": b64(bytes([index]) * 32)} for index in range(1, 6)]
    assert len(keyring(active_key_id="key-1", keys=keys[:4]).keys) == 4
    rejected(auth.load_keyring, canonical(keyring_document(active_key_id="key-1", keys=keys)))


@pytest.mark.parametrize(
    "raw",
    [
        canonical(keyring_document()) + b"\n",
        canonical(keyring_document()).replace(
            b'"active_key_id":"current"', b'"active_key_id":"current","active_key_id":"current"'
        ),
        canonical(keyring_document()).replace(b'"key_id":"current"', b'"key_id":"current","key_id":"current"'),
        b"{" + b" " * 4095 + b"}",
        b"[" * 2047 + b"]" * 2047,
        b"{",
        b"\xff",
        b"",
        "{}",
    ],
)
def test_keyring_input_is_bounded_canonical_and_errors_drop_parser_context(raw):
    rejected(auth.load_keyring, raw)


def test_sensitive_malformed_inputs_never_enter_errors_or_logs(caplog):
    rejected(verify, headers=replacing(signed_headers(), "Authorization", "Bearer DO-NOT-LOG!"))
    rejected(verify, body=b'{"DO-NOT-LOG":')
    rejected(verify, headers=signed_headers(b'{"DO-NOT-LOG":'))
    rejected(auth.load_keyring, b'{"key_base64url":"DO-NOT-LOG"')
    assert caplog.records == []
