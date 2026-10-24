# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Synthetic checks for capture authority before any request body is consumed."""

import base64
import hashlib
import hmac
import inspect
import json
from dataclasses import FrozenInstanceError, asdict, fields
from datetime import datetime, timedelta, timezone

import pytest

from process.custom_import import admission_authorization as admission
from process.custom_import import capture_authorization as auth
from process.custom_import.capture_limits import CaptureLimits

NOW = datetime(2030, 1, 1, tzinfo=timezone.utc)
ORIGIN = "https://engine.example.invalid"
KEY = b"synthetic-current-key-material!!"
OLD_KEY = b"synthetic-previous-key-material!"
TOKEN = b"opaque-capture-token\x00\xff"
DEFAULT_LIMITS = asdict(CaptureLimits())


def canonical(document):
    return json.dumps(document, ensure_ascii=True, allow_nan=False, sort_keys=True, separators=(",", ":")).encode(
        "ascii"
    )


def b64(value):
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode("ascii")


def permit(**changes):
    return {
        "audience": "custom-import-engine",
        "contract": "custom-import-capture-permit/v1",
        "dataset_id": 11,
        "definition_revision_id": 12,
        "definition_sha256": "a" * 64,
        "expected_content_sha256": "b" * 64,
        "expires_at": "2030-01-01T00:15:00Z",
        "idempotency_key": "synthetic-capture-001",
        "issued_at": "2030-01-01T00:00:00Z",
        "issuer": "custom-import-execution-controller",
        "limits": dict(DEFAULT_LIMITS),
        "maximum_dataset_retained_bytes": 0,
        "method": "POST",
        "origin": ORIGIN,
        "path": "/control/v1/custom-import/capture",
        "request_identity_sha256": "c" * 64,
        "schema_revision_id": 13,
        "timeout_seconds": 300,
        **changes,
    }


def keyring(active="current"):
    return admission.load_keyring(
        canonical(
            {
                "active_key_id": active,
                "contract": admission.KEYRING_CONTRACT,
                "keys": [
                    {"key_id": "current", "key_base64url": b64(KEY)},
                    {"key_id": "previous", "key_base64url": b64(OLD_KEY)},
                ],
            }
        )
    )


def signed_headers(context=None, *, key_id="current", key=KEY, domain=b"CUSTOM_IMPORT_CAPTURE_PERMIT_V1\x00"):
    if context is None:
        context = canonical(permit())
    encoded_id = key_id.encode("ascii")
    framing = domain + len(encoded_id).to_bytes(2, "big") + encoded_id + len(context).to_bytes(8, "big") + context
    return [
        ("Content-Type", "application/octet-stream"),
        ("Authorization", admission.encode_lease_bearer(TOKEN)),
        (auth.CONTEXT_HEADER, b64(context)),
        (auth.KEY_ID_HEADER, key_id),
        (auth.SIGNATURE_HEADER, b64(hmac.new(key, framing, hashlib.sha256).digest())),
        (auth.EXECUTION_ID_HEADER, "40"),
        (auth.FENCE_HEADER, "7"),
    ]


def replacing(headers, name, value):
    return [(candidate, value if candidate.lower() == name.lower() else supplied) for candidate, supplied in headers]


def verify(**changes):
    return auth.verify_request(
        **{
            "headers": signed_headers(),
            "method": "POST",
            "path": auth.CAPTURE_PATH + "-bundle",
            "query_string": "",
            "trusted_now": NOW,
            "expected_origin": ORIGIN,
            "keyring": keyring(),
            **changes,
        }
    )


def rejected(call, *args, **kwargs):
    with pytest.raises(auth.CaptureAuthorizationError) as caught:
        call(*args, **kwargs)
    error = caught.value
    assert error.args == (auth.ERROR,)
    assert error.__context__ is None and error.__cause__ is None
    assert "opaque-capture-token" not in str(error) + repr(error)


@pytest.mark.parametrize("suffix", ["-preflight", "-bundle", "-retained"])
def test_fixed_capture_routes_return_immutable_typed_authority_without_body(suffix, caplog):
    result = verify(path=auth.CAPTURE_PATH + suffix)
    assert {item.name for item in fields(result.permit)} == permit().keys()
    assert result.permit.limits == CaptureLimits()
    assert result.permit.expires_at == NOW + timedelta(minutes=15)
    assert (result.execution_id, result.fence, result.token.value) == (40, 7, TOKEN)
    assert repr(result.token) == "LeaseToken()"
    assert "opaque-capture-token" not in repr(result)
    assert b64(TOKEN) not in repr(result)
    assert permit()["idempotency_key"] not in repr(result.permit)
    assert KEY.decode() not in repr(keyring())
    assert "body" not in inspect.signature(auth.verify_request).parameters
    assert not caplog.records
    with pytest.raises(FrozenInstanceError):
        result.fence = 8
    with pytest.raises(FrozenInstanceError):
        result.permit.timeout_seconds = 1
    with pytest.raises(FrozenInstanceError):
        result.permit.limits.maximum_records = 1


@pytest.mark.parametrize("active,key", [("current", KEY), ("previous", OLD_KEY)])
def test_signer_matches_independent_framing_and_retained_key(active, key):
    context = canonical(permit())
    signed = auth.sign_permit(
        context,
        expected_origin=ORIGIN,
        trusted_now=NOW,
        launch_expires_at=NOW + timedelta(minutes=15),
        keyring=keyring(active),
    )
    header_by_name = dict(signed_headers(context, key_id=active, key=key))
    assert isinstance(signed, admission.SignedPermit)
    assert (signed.context, signed.key_id, signed.signature) == (
        header_by_name[auth.CONTEXT_HEADER],
        active,
        header_by_name[auth.SIGNATURE_HEADER],
    )
    assert signed.context not in repr(signed) and signed.signature not in repr(signed)
    assert verify(headers=list(header_by_name.items())).token.value == TOKEN
    assert auth._permit(context, expected_origin=ORIGIN, trusted_now=NOW).limits == CaptureLimits()


@pytest.mark.parametrize(
    "purpose,path,domain",
    [
        ("admission", admission.ADMISSION_PATH, b"CUSTOM_IMPORT_ADMISSION_PERMIT_V1\x00"),
        ("source", admission.SOURCE_PATH, b"CUSTOM_IMPORT_SOURCE_PERMIT_V1\x00"),
        ("projection", admission.PROJECTION_PATH, b"CUSTOM_IMPORT_QUERY_PROJECTION_PERMIT_V1\x00"),
    ],
)
def test_shared_key_never_allows_cross_purpose_replay(purpose, path, domain):
    rejected(verify, headers=signed_headers(domain=domain))
    label = "Projection" if purpose == "projection" else purpose.title()
    headers = [
        (name.replace("-Capture-", f"-{label}-"), "application/json" if name == "Content-Type" else value)
        for name, value in signed_headers()[:5]
    ]
    with pytest.raises(admission.AdmissionAuthorizationError):
        admission._verify_authority(
            headers=headers,
            method="POST",
            path=path,
            query_string="",
            trusted_now=NOW,
            expected_origin=ORIGIN,
            keyring=keyring(),
            purpose=purpose,
        )


def test_signature_is_checked_before_canonical_context_parser(monkeypatch):
    def unread(*args, **kwargs):
        raise AssertionError("unauthenticated context was parsed")

    monkeypatch.setattr(admission, "_object", unread)
    ring = admission.AdmissionKeyring("current", (("current", KEY),))
    rejected(
        auth.verify_request,
        headers=signed_headers(b"not-json", key=OLD_KEY),
        method="POST",
        path=auth.CAPTURE_PATH + "-bundle",
        query_string="",
        trusted_now=NOW,
        expected_origin=ORIGIN,
        keyring=ring,
    )


@pytest.mark.parametrize(
    "changes",
    [
        {"method": "GET"},
        {"method": "post"},
        {"path": auth.CAPTURE_PATH},
        {"path": auth.CAPTURE_PATH + "/bundle"},
        {"path": auth.CAPTURE_PATH + "-bundle/"},
        {"path": auth.CAPTURE_PATH + "-bundle?x=1"},
        {"path": admission.SOURCE_PATH},
        {"query_string": "x=1"},
        {"query_string": b""},
        {"expected_origin": "https://other.example.invalid"},
    ],
)
def test_actual_route_origin_and_query_are_exact(changes):
    rejected(verify, **changes)


@pytest.mark.parametrize(
    "field,value",
    [
        ("audience", "other-engine"),
        ("issuer", "other-controller"),
        ("contract", admission.PERMIT_CONTRACT),
        ("method", "GET"),
        ("path", auth.CAPTURE_PATH + "-bundle"),
        ("origin", ORIGIN + "/"),
        ("idempotency_key", ""),
        ("idempotency_key", "a" * 129),
        ("idempotency_key", "a b"),
        ("timeout_seconds", 0),
        ("timeout_seconds", 3601),
        ("timeout_seconds", True),
        ("maximum_dataset_retained_bytes", -1),
        ("maximum_dataset_retained_bytes", True),
        ("maximum_dataset_retained_bytes", admission.MAX_BIGINT + 1),
    ],
)
def test_signed_scope_and_resource_semantics_are_not_optional(field, value):
    rejected(verify, headers=signed_headers(canonical(permit(**{field: value}))))


@pytest.mark.parametrize("field", ["dataset_id", "definition_revision_id", "schema_revision_id"])
@pytest.mark.parametrize("value", [True, 0, -1, "1", admission.MAX_BIGINT + 1])
def test_identity_fields_require_positive_bigints(field, value):
    rejected(verify, headers=signed_headers(canonical(permit(**{field: value}))))


@pytest.mark.parametrize("field", ["definition_sha256", "request_identity_sha256", "expected_content_sha256"])
@pytest.mark.parametrize("value", ["0" * 64, "A" * 64, "a" * 63, None])
def test_content_and_retained_scope_digests_require_nonzero_lowercase_sha256(field, value):
    rejected(verify, headers=signed_headers(canonical(permit(**{field: value}))))


@pytest.mark.parametrize("field", tuple(permit()))
def test_permit_requires_every_field(field):
    document = permit()
    del document[field]
    rejected(verify, headers=signed_headers(canonical(document)))


@pytest.mark.parametrize(
    "context",
    [
        canonical(permit(extra="unexpected")),
        canonical(permit()) + b"\n",
        canonical(permit()).replace(b'"dataset_id":11', b'"dataset_id":11,"dataset_id":11'),
        canonical(permit()).replace(b'"dataset_id":11', b'"dataset_id":1.1'),
        canonical(permit()).replace(b'"dataset_id":11', b'"dataset_id":NaN'),
        b"x" * 4097,
        b"[]",
        b"null",
        b"\xff",
        b"",
    ],
)
def test_context_is_bounded_closed_unique_and_canonical(context):
    rejected(verify, headers=signed_headers(context))


@pytest.mark.parametrize("field", tuple(DEFAULT_LIMITS))
@pytest.mark.parametrize("kind", ["zero", "boolean", "loosened"])
def test_each_capture_limit_can_only_tighten_defaults(field, kind):
    limit_by_name = {**DEFAULT_LIMITS, field: {"zero": 0, "boolean": True, "loosened": DEFAULT_LIMITS[field] + 1}[kind]}
    rejected(verify, headers=signed_headers(canonical(permit(limits=limit_by_name))))


@pytest.mark.parametrize(
    "limits",
    [
        {},
        {**DEFAULT_LIMITS, "extra": 1},
        {key: value for key, value in DEFAULT_LIMITS.items() if key != "maximum_records"},
        {**DEFAULT_LIMITS, "maximum_decoded_bytes": 1},
        None,
        [],
    ],
)
def test_limits_mapping_is_exact_and_still_covers_a_record(limits):
    rejected(verify, headers=signed_headers(canonical(permit(limits=limits))))


def test_tightened_limits_and_numeric_boundaries_remain_usable():
    limit_by_name = {name: 1 for name in DEFAULT_LIMITS}
    document = permit(
        limits=limit_by_name,
        timeout_seconds=1,
        maximum_dataset_retained_bytes=admission.MAX_BIGINT,
        dataset_id=admission.MAX_BIGINT,
    )
    result = verify(headers=signed_headers(canonical(document)))
    assert asdict(result.permit.limits) == limit_by_name
    assert result.permit.maximum_dataset_retained_bytes == admission.MAX_BIGINT
    assert result.permit.dataset_id == admission.MAX_BIGINT
    assert verify(headers=signed_headers(canonical(permit(timeout_seconds=3600)))).permit.timeout_seconds == 3600


@pytest.mark.parametrize(
    "changes",
    [
        {"issued_at": "2030-01-01T00:00:01Z"},
        {"issued_at": "2030-01-01T00:00:00+00:00"},
        {"expires_at": "2030-01-01T00:00:00Z"},
        {"expires_at": "2029-12-31T23:59:59Z"},
        {"expires_at": "2030-01-02T01:00:01Z"},
        {"expires_at": "2030-02-30T00:00:00Z"},
    ],
)
def test_permit_time_window_is_canonical_current_and_bounded(changes):
    rejected(verify, headers=signed_headers(canonical(permit(**changes))))


def test_expiry_remains_exclusive_across_retry_and_launch_cap():
    deadline = NOW + timedelta(minutes=15)
    headers = signed_headers()
    assert verify(headers=headers, trusted_now=deadline - timedelta(microseconds=1)) == verify(headers=headers)
    rejected(verify, headers=headers, trusted_now=deadline)
    rejected(verify, trusted_now=NOW - timedelta(microseconds=1))
    assert verify(headers=signed_headers(canonical(permit(expires_at="2030-01-02T01:00:00Z"))))
    rejected(
        auth.sign_permit,
        canonical(permit()),
        expected_origin=ORIGIN,
        trusted_now=NOW,
        launch_expires_at=deadline - timedelta(microseconds=1),
        keyring=keyring(),
    )
    rejected(verify, trusted_now=NOW.replace(tzinfo=None))


@pytest.mark.parametrize("name", [name for name, _ in signed_headers()])
def test_duplicate_and_missing_headers_are_rejected_without_mapping_loss(name):
    headers = signed_headers()
    rejected(verify, headers=[pair for pair in headers if pair[0] != name])
    rejected(verify, headers=headers + [(name.lower(), dict(headers)[name])])
    duplicate_headers = list(headers)
    duplicate_headers[0 if name != "Content-Type" else 1] = (name.lower(), dict(headers)[name])
    rejected(verify, headers=duplicate_headers)


@pytest.mark.parametrize(
    "pair",
    [
        None,
        ("Content-Type",),
        ("Content-Type", "application/octet-stream", "extra"),
        (None, "application/octet-stream"),
        ("x" * 65, "application/octet-stream"),
        ("Content-Typé", "application/octet-stream"),
    ],
)
def test_malformed_header_pairs_are_rejected_before_context_parsing(monkeypatch, pair):
    def unread(*_args, **_kwargs):
        raise AssertionError("malformed headers must not reach permit parsing")

    monkeypatch.setattr(auth, "_permit", unread)
    rejected(verify, headers=[pair, *signed_headers()[1:]])


@pytest.mark.parametrize(
    "name,value",
    [
        ("Content-Type", "application/json"),
        ("Content-Type", "application/octet-stream; charset=utf-8"),
        (auth.CONTEXT_HEADER, "e30="),
        (auth.KEY_ID_HEADER, "unknown"),
        (auth.KEY_ID_HEADER, "Current"),
        (auth.SIGNATURE_HEADER, "A" * 43),
        ("Authorization", "bearer " + b64(TOKEN)),
        ("Authorization", "Bearer " + b64(TOKEN) + "="),
        ("Authorization", "Bearer "),
        ("Authorization", "Bearer " + b64(b"x" * 4097)),
        ("Authorization", "Bearer " + b64(TOKEN) + "\n"),
    ],
)
def test_header_values_are_strict_bounded_and_redacted(name, value):
    rejected(verify, headers=replacing(signed_headers(), name, value))


@pytest.mark.parametrize("name", [auth.EXECUTION_ID_HEADER, auth.FENCE_HEADER])
@pytest.mark.parametrize("value", ["0", "01", "+1", "-1", "1.0", " 1", "1 ", "１", str(admission.MAX_BIGINT + 1)])
def test_execution_and_fence_headers_require_canonical_positive_decimal(name, value):
    rejected(verify, headers=replacing(signed_headers(), name, value))


def test_runtime_pins_are_not_lease_authority_and_accept_bigint_boundary():
    result = verify()
    headers = replacing(signed_headers(), auth.EXECUTION_ID_HEADER, str(admission.MAX_BIGINT))
    headers = replacing(headers, auth.FENCE_HEADER, str(admission.MAX_BIGINT))
    changed = verify(headers=tuple((name.lower(), value) for name, value in headers))
    assert changed.permit == result.permit and changed.token == result.token
    assert (changed.execution_id, changed.fence) == (admission.MAX_BIGINT, admission.MAX_BIGINT)


@pytest.mark.parametrize(
    "token", [b"\x00\xff\r\n", "synthetic-☃", bytearray(b"mutable-token"), memoryview(b"view-token")]
)
def test_bearer_preserves_opaque_bytes_without_text_normalization(token):
    headers = replacing(signed_headers(), "Authorization", admission.encode_lease_bearer(token))
    result = verify(headers=headers)
    assert result.token.value == (token.encode("utf-8") if isinstance(token, str) else bytes(token))


def test_unknown_header_dictionary_input_and_noncanonical_origin_are_rejected():
    headers = signed_headers()
    rejected(verify, headers=dict(headers))
    rejected(verify, headers=headers + [("X-Custom-Import-Capture-Other", "1")])
    rejected(verify, headers=[("X-Custom-Import-Capture-Other", "1"), *headers[1:]])
    bad_origin = "https://user:synthetic-password@engine.example.invalid"
    rejected(verify, expected_origin=bad_origin, headers=signed_headers(canonical(permit(origin=bad_origin))))
