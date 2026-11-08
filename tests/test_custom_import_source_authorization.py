# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Independent SOURCE framing, fixed-purpose isolation and complete cursor bounds."""

from dataclasses import FrozenInstanceError, asdict
from datetime import timedelta

import pytest

from process.custom_import import admission_authorization as admission
from process.custom_import import source_authorization as source
from tests import test_custom_import_admission_authorization as legacy

CURSOR = {"next_part_ordinal": 1, "next_part_row_ordinal": 0, "next_source_ordinal": 0, "next_pack_ordinal": 0}
BODY = {"build_id": 51, "execution_id": 40, "fence": 7, "stream_slot": 2, "expected_cursor": CURSOR}


def context(**changes):
    return legacy.permit(contract=source.PERMIT_CONTRACT, path=source.SOURCE_PATH, **changes)


def headers(document=None, *, domain=b"CUSTOM_IMPORT_SOURCE_PERMIT_V1\x00"):
    pairs = legacy.signed_headers(legacy.canonical(context() if document is None else document), domain=domain)
    return [(name.replace("-Admission-", "-Source-"), value) for name, value in pairs]


def verify(**changes):
    return source.verify_request(
        **(
            {
                "headers": headers(),
                "body": legacy.canonical(BODY),
                "method": "POST",
                "path": source.SOURCE_PATH,
                "query_string": "",
                "trusted_now": legacy.NOW,
                "expected_origin": legacy.ORIGIN,
                "keyring": legacy.keyring(),
            }
            | changes
        )
    )


def test_source_keeps_exact_opaque_lease_and_immutable_full_cursor():
    verified = verify()
    assert type(verified.permit) is source.SourcePermit
    assert asdict(verified.pins) == BODY and verified.token.value == legacy.TOKEN
    assert len(legacy.canonical(BODY)) <= 512
    assert "opaque" not in repr(verified) and "opaque" not in repr(verified.token)
    with pytest.raises(FrozenInstanceError):
        verified.permit.path = admission.ADMISSION_PATH


def test_issuer_source_signature_matches_independent_framing():
    signed = admission.sign_permit(
        legacy.canonical(context()),
        expected_origin=legacy.ORIGIN,
        trusted_now=legacy.NOW,
        launch_expires_at=legacy.NOW + timedelta(minutes=15),
        keyring=legacy.keyring(),
        is_source=True,
    )
    headers_by_name = dict(headers())
    assert (signed.context, signed.key_id, signed.signature) == (
        headers_by_name[source.CONTEXT_HEADER],
        headers_by_name[source.KEY_ID_HEADER],
        headers_by_name[source.SIGNATURE_HEADER],
    )


@pytest.mark.parametrize(
    "document,domain",
    [
        (legacy.permit(), b"CUSTOM_IMPORT_SOURCE_PERMIT_V1\x00"),
        (context(), b"CUSTOM_IMPORT_ADMISSION_PERMIT_V1\x00"),
        (context() | {"path": admission.ADMISSION_PATH}, b"CUSTOM_IMPORT_SOURCE_PERMIT_V1\x00"),
        (context() | {"contract": admission.PERMIT_CONTRACT}, b"CUSTOM_IMPORT_SOURCE_PERMIT_V1\x00"),
        (context(), b"CUSTOM_IMPORT_SOURCE_PERMIT_V1"),
    ],
)
def test_other_purpose_path_and_domain_cannot_authorize_source(document, domain):
    with pytest.raises(admission.AdmissionAuthorizationError):
        verify(headers=headers(document, domain=domain))


def test_source_cannot_authorize_admission_even_after_header_relabel():
    relabeled_headers = [(name.replace("-Source-", "-Admission-"), value) for name, value in headers()]
    with pytest.raises(admission.AdmissionAuthorizationError):
        legacy.verify(headers=relabeled_headers)


@pytest.mark.parametrize("name", sorted(source.CURSOR_FIELDS))
@pytest.mark.parametrize("value", [True, 1.0, -1, 1 << 63, None, "1"])
def test_each_cursor_coordinate_rejects_unsafe_types_and_overflow(name, value):
    with pytest.raises(admission.AdmissionAuthorizationError):
        verify(body=legacy.canonical(BODY | {"expected_cursor": CURSOR | {name: value}}))


@pytest.mark.parametrize(
    "name,value",
    [
        ("next_part_ordinal", 0),
        ("next_part_ordinal", 1 << 31),
        ("next_pack_ordinal", 1 << 31),
    ],
)
def test_part_and_pack_have_database_integer_bounds(name, value):
    with pytest.raises(admission.AdmissionAuthorizationError):
        verify(body=legacy.canonical(BODY | {"expected_cursor": CURSOR | {name: value}}))


def test_maximum_cursor_values_are_exact_without_float_coercion():
    cursor_by_name = {
        "next_part_ordinal": 2**31 - 1,
        "next_part_row_ordinal": 2**63 - 1,
        "next_source_ordinal": 2**63 - 1,
        "next_pack_ordinal": 2**31 - 1,
    }
    assert (
        asdict(verify(body=legacy.canonical(BODY | {"expected_cursor": cursor_by_name})).pins.expected_cursor)
        == cursor_by_name
    )


@pytest.mark.parametrize("name", ["build_id", "execution_id", "fence", "stream_slot"])
@pytest.mark.parametrize("value", [0, True, 1.0, 2**63, "1"])
def test_run_and_stream_pins_are_positive_exact_integers(name, value):
    with pytest.raises(admission.AdmissionAuthorizationError):
        verify(body=legacy.canonical(BODY | {name: value}))


@pytest.mark.parametrize(
    "document",
    [
        BODY | {"stream_slot": 2**15},
        BODY | {"extra": 1},
        BODY | {"expected_cursor": CURSOR | {"extra": 0}},
        BODY | {"expected_cursor": []},
        {name: value for name, value in BODY.items() if name != "stream_slot"},
    ],
)
def test_request_is_closed_at_both_levels(document):
    with pytest.raises(admission.AdmissionAuthorizationError):
        verify(body=legacy.canonical(document))


@pytest.mark.parametrize(
    "raw",
    [
        b" " + legacy.canonical(BODY),
        legacy.canonical(BODY) + b"\n",
        b"x" * 513,
        b'{"build_id":51,' + legacy.canonical(BODY)[1:],
        legacy.canonical(BODY).replace(b'"next_pack_ordinal":0', b'"next_pack_ordinal":0,"next_pack_ordinal":0'),
        legacy.canonical(BODY).replace(b'"next_pack_ordinal":0', b'"next_pack_ordinal":NaN'),
    ],
)
def test_noncanonical_duplicate_or_oversized_request_denied(raw):
    with pytest.raises(admission.AdmissionAuthorizationError, match="^" + admission.ERROR + "$"):
        verify(body=raw)


@pytest.mark.parametrize(
    "changes",
    [
        {"method": "GET"},
        {"path": admission.ADMISSION_PATH},
        {"query_string": "extra=1"},
        {"expected_origin": "https://other.example.invalid"},
        {"trusted_now": legacy.NOW - timedelta(seconds=1)},
        {"trusted_now": legacy.NOW + timedelta(minutes=15)},
        {"headers": headers() + [(source.KEY_ID_HEADER, "current")]},
        {"headers": headers() + [("X-Custom-Import-Source-Other", "1")]},
        {"headers": legacy.signed_headers()},
    ],
)
def test_destination_time_and_headers_remain_fixed(changes):
    with pytest.raises(admission.AdmissionAuthorizationError):
        verify(**changes)
