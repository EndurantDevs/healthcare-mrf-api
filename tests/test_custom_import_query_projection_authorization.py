# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Projection-purpose isolation, closed pins and exact signed cursor bounds."""

from dataclasses import FrozenInstanceError, asdict
from datetime import timedelta

import pytest

from process.custom_import import admission_authorization as admission
from process.custom_import import query_projection_authorization as projection
from tests import test_custom_import_admission_authorization as legacy
from tests import test_custom_import_source_authorization as source

DOMAIN = b"CUSTOM_IMPORT_QUERY_PROJECTION_PERMIT_V1\x00"
PREFIX = "X-Custom-Import-Projection-"
BODY = {
    "build_id": 51,
    "execution_id": 40,
    "fence": 7,
    "expected_phase": "root",
    "expected_after_entity_binding_id": 0,
    "expected_after_selected_value": None,
    "expected_after_child_key_sha256": None,
    "expected_index_count": 0,
    "expected_proof_count": 0,
}


@pytest.mark.parametrize("value", [True, -1, 2**63, "0", None])
def test_projection_proof_counter_requires_a_bounded_native_integer(value):
    with pytest.raises(admission.AdmissionAuthorizationError):
        projection.batch_pins(BODY | {"expected_phase": "digest_child", "expected_proof_count": value})


@pytest.mark.parametrize("phase", ["root", "child", "indexes"])
def test_projection_early_phases_reject_advertised_proof_progress(phase):
    with pytest.raises(admission.AdmissionAuthorizationError):
        projection.batch_pins(BODY | {"expected_phase": phase, "expected_proof_count": 1})


def context(**changes):
    return legacy.permit(
        **({"contract": projection.PROJECTION_PERMIT_CONTRACT, "path": projection.PROJECTION_PATH} | changes)
    )


def headers(document=None, *, domain=DOMAIN):
    pairs = legacy.signed_headers(legacy.canonical(context() if document is None else document), domain=domain)
    return [(name.replace("-Admission-", "-Projection-"), value) for name, value in pairs]


def verify(**changes):
    return projection.verify_request(
        **(
            {
                "headers": headers(),
                "body": legacy.canonical(BODY),
                "method": "POST",
                "path": projection.PROJECTION_PATH,
                "query_string": "",
                "trusted_now": legacy.NOW,
                "expected_origin": legacy.ORIGIN,
                "keyring": legacy.keyring(),
            }
            | changes
        )
    )


def test_signer_projection_roundtrip_matches_independent_framing_and_redacts_credentials(caplog):
    signed = admission.sign_permit(
        legacy.canonical(context()),
        expected_origin=legacy.ORIGIN,
        trusted_now=legacy.NOW,
        launch_expires_at=legacy.NOW + timedelta(minutes=15),
        keyring=legacy.keyring(),
        is_projection=True,
    )
    pairs = [
        ("Content-Type", "application/json"),
        ("Authorization", "Bearer " + legacy.b64(legacy.TOKEN)),
        (PREFIX + "Context", signed.context),
        (PREFIX + "Key-Id", signed.key_id),
        (PREFIX + "Signature", signed.signature),
    ]
    assert dict(pairs) == dict(headers())
    result = verify(headers=pairs)
    assert type(result.permit) is projection.ProjectionPermit
    assert asdict(result.pins) == BODY and result.token.value == legacy.TOKEN
    assert result.permit.expires_at == legacy.NOW + timedelta(minutes=15)
    assert legacy.b64(legacy.TOKEN) not in repr(result) and "DO-NOT-LOG" not in repr(result)
    assert caplog.records == []
    with pytest.raises(FrozenInstanceError):
        result.pins.expected_phase = "child"


@pytest.mark.parametrize(
    "document,domain",
    [
        (context(), b"CUSTOM_IMPORT_ADMISSION_PERMIT_V1\x00"),
        (context(), b"CUSTOM_IMPORT_SOURCE_PERMIT_V1\x00"),
        (context(), DOMAIN[:-1]),
        (legacy.permit(), DOMAIN),
        (source.context(), DOMAIN),
        (context(path=admission.ADMISSION_PATH), DOMAIN),
        (context(contract=admission.PERMIT_CONTRACT), DOMAIN),
    ],
)
def test_other_purpose_contract_path_or_signature_domain_cannot_authorize_projection(document, domain):
    legacy.rejected(verify, headers=headers(document, domain=domain))


def test_projection_cannot_authorize_either_older_purpose_after_header_relabel():
    legacy.rejected(
        legacy.verify, headers=[(name.replace("-Projection-", "-Admission-"), value) for name, value in headers()]
    )
    legacy.rejected(
        source.verify, headers=[(name.replace("-Projection-", "-Source-"), value) for name, value in headers()]
    )
    legacy.rejected(
        verify,
        headers=[(name.replace("-Admission-", "-Projection-"), value) for name, value in legacy.signed_headers()],
    )
    legacy.rejected(
        verify, headers=[(name.replace("-Source-", "-Projection-"), value) for name, value in source.headers()]
    )


@pytest.mark.parametrize(
    "changes",
    [
        {"method": "GET"},
        {"method": "post"},
        {"path": projection.PROJECTION_PATH + "/"},
        {"path": admission.ADMISSION_PATH},
        {"path": admission.SOURCE_PATH},
        {"query_string": "cursor=0"},
        {"query_string": b""},
        {"expected_origin": "https://other.example.invalid"},
        {"trusted_now": legacy.NOW - timedelta(microseconds=1)},
        {"trusted_now": legacy.NOW + timedelta(minutes=15)},
    ],
)
def test_exact_destination_and_exclusive_permit_expiry(changes):
    legacy.rejected(verify, **changes)


def test_retry_preserves_expiry_and_rejects_changed_context_authority():
    pairs = headers()
    assert verify(headers=pairs, trusted_now=legacy.NOW + timedelta(minutes=15) - timedelta(microseconds=1))
    altered = legacy.b64(legacy.canonical(context(dataset_id=99)))
    legacy.rejected(verify, headers=legacy.replacing(pairs, PREFIX + "Context", altered))
    legacy.rejected(verify, headers=legacy.replacing(pairs, PREFIX + "Signature", legacy.b64(b"x" * 32)))
    legacy.rejected(verify, headers=headers(context(origin="https://other.example.invalid")))


@pytest.mark.parametrize(
    "name", ["Content-Type", "Authorization", PREFIX + "Context", PREFIX + "Key-Id", PREFIX + "Signature"]
)
def test_missing_duplicate_and_case_duplicate_headers_are_denied(name):
    pairs = headers()
    value = dict(pairs)[name]
    legacy.rejected(verify, headers=[pair for pair in pairs if pair[0] != name])
    legacy.rejected(verify, headers=pairs + [(name.lower(), value)])
    replaced = [(name.lower(), value), *pairs[1:]] if name != "Content-Type" else [*pairs[:-1], (name.lower(), value)]
    legacy.rejected(verify, headers=replaced)


@pytest.mark.parametrize(
    "extra", [(PREFIX + "Extra", "1"), (admission.CONTEXT_HEADER, "x"), (admission.SOURCE_CONTEXT_HEADER, "x")]
)
def test_header_surface_is_closed_across_purposes(extra):
    legacy.rejected(verify, headers=headers() + [extra])


def test_duplicate_permit_fields_and_unknown_keys_cannot_gain_projection_authority():
    raw = legacy.canonical(context()).replace(b'"dataset_id":11', b'"dataset_id":11,"dataset_id":11')
    pairs = legacy.signed_headers(raw, domain=DOMAIN)
    legacy.rejected(verify, headers=[(name.replace("-Admission-", "-Projection-"), value) for name, value in pairs])
    legacy.rejected(verify, headers=legacy.replacing(headers(), PREFIX + "Key-Id", "unknown"))


@pytest.mark.parametrize("field", tuple(BODY))
def test_every_projection_body_pin_is_required(field):
    legacy.rejected(verify, body=legacy.canonical({key: value for key, value in BODY.items() if key != field}))


@pytest.mark.parametrize("field", ["build_id", "execution_id", "fence"])
@pytest.mark.parametrize("value", [0, -1, True, False, 1.0, "1", None, [], 2**63])
def test_runtime_identity_pins_are_positive_exact_bigints(field, value):
    legacy.rejected(verify, body=legacy.canonical(BODY | {field: value}))


@pytest.mark.parametrize("value", [-1, True, False, 1.0, "1", None, [], 2**63])
def test_entity_cursor_is_a_nonnegative_exact_bigint(value):
    legacy.rejected(verify, body=legacy.canonical(BODY | {"expected_after_entity_binding_id": value}))


@pytest.mark.parametrize("value", [-(2**63) - 1, 2**63, True, False, 1.0, "1", [], {}])
def test_selected_cursor_is_an_exact_signed_bigint(value):
    legacy.rejected(
        verify,
        body=legacy.canonical(
            BODY
            | {
                "expected_after_entity_binding_id": 1,
                "expected_after_selected_value": value,
            }
        ),
    )


@pytest.mark.parametrize("phase", ["root", "child", "indexes", "complete"])
@pytest.mark.parametrize("selected", [-(2**63), -1, 0, 2**63 - 1])
def test_signed_selection_boundaries_and_complete_context_cursor_are_preserved(phase, selected):
    document = BODY | {
        "build_id": 2**63 - 1,
        "execution_id": 2**63 - 1,
        "fence": 2**63 - 1,
        "expected_phase": phase,
        "expected_after_entity_binding_id": 2**63 - 1,
        "expected_after_selected_value": selected,
        "expected_after_child_key_sha256": "a" * 64 if phase != "root" else None,
    }
    assert asdict(verify(body=legacy.canonical(document)).pins) == document
    assert len(legacy.canonical(document)) <= 512


@pytest.mark.parametrize("value", [True, False, 1, [], "A" * 64, "g" * 64, "a" * 63, "a" * 65, " " + "a" * 63, ""])
def test_child_key_digest_requires_exact_lowercase_hex(value):
    document = BODY | {
        "expected_phase": "child",
        "expected_after_entity_binding_id": 1,
        "expected_after_selected_value": 0,
        "expected_after_child_key_sha256": value,
    }
    legacy.rejected(verify, body=legacy.canonical(document))


@pytest.mark.parametrize(
    "changes",
    [
        {"expected_after_selected_value": 0},
        {"expected_after_child_key_sha256": "a" * 64},
        {"expected_after_entity_binding_id": 1},
        {
            "expected_phase": "root",
            "expected_after_entity_binding_id": 1,
            "expected_after_selected_value": 0,
            "expected_after_child_key_sha256": "a" * 64,
        },
        {"expected_phase": "child", "expected_after_entity_binding_id": 1, "expected_after_selected_value": 0},
    ],
)
def test_cursor_components_cannot_be_partial_or_cross_phase(changes):
    legacy.rejected(verify, body=legacy.canonical(BODY | changes))


@pytest.mark.parametrize("phase", [None, True, 1, [], "", "ROOT", "verifying", "output", "root "])
def test_projection_phase_is_a_closed_string_enum(phase):
    legacy.rejected(verify, body=legacy.canonical(BODY | {"expected_phase": phase}))


@pytest.mark.parametrize("value", [None, True, -1, 34, 1.0, "0", []])
def test_index_cursor_requires_a_bounded_integer(value):
    legacy.rejected(verify, body=legacy.canonical(BODY | {"expected_phase": "indexes", "expected_index_count": value}))


@pytest.mark.parametrize("phase", ["root", "child"])
def test_numeric_pages_cannot_carry_index_progress(phase):
    legacy.rejected(verify, body=legacy.canonical(BODY | {"expected_phase": phase, "expected_index_count": 1}))


@pytest.mark.parametrize(
    "raw",
    [
        legacy.canonical(BODY | {"sql": "SELECT 1"}),
        legacy.canonical(BODY | {"rows": []}),
        b" " + legacy.canonical(BODY),
        legacy.canonical(BODY) + b"\n",
        b"x" * 513,
        b"[]",
        b"null",
        b"{",
        b"\xff",
        b'{"build_id":51,' + legacy.canonical(BODY)[1:],
        legacy.canonical(BODY).replace(b'"fence":7', b'"fence":7,"fence":7'),
        legacy.canonical(BODY).replace(b'"fence":7', b'"fence":NaN'),
        legacy.canonical(BODY).replace(b'"fence":7', b'"fence":7e0'),
        legacy.canonical(BODY).replace(
            b'"expected_after_entity_binding_id":0', b'"expected_after_entity_binding_id":-0'
        ),
    ],
)
def test_closed_canonical_body_rejects_extra_duplicate_noncanonical_and_oversized_data(raw):
    legacy.rejected(verify, body=raw)


@pytest.mark.parametrize(
    "flags",
    [
        {"is_projection": True, "is_source": True},
        {"is_projection": 1},
        {"is_projection": "true"},
        {"is_source": None},
    ],
)
def test_issuer_purpose_flags_cannot_be_ambiguous_or_coerced(flags):
    legacy.rejected(
        admission.sign_permit,
        legacy.canonical(context()),
        expected_origin=legacy.ORIGIN,
        trusted_now=legacy.NOW,
        launch_expires_at=legacy.NOW + timedelta(minutes=15),
        keyring=legacy.keyring(),
        **flags,
    )


def test_body_cursor_is_not_a_replacement_for_locked_lease_authority():
    first = verify()
    other = verify(body=legacy.canonical(BODY | {"build_id": 99, "execution_id": 98, "fence": 97}))
    assert first.permit == other.permit and first.token == other.token
    assert first.pins != other.pins
