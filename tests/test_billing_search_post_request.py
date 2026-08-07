# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import copy
import json
import pickle

import pytest

from api import billing_search_post_request_values as request_values
from api.billing_search_post_request import (
    BILLING_SEARCH_POST_MAX_CURSOR_CHARACTERS,
    BILLING_SEARCH_POST_MAX_LIMIT,
    BillingSearchPostRequestError,
    apply_entitled_billing_search_tax_identity,
    bind_billing_search_post_request_fingerprint,
    parse_billing_search_post_request,
    validate_billing_search_post_request,
)
from process.provider_directory_profile import is_valid_npi


def _synthetic_ein(seed: int = 0, *, formatted: bool = False) -> str:
    digits = f"{12 + seed:02d}{3_456_789 + seed:07d}"
    return f"{digits[:2]}-{digits[2:]}" if formatted else digits


def _synthetic_npi(seed: int = 0) -> str:
    prefix = f"{100_000_000 + seed:09d}"
    return next(
        prefix + str(check_digit)
        for check_digit in range(10)
        if is_valid_npi(prefix + str(check_digit))
    )


def _payload(*, identity: dict[str, object] | None = None) -> dict[str, object]:
    return {
        "healthporta_plan_id": "hpplan_" + "0" * 26,
        "billing_identity": (
            identity
            if identity is not None
            else {
                "tax_identity": {
                    "type": "ein",
                    "value": _synthetic_ein(formatted=True),
                }
            }
        ),
        "procedure": {
            "code_system": "CPT",
            "code": "99213",
            "modifiers": [],
            "place_of_service": [],
        },
        "geo": {"zip5": "12345", "radius_miles": 0},
    }


def _assert_invalid(payload: object) -> BillingSearchPostRequestError:
    with pytest.raises(BillingSearchPostRequestError) as captured:
        parse_billing_search_post_request(payload)
    error = captured.value
    assert str(error) == "billing_search_post_request_invalid"
    assert error.__cause__ is None
    assert error.__context__ is None
    return error


def test_request_is_redacted_immutable_and_has_safe_service_projection() -> None:
    request_body_by_field = _payload()
    request_body_by_field["provider_npi"] = _synthetic_npi()
    request = parse_billing_search_post_request(request_body_by_field)
    service_query = request.service_query

    assert repr(request) == "<redacted-billing-search-post-request>"
    assert repr(service_query) == "<redacted-billing-search-post-service-query>"
    assert copy.copy(request) is request
    assert copy.deepcopy(request) is request
    assert copy.copy(service_query) is service_query
    assert copy.deepcopy(service_query) is service_query
    with pytest.raises(BillingSearchPostRequestError):
        pickle.dumps(request)
    with pytest.raises(BillingSearchPostRequestError):
        pickle.dumps(service_query)
    with pytest.raises(BillingSearchPostRequestError):
        type(service_query)()
    with pytest.raises(TypeError):
        request.limit = 30
    with pytest.raises(TypeError):
        service_query.limit = 30
    with pytest.raises(TypeError):
        del service_query.limit

    assert request.selector_kind == "tax_identity"
    assert request.tax_identity_type == "ein"
    assert request.billing_entity_ref is None
    assert not hasattr(request, "tax_identity_value")
    assert request.provider_npi == int(_synthetic_npi())
    assert request.include_evidence is False
    assert service_query.healthporta_plan_id == request.healthporta_plan_id
    assert service_query.selector_kind == request.selector_kind
    assert service_query.tax_identity_type == request.tax_identity_type
    assert service_query.code_system == "CPT"
    assert service_query.code == "99213"
    assert service_query.modifiers == ()
    assert service_query.place_of_service == ()
    assert service_query.zip5 == "12345"
    assert service_query.radius_miles == 0.0
    assert service_query.provider_npi == request.provider_npi
    assert service_query.include_evidence is False
    assert service_query.limit == 25
    assert service_query.cursor is None
    assert service_query.request_shape_sha256 == request.request_shape_sha256
    assert service_query.procedure_args == {
        "code_system": "CPT",
        "code": "99213",
        "modifiers": (),
        "place_of_service": (),
    }
    assert service_query.geo_args == {"zip5": "12345", "radius_miles": 0.0}
    assert service_query.page_args == {"limit": 25, "cursor": None}
    assert not hasattr(service_query, "billing_entity_ref")
    assert not hasattr(service_query, "tax_identity_value")
    assert validate_billing_search_post_request(request) is request


def test_optional_include_evidence_is_strict_and_shape_bound() -> None:
    ordinary = parse_billing_search_post_request(_payload())
    detailed_payload = _payload()
    detailed_payload["include_evidence"] = True
    detailed = parse_billing_search_post_request(detailed_payload)

    assert detailed.include_evidence is True
    assert detailed.service_query.include_evidence is True
    assert detailed.request_shape_sha256 != ordinary.request_shape_sha256

    for invalid_value in (0, 1, None, "false", []):
        invalid_payload = _payload()
        invalid_payload["include_evidence"] = invalid_value
        _assert_invalid(invalid_payload)


def test_exactly_one_closed_selector_and_closed_objects_are_required() -> None:
    reference = "be1_" + "A" * 64
    invalid_identities = (
        {},
        {
            "tax_identity": _payload()["billing_identity"]["tax_identity"],
            "billing_entity_ref": reference,
        },
        {"unknown": reference},
        {"billing_entity_ref": reference, "unknown": False},
    )
    for identity in invalid_identities:
        _assert_invalid(_payload(identity=identity))

    extra_top_level = _payload()
    extra_top_level["snapshot_id"] = "not-client-selectable"
    _assert_invalid(extra_top_level)

    extra_procedure = _payload()
    extra_procedure["procedure"]["description"] = "not-accepted"
    _assert_invalid(extra_procedure)

    extra_geo = _payload()
    extra_geo["geo"]["state"] = "NA"
    _assert_invalid(extra_geo)


def test_reviewed_ein_forms_are_normalized_only_inside_entitled_callback() -> None:
    for raw_value in (_synthetic_ein(), "  " + _synthetic_ein(formatted=True) + "\n"):
        payload = _payload()
        payload["billing_identity"]["tax_identity"]["value"] = raw_value
        request = parse_billing_search_post_request(payload)
        seen_identities: list[tuple[str, str]] = []

        result = apply_entitled_billing_search_tax_identity(
            request,
            lambda tin_type, value: seen_identities.append((tin_type, value))
            or "resolved",
        )

        assert result == "resolved"
        assert seen_identities == [("ein", _synthetic_ein())]

    for malformed in (
        _synthetic_ein() + "0",
        _synthetic_ein()[:2] + " " + _synthetic_ein()[2:],
        "",
    ):
        payload = _payload()
        payload["billing_identity"]["tax_identity"]["value"] = malformed
        error = _assert_invalid(payload)
        if malformed:
            assert malformed not in repr(error)


def test_billing_and_provider_npis_require_valid_checksums() -> None:
    billing_npi = _synthetic_npi(1)
    provider_npi = _synthetic_npi(2)
    payload = _payload(identity={"tax_identity": {"type": "npi", "value": billing_npi}})
    payload["provider_npi"] = provider_npi
    request = parse_billing_search_post_request(payload)

    assert request.tax_identity_type == "npi"
    assert request.provider_npi == int(provider_npi)
    assert apply_entitled_billing_search_tax_identity(
        request,
        lambda tin_type, value: (tin_type, value),
    ) == ("npi", billing_npi)

    invalid_npi = billing_npi[:-1] + str((int(billing_npi[-1]) + 1) % 10)
    invalid_billing = copy.deepcopy(payload)
    invalid_billing["billing_identity"]["tax_identity"]["value"] = invalid_npi
    _assert_invalid(invalid_billing)
    invalid_provider = copy.deepcopy(payload)
    invalid_provider["provider_npi"] = invalid_npi
    _assert_invalid(invalid_provider)


def test_reference_is_structurally_parsed_but_not_resolved_or_exposed_to_service() -> (
    None
):
    reference = "be1_" + "b" * 64
    request = parse_billing_search_post_request(
        _payload(identity={"billing_entity_ref": reference})
    )

    assert request.selector_kind == "billing_entity_ref"
    assert request.tax_identity_type is None
    assert request.billing_entity_ref == reference
    assert not hasattr(request.service_query, "billing_entity_ref")
    with pytest.raises(BillingSearchPostRequestError):
        apply_entitled_billing_search_tax_identity(
            request,
            lambda _tin_type, _value: None,
        )

    for invalid_reference in (
        "be1_",
        "be1_" + "a" * 63,
        "be1_" + "a" * 65,
        "be1_" + "a" * 63 + ".",
        "be2_" + "a" * 64,
    ):
        _assert_invalid(_payload(identity={"billing_entity_ref": invalid_reference}))


def test_procedure_geo_and_page_are_exact_and_bounded() -> None:
    boundary_payload = _payload()
    boundary_payload["procedure"]["modifiers"] = ["25", "TC"]
    boundary_payload["procedure"]["place_of_service"] = ["11", "22"]
    boundary_payload["geo"]["radius_miles"] = 100
    boundary_payload["page"] = {
        "limit": BILLING_SEARCH_POST_MAX_LIMIT,
        "cursor": "c" * BILLING_SEARCH_POST_MAX_CURSOR_CHARACTERS,
    }
    request = parse_billing_search_post_request(boundary_payload)
    assert request.radius_miles == 100.0
    assert request.limit == BILLING_SEARCH_POST_MAX_LIMIT

    invalid_payloads = []
    for radius in (-0.1, 100.1, True, "25"):
        candidate = _payload()
        candidate["geo"]["radius_miles"] = radius
        invalid_payloads.append(candidate)
    for limit in (0, BILLING_SEARCH_POST_MAX_LIMIT + 1, True, 25.0):
        candidate = _payload()
        candidate["page"] = {"limit": limit, "cursor": None}
        invalid_payloads.append(candidate)
    candidate = _payload()
    candidate["page"] = {"limit": 25, "offset": 0}
    invalid_payloads.append(candidate)
    candidate = _payload()
    candidate["page"] = {
        "limit": 25,
        "cursor": "c" * (BILLING_SEARCH_POST_MAX_CURSOR_CHARACTERS + 1),
    }
    invalid_payloads.append(candidate)
    candidate = _payload()
    candidate["procedure"]["modifiers"] = ["TC", "25"]
    invalid_payloads.append(candidate)
    candidate = _payload()
    candidate["procedure"]["place_of_service"] = ["11", "11"]
    invalid_payloads.append(candidate)
    candidate = _payload()
    candidate["procedure"]["code"] = " 99213 "
    invalid_payloads.append(candidate)
    candidate = _payload()
    candidate["geo"]["zip5"] = "1234"
    invalid_payloads.append(candidate)

    for invalid_payload in invalid_payloads:
        _assert_invalid(invalid_payload)


def test_request_shape_omits_selector_values_and_cursor_but_binds_filters() -> None:
    first_payload = _payload()
    second_payload = _payload()
    second_payload["billing_identity"]["tax_identity"]["value"] = _synthetic_ein(1)
    second_payload["page"] = {"limit": 25, "cursor": "next-page"}
    first = parse_billing_search_post_request(first_payload)
    second = parse_billing_search_post_request(second_payload)

    assert first.request_shape_sha256 == second.request_shape_sha256

    filtered_payload = _payload()
    filtered_payload["geo"]["zip5"] = "54321"
    filtered = parse_billing_search_post_request(filtered_payload)
    assert filtered.request_shape_sha256 != first.request_shape_sha256

    ref_one = parse_billing_search_post_request(
        _payload(identity={"billing_entity_ref": "be1_" + "a" * 64})
    )
    ref_two = parse_billing_search_post_request(
        _payload(identity={"billing_entity_ref": "be1_" + "b" * 64})
    )
    assert ref_one.request_shape_sha256 == ref_two.request_shape_sha256
    assert ref_one.request_shape_sha256 != first.request_shape_sha256


def test_shape_and_fingerprint_payloads_never_receive_selector_values(
    monkeypatch,
) -> None:
    encoded_payloads: list[str] = []
    original_encoder = request_values._canonical_json_bytes

    def capture(value: object) -> bytes:
        encoded_payloads.append(json.dumps(value, sort_keys=True))
        return original_encoder(value)

    monkeypatch.setattr(request_values, "_canonical_json_bytes", capture)
    raw_value = _synthetic_ein(3)
    reference = "be1_" + "z" * 64
    tax_payload = _payload()
    tax_payload["billing_identity"]["tax_identity"]["value"] = raw_value
    tax_request = parse_billing_search_post_request(tax_payload)
    parse_billing_search_post_request(
        _payload(identity={"billing_entity_ref": reference})
    )
    bind_billing_search_post_request_fingerprint(
        tax_request,
        selector_scope_sha256="1" * 64,
    )

    encoded = "\n".join(encoded_payloads)
    assert raw_value not in encoded
    assert reference not in encoded


def test_fingerprint_requires_server_derived_strict_selector_scope() -> None:
    request = parse_billing_search_post_request(_payload())
    first = bind_billing_search_post_request_fingerprint(
        request,
        selector_scope_sha256="1" * 64,
    )
    second = bind_billing_search_post_request_fingerprint(
        request,
        selector_scope_sha256="2" * 64,
    )
    assert len(first) == 64
    assert first != second

    for invalid_scope in (None, "0" * 64, "A" * 64, "1" * 63, b"1" * 64):
        with pytest.raises(BillingSearchPostRequestError):
            bind_billing_search_post_request_fingerprint(
                request,
                selector_scope_sha256=invalid_scope,
            )


def test_callback_errors_are_value_free_and_query_tamper_is_rejected() -> None:
    raw_value = _synthetic_ein(4)
    request_body_by_field = _payload()
    request_body_by_field["billing_identity"]["tax_identity"]["value"] = raw_value
    request = parse_billing_search_post_request(request_body_by_field)

    def fail(_tin_type: str, value: str) -> None:
        raise RuntimeError("callback rejected " + value)

    with pytest.raises(BillingSearchPostRequestError) as captured:
        apply_entitled_billing_search_tax_identity(request, fail)
    assert raw_value not in repr(captured.value)
    assert captured.value.__context__ is None

    object.__setattr__(
        request.service_query,
        "_BillingSearchPostServiceQuery__zip5",
        "99999",
    )
    with pytest.raises(BillingSearchPostRequestError):
        validate_billing_search_post_request(request)


def test_canonical_selector_substitution_breaks_the_internal_keyed_seal() -> None:
    original_ein = _synthetic_ein(6)
    replacement_ein = _synthetic_ein(7)
    request_body_by_field = _payload()
    request_body_by_field["billing_identity"]["tax_identity"]["value"] = (
        original_ein
    )
    request = parse_billing_search_post_request(request_body_by_field)

    object.__setattr__(
        request,
        "_BillingSearchPostRequest__tax_identity_value",
        replacement_ein,
    )

    with pytest.raises(BillingSearchPostRequestError) as captured:
        validate_billing_search_post_request(request)
    with pytest.raises(BillingSearchPostRequestError):
        apply_entitled_billing_search_tax_identity(
            request,
            lambda identity_type, normalized_identity_value: (
                identity_type,
                normalized_identity_value,
            ),
        )
    assert original_ein not in repr(captured.value)
    assert replacement_ein not in repr(captured.value)

    original_reference = "be1_" + "a" * 64
    replacement_reference = "be1_" + "b" * 64
    reference_request = parse_billing_search_post_request(
        _payload(identity={"billing_entity_ref": original_reference})
    )
    object.__setattr__(
        reference_request,
        "_BillingSearchPostRequest__billing_entity_ref",
        replacement_reference,
    )
    with pytest.raises(BillingSearchPostRequestError) as reference_error:
        validate_billing_search_post_request(reference_request)
    assert original_reference not in repr(reference_error.value)
    assert replacement_reference not in repr(reference_error.value)


def test_request_parser_error_drops_sensitive_input_from_its_frame() -> None:
    sensitive_value = _synthetic_ein(5)
    malformed_body_by_field = _payload()
    malformed_body_by_field["billing_identity"]["tax_identity"]["value"] = (
        sensitive_value + "0"
    )
    error = _assert_invalid(malformed_body_by_field)

    traceback = error.__traceback__
    parser_frames = []
    while traceback is not None:
        if traceback.tb_frame.f_code.co_name == "parse_billing_search_post_request":
            parser_frames.append(traceback.tb_frame.f_locals)
        traceback = traceback.tb_next
    assert parser_frames
    assert all("payload" not in local_values for local_values in parser_frames)
