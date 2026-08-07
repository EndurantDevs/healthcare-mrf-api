# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import pytest

from api import billing_search_post_request as request_module


def _payload() -> dict[str, object]:
    return {
        "billing_identity": {"tax_identity": {"type": "ein", "value": "12-3333333"}},
        "geo": {"radius_miles": 0, "zip5": "00000"},
        "healthporta_plan_id": "hpplan_" + "0" * 26,
        "procedure": {
            "code": "00000",
            "code_system": "CPT",
            "modifiers": [],
            "place_of_service": [],
        },
    }


def _request() -> request_module.BillingSearchPostRequest:
    return request_module.parse_billing_search_post_request(_payload())


def test_request_constructor_deletion_and_noncallable_callback_are_closed() -> None:
    request = _request()
    with pytest.raises(TypeError):
        del request.limit
    with pytest.raises(request_module.BillingSearchPostRequestError):
        request_module.BillingSearchPostRequest()
    with pytest.raises(request_module.BillingSearchPostRequestError):
        request_module.apply_entitled_billing_search_tax_identity(request, object())


def test_request_consumers_reject_wrong_runtime_type() -> None:
    with pytest.raises(request_module.BillingSearchPostRequestError):
        request_module.validate_billing_search_post_request(object())
    with pytest.raises(request_module.BillingSearchPostRequestError):
        request_module._billing_search_post_request_auth_binding(object())
    with pytest.raises(request_module.BillingSearchPostRequestError):
        request_module.bind_billing_search_post_request_fingerprint(
            object(),
            selector_scope_sha256="1" * 64,
        )


def test_request_revalidation_rejects_invalid_normalized_field() -> None:
    request = _request()
    object.__setattr__(
        request,
        "_BillingSearchPostRequest__code",
        " noncanonical ",
    )
    with pytest.raises(request_module.BillingSearchPostRequestError):
        request_module.validate_billing_search_post_request(request)


def test_request_revalidation_rejects_canonical_type_substitution() -> None:
    request = _request()
    object.__setattr__(
        request,
        "_BillingSearchPostRequest__radius_miles",
        0,
    )
    with pytest.raises(request_module.BillingSearchPostRequestError):
        request_module.validate_billing_search_post_request(request)


def test_request_revalidation_rejects_shape_substitution() -> None:
    request = _request()
    object.__setattr__(
        request,
        "_BillingSearchPostRequest__request_shape_sha256",
        "f" * 64,
    )
    with pytest.raises(request_module.BillingSearchPostRequestError):
        request_module.validate_billing_search_post_request(request)


def test_request_revalidation_closes_missing_internal_state() -> None:
    request = _request()
    object.__delattr__(request, "_BillingSearchPostRequest__code")
    with pytest.raises(request_module.BillingSearchPostRequestError):
        request_module.validate_billing_search_post_request(request)


def test_request_parser_closes_failed_internal_revalidation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        request_module,
        "_validated_request_or_none",
        lambda _request_candidate: None,
    )
    with pytest.raises(request_module.BillingSearchPostRequestError):
        request_module.parse_billing_search_post_request(_payload())
