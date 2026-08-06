# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Canonical cross-service billing-search request contract tests."""

from __future__ import annotations

import pytest

from api import billing_search_transport_contract as contract

PLAN_RELEASE_ID = "hprelease_01K123456789ABCDEFGHJKMNPQ"
PLAN_ENTITLEMENT_SHA256 = (
    "5c9c10b366adebd6b43837ce55dce562deb56734195232b644721d1e92cb0aa8"
)
QUERY_SHA256 = "a8cdd2c68e374889cb8b054644cd730ea6d3cab35b9bfaa22a3fbb657bebc8ab"
QUOTA_SHA256 = "6528ad0abcd846bc4fed67b1989759c9897054ddd5c68eabf00ccc9743169eec"
METERING_RECEIPT_SHA256 = (
    "e346f675956997adab11447cacb50203fa1f0d2bb4dc71c57bed464acca8566c"
)
REQUEST_ID = "123e4567-e89b-42d3-a456-426614174000"
BILLING_ENTITY_REF = (
    "be1_AAECAwQFBgcICQoLDA0ODxIr3ljg-uNk13KslT9vSXm4lGO1maZsqjUk0Jf9HUBm"
)
QUERY_PAIRS = (
    ("billing_entity_ref", BILLING_ENTITY_REF),
    ("code", "99213"),
    ("code_system", "CPT"),
    ("limit", "25"),
    ("plan_release_id", PLAN_RELEASE_ID),
    ("zip5", "25701"),
)


def test_cross_language_query_and_plan_golden_vectors() -> None:
    normalized_pairs = contract.normalize_billing_search_query_pairs(
        tuple(reversed(QUERY_PAIRS))
    )
    encoded_pairs = contract._canonical_json_bytes(normalized_pairs)

    assert normalized_pairs == QUERY_PAIRS
    assert len(encoded_pairs) == 225
    assert contract.billing_search_query_sha256(QUERY_PAIRS) == QUERY_SHA256
    assert (
        contract.billing_search_plan_entitlement_sha256(PLAN_RELEASE_ID)
        == PLAN_ENTITLEMENT_SHA256
    )


@pytest.mark.parametrize(
    "plan_release_id",
    [
        f" {PLAN_RELEASE_ID}",
        f"{PLAN_RELEASE_ID} ",
        f"\t{PLAN_RELEASE_ID}\n",
    ],
)
def test_plan_entitlement_rejects_noncanonical_release_ids(plan_release_id) -> None:
    with pytest.raises(
        contract.BillingSearchTransportError,
        match="^billing_search_transport_invalid$",
    ):
        contract.billing_search_plan_entitlement_sha256(plan_release_id)


def test_cross_language_metering_receipt_golden_vector() -> None:
    assert (
        contract.billing_search_metering_receipt_sha256(
            method="GET",
            path=contract.BILLING_SEARCH_TRANSPORT_PATH,
            plan_entitlement_sha256=PLAN_ENTITLEMENT_SHA256,
            query_sha256=QUERY_SHA256,
            quota_scope_sha256=QUOTA_SHA256,
            request_id=REQUEST_ID,
        )
        == METERING_RECEIPT_SHA256
    )


def test_request_binding_is_closed_and_redacted() -> None:
    binding = contract.BillingSearchTransportRequestBinding(
        method="GET",
        path=contract.BILLING_SEARCH_TRANSPORT_PATH,
        query_pairs=tuple(reversed(QUERY_PAIRS)),
        plan_release_id=PLAN_RELEASE_ID,
        trusted_now="2031-01-02T03:04:05Z",
    )

    assert binding.query_sha256 == QUERY_SHA256
    assert binding.plan_entitlement_sha256 == PLAN_ENTITLEMENT_SHA256
    assert repr(binding) == "<redacted-billing-search-transport>"
    assert BILLING_ENTITY_REF not in repr(binding)


@pytest.mark.parametrize(
    "query_pairs",
    [
        [],
        (),
        (("code", "99213"), ("code", "99214")),
        (("", "99213"),),
        (("code", ""),),
        (("code", "\N{SNOWMAN}"),),
        (("code", "line\nbreak"),),
        (("x" * 65, "value"),),
        (("key", "x" * 2049),),
        (("key",),),
        (["key", "value"],),
        tuple((f"key-{index}", "value") for index in range(33)),
    ],
)
def test_query_pairs_reject_noncanonical_or_ambiguous_shapes(query_pairs) -> None:
    with pytest.raises(
        contract.BillingSearchTransportError,
        match="^billing_search_transport_invalid$",
    ):
        contract.billing_search_query_sha256(query_pairs)


@pytest.mark.parametrize(
    "overrides",
    [
        {"method": "POST"},
        {"path": "/api/v1/pricing/providers/by-service"},
        {"plan_release_id": "not-a-release"},
        {"trusted_now": "2031-01-02T03:04:05+00:00"},
        {"query_pairs": [["code", "99213"]]},
    ],
)
def test_request_binding_rejects_wrong_coordinates(overrides) -> None:
    binding_fields_by_name = {
        "method": "GET",
        "path": contract.BILLING_SEARCH_TRANSPORT_PATH,
        "query_pairs": QUERY_PAIRS,
        "plan_release_id": PLAN_RELEASE_ID,
        "trusted_now": "2031-01-02T03:04:05Z",
    }
    binding_fields_by_name.update(overrides)

    with pytest.raises(
        contract.BillingSearchTransportError,
        match="^billing_search_transport_invalid$",
    ):
        contract.BillingSearchTransportRequestBinding(**binding_fields_by_name)


@pytest.mark.parametrize(
    "overrides",
    [
        {"method": "POST"},
        {"path": "/different"},
        {"plan_entitlement_sha256": "0" * 64},
        {"query_sha256": "A" * 64},
        {"quota_scope_sha256": "f" * 63},
        {"request_id": "123e4567-e89b-12d3-a456-426614174000"},
    ],
)
def test_metering_receipt_rejects_noncanonical_coordinates(overrides) -> None:
    receipt_fields_by_name = {
        "method": "GET",
        "path": contract.BILLING_SEARCH_TRANSPORT_PATH,
        "plan_entitlement_sha256": PLAN_ENTITLEMENT_SHA256,
        "query_sha256": QUERY_SHA256,
        "quota_scope_sha256": QUOTA_SHA256,
        "request_id": REQUEST_ID,
    }
    receipt_fields_by_name.update(overrides)

    with pytest.raises(
        contract.BillingSearchTransportError,
        match="^billing_search_transport_invalid$",
    ):
        contract.billing_search_metering_receipt_sha256(**receipt_fields_by_name)
