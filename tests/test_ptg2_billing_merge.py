# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Serving merge coverage for exact billing-association attachment."""

from __future__ import annotations

from api import ptg2_serving


_BASE_PROVIDER = {
    "npi": 1234567890,
    "location_hash": "synthetic-location",
    "reported_code": "29881",
    "reported_code_system": "CPT",
    "address": {"first_line": "100 Example Street"},
}


def _lineage_ref(value: int) -> str:
    return f"{value:032x}"


def _provider_rate(provider_set_ref: str, ordinal: int):
    return {
        **_BASE_PROVIDER,
        "provider_set_hash": provider_set_ref,
        "price_set_hash": _lineage_ref(100 + ordinal),
        "rate_pack_hash": _lineage_ref(200 + ordinal),
        "prices": [{"negotiated_rate": ordinal * 100}],
    }


def _billing_association(hex_character: str):
    return {
        "provider_group_ref": hex_character * 32,
        "tax_identity_status": "matched_ein",
        "tin_type": "ein",
        "billing_entity_ref": f"be1_{hex_character * 64}",
    }


def test_provider_rate_merge_attaches_billing_edges_after_grouping():
    first_set_ref = _lineage_ref(1)
    second_set_ref = _lineage_ref(2)
    associations_by_set = {
        first_set_ref: [_billing_association("1")],
        second_set_ref: [_billing_association("2")],
    }
    merged = ptg2_serving._merge_provider_rates_for_request(
        [_provider_rate(first_set_ref, 1), _provider_rate(second_set_ref, 2)],
        associations_by_set,
    )

    assert [
        option["billing_associations"][0]["association_ordinal"]
        for option in merged[0]["rate_options"]
    ] == [1, 1]
    assert all(
        "provider_group_ref" not in option["billing_associations"][0]
        for option in merged[0]["rate_options"]
    )
    assert merged[0]["billing_entity_count"] == 2
    assert merged[0]["billing_entity_count_status"] == "exact"
    legacy_shape = ptg2_serving._merge_provider_rates_for_request(
        [_provider_rate(first_set_ref, 1)],
        {},
    )[0]
    assert "billing_entity_count" not in legacy_shape
