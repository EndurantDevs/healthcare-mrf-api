# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

from types import SimpleNamespace
from uuid import UUID

from api import ptg2_serving_utils as serving_utils
from api.ptg2_types import PTG2ServingIndex


def _serving_index(providers):
    return PTG2ServingIndex(
        snapshot_id="snapshot",
        version=3,
        plans={},
        procedures={},
        providers=providers,
        rates={},
    )


def test_serving_identity_helpers_cover_empty_and_driver_values():
    assert serving_utils.ein_plan_id_variants("   ") == []
    assert serving_utils._provider_payload(
        _serving_index({"4": {"npi": "1234567890"}}),
        4,
    ) == {"npi": "1234567890", "provider_ordinal": 4}
    assert serving_utils._provider_payload(
        _serving_index({"4": {"provider_ordinal": 9}}),
        4,
    ) == {"provider_ordinal": 9}
    assert serving_utils._row_mapping(
        SimpleNamespace(_mapping={"value": 1})
    ) == {"value": 1}
    assert serving_utils._row_mapping({"value": 2}) == {"value": 2}
    assert serving_utils._row_mapping((("value", 3),)) == {"value": 3}
    assert serving_utils._uuid_to_hex(None) == ""
    assert serving_utils._uuid_to_hex("  ") == ""
    assert serving_utils._uuid_to_hex(
        UUID("12345678-1234-5678-1234-567812345678")
    ) == "12345678123456781234567812345678"
    assert serving_utils._uuid_to_hex(b"\x01" * 16) == "01" * 16


def test_price_filter_clauses_cover_all_exact_filter_families():
    params_by_name = {}

    clauses, query_values = serving_utils._price_filter_clauses(
        {
            "service_code": "11",
            "billing_code_modifier": "26",
            "negotiated_rate": "12.50",
        },
        params_by_name,
    )

    assert len(clauses) == 3
    assert query_values == {
        "service_code": ["11"],
        "pos": "11",
        "billing_code_modifier": ["26"],
        "negotiated_rate": 12.5,
        "rate_tolerance": 0.01,
    }
    assert params_by_name == {
        "price_service_codes": ["11"],
        "price_modifier_codes": ["26"],
        "price_negotiated_rate": serving_utils.Decimal("12.50"),
        "price_rate_tolerance": serving_utils.Decimal("0.01"),
    }


def test_price_filter_clauses_preserve_explicit_rate_tolerance():
    params_by_name = {}

    clauses, query_values = serving_utils._price_filter_clauses(
        {
            "rate": "8.75",
            "rate_tolerance": "0.25",
        },
        params_by_name,
    )

    assert len(clauses) == 1
    assert query_values == {
        "negotiated_rate": 8.75,
        "rate_tolerance": 0.25,
    }
    assert params_by_name["price_rate_tolerance"] == serving_utils.Decimal("0.25")
