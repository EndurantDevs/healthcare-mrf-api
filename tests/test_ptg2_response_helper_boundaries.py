# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from decimal import Decimal

from api import ptg2_response as response


def test_request_boolean_sequences_and_defaults_are_deterministic():
    assert response._is_request_flag_enabled(["yes"]) is True
    assert response._is_request_flag_enabled([], default=True) is True
    assert response._is_request_flag_enabled([""], default=True) is True


def test_optional_number_parsers_reject_non_numeric_values():
    assert response._optional_float(object()) is None
    assert response._optional_decimal(object()) is None


def test_response_shape_and_exact_number_fragment_cover_nested_tuples():
    assert response._shape_ptg2_response(
        {"items": [], "query": {}},
        {"include_details": True},
    ) == {"items": [], "query": {}}
    assert response._fragment_exact_numbers((1, {"value": 2})) == (
        1,
        {"value": 2},
    )


def test_catalog_helpers_reject_empty_keys_and_build_modifier_fallbacks():
    assert response._catalog_key(None, None) is None
    assert response._missing_modifier_detail(None) is None
    assert response._missing_modifier_detail("26") == {
        "code_system": "MODIFIER",
        "code": "26",
        "display_name": "Modifier 26",
        "short_description": None,
        "catalog_status": "missing",
    }


def test_numeric_rate_guards_non_numbers_nonfinite_values_and_signed_zero():
    marker = object()
    assert response._coerce_numeric_rate(marker) is marker
    assert response._coerce_numeric_rate(Decimal("NaN")).is_nan()
    assert response._coerce_numeric_rate(Decimal("-0.00")) == 0


def test_string_list_normalization_handles_json_lists_and_scalar_values():
    assert response._normalize_string_list('["A", null, ""]') == ["A"]
    assert response._normalize_string_list(7) == ["7"]


def test_price_payload_normalization_skips_wrong_shapes_and_rows():
    assert response._normalize_price_payload("{}") == []
    assert response._normalize_price_payload([None, {"negotiated_rate": "4"}]) == [
        {"negotiated_rate": 4}
    ]


def test_filter_and_component_normalization_cover_empty_and_literal_paths():
    assert response._normalize_filter_string_list(" ,x", upper=False) == ["x"]
    assert response._price_component(["26"]) == "professional"
    assert response._price_component(["TC"]) == "technical"
