# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Fail-closed boundary coverage for billing-search response shaping."""

from __future__ import annotations

from dataclasses import replace
from decimal import Decimal
from types import SimpleNamespace

import pytest

from api import billing_search_response as response
from api import billing_search_response_values as response_values
from api.ptg2_billing_search_contract import BillingSearchServingUnavailableError
from tests.billing_search_post_support import matched_result, query, selection


def _budget() -> response_values.PublicResponseBudget:
    return response_values.PublicResponseBudget()


def _provider():
    return matched_result().providers[0]


def _price() -> dict[str, object]:
    return {
        "negotiated_rate": "20.50",
        "service_code": ["11"],
        "billing_code_modifier": [],
    }


def test_response_budget_rejects_excess_atoms_and_text(monkeypatch) -> None:
    monkeypatch.setattr(response_values, "MAX_PUBLIC_PRICE_ATOMS", 0)
    with pytest.raises(BillingSearchServingUnavailableError):
        _budget().retain_price_atom()

    with pytest.raises(BillingSearchServingUnavailableError):
        _budget().retain_text("too-long", maximum_bytes=3)


def test_public_text_rejects_nonprintable_and_array_shape() -> None:
    with pytest.raises(BillingSearchServingUnavailableError):
        response_values.public_text("line\nbreak", _budget(), optional=False)

    with pytest.raises(BillingSearchServingUnavailableError):
        response_values.public_text_array(object(), _budget())


def test_public_text_array_rechecks_normalized_members(monkeypatch) -> None:
    monkeypatch.setattr(
        response_values,
        "public_text",
        lambda *_args, **_kwargs: None,
    )

    with pytest.raises(BillingSearchServingUnavailableError):
        response_values.public_text_array(["synthetic"], _budget())


def test_projected_decimal_size_covers_special_and_exponent_boundaries() -> None:
    with pytest.raises(BillingSearchServingUnavailableError):
        response_values._projected_plain_decimal_characters(Decimal("NaN"))

    assert response_values._projected_plain_decimal_characters(Decimal("1E+2")) == 3
    assert response_values._projected_plain_decimal_characters(Decimal("0.001")) == 5


@pytest.mark.parametrize(
    "rate_value",
    [
        True,
        1 << 513,
        "1e2",
        Decimal("NaN"),
    ],
)
def test_public_rate_rejects_unsafe_numeric_forms(rate_value) -> None:
    with pytest.raises(BillingSearchServingUnavailableError):
        response_values.validate_public_rate_value(rate_value)


def test_public_rate_sanitizes_decimal_parser_failure(monkeypatch) -> None:
    def fail_decimal_parse(_encoded_rate):
        raise response_values.InvalidOperation

    monkeypatch.setattr(response_values, "Decimal", fail_decimal_parse)

    with pytest.raises(BillingSearchServingUnavailableError) as failure:
        response_values.validate_public_rate_value(1)

    assert failure.value.__cause__ is None


def test_public_timestamp_rejects_shape_and_calendar_errors() -> None:
    for timestamp in (
        "not-a-timestamp",
        "2026-99-01T00:00:00Z",
    ):
        with pytest.raises(BillingSearchServingUnavailableError) as failure:
            response_values.public_timestamp(timestamp, _budget())
        assert failure.value.__cause__ is None


def test_public_timestamp_requires_timezone_after_parsing(monkeypatch) -> None:
    parsed_without_timezone = SimpleNamespace(utcoffset=lambda: None)
    monkeypatch.setattr(
        response_values,
        "datetime",
        SimpleNamespace(fromisoformat=lambda _value: parsed_without_timezone),
    )

    with pytest.raises(BillingSearchServingUnavailableError):
        response_values.public_timestamp("2026-08-01T00:00:00Z", _budget())


def test_total_response_text_budget_walks_nested_values(monkeypatch) -> None:
    monkeypatch.setattr(response_values, "MAX_PUBLIC_TOTAL_TEXT_BYTES", 3)

    with pytest.raises(BillingSearchServingUnavailableError):
        response_values.validate_total_text_budget({"nested": ["ab", {"value": "cd"}]})


def test_validated_page_rechecks_provider_count_against_limit() -> None:
    result = matched_result()
    object.__setattr__(result.request, "limit", 0)

    with pytest.raises(BillingSearchServingUnavailableError):
        response_values.validated_response_page(result, None)


def test_validated_continuation_requires_bounded_cursor() -> None:
    terminal = matched_result()
    provider = terminal.providers[0]
    continuation = replace(
        terminal,
        request=query(limit=1),
        has_more=True,
        next_sort_key=provider.candidate.sort_key,
    )

    with pytest.raises(BillingSearchServingUnavailableError):
        response_values.validated_response_page(continuation, None)


def test_public_price_rejects_open_or_incomplete_atom_shape() -> None:
    for raw_price in (
        {},
        {"negotiated_rate": "20.50", "internal_coordinate": "hidden"},
    ):
        with pytest.raises(BillingSearchServingUnavailableError):
            response._public_price(
                raw_price,
                _budget(),
                required_modifiers=frozenset(),
                required_place_of_service=frozenset(),
            )


def test_public_price_rejects_string_after_numeric_normalization(
    monkeypatch,
) -> None:
    normalized_price_by_name = {
        field_name: (
            "20.50"
            if field_name == "negotiated_rate"
            else [] if field_name in response._PRICE_ARRAY_FIELDS else None
        )
        for field_name in response._PUBLIC_PRICE_FIELDS
    }
    monkeypatch.setattr(
        response,
        "_canonical_price_row",
        lambda _raw_price: normalized_price_by_name,
    )

    with pytest.raises(BillingSearchServingUnavailableError):
        response._public_price(
            _price(),
            _budget(),
            required_modifiers=frozenset(),
            required_place_of_service=frozenset(),
        )


def test_public_price_rechecks_exact_modifier_filter() -> None:
    with pytest.raises(BillingSearchServingUnavailableError):
        response._public_price(
            _price(),
            _budget(),
            required_modifiers=frozenset({"26"}),
            required_place_of_service=frozenset(),
        )


def test_public_address_requires_mapping_display() -> None:
    provider = _provider()
    object.__setattr__(provider.candidate.address, "display", ["not", "mapping"])

    with pytest.raises(BillingSearchServingUnavailableError):
        response._public_address(provider, _budget())


def test_address_provenance_rechecks_evidence_level() -> None:
    provider = _provider()
    object.__setattr__(
        provider.candidate.address,
        "geo_evidence_level",
        "synthetic_unknown_level",
    )

    with pytest.raises(BillingSearchServingUnavailableError):
        response._validated_address_provenance(provider)


def test_radius_distance_is_required() -> None:
    radius_query = query(
        zip5=None,
        latitude=38.0,
        longitude=-81.0,
        radius_miles=10.0,
    )

    with pytest.raises(BillingSearchServingUnavailableError):
        response._public_distance(radius_query, _provider())


def test_public_distance_rejects_wrong_and_invalid_numeric_values() -> None:
    provider = _provider()
    for distance_value in ("near", -1.0):
        object.__setattr__(
            provider.candidate.address,
            "distance_miles",
            distance_value,
        )
        with pytest.raises(BillingSearchServingUnavailableError):
            response._public_distance(query(), provider)


def test_public_distance_normalizes_signed_zero() -> None:
    provider = _provider()
    object.__setattr__(provider.candidate.address, "distance_miles", -0.0)

    distance_miles = response._public_distance(query(), provider)

    assert distance_miles == 0.0
    assert str(distance_miles) == "0.0"


def test_public_price_atoms_require_nonempty_typed_tuple() -> None:
    invalid_witness = SimpleNamespace(prices=[])

    with pytest.raises(BillingSearchServingUnavailableError):
        response._public_price_atoms(
            invalid_witness,
            _budget(),
            required_modifiers=frozenset(),
            required_place_of_service=frozenset(),
        )


def test_rate_occurrences_reject_wrong_price_witness_type() -> None:
    provider = _provider()
    object.__setattr__(provider, "price_witnesses", (object(),))

    with pytest.raises(BillingSearchServingUnavailableError):
        response._public_rate_occurrences(query(), provider, _budget())


def test_rate_occurrences_recheck_code_witness_scope() -> None:
    with pytest.raises(BillingSearchServingUnavailableError):
        response._public_rate_occurrences(
            query(code="00000"),
            _provider(),
            _budget(),
        )


def test_rate_occurrences_require_at_least_one_witness() -> None:
    provider = _provider()
    object.__setattr__(provider, "price_witnesses", ())

    with pytest.raises(BillingSearchServingUnavailableError):
        response._public_rate_occurrences(query(), provider, _budget())


def test_provider_scope_rechecks_result_binding_pin_membership() -> None:
    result = matched_result()
    object.__setattr__(result, "binding_pins", ())

    with pytest.raises(BillingSearchServingUnavailableError):
        response._validate_provider_scope(result, result.providers[0])


def test_public_provider_requires_exact_matched_provider_type() -> None:
    with pytest.raises(BillingSearchServingUnavailableError):
        response._public_provider(matched_result(), object(), _budget())


def test_public_provider_sanitizes_npi_validation_failure(monkeypatch) -> None:
    result = matched_result()

    def fail_npi(_value):
        raise ValueError("synthetic-npi-detail")

    monkeypatch.setattr(response, "validated_provider_npi", fail_npi)
    with pytest.raises(BillingSearchServingUnavailableError) as failure:
        response._public_provider(result, result.providers[0], _budget())

    assert failure.value.__cause__ is None
    assert "synthetic-npi-detail" not in str(failure.value)


def test_public_provider_rechecks_exact_requested_npi() -> None:
    result = matched_result()
    filtered_result = replace(
        result,
        request=query(provider_npi=1234567893),
    )

    with pytest.raises(BillingSearchServingUnavailableError):
        response._public_provider(
            filtered_result,
            filtered_result.providers[0],
            _budget(),
        )


def test_release_metadata_rejects_open_field_set(monkeypatch) -> None:
    selected_release = selection()
    metadata = selected_release.response_metadata()
    monkeypatch.setattr(
        type(selected_release),
        "response_metadata",
        lambda _selection: {**metadata, "internal_generation": "hidden"},
    )

    with pytest.raises(BillingSearchServingUnavailableError):
        response._public_release_metadata(selected_release, _budget())


def test_release_metadata_requires_current_release(monkeypatch) -> None:
    selected_release = selection()
    metadata = selected_release.response_metadata()
    monkeypatch.setattr(
        type(selected_release),
        "response_metadata",
        lambda _selection: {**metadata, "is_current": "false"},
    )

    with pytest.raises(BillingSearchServingUnavailableError):
        response._public_release_metadata(selected_release, _budget())


def test_matched_reference_collection_rejects_missing_reference() -> None:
    result = matched_result()
    selector_binding = result.selector_scope.bindings[0]
    object.__setattr__(selector_binding, "billing_entity_ref", None)

    with pytest.raises(BillingSearchServingUnavailableError):
        response._matched_billing_entity_refs(result, _budget())
