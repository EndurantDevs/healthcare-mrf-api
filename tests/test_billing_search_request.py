# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Strict internal billing-search request tests."""

from __future__ import annotations

import copy
import pickle

import pytest

from api import billing_search_request as request_contract

BILLING_ENTITY_REF = (
    "be1_AAECAwQFBgcICQoLDA0ODxIr3ljg-uNk13KslT9vSXm4lGO1maZsqjUk0Jf9HUBm"
)
PLAN_RELEASE_ID = "hprelease_01K123456789ABCDEFGHJKMNPQ"
VALID_NPI = "1234567893"

_RAW_IDENTITY_ALIASES = (
    "ein",
    "tax_id",
    "tax_identity",
    "tax_identity_type",
    "tax_identity_value",
    "tin",
)


class _MultiParameters(dict):
    def __init__(self, pairs):
        super().__init__(pairs)
        self._pairs = list(pairs)

    def items(self, multi=False):
        return list(self._pairs) if multi else super().items()

    def getlist(self, name):
        return [value for key, value in self._pairs if key == name]


def _zip_pairs(**overrides):
    values_by_name = {
        "billing_entity_ref": BILLING_ENTITY_REF,
        "code": "99213",
        "code_system": "CPT",
        "limit": "25",
        "plan_release_id": PLAN_RELEASE_ID,
        "zip5": "00000",
    }
    values_by_name.update(overrides)
    return tuple(values_by_name.items())


def _coordinate_pairs(**overrides):
    values_by_name = dict(_zip_pairs())
    del values_by_name["zip5"]
    values_by_name.update(
        {
            "lat": "0",
            "long": "0",
            "radius_miles": "25",
        }
    )
    values_by_name.update(overrides)
    return tuple(values_by_name.items())


def _parse(pairs=None):
    return request_contract.parse_billing_search_request(
        _MultiParameters(pairs or _zip_pairs())
    )


def test_exact_zip_request_is_redacted_immutable_and_fingerprinted() -> None:
    parsed = _parse()

    assert parsed.plan_release_id == PLAN_RELEASE_ID
    assert parsed.code_system == "CPT"
    assert parsed.code == "99213"
    assert parsed.geo_args == {"zip5": "00000"}
    assert parsed.limit == 25
    assert parsed.cursor is None
    assert parsed.provider_npi is None
    assert parsed.price_filter_args == {"modifiers": (), "place_of_service": ()}
    assert parsed.query_pairs == (
        ("billing_entity_ref", BILLING_ENTITY_REF),
        ("code", "99213"),
        ("code_system", "CPT"),
        ("limit", "25"),
        ("plan_release_id", PLAN_RELEASE_ID),
        ("zip5", "00000"),
    )
    assert len(parsed.request_fingerprint_sha256) == 64
    assert repr(parsed) == "<redacted-billing-search-request>"
    assert BILLING_ENTITY_REF not in repr(parsed)
    with pytest.raises(TypeError):
        parsed._BillingSearchRequest__limit = 50
    with pytest.raises(TypeError):
        del parsed._BillingSearchRequest__limit
    with pytest.raises(request_contract.BillingSearchRequestError):
        pickle.dumps(parsed)
    assert copy.copy(parsed) is parsed
    assert copy.deepcopy(parsed) is parsed
    assert request_contract.validate_billing_search_request(parsed) is parsed


def test_request_rejects_direct_construction_and_all_field_tampering() -> None:
    with pytest.raises(request_contract.BillingSearchRequestError):
        request_contract.BillingSearchRequest()

    tampering_by_field = {
        "billing_entity_ref": "be1_" + "A" * 64,
        "code": "99214",
        "code_system": "HCPCS",
        "cursor": "bsc1_k1_" + "B" * 64,
        "include_evidence": True,
        "latitude": 1.0,
        "limit": 100,
        "longitude": 1.0,
        "modifiers": ("59",),
        "place_of_service": ("22",),
        "plan_release_id": "hprelease_01K123456789ABCDEFGHJKMNP0",
        "provider_npi": int(VALID_NPI),
        "query_pairs": object(),
        "radius_miles": 1.0,
        "request_fingerprint_sha256": "1" * 64,
        "state_sha256": "1" * 64,
        "zip5": "99999",
    }
    for field_name, field_value in tampering_by_field.items():
        parsed = _parse()
        object.__setattr__(
            parsed,
            f"_BillingSearchRequest__{field_name}",
            field_value,
        )
        with pytest.raises(request_contract.BillingSearchRequestError):
            request_contract.validate_billing_search_request(parsed)


def test_request_rejects_query_pairs_that_normalize_to_no_request() -> None:
    parsed = _parse()
    object.__setattr__(parsed, "_BillingSearchRequest__query_pairs", ())

    with pytest.raises(request_contract.BillingSearchRequestError):
        request_contract.validate_billing_search_request(parsed)


@pytest.mark.parametrize(
    ("field_name", "type_confusable_value"),
    [("include_evidence", 0), ("limit", 25.0)],
)
def test_request_rejects_type_confusable_field_tampering(
    field_name,
    type_confusable_value,
) -> None:
    parsed = _parse()
    object.__setattr__(
        parsed,
        f"_BillingSearchRequest__{field_name}",
        type_confusable_value,
    )

    with pytest.raises(request_contract.BillingSearchRequestError):
        request_contract.validate_billing_search_request(parsed)


def test_request_revalidation_rejects_wrong_runtime_type() -> None:
    with pytest.raises(request_contract.BillingSearchRequestError):
        request_contract.validate_billing_search_request(object())


def test_coordinate_request_normalizes_typed_filters() -> None:
    parameter_values_by_name = dict(_coordinate_pairs())
    parameter_values_by_name.update(
        {
            "npi": VALID_NPI,
            "modifiers": "25,59",
            "place_of_service": "11,22",
            "include_evidence": "true",
        }
    )

    parsed = _parse(tuple(parameter_values_by_name.items()))

    assert parsed.geo_args == {
        "lat": 0.0,
        "long": 0.0,
        "radius_miles": 25.0,
    }
    assert parsed.provider_npi == int(VALID_NPI)
    assert parsed.modifiers == ("25", "59")
    assert parsed.place_of_service == ("11", "22")
    assert parsed.include_evidence is True


def test_decimal_scalar_accepts_exact_transport_value_limit() -> None:
    exact_limit_decimal = "0." + "0" * 2046
    assert len(exact_limit_decimal) == 2048

    parsed = _parse(_coordinate_pairs(lat=exact_limit_decimal))

    assert parsed.latitude == 0.0
    assert dict(parsed.query_pairs)["lat"] == exact_limit_decimal


def test_decimal_scalar_rejects_above_transport_value_limit() -> None:
    oversized_decimal = "0." + "0" * 2047
    assert len(oversized_decimal) == 2049

    with pytest.raises(
        request_contract.BillingSearchRequestError,
        match="^billing_search_request_invalid$",
    ):
        _parse(_coordinate_pairs(lat=oversized_decimal))


def test_cursor_is_excluded_only_from_logical_request_fingerprint() -> None:
    first_page = _parse()
    next_page = _parse(_zip_pairs(cursor="bsc1_k1_" + "A" * 64))

    assert first_page.request_fingerprint_sha256 == next_page.request_fingerprint_sha256
    assert first_page.query_pairs != next_page.query_pairs


@pytest.mark.parametrize(
    "pairs",
    [
        _zip_pairs(extra="value"),
        tuple((key, value) for key, value in _zip_pairs() if key != "limit"),
        _zip_pairs(healthporta_plan_id="hpplan_01K123456789ABCDEFGHJKMNPQ"),
        _zip_pairs(plan_release_id=f" {PLAN_RELEASE_ID}"),
        _zip_pairs(plan_release_id="not-a-release"),
        _zip_pairs(billing_entity_ref="be1_invalid"),
        _zip_pairs(code_system="cpt"),
        _zip_pairs(code=" 99213"),
        _zip_pairs(code="ABCDE"),
        _zip_pairs(zip5="0000"),
        _zip_pairs(zip5="00000", lat="0", long="0", radius_miles="25"),
        _coordinate_pairs(lat="1e0"),
        _coordinate_pairs(lat="91"),
        tuple(
            (key, value)
            for key, value in _coordinate_pairs()
            if key != "radius_miles"
        ),
        tuple(
            {
                **dict(_zip_pairs()),
                "zip5": None,
                "lat": "0",
                "long": "0",
            }.items()
        ),
        _zip_pairs(radius_miles="25"),
        _zip_pairs(limit="0"),
        _zip_pairs(limit="101"),
        _zip_pairs(limit="025"),
        _zip_pairs(include_evidence="1"),
        _zip_pairs(npi="1234567890"),
        _zip_pairs(modifiers="59,25"),
        _zip_pairs(modifiers="25,25"),
        _zip_pairs(place_of_service="1"),
        _zip_pairs(cursor="opaque"),
    ],
)
def test_request_rejects_noncanonical_or_ambiguous_parameters(pairs) -> None:
    with pytest.raises(
        request_contract.BillingSearchRequestError,
        match="^billing_search_request_invalid$",
    ):
        _parse(pairs)


def test_request_rejects_duplicate_keys_before_mapping_collapse() -> None:
    pairs = (*_zip_pairs(), ("code", "99214"))

    with pytest.raises(request_contract.BillingSearchRequestError):
        _parse(pairs)


@pytest.mark.parametrize("raw_identity_alias", _RAW_IDENTITY_ALIASES)
def test_request_rejects_raw_identity_aliases(raw_identity_alias) -> None:
    pairs = (*_zip_pairs(), (raw_identity_alias, BILLING_ENTITY_REF))

    with pytest.raises(
        request_contract.BillingSearchRequestError,
        match="^billing_search_request_invalid$",
    ) as failure:
        _parse(pairs)

    assert BILLING_ENTITY_REF not in repr(failure.value)


@pytest.mark.parametrize(
    "invalid_scalar",
    [
        None,
        0,
        b"bytes",
        "",
        " leading",
        "trailing ",
        "line\nbreak",
        "snowman-\N{SNOWMAN}",
        {"nested": "value"},
        ["value"],
    ],
)
def test_request_rejects_noncanonical_scalar_values(invalid_scalar) -> None:
    with pytest.raises(
        request_contract.BillingSearchRequestError,
        match="^billing_search_request_invalid$",
    ):
        _parse(_zip_pairs(code=invalid_scalar))


class _SecretRaisingParameters(dict):
    def items(self, multi=False):
        del multi
        raise RuntimeError(BILLING_ENTITY_REF)


class _SecretRaisingAccessorParameters(_MultiParameters):
    def getall(self, name):
        del name
        raise RuntimeError(BILLING_ENTITY_REF)


class _DuplicateAccessorParameters(_MultiParameters):
    def getall(self, name):
        values = self.getlist(name)
        return values * 2 if name == "code" else values


class _FallbackAccessorParameters(_MultiParameters):
    def getall(self, name):
        raise KeyError(name)


class _FallbackItemsErrorParameters(dict):
    def items(self, **options):
        if options:
            raise TypeError
        raise RuntimeError(BILLING_ENTITY_REF)


class _SecretRaisingGetParameters(dict):
    def get(self, name, default=None):
        del name, default
        raise RuntimeError(BILLING_ENTITY_REF)


class _MissingItemsParameters(dict):
    items = None


def test_request_sanitizes_parameter_accessor_exceptions() -> None:
    with pytest.raises(request_contract.BillingSearchRequestError) as failure:
        request_contract.parse_billing_search_request(_SecretRaisingParameters())

    assert failure.value.__cause__ is None
    assert failure.value.__context__ is None
    assert BILLING_ENTITY_REF not in repr(failure.value)


def test_request_sanitizes_value_accessor_exceptions() -> None:
    parameters = _SecretRaisingAccessorParameters(_zip_pairs())

    with pytest.raises(request_contract.BillingSearchRequestError) as failure:
        request_contract.parse_billing_search_request(parameters)

    assert failure.value.__cause__ is None
    assert failure.value.__context__ is None
    assert BILLING_ENTITY_REF not in repr(failure.value)


def test_request_rejects_duplicate_accessor_values() -> None:
    parameters = _DuplicateAccessorParameters(_zip_pairs())

    with pytest.raises(
        request_contract.BillingSearchRequestError,
        match="^billing_search_request_invalid$",
    ):
        request_contract.parse_billing_search_request(parameters)


def test_request_falls_back_from_missing_getall_value() -> None:
    parameters = _FallbackAccessorParameters(_zip_pairs())

    parsed = request_contract.parse_billing_search_request(parameters)

    assert parsed.geo_args == {"zip5": "00000"}


@pytest.mark.parametrize(
    "parameters",
    [
        _FallbackItemsErrorParameters(_zip_pairs()),
        _SecretRaisingGetParameters(_zip_pairs()),
        _MissingItemsParameters(_zip_pairs()),
    ],
)
def test_request_sanitizes_mapping_accessor_failures(parameters) -> None:
    with pytest.raises(request_contract.BillingSearchRequestError) as failure:
        request_contract.parse_billing_search_request(parameters)

    assert failure.value.__cause__ is None
    assert failure.value.__context__ is None
    assert BILLING_ENTITY_REF not in repr(failure.value)


def test_request_rejects_collection_scalar_from_plain_mapping() -> None:
    parameters_by_name = dict(_zip_pairs(code=["99213"]))

    with pytest.raises(request_contract.BillingSearchRequestError):
        request_contract.parse_billing_search_request(parameters_by_name)


def test_request_rejects_non_mapping_parameters() -> None:
    with pytest.raises(request_contract.BillingSearchRequestError):
        request_contract.parse_billing_search_request([])
