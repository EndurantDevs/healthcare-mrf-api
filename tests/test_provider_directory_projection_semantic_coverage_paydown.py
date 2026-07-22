# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Fail-closed semantic and canonical-proof coverage for native projection."""

from __future__ import annotations

from decimal import Decimal
import hashlib

import pytest

from process.provider_directory_projection_fhir_values import (
    fhir_list,
    normalized_address_map,
    normalized_coding_map,
    normalized_concept_map,
    normalized_human_name_map,
    normalized_period_map,
    normalized_position_map,
    normalized_text,
    optional_field_text,
    profile_concept_maps,
    profile_contact_maps,
    profile_name_maps,
)
from process.provider_directory_projection_transform import (
    _extension_network_reference_maps,
    _normalized_extension_url,
    _normalized_reference_map,
    _profile_address_maps,
    _reference_entries,
    _reference_target_id,
    _relationship_reference_maps,
    projection_resource_row,
)
from process.provider_directory_projection_types import ProviderDirectoryProjectionError
from tests.provider_directory_projection_materializer_context import (
    synthetic_projection_context,
)


def _digest(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


@pytest.fixture(scope="module")
def projection_claim():
    return synthetic_projection_context("ndjson").claim


def _projection_row(payload, projection_claim, *, input_ordinal: int = 0):
    return projection_resource_row(
        payload,
        claim=projection_claim,
        input_ordinal=input_ordinal,
        payload_hash=_digest(repr(payload).encode("utf-8")),
    )


def test_supported_fhir_value_shapes_remain_lossless_and_bounded() -> None:
    """Preserve supported text, period, coding, and concept shapes."""

    assert normalized_text("  useful  ") == "useful"
    assert normalized_text(7) is None
    assert optional_field_text({}, "missing") is None
    assert fhir_list({}, "missing") == []
    assert normalized_period_map(None) is None
    assert normalized_period_map(
        {"start": " 2026-01-01 ", "end": "2026-12-31"}
    ) == {"start": "2026-01-01", "end": "2026-12-31"}

    coding_map = normalized_coding_map(
        {
            "system": "urn:taxonomy",
            "version": "1",
            "code": "207Q00000X",
            "display": "Family Medicine",
            "userSelected": False,
        }
    )
    assert coding_map["userSelected"] is False
    assert normalized_concept_map({"text": "Primary care"}) == {
        "text": "Primary care"
    }
    assert profile_concept_maps(None) == []
    assert profile_concept_maps({"coding": [coding_map]})[0]["coding"] == [
        coding_map
    ]


def test_supported_fhir_profile_shapes_remain_lossless_and_bounded() -> None:
    """Preserve supported name, contact, address, and position shapes."""

    name_map = normalized_human_name_map(
        {
            "use": "official",
            "text": "Dr Alex Example",
            "family": "Example",
            "given": ["Alex"],
            "prefix": ["Dr"],
            "suffix": ["MD"],
            "period": {"start": "2020-01-01"},
        }
    )
    assert name_map["given"] == ["Alex"]
    assert profile_name_maps({"name": name_map}) == [name_map]
    assert profile_name_maps({"alias": ["Alternate name"]}) == [
        {"text": "Alternate name"}
    ]

    contacts = profile_contact_maps(
        {
            "contact": [
                {
                    "system": "email",
                    "use": "work",
                    "value": "directory@example.test",
                    "rank": 1,
                    "period": {"end": "2027-01-01"},
                }
            ]
        },
        "Endpoint",
    )
    assert contacts[0]["rank"] == 1
    assert contacts[0]["period"] == {"end": "2027-01-01"}


def test_supported_fhir_address_shapes_remain_lossless_and_bounded() -> None:
    """Preserve supported address and exact geographic position shapes."""

    address = normalized_address_map(
        {
            "use": "work",
            "type": "physical",
            "text": "1 Main St, Des Moines, IA",
            "line": ["1 Main St"],
            "city": "Des Moines",
            "district": "Polk",
            "state": "IA",
            "postalCode": "50309",
            "country": "US",
            "period": {"start": "2021-01-01"},
        }
    )
    assert address["line"] == ["1 Main St"]
    assert normalized_address_map({"city": "Ames"}) == {"city": "Ames"}
    assert normalized_address_map({"line": [], "city": "Ames"}) == {
        "city": "Ames"
    }
    assert normalized_position_map(None) is None
    assert normalized_position_map(
        {"latitude": Decimal("41.5868"), "longitude": Decimal("-93.6250")}
    ) == {
        "latitude_microdegrees": 41_586_800,
        "longitude_microdegrees": -93_625_000,
    }


@pytest.mark.parametrize(
    ("operation", "error_code"),
    (
        (lambda: optional_field_text({"value": None}, "value"), "value_invalid"),
        (lambda: fhir_list({"entry": {}}, "entry"), "entry_invalid"),
        (lambda: normalized_period_map("2026"), "period_invalid"),
        (lambda: normalized_period_map({}), "period_invalid"),
        (lambda: normalized_coding_map("code"), "coding_invalid"),
        (
            lambda: normalized_coding_map({"code": "x", "userSelected": 1}),
            "coding_user_selected_invalid",
        ),
        (lambda: normalized_coding_map({"system": "urn:test"}), "coding_invalid"),
        (lambda: normalized_concept_map("concept"), "codeable_concept_invalid"),
        (lambda: normalized_concept_map({}), "codeable_concept_invalid"),
        (
            lambda: profile_concept_maps([{"text": "ok"}, "bad"]),
            "codeable_concept_invalid",
        ),
        (lambda: normalized_human_name_map("name"), "name_invalid"),
        (
            lambda: normalized_human_name_map({"given": [None]}),
            "name_given_invalid",
        ),
        (lambda: normalized_human_name_map({"use": "usual"}), "name_invalid"),
        (lambda: profile_name_maps({"name": "   "}), "name_invalid"),
        (lambda: profile_name_maps({"name": 17}), "name_invalid"),
        (lambda: profile_name_maps({"alias": [None]}), "alias_invalid"),
        (lambda: profile_contact_maps({"telecom": ["bad"]}, "Organization"), "telecom_invalid"),
        (lambda: profile_contact_maps({"telecom": [{}]}, "Organization"), "telecom_value_invalid"),
        (
            lambda: profile_contact_maps(
                {"telecom": [{"value": "5550100", "rank": False}]},
                "Organization",
            ),
            "telecom_rank_invalid",
        ),
        (lambda: normalized_address_map("address"), "address_invalid"),
        (lambda: normalized_address_map({"line": [None]}), "address_line_invalid"),
        (lambda: normalized_address_map({"use": "work"}), "address_invalid"),
        (lambda: normalized_position_map("41,-93"), "position_invalid"),
    ),
)
def test_malformed_fhir_value_shapes_have_stable_fail_closed_codes(
    operation,
    error_code,
) -> None:
    with pytest.raises(
        ProviderDirectoryProjectionError,
        match=rf"provider_directory_projection_fhir_{error_code}$",
    ):
        operation()


@pytest.mark.parametrize(
    "position",
    (
        {"latitude": True, "longitude": 0},
        {"latitude": 0, "longitude": False},
        {"longitude": 0},
        {"latitude": 0},
        {"latitude": float("inf"), "longitude": 0},
        {"latitude": 0, "longitude": float("nan")},
        {"latitude": -91, "longitude": 0},
        {"latitude": 91, "longitude": 0},
        {"latitude": 0, "longitude": -181},
        {"latitude": 0, "longitude": 181},
    ),
)
def test_position_rejects_non_numeric_nonfinite_or_out_of_range_values(
    position,
) -> None:
    with pytest.raises(
        ProviderDirectoryProjectionError,
        match="provider_directory_projection_fhir_position_invalid$",
    ):
        normalized_position_map(position)


def test_transform_accepts_rich_plan_net_shapes(projection_claim) -> None:
    location_row = _projection_row(
        {
            "resourceType": "Location",
            "id": "location-rich",
            "status": "active",
            "address": {"city": "Des Moines", "state": "IA"},
            "position": {"latitude": 41.5868, "longitude": -93.625},
            "period": {"start": "2025-01-01", "end": "2027-01-01"},
            "meta": {"lastUpdated": "2026-07-22T00:00:00Z"},
        },
        projection_claim,
    )
    assert location_row["summary_addressed_location"] is True
    assert location_row["summary_geocoded_location"] is True
    assert location_row["effective_start"] == "2025-01-01"

    reference_map = _normalized_reference_map(
        {
            "type": "Organization",
            "id": "network-1",
            "identifier": {"system": "urn:network", "value": "network-1"},
        }
    )
    assert reference_map["identifier"]["value"] == "network-1"
    assert _reference_target_id("network-1", "Organization") == "network-1"
    assert _reference_target_id(None, "Organization") is None
    assert _normalized_extension_url(None) is None

    plan_row = _projection_row(
        {
            "resourceType": "InsurancePlan",
            "id": "plan-rich",
            "status": "draft",
            "plan": [{"network": {"reference": "Organization/network-plan"}}],
            "coverage": [
                {"network": [{"reference": "Organization/network-coverage"}]}
            ],
            "extension": {
                "url": (
                    "https://hl7.org/fhir/us/davinci-pdex-plan-net/"
                    "StructureDefinition/network-reference|1.2.0/"
                ),
                "valueReference": {"reference": "Organization/network-extension"},
            },
        },
        projection_claim,
    )
    assert plan_row["active"] is False
    assert plan_row["summary_network_link_count"] == 3


@pytest.mark.parametrize(
    ("payload", "error_code"),
    (
        (
            {"resourceType": "Location", "id": "loc", "address": "bad"},
            "address_invalid",
        ),
        (
            {"resourceType": "Organization", "id": "org", "partOf": ["bad"]},
            "reference_invalid",
        ),
        (
            {"resourceType": "Organization", "id": "org", "partOf": "bad"},
            "reference_invalid",
        ),
        (
            {
                "resourceType": "Organization",
                "id": "org",
                "partOf": {"identifier": "bad"},
            },
            "reference_identifier_invalid",
        ),
        (
            {
                "resourceType": "Organization",
                "id": "org",
                "partOf": {"identifier": {"system": "urn:test"}},
            },
            "reference_identifier_invalid",
        ),
        (
            {"resourceType": "Organization", "id": "org", "partOf": {}},
            "reference_invalid",
        ),
        (
            {"resourceType": "InsurancePlan", "id": "plan", "extension": [7]},
            "extension_invalid",
        ),
        (
            {
                "resourceType": "InsurancePlan",
                "id": "plan",
                "extension": {
                    "url": (
                        "http://hl7.org/fhir/us/davinci-pdex-plan-net/"
                        "StructureDefinition/network-reference"
                    )
                },
            },
            "network_extension_invalid",
        ),
        (
            {"resourceType": "InsurancePlan", "id": "plan", "plan": [7]},
            "plan_invalid",
        ),
        (
            {
                "resourceType": "InsurancePlan",
                "id": "plan",
                "network": {"reference": "Location/wrong-family"},
            },
            "relationship_reference_invalid",
        ),
        (
            {"resourceType": "Practitioner", "id": "p", "qualification": [7]},
            "qualification_invalid",
        ),
        (
            {
                "resourceType": "Practitioner",
                "id": "p",
                "identifier": [{"type": "bad", "value": "1234567893"}],
            },
            "identifier_type_invalid",
        ),
        (
            {
                "resourceType": "Practitioner",
                "id": "p",
                "identifier": [
                    {
                        "type": {"coding": [7]},
                        "value": "1234567893",
                    }
                ],
            },
            "identifier_type_invalid",
        ),
        (
            {"resourceType": "Practitioner", "id": "p", "identifier": [7]},
            "identifier_invalid",
        ),
        (
            {"resourceType": "Organization", "id": "org", "active": "true"},
            "active_invalid",
        ),
        (
            {"resourceType": "Endpoint", "id": "ep", "status": "unknown"},
            "status_invalid",
        ),
        (
            {"resourceType": "Organization", "id": "org", "meta": []},
            "meta_invalid",
        ),
    ),
)
def test_malformed_plan_net_shapes_have_stable_transform_codes(
    payload,
    error_code,
    projection_claim,
) -> None:
    with pytest.raises(
        ProviderDirectoryProjectionError,
        match=rf"provider_directory_projection_fhir_{error_code}$",
    ):
        _projection_row(payload, projection_claim)


def test_transform_helper_boundaries_and_optional_statuses(projection_claim) -> None:
    assert _profile_address_maps({}, "Endpoint") == []
    assert _reference_entries(None) == ()
    assert tuple(_reference_entries({"reference": "Organization/o"})) == (
        {"reference": "Organization/o"},
    )
    assert _reference_target_id("not/a/known/type", "Organization") is None
    assert _extension_network_reference_maps({}) == []
    assert _relationship_reference_maps({}, "InsurancePlan", "network") == []

    endpoint_row = _projection_row(
        {"resourceType": "Endpoint", "id": "endpoint-no-status"},
        projection_claim,
    )
    organization_row = _projection_row(
        {
            "resourceType": "Organization",
            "id": "organization-npi-candidates",
            "identifier": [
                {},
                {"system": "urn:other", "value": "1234567893"},
                {
                    "system": "urn:npi",
                    "type": {
                        "text": "Other identifier",
                        "coding": [
                            {
                                "system": "urn:identifier-kind",
                                "code": "OTHER",
                                "display": "Other",
                            }
                        ],
                    },
                    "value": "not-ten-digits",
                },
            ],
        },
        projection_claim,
    )
    assert endpoint_row["active"] is None
    assert organization_row["active"] is None
    assert organization_row["summary_npi"] is None


@pytest.mark.parametrize(
    ("payload", "input_ordinal"),
    (
        ({"resourceType": "Unknown", "id": "x"}, 0),
        ({"resourceType": "Organization", "id": "bad/id"}, 0),
        ({"resourceType": "Organization", "id": "ok"}, True),
        ({"resourceType": "Organization", "id": "ok"}, -1),
    ),
)
def test_native_record_coordinates_fail_closed(
    payload,
    input_ordinal,
    projection_claim,
) -> None:
    with pytest.raises(
        ProviderDirectoryProjectionError,
        match="provider_directory_projection_native_record_invalid$",
    ):
        _projection_row(payload, projection_claim, input_ordinal=input_ordinal)
