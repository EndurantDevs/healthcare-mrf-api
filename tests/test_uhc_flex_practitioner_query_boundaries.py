# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Boundary coverage for exact Flex Practitioner response validation."""

import json

import pytest

from process import uhc_flex_practitioner_query as query
from process.uhc_flex_official_cohort_contract import UHC_FLEX_OFFICIAL_NPI_SYSTEM


REQUESTED_NPI = 1234567893


def _practitioner(resource_id="practitioner-a"):
    return {
        "resourceType": "Practitioner",
        "id": resource_id,
        "identifier": [
            {"system": UHC_FLEX_OFFICIAL_NPI_SYSTEM, "value": str(REQUESTED_NPI)}
        ],
    }


def _canonical_resource(resource=None):
    return json.dumps(
        resource or _practitioner(),
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )


def _error_code(callable_object, *args):
    with pytest.raises(query.UHCFlexPractitionerQueryError) as error_info:
        callable_object(*args)
    return error_info.value.code


@pytest.mark.parametrize(
    ("category", "reason_code"),
    [("unknown", "reason"), ("success", ""), ("success", None)],
)
def test_retry_decision_rejects_invalid_identity(category, reason_code):
    with pytest.raises(ValueError, match="retry decision is invalid"):
        query.UHCFlexPractitionerRetryDecision(category, reason_code)


def test_query_error_falls_back_without_echoing_unknown_code():
    error = query.UHCFlexPractitionerQueryError("provider-secret")

    assert error.code == "payload_invalid"
    assert "provider-secret" not in str(error)


def test_resource_json_rejects_oversized_or_unserializable_payload(monkeypatch):
    monkeypatch.setattr(query, "UHC_FLEX_PRACTITIONER_MAX_RESOURCE_JSON_BYTES", 1)
    assert _error_code(query._canonical_resource_json, {"value": "large"}) == (
        "payload_invalid"
    )
    assert _error_code(query._canonical_resource_json, {"value": object()}) == (
        "payload_invalid"
    )


@pytest.mark.parametrize(
    ("identifiers", "expected_code"),
    [
        (None, "requested_npi_missing"),
        ([], "requested_npi_missing"),
        ([None], "payload_invalid"),
        ([{"system": UHC_FLEX_OFFICIAL_NPI_SYSTEM, "value": "1588616783"}], "cross_npi"),
        ([{"system": UHC_FLEX_OFFICIAL_NPI_SYSTEM, "value": None}], "resource_npi_invalid"),
        ([{"system": UHC_FLEX_OFFICIAL_NPI_SYSTEM, "value": "123"}], "resource_npi_invalid"),
        ([{"system": UHC_FLEX_OFFICIAL_NPI_SYSTEM, "value": "1234567890"}], "resource_npi_invalid"),
    ],
)
def test_resource_identifier_boundaries(identifiers, expected_code):
    resource = _practitioner()
    resource["identifier"] = identifiers

    assert _error_code(query._validate_resource_npi, resource, REQUESTED_NPI) == (
        expected_code
    )


def test_bundle_entries_cover_missing_and_wrong_container_boundaries():
    assert query._bundle_entries({}) == []
    assert _error_code(query._bundle_entries, {"entry": {}}) == "entry_invalid"


@pytest.mark.parametrize(
    "links",
    [
        {},
        [None],
        [{"relation": None}],
        [{"relation": " next"}],
    ],
)
def test_bundle_links_reject_invalid_shapes(links):
    assert _error_code(query._reject_next_link, {"link": links}) == "payload_invalid"


def test_non_next_bundle_links_are_finite_and_allowed():
    assert query._reject_next_link({"link": [{"relation": "self"}]}) is None


def test_entry_requires_a_resource_mapping():
    assert _error_code(query._entry_practitioner, {}) == "entry_invalid"


@pytest.mark.parametrize(
    "resource_rows",
    [
        [],
        (None,),
        (("practitioner-a", "{"),),
        (("practitioner-a", "[]"),),
    ],
)
def test_stored_result_rejects_container_and_json_drift(resource_rows):
    assert _error_code(
        query._validate_stored_result_resources,
        REQUESTED_NPI,
        resource_rows,
    ) == "result_invalid"


def test_stored_result_rejects_id_canonicalization_and_order_drift():
    canonical_row = ("practitioner-a", _canonical_resource())
    spaced_json = json.dumps(_practitioner(), sort_keys=True)
    second_row = (
        "practitioner-b",
        _canonical_resource(_practitioner("practitioner-b")),
    )
    invalid_rows = (
        (("wrong-id", canonical_row[1]),),
        (("practitioner-a", spaced_json),),
        (second_row, canonical_row),
        (canonical_row, canonical_row),
    )

    for resource_rows in invalid_rows:
        assert _error_code(
            query._validate_stored_result_resources,
            REQUESTED_NPI,
            resource_rows,
        ) == "result_invalid"


def test_search_bundle_requires_a_mapping_payload():
    assert _error_code(
        query.validate_uhc_flex_practitioner_search_bundle,
        REQUESTED_NPI,
        [],
    ) == "payload_invalid"
