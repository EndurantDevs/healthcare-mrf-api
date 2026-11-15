# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Named acquisition preserves incomplete Location fixtures without inventing an office."""

import pytest

from tests import generic_source_successor_postgres_support as source

_ADDRESS_FIELDS = ("first_line", "second_line", "city_name", "state_name", "postal_code", "country_code")


@pytest.mark.parametrize(
    "payload,expected",
    [
        ({}, {}),
        (dict.fromkeys(_ADDRESS_FIELDS), {}),
        (dict.fromkeys(_ADDRESS_FIELDS, ""), {}),
        ({"first_line": "123 Example Street"}, {"first_line": "123 Example Street"}),
        (
            {"city_name": "Sample City", "state_name": "New York"},
            {"city_name": "Sample City", "state_name": "New York"},
        ),
        (
            {
                "first_line": "123 Example Street",
                "second_line": "Suite 2",
                "city_name": "Sample City",
                "state_code": "NY",
                "postal_code": "10001",
                "country_code": "US",
            },
            {
                "first_line": "123 Example Street",
                "second_line": "Suite 2",
                "city_name": "Sample City",
                "state_name": "NY",
                "postal_code": "10001",
                "country_code": "US",
            },
        ),
    ],
    ids=("missing", "null", "empty", "street-only", "city-state-only", "complete"),
)
def test_location_fixture_roundtrip_keeps_supplied_address_fields(payload, expected):
    resource = source._r4_resource_from_normalized(
        {"resource_type": "Location", "resource_id": "example-office", "payload_json": payload}
    )
    row = source.directory.materialize_provider_directory_dataset_fhir_resource(
        source_id=source.SOURCE,
        dataset_id="dataset",
        resource={"resourceType": "Location", **resource},
        run_id="run",
        semantic_projection_as_of="2026-10-01",
        resource_hash_contract=source.directory.SEMANTIC_CONTENT_V4_RESOURCE_HASH_CONTRACT,
    )["payload_json"]
    assert {field: row[field] for field in _ADDRESS_FIELDS} == {field: expected.get(field) for field in _ADDRESS_FIELDS}
    assert row["resource_id"] == "example-office" and row["status"] == "active"
