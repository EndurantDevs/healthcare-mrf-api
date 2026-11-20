# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Catalog successors validate carried source rows written before and after named publication."""

import pytest

from process import registry_fhir_catalog_validation as validation
from process.registry_fhir_catalog_sources import fhir_catalog_source_digest


def _descriptor(*, previous_version):
    descriptor_by_field = {
        "coordinates": {
            "source_system": "fhir",
            "source_id": "sample-fhir",
            "dataset_schema": "sample_source",
            "dataset_id": "dataset-one",
            "producer_id": "registry-fhir-catalog-v1",
            "edition_id": "edition-one",
        },
        "profile": "raw_fhir",
        "endpoint_id": "endpoint-one",
        "dataset_sha256": "a" * 64,
        "source_uri": "https://fhir.example.test/",
        "accepted_receipt_id": "receipt-one",
        "accepted_receipt_sha256": "b" * 64,
        "complete": True,
        "previous_dataset_id": None,
    }
    if previous_version:
        descriptor_by_field["resource_table_oid"] = 41000
    else:
        descriptor_by_field["tables"] = {
            "header": {"schema": "catalog", "name": "provider_directory_endpoint_dataset"},
            "resource": {"schema": "sample_source", "name": "provider_directory_dataset_resource"},
        }
    return descriptor_by_field


def _source_row(descriptor_by_field):
    return {
        "source_id": "sample-fhir",
        "profile": "raw_fhir",
        "source_generation": fhir_catalog_source_digest(descriptor_by_field),
        "accepted_at": "2026-01-01T00:00:00+00:00",
        "created_at": "2026-01-01T00:00:00+00:00",
        "descriptor_json": descriptor_by_field,
    }


@pytest.mark.parametrize("previous_version", [False, True])
def test_carried_source_row_validates_against_its_original_generation(previous_version):
    validation._source_document(_source_row(_descriptor(previous_version=previous_version)))


@pytest.mark.parametrize("damage", ["schema", "both_shapes", "neither_shape"])
def test_carried_previous_version_source_row_still_refuses_changed_evidence(damage):
    row_by_field = _source_row(_descriptor(previous_version=True))
    descriptor_by_field = row_by_field["descriptor_json"]
    if damage == "schema":
        descriptor_by_field["coordinates"]["dataset_schema"] = "not a schema"
        row_by_field["source_generation"] = fhir_catalog_source_digest(descriptor_by_field)
    elif damage == "both_shapes":
        descriptor_by_field["tables"] = _descriptor(previous_version=False)["tables"]
        row_by_field["source_generation"] = fhir_catalog_source_digest(descriptor_by_field)
    else:
        del descriptor_by_field["resource_table_oid"]
        row_by_field["source_generation"] = fhir_catalog_source_digest(descriptor_by_field)
    with pytest.raises(ValueError, match="registry_fhir_catalog_source_invalid|registry_source_coordinates_invalid"):
        validation._source_document(row_by_field)
