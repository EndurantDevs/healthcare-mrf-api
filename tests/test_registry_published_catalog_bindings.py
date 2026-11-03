# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Published metadata bindings have a closed scope independent of legacy capture readers."""

from dataclasses import replace

import pytest

from process.network_approved_membership_source import ApprovedMembershipSource
from process.network_approved_source_bindings import RegistryNetworkSourceCoordinates
from process.network_fhir_membership_source import (
    FHIRMembershipSourceError,
    PinnedFHIRMembershipSource,
    _reviewed_descriptor,
)
from process.registry_network_catalog_read import RegistryNetworkCatalogError, catalog_binding_document


def published_document():
    return {
        "namespace": "source_binding",
        "binding_id": "01234567-89ab-4def-8123-456789abcdef",
        "revision": 1,
        "source_system": "fhir",
        "source_id": "example-source",
        "dataset_schema": "example_source",
        "dataset_id": "dataset-one",
        "producer_id": "registry-fhir-catalog-v1",
        "edition_id": "dataset-one",
        "source_key": "organization-one",
        "source_scope": {"organization_id": "organization-one", "source_generation": "b" * 64},
        "evidence_id": "b" * 64 + ":Organization/organization-one",
        "evidence_sha256": "a" * 64,
    }


def test_published_catalog_binding_accepts_exact_metadata_scope():
    assert catalog_binding_document(published_document()) is None


@pytest.mark.parametrize("field", ["producer_id", "edition_id", "source_key", "evidence_id", "evidence_sha256"])
def test_published_catalog_binding_rejects_inconsistent_fields(field):
    with pytest.raises(RegistryNetworkCatalogError):
        catalog_binding_document(published_document() | {field: "other"})


@pytest.mark.parametrize(
    "scope",
    [
        {"organization_id": "organization-one", "source_generation": "A" * 64},
        {"organization_id": "organization-one", "source_generation": "b" * 64, "capture_sha256": "a" * 64},
        {
            "organization_id": "organization-one",
            "legacy_uuid": "01234567-89ab-4def-8123-456789abcdef",
            "alias_scope": "scope",
        },
    ],
)
def test_published_catalog_binding_rejects_mixed_or_legacy_scope(scope):
    with pytest.raises(RegistryNetworkCatalogError):
        catalog_binding_document(published_document() | {"source_scope": scope})


def test_old_reader_refuses_catalog_only_pin_and_preserves_legacy_descriptor():
    source = PinnedFHIRMembershipSource(
        "example_source",
        "example-source",
        "endpoint-one",
        "dataset-one",
        "a" * 64,
        "release-one",
        123,
        "scope-one",
        "2026-01-01",
    )
    approved = ApprovedMembershipSource(1, "c" * 64)
    coordinates = RegistryNetworkSourceCoordinates(
        "fhir", source.source_id, source.schema_name, source.dataset_id, "registry-fhir-catalog-v1", source.dataset_id
    )
    with pytest.raises(FHIRMembershipSourceError, match="typed Source reader"):
        _reviewed_descriptor(source, approved, coordinates, True)
    legacy = replace(coordinates, producer_id="legacy-fhir-v1")
    assert _reviewed_descriptor(source, approved, legacy, True)["alias_scope"] == source.alias_scope
    assert _reviewed_descriptor(source, None, None) is None
