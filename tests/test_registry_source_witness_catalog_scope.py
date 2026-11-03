# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Source witness Organization metadata uses the existing closed capture scope."""

import pytest

from process.registry_fhir_catalog_read import fhir_catalog_binding_document
from process.registry_network_catalog_read import catalog_binding_document
from tests.test_registry_fhir_catalog_read import binding_document


def _binding(enriched):
    binding = binding_document()
    binding["producer_id"] = "registry-source-fhir-witness-v1"
    if not enriched:
        for field in ("metadata_sha256", "accepted_at", "status"):
            del binding[field]
    return binding


@pytest.mark.parametrize("enriched", [False, True])
@pytest.mark.parametrize("producer", ["registry-source-fhir-witness-v1", "registry-retained-normalized-fhir-v1"])
def test_organization_capture_scope_accepts_both_existing_producers(enriched, producer):
    binding = _binding(enriched) | {"producer_id": producer}
    validator = fhir_catalog_binding_document if enriched else catalog_binding_document
    assert validator(binding) == (binding if enriched else None)


@pytest.mark.parametrize("enriched", [False, True])
@pytest.mark.parametrize(
    "changes",
    [
        {"producer_id": "unknown-producer"},
        {"producer_id": "registry-fhir-catalog-v1"},
        {"source_scope": {"organization_id": "network-one"}},
        {"source_scope": {"organization_id": "network-one", "capture_sha256": "C" * 64}},
        {"source_scope": {"organization_id": "network-one", "capture_sha256": "c" * 63}},
        {"source_scope": {"organization_id": "network_one", "capture_sha256": "c" * 64}},
        {"source_scope": {"organization_id": None, "capture_sha256": "c" * 64}},
        {"source_scope": {"organization_id": "network-one", "capture_sha256": "c" * 64, "extra": "field"}},
        {"source_scope": {"organization_id": "network-one", "capture_sha256": "c" * 64, "alias_scope": "legacy"}},
        {"source_scope": {"insurance_plan_id": "plan-one", "capture_sha256": "c" * 64, "product_network_id": 7}},
        {"evidence_sha256": "invalid"},
        {"unexpected": "field"},
    ],
)
def test_source_witness_metadata_preserves_closed_scope_refusals(enriched, changes):
    validator = fhir_catalog_binding_document if enriched else catalog_binding_document
    with pytest.raises(ValueError):
        validator(_binding(enriched) | changes, network_id=7)
