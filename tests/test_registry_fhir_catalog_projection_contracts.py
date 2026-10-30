# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""COPY projection refuses malformed identity batches before producing rows."""

from dataclasses import replace

import pytest

from process.registry_fhir_catalog_projection import FHIRCatalogProjectionError, format_fhir_catalog_projection
from tests.test_registry_fhir_catalog_projection import source_batch, source_context


@pytest.mark.parametrize("coordinates", [None, object(), {"source_system": "fhir"}])
def test_projection_requires_typed_source_coordinates(coordinates):
    organizations, relationships = source_batch()
    with pytest.raises(FHIRCatalogProjectionError):
        format_fhir_catalog_projection(
            organizations, relationships, (("network", 7),), replace(source_context(), coordinates=coordinates)
        )


@pytest.mark.parametrize("bindings", [[], (("network",),), (("missing", 7),), (("network", 7), ("network", 8))])
def test_projection_refuses_ambiguous_or_foreign_organization_assignments(bindings):
    organizations, relationships = source_batch()
    with pytest.raises(FHIRCatalogProjectionError):
        format_fhir_catalog_projection(organizations, relationships, bindings, source_context())


@pytest.mark.parametrize("networks", [[], (None,), ({"network_id": 7},) * 2])
def test_projection_requires_complete_canonical_network_records(networks):
    organizations, relationships = source_batch()
    canonical_by_field = {"network_id": 7, "display_name": "Reviewed", "aliases": [], "archived": False, "revision": 1}
    if networks == ({"network_id": 7},) * 2:
        networks = (canonical_by_field, canonical_by_field)
    with pytest.raises(FHIRCatalogProjectionError):
        format_fhir_catalog_projection(
            organizations, relationships, (("network", 7),), source_context(), canonical_networks=networks
        )


@pytest.mark.parametrize("aliases", [("Same", "Same"), (None,), [" "], ["a"] * 101])
def test_projection_refuses_invalid_manual_aliases(aliases):
    organizations, relationships = source_batch()
    canonical_by_field = {
        "network_id": 7,
        "display_name": "Reviewed",
        "aliases": aliases,
        "archived": False,
        "revision": 1,
    }
    with pytest.raises(FHIRCatalogProjectionError):
        format_fhir_catalog_projection(
            organizations,
            relationships,
            (("network", 7),),
            source_context(),
            canonical_networks=(canonical_by_field,),
        )


@pytest.mark.parametrize(
    "aliases",
    [
        [],
        (("network", "11111111-1111-4111-8111-111111111111"),),
        (("bad/id", "11111111-1111-4111-8111-111111111111", "scope"),),
        (("network", "00000000-0000-0000-0000-000000000000", "scope"),),
        (("network", "11111111-1111-4111-8111-111111111111", ""),),
    ],
)
def test_raw_projection_requires_complete_source_qualified_legacy_aliases(aliases):
    organizations, relationships = source_batch()
    context = source_context()
    context = replace(
        context,
        coordinates=replace(context.coordinates, producer_id="raw-directory-v1"),
        profile="raw_fhir",
        capture_sha256=None,
        legacy_aliases=aliases,
    )
    with pytest.raises(FHIRCatalogProjectionError):
        format_fhir_catalog_projection(organizations, relationships, (("network", 7),), context)


@pytest.mark.parametrize("damage", ["organization-kind", "relationship-kind", "duplicate-organization", "row-limit"])
def test_projection_refuses_incomplete_or_duplicate_input_batches(damage):
    organizations, relationships = source_batch()
    if damage == "organization-kind":
        organizations = (object(),)
    elif damage == "relationship-kind":
        relationships = (object(),)
    elif damage == "duplicate-organization":
        organizations = (*organizations, organizations[0])
    else:
        organizations = (organizations[0],) * 1001
    with pytest.raises(FHIRCatalogProjectionError):
        format_fhir_catalog_projection(organizations, relationships, (("network", 7),), source_context())


@pytest.mark.parametrize("maximum", [True, 0, -1, "1024"])
def test_projection_rejects_nonpositive_or_noninteger_output_budget(maximum):
    organizations, relationships = source_batch()
    with pytest.raises(FHIRCatalogProjectionError):
        format_fhir_catalog_projection(
            organizations, relationships, (("network", 7),), source_context(), max_output_bytes=maximum
        )
