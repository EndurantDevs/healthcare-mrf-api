# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Scoped network output never implies an exhaustive provider directory."""

from copy import deepcopy

import pytest

from process import provider_directory_network_snapshot_scope as scope


def example():
    return scope.make_scope(
        root_name="Example",
        owner_reference="Organization/owner",
        discovery_counts={
            "owned_network_roots": 1,
            "affiliations": 3,
            "discovered_networks": 2,
            "unresolved_networks": 1,
            "membership_queries": 2,
            "membership_queries_complete": 2,
            "participating_organizations": 4,
            "unresolved_organization_references": 1,
        },
    )


def test_scoped_snapshot_preserves_unknowns_and_other_sources():
    value = example()
    diagnostics_by_resource = {kind: {scope.SCOPE_KEY: deepcopy(value)} for kind in scope.RESOURCE_TYPES}
    output = scope.publication_scope_metadata(diagnostics_by_resource, required=True)
    assert output["endpoint_complete"] is False
    assert output[scope.SCOPE_KEY]["commercial_scope"] == "unknown"
    assert output[scope.SCOPE_KEY]["discovery_counts"]["unresolved_networks"] == 1
    assert scope.publication_scope_metadata({}) == {}
    with pytest.raises(RuntimeError, match="scope_invalid"):
        scope.publication_scope_metadata({}, required=True)
    diagnostics_by_resource["OrganizationAffiliation"][scope.SCOPE_KEY]["endpoint_complete"] = True
    with pytest.raises(RuntimeError, match="scope_invalid"):
        scope.publication_scope_metadata(diagnostics_by_resource)


@pytest.mark.parametrize(
    "counts",
    [
        {"owned_network_roots": 0},
        {"membership_queries_complete": 1},
        {"affiliations": True},
        {"unresolved_organization_references": -1},
    ],
)
def test_rejects_false_accounting(counts):
    value = example()
    value["discovery_counts"].update(counts)
    with pytest.raises(RuntimeError, match="scope_invalid"):
        scope.validate_scope(value)
