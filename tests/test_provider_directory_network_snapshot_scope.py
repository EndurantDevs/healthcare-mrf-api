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


def test_extended_name_queries_preserve_legacy_scope_and_disclose_unverified_searches():
    legacy = example()
    assert scope.validate_scope(legacy) == legacy
    selection = scope.make_scope(
        root_name="Example",
        additional_names=["A", "B"],
        owner_reference=legacy["owner_reference"],
        discovery_counts=legacy["discovery_counts"],
    )
    assert scope.validate_scope(selection) == selection
    assert selection["root_selection"]["additional_name_queries"] == ["A", "B"]
    assert set(selection["root_selection"]["unverified_name_queries"]) == set("CDEFGHIJKLMNOPQRSTUVWXYZ0123456789")
    selection["root_selection"]["unverified_name_queries"] = []
    with pytest.raises(RuntimeError, match="scope_invalid"):
        scope.validate_scope(selection)


def test_membership_reconciliation_is_bound_to_both_diagnostics_and_header():
    value = example()
    key = scope.MEMBERSHIP_RECONCILIATION_KEY
    reconciliation_by_field = {"search_edges": 4, "canonical_only_edges": 2}
    diagnostics_by_resource = {
        kind: {scope.SCOPE_KEY: deepcopy(value), key: dict(reconciliation_by_field)} for kind in scope.RESOURCE_TYPES
    }
    result = scope.publication_scope_metadata(diagnostics_by_resource, required=True)
    assert result[key] == reconciliation_by_field
    header_by_field = {**result, "resource_diagnostics": diagnostics_by_resource}
    assert scope.published_header_scope(header_by_field) == result
    with pytest.raises(RuntimeError, match="scope_invalid"):
        scope.published_header_scope({**header_by_field, key: {"search_edges": 4, "canonical_only_edges": 1}})
    diagnostics_by_resource["Organization"][key]["canonical_only_edges"] = False
    with pytest.raises(RuntimeError, match="scope_invalid"):
        scope.publication_scope_metadata(diagnostics_by_resource)


@pytest.mark.parametrize("invalid_value", [None, False, 0.0])
def test_published_header_rejects_invalid_membership_count_encodings(invalid_value):
    key = scope.MEMBERSHIP_RECONCILIATION_KEY
    counts_by_field = {"search_edges": 4, "canonical_only_edges": 0}
    diagnostics_by_resource = {
        kind: {scope.SCOPE_KEY: example(), key: dict(counts_by_field)} for kind in scope.RESOURCE_TYPES
    }
    expected = scope.publication_scope_metadata(diagnostics_by_resource)
    invalid_counts = {**counts_by_field, "canonical_only_edges": invalid_value} if invalid_value is not None else None
    header_by_field = {**expected, "resource_diagnostics": diagnostics_by_resource, key: invalid_counts}
    with pytest.raises(RuntimeError, match="scope_invalid"):
        scope.published_header_scope(header_by_field)
    legacy_diagnostics_by_resource = {kind: {scope.SCOPE_KEY: example()} for kind in scope.RESOURCE_TYPES}
    legacy_header_by_field = {
        **scope.publication_scope_metadata(legacy_diagnostics_by_resource),
        "resource_diagnostics": legacy_diagnostics_by_resource,
    }
    assert scope.published_header_scope(legacy_header_by_field)[scope.SCOPE_KEY] == example()
    with pytest.raises(RuntimeError, match="scope_invalid"):
        scope.published_header_scope({**legacy_header_by_field, key: None})


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
