# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Honest coverage labels for query-discovered institutional network snapshots."""

from copy import deepcopy
from typing import Any

SCOPE_KEY = "network_institutional_scope"
SOURCE_CONFIG_KEY = "provider_directory_network_snapshot"
PUBLICATION_KIND = "network_institutional_snapshot"
RESOURCE_TYPES = ("Organization", "OrganizationAffiliation")
COUNT_KEYS = frozenset(
    {
        "owned_network_roots",
        "affiliations",
        "discovered_networks",
        "unresolved_networks",
        "membership_queries",
        "membership_queries_complete",
        "participating_organizations",
        "unresolved_organization_references",
    }
)


def _require(valid):
    if not valid:
        raise RuntimeError("provider_directory_network_snapshot_scope_invalid")


def make_scope(*, root_name, owner_reference, discovery_counts):
    """Describe observed membership without promoting it to endpoint coverage."""
    _require(type(root_name) is str and bool(root_name.strip()) and root_name == root_name.strip())
    _require(
        type(owner_reference) is str
        and owner_reference.startswith("Organization/")
        and len(owner_reference.split("/")) == 2
        and bool(owner_reference.split("/")[1])
    )
    _require(
        type(discovery_counts) is dict
        and set(discovery_counts) == COUNT_KEYS
        and all(
            type(key) is str and bool(key) and type(resource_count) is int and resource_count >= 0
            for key, resource_count in discovery_counts.items()
        )
    )
    _require(
        discovery_counts["owned_network_roots"] > 0
        and discovery_counts["owned_network_roots"] <= discovery_counts["discovered_networks"]
        and discovery_counts["membership_queries"] == discovery_counts["discovered_networks"]
        and discovery_counts["membership_queries_complete"] == discovery_counts["membership_queries"]
    )
    return {
        "publication_kind": PUBLICATION_KIND,
        "root_selection": {"resourceType": "Organization", "name": root_name, "type": "ntwk"},
        "owner_reference": owner_reference,
        "network_membership": "observed_native_affiliation_references",
        "network_identity": "native_ntwk_coding_and_matching_partOf",
        "commercial_scope": "unknown",
        "endpoint_collection_complete": False,
        "endpoint_complete": False,
        "clinician_coverage": "not_established",
        "product_coverage": "not_established",
        "unresolved_linkage": "retained_as_unresolved",
        "discovery_counts": dict(discovery_counts),
    }


def validate_scope(value):
    """Reject changed guarantees or malformed accounting at publication."""
    _require(type(value) is dict and type(value.get("root_selection")) is dict)
    expected = make_scope(
        root_name=value["root_selection"].get("name"),
        owner_reference=value.get("owner_reference"),
        discovery_counts=value.get("discovery_counts"),
    )
    _require(
        value == expected
        and value.get("endpoint_collection_complete") is False
        and value.get("endpoint_complete") is False
    )
    return deepcopy(expected)


def publication_scope_metadata(diagnostics, *, required=False):
    """Require identical scope labels on both selected resource families."""
    _require(type(diagnostics) is dict)
    _require(all(type(diagnostics.get(kind, {})) is dict for kind in RESOURCE_TYPES))
    scopes = [diagnostics.get(kind, {}).get(SCOPE_KEY) for kind in RESOURCE_TYPES]
    if not required and all(scope is None for scope in scopes):
        return {}
    scope = validate_scope(scopes[0])
    _require(validate_scope(scopes[1]) == scope)
    return {
        SCOPE_KEY: scope,
        "publication_kind": PUBLICATION_KIND,
        "endpoint_collection_complete": False,
        "endpoint_complete": False,
    }


async def followup_scope_metadata(directory: Any, dataset: Any, followup: dict[str, Any]) -> dict[str, Any]:
    """Attach validated durable scope to one exact published snapshot descriptor."""
    if set(dataset.selected_resources) != set(RESOURCE_TYPES):
        return followup
    dataset_state = await directory._endpoint_dataset_state(dataset.dataset_id)
    publication_metadata = directory._json_object(dataset_state.get("publication_metadata_json"))
    if SCOPE_KEY not in publication_metadata:
        return followup
    scope_metadata = publication_scope_metadata(publication_metadata.get("resource_diagnostics", {}), required=True)
    _require(all(publication_metadata.get(key) == scope_value for key, scope_value in scope_metadata.items()))
    return {**followup, **scope_metadata}
