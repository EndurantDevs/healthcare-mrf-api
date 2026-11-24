# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Honest coverage labels for query-discovered institutional network snapshots."""

from copy import deepcopy
from string import ascii_uppercase, digits
from typing import Any

SCOPE_KEY = "network_institutional_scope"
MEMBERSHIP_RECONCILIATION_KEY = "network_membership_reconciliation"
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


def make_scope(*, root_name, owner_reference, discovery_counts, additional_names=()):
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
    query_by_parameter = {"resourceType": "Organization", "name": root_name, "type": "ntwk"}
    _require(isinstance(additional_names, (list, tuple)))
    if additional_names:
        alphabet = ascii_uppercase + digits
        _require(all(type(name) is str and len(name) == 1 and name in alphabet for name in additional_names))
        _require(len(set(additional_names)) == len(additional_names) and root_name not in additional_names)
        query_by_parameter["additional_name_queries"] = list(additional_names)
        query_by_parameter["unverified_name_queries"] = [name for name in alphabet if name not in additional_names]
    return {
        "publication_kind": PUBLICATION_KIND,
        "root_selection": query_by_parameter,
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
        additional_names=value["root_selection"].get("additional_name_queries", ()),
        owner_reference=value.get("owner_reference"),
        discovery_counts=value.get("discovery_counts"),
    )
    _require(
        value == expected
        and value.get("endpoint_collection_complete") is False
        and value.get("endpoint_complete") is False
    )
    return deepcopy(expected)


def _membership_counts(record):
    if MEMBERSHIP_RECONCILIATION_KEY not in record:
        return None
    counts_by_field = record[MEMBERSHIP_RECONCILIATION_KEY]
    _require(
        type(counts_by_field) is dict
        and set(counts_by_field) == {"search_edges", "canonical_only_edges"}
        and all(type(count) is int and count >= 0 for count in counts_by_field.values())
    )
    return counts_by_field


def publication_scope_metadata(diagnostics, *, required=False):
    """Require identical scope labels on both selected resource families."""
    _require(type(diagnostics) is dict)
    _require(all(type(diagnostics.get(kind, {})) is dict for kind in RESOURCE_TYPES))
    scopes = [diagnostics.get(kind, {}).get(SCOPE_KEY) for kind in RESOURCE_TYPES]
    membership_counts = [_membership_counts(diagnostics.get(kind, {})) for kind in RESOURCE_TYPES]
    if not required and all(scope is None for scope in scopes):
        _require(all(counts is None for counts in membership_counts))
        return {}
    scope = validate_scope(scopes[0])
    _require(validate_scope(scopes[1]) == scope)
    evidence, other_evidence = membership_counts
    _require(evidence == other_evidence)
    if evidence is not None:
        discovery_counts = scope["discovery_counts"]
        _require(
            discovery_counts["affiliations"] <= evidence["search_edges"]
            and evidence["search_edges"] + evidence["canonical_only_edges"]
            <= discovery_counts["affiliations"] * discovery_counts["discovered_networks"]
        )
    return {
        SCOPE_KEY: scope,
        "publication_kind": PUBLICATION_KIND,
        "endpoint_collection_complete": False,
        "endpoint_complete": False,
        **({MEMBERSHIP_RECONCILIATION_KEY: dict(evidence)} if evidence is not None else {}),
    }


def published_header_scope(publication_metadata: dict[str, Any], *, source_id: str | None = None) -> dict[str, Any]:
    """Check the immutable header against both resource diagnostics."""
    from process.provider_directory_network_traversal import CONFIG, LEGACY_CONFIG, SOURCE_ID

    scope_metadata = publication_scope_metadata(publication_metadata.get("resource_diagnostics", {}), required=True)
    _require(
        validate_scope(publication_metadata.get(SCOPE_KEY)) == scope_metadata[SCOPE_KEY]
        and publication_metadata.get("publication_kind") == PUBLICATION_KIND
        and publication_metadata.get("endpoint_complete") is False
        and publication_metadata.get("endpoint_collection_complete") is False
        and _membership_counts(publication_metadata) == scope_metadata.get(MEMBERSHIP_RECONCILIATION_KEY)
    )
    if source_id == SOURCE_ID:
        scope = scope_metadata[SCOPE_KEY]
        selection = scope["root_selection"]
        _require(
            publication_metadata.get("source_ids") == [source_id]
            and publication_metadata.get("selected_resources") == list(RESOURCE_TYPES)
            and scope["owner_reference"] == LEGACY_CONFIG["owner_reference"]
            and selection["name"] == LEGACY_CONFIG["name"]
            and (
                "additional_name_queries" not in selection
                or selection["additional_name_queries"] == list(CONFIG["name_search_characters"])
            )
        )
    return scope_metadata


async def followup_scope_metadata(directory: Any, dataset: Any, followup: dict[str, Any]) -> dict[str, Any]:
    """Attach validated coverage from the selected current edition."""
    from process.provider_directory_network_traversal import SOURCE_ID

    is_scoped_source = dataset.source_id == SOURCE_ID
    if set(dataset.selected_resources) != set(RESOURCE_TYPES):
        _require(not is_scoped_source)
        return followup
    if dataset.storage_schema is None:
        header = await directory._endpoint_dataset_state(dataset.dataset_id)
    else:
        async with directory._artifact_selection_snapshot([dataset.source_id]) as editions:
            _require(len(editions) == 1)
            edition = editions[0]
            _require(
                edition.schema_name == dataset.storage_schema
                and edition.dataset_id == dataset.dataset_id
                and edition.endpoint_id == dataset.endpoint_id
                and dataset.source_id in edition.source_ids
                and edition.header["acquisition_root_run_id"] == dataset.evidence_run_id
                and edition.header["dataset_hash"] == dataset.dataset_hash
            )
            header = edition.header
    metadata = directory._json_object(header.get("publication_metadata_json"))
    if not is_scoped_source and SCOPE_KEY not in metadata:
        return followup
    return {**followup, **published_header_scope(metadata, source_id=dataset.source_id)}
