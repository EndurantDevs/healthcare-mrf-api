# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Atomic next-frontier derivation from terminal rooted-graph witnesses."""

from __future__ import annotations

from typing import Any

from process.provider_directory_rooted_graph_contract import (
    PROVIDER_DIRECTORY_ROOTED_GRAPH_DIRECT_READ_TYPES,
)
from process.provider_directory_rooted_graph_query import (
    build_provider_directory_organization_affiliation_query,
    build_rooted_graph_direct_read,
)
from process.provider_directory_rooted_graph_result_contract import (
    ProviderDirectoryRootedGraphQueryResult,
)
from process.provider_directory_rooted_graph_store_contract import (
    ProviderDirectoryRootedGraphStoreError,
    ProviderDirectoryRootedGraphWorkClaim,
    build_provider_directory_rooted_graph_work_spec,
)
from process.provider_directory_rooted_graph_store_support import (
    ACQUISITION_TABLE,
    ENDPOINT_TABLE,
    insert_work_spec,
    row_fields,
    set_store_action,
    table_ref,
)


async def _acquisition_api_base(database: Any, acquisition_id: str) -> str:
    fields = row_fields(
        await database.first(
            f"""
            SELECT endpoint.canonical_api_base
              FROM {table_ref(ACQUISITION_TABLE)} AS acquisition
              JOIN {table_ref(ENDPOINT_TABLE)} AS endpoint
                ON endpoint.endpoint_id = acquisition.acquisition_endpoint_id
             WHERE acquisition.acquisition_id = :acquisition_id
               AND acquisition.status = 'building';
            """,
            acquisition_id=acquisition_id,
        )
    )
    api_base = fields.get("canonical_api_base")
    if type(api_base) is not str:
        raise ProviderDirectoryRootedGraphStoreError("state")
    return api_base


def _derived_work_specs(claim, query_result, api_base):
    """Derive the complete deduplicated next frontier from retained witnesses."""

    specs_by_query_id = {}
    for edge in query_result.edges:
        if (
            edge.closure_scope not in {"root", "plan"}
            or edge.target_resource_type
            not in PROVIDER_DIRECTORY_ROOTED_GRAPH_DIRECT_READ_TYPES
        ):
            continue
        spec = build_provider_directory_rooted_graph_work_spec(
            claim.scope_id,
            build_rooted_graph_direct_read(
                api_base=api_base,
                resource_type=edge.target_resource_type,
                resource_id=edge.target_resource_id,
            ),
            closure_scope=edge.closure_scope,
            discovered_by_query_id=claim.query_id,
            discovered_source_type=edge.source_resource_type,
            discovered_source_id=edge.source_resource_id,
            discovered_edge_sha256=edge.edge_sha256,
        )
        specs_by_query_id.setdefault(spec.query_id, spec)
    for resource in query_result.resources:
        if resource.resource_type != "Organization" or resource.closure_scope not in {
            "root",
            "plan",
        }:
            continue
        spec = build_provider_directory_rooted_graph_work_spec(
            claim.scope_id,
            build_provider_directory_organization_affiliation_query(
                api_base,
                resource.resource_id,
            ),
            closure_scope=resource.closure_scope,
            discovered_by_query_id=claim.query_id,
            discovered_source_type="Organization",
            discovered_source_id=resource.resource_id,
        )
        specs_by_query_id.setdefault(spec.query_id, spec)
    return tuple(specs_by_query_id[key] for key in sorted(specs_by_query_id))


def _has_derived_frontier(query_result) -> bool:
    has_direct_frontier = any(
        edge.closure_scope in {"root", "plan"}
        and edge.target_resource_type
        in PROVIDER_DIRECTORY_ROOTED_GRAPH_DIRECT_READ_TYPES
        for edge in query_result.edges
    )
    has_affiliation_frontier = any(
        resource.resource_type == "Organization"
        and resource.closure_scope in {"root", "plan"}
        for resource in query_result.resources
    )
    return has_direct_frontier or has_affiliation_frontier


async def register_rooted_graph_frontier(
    database: Any,
    claim: ProviderDirectoryRootedGraphWorkClaim,
    query_result: ProviderDirectoryRootedGraphQueryResult,
) -> None:
    """Register every derived query before the terminal transaction can commit."""

    if not _has_derived_frontier(query_result):
        return
    specs = _derived_work_specs(
        claim,
        query_result,
        await _acquisition_api_base(database, claim.acquisition_id),
    )
    if not specs:
        return
    await set_store_action(database, "derive", claim.acquisition_id, claim.lease_token)
    for spec in specs:
        await insert_work_spec(database, claim.acquisition_id, spec)


__all__ = ("register_rooted_graph_frontier",)
