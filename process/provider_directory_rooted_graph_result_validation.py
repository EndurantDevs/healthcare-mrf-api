# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Fail-closed value validation for immutable rooted-graph witnesses."""

from __future__ import annotations

import json
import re
from typing import Any, Callable

from process.provider_directory_rooted_graph_identity import (
    ROOTED_GRAPH_QUERY_PATTERN,
    SHA256_PATTERN,
    canonical_fhir_resource_id,
)
from process.provider_directory_rooted_graph_query import (
    ROOTED_GRAPH_QUERY_DIRECT_READ,
    ROOTED_GRAPH_QUERY_EXACT_SEARCH,
    ROOTED_GRAPH_QUERY_INSURANCE_PLAN_CENSUS,
)
from process.provider_directory_rooted_graph_references import (
    provider_directory_rooted_graph_resource_references,
)
from process.provider_directory_rooted_graph_store_contract import (
    PROVIDER_DIRECTORY_ROOTED_GRAPH_CLOSURE_SCOPES,
    _canonical_json,
    _sha256_text,
)


FIELD_PATH_PATTERN = re.compile(
    r"(?:[A-Za-z][A-Za-z0-9]*(?:\[[0-9]+\])?"
    r"|extension\[[0-9]+\](?:\.extension\[[0-9]+\]){0,5}"
    r"\.valueReference)\Z"
)
MAX_RESOURCE_JSON_BYTES = 1_048_576
ROOTED_GRAPH_RESOURCE_TYPES = frozenset(
    {
        "PractitionerRole",
        "OrganizationAffiliation",
        "Organization",
        "Location",
        "HealthcareService",
        "InsurancePlan",
        "Endpoint",
    }
)
ROOTED_GRAPH_REFERENCE_TYPES = ROOTED_GRAPH_RESOURCE_TYPES | {"Practitioner"}


def validate_rooted_graph_resource_witness(resource_witness: Any) -> None:
    """Recompute one canonical retained payload and its storage bounds."""

    try:
        canonical_resource_id = canonical_fhir_resource_id(resource_witness.resource_id)
        if type(resource_witness.payload_json_text) is not str:
            raise ValueError
        resource_by_field = json.loads(resource_witness.payload_json_text)
        canonical_payload = _canonical_json(resource_by_field)
        payload_size = len(resource_witness.payload_json_text.encode("utf-8"))
        provider_directory_rooted_graph_resource_references(resource_by_field)
    except (MemoryError, RecursionError, TypeError, UnicodeError, ValueError):
        raise ValueError("provider_directory_rooted_graph_resource_invalid") from None
    if (
        type(resource_by_field) is not dict
        or resource_witness.resource_type not in ROOTED_GRAPH_RESOURCE_TYPES
        or canonical_resource_id != resource_witness.resource_id
        or resource_by_field.get("resourceType") != resource_witness.resource_type
        or resource_by_field.get("id") != resource_witness.resource_id
        or canonical_payload != resource_witness.payload_json_text
        or payload_size > MAX_RESOURCE_JSON_BYTES
        or type(resource_witness.payload_sha256) is not str
        or SHA256_PATTERN.fullmatch(resource_witness.payload_sha256) is None
        or resource_witness.payload_sha256
        != _sha256_text(resource_witness.payload_json_text)
        or resource_witness.closure_scope
        not in PROVIDER_DIRECTORY_ROOTED_GRAPH_CLOSURE_SCOPES
    ):
        raise ValueError("provider_directory_rooted_graph_resource_invalid")


def validate_rooted_graph_edge_witness(edge_witness: Any) -> None:
    """Recompute one closed, typed local-reference edge identity."""

    try:
        canonical_source_id = canonical_fhir_resource_id(
            edge_witness.source_resource_id
        )
        canonical_target_id = canonical_fhir_resource_id(
            edge_witness.target_resource_id
        )
        edge_identity = "\x1f".join(
            (
                edge_witness.source_resource_type,
                canonical_source_id,
                edge_witness.field_path,
                edge_witness.target_resource_type,
                canonical_target_id,
            )
        )
    except (TypeError, ValueError):
        raise ValueError("provider_directory_rooted_graph_edge_invalid") from None
    if (
        edge_witness.source_resource_type not in ROOTED_GRAPH_RESOURCE_TYPES
        or edge_witness.target_resource_type not in ROOTED_GRAPH_REFERENCE_TYPES
        or canonical_source_id != edge_witness.source_resource_id
        or canonical_target_id != edge_witness.target_resource_id
        or type(edge_witness.field_path) is not str
        or FIELD_PATH_PATTERN.fullmatch(edge_witness.field_path) is None
        or type(edge_witness.edge_sha256) is not str
        or SHA256_PATTERN.fullmatch(edge_witness.edge_sha256) is None
        or edge_witness.edge_sha256 != _sha256_text(edge_identity)
        or edge_witness.closure_scope
        not in PROVIDER_DIRECTORY_ROOTED_GRAPH_CLOSURE_SCOPES
    ):
        raise ValueError("provider_directory_rooted_graph_edge_invalid")


def _is_witness_vector_canonical(
    query_result: Any,
    resource_witness_type: type,
    edge_witness_type: type,
    expected_edges: tuple[Any, ...],
) -> bool:
    resources = query_result.resources
    edges = query_result.edges
    return bool(
        type(resources) is tuple
        and all(type(witness) is resource_witness_type for witness in resources)
        and tuple(
            sorted(resources, key=lambda resource_witness: resource_witness.resource_id)
        )
        == resources
        and len({witness.resource_id for witness in resources}) == len(resources)
        and type(edges) is tuple
        and all(type(witness) is edge_witness_type for witness in edges)
        and tuple(sorted(edges, key=lambda edge_witness: edge_witness.edge_sha256))
        == edges
        and len({witness.edge_sha256 for witness in edges}) == len(edges)
        and edges == expected_edges
    )


def validate_rooted_graph_query_result_integrity(
    query_result: Any,
    *,
    resource_witness_type: type,
    edge_witness_type: type,
    expected_edges: tuple[Any, ...],
    expected_resource_hash: str,
    expected_edge_hash: str,
) -> None:
    """Verify deterministic ordering, set hashes, and terminal finiteness."""

    is_advertised_total_valid = query_result.advertised_total is None or (
        type(query_result.advertised_total) is int
        and query_result.advertised_total >= 0
    )
    if (
        not _is_witness_vector_canonical(
            query_result,
            resource_witness_type,
            edge_witness_type,
            expected_edges,
        )
        or type(query_result.query_id) is not str
        or ROOTED_GRAPH_QUERY_PATTERN.fullmatch(query_result.query_id) is None
        or query_result.resource_set_sha256 != expected_resource_hash
        or query_result.edge_set_sha256 != expected_edge_hash
        or query_result.result_sha256
        != _sha256_text(expected_resource_hash + "\x1f" + expected_edge_hash)
        or type(query_result.terminal_record_sha256) is not str
        or SHA256_PATTERN.fullmatch(query_result.terminal_record_sha256) is None
        or not is_advertised_total_valid
        or type(query_result.terminal_page_count) is not int
        or query_result.terminal_page_count < 1
        or query_result.pagination_terminal is not True
    ):
        raise ValueError("provider_directory_rooted_graph_result_invalid")


def _validate_exact_result_binding(claim: Any, query_result: Any) -> None:
    reference_field = {
        "PractitionerRole": "practitioner",
        "OrganizationAffiliation": "participatingOrganization",
    }[claim.resource_type]
    expected_reference = f"{claim.reference_type}/{claim.reference_id}"
    if query_result.advertised_total is not None and (
        query_result.advertised_total != len(query_result.resources)
    ):
        raise ValueError("provider_directory_rooted_graph_result_invalid")
    for resource_witness in query_result.resources:
        resource_by_field = json.loads(resource_witness.payload_json_text)
        reference_by_field = resource_by_field.get(reference_field)
        if (
            type(reference_by_field) is not dict
            or reference_by_field.get("reference") != expected_reference
            or resource_witness.closure_scope != claim.closure_scope
        ):
            raise ValueError("provider_directory_rooted_graph_result_invalid")


def _validate_direct_result_binding(claim: Any, query_result: Any) -> None:
    if (
        len(query_result.resources) != 1
        or query_result.resources[0].resource_id != claim.reference_id
        or query_result.resources[0].closure_scope != claim.closure_scope
        or query_result.terminal_page_count != 1
    ):
        raise ValueError("provider_directory_rooted_graph_result_invalid")


def _validate_census_result_binding(query_result: Any) -> None:
    if query_result.advertised_total != len(query_result.resources) or any(
        resource_witness.closure_scope not in {"plan", "census"}
        for resource_witness in query_result.resources
    ):
        raise ValueError("provider_directory_rooted_graph_result_invalid")


def validate_rooted_graph_query_result_claim(
    claim: Any,
    query_result: Any,
    *,
    expected_terminal_hash: str,
) -> None:
    """Bind one internally valid result to its exact leased query shape."""

    if query_result.query_id != claim.query_id or any(
        resource_witness.resource_type != claim.resource_type
        for resource_witness in query_result.resources
    ):
        raise ValueError("provider_directory_rooted_graph_result_invalid")
    validator_by_kind: dict[str, Callable[..., None]] = {
        ROOTED_GRAPH_QUERY_EXACT_SEARCH: _validate_exact_result_binding,
        ROOTED_GRAPH_QUERY_DIRECT_READ: _validate_direct_result_binding,
        ROOTED_GRAPH_QUERY_INSURANCE_PLAN_CENSUS: (
            lambda _claim, candidate_result: _validate_census_result_binding(
                candidate_result
            )
        ),
    }
    validator_by_kind[claim.kind](claim, query_result)
    if (
        claim.kind == ROOTED_GRAPH_QUERY_DIRECT_READ
        and query_result.advertised_total is not None
    ) or query_result.terminal_record_sha256 != expected_terminal_hash:
        raise ValueError("provider_directory_rooted_graph_result_invalid")


__all__ = (
    "validate_rooted_graph_edge_witness",
    "validate_rooted_graph_query_result_claim",
    "validate_rooted_graph_query_result_integrity",
    "validate_rooted_graph_resource_witness",
)
