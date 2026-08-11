# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Immutable result and comparison-root contracts for rooted graphs."""

from __future__ import annotations

from dataclasses import dataclass, field
import json
from typing import Any

from process.provider_directory_rooted_graph_identity import (
    ROOTED_GRAPH_SCOPE_PATTERN,
    SHA256_PATTERN,
    canonical_fhir_resource_id,
)
from process.provider_directory_rooted_graph_query import (
    ROOTED_GRAPH_QUERY_DIRECT_READ,
    ROOTED_GRAPH_QUERY_EXACT_SEARCH,
    ROOTED_GRAPH_QUERY_INSURANCE_PLAN_CENSUS,
)
from process.provider_directory_rooted_graph_references import (
    build_provider_directory_insurance_plan_census,
    intersect_provider_directory_insurance_plan_census,
    provider_directory_rooted_graph_indexed_references,
    provider_directory_rooted_graph_resource_references,
)
from process.provider_directory_rooted_graph_store_contract import (
    ACQUISITION_PATTERN,
    ProviderDirectoryRootedGraphWorkClaim,
    _canonical_json,
    _sha256_text,
)
from process.provider_directory_rooted_graph_result_validation import (
    validate_rooted_graph_edge_witness,
    validate_rooted_graph_query_result_claim,
    validate_rooted_graph_query_result_integrity,
    validate_rooted_graph_resource_witness,
)
from process.provider_directory_rooted_graph_terminal import (
    ERROR_PATTERN,
    ProviderDirectoryRootedGraphMissingWitness,
    _edge_hash,
    _resource_hash,
    _terminal_hash,
    build_rooted_graph_missing_witness,
    rooted_graph_error_terminal_sha256,
)


@dataclass(frozen=True, slots=True)
class ProviderDirectoryRootedGraphResourceWitness:
    resource_type: str
    resource_id: str
    payload_sha256: str
    payload_json_text: str = field(repr=False)
    closure_scope: str

    def __post_init__(self) -> None:
        validate_rooted_graph_resource_witness(self)


@dataclass(frozen=True, slots=True)
class ProviderDirectoryRootedGraphEdgeWitness:
    source_resource_type: str
    source_resource_id: str
    field_path: str
    target_resource_type: str
    target_resource_id: str
    edge_sha256: str
    closure_scope: str

    def __post_init__(self) -> None:
        validate_rooted_graph_edge_witness(self)


@dataclass(frozen=True, slots=True, repr=False)
class ProviderDirectoryRootedGraphQueryResult:
    """One terminal, finite response reduced to immutable witnesses."""

    query_id: str
    result_sha256: str
    terminal_record_sha256: str
    resources: tuple[ProviderDirectoryRootedGraphResourceWitness, ...]
    edges: tuple[ProviderDirectoryRootedGraphEdgeWitness, ...]
    resource_set_sha256: str
    edge_set_sha256: str
    advertised_total: int | None
    terminal_page_count: int
    pagination_terminal: bool = True

    def __post_init__(self) -> None:
        validate_rooted_graph_query_result_integrity(
            self,
            resource_witness_type=ProviderDirectoryRootedGraphResourceWitness,
            edge_witness_type=ProviderDirectoryRootedGraphEdgeWitness,
            expected_edges=_query_edges(self.resources),
            expected_resource_hash=_resource_hash(self.resources),
            expected_edge_hash=_edge_hash(self.edges),
        )


def _edge_witnesses(
    resource_by_field: dict[str, Any],
    closure_scope: str,
) -> tuple[ProviderDirectoryRootedGraphEdgeWitness, ...]:
    source_type = resource_by_field["resourceType"]
    source_id = resource_by_field["id"]
    edge_witnesses = []
    for field_path, reference in provider_directory_rooted_graph_indexed_references(
        resource_by_field
    ):
        edge_identity = "\x1f".join(
            (
                source_type,
                source_id,
                field_path,
                reference.resource_type,
                reference.resource_id,
            )
        )
        edge_witnesses.append(
            ProviderDirectoryRootedGraphEdgeWitness(
                source_resource_type=source_type,
                source_resource_id=source_id,
                field_path=field_path,
                target_resource_type=reference.resource_type,
                target_resource_id=reference.resource_id,
                edge_sha256=_sha256_text(edge_identity),
                closure_scope=closure_scope,
            )
        )
    return tuple(sorted(edge_witnesses, key=lambda edge: edge.edge_sha256))


def _plan_inputs(
    resource_inputs: list[object],
    reachable_network_references: object,
    advertised_total: int | None,
    terminal_page_count: int,
) -> tuple[list[dict[str, Any]], set[str]]:
    census = build_provider_directory_insurance_plan_census(
        resource_inputs,
        advertised_total=advertised_total,
        terminal_page_count=terminal_page_count,
    )
    selected_plan_ids = {
        plan_resource["id"]
        for plan_resource in intersect_provider_directory_insurance_plan_census(
            census,
            reachable_network_references,
        )
    }
    return list(census.resources()), selected_plan_ids


def _normalized_inputs(
    claim: ProviderDirectoryRootedGraphWorkClaim,
    resource_inputs: object,
    reachable_network_references: object,
    advertised_total: int | None,
    terminal_page_count: int,
) -> tuple[list[dict[str, Any]], set[str]]:
    if type(resource_inputs) not in {list, tuple}:
        raise ValueError("provider_directory_rooted_graph_result_invalid")
    normalized_inputs = list(resource_inputs)
    if claim.kind == ROOTED_GRAPH_QUERY_INSURANCE_PLAN_CENSUS:
        return _plan_inputs(
            normalized_inputs,
            reachable_network_references,
            advertised_total,
            terminal_page_count,
        )
    if claim.kind == ROOTED_GRAPH_QUERY_DIRECT_READ and advertised_total is not None:
        raise ValueError("provider_directory_rooted_graph_result_invalid")
    if (
        claim.kind == ROOTED_GRAPH_QUERY_EXACT_SEARCH
        and advertised_total is not None
        and advertised_total != len(normalized_inputs)
    ):
        raise ValueError("provider_directory_rooted_graph_result_invalid")
    return normalized_inputs, set()


def _resource_witnesses(
    claim: ProviderDirectoryRootedGraphWorkClaim,
    resource_inputs: object,
    reachable_network_references: object,
    advertised_total: int | None,
    terminal_page_count: int,
) -> tuple[ProviderDirectoryRootedGraphResourceWitness, ...]:
    normalized_inputs, selected_plan_ids = _normalized_inputs(
        claim,
        resource_inputs,
        reachable_network_references,
        advertised_total,
        terminal_page_count,
    )
    resource_witnesses = []
    seen_ids = set()
    for resource_by_field in normalized_inputs:
        if type(resource_by_field) is not dict:
            raise ValueError("provider_directory_rooted_graph_result_invalid")
        provider_directory_rooted_graph_resource_references(resource_by_field)
        resource_type = resource_by_field.get("resourceType")
        resource_id = canonical_fhir_resource_id(resource_by_field.get("id"))
        if resource_type != claim.resource_type or resource_id in seen_ids:
            raise ValueError("provider_directory_rooted_graph_result_invalid")
        if claim.kind == ROOTED_GRAPH_QUERY_EXACT_SEARCH:
            reference_field = {
                "PractitionerRole": "practitioner",
                "OrganizationAffiliation": "participatingOrganization",
            }.get(claim.resource_type)
            query_reference = resource_by_field.get(reference_field)
            expected_reference = f"{claim.reference_type}/{claim.reference_id}"
            if (
                type(query_reference) is not dict
                or query_reference.get("reference") != expected_reference
            ):
                raise ValueError("provider_directory_rooted_graph_result_invalid")
        seen_ids.add(resource_id)
        canonical_json = _canonical_json(resource_by_field)
        closure_scope = (
            "plan"
            if claim.kind == ROOTED_GRAPH_QUERY_INSURANCE_PLAN_CENSUS
            and resource_id in selected_plan_ids
            else claim.closure_scope
        )
        resource_witnesses.append(
            ProviderDirectoryRootedGraphResourceWitness(
                resource_type=resource_type,
                resource_id=resource_id,
                payload_sha256=_sha256_text(canonical_json),
                payload_json_text=canonical_json,
                closure_scope=closure_scope,
            )
        )
    if claim.kind == ROOTED_GRAPH_QUERY_DIRECT_READ and (
        len(resource_witnesses) != 1
        or resource_witnesses[0].resource_id != claim.reference_id
    ):
        raise ValueError("provider_directory_rooted_graph_result_invalid")
    return tuple(sorted(resource_witnesses, key=lambda witness: witness.resource_id))


def _query_edges(resource_witnesses):
    return tuple(
        sorted(
            (
                edge_witness
                for resource_witness in resource_witnesses
                for edge_witness in _edge_witnesses(
                    json.loads(resource_witness.payload_json_text),
                    resource_witness.closure_scope,
                )
            ),
            key=lambda edge_witness: edge_witness.edge_sha256,
        )
    )


def validate_rooted_graph_query_result(
    claim: ProviderDirectoryRootedGraphWorkClaim,
    query_result: ProviderDirectoryRootedGraphQueryResult,
) -> None:
    """Bind a self-validating result to the exact live query claim."""

    if (
        type(claim) is not ProviderDirectoryRootedGraphWorkClaim
        or type(query_result) is not ProviderDirectoryRootedGraphQueryResult
    ):
        raise ValueError("provider_directory_rooted_graph_result_invalid")
    expected_terminal = _terminal_hash(
        claim,
        query_result.result_sha256,
        len(query_result.resources),
        len(query_result.edges),
        query_result.advertised_total,
        query_result.terminal_page_count,
    )
    validate_rooted_graph_query_result_claim(
        claim,
        query_result,
        expected_terminal_hash=expected_terminal,
    )


def build_rooted_graph_query_result(
    claim: ProviderDirectoryRootedGraphWorkClaim,
    resource_inputs: object,
    *,
    advertised_total: int | None = None,
    terminal_page_count: int = 1,
    reachable_network_references: object = (),
) -> ProviderDirectoryRootedGraphQueryResult:
    """Canonicalize one terminal response and its local plan intersection."""

    if type(claim) is not ProviderDirectoryRootedGraphWorkClaim:
        raise ValueError("provider_directory_rooted_graph_claim_invalid")
    if type(terminal_page_count) is not int or terminal_page_count < 1:
        raise ValueError("provider_directory_rooted_graph_result_invalid")
    resource_witnesses = _resource_witnesses(
        claim,
        resource_inputs,
        reachable_network_references,
        advertised_total,
        terminal_page_count,
    )
    edge_witnesses = _query_edges(resource_witnesses)
    resource_set_sha256 = _resource_hash(resource_witnesses)
    edge_set_sha256 = _edge_hash(edge_witnesses)
    result_sha256 = _sha256_text(resource_set_sha256 + "\x1f" + edge_set_sha256)
    terminal_record_sha256 = _terminal_hash(
        claim,
        result_sha256,
        len(resource_witnesses),
        len(edge_witnesses),
        advertised_total,
        terminal_page_count,
    )
    query_result = ProviderDirectoryRootedGraphQueryResult(
        query_id=claim.query_id,
        result_sha256=result_sha256,
        terminal_record_sha256=terminal_record_sha256,
        resources=resource_witnesses,
        edges=edge_witnesses,
        resource_set_sha256=resource_set_sha256,
        edge_set_sha256=edge_set_sha256,
        advertised_total=advertised_total,
        terminal_page_count=terminal_page_count,
    )
    validate_rooted_graph_query_result(claim, query_result)
    return query_result


@dataclass(frozen=True, slots=True)
class ProviderDirectoryRootedGraphAcquisitionSummary:
    acquisition_id: str
    scope_id: str
    completed_count: int
    error_count: int
    resource_count: int
    edge_count: int
    terminal_set_sha256: str
    resource_set_sha256: str
    edge_set_sha256: str
    rooted_graph_sha256: str
    rooted_graph_complete: bool
    endpoint_collection_complete: bool
    endpoint_complete: bool

    def __post_init__(self) -> None:
        hashes = (
            self.terminal_set_sha256,
            self.resource_set_sha256,
            self.edge_set_sha256,
            self.rooted_graph_sha256,
        )
        counts = (
            self.completed_count,
            self.error_count,
            self.resource_count,
            self.edge_count,
        )
        if (
            ACQUISITION_PATTERN.fullmatch(self.acquisition_id) is None
            or ROOTED_GRAPH_SCOPE_PATTERN.fullmatch(self.scope_id) is None
            or any(type(count) is not int or count < 0 for count in counts)
            or self.error_count != 0
            or any(SHA256_PATTERN.fullmatch(value) is None for value in hashes)
            or self.rooted_graph_complete is not True
            or self.endpoint_collection_complete is not False
            or self.endpoint_complete is not False
        ):
            raise ValueError("provider_directory_rooted_graph_summary_invalid")


build_provider_directory_rooted_graph_query_result = build_rooted_graph_query_result
build_provider_directory_rooted_graph_missing_witness = (
    build_rooted_graph_missing_witness
)
provider_directory_rooted_graph_error_terminal_sha256 = (
    rooted_graph_error_terminal_sha256
)
validate_provider_directory_rooted_graph_query_result = (
    validate_rooted_graph_query_result
)


__all__ = (
    "build_provider_directory_rooted_graph_query_result",
    "build_provider_directory_rooted_graph_missing_witness",
    "build_rooted_graph_missing_witness",
    "build_rooted_graph_query_result",
    "provider_directory_rooted_graph_error_terminal_sha256",
    "rooted_graph_error_terminal_sha256",
    "validate_provider_directory_rooted_graph_query_result",
    "validate_rooted_graph_query_result",
    "ProviderDirectoryRootedGraphAcquisitionSummary",
    "ProviderDirectoryRootedGraphEdgeWitness",
    "ProviderDirectoryRootedGraphMissingWitness",
    "ProviderDirectoryRootedGraphQueryResult",
    "ProviderDirectoryRootedGraphResourceWitness",
    "ERROR_PATTERN",
)
