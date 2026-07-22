# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

from copy import deepcopy
from collections.abc import Mapping
import hashlib
from typing import Any

from process.provider_directory_projection_contribution import (
    reduce_semantic_outcomes,
)
from process.provider_directory_projection_typed_evidence import (
    SEMANTIC_GEOCODE_EVIDENCE_KEY,
    SEMANTIC_RELATIONSHIP_EVIDENCE_KEY,
)


def digest(label: str) -> str:
    return hashlib.sha256(label.encode()).hexdigest()


def _relationship_evidence(
    kind: str,
    target_resource_type: str,
    target_resource_id: str,
) -> dict[str, Any]:
    return {
        SEMANTIC_RELATIONSHIP_EVIDENCE_KEY: {
            "kind": kind,
            "target_resource_type": target_resource_type,
            "target_resource_id": target_resource_id,
        }
    }


def _fixture_evidence(
    evidence_by_name: Mapping[str, list[Any]] | None,
    geocode: tuple[int, int] | None,
    network_targets: tuple[tuple[str, str], ...],
    affiliation_relationships: tuple[tuple[str, str, str], ...],
) -> dict[str, list[Any]]:
    supplied_evidence_by_name = dict(evidence_by_name or {})
    normalized_evidence_by_name = {
        field_name: list(supplied_evidence_by_name.get(field_name) or [])
        for field_name in (
            "names",
            "specialties",
            "contacts",
            "addresses",
            "references",
        )
    }
    if geocode is not None:
        normalized_evidence_by_name["addresses"][0][
            SEMANTIC_GEOCODE_EVIDENCE_KEY
        ] = {
            "latitude_microdegrees": geocode[0],
            "longitude_microdegrees": geocode[1],
        }
    normalized_evidence_by_name["references"].extend(
        _relationship_evidence("network", target_type, target_id)
        for target_type, target_id in network_targets
    )
    normalized_evidence_by_name["references"].extend(
        _relationship_evidence(kind, target_type, target_id)
        for kind, target_type, target_id in affiliation_relationships
    )
    return normalized_evidence_by_name


def _semantic_identity(resource_type: str, resource_id: str) -> dict[str, Any]:
    return {
        "resource_type": resource_type,
        "resource_id": resource_id,
        "proof_partition_id": digest(f"partition:{resource_type}"),
        "payload_hash": digest(f"payload:{resource_type}:{resource_id}"),
        "source_rank": f"00000000:{resource_type}:{resource_id}",
        "active": True,
        "effective_start": "2026-01-01",
        "effective_end": None,
        "observed_at": "2026-07-22T00:00:00Z",
    }


def semantic_pair(
    resource_type: str,
    resource_id: str,
    *,
    npi: int | None = None,
    evidence_by_name: Mapping[str, list[Any]] | None = None,
    geocode: tuple[int, int] | None = None,
    network_targets: tuple[tuple[str, str], ...] = (),
    affiliation_relationships: tuple[tuple[str, str, str], ...] = (),
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Build one exact semantic winner/Profile contribution fixture pair."""

    normalized_evidence_by_name = _fixture_evidence(
        evidence_by_name,
        geocode,
        network_targets,
        affiliation_relationships,
    )
    inline_evidence_by_name = {
        field_name: evidence_entries
        for field_name, evidence_entries in normalized_evidence_by_name.items()
        if evidence_entries
    }
    identity_map = _semantic_identity(resource_type, resource_id)
    resource_map = {
        **identity_map,
        "payload_json": {"resourceType": resource_type, "id": resource_id},
        "summary_npi": npi,
        "summary_address_count": len(normalized_evidence_by_name["addresses"]),
        "summary_addressed_location": (
            resource_type == "Location"
            and bool(normalized_evidence_by_name["addresses"])
        ),
        "summary_geocoded_location": geocode is not None,
        "summary_network_link_count": len(network_targets),
        "summary_affiliation_link_count": len(affiliation_relationships),
        "profile_evidence_json": deepcopy(inline_evidence_by_name) or None,
    }
    contribution_map = {
        **identity_map,
        "direct_npi": npi,
        **{
            f"{field_name}_json": deepcopy(evidence_entries)
            for field_name, evidence_entries in normalized_evidence_by_name.items()
        },
    }
    return resource_map, contribution_map


def semantic_rows_and_contributions():
    pairs = (
        semantic_pair(
            "Practitioner",
            "p-1",
            npi=1_234_567_890,
            evidence_by_name={
                "names": [{"text": "Pat One"}],
                "specialties": [{"code": "207Q00000X"}],
                "contacts": [{"system": "phone", "value": "555-0100"}],
                "addresses": [{"line": ["1 Main St"]}],
                "references": [{"type": "Organization", "id": "o-1"}],
            },
        ),
        semantic_pair(
            "PractitionerRole",
            "pr-1",
            npi=1_234_567_890,
            evidence_by_name={
                "specialties": [
                    {"code": "207Q00000X"},
                    {"code": "208D00000X"},
                ],
                "references": [
                    {"type": "Practitioner", "id": "p-1"},
                    {"type": "Organization", "id": "o-1"},
                ],
            },
        ),
        semantic_pair("Organization", "o-1", npi=1_987_654_321),
        semantic_pair(
            "Location",
            "l-1",
            evidence_by_name={"addresses": [{"line": ["2 Main St"]}]},
            geocode=(40_712_800, -74_006_000),
        ),
        semantic_pair(
            "InsurancePlan",
            "ip-1",
            network_targets=(
                ("Organization", "network-1"),
                ("Organization", "network-2"),
                ("Organization", "network-3"),
            ),
        ),
        semantic_pair(
            "OrganizationAffiliation",
            "oa-1",
            affiliation_relationships=(
                ("organization", "Organization", "o-1"),
                ("network", "Organization", "network-1"),
            ),
        ),
    )
    return [pair[0] for pair in pairs], [pair[1] for pair in pairs]


def candidate_semantic_outcome():
    """Build a non-authoritative semantic aggregate for contract tests."""

    resources, contributions = semantic_rows_and_contributions()
    return reduce_semantic_outcomes(resources, contributions)
