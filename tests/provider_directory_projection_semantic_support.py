# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

from copy import deepcopy
from collections.abc import Mapping
import hashlib
from typing import Any

from process.provider_directory_projection_contribution import (
    reduce_semantic_outcomes,
)


def digest(label: str) -> str:
    return hashlib.sha256(label.encode()).hexdigest()


def semantic_pair(
    resource_type: str,
    resource_id: str,
    *,
    npi: int | None = None,
    evidence_by_name: Mapping[str, list[Any]] | None = None,
    geocoded: bool = False,
    network_links: int = 0,
    affiliation_links: int = 0,
) -> tuple[dict[str, Any], dict[str, Any]]:
    supplied_evidence_by_name = dict(evidence_by_name or {})
    evidence_by_name = {
        field_name: list(supplied_evidence_by_name.get(field_name) or [])
        for field_name in (
            "names",
            "specialties",
            "contacts",
            "addresses",
            "references",
        )
    }
    inline_evidence_by_name = {
        field_name: evidence_entries
        for field_name, evidence_entries in evidence_by_name.items()
        if evidence_entries
    }
    identity_map = {
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
    resource_map = {
        **identity_map,
        "payload_json": {"resourceType": resource_type, "id": resource_id},
        "summary_npi": npi,
        "summary_address_count": len(evidence_by_name["addresses"]),
        "summary_addressed_location": (
            resource_type == "Location" and bool(evidence_by_name["addresses"])
        ),
        "summary_geocoded_location": geocoded,
        "summary_network_link_count": network_links,
        "summary_affiliation_link_count": affiliation_links,
        "profile_evidence_json": deepcopy(inline_evidence_by_name) or None,
    }
    contribution_map = {
        **identity_map,
        "direct_npi": npi,
        **{
            f"{field_name}_json": deepcopy(evidence_entries)
            for field_name, evidence_entries in evidence_by_name.items()
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
            geocoded=True,
        ),
        semantic_pair("InsurancePlan", "ip-1", network_links=3),
        semantic_pair(
            "OrganizationAffiliation",
            "oa-1",
            affiliation_links=2,
        ),
    )
    return [pair[0] for pair in pairs], [pair[1] for pair in pairs]


def semantic_outcome_proof():
    resources, contributions = semantic_rows_and_contributions()
    return reduce_semantic_outcomes(resources, contributions)
