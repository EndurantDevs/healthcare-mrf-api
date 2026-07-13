"""Report helpers for Provider Directory API evidence checks."""

from __future__ import annotations

from typing import Any, Mapping

from scripts.research.provider_directory_api_evidence_support import SourceSelection
from scripts.research.provider_directory_api_evidence_typed import MappedEvidenceWitness


def database_failure_result(selection: SourceSelection) -> dict[str, Any]:
    """Build one source result when the current-dataset probe failed."""
    capability_by_name = {}
    for resource_type, capability_name in (
        ("PractitionerRole", "practitioner_role"),
        ("OrganizationAffiliation", "organization_affiliation"),
    ):
        capability_map: dict[str, Any] = {
            "declared": resource_type in selection.resources,
            "witness_count": 0,
            "completion_witness_count": 0,
            "current_dataset_completion": "unproven",
            "state": (
                "not_observed"
                if resource_type in selection.resources
                else "not_applicable"
            ),
        }
        if resource_type in selection.resources:
            capability_map["reason"] = "database_probe_failed"
        capability_by_name[capability_name] = capability_map
    return {
        "entry_id": selection.entry_id,
        "source_id": selection.source_id,
        "classification": selection.classification,
        "required": selection.required,
        "resources": list(selection.resources),
        "status": "fail" if selection.required else "skip",
        "reason": "database_probe_failed",
        "samples": [],
        "mapped_evidence_capabilities": capability_by_name,
    }


def mapped_completion_summary(
    *,
    require_mapped_evidence: bool,
    source_result_list: list[dict[str, Any]],
    witness_list_by_source: Mapping[str, list[MappedEvidenceWitness]],
    witness_probe_error: str | None,
) -> dict[str, Any]:
    """Summarize strict completion across every selected declared capability."""
    witness_count = sum(
        len(witness_list) for witness_list in witness_list_by_source.values()
    )
    plan_network_context_count = sum(
        witness.supports_plan_network_context
        for witness_list in witness_list_by_source.values()
        for witness in witness_list
    )
    declared_capability_list = [
        capability_map
        for source_result in source_result_list
        for capability_map in source_result["mapped_evidence_capabilities"].values()
        if capability_map["declared"]
    ]
    completed_capability_count = sum(
        capability_map["state"] in {"pass", "completed_empty"}
        for capability_map in declared_capability_list
    )
    is_inconclusive = bool(
        require_mapped_evidence
        and (
            not declared_capability_list
            or completed_capability_count != len(declared_capability_list)
        )
    )
    return {
        "mapped_evidence_witnesses": witness_count,
        "mapped_plan_network_context_witnesses": plan_network_context_count,
        "mapped_role_plan_network_witnesses": plan_network_context_count,
        "mapped_evidence_declared_capabilities": len(declared_capability_list),
        "mapped_evidence_passed_capabilities": completed_capability_count,
        "mapped_evidence_incomplete_capabilities": (
            len(declared_capability_list) - completed_capability_count
        ),
        "mapped_evidence_completion": (
            "inconclusive"
            if is_inconclusive
            else "pass" if require_mapped_evidence else "not_requested"
        ),
        "completion_inconclusive": is_inconclusive,
        "mapped_evidence_probe_failed": bool(witness_probe_error),
    }
