"""Typed role and affiliation evidence checks for the API harness."""

from __future__ import annotations

from typing import Any, Mapping

from scripts.research.provider_directory_api_evidence_models import (
    SourceEvaluationContext,
    SourceSelection,
)
from scripts.research.provider_directory_api_evidence_typed import (
    MappedEvidenceWitness,
    has_detail_witness,
    has_provider_search_witness,
)


def _http_summary(http_result: Any) -> dict[str, Any]:
    summary_map: dict[str, Any] = {
        "status_code": http_result.status_code,
        "latency_ms": http_result.latency_ms,
    }
    if http_result.error:
        summary_map["error"] = http_result.error
    return summary_map


def _is_within_latency_slo(http_result: Any, latency_slo_ms: float) -> bool:
    return latency_slo_ms == 0 or http_result.latency_ms <= latency_slo_ms


def _safe_witness_summary(witness: MappedEvidenceWitness) -> dict[str, Any]:
    return {
        "npi": witness.npi,
        "resource_type": witness.resource_type,
        "resource_id": witness.resource_id,
        "insurance_plan_ids": list(witness.insurance_plan_ids),
        "network_ids": [network.resource_id for network in witness.networks],
    }


def evaluate_witness(
    witness: MappedEvidenceWitness,
    api_client: Any,
    api_latency_slo_ms: float,
) -> dict[str, Any]:
    """Verify one exact typed witness on detail and provider-search surfaces."""
    detail_param_map = {"include_sources": "true", "include_evidence": "true"}
    if witness.address_key:
        detail_param_map["address_key"] = witness.address_key
    detail_result = api_client.get_json(f"providers/{witness.npi}", detail_param_map)
    search_result = api_client.get_json(
        "providers",
        {
            "npi": str(witness.npi),
            "include_sources": "true",
            "include_evidence": "true",
            **({"address_key": witness.address_key} if witness.address_key else {}),
        },
    )
    return {
        "witness": _safe_witness_summary(witness),
        "detail": _http_summary(detail_result),
        "detail_evidence_present": detail_result.status_code == 200
        and has_detail_witness(detail_result.payload, witness),
        "detail_within_latency_slo": _is_within_latency_slo(
            detail_result, api_latency_slo_ms
        ),
        "provider_search": _http_summary(search_result),
        "provider_search_evidence_present": search_result.status_code == 200
        and has_provider_search_witness(search_result.payload, witness),
        "provider_search_within_latency_slo": _is_within_latency_slo(
            search_result, api_latency_slo_ms
        ),
    }


def _is_witness_check_passing(witness_check: Mapping[str, Any]) -> bool:
    return bool(
        witness_check["detail_evidence_present"]
        and witness_check["detail_within_latency_slo"]
        and witness_check["provider_search_evidence_present"]
        and witness_check["provider_search_within_latency_slo"]
    )


def _resource_capability(
    selection: SourceSelection,
    resource_type: str,
    resource_witnesses: list[MappedEvidenceWitness],
    capability_checks: list[Mapping[str, Any]],
    completion_state: str,
    api_enabled: bool,
    context: SourceEvaluationContext,
) -> dict[str, Any]:
    capability_map: dict[str, Any] = {
        "declared": resource_type in selection.resources,
        "witness_count": len(resource_witnesses),
        "completion_witness_count": sum(
            witness.supports_completion for witness in resource_witnesses
        ),
        "current_dataset_completion": completion_state,
    }
    if resource_type not in selection.resources:
        capability_map["state"] = "not_applicable"
    elif not resource_witnesses:
        capability_map.update(_empty_capability(completion_state, context))
    elif not api_enabled:
        capability_map.update(
            state="fail",
            reason=context.api_skip_reason or "api_credentials_unavailable",
        )
    elif completion_state == "completed_empty":
        capability_map.update(
            state="fail", reason="completion_proof_contradicts_mapped_witness"
        )
    else:
        capability_map["state"] = (
            "pass"
            if capability_checks and all(map(_is_witness_check_passing, capability_checks))
            else "fail"
        )
    return capability_map


def _empty_capability(
    completion_state: str, context: SourceEvaluationContext
) -> dict[str, str]:
    state = (
        completion_state
        if completion_state in {"completed_empty", "provider_surface_not_applicable"}
        else "not_observed"
    )
    if context.completion_probe_error:
        return {"state": state, "reason": "current_dataset_completion_probe_failed"}
    if context.witness_probe_error:
        return {"state": state, "reason": "mapped_evidence_probe_failed"}
    return {"state": state}


def mapped_evidence_capabilities(
    selection: SourceSelection,
    witnesses: list[MappedEvidenceWitness],
    witness_checks: list[Mapping[str, Any]],
    context: SourceEvaluationContext,
    completion_proofs: Mapping[str, Mapping[str, Any]] | None,
    *,
    api_enabled: bool,
) -> dict[str, dict[str, Any]]:
    """Classify the declared role and affiliation capabilities for one source."""
    checks_by_resource: dict[str, list[Mapping[str, Any]]] = {}
    for witness_check in witness_checks:
        resource_type = str(witness_check["witness"]["resource_type"])
        checks_by_resource.setdefault(resource_type, []).append(witness_check)
    return {
        capability_name: _resource_capability(
            selection,
            resource_type,
            [witness for witness in witnesses if witness.resource_type == resource_type],
            checks_by_resource.get(resource_type, []),
            str((completion_proofs or {}).get(resource_type, {}).get("state") or "unproven"),
            api_enabled,
            context,
        )
        for resource_type, capability_name in (
            ("PractitionerRole", "practitioner_role"),
            ("OrganizationAffiliation", "organization_affiliation"),
        )
    }
