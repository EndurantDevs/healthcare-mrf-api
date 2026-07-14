"""Credential-safe A/P/G/F/V result builders for Provider Directory samples."""

from __future__ import annotations

import urllib.parse
from typing import Any, Mapping

from scripts.research.provider_directory_api_evidence_models import (
    OverlaySample,
    SourceProvenance,
    SourceSelection,
)


def safe_api_base(api_base_value: Any) -> str | None:
    """Normalize a public HTTP(S) base or reject a credential-bearing URL."""
    candidate = str(api_base_value or "").strip().rstrip("/")
    parsed_base = urllib.parse.urlsplit(candidate)
    if (
        not candidate
        or parsed_base.scheme not in {"http", "https"}
        or not parsed_base.netloc
        or parsed_base.username
        or parsed_base.password
        or parsed_base.query
        or parsed_base.fragment
    ):
        return None
    return candidate


def safe_provenance(expected_provenance: SourceProvenance) -> dict[str, Any]:
    """Return the public provenance tuple without credentials or headers."""
    provenance_map: dict[str, Any] = {
        "source_id": expected_provenance.source_id,
        "endpoint_id": expected_provenance.endpoint_id,
        "dataset_id": expected_provenance.dataset_id,
    }
    if api_base := safe_api_base(expected_provenance.api_base):
        provenance_map["api_base"] = api_base
    if expected_provenance.org_name:
        provenance_map["org_name"] = expected_provenance.org_name
    if expected_provenance.plan_name:
        provenance_map["plan_name"] = expected_provenance.plan_name
    return provenance_map


def _has_expected_provenance(
    observed_map: Any, expected_provenance: SourceProvenance
) -> bool:
    if not isinstance(observed_map, Mapping):
        return False
    return bool(
        str(observed_map.get("source_id") or "") == expected_provenance.source_id
        and str(observed_map.get("endpoint_id") or "")
        == expected_provenance.endpoint_id
        and str(observed_map.get("dataset_id") or "") == expected_provenance.dataset_id
        and safe_api_base(observed_map.get("api_base"))
        == safe_api_base(expected_provenance.api_base)
        and (
            expected_provenance.org_name is None
            or observed_map.get("org_name") == expected_provenance.org_name
        )
        and (
            expected_provenance.plan_name is None
            or observed_map.get("plan_name") == expected_provenance.plan_name
        )
    )


def _detail_map(detail_payload: Mapping[str, Any] | None) -> Mapping[str, Any]:
    if not isinstance(detail_payload, Mapping):
        return {}
    response_data = detail_payload.get("data")
    if not isinstance(response_data, Mapping):
        return detail_payload
    npi_detail = response_data.get("npi")
    return npi_detail if isinstance(npi_detail, Mapping) else response_data


def _profile_source_list(
    detail_payload: Mapping[str, Any] | None,
) -> list[Mapping[str, Any]]:
    detail_map = _detail_map(detail_payload)
    source_map_list: list[Mapping[str, Any]] = []
    for profile_key in (
        "provider_directory_profile",
        "provider_directory_profile_evidence",
    ):
        profile_map = detail_map.get(profile_key)
        raw_source_list = profile_map.get("sources") if isinstance(profile_map, Mapping) else None
        if isinstance(raw_source_list, list):
            source_map_list.extend(
                source_map
                for source_map in raw_source_list
                if isinstance(source_map, Mapping)
            )
    return source_map_list


def _profile_fact_evidence_list(
    detail_payload: Mapping[str, Any] | None,
) -> list[Mapping[str, Any]]:
    detail_map = _detail_map(detail_payload)
    profile_evidence = detail_map.get("provider_directory_profile_evidence")
    facts_map = profile_evidence.get("facts") if isinstance(profile_evidence, Mapping) else None
    if not isinstance(facts_map, Mapping):
        return []
    evidence_map_list: list[Mapping[str, Any]] = []
    for fact_category in facts_map.values():
        item_list = fact_category.get("items") if isinstance(fact_category, Mapping) else None
        if not isinstance(item_list, list):
            continue
        for fact_item in item_list:
            raw_evidence_list = fact_item.get("evidence") if isinstance(fact_item, Mapping) else None
            if isinstance(raw_evidence_list, list):
                evidence_map_list.extend(
                    evidence_map
                    for evidence_map in raw_evidence_list
                    if isinstance(evidence_map, Mapping)
                )
    return evidence_map_list


def _check_state(
    state: str,
    *,
    reason: str | None = None,
    request_summary: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    check_map: dict[str, Any] = {"state": state}
    if reason:
        check_map["reason"] = reason
    if request_summary:
        check_map.update(request_summary)
    return check_map


def _address_check(sample: OverlaySample, sample_check: Mapping[str, Any]) -> dict[str, Any]:
    if not sample.address_key:
        return _check_state("missing_data", reason="current_overlay_address_key_not_found")
    return _check_state(
        "pass"
        if sample_check["detail_source_present"]
        and sample_check["detail_within_latency_slo"]
        else "fail",
        request_summary={"detail": sample_check["detail"]},
    )


def _phone_check(sample: OverlaySample, sample_check: Mapping[str, Any]) -> dict[str, Any]:
    if not sample.phone:
        return _check_state("missing_data", reason="current_overlay_phone_not_found")
    return _check_state(
        "pass"
        if sample_check.get("phone_source_present")
        and sample_check.get("phone_within_latency_slo")
        else "fail",
        request_summary={"phone_match_candidates": sample_check["phone_match_candidates"]},
    )


def _geo_check(geo_check: Mapping[str, Any] | None) -> dict[str, Any]:
    if geo_check is None:
        return _check_state("missing_data", reason="current_overlay_coordinates_not_found")
    detail_source_present = bool(geo_check["geo_detail_source_present"])
    surface_available = bool(geo_check["geo_surface_available"])
    reason = None
    if not detail_source_present:
        reason = "exact_geo_provenance_not_found"
    elif not surface_available:
        reason = "geo_surface_unavailable"
    return _check_state(
        "pass"
        if detail_source_present
        and surface_available
        and geo_check["geo_within_latency_slo"]
        else "fail",
        reason=reason,
        request_summary={
            "geo_detail": geo_check["geo_detail"],
            "geo_match_candidates": geo_check["geo_match_candidates"],
            "geo_candidate_source_present": geo_check["geo_candidate_source_present"],
        },
    )


def _profile_fact_check(
    selection: SourceSelection,
    sample_check: Mapping[str, Any],
    expected_provenance: SourceProvenance | None,
) -> dict[str, Any]:
    if not selection.profile_fact_resources:
        return _check_state(
            "not_applicable",
            reason="source_resource_profile_has_no_profile_fact_resources",
        )
    has_fact_evidence = any(
        expected_provenance
        and _has_expected_provenance(evidence_map, expected_provenance)
        and str(evidence_map.get("resource_type") or "")
        in selection.profile_fact_resources
        and str(evidence_map.get("resource_id") or "")
        for evidence_map in _profile_fact_evidence_list(sample_check.get("_detail_payload"))
    )
    return _check_state(
        "pass"
        if has_fact_evidence and sample_check["detail_within_latency_slo"]
        else "fail",
        reason=None if has_fact_evidence else "profile_fact_evidence_not_found",
        request_summary={"detail": sample_check["detail"]},
    )


def _provenance_check(
    sample_check: Mapping[str, Any], expected_provenance: SourceProvenance | None
) -> dict[str, Any]:
    if expected_provenance is None:
        return _check_state("missing_data", reason="current_source_provenance_not_found")
    has_profile_provenance = any(
        _has_expected_provenance(source_map, expected_provenance)
        for source_map in _profile_source_list(sample_check.get("_detail_payload"))
    )
    return _check_state(
        "pass"
        if has_profile_provenance and sample_check["detail_within_latency_slo"]
        else "fail",
        reason=(None if has_profile_provenance else "exact_profile_provenance_not_found"),
        request_summary={"detail": sample_check["detail"]},
    )


def build_matrix_checks(
    selection: SourceSelection,
    sample: OverlaySample,
    sample_check: Mapping[str, Any],
    geo_check: Mapping[str, Any] | None,
    expected_provenance: SourceProvenance | None,
) -> dict[str, dict[str, Any]]:
    """Return the configured A/P/G/F/V evidence checks for one API sample."""
    check_builder_by_code = {
        "A": lambda: _address_check(sample, sample_check),
        "P": lambda: _phone_check(sample, sample_check),
        "G": lambda: _geo_check(geo_check),
        "F": lambda: _profile_fact_check(
            selection, sample_check, expected_provenance
        ),
        "V": lambda: _provenance_check(sample_check, expected_provenance),
    }
    return {
        check_code: check_builder_by_code[check_code]()
        for check_code in selection.matrix_checks
    }


def has_passing_source_checks(source_result: Mapping[str, Any]) -> bool:
    """Return whether all baseline and configured matrix checks passed."""
    is_baseline_passed = all(
        source_check["detail_source_present"]
        and source_check["detail_within_latency_slo"]
        and source_check.get("phone_source_present", True)
        and source_check.get("phone_within_latency_slo", True)
        for source_check in source_result["checks"]
    )
    is_matrix_passed = all(
        check["state"] in {"pass", "not_applicable"}
        for matrix_check in source_result.get("verification_matrix", [])
        for check in matrix_check.values()
    )
    return bool(is_baseline_passed and is_matrix_passed)
