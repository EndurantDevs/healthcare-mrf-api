# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""PTG price-address assurance helpers.

These checks intentionally separate rate/network evidence from displayed-address
evidence. A row can prove a PTG price for an NPI/TIN/provider group without
proving the displayed office address.
"""

from __future__ import annotations

from pathlib import Path
import re
from typing import Any

from process.ptg_parts.provider_location_evidence import audit_tic_provider_location_evidence


ALLOWED_ADDRESS_BINDINGS = {
    "payer_confirmed_location",
    "payer_directory_corroborated_location",
    "inferred_from_provider_identity",
}
ALLOWED_ADDRESS_EVIDENCE_LEVELS = {
    "payer_confirmed_location",
    "payer_directory_network_location",
    "provider_directory_address",
    "multi_source_direct_mrf_address",
    "direct_mrf_address",
    "multi_source_provider_address",
    "nppes_provider_address",
    "unified_provider_address",
    "city_zip_fallback",
    "unknown",
}
ALLOWED_RATE_BINDINGS = {"tic_provider_group_npi_tin"}
DIRECT_PAYER_ADDRESS_SOURCES = {
    "payer_confirmed_location",
    "payer_provider_group_location",
    "ptg_provider_group_location",
    "tic_provider_group_location",
}
DIRECT_PAYER_LOCATION_RECORD_KEYS = {
    "source_record_id",
    "source_record_key",
    "source_provider_reference_id",
    "provider_reference_id",
    "provider_reference",
    "provider_group_id",
    "provider_group_global_id_128",
    "provider_group_hash",
    "provider_group_location_hash",
    "raw_provider_location_key",
    "json_pointer",
}
NO_DISPLAY_ADDRESS_FIELDS = {
    "address",
    "formatted_address",
    "address_key",
    "city",
    "state",
    "zip5",
    "zip_code",
    "postal_code",
    "lat",
    "long",
    "latitude",
    "longitude",
    "distance",
    "distance_miles",
    "zip_match_type",
    "coordinates",
    "google_maps_url",
    "google_map_url",
    "maps_url",
    "phone",
    "telephone",
    "telephone_number",
    "phone_number",
    "fax",
    "fax_number",
    "location_hash",
    "location_source",
    "location_confidence_code",
    "address_sources",
    "address_precision",
    "source_count",
    "multi_source_confirmed",
    "source_mask",
    "address_source_mask",
}
NO_DISPLAY_VERIFICATION_FIELDS = {
    "location_source",
    "location_confidence_code",
    "address_precision",
    "address_sources",
    "source_count",
    "multi_source_confirmed",
    "source_mask",
    "address_source_mask",
    "provider_directory_source_id",
    "provider_directory_location_resource_id",
    "provider_directory_location_name",
    "provider_directory_plan_context_matched",
    "provider_directory_network_name_matched",
    "provider_directory_network_context_present",
    "provider_directory_network_refs",
    "provider_directory_network_names",
    "provider_directory_network_matches",
    "provider_directory_insurance_plan_refs",
    "provider_directory_insurance_plan_matches",
    "provider_directory_match_type",
    "address_verification_evidence",
}


def _items_from_payload(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, dict):
        data = payload.get("data")
        if isinstance(data, dict) and isinstance(data.get("items"), list):
            return [item for item in data["items"] if isinstance(item, dict)]
        if isinstance(payload.get("items"), list):
            return [item for item in payload["items"] if isinstance(item, dict)]
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    return []


def _source_file_version_ids_from_value(value: Any, found: set[str]) -> None:
    if isinstance(value, dict):
        source_file_version_id = str(value.get("source_file_version_id") or "").strip()
        if source_file_version_id:
            found.add(source_file_version_id)
        for child in value.values():
            _source_file_version_ids_from_value(child, found)
    elif isinstance(value, list):
        for child in value:
            _source_file_version_ids_from_value(child, found)


def source_file_version_ids_from_ptg_payload(payload: Any) -> list[str]:
    """Extract retained source-file IDs from PTG API source_trace payloads."""

    found: set[str] = set()
    _source_file_version_ids_from_value(payload, found)
    return sorted(found)


def _source_file_version_ids_from_item(item: dict[str, Any]) -> list[str]:
    found: set[str] = set()
    _source_file_version_ids_from_value(item, found)
    return sorted(found)


def _network_names_from_item(item: dict[str, Any]) -> list[str]:
    names = item.get("network_names")
    if not isinstance(names, list):
        return []
    return sorted({str(name).strip() for name in names if str(name or "").strip()})


def _canonical_network_name(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(value or "").lower())


def _nonempty(source: dict[str, Any], *keys: str) -> bool:
    return any(source.get(key) not in (None, "", [], {}) for key in keys)


def _usable_address_source(source: dict[str, Any]) -> bool:
    if _nonempty(source, "first_line", "address_line_1", "street", "street_address"):
        return True
    return _nonempty(source, "city", "city_name") and _nonempty(
        source,
        "state",
        "state_name",
        "postal_code",
        "zip5",
    )


def _usable_address(item: dict[str, Any]) -> bool:
    address = item.get("address")
    return (isinstance(address, dict) and _usable_address_source(address)) or _usable_address_source(item)


def _issue(message: str, *, index: int | None, severity: str = "error") -> dict[str, Any]:
    return {"severity": severity, "item_index": index, "message": message}


def _nonempty_no_display_fields(item: dict[str, Any]) -> list[str]:
    return sorted(
        key
        for key in NO_DISPLAY_ADDRESS_FIELDS
        if item.get(key) not in (None, "", [], {})
    )


def _nonempty_no_display_verification_fields(verification: dict[str, Any]) -> list[str]:
    return sorted(
        key
        for key in NO_DISPLAY_VERIFICATION_FIELDS
        if verification.get(key) not in (None, "", [], {})
    )


def _normalized_values(*values: Any) -> set[str]:
    return {
        str(value or "").strip().lower().replace("-", "_")
        for value in values
        if str(value or "").strip()
    }


def _has_direct_payer_location_evidence(
    verification: dict[str, Any],
    normalized_address_sources: set[str],
) -> bool:
    evidence = verification.get("address_verification_evidence")
    evidence_payload = evidence if isinstance(evidence, dict) else {}
    markers = set(normalized_address_sources)
    markers.update(
        _normalized_values(
            verification.get("location_source"),
            verification.get("location_confidence_code"),
            evidence_payload.get("source"),
            evidence_payload.get("location_source"),
            evidence_payload.get("match_type"),
        )
    )
    return bool(markers & DIRECT_PAYER_ADDRESS_SOURCES)


def _has_materialized_payer_location_record_evidence(verification: dict[str, Any]) -> bool:
    evidence = verification.get("address_verification_evidence")
    if not isinstance(evidence, dict):
        return False
    return any(evidence.get(key) not in (None, "", [], {}) for key in DIRECT_PAYER_LOCATION_RECORD_KEYS)


def _has_concrete_provider_directory_network_match(value: Any) -> bool:
    if not isinstance(value, list):
        return False
    for item in value:
        if not isinstance(item, dict):
            continue
        if (
            str(item.get("ptg_network_name") or "").strip()
            and str(item.get("provider_directory_network_name") or "").strip()
        ):
            return True
    return False


def _network_match_ptg_name_keys(value: Any) -> set[str]:
    if not isinstance(value, list):
        return set()
    return {
        _canonical_network_name(entry.get("ptg_network_name"))
        for entry in value
        if isinstance(entry, dict) and _canonical_network_name(entry.get("ptg_network_name"))
    }


def _network_matches_align_with_served_network_names(
    item: dict[str, Any],
    *match_lists: Any,
) -> bool:
    served_network_keys = {_canonical_network_name(name) for name in _network_names_from_item(item)}
    served_network_keys.discard("")
    if not served_network_keys:
        return False
    matched_keys: set[str] = set()
    for match_list in match_lists:
        matched_keys.update(_network_match_ptg_name_keys(match_list))
    matched_keys.discard("")
    return bool(matched_keys) and matched_keys.issubset(served_network_keys)


def _network_match_entries_issue(value: Any, *, field_name: str, index: int | None) -> dict[str, Any] | None:
    if value is None:
        return None
    if not isinstance(value, list):
        return _issue(f"{field_name} must be a list when present", index=index)
    for entry in value:
        if not isinstance(entry, dict):
            return _issue(
                f"{field_name} entries must include ptg_network_name and provider_directory_network_name",
                index=index,
            )
        if not (
            str(entry.get("ptg_network_name") or "").strip()
            and str(entry.get("provider_directory_network_name") or "").strip()
        ):
            return _issue(
                f"{field_name} entries must include ptg_network_name and provider_directory_network_name",
                index=index,
            )
    return None


def _is_string_list(value: Any) -> bool:
    return isinstance(value, list) and all(isinstance(item, str) for item in value)


def _has_provider_directory_network_context_evidence(verification: dict[str, Any]) -> bool:
    evidence = verification.get("address_verification_evidence")
    evidence_payload = evidence if isinstance(evidence, dict) else {}
    matched_on = str(evidence_payload.get("matched_on") or "").strip().lower()
    network_matches = verification.get("provider_directory_network_matches")
    evidence_network_matches = evidence_payload.get("network_name_matches")
    return (
        verification.get("provider_directory_network_name_matched") is True
        and matched_on.endswith("_network_name")
        and (
            _has_concrete_provider_directory_network_match(network_matches)
            or _has_concrete_provider_directory_network_match(evidence_network_matches)
        )
    )


def validate_ptg_price_address_item(
    item: dict[str, Any],
    *,
    index: int = 0,
    require_displayed_address: bool = True,
    require_network_bound_address: bool = False,
) -> list[dict[str, Any]]:
    issues: list[dict[str, Any]] = []
    verification = item.get("address_verification")
    has_address = _usable_address(item)
    if not isinstance(verification, dict) or not verification:
        issues.append(_issue("PTG price row is missing address_verification", index=index))
        return issues

    rate_binding = str(verification.get("rate_network_binding") or "").strip()
    address_binding = str(verification.get("address_network_binding") or "").strip()
    evidence_level = str(verification.get("address_evidence_level") or "").strip()
    requires_confirmation = verification.get("requires_location_confirmation")
    displayed_address_present = verification.get("displayed_address_present")
    network_bound_address = verification.get("network_bound_address")
    plan_context = verification.get("provider_directory_plan_context_matched")
    network_context_present = verification.get("provider_directory_network_context_present")
    network_name_matched = verification.get("provider_directory_network_name_matched")
    address_sources = verification.get("address_sources")
    provider_directory_network_refs = verification.get("provider_directory_network_refs")
    provider_directory_network_names = verification.get("provider_directory_network_names")
    provider_directory_network_matches = verification.get("provider_directory_network_matches")
    provider_directory_insurance_plan_refs = verification.get("provider_directory_insurance_plan_refs")
    provider_directory_insurance_plan_matches = verification.get("provider_directory_insurance_plan_matches")
    evidence = verification.get("address_verification_evidence")
    evidence_payload = evidence if isinstance(evidence, dict) else {}
    evidence_network_name_matches = evidence_payload.get("network_name_matches")
    normalized_address_sources = {
        str(value or "").strip().lower().replace("-", "_")
        for value in address_sources
        if str(value or "").strip()
    } if isinstance(address_sources, list) else set()

    if rate_binding not in ALLOWED_RATE_BINDINGS:
        issues.append(_issue(f"invalid rate_network_binding={rate_binding!r}", index=index))
    if address_binding not in ALLOWED_ADDRESS_BINDINGS:
        issues.append(_issue(f"invalid address_network_binding={address_binding!r}", index=index))
    if evidence_level not in ALLOWED_ADDRESS_EVIDENCE_LEVELS:
        issues.append(_issue(f"invalid address_evidence_level={evidence_level!r}", index=index))
    elif evidence_level == "unknown" and require_displayed_address:
        issues.append(_issue("address_evidence_level=unknown is not sufficient for a displayed PTG address", index=index))
    if not isinstance(requires_confirmation, bool):
        issues.append(_issue("requires_location_confirmation must be boolean", index=index))
    if not isinstance(displayed_address_present, bool):
        issues.append(_issue("displayed_address_present must be boolean", index=index))
    elif displayed_address_present is False and require_displayed_address:
        issues.append(_issue("displayed_address_present=false", index=index))
    elif displayed_address_present is False and has_address:
        issues.append(_issue("displayed_address_present=false but usable address fields are present", index=index))
    if displayed_address_present is False:
        leaked_fields = _nonempty_no_display_fields(item)
        if leaked_fields:
            issues.append(
                _issue(
                    "displayed_address_present=false but address/map/phone/location fields are present: "
                    + ", ".join(leaked_fields),
                    index=index,
                )
            )
        leaked_verification_fields = _nonempty_no_display_verification_fields(verification)
        if leaked_verification_fields:
            issues.append(
                _issue(
                    "displayed_address_present=false but address_verification includes address/location "
                    "evidence fields: " + ", ".join(leaked_verification_fields),
                    index=index,
                )
            )
    expected_network_bound = (
        displayed_address_present is True
        and address_binding in {"payer_confirmed_location", "payer_directory_corroborated_location"}
    )
    if network_bound_address is None:
        issues.append(_issue("network_bound_address is required", index=index))
    elif not isinstance(network_bound_address, bool):
        issues.append(_issue("network_bound_address must be boolean", index=index))
    elif isinstance(network_bound_address, bool) and network_bound_address is not expected_network_bound:
        issues.append(
            _issue(
                "network_bound_address must match address_network_binding and displayed_address_present",
                index=index,
            )
        )
    if plan_context is not None and not isinstance(plan_context, bool):
        issues.append(_issue("provider_directory_plan_context_matched must be boolean when present", index=index))
    if network_context_present is not None and not isinstance(network_context_present, bool):
        issues.append(_issue("provider_directory_network_context_present must be boolean when present", index=index))
    if network_name_matched is not None and not isinstance(network_name_matched, bool):
        issues.append(_issue("provider_directory_network_name_matched must be boolean when present", index=index))
    if address_sources is not None and not _is_string_list(address_sources):
        issues.append(_issue("address_sources must be a string list when present", index=index))
    if provider_directory_network_refs is not None and not _is_string_list(provider_directory_network_refs):
        issues.append(_issue("provider_directory_network_refs must be a string list when present", index=index))
    if provider_directory_network_names is not None and not _is_string_list(provider_directory_network_names):
        issues.append(_issue("provider_directory_network_names must be a string list when present", index=index))
    network_match_issue = _network_match_entries_issue(
        provider_directory_network_matches,
        field_name="provider_directory_network_matches",
        index=index,
    )
    if network_match_issue:
        issues.append(network_match_issue)
    evidence_network_match_issue = _network_match_entries_issue(
        evidence_network_name_matches,
        field_name="address_verification_evidence.network_name_matches",
        index=index,
    )
    if evidence_network_match_issue:
        issues.append(evidence_network_match_issue)
    if provider_directory_insurance_plan_refs is not None and not _is_string_list(provider_directory_insurance_plan_refs):
        issues.append(_issue("provider_directory_insurance_plan_refs must be a string list when present", index=index))
    if provider_directory_insurance_plan_matches is not None and not _is_string_list(provider_directory_insurance_plan_matches):
        issues.append(_issue("provider_directory_insurance_plan_matches must be a string list when present", index=index))
    if not has_address and require_displayed_address:
        issues.append(_issue("address_verification is present but no usable address fields are displayed", index=index))
    if (
        require_network_bound_address
        and displayed_address_present is True
        and not expected_network_bound
    ):
        issues.append(
            _issue(
                "displayed PTG address is not network-bound to the priced plan or network",
                index=index,
            )
        )
    if not require_displayed_address and displayed_address_present is False and not has_address:
        if address_binding != "inferred_from_provider_identity":
            issues.append(_issue("no-address PTG rows must keep address_network_binding='inferred_from_provider_identity'", index=index))
        if evidence_level != "unknown":
            issues.append(_issue("no-address PTG rows must use address_evidence_level='unknown'", index=index))
        if requires_confirmation is not True:
            issues.append(_issue("no-address PTG rows must require location confirmation", index=index))
    if normalized_address_sources & {"ptg", "tic", "tic_provider_group"} and address_binding != "payer_confirmed_location":
        issues.append(_issue("PTG/TiC may not appear in address_sources unless the address is payer-confirmed", index=index))

    if address_binding == "payer_confirmed_location":
        if evidence_level != "payer_confirmed_location":
            issues.append(_issue("payer-confirmed address must use payer_confirmed_location evidence", index=index))
        if requires_confirmation is not False:
            issues.append(_issue("payer-confirmed address must not require location confirmation", index=index))
        if not _has_direct_payer_location_evidence(verification, normalized_address_sources):
            issues.append(_issue("payer-confirmed address must include direct PTG/TiC payer-location evidence", index=index))
        if not _has_materialized_payer_location_record_evidence(verification):
            issues.append(_issue("payer-confirmed address must include materialized PTG/TiC source record evidence", index=index))
        if not _source_file_version_ids_from_item(item):
            issues.append(
                _issue(
                    "payer-confirmed address must include source_trace.source_file_version_id for raw TiC verification",
                    index=index,
                )
            )
    if address_binding == "payer_directory_corroborated_location":
        if evidence_level != "payer_directory_network_location":
            issues.append(_issue("payer-directory context match must use payer_directory_network_location evidence", index=index))
        if requires_confirmation is not False:
            issues.append(_issue("payer-directory context match must not require location confirmation", index=index))
        if plan_context is not True and not _has_provider_directory_network_context_evidence(verification):
            issues.append(_issue("payer-directory context match must expose plan context or network-name proof", index=index))
        elif plan_context is not True and not _network_matches_align_with_served_network_names(
            item,
            provider_directory_network_matches,
            evidence_network_name_matches,
        ):
            issues.append(
                _issue(
                    "payer-directory network-name proof must match served row network_names",
                    index=index,
                )
            )
    if evidence_level == "provider_directory_address":
        if address_binding != "inferred_from_provider_identity":
            issues.append(_issue("provider_directory_address must stay inferred_from_provider_identity", index=index))
        if requires_confirmation is not True:
            issues.append(_issue("provider_directory_address must require location confirmation", index=index))
    return issues


def summarize_ptg_price_address_payload(
    payload: Any,
    *,
    require_displayed_address: bool = True,
    require_network_names: bool = False,
    require_source_file_version_id: bool = False,
    require_network_bound_address: bool = False,
) -> dict[str, Any]:
    require_network_names = require_network_names or require_network_bound_address
    require_source_file_version_id = require_source_file_version_id or require_network_bound_address
    items = _items_from_payload(payload)
    issues: list[dict[str, Any]] = []
    binding_counts: dict[str, int] = {}
    evidence_counts: dict[str, int] = {}
    network_name_values: set[str] = set()
    displayed_address_rows = 0
    verification_rows = 0
    network_name_rows = 0
    source_trace_rows = 0
    source_file_version_id_rows = 0
    network_bound_address_rows = 0
    if not items:
        issues.append(_issue("no PTG price rows found", index=None))
    for index, item in enumerate(items):
        if _usable_address(item):
            displayed_address_rows += 1
        network_names = _network_names_from_item(item)
        if network_names:
            network_name_rows += 1
            network_name_values.update(network_names)
        if isinstance(item.get("source_trace"), list) and item["source_trace"]:
            source_trace_rows += 1
        if _source_file_version_ids_from_item(item):
            source_file_version_id_rows += 1
        verification = item.get("address_verification")
        if isinstance(verification, dict) and verification:
            verification_rows += 1
            if verification.get("network_bound_address") is True:
                network_bound_address_rows += 1
            binding = str(verification.get("address_network_binding") or "missing")
            evidence = str(verification.get("address_evidence_level") or "missing")
            binding_counts[binding] = binding_counts.get(binding, 0) + 1
            evidence_counts[evidence] = evidence_counts.get(evidence, 0) + 1
        issues.extend(
            validate_ptg_price_address_item(
                item,
                index=index,
                require_displayed_address=require_displayed_address,
                require_network_bound_address=require_network_bound_address,
            )
        )
    if require_network_names and items and network_name_rows == 0:
        issues.append(_issue("no PTG price rows include network_names", index=None))
    elif require_network_names and items and network_name_rows < len(items):
        missing_indexes = [
            index
            for index, item in enumerate(items)
            if not _network_names_from_item(item)
        ]
        issues.append(
            _issue(
                "some PTG price rows do not include network_names: "
                + ", ".join(str(index) for index in missing_indexes[:10]),
                index=None,
            )
        )
    if require_source_file_version_id and items and source_file_version_id_rows == 0:
        issues.append(_issue("no PTG price rows include source_trace.source_file_version_id", index=None))
    elif require_source_file_version_id and items and source_file_version_id_rows < len(items):
        missing_indexes = [
            index
            for index, item in enumerate(items)
            if not _source_file_version_ids_from_item(item)
        ]
        issues.append(
            _issue(
                "some PTG price rows do not include source_trace.source_file_version_id: "
                + ", ".join(str(index) for index in missing_indexes[:10]),
                index=None,
            )
        )
    return {
        "ok": not any(issue["severity"] == "error" for issue in issues),
        "item_count": len(items),
        "displayed_address_rows": displayed_address_rows,
        "address_verification_rows": verification_rows,
        "network_name_rows": network_name_rows,
        "source_trace_rows": source_trace_rows,
        "source_file_version_id_rows": source_file_version_id_rows,
        "network_bound_address_rows": network_bound_address_rows,
        "network_name_values": sorted(network_name_values),
        "address_network_binding_counts": dict(sorted(binding_counts.items())),
        "address_evidence_level_counts": dict(sorted(evidence_counts.items())),
        "issues": issues,
    }


def build_ptg_address_assurance_report(
    *,
    api_payload: Any | None = None,
    raw_artifact_paths: list[str | Path] | None = None,
    raw_artifact_source_file_version_ids_by_path: dict[str, str | list[str]] | None = None,
    max_samples: int = 5,
    require_displayed_address: bool = True,
    require_network_names: bool = False,
    require_source_file_version_id: bool = False,
    require_network_bound_address: bool = False,
) -> dict[str, Any]:
    raw_reports = []
    raw_source_map = raw_artifact_source_file_version_ids_by_path or {}
    for raw_path in raw_artifact_paths or []:
        raw_report = audit_tic_provider_location_evidence(
            raw_path,
            max_samples=max(max_samples, 0),
        )
        mapped_source_ids = raw_source_map.get(str(raw_path)) or raw_source_map.get(str(Path(raw_path)))
        if isinstance(mapped_source_ids, str):
            mapped_source_ids = [mapped_source_ids]
        if mapped_source_ids:
            raw_report["source_file_version_ids"] = [
                str(source_id).strip()
                for source_id in mapped_source_ids
                if str(source_id or "").strip()
            ]
        raw_reports.append(raw_report)
    api_report = (
        summarize_ptg_price_address_payload(
            api_payload,
            require_displayed_address=require_displayed_address,
            require_network_names=require_network_names,
            require_source_file_version_id=require_source_file_version_id,
            require_network_bound_address=require_network_bound_address,
        )
        if api_payload is not None
        else None
    )
    api_source_file_version_ids = (
        source_file_version_ids_from_ptg_payload(api_payload)
        if api_payload is not None
        else []
    )
    raw_direct_location_present = any(report.get("direct_location_fields_present") for report in raw_reports)
    raw_direct_displayable_location_present = any(
        report.get("direct_displayable_location_fields_present") for report in raw_reports
    )
    issues = []
    raw_displayable_by_source_file_version_id: dict[str, bool] = {}
    for raw_report in raw_reports:
        for source_file_version_id in raw_report.get("source_file_version_ids") or []:
            raw_displayable_by_source_file_version_id[str(source_file_version_id)] = bool(
                raw_report.get("direct_displayable_location_fields_present")
            )
    if api_payload is not None and raw_reports:
        for index, item in enumerate(_items_from_payload(api_payload)):
            verification = item.get("address_verification")
            if not isinstance(verification, dict):
                continue
            if verification.get("address_network_binding") != "payer_confirmed_location":
                continue
            item_source_ids = _source_file_version_ids_from_item(item)
            if not item_source_ids:
                issues.append(
                    _issue(
                        "payer-confirmed address must include source_file_version_id in source_trace for raw TiC verification",
                        index=index,
                    )
                )
                continue
            missing_source_ids = [
                source_id
                for source_id in item_source_ids
                if source_id not in raw_displayable_by_source_file_version_id
            ]
            if missing_source_ids:
                issues.append(
                    _issue(
                        "payer-confirmed address source_file_version_id was not resolved to an audited raw artifact: "
                        + ", ".join(missing_source_ids),
                        index=index,
                    )
                )
                continue
            weak_source_ids = [
                source_id
                for source_id in item_source_ids
                if not raw_displayable_by_source_file_version_id.get(source_id)
            ]
            if weak_source_ids:
                issues.append(
                    _issue(
                        "payer-confirmed address source raw TiC artifact has no direct displayable provider location fields: "
                        + ", ".join(weak_source_ids),
                        index=index,
                    )
                )
    if api_payload is None and not raw_reports:
        issues.append(
            {
                "severity": "error",
                "message": "no API payload or raw artifacts supplied",
            }
        )
    return {
        "ok": bool((api_report is None or api_report["ok"]) and not any(issue["severity"] == "error" for issue in issues)),
        "api_payload": api_report,
        "api_source_file_version_ids": api_source_file_version_ids,
        "raw_artifacts": raw_reports,
        "raw_direct_location_fields_present": raw_direct_location_present,
        "raw_direct_displayable_location_fields_present": raw_direct_displayable_location_present,
        "issues": issues,
        "notes": [
            "network_names identify the priced TiC/PTG network; they do not prove the displayed street address",
            "strict displayed-address assurance requires usable address fields; schema-only mode accepts explicit no-address rows",
            "raw direct location fields require an explicit materialization path before serving as payer_confirmed_location",
        ],
    }
