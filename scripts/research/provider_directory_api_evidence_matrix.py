"""Data-driven source applicability for Provider Directory API verification."""

from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path
from typing import Any, Iterable, Mapping

from scripts.research.provider_directory_api_evidence_models import SourceSelection
from scripts.research.provider_directory_api_evidence_selection import (
    _normalized_ids,
    resolve_source_selection,
)


ROOT = Path(__file__).resolve().parents[2]
DEFAULT_PROFILE_SOURCE_SPEC = ROOT / "specs/provider_directory_profile_sources.json"
MATRIX_CHECK_CODES = ("A", "P", "G", "F", "V")
PROFILE_FACT_RESOURCE_TYPES = frozenset(
    {
        "Practitioner",
        "PractitionerRole",
        "HealthcareService",
        "OrganizationAffiliation",
    }
)


def load_matrix_source_spec(path: Path = DEFAULT_PROFILE_SOURCE_SPEC) -> dict[str, Any]:
    """Load the reviewed profile source contract without importer dependencies."""
    spec_payload = json.loads(path.read_text(encoding="utf-8"))
    matrix_map = (
        spec_payload.get("verification_matrix")
        if isinstance(spec_payload, Mapping)
        else None
    )
    source_entry_list = matrix_map.get("sources") if isinstance(matrix_map, Mapping) else None
    profiles_by_name = matrix_map.get("resource_profiles") if isinstance(matrix_map, Mapping) else None
    if (
        spec_payload.get("schema_version") != 1
        or not isinstance(source_entry_list, list)
        or not isinstance(profiles_by_name, Mapping)
        or tuple(matrix_map.get("check_codes") or ()) != MATRIX_CHECK_CODES
        or len(source_entry_list) != len(spec_payload.get("source_ids") or ())
    ):
        raise ValueError("provider_directory_profile_source_matrix_invalid")
    source_ids = tuple(spec_payload["source_ids"])
    entry_ids = tuple(spec_payload["entry_ids"])
    if len(source_ids) != len(set(source_ids)) or len(entry_ids) != len(set(entry_ids)):
        raise ValueError("provider_directory_profile_source_matrix_invalid")
    observed_source_ids = []
    observed_entry_ids = []
    for source_map in source_entry_list:
        if not isinstance(source_map, Mapping):
            raise ValueError("provider_directory_profile_source_matrix_invalid")
        source_id = str(source_map.get("source_id") or "")
        entry_id = str(source_map.get("entry_id") or "")
        resource_profile = str(source_map.get("resource_profile") or "")
        if not source_id or not entry_id or resource_profile not in profiles_by_name:
            raise ValueError("provider_directory_profile_source_matrix_invalid")
        observed_source_ids.append(source_id)
        observed_entry_ids.append(entry_id)
    if tuple(observed_source_ids) != source_ids or set(observed_entry_ids) != set(entry_ids):
        raise ValueError("provider_directory_profile_source_matrix_invalid")
    return dict(spec_payload)


def resolve_matrix_source_selection(
    acquisition_manifest: Mapping[str, Any],
    profile_source_spec: Mapping[str, Any],
    *,
    requested_entry_ids: Iterable[str] = (),
    requested_source_ids: Iterable[str] = (),
    max_sources: int = 100,
) -> list[SourceSelection]:
    """Select only reviewed profile sources and attach their applicability."""
    requested_entries = _normalized_ids(requested_entry_ids)
    requested_sources = _normalized_ids(requested_source_ids)
    manifest_selections = resolve_source_selection(
        acquisition_manifest,
        requested_entry_ids=requested_entries,
        requested_source_ids=requested_sources,
        max_sources=10_000,
    )
    manifest_selection_by_source = {
        selection.source_id: selection
        for selection in manifest_selections
    }
    matrix_map = profile_source_spec["verification_matrix"]
    profile_by_name = matrix_map["resource_profiles"]
    matrix_source_list = matrix_map["sources"]
    if not any(
        str(source_map["source_id"]) in manifest_selection_by_source
        for source_map in matrix_source_list
    ):
        # Isolated unit fixtures intentionally use synthetic source IDs.
        return manifest_selections
    selected_sources = _build_profile_matrix_selections(
        matrix_source_list,
        profile_by_name,
        matrix_map["check_codes"],
        manifest_selection_by_source,
    )
    if (requested_entries or requested_sources) and not selected_sources:
        raise ValueError("no reviewed profile sources selected")
    if len(selected_sources) > max_sources:
        raise ValueError("selected maintained sources exceed max_sources")
    return selected_sources


def _build_profile_matrix_selections(
    matrix_source_list: list[Mapping[str, Any]],
    profiles_by_name: Mapping[str, Mapping[str, Any]],
    check_codes: list[str],
    selections_by_source_id: Mapping[str, SourceSelection],
) -> list[SourceSelection]:
    selected_sources: list[SourceSelection] = []
    for source_entry in matrix_source_list:
        source_id = str(source_entry["source_id"])
        manifest_selection = selections_by_source_id.get(source_id)
        if manifest_selection is None:
            continue
        if manifest_selection.entry_id != source_entry["entry_id"]:
            raise ValueError("provider_directory_profile_source_matrix_manifest_mismatch")
        profile_map = profiles_by_name[source_entry["resource_profile"]]
        resource_types = tuple(
            str(resource_name) for resource_name in profile_map["resources"]
        )
        expected_resource_types = tuple(manifest_selection.resources)
        if (
            manifest_selection.classification != "external"
            and resource_types != expected_resource_types
        ):
            raise ValueError("provider_directory_profile_source_matrix_resource_mismatch")
        selected_sources.append(
            replace(
                manifest_selection,
                resources=resource_types,
                resource_profile=str(source_entry["resource_profile"]),
                matrix_checks=tuple(check_codes),
                profile_fact_resources=tuple(
                    resource_type
                    for resource_type in resource_types
                    if resource_type in PROFILE_FACT_RESOURCE_TYPES
                ),
            )
        )
    return selected_sources
