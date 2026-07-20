# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Immutable, source-local Provider Directory outcome summaries."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any


SOURCE_SUMMARY_CONTRACT_ID = "healthporta.provider-directory.source-summary.v1"
SOURCE_SUMMARY_CONTRACT_VERSION = 1
SOURCE_SUMMARY_METADATA_KEY = "source_summary_v1"
SOURCE_SUMMARY_FHIR_SEMANTIC_CONTRACT_ID = (
    "healthporta.provider-directory.fhir-normalized-resource.v1"
)
SOURCE_SUMMARY_UHC_SEMANTIC_CONTRACT_ID = (
    "healthporta.uhc.semantic-facts.v1"
)
SOURCE_SUMMARY_UHC_SELECTED_RESOURCES = (
    "InsurancePlan",
    "Location",
    "Organization",
    "OrganizationAffiliation",
    "Practitioner",
    "PractitionerRole",
)

SOURCE_SUMMARY_COUNT_FIELDS = (
    "raw_provider_records",
    "raw_plan_records",
    "raw_individual_records",
    "raw_facility_records",
    "raw_address_rows",
    "raw_provider_plan_rows",
    "raw_formulary_entries",
    "named_facility_records",
    "facility_type_values",
    "dated_records",
    "total_resources",
    "distinct_npis",
    "duplicate_npi_groups",
    "conflicting_npi_groups",
    "individual_practitioners",
    "facility_organizations",
    "organization_resources",
    "normalized_addresses",
    "address_records",
    "addressed_locations",
    "geocoded_locations",
    "provider_memberships",
    "practitioner_role_resources",
    "network_plan_links",
    "organization_affiliation_links",
    "accepting_newpt_records",
    "accepting_nopt_records",
    "accepting_null_records",
    "invalid_npi_count",
    "valid_phone_count",
    "invalid_phone_count",
    "multi_address_provider_records",
    "plan_year_rows",
    "provider_file_count",
    "plan_file_count",
    "membership_plan_key_count",
    "detail_plan_key_count",
    "matched_plan_key_count",
    "missing_plan_detail_count",
    "orphan_plan_detail_count",
)
SOURCE_SUMMARY_COUNT_MAP_FIELDS = (
    "resource_counts",
    "conflict_counts",
    "rejected_counts",
    "unknown_field_counts",
    "intentional_drop_counts",
)
SOURCE_SUMMARY_HASH_MAP_FIELDS = ("resource_hashes",)
SOURCE_SUMMARY_OPTIONAL_TEXT_FIELDS = (
    "input_set_sha256",
    "layout_set_sha256",
    "semantic_contract_id",
    "encoder_digest",
)
SOURCE_SUMMARY_REQUIRED_FIELDS = frozenset(
    {
        "contract_id",
        "contract_version",
        "complete",
        "source_ids",
        "endpoint_id",
        "dataset_id",
        "acquisition_root_run_id",
        "selected_resources",
        "dataset_hash",
        "total_resources",
        "resource_counts",
        "resource_hashes",
    }
)
SOURCE_SUMMARY_ALLOWED_FIELDS = frozenset(
    {
        *SOURCE_SUMMARY_REQUIRED_FIELDS,
        *SOURCE_SUMMARY_COUNT_FIELDS,
        *SOURCE_SUMMARY_COUNT_MAP_FIELDS,
        *SOURCE_SUMMARY_HASH_MAP_FIELDS,
        *SOURCE_SUMMARY_OPTIONAL_TEXT_FIELDS,
        "summary_sha256",
    }
)
SOURCE_SUMMARY_OUTCOME_COUNT_FIELDS = (
    "distinct_npis",
    "individual_practitioners",
    "organization_resources",
    "address_records",
    "addressed_locations",
    "geocoded_locations",
    "practitioner_role_resources",
    "network_plan_links",
    "organization_affiliation_links",
)
SOURCE_SUMMARY_UHC_OUTCOME_COUNT_FIELDS = (
    "raw_provider_records",
    "raw_plan_records",
    "raw_individual_records",
    "raw_facility_records",
    "raw_address_rows",
    "raw_provider_plan_rows",
    "raw_formulary_entries",
    "named_facility_records",
    "facility_type_values",
    "dated_records",
    "distinct_npis",
    "duplicate_npi_groups",
    "conflicting_npi_groups",
    "accepting_newpt_records",
    "accepting_nopt_records",
    "accepting_null_records",
    "invalid_npi_count",
    "valid_phone_count",
    "invalid_phone_count",
    "multi_address_provider_records",
    "plan_year_rows",
    "provider_file_count",
    "plan_file_count",
    "membership_plan_key_count",
    "detail_plan_key_count",
    "matched_plan_key_count",
    "missing_plan_detail_count",
    "orphan_plan_detail_count",
)
SOURCE_SUMMARY_SEMANTIC_COUNT_FIELDS = {
    SOURCE_SUMMARY_FHIR_SEMANTIC_CONTRACT_ID: (
        SOURCE_SUMMARY_OUTCOME_COUNT_FIELDS
    ),
    SOURCE_SUMMARY_UHC_SEMANTIC_CONTRACT_ID: (
        SOURCE_SUMMARY_UHC_OUTCOME_COUNT_FIELDS
    ),
}
SOURCE_SUMMARY_SEMANTIC_COUNT_MAP_FIELDS = {
    SOURCE_SUMMARY_FHIR_SEMANTIC_CONTRACT_ID: (
        "intentional_drop_counts",
        "unknown_field_counts",
    ),
    SOURCE_SUMMARY_UHC_SEMANTIC_CONTRACT_ID: (
        "conflict_counts",
        "intentional_drop_counts",
        "unknown_field_counts",
    ),
}
SOURCE_SUMMARY_SEMANTIC_OUTCOME_FIELDS = {
    SOURCE_SUMMARY_FHIR_SEMANTIC_CONTRACT_ID: (
        SOURCE_SUMMARY_OUTCOME_COUNT_FIELDS
    ),
    SOURCE_SUMMARY_UHC_SEMANTIC_CONTRACT_ID: (
        *SOURCE_SUMMARY_UHC_OUTCOME_COUNT_FIELDS,
        "conflict_counts",
        "intentional_drop_counts",
        "unknown_field_counts",
    ),
}
SOURCE_SUMMARY_UHC_REQUIRED_IDENTITY_FIELDS = (
    "input_set_sha256",
    "layout_set_sha256",
    "encoder_digest",
)


class ProviderDirectorySourceSummaryError(ValueError):
    """Reject a summary that is not exact, canonical, and dataset-bound."""


@dataclass(frozen=True)
class ProviderDirectorySourceSummaryBinding:
    """Bind one source summary to its immutable dataset lineage."""

    dataset_id: str
    endpoint_id: str
    acquisition_root_run_id: str
    dataset_hash: str


def _text(raw_value: Any, field_name: str) -> str:
    if (
        not isinstance(raw_value, str)
        or not raw_value
        or raw_value != raw_value.strip()
    ):
        raise ProviderDirectorySourceSummaryError(
            f"provider_directory_source_summary_{field_name}_invalid"
        )
    return raw_value


def _string_list(raw_values: Any, field_name: str) -> list[str]:
    if (
        not isinstance(raw_values, Sequence)
        or isinstance(raw_values, (str, bytes, bytearray))
        or not raw_values
    ):
        raise ProviderDirectorySourceSummaryError(
            f"provider_directory_source_summary_{field_name}_invalid"
        )
    normalized_list = [_text(item, field_name) for item in raw_values]
    if normalized_list != sorted(set(normalized_list)):
        raise ProviderDirectorySourceSummaryError(
            f"provider_directory_source_summary_{field_name}_not_canonical"
        )
    return normalized_list


def _count(raw_value: Any, field_name: str) -> int:
    if (
        isinstance(raw_value, bool)
        or not isinstance(raw_value, int)
        or raw_value < 0
    ):
        raise ProviderDirectorySourceSummaryError(
            f"provider_directory_source_summary_{field_name}_invalid"
        )
    return raw_value


def _count_map(
    raw_count_by_name: Any,
    field_name: str,
) -> dict[str, int]:
    if not isinstance(raw_count_by_name, Mapping):
        raise ProviderDirectorySourceSummaryError(
            f"provider_directory_source_summary_{field_name}_invalid"
        )
    normalized_count_by_name = {
        _text(key, field_name): _count(item, field_name)
        for key, item in raw_count_by_name.items()
    }
    return dict(sorted(normalized_count_by_name.items()))


def _sha256(raw_value: Any, field_name: str) -> str:
    normalized_hash = _text(raw_value, field_name)
    if len(normalized_hash) != 64 or any(
        character not in "0123456789abcdef"
        for character in normalized_hash
    ):
        raise ProviderDirectorySourceSummaryError(
            f"provider_directory_source_summary_{field_name}_invalid"
        )
    return normalized_hash


def _hash_map(
    raw_hash_by_name: Any,
    field_name: str,
) -> dict[str, str]:
    if not isinstance(raw_hash_by_name, Mapping):
        raise ProviderDirectorySourceSummaryError(
            f"provider_directory_source_summary_{field_name}_invalid"
        )
    normalized_hash_by_name = {
        _text(key, field_name): _sha256(item, field_name)
        for key, item in raw_hash_by_name.items()
    }
    return dict(sorted(normalized_hash_by_name.items()))


def canonical_source_summary_bytes(summary_map: Mapping[str, Any]) -> bytes:
    """Encode the proof deterministically without its self-hash."""
    payload_by_field = {
        key: field_value
        for key, field_value in summary_map.items()
        if key != "summary_sha256"
    }
    return json.dumps(
        payload_by_field,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def source_summary_sha256(summary_map: Mapping[str, Any]) -> str:
    """Hash one canonical source-summary envelope."""
    return hashlib.sha256(
        canonical_source_summary_bytes(summary_map)
    ).hexdigest()


def build_source_summary(
    *,
    binding: ProviderDirectorySourceSummaryBinding,
    source_ids: Sequence[str],
    selected_resources: Sequence[str],
    count_by_resource: Mapping[str, int],
    hash_by_resource: Mapping[str, str],
    count_by_field: Mapping[str, int] | None = None,
    count_by_category: Mapping[str, Mapping[str, int]] | None = None,
    identity_by_field: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    """Build and self-hash one canonical, immutable dataset summary."""
    summary_map: dict[str, Any] = {
        "contract_id": SOURCE_SUMMARY_CONTRACT_ID,
        "contract_version": SOURCE_SUMMARY_CONTRACT_VERSION,
        "complete": True,
        "source_ids": sorted(source_ids),
        "endpoint_id": binding.endpoint_id,
        "dataset_id": binding.dataset_id,
        "acquisition_root_run_id": binding.acquisition_root_run_id,
        "selected_resources": sorted(selected_resources),
        "dataset_hash": binding.dataset_hash,
        "total_resources": sum(count_by_resource.values()),
        "resource_counts": dict(sorted(count_by_resource.items())),
        "resource_hashes": dict(sorted(hash_by_resource.items())),
    }
    for field_name, field_value in sorted((identity_by_field or {}).items()):
        if field_name not in SOURCE_SUMMARY_OPTIONAL_TEXT_FIELDS:
            raise ProviderDirectorySourceSummaryError(
                "provider_directory_source_summary_identity_field_unsupported"
            )
        summary_map[field_name] = field_value
    for field_name, field_value in sorted((count_by_field or {}).items()):
        if field_name not in SOURCE_SUMMARY_COUNT_FIELDS:
            raise ProviderDirectorySourceSummaryError(
                "provider_directory_source_summary_count_field_unsupported"
            )
        summary_map[field_name] = field_value
    for field_name, count_by_name in sorted((count_by_category or {}).items()):
        if field_name not in SOURCE_SUMMARY_COUNT_MAP_FIELDS:
            raise ProviderDirectorySourceSummaryError(
                "provider_directory_source_summary_count_map_field_unsupported"
            )
        summary_map[field_name] = dict(sorted(count_by_name.items()))
    validated_summary_map = validate_source_summary(
        summary_map,
        require_hash=False,
    )
    validated_summary_map["summary_sha256"] = source_summary_sha256(
        validated_summary_map
    )
    return validated_summary_map


def _source_summary_map(raw_summary: Any) -> dict[str, Any]:
    if not isinstance(raw_summary, Mapping):
        raise ProviderDirectorySourceSummaryError(
            "provider_directory_source_summary_invalid"
        )
    summary_map = dict(raw_summary)
    unknown_fields = set(summary_map) - SOURCE_SUMMARY_ALLOWED_FIELDS
    if unknown_fields:
        raise ProviderDirectorySourceSummaryError(
            "provider_directory_source_summary_field_unsupported"
        )
    missing_fields = SOURCE_SUMMARY_REQUIRED_FIELDS - set(summary_map)
    if missing_fields:
        raise ProviderDirectorySourceSummaryError(
            "provider_directory_source_summary_field_missing"
        )
    contract_version = summary_map.get("contract_version")
    if (
        summary_map.get("contract_id") != SOURCE_SUMMARY_CONTRACT_ID
        or type(contract_version) is not int
        or contract_version != SOURCE_SUMMARY_CONTRACT_VERSION
        or summary_map.get("complete") is not True
    ):
        raise ProviderDirectorySourceSummaryError(
            "provider_directory_source_summary_contract_invalid"
        )
    return summary_map


def _normalize_source_summary_fields(
    summary_map: dict[str, Any],
) -> None:
    for field_name in (
        "dataset_id",
        "endpoint_id",
        "acquisition_root_run_id",
    ):
        summary_map[field_name] = _text(
            summary_map.get(field_name),
            field_name,
        )
    summary_map["dataset_hash"] = _sha256(
        summary_map.get("dataset_hash"), "dataset_hash"
    )
    summary_map["source_ids"] = _string_list(
        summary_map.get("source_ids"), "source_ids"
    )
    summary_map["selected_resources"] = _string_list(
        summary_map.get("selected_resources"), "selected_resources"
    )
    for field_name in SOURCE_SUMMARY_COUNT_FIELDS:
        if field_name in summary_map:
            summary_map[field_name] = _count(
                summary_map[field_name],
                field_name,
            )
    for field_name in SOURCE_SUMMARY_COUNT_MAP_FIELDS:
        if field_name in summary_map:
            summary_map[field_name] = _count_map(
                summary_map[field_name],
                field_name,
            )
    summary_map["resource_hashes"] = _hash_map(
        summary_map["resource_hashes"],
        "resource_hashes",
    )


def _validate_source_summary_resource_proof(
    summary_map: dict[str, Any],
) -> None:
    if set(summary_map["resource_counts"]) != set(
        summary_map["resource_hashes"]
    ):
        raise ProviderDirectorySourceSummaryError(
            "provider_directory_source_summary_resource_proof_mismatch"
        )
    if set(summary_map["selected_resources"]) - set(
        summary_map["resource_counts"]
    ):
        raise ProviderDirectorySourceSummaryError(
            "provider_directory_source_summary_selected_resources_missing"
        )
    if summary_map.get("total_resources") != sum(
        summary_map["resource_counts"].values()
    ):
        raise ProviderDirectorySourceSummaryError(
            "provider_directory_source_summary_total_mismatch"
        )


def _normalize_source_summary_optional_identity(
    summary_map: dict[str, Any],
) -> None:
    for field_name in SOURCE_SUMMARY_OPTIONAL_TEXT_FIELDS:
        if field_name not in summary_map:
            continue
        if field_name.endswith("sha256") or field_name == "encoder_digest":
            summary_map[field_name] = _sha256(
                summary_map[field_name],
                field_name,
            )
        else:
            summary_map[field_name] = _text(
                summary_map[field_name],
                field_name,
            )


def _validate_source_summary_hash(
    summary_map: dict[str, Any],
    *,
    require_hash: bool,
) -> None:
    supplied_hash = summary_map.get("summary_sha256")
    if supplied_hash is None and require_hash:
        raise ProviderDirectorySourceSummaryError(
            "provider_directory_source_summary_hash_missing"
        )
    if supplied_hash is not None:
        supplied_hash = _sha256(supplied_hash, "summary_sha256")
        if supplied_hash != source_summary_sha256(summary_map):
            raise ProviderDirectorySourceSummaryError(
                "provider_directory_source_summary_hash_mismatch"
            )
        summary_map["summary_sha256"] = supplied_hash


def _validate_expected_source_summary_fields(
    summary_map: dict[str, Any],
    expected_by_field: Mapping[str, Any] | None,
) -> None:
    for field_name, expected_value in (expected_by_field or {}).items():
        if summary_map.get(field_name) != expected_value:
            raise ProviderDirectorySourceSummaryError(
                f"provider_directory_source_summary_{field_name}_mismatch"
            )


def validate_source_summary(
    raw_summary: Any,
    *,
    expected_by_field: Mapping[str, Any] | None = None,
    require_hash: bool = True,
) -> dict[str, Any]:
    """Validate canonical types, self-hash, totals, and optional identity."""
    summary_map = _source_summary_map(raw_summary)
    _normalize_source_summary_fields(summary_map)
    _validate_source_summary_resource_proof(summary_map)
    _normalize_source_summary_optional_identity(summary_map)
    _validate_source_summary_hash(summary_map, require_hash=require_hash)
    _validate_expected_source_summary_fields(
        summary_map,
        expected_by_field,
    )
    return summary_map


def validate_fhir_source_summary(
    raw_summary: Any,
    *,
    expected_by_field: Mapping[str, Any],
) -> dict[str, Any]:
    """Require the complete FHIR v1 metric set and semantic identity."""
    return validate_semantic_source_summary(
        raw_summary,
        expected_by_field=expected_by_field,
        expected_semantic_contract_id=(
            SOURCE_SUMMARY_FHIR_SEMANTIC_CONTRACT_ID
        ),
    )


def _validate_semantic_metric_fields(
    summary_map: dict[str, Any],
    semantic_contract_id: str,
) -> None:
    required_count_fields = set(
        SOURCE_SUMMARY_SEMANTIC_COUNT_FIELDS[semantic_contract_id]
    )
    required_count_map_fields = set(
        SOURCE_SUMMARY_SEMANTIC_COUNT_MAP_FIELDS[semantic_contract_id]
    )
    missing_fields = (
        required_count_fields | required_count_map_fields
    ) - set(summary_map)
    if semantic_contract_id == SOURCE_SUMMARY_UHC_SEMANTIC_CONTRACT_ID:
        missing_fields.update(
            set(SOURCE_SUMMARY_UHC_REQUIRED_IDENTITY_FIELDS)
            - set(summary_map)
        )
    supplied_count_fields = set(SOURCE_SUMMARY_COUNT_FIELDS) & set(summary_map)
    supplied_count_fields.discard("total_resources")
    supplied_count_map_fields = (
        set(SOURCE_SUMMARY_COUNT_MAP_FIELDS) & set(summary_map)
    )
    supplied_count_map_fields.discard("resource_counts")
    if missing_fields:
        raise ProviderDirectorySourceSummaryError(
            "provider_directory_source_summary_semantic_fields_missing"
        )
    if (
        supplied_count_fields != required_count_fields
        or supplied_count_map_fields != required_count_map_fields
    ):
        raise ProviderDirectorySourceSummaryError(
            "provider_directory_source_summary_semantic_fields_mismatch"
        )
    if semantic_contract_id == SOURCE_SUMMARY_UHC_SEMANTIC_CONTRACT_ID and (
        summary_map["unknown_field_counts"]
        or summary_map["intentional_drop_counts"]
    ):
        raise ProviderDirectorySourceSummaryError(
            "provider_directory_source_summary_uhc_unaccounted_fields"
        )
    if semantic_contract_id == SOURCE_SUMMARY_UHC_SEMANTIC_CONTRACT_ID and (
        tuple(summary_map["selected_resources"])
        != SOURCE_SUMMARY_UHC_SELECTED_RESOURCES
        or set(summary_map["resource_counts"])
        != set(SOURCE_SUMMARY_UHC_SELECTED_RESOURCES)
    ):
        raise ProviderDirectorySourceSummaryError(
            "provider_directory_source_summary_uhc_resource_scope_invalid"
        )
    if semantic_contract_id == SOURCE_SUMMARY_FHIR_SEMANTIC_CONTRACT_ID:
        _validate_fhir_semantic_relationships(summary_map)
    if semantic_contract_id == SOURCE_SUMMARY_UHC_SEMANTIC_CONTRACT_ID:
        _validate_uhc_semantic_relationships(summary_map)


def _validate_fhir_semantic_relationships(
    summary_map: dict[str, Any],
) -> None:
    """Bind direct FHIR metrics to their immutable resource counts."""
    resource_count_by_type = summary_map["resource_counts"]
    direct_metric_by_resource_type = {
        "Practitioner": "individual_practitioners",
        "Organization": "organization_resources",
        "PractitionerRole": "practitioner_role_resources",
    }
    direct_counts_match = all(
        summary_map[field_name]
        == resource_count_by_type.get(resource_type, 0)
        for resource_type, field_name in direct_metric_by_resource_type.items()
    )
    location_count = resource_count_by_type.get("Location", 0)
    location_counts_valid = all(
        summary_map[field_name] <= location_count
        for field_name in ("addressed_locations", "geocoded_locations")
    )
    if (
        not direct_counts_match
        or not location_counts_valid
        or summary_map["distinct_npis"] > summary_map["total_resources"]
    ):
        raise ProviderDirectorySourceSummaryError(
            "provider_directory_source_summary_fhir_metrics_inconsistent"
        )


def _validate_uhc_semantic_relationships(
    summary_map: dict[str, Any],
) -> None:
    """Enforce algebra guaranteed by the strict setwise UHC builder."""
    provider_count = summary_map["raw_provider_records"]
    membership_key_count = summary_map["membership_plan_key_count"]
    detail_key_count = summary_map["detail_plan_key_count"]
    matched_key_count = summary_map["matched_plan_key_count"]
    is_relationship_set_valid = (
        summary_map["raw_individual_records"]
        + summary_map["raw_facility_records"]
        == provider_count
        and summary_map["accepting_newpt_records"]
        + summary_map["accepting_nopt_records"]
        + summary_map["accepting_null_records"]
        == provider_count
        and summary_map["distinct_npis"] <= provider_count
        and provider_count
        >= summary_map["distinct_npis"]
        + summary_map["duplicate_npi_groups"]
        and summary_map["conflicting_npi_groups"]
        <= summary_map["duplicate_npi_groups"]
        and summary_map["multi_address_provider_records"] <= provider_count
        and summary_map["named_facility_records"]
        <= summary_map["raw_facility_records"]
        and summary_map["valid_phone_count"]
        + summary_map["invalid_phone_count"]
        <= summary_map["raw_address_rows"]
        and summary_map["invalid_npi_count"] == 0
        and summary_map["dated_records"]
        <= provider_count + summary_map["raw_plan_records"]
        and summary_map["plan_year_rows"]
        >= summary_map["raw_plan_records"]
        and membership_key_count <= summary_map["raw_provider_plan_rows"]
        and detail_key_count <= summary_map["raw_plan_records"]
        and matched_key_count <= min(membership_key_count, detail_key_count)
        and summary_map["missing_plan_detail_count"] + matched_key_count
        == membership_key_count
        and summary_map["orphan_plan_detail_count"] + matched_key_count
        == detail_key_count
        and summary_map["provider_file_count"] > 0
        and summary_map["plan_file_count"] > 0
    )
    conflict_bound = summary_map["conflicting_npi_groups"]
    if not is_relationship_set_valid or any(
        conflict_count > conflict_bound
        for conflict_count in summary_map["conflict_counts"].values()
    ):
        raise ProviderDirectorySourceSummaryError(
            "provider_directory_source_summary_uhc_metrics_inconsistent"
        )


def validate_semantic_source_summary(
    raw_summary: Any,
    *,
    expected_by_field: Mapping[str, Any],
    expected_semantic_contract_id: str | None = None,
) -> dict[str, Any]:
    """Validate and dispatch one known, complete semantic fact contract."""
    summary_map = validate_source_summary(
        raw_summary,
        expected_by_field=expected_by_field,
    )
    semantic_contract_id = summary_map.get("semantic_contract_id")
    if (
        semantic_contract_id not in SOURCE_SUMMARY_SEMANTIC_COUNT_FIELDS
        or (
            expected_semantic_contract_id is not None
            and semantic_contract_id != expected_semantic_contract_id
        )
    ):
        raise ProviderDirectorySourceSummaryError(
            "provider_directory_source_summary_semantic_contract_invalid"
        )
    _validate_semantic_metric_fields(summary_map, semantic_contract_id)
    return summary_map


def validated_source_summary_outcome(
    raw_summary: Any,
    *,
    expected_by_field: Mapping[str, Any],
    relation_count_by_field: Mapping[str, int],
) -> dict[str, Any] | None:
    """Return UI-safe summary counters only for one exact sealed dataset."""
    try:
        summary_map = validate_semantic_source_summary(
            raw_summary,
            expected_by_field=expected_by_field,
        )
    except ProviderDirectorySourceSummaryError:
        return None
    relation_fields = (
        "network_plan_links",
        "organization_affiliation_links",
    )
    if any(
        field_name in summary_map
        and (
            field_name not in relation_count_by_field
            or summary_map[field_name] != relation_count_by_field[field_name]
        )
        for field_name in relation_fields
    ):
        return None
    semantic_contract_id = summary_map["semantic_contract_id"]
    outcome_by_field = {"semantic_contract_id": semantic_contract_id}
    for field_name in SOURCE_SUMMARY_SEMANTIC_OUTCOME_FIELDS[
        semantic_contract_id
    ]:
        outcome_by_field[field_name] = summary_map[field_name]
    return outcome_by_field
