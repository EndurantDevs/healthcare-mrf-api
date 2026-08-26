"""Validation helpers for operator-supplied Provider Directory results."""

from __future__ import annotations

import re
from typing import Any


SCAN_PROVIDER_DIRECTORY_BASE = "https://providerdirectory.scanhealthplan.com"
CARESOURCE_PROVIDER_DIRECTORY_BASE = (
    "https://orchestrateserver.caresource.careevolution.com/api/fhir/provider-directory"
)
LAST_UPDATED_PROOF_METRIC_NAMES = (
    "last_updated_unfiltered_pre",
    "last_updated_ranged_root_pre",
    "last_updated_exact_leaf_count_sum",
    "last_updated_pass1_unique",
    "last_updated_pass2_unique",
    "last_updated_staged_candidate_count",
    "last_updated_ranged_root_post",
    "last_updated_unfiltered_post",
)
CARESOURCE_PROOF_METRIC_NAMES = (
    "caresource_opaque_cursor_pre_count",
    "caresource_opaque_cursor_processed_rows",
    "caresource_opaque_cursor_unique_candidate_rows",
    "caresource_opaque_cursor_post_count",
)
RUN_ID_PATTERN = re.compile(r"^run_[0-9a-f]{32}$")
RUN_LINEAGE_PARAM_FIELDS = {
    "provider_directory_endpoint_scope",
    "provider_directory_pagination_attempt",
    "provider_directory_pagination_root_run_id",
    "retry_of_run_id",
}
PROBE_OMITTED_KEYS = {
    "resources",
    "resource_limit",
    "resource_deadline_seconds",
    "linked_resource_limit",
    "linked_resource_deadline_seconds",
    "page_limit",
    "page_count",
    "stream_batch_size",
    "bulk_export",
    "source_concurrency",
}


def _last_updated_completeness_errors(
    entry: dict[str, Any],
    resource_type: str,
    resource_stats: dict[str, Any],
) -> list[str]:
    """Validate exact partition reconciliation for sources that expose it."""

    canonical_base = str(entry.get("canonical_base") or "").rstrip("/")
    partition_source_count = resource_stats.get("last_updated_partition_sources")
    is_proof_required = (
        canonical_base == SCAN_PROVIDER_DIRECTORY_BASE
        or partition_source_count not in (None, 0)
    )
    if not is_proof_required:
        return []
    expected_source_count = len(entry["source_ids"])
    errors: list[str] = []
    if partition_source_count != expected_source_count:
        errors.append(
            f"{resource_type} lacks exact last-updated partition source metrics"
        )
    if (
        resource_stats.get("last_updated_completeness_verified_sources")
        != expected_source_count
    ):
        errors.append(
            f"{resource_type} lacks verified last-updated completeness proof"
        )
    proof_values = [
        resource_stats.get(metric_name)
        for metric_name in LAST_UPDATED_PROOF_METRIC_NAMES
    ]
    if any(
        isinstance(proof_value, bool) or not isinstance(proof_value, int)
        for proof_value in proof_values
    ) or len(set(proof_values)) != 1:
        errors.append(
            f"{resource_type} last-updated completeness counts do not reconcile"
        )
    return errors


def _caresource_completeness_errors(
    entry: dict[str, Any],
    resource_type: str,
    resource_stats: dict[str, Any],
) -> list[str]:
    """Require the durable opaque-cursor census for every CareSource source."""

    canonical_base = str(entry.get("canonical_base") or "").rstrip("/")
    proof_source_count = resource_stats.get("caresource_opaque_cursor_sources")
    is_proof_required = (
        canonical_base == CARESOURCE_PROVIDER_DIRECTORY_BASE
        or proof_source_count not in (None, 0)
    )
    if not is_proof_required:
        return []
    expected_source_count = len(entry["source_ids"])
    errors: list[str] = []
    if proof_source_count != expected_source_count:
        errors.append(f"{resource_type} lacks exact CareSource census source metrics")
    if (
        resource_stats.get("caresource_opaque_cursor_verified_sources")
        != expected_source_count
    ):
        errors.append(f"{resource_type} lacks verified CareSource census proof")
    proof_values = [
        resource_stats.get(metric_name)
        for metric_name in CARESOURCE_PROOF_METRIC_NAMES
    ]
    if any(
        isinstance(proof_value, bool) or not isinstance(proof_value, int)
        for proof_value in proof_values
    ) or len(set(proof_values)) != 1:
        errors.append(f"{resource_type} CareSource census counts do not reconcile")
    return errors


def acquisition_metric_errors(
    entry: dict[str, Any], metrics: dict[str, Any]
) -> list[str]:
    """Validate exact completion metrics for one REST acquisition entry."""

    errors: list[str] = []
    source_ids = list(entry["source_ids"])
    if metrics.get("source_ids") != source_ids:
        errors.append("metrics.source_ids does not match the endpoint")
    if metrics.get("source_import_sources_selected") != len(source_ids):
        errors.append("selected source count is not exact")
    if metrics.get("source_import_groups_attempted") != 1:
        errors.append("selected source group count is not one")
    completion = metrics.get("resource_fetch_completed_source_ids")
    completion_by_resource = completion if isinstance(completion, dict) else {}
    stats = metrics.get("resource_fetch_stats")
    stats_by_resource = stats if isinstance(stats, dict) else {}
    for resource_type in entry["resources"]:
        if completion_by_resource.get(resource_type) != source_ids:
            errors.append(f"{resource_type} did not complete for the exact source")
        resource_stats = stats_by_resource.get(resource_type)
        if (
            not isinstance(resource_stats, dict)
            or resource_stats.get("sources_completed", 0) < 1
        ):
            errors.append(f"{resource_type} lacks completed fetch metrics")
        elif resource_stats.get("sources_bounded", 0) or resource_stats.get(
            "sources_failed", 0
        ):
            errors.append(f"{resource_type} was bounded or failed")
        if isinstance(resource_stats, dict):
            errors.extend(
                _last_updated_completeness_errors(
                    entry,
                    resource_type,
                    resource_stats,
                )
            )
            errors.extend(
                _caresource_completeness_errors(
                    entry,
                    resource_type,
                    resource_stats,
                )
            )
    return errors


def bulk_acquisition_metric_errors(
    entry: dict[str, Any], metrics: dict[str, Any]
) -> list[str]:
    """Validate that every selected resource used the configured Bulk path."""

    bulk_export_mode = metrics.get("bulk_export_mode")
    if bulk_export_mode is None:
        return []
    if (
        not isinstance(bulk_export_mode, dict)
        or bulk_export_mode.get("effective") is not True
    ):
        return ["bulk_export_mode must be effective for every selected resource"]
    source_count = len(entry["source_ids"])
    expected_fetches = len(entry["resources"]) * source_count
    if bulk_export_mode.get("effective_resource_fetches") != expected_fetches:
        return ["bulk_export_mode effective resource fetch count is not exact"]
    stats_by_resource = metrics.get("resource_fetch_stats")
    if not isinstance(stats_by_resource, dict):
        return ["bulk_export_mode lacks resource fetch metrics"]
    return [
        f"{resource_type} was not effectively acquired through bulk export"
        for resource_type in entry["resources"]
        if not isinstance(stats_by_resource.get(resource_type), dict)
        or stats_by_resource[resource_type].get("bulk_export_sources") != source_count
    ]


def _positive_resource_row_count(metrics: dict[str, Any]) -> int:
    resource_rows = metrics.get("resource_rows")
    if not isinstance(resource_rows, dict):
        return 0
    total = 0
    for row_count in resource_rows.values():
        try:
            total += max(0, int(row_count or 0))
        except (TypeError, ValueError):
            continue
    return total


def external_result_errors(
    entry: dict[str, Any],
    operator_result_by_field: dict[str, Any],
    *,
    expected_importer: str,
) -> list[str]:
    """Validate a local result supplied for an externally acquired dataset."""

    errors: list[str] = []
    params_by_name = operator_result_by_field.get("params")
    params_by_name = params_by_name if isinstance(params_by_name, dict) else {}
    metrics_by_name = operator_result_by_field.get("metrics")
    metrics_by_name = metrics_by_name if isinstance(metrics_by_name, dict) else {}
    if operator_result_by_field.get("importer") != expected_importer:
        errors.append("result importer does not match the manifest")
    expected_source_ids = list(entry["source_ids"])
    if params_by_name.get("source_ids") != expected_source_ids:
        errors.append("result params.source_ids does not match the endpoint")
    if metrics_by_name.get("source_ids") != expected_source_ids:
        errors.append("result metrics.source_ids does not match the endpoint")
    if metrics_by_name.get("source_import_sources_selected") != len(
        expected_source_ids
    ):
        errors.append("result selected source count is not exact")
    if metrics_by_name.get("source_import_groups_attempted") != 1:
        errors.append("result selected source group count is not one")
    if _positive_resource_row_count(metrics_by_name) <= 0:
        errors.append("result has no imported resource rows")
    resource_stats = metrics_by_name.get("resource_fetch_stats")
    if not isinstance(resource_stats, dict) or not resource_stats:
        errors.append("result has no resource completion metrics")
    elif any(
        not isinstance(stats, dict)
        or stats.get("sources_completed", 0) < 1
        or stats.get("sources_bounded", 0)
        or stats.get("sources_failed", 0)
        for stats in resource_stats.values()
    ):
        errors.append("result contains incomplete resource metrics")
    return errors


def result_param_errors(
    manifest: dict[str, Any],
    entry: dict[str, Any],
    result_by_field: dict[str, Any],
    expected_params: dict[str, Any],
) -> list[str]:
    """Validate planned parameters while allowing controlled run lineage."""
    errors: list[str] = []
    if result_by_field.get("importer") != manifest["importer"]:
        errors.append("result importer does not match the manifest")
    actual_params = result_by_field.get("params")
    actual_params = actual_params if isinstance(actual_params, dict) else {}
    for param_name, expected_value in expected_params.items():
        if actual_params.get(param_name) != expected_value:
            errors.append(f"params.{param_name} does not match the manifest")
    unsupported_params = sorted(
        set(actual_params) - set(expected_params) - RUN_LINEAGE_PARAM_FIELDS
    )
    if unsupported_params:
        errors.append("result params are not controlled: " + ",".join(unsupported_params))
    endpoint_scope = actual_params.get("provider_directory_endpoint_scope")
    if endpoint_scope is not None and endpoint_scope != entry["canonical_base"]:
        errors.append("params.provider_directory_endpoint_scope does not match the manifest")
    pagination_attempt = actual_params.get("provider_directory_pagination_attempt")
    if pagination_attempt is not None and (
        type(pagination_attempt) is not int or pagination_attempt < 1
    ):
        errors.append("params.provider_directory_pagination_attempt is invalid")
    for run_id_field in ("provider_directory_pagination_root_run_id", "retry_of_run_id"):
        run_id = actual_params.get(run_id_field)
        if run_id is not None and (
            not isinstance(run_id, str) or not RUN_ID_PATTERN.fullmatch(run_id)
        ):
            errors.append(f"params.{run_id_field} is invalid")
    if entry["classification"] == "probe_only":
        forbidden_keys = sorted(PROBE_OMITTED_KEYS.intersection(actual_params))
        if forbidden_keys:
            errors.append(
                "probe-only result contains resource/pagination params: "
                + ",".join(forbidden_keys)
            )
    return errors


def result_source_identity_errors(
    manifest: dict[str, Any],
    entry: dict[str, Any],
    result_by_field: dict[str, Any],
) -> list[str]:
    """Reject observations that do not identify the exact configured source."""
    errors = []
    if result_by_field.get("importer") != manifest["importer"]:
        errors.append("result importer does not match the manifest")
    params_by_name = result_by_field.get("params")
    params_by_name = params_by_name if isinstance(params_by_name, dict) else {}
    result_source_ids = params_by_name.get("source_ids")
    if result_source_ids != entry["source_ids"] and (
        entry["classification"] != "manual_acquisition" or result_source_ids is not None
    ):
        errors.append("result params.source_ids does not match the endpoint")
    endpoint_scope = params_by_name.get("provider_directory_endpoint_scope")
    if endpoint_scope is not None and endpoint_scope != entry["canonical_base"]:
        errors.append("result endpoint scope does not match the manifest")
    metrics_by_name = result_by_field.get("metrics")
    metrics_by_name = metrics_by_name if isinstance(metrics_by_name, dict) else {}
    if metrics_by_name.get("source_ids") not in (None, entry["source_ids"]):
        errors.append("result metrics.source_ids does not match the endpoint")
    return errors
