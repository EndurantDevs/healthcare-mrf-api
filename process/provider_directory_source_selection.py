# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Source admission and bounded qualification for isolated acquisitions."""

import asyncio

from process import provider_directory_generic_source_policy as source_policy

_ISOLATED_ACQUISITION_FLAGS = {
    "import_resources": True,
    "defer_typed_materialization": True,
    "full_refresh": True,
    "probe": False,
    "bulk_export": False,
    "stale_cleanup": False,
    "publish_artifacts": False,
    "publish_after_acquisition": False,
    "publish_corroboration": False,
}


def resource_import_selection_metrics(source_count: int) -> dict[str, int]:
    """Keep the existing selection counters stable for every acquisition path."""
    return {
        "source_import_sources_considered": source_count,
        "source_import_sources_selected": 0,
        "source_import_skipped_blocked_source": 0,
        "source_import_skipped_blocked_source_shared_backend_unverified": 0,
        "source_import_skipped_blocked_source_coverage_mode_probe_only": 0,
        "source_import_skipped_blocked_source_fully_enumerable_resources_empty": 0,
        "source_import_skipped_blocked_source_upstream_search_window_incomplete": 0,
        "source_import_skipped_missing_api_base": 0,
        "source_import_skipped_probe_not_valid": 0,
        "source_import_skipped_open_only": 0,
        "source_import_skipped_validation_status": 0,
        "source_import_skipped_auth_required_policy": 0,
        "source_import_skipped_manual_only": 0,
        "source_import_sources_selected_live_probe_valid": 0,
        "source_import_sources_selected_checkpoint_retry": 0,
        "source_import_sources_selected_declared_credentialed": 0,
        "source_import_sources_selected_auth_required_seed": 0,
    }


async def qualify_stale_isolated_source(directory, source_rows, task, *, timeout, run_id):
    """Refresh one stale seed verdict with transport reads, without catalog probe writes."""
    if (
        not directory.is_source_storage_required()
        or getattr(source_policy._FACTORY.get(), "storage_binding", None) is None
        or any(task.get(name) is not required_value for name, required_value in _ISOLATED_ACQUISITION_FLAGS.items())
        or task.get("publish_source_dataset_only", False) is not False
        or task.get("seed_only")
        or task.get("test")
        or task.get("test_mode")
        or task.get("retry_of_run_id")
        or task.get("provider_directory_pagination_root_run_id")
        or len(source_rows) != 1
    ):
        return source_rows
    source_record = source_rows[0]
    if (
        task.get("source_ids") != [source_record["source_id"]]
        or directory._clean_text(source_record.get("last_validated_status")) != "valid_non_fhir"
        or directory.current_version_census_contract(source_record) is not None
        or directory._uses_alohr_graphql_connector(source_record)
        or directory._uses_uhc_official_file_connector(source_record)
    ):
        return source_rows
    decision = directory._resource_import_source_decision(
        source_record,
        valid_source_ids=None,
        open_only=bool(task.get("open_only", True)),
        include_auth_required=bool(task.get("include_auth_required", False)),
        checkpoint_retry_ids=set(),
        requested_resource_types=directory._selected_resources(task.get("resources")),
    )
    if decision.skip_metric != "source_import_skipped_validation_status":
        return source_rows
    directory._validate_provider_directory_endpoint_scope(source_rows, task.get("provider_directory_endpoint_scope"))
    try:
        probe, _capability = await asyncio.wait_for(
            directory._probe_source(source_record, timeout=timeout, run_id=run_id),
            timeout=directory._source_probe_hard_timeout_seconds(source_record, timeout=timeout),
        )
    except TimeoutError:
        return source_rows
    resource_probe = probe.get("resource_probe") or {}
    expected_base = directory._canonical_base(source_record.get("canonical_api_base") or source_record.get("api_base"))
    if (
        probe.get("status") == "valid"
        and probe.get("http_status") == 200
        and probe.get("error") is None
        and directory._canonical_base(probe.get("api_base")) == expected_base
        and resource_probe.get("status") == "reachable"
        and resource_probe.get("http_status") == 200
        and resource_probe.get("error") is None
    ):
        return [{**source_record, "last_validated_status": "valid"}]
    return source_rows
