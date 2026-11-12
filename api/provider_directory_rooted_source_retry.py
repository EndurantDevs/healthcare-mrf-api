# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Checked rooted and partitioned continuity for stopped native Source retries."""

import asyncio


def has_same_rooted_operation(params, current_params):
    """Preserve ordinary ancestry and require the complete immutable rooted operation."""
    if "rooted_graph" not in params and "rooted_graph" not in current_params:
        return True
    from process.provider_directory_rooted_graph_worker import validated_rooted_graph_task

    try:
        return validated_rooted_graph_task(params) == validated_rooted_graph_task(current_params)
    except ValueError:
        return False


def _is_rooted_route_parent(launch, params, current_params):
    from process.provider_directory_rooted_graph_source_contract import (
        PROVIDER_DIRECTORY_ROOTED_GRAPH_API_BASE,
        PROVIDER_DIRECTORY_ROOTED_GRAPH_SOURCE_ID,
    )

    return (
        launch.source_id == PROVIDER_DIRECTORY_ROOTED_GRAPH_SOURCE_ID
        and launch.endpoint_scope == PROVIDER_DIRECTORY_ROOTED_GRAPH_API_BASE
        and "rooted_graph" in params
        and has_same_rooted_operation(params, current_params)
    )


def _has_partition_audit_root(audit, launch, ancestor_by_field, params):
    root_key = "provider_directory_pagination_root_run_id"
    return root_key in audit and (
        audit[root_key] == launch.root_run_id
        or audit[root_key] is None
        and ancestor_by_field["run_id"] == launch.root_run_id
        and params.get(root_key) is None
    )


def _partition_audit_group(launch, ancestor_by_field, current_params):
    """Use the retained server audit, never a request-supplied strategy claim."""
    params = ancestor_by_field["params"]
    audit = ancestor_by_field.get("source_audit")
    lineage_keys = {
        "retry_of_run_id",
        "provider_directory_pagination_root_run_id",
        "provider_directory_pagination_attempt",
    }
    if (
        "rooted_graph" in params
        or ancestor_by_field.get("started_at") is None
        or params.get("allow_retained_pagination_reset") is not False
        or {key: param_value for key, param_value in params.items() if key not in lineage_keys}
        != {key: param_value for key, param_value in current_params.items() if key not in lineage_keys}
        or type(audit) is not dict
        or not _has_partition_audit_root(audit, launch, ancestor_by_field, params)
        or audit.get("retry_of_run_id") != params.get("retry_of_run_id")
        or audit.get("source_ids") != [launch.source_id]
        or audit.get("provider_directory_endpoint_scope") != launch.endpoint_scope
        or audit.get("provider_directory_endpoint_scope_resolved") != launch.endpoint_scope
        or any(
            audit.get(key) is not expected
            for key, expected in (
                ("allow_retained_pagination_reset", False),
                ("pagination_checkpoints_enabled", True),
                ("full_refresh", True),
                ("bulk_export", False),
            )
        )
        or any(type(audit.get(key)) is not int or audit[key] != 0 for key in ("resource_limit", "page_limit"))
    ):
        return None
    proxy = audit.get("source_http_proxy")
    if proxy is not None and (
        type(proxy) is not dict
        or proxy.get("binding") is not None
        or proxy.get("cooldowns") is not None
        or proxy.get("enabled") is True
    ):
        return None
    groups = audit.get("active_source_groups")
    if type(groups) is not list or len(groups) != 1 or type(groups[0]) is not dict:
        return None
    group = groups[0]
    if (
        group.get("source_ids") != [launch.source_id]
        or type(group.get("source_count")) is not int
        or group["source_count"] != 1
        or group.get("api_base") != launch.endpoint_scope
    ):
        return None
    return group


def _is_partition_route_parent(launch, ancestor_by_field, current_params):
    """Admit strategy continuity; Source still verifies the full checkpoint and input proof."""
    from process.provider_directory_fhir import _selected_resources

    group = _partition_audit_group(launch, ancestor_by_field, current_params)
    if group is None:
        return False
    resources = set(_selected_resources(ancestor_by_field["params"].get("resources")))
    timings, scans = group.get("resource_fetch_timing"), group.get("resource_scans")
    if (
        not resources
        or type(timings) is not dict
        or type(scans) is not dict
        or set(timings) != resources
        or set(scans) != resources
        or any(type(timing) is not dict or type(timing.get("fetch_mode")) is not str for timing in timings.values())
    ):
        return False
    partitioned_resources = {
        name for name, timing in timings.items() if timing.get("fetch_mode") == "last_updated_partition"
    }
    if not partitioned_resources:
        return False
    for name, scan in scans.items():
        if type(scan) is not dict or scan.get("error") is not None or type(scan.get("complete")) is not bool:
            return False
        if name in partitioned_resources:
            if scan.get("state") not in {"fetching", "complete"} or scan["complete"] != (scan["state"] == "complete"):
                return False
        elif (
            scan.get("state") != "complete"
            or scan["complete"] is not True
            or type(scan.get("pages_processed")) is not int
            or scan["pages_processed"] < 1
            or type(scan.get("rows_processed")) is not int
            or scan["rows_processed"] < 0
        ):
            return False
    return True


async def is_stopped_source_route_parent(connection, launch, ancestor_by_field, current_params):
    """Admit a checked predecessor only while its exact worker is freshly stopped."""
    from api import control_workers as workers
    from api.provider_directory_cms_registry_stop import require_worker_presence
    from api.provider_directory_source_worker import FHIRSourcePublicationLaunch, FHIRSourceWorkerLaunch

    params = ancestor_by_field.get("params")
    if (
        type(launch) not in (FHIRSourceWorkerLaunch, FHIRSourcePublicationLaunch)
        or ancestor_by_field.get("run_id") == launch.run_id
        or ancestor_by_field.get("status") != "canceled"
        or ancestor_by_field.get("finished_at") is None
        or type(params) is not dict
        or type(current_params) is not dict
        or ancestor_by_field.get("run_id") == launch.root_run_id
        and (
            ancestor_by_field.get("retry_of_run_id") is not None
            or params.get("retry_of_run_id") is not None
            or params.get("provider_directory_pagination_root_run_id") not in (None, launch.root_run_id)
        )
        or ancestor_by_field.get("source_http_proxy_binding") is not None
        or ancestor_by_field.get("source_http_proxy_cooldowns") is not None
    ):
        return False
    if not (
        type(launch) is FHIRSourceWorkerLaunch
        and _is_rooted_route_parent(launch, params, current_params)
        or _is_partition_route_parent(launch, ancestor_by_field, current_params)
    ):
        return False
    await workers.acquire_control_run_worker_action_lock(connection, ancestor_by_field["run_id"])
    try:
        presence = await asyncio.to_thread(
            workers.exact_worker_presence,
            {"run_id": ancestor_by_field["run_id"], "importer": "provider-directory-fhir"},
        )
        return require_worker_presence(presence)["stopped"]
    except Exception:
        return False
