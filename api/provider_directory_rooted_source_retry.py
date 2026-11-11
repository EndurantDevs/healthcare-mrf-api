# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exact rooted-operation continuity for stopped native Source retries."""

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


async def is_stopped_rooted_route_parent(connection, launch, ancestor_by_field, current_params):
    """Admit only a terminal rooted predecessor whose exact worker is freshly stopped."""
    from api import control_workers as workers
    from api.provider_directory_cms_registry_stop import require_worker_presence
    from api.provider_directory_source_worker import FHIRSourceWorkerLaunch
    from process.provider_directory_rooted_graph_source_contract import (
        PROVIDER_DIRECTORY_ROOTED_GRAPH_API_BASE,
        PROVIDER_DIRECTORY_ROOTED_GRAPH_SOURCE_ID,
    )

    params = ancestor_by_field.get("params")
    if (
        type(launch) is not FHIRSourceWorkerLaunch
        or launch.source_id != PROVIDER_DIRECTORY_ROOTED_GRAPH_SOURCE_ID
        or launch.endpoint_scope != PROVIDER_DIRECTORY_ROOTED_GRAPH_API_BASE
        or ancestor_by_field.get("run_id") == launch.run_id
        or ancestor_by_field.get("status") != "canceled"
        or ancestor_by_field.get("finished_at") is None
        or type(params) is not dict
        or "rooted_graph" not in params
        or not has_same_rooted_operation(params, current_params)
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
    await workers.acquire_control_run_worker_action_lock(connection, ancestor_by_field["run_id"])
    try:
        presence = await asyncio.to_thread(
            workers.exact_worker_presence,
            {"run_id": ancestor_by_field["run_id"], "importer": "provider-directory-fhir"},
        )
        return require_worker_presence(presence)["stopped"]
    except Exception:
        return False
