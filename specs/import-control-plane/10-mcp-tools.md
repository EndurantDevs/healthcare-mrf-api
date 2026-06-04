# 10 MCP Tools

## Purpose

Define MCP tools for import discovery and control.

## Client Tools

Client tools route through `api-layer`:

- `search_mrf_sources`
- `search_group_plans`
- `get_mrf_source_coverage`
- `submit_mrf_index_source`
- `subscribe_group_plan`
- `unsubscribe_group_plan`
- `get_import_request_status`
- `list_import_requests`
- `cancel_import_request`

## Admin Tools

Admin tools may call `import-control` directly:

- `list_importers`
- `run_import`
- `list_import_runs`
- `get_import_run`
- `cancel_import_run`
- `retry_import_run`
- `list_import_schedules`
- `set_import_schedule`
- `crawl_mrf_source`
- `import_mrf_source_seeds`
- `review_mrf_source_seed`
- `list_catalog_health`
- `list_nodes`
- `list_node_placements`
- `drain_import_node`
- `repair_mrf_source`

## Safety

- Tool schemas must match API contracts.
- Client tools must not expose admin-only fields.
- Tools return run/request ids, not blocking import results.
- Long-running work is always async.

## Acceptance Criteria

- Client MCP cannot call admin-only control paths.
- Admin MCP can diagnose nodes and catalog health.
- Tool outputs are compact and redact sensitive fields.
