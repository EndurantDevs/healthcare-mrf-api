# 04 API Contracts

## Purpose

Define the HTTP contracts between `import-control`, engines, `api-layer`, `api-app`, and `api-mcp`.

## Shared Conventions

- JSON request/response bodies.
- UTC ISO-8601 timestamps.
- Cursor pagination for lists.
- Idempotency keys on all start/import mutations.
- Engine `/control/v1` endpoints require service authentication. Bootstrap deployments may use `HLTHPRT_CONTROL_API_TOKEN` via `Authorization: Bearer ...` or `X-HealthPorta-Control-Token`; production should move to service JWT with the same route contracts.
- Error shape:

```json
{
  "error": {
    "code": "invalid_request",
    "message": "Human readable summary",
    "detail": {},
    "request_id": "req_..."
  }
}
```

Error codes:

- `invalid_request`
- `unauthorized`
- `forbidden`
- `not_found`
- `conflict`
- `quota_exceeded`
- `engine_busy`
- `node_unavailable`
- `not_imported`
- `stale_route`
- `internal`

## Engine `/control/v1`

- `GET /importers`
- `POST /imports`
- `GET /imports`
- `GET /imports/{run_id}`
- `POST /imports/{run_id}/cancel`
- `POST /imports/{run_id}/retry`
- `POST /ptg/parse-toc-preview`
- `POST /ptg/import-file`
- `GET /health/node`

`POST /imports` body:

```json
{
  "run_id": "optional",
  "importer": "ptg",
  "params": {},
  "idempotency_key": "client-source-content-month",
  "triggered_by": "subscription",
  "schedule_id": null,
  "subscription_id": null,
  "source_file_import_id": null
}
```

## Import-Control Internal API

Catalog:

- `GET /v1/catalog/sources`
- `POST /v1/catalog/sources`
- `GET /v1/catalog/sources/{source_id}`
- `PATCH /v1/catalog/sources/{source_id}`
- `POST /v1/catalog/sources/{source_id}/validate`
- `POST /v1/catalog/sources/{source_id}/crawl`
- `POST /v1/catalog/seeds/import`
- `GET /v1/catalog/seeds`
- `POST /v1/catalog/seeds/{seed_id}/promote`
- `POST /v1/catalog/seeds/{seed_id}/reject`

Plans/subscriptions:

- `GET /v1/ptg/plans`
- `GET /v1/ptg/plans/{discovered_plan_id}`
- `GET /v1/ptg/subscriptions`
- `POST /v1/ptg/subscriptions`
- `DELETE /v1/ptg/subscriptions/{subscription_id}`
- `POST /v1/ptg/subscriptions/{subscription_id}/run-now`

Runs/schedules:

- `GET /v1/importers`
- `GET /v1/runs`
- `POST /v1/runs`
- `GET /v1/runs/{run_id}`
- `POST /v1/runs/{run_id}/cancel`
- `POST /v1/runs/{run_id}/retry`
- `GET /v1/schedules`
- `POST /v1/schedules`
- `PATCH /v1/schedules/{schedule_id}`
- `POST /v1/schedules/{schedule_id}/pause`
- `POST /v1/schedules/{schedule_id}/resume`

Nodes/routing:

- `GET /v1/nodes`
- `GET /v1/nodes/{node_id}`
- `PATCH /v1/nodes/{node_id}`
- `GET /v1/nodes/{node_id}/datasets`
- `GET /v1/ptg/routes`
- `POST /v1/ptg/routes/resolve`
- `POST /v1/ptg/imports/{source_file_import_id}/replicate`

## Client-Safe api-layer Routes

- `GET /api/v1/imports/importers`
- `GET /api/v1/imports/runs`
- `GET /api/v1/imports/runs/{run_id}`
- `POST /api/v1/imports/runs`
- `POST /api/v1/imports/runs/{run_id}/cancel`
- `GET /api/v1/imports/coverage/sources`
- `GET /api/v1/imports/coverage/sources/{source_id}`
- `GET /api/v1/imports/coverage/group-plans`
- `POST /api/v1/imports/coverage/client-sources`
- `POST /api/v1/imports/coverage/group-plans/{discovered_plan_id}/subscribe`
- `DELETE /api/v1/imports/coverage/group-plans/{discovered_plan_id}/subscribe`

Client responses must omit other clients' request identities, private URLs, signed URLs, node internals, and raw seed payloads.

## Public Coverage API

- `GET /api/v1/coverage/mrf/summary`
- `GET /api/v1/coverage/mrf/sources`
- `GET /api/v1/coverage/mrf/sources/{source_key}`
- `GET /api/v1/coverage/mrf/group-plans`

Public API exposes only public sources and aggregate/imported coverage metrics.

## Route Resolution API

`api-layer` calls `POST /v1/ptg/routes/resolve` with PTG request metadata:

```json
{
  "plan_id": "TESTPLAN001",
  "plan_id_type": "ein",
  "market_type": "group",
  "source_key": "optional",
  "code_system": "CPT",
  "code": "70551"
}
```

Response:

```json
{
  "status": "routable",
  "node_id": "mrf-east-1",
  "base_url": "https://mrf-east-1.internal",
  "snapshot_id": "snapshot",
  "source_key": "example_source_a",
  "cache_ttl_seconds": 60
}
```

If ambiguous, return `not_imported` or `ambiguous_route`; do not fan out in v1.

## Acceptance Criteria

- Every UI/MCP action maps to a contract.
- Client and admin routes have distinct authorization behavior.
- Route resolution can return a single node without engine DB access.
