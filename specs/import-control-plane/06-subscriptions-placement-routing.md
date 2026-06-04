# 06 Subscriptions Placement Routing

## Purpose

Define how plan subscriptions become file-level imports, how work is shared across clients, and how PTG requests route to serving nodes.

## Subscription Resolution

Plan-level subscription:

- Client/admin subscribes to `discovered_plan_id`.
- Subscription does not imply private data ownership.
- Subscription is a request to keep public CMS data fresh.

Resolver steps:

1. Load due enabled subscriptions.
2. Resolve each plan to current `discovered_plan_file` rows.
3. Group by `(source_file_id, content_version, import_month)`.
4. Reuse existing active/completed `source_file_import` where possible.
5. Select an assigned node if no reusable import exists.
6. Attach one `source_file_import_request` per client/subscription/manual request.
7. Submit one engine import per grouped file.
8. Fan out completion to every request/subscription.

## Coalescing Rules

- Execution unit is source file/content version, not plan.
- Ten clients requesting the same file create one engine run.
- Plans in the same file are refreshed together.
- Quotas are measured in file imports, bytes, runtime, and PTG slots; plan count is display metadata.

## Placement Rules

Select node by:

- Existing placement for the same source file.
- Healthy node status.
- Base dataset freshness.
- Disk free.
- PTG slot availability.
- Queue depth.
- Optional source/client affinity.
- Replication requirements.

Default PTG replication factor is 1.

## Route Index

After successful import:

- Insert/update `ptg_file_placement`.
- Generate `ptg_route_index` rows for all plans/files covered by the imported file.
- Mark old routes stale if superseded.
- Publish route update event for api-layer cache invalidation where available.

## api-layer Routing

- Non-PTG routes use default/base upstream.
- PTG routes ask `import-control` route resolver or use a short-lived local route cache.
- Exact source/plan requests route to one node.
- Ambiguous requests return an actionable error; no unbounded fanout.
- If replicated candidates exist, api-layer can retry one healthy fallback.

## Acceptance Criteria

- Multiple plan subscriptions in one file produce one import.
- Multiple client requests to one file produce one import.
- Route index can route exact-source PTG queries to the correct node.
- Disabling a node removes or de-prioritizes its routes.
