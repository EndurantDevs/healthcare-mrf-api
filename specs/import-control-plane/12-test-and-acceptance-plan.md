# 12 Test And Acceptance Plan

## Unit Tests

- Stable plan identity excludes mutable URL/name fields.
- Source file identity canonicalizes URLs.
- Resolver groups subscriptions by source file/content version.
- Quota accounting uses bytes/files/runtime/PTG slots.
- RBAC allows/denies expected actions.
- Route resolver returns one node or a clear ambiguous/not-imported result.

## Integration Tests

- Engine control starts a test import and writes `import_run`.
- Auto-finalize watcher enqueues finish once.
- Cancel flag is honored at chunk boundaries.
- Import-control mirrors engine run state.
- Scheduler fires once under concurrent workers.
- Seed import creates review records only.
- Source promotion creates active catalog source.

## Crawler Tests

- Fixture CMS master index crawl.
- Limited production crawl bounds.
- ETag unchanged no-op.
- Broken source is marked, not deleted.
- Large TOC path is streamed.
- Plan rename preserves subscription identity.

## Distributed Routing Tests

- Register two nodes.
- Base dataset freshness tracked independently.
- PTG file imported on node A routes to node A.
- Second PTG file imported on node B routes to node B.
- Disabled node is removed/de-prioritized.
- Replicated file can fail over.

## Client Safety Tests

- Client sees public sources plus own private sources.
- Client cannot see another client's private source or request identity.
- Public coverage never shows private/signed URLs.
- Client MCP routes through api-layer.

## End-To-End Acceptance

1. Admin imports seed sources.
2. Admin promotes one source.
3. Crawler discovers plans/files.
4. Client searches plan and subscribes.
5. Resolver coalesces to one file import.
6. Placement assigns a node.
7. Engine imports file and publishes snapshot.
8. Route index updates.
9. api-layer routes PTG pricing call to assigned node.
10. Dashboard shows run, placement, freshness, and coverage.

## Performance Acceptance

- Route resolution p95 under 20 ms from api-layer cache and under 100 ms on cache miss.
- Catalog search p95 under 300 ms for common filters.
- Scheduler tick handles due subscriptions without duplicate file imports.
- Crawler does not buffer full multi-GB indexes in memory.
