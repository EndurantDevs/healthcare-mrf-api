# 13 Implementation Phases

## P0: Specs And Contracts

- Review this spec package.
- Resolve open questions.
- Add API contract fixtures.
- Create implementation tickets by phase.

Exit criteria:

- Data identities, APIs, route behavior, and security rules are approved.

## P1: Engine Control Foundation

- Add engine `import_run`.
- Add `/control/v1`.
- Add importer registry.
- Add idempotency.
- Add test import start/status/cancel/retry.
- Add auto-finalize watcher for one importer, then expand.

Exit criteria:

- A test import can run without CLI intervention.

## P2: Import-Control Core

- Create service/repo.
- Add schema and migrations.
- Add node registry.
- Add run mirror.
- Add schedules.
- Add quotas/locks/audit.

Exit criteria:

- Schedule fires and creates engine run.

## P3: Coverage Catalog

- Add source catalog.
- Add seed import/review.
- Add source validation.
- Add discovery plan/file tables.
- Add `mrf-source-discovery` importer and worker registration.
- Add curated issuer/network/TPA source registry files.
- Add internal search.

Exit criteria:

- Admin can promote a source and search discovered plans from fixture crawl.

## P4: Crawler

- Add streaming crawl.
- Add ETag skips.
- Add crawl checkpoints.
- Add limited production crawl.
- Add platform resolvers for HealthSparq/Aetna Health1, Sapphire, UHC/Optum, Highmark, and TPA metadata text indexes.
- Add file `HEAD` probing for size, ETag, and Last-Modified.
- Add coverage metric materialization.

Exit criteria:

- Limited crawl completes within configured bounds and re-run skips unchanged sources.
- TPA-only crawl and probe smoke complete without full body downloads.

## P5: Subscriptions, Coalescing, Placement

- Add plan subscriptions.
- Add file-level resolver.
- Add shared import requests.
- Add node placement.
- Add PTG file placement and route index.

Exit criteria:

- Multiple clients/plans in one file create one engine run and route index entry.

## P6: api-layer Routing And Client APIs

- Add multi-upstream MRF config.
- Add route resolver client/cache.
- Add client-safe import/coverage routes.
- Keep pricing response shapes unchanged.

Exit criteria:

- PTG pricing routes to node with imported data.

## P7: api-app UI

- Add admin dashboards.
- Add client import pages.
- Add public coverage pages.
- Add admin schedule controls for source-discovery provider/source/probe filters.

Exit criteria:

- Admin/client/public flows pass E2E tests.

## P8: api-mcp Tools

- Add client and admin tools.
- Route client tools through api-layer.
- Add tool schema tests.

Exit criteria:

- MCP can search, subscribe, start, monitor, and cancel allowed imports.

## P9: Hardening

- Expand cancellation coverage.
- Add alerts/dashboards.
- Add fair queueing.
- Add node drain/failover operations.
- Run full import and route tests across two nodes.

Exit criteria:

- System is ready for production canary.
