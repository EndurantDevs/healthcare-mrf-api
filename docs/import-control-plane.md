# HealthPorta Import Control Plane — Design Plan

## Context

Imports today are driven by a Click CLI in `healthcare-mrf-api` (25+ importers on an **arq/Redis** queue) with only read-only status endpoints, no scheduling, no cancel/retry, no unified run registry, and no way to discover which group plans exist. We want a single control plane to: schedule the "main" recurring importers, let operators **search CMS group plans and subscribe/unsubscribe** them to scheduled PTG imports, and **control live runs** (start/cancel/retry/monitor) across engines — surfaced through `api-app` (UI), `api-mcp` (agent tools), reusing `api-layer` auth.

Decisions taken: brain = **new dedicated `import-control` service**; scope = **all imports** (main ones scheduled; PTG = search + subscribe); discovery = **full CMS payer-index auto-crawl in v1**.

## Landscape (verified)

- **healthcare-mrf-api** — Sanic API (`:8085`) + Click CLI + arq workers. Importer registry in `process/__init__.py`; per-family run table `PartDImportRun` (`db/models/system.py`); rich PTG2 discovery primitives: `parse_toc_catalog_entries` (`process/ptg_parts/source_jobs.py`), `PTG2SourceCatalog` / `PTG2CurrentPlanSource` / source-identity/version/content hashes (`db/models/_legacy.py`, `source_versions.py`). Gaps: no HTTP trigger/cancel/retry, no unified registry, no CMS index crawler, no cooperative cancel.
- **api-app** — Next.js dashboard + Sanic internal API, schema `hp_api_app`; admin auth, client lifecycle, billing. Natural home for the operator **UI**.
- **api-layer** — Rust gateway (API-key/JWT auth, entitlements, rate-limit) in front of the data services. Public read edge; control plane stays admin-internal but reuses its auth model.
- **api-mcp** — Sanic + FastMCP (OAuth/PKCE, OpenAPI-driven execution, orchestrator tools). Home for **agent tools**.

## Target architecture (5 components)

### 1. Engine Control API (`control` blueprint in each engine; first in healthcare-mrf-api)
Uniform HTTP surface the brain calls. Wraps existing arq enqueue + discovery.
- `GET /control/v1/importers` — catalog from `process/__init__.py` (name, kind, schedulable, params schema).
- `POST /control/v1/imports` — enqueue a run (importer, params, `idempotency_key`, `run_id`); wraps `initiate_*` / `main()`.
- `GET /control/v1/imports?status=&importer=&since=` — list runs (from unified registry).
- `GET /control/v1/imports/{run_id}` — status + progress + metrics + error.
- `POST /control/v1/imports/{run_id}/cancel` — cooperative cancel.
- `POST /control/v1/imports/{run_id}/retry` — re-enqueue same params.
- PTG discovery/import helpers: `POST /control/v1/ptg/parse-toc-preview` and `POST /control/v1/ptg/import-file`.

### 2. Unified Run Registry (DB)
Generalize `PartDImportRun` to **all** importers: one `import_run` table — `run_id, engine, importer, status{queued|running|succeeded|failed|canceled}, params, idempotency_key, started_at, finished_at, progress(JSON), metrics(JSON), error, schedule_id, snapshot_id`. A small adapter makes every importer report state (replacing the ad-hoc `ImportLog`/`ImportHistory`/`PartDImportRun` mix). **Truth lives in the engine; the brain mirrors/aggregates** — keeps engines autonomous and the brain resilient.

### 3. `import-control` service — the brain
- **Scheduler**: runtime-editable `schedule` table (`importer, engine, cron, params, enabled, last_run, next_run`); a scheduler worker (APScheduler or arq-cron) fires → calls engine `POST /imports`. Main importers (NPI, geo, medicare, pharmacy, claims, formulary…) get default cadences.
- **PTG plan subscriptions**: `ptg_plan_subscription` (`plan_id, market_type, issuer, cadence, enabled`). A monthly resolver maps subscribed plans → current TiC source files (via discovery) → enqueues PTG imports for just those plans.
- **CMS auto-crawl**: recurring job fetches the CMS master payer index → enumerates payer TiC index URLs → crawls `reporting_structure` → upserts a `discovered_plan` catalog (`plan_id, issuer, sponsor/EIN, market_type, index_url, in_network_file refs, last_seen, content version`). Incremental, ETag-aware, rate-limited, resumable; stores raw index snapshots. Powers plan search.
- **Policy & aggregation**: dedupe concurrent runs (one active per importer/plan), retry/backoff, **global concurrency caps** (incl. the PTG scanner memory budget — see cross-cutting), audit log, run mirror.
- **Internal Control API**: what api-app/api-mcp call; mirrors engine control + adds schedule/subscription/catalog, route resolution, quota, source crawl/validate, source-file import, audit, ops dashboard, and alert endpoints. Service-to-service JWT (reuse api-app delegation). Admin-only.

### 4. api-app — operator dashboard
New "Imports" section:
- **Schedules**: main importers, cron, enable/disable, edit cadence, next/last run, run-now.
- **Group Plans**: search the `discovered_plan` catalog (issuer, sponsor/employer, state, market type, name), freshness, subscribe/unsubscribe a plan or whole issuer, "what would import" preview.
- **Runs**: live cross-engine table (status, progress, duration, rows) with cancel/retry and drill-down.
Backed by api-app's Sanic internal API proxying to the brain; reuse admin auth/RBAC.

### 5. api-mcp — agent tools (FastMCP, admin scope)
Mirror the brain: `list_importers`, `get/set_import_schedule`, `run_import`, `list/get/cancel_import_run`; `search_group_plans`, `preview_plan_import`, `subscribe_plan`/`unsubscribe_plan`, `crawl_cms_index`. Reuses existing OAuth/PKCE + delegation.

### 6. api-layer — client-safe import edge
The control plane remains admin-internal, but `api-layer` exposes the client-safe subset through authenticated `/api/v1/imports/*` routes:
- Importer/run discovery and client PTG run requests, with idempotency keys.
- Client-visible coverage source and group-plan lookup.
- Client source submission and group-plan subscription management.

Public read-only coverage metadata remains available under `/api/v1/coverage/mrf/*`.

## Cross-cutting
- **Cancellation** (hardest engine change): cooperative — workers poll a Redis `cancel:{run_id}` flag at chunk boundaries and abort; not forced kill.
- **Idempotency**: `idempotency_key` on enqueue prevents duplicate runs (schedule misfires, double-clicks).
- **Concurrency policy**: brain enforces global caps. **Tie-in to the PTG memory work** — until the scanner spill-to-disk fix lands, cap concurrent PTG imports to the RAM budget (one big file ≈ tens of GB); the brain is where that policy is enforced.
- **Config store**: schedules/subscriptions/crawl targets live in the brain's own DB schema (not `.env`). Source credentials still per-engine.
- **Observability**: one progress/metrics schema persisted to `import_run`; engines push async status events to the brain. Normal dashboard/API reads use the central mirror and do not add load to importer nodes. Live Redis progress expires, smoke history defaults to 10 days, and terminal mirror history defaults to 90 days.

## Phasing
- **P0 — Engine foundations** (healthcare-mrf-api): unified `import_run` registry + adapter for all importers; `control` blueprint (importers, imports CRUD, status); cooperative cancel; idempotency.
- **P1 — Brain MVP**: `import-control` repo; `schedule` table + scheduler worker; calls engine control; Runs aggregation; schedule the main importers.
- **P2 — PTG discovery**: CMS master-index crawler → `discovered_plan` catalog; plan search API; `ptg_plan_subscription` + monthly resolver.
- **P3 — UI**: api-app Imports section (admin Schedules, Group Plans, Runs live; client source submissions, subscriptions, and PTG import requests).
- **P4 — MCP**: api-mcp tools mirroring admin brain actions plus client-safe import request and coverage tools.
- **P5 — Hardening**: RBAC, audit, retry/backoff, concurrency policy, alerting, add engines (drug-api, rx-data-importer).

## Key tradeoffs
- Registry ownership: per-engine truth + brain mirror (chosen) vs centralized — chosen keeps engines autonomous.
- Scheduler: runtime DB-backed schedules + worker (chosen) vs code-defined cron — chosen so operators edit cadence without deploys.
- CMS crawl: incremental/ETag/resumable (chosen) vs full re-fetch — the index is large; never full-crawl naively.

## Verification
- Engine: unit tests for control endpoints; integration test enqueues a `--test` import, polls status, cancels, retries; cancel flag honored at a chunk boundary.
- Brain: schedule fires → mock engine receives enqueue; subscription resolver enqueues exactly the subscribed plans; crawler upserts catalog from a fixture CMS index (ETag no-op on re-run).
- E2E: api-app triggers a test import and shows it live; an MCP tool runs + cancels an import; subscribe a plan → next monthly tick enqueues it.
