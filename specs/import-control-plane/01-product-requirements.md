# 01 Product Requirements

## Purpose

Create a single import control plane for operators, clients, public coverage discovery, and agent tools. The first version must control existing imports, discover and catalog MRF/PTG sources, subscribe group plans to refresh workflows, and track which serving node holds which PTG data.

## Audiences

- Platform admins: operate all imports, manage source catalog, schedule recurring jobs, control nodes, inspect failures, override quotas.
- Client users: search public coverage, submit their own index/TOC URLs, subscribe group plans, request allowed imports, view their own request status.
- Public visitors: see HealthPorta MRF/PTG coverage and freshness without authenticated import control.
- Agent users: use MCP tools to search plans, subscribe, start allowed imports, and inspect run state.
- Engineers/on-call: troubleshoot import failures, route-index gaps, node drift, crawler failures, and stuck runs.

## Product Capabilities

- Catalog payer/MRF sources with searchable metadata, freshness, crawl status, and import coverage.
- Search group plans by payer, sponsor, issuer, plan id, market type, state, and imported/not-imported status.
- Let clients submit MRF index/TOC URLs; keep them private until admin promotion.
- Let admins ingest third-party source seeds into a review queue.
- Let admins and entitled clients create plan subscriptions.
- Resolve subscriptions to source files, coalesce work, and schedule file-level PTG imports.
- Control all import families through a unified run registry and state machine.
- Run base imports across serving nodes and PTG imports on selected nodes.
- Route PTG API traffic to nodes with the requested data.
- Show public coverage pages with safe counts, freshness, and methodology.

## Success Metrics

- A plan search can show whether a plan is discovered, imported, stale, queued, or unavailable.
- Multiple subscriptions to plans in the same source file create one import execution.
- Multiple clients requesting the same source file share one execution.
- Admin dashboard shows every node, base dataset freshness, PTG placements, active runs, and errors.
- `api-layer` can route PTG pricing requests to the correct node without fanout.
- A client can submit a TOC URL, request validation, subscribe a plan, and track the import asynchronously.
- Public coverage pages expose useful payer/source coverage without leaking private or signed URLs.

## Out Of Scope For V1

- Automatic arbitrary web crawling beyond configured master indexes, source seeds, and submitted URLs.
- Tenant-isolated PTG data copies.
- Blocking import execution inside public API requests.
- Automatic Postgres replication between serving nodes.
- Forced process cancellation as the primary cancel mechanism.
- Billing implementation details beyond usage attribution fields.

## Acceptance Criteria

- Product flows exist for admin source review, client source submission, plan search, subscription, run monitoring, cancel/retry, node status, and public coverage.
- Every user-visible action maps to an API contract in `04-api-contracts.md`.
- Every mutable action has RBAC and audit behavior defined in `08-security-rbac-quotas.md`.
