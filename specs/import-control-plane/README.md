# Import Control Plane Specifications

This package is the implementation contract for the HealthPorta import control plane. It replaces the older one-page design note with a spec-first breakdown that can be executed phase by phase across `healthcare-mrf-api`, the new `import-control` service, `api-app`, `api-layer`, and `api-mcp`.

The control plane has five goals:

- Maintain a searchable MRF/PTG coverage catalog.
- Include issuer, network, and TPA-hosted TiC sources in that catalog.
- Let admins and entitled clients discover group plans and request imports.
- Schedule and control all major import families.
- Coalesce PTG execution by source file and share completed imports across clients.
- Route PTG API calls to the serving node that owns the requested snapshot.

## Spec Map

- [01 Product Requirements](01-product-requirements.md)
- [02 Architecture](02-architecture.md)
- [03 Data Model](03-data-model.md)
- [04 API Contracts](04-api-contracts.md)
- [05 PTG Discovery And Crawler](05-ptg-discovery-and-crawler.md)
- [06 Subscriptions Placement Routing](06-subscriptions-placement-routing.md)
- [07 Import Lifecycle](07-import-lifecycle.md)
- [08 Security RBAC Quotas](08-security-rbac-quotas.md)
- [09 UI Spec](09-ui-spec.md)
- [10 MCP Tools](10-mcp-tools.md)
- [11 Observability Ops](11-observability-ops.md)
- [12 Test And Acceptance Plan](12-test-and-acceptance-plan.md)
- [13 Implementation Phases](13-implementation-phases.md)

## Non-Negotiable Design Rules

- PTG subscriptions are plan-level; PTG execution is file-level.
- A single source file/content version is imported once and shared across every requesting client.
- PTG imported pricing is public CMS data in shared snapshots; client ownership applies to requests, subscriptions, private source submissions, quotas, and UI visibility.
- Plan identity must not include mutable names, TOC URLs, file URLs, or source placement.
- The central control service owns global catalog/routing knowledge; serving nodes own local import execution and PostgreSQL data.
- Client-facing control APIs go through `api-layer`; admin-only control may call `import-control` directly.
- Public coverage APIs must never leak private client source URLs, signed URLs, credentials, internal node placement, or raw crawler payloads.

## Implementation Gate

Implementation should not start for a phase until the relevant spec is reviewed, open questions are resolved or explicitly deferred, and the acceptance tests for that phase are listed in [12 Test And Acceptance Plan](12-test-and-acceptance-plan.md).
