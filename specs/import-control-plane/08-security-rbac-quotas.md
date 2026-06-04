# 08 Security RBAC Quotas

## Purpose

Define authorization, visibility, secret handling, quotas, and audit.

## Roles

- `import.viewer`: read runs, schedules, sources, node status.
- `import.operator`: start/cancel/retry allowed imports; manage schedules where permitted.
- `import.admin`: manage all sources, seeds, nodes, quotas, route overrides, and force actions.
- Client import entitlement: client-safe import search/subscription/request operations through api-layer.

## Service Authentication

- Engine `/control/v1` endpoints are service-only.
- Bootstrap mode may use `HLTHPRT_CONTROL_API_TOKEN`.
- Production mode should use service JWTs issued by the platform auth service.
- Client-facing calls must never use engine service credentials directly.

## Visibility Rules

- Public coverage sees only public sources and aggregate metrics.
- Client sees public sources plus their own private source submissions.
- Client sees their own subscriptions and request records.
- Client does not see other clients' identities, private URLs, or raw submitted data.
- Imported PTG pricing data is public/shared after import.

## Secret Handling

- Do not store credentials in catalog rows.
- Store `credentials_ref` where needed.
- Redact signed URLs by default.
- Raw seed payloads are internal-only.
- Audit logs may store request hashes instead of full sensitive payloads.

## Quotas

Quota units:

- Source file imports.
- Bytes requested/imported.
- Estimated runtime.
- Actual runtime.
- PTG concurrent slots.
- Crawl bytes.
- Manual run count.

PTG plan count is not a quota unit.

## Fair Queueing

- Global PTG semaphore protects RAM.
- Per-client fair queue prevents starvation.
- Admin priority can override but is audited.
- Shared file imports can attribute cost by configured policy: first requester, split by attached requests, or platform-paid for public coverage imports.

## Audit

Audit every mutation:

- Actor.
- Client.
- Action.
- Target.
- Before/after summary.
- Request id.
- Timestamp.

## Acceptance Criteria

- Client tools cannot bypass api-layer entitlements.
- Private client URLs never appear in public APIs.
- Shared imports are visible as shared work without exposing other clients.
