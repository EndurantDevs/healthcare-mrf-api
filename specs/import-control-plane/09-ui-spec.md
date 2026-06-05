# 09 UI Spec

## Purpose

Define the `api-app` user experience for admins, clients, and public coverage.

## Admin Pages

### Imports Overview

- Active runs table.
- Status, importer, node, progress, duration, queue age, requester.
- Actions: cancel, retry, view details.
- `mrf-source-discovery` run details show provider list, source filters, probe filters, URL counts,
  discovered plans/files, probe OK count, ETag/Last-Modified/size coverage, and crawl errors.

### Schedules

- Importer schedules.
- Cron/cadence editor.
- Enable/disable.
- Run now.
- Last/next run.
- Conflict/lock indicators.
- Parameter editor for source-discovery:
  - provider (`master-list`, `accessmrf`, `payerset`, `mrfdatasolutions`, `all`)
  - source entity types (`tpa`, `network/tpa`, etc.)
  - source payer query
  - crawl/check/probe flags
  - file probe entity types and payer query
  - bounded limits and concurrency

### MRF Sources

- Search/filter sources.
- Entity-type filter with first-class TPA and network/TPA values.
- Source detail with crawl history.
- Add source.
- Validate URL.
- Crawl now.
- Promote/reject seeds.
- Mark duplicate/broken/disabled.
- TPA source detail identifies the hosting style: platform TOC, metadata text index, carrier/EIN
  redirect, or unresolved landing page.

### Group Plans

- Search by payer, sponsor, issuer, state, market, name, plan id.
- Freshness badges.
- Discovered/imported/queued/stale status.
- Subscribe/unsubscribe.
- Preview source files and estimated bytes.

### Nodes

- Node health.
- Base dataset freshness.
- Disk/RAM/PTG slots.
- Queue depth.
- PTG placements.
- Drain/enable/disable.

## Client Pages

- Coverage search.
- Submitted sources.
- Plan subscriptions.
- Import requests and status.
- Quota usage.
- Clear async state: requested, queued, running, imported, failed.

## Public Pages

- `/coverage/mrf`
- `/coverage/mrf/sources`
- `/coverage/mrf/sources/[source_key]`
- `/coverage/mrf/plans`
- `/coverage/mrf/methodology`

Public pages show coverage and freshness, not control actions.

## UI Rules

- Never show private/signed URLs to unauthorized users.
- Distinguish "your request" from "shared import".
- Explain that imported CMS pricing data is shared.
- Show staleness before users subscribe.
- Prefer dense admin tables over marketing layouts for operational screens.

## Acceptance Criteria

- Admin can manage a source from seed to active crawl.
- Admin can schedule and run a TPA-only source-discovery crawl and a TPA-only file-probe smoke.
- Client can submit a source and track review/import status.
- Public user can understand coverage without logging in.
